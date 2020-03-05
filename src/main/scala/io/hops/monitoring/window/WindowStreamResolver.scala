package io.hops.monitoring.window

import java.sql.Timestamp

import io.hops.monitoring.streams.resolver.{StreamResolverBase, StreamResolverSignature, StreamResolverType}
import io.hops.monitoring.util.Constants.File
import io.hops.monitoring.util.Constants.Window._
import io.hops.monitoring.util.DataFrameUtil.Encoders
import io.hops.monitoring.util.{LoggerUtil, WindowUtil}
import io.hops.monitoring.util.RichOption._
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}

import scala.collection.immutable.HashMap

class WindowStreamResolver(wdf: WindowedDataFrame, timeColName: String) extends StreamResolverBase with java.io.Serializable {

  val signature: StreamResolverSignature = StreamResolverSignature(
    java.util.UUID.randomUUID.toString,
    StreamResolverType.Window,
    HashMap("timestampColName" -> timeColName))

  private var logsPath = WindowUtil.defaultWindowStreamResolverLogsPath(signature)
  private val queryName = WindowUtil.windowStreamResolverQueryName(signature)
  private val setting: WindowSetting = WindowUtil.defaultSetting
  private val df: DataFrame = wdf.df

  private var sampleSize: Option[Int] = None
  private var resolve: (WindowSetting, Long) => WindowSetting = defaultResolver
  private var resolverCallback: Option[StreamResolverSignature => Any] = None
  private var sq: Option[StreamingQuery] = None

  // Access methods

  private def setSampleSize(sampleSize: Int): Unit = this.sampleSize = Some(sampleSize)
  private def setResolver(resolver: (WindowSetting, Long) => WindowSetting): Unit = this.resolve = resolver
  private def setLogsPath(logsPath: String): Unit = this.logsPath = logsPath

  // Window construction

  private def buildWdf: DataFrame = {
    LoggerUtil.log.info(s"[WindowStreamResolver] Building WDF with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

    df.select(col(timeColName)) // shrink dataframe
      .withWatermark(timeColName, WindowUtil.durationToString(setting.watermarkDelay))
      .withColumn(WindowColName, window(col(timeColName), WindowUtil.durationToString(setting.duration), WindowUtil.durationToString(setting.slideDuration)))
  }
  private def buildKvgd(wwdf: DataFrame): KeyValueGroupedDataset[Window, Row] =
    wwdf.groupByKey[Window](selectWindow)(Encoders.windowEncoder)

  private def buildMdf(kvgd: KeyValueGroupedDataset[Window, Row], rowSchema: StructType): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(rowSchema)
    kvgd.mapGroups(computeGroupCount)(rowEncoder)
  }

  private def buildSw(mdf: DataFrame): DataStreamWriter[Row] = {
    mdf.writeStream
      .format(File.ParquetFormat)
      .queryName(queryName)
      .outputMode(OutputMode.Append())
      .option(File.Path, s"$logsPath-parquet")
      .option(File.CheckpointLocation, s"$logsPath-parquet-checkpoint")
  }

  // Behavior

  private def defaultResolver(setting: WindowSetting, rps: Long): WindowSetting = {
    assert(sampleSize isDefined)

    val duration = sampleSize.get / rps
    val slideDuration = duration * 0.1.toLong
    val watermarkDelay = duration * 0.4.toLong

    // Window estimation
    WindowSetting(Seconds(duration), Seconds(slideDuration), Seconds(watermarkDelay))
  }

  private def takeAction(setting: WindowSetting): Unit = {
    wdf.setWindow(setting) // Update window settings
    resolverCallback!(_(signature)) // Callback
  }

  private def selectWindow: Row => Window = t => {
    val windowRow = t.getAs[Row](WindowColName)
    Window(windowRow.getAs[Timestamp](0), windowRow.getAs[Timestamp](1))
  }

  private def process(rqs: Long): (WindowSetting, WindowSetting, Boolean) = {
    LoggerUtil.log.info(s"[WindowStreamResolver] Processing window with RQS $rqs")

    // estimate next window setting
    val setting = wdf.getSetting
    val nextSettingRaw = resolve(setting, rqs) // decide next window setting
    val nextSetting = processSetting(setting, nextSettingRaw) // (smooth, ...)

    // decide to take action
    val execute = isActionRequired(setting, nextSetting)
    if (execute) {
      takeAction(nextSetting)
    }

    (setting, nextSetting, execute)
  }

  private def processSetting(setting: WindowSetting, nextSetting: WindowSetting): WindowSetting = {
    // TODO: Process window (smooth, ...)
    WindowSetting(setting.duration, setting.slideDuration, setting.watermarkDelay)
  }

  private def isActionRequired(setting: WindowSetting, nextSetting: WindowSetting): Boolean = {
    // TODO: Decide to take action or not
    false
  }

  private def buildRow(window: Window, nextSettingRaw: WindowSetting, nextSetting: WindowSetting, executed: Boolean): Row =
    Row(Row(window.start, window.end),
      nextSettingRaw.duration.milliseconds, nextSettingRaw.slideDuration.milliseconds, nextSettingRaw.watermarkDelay.milliseconds,
      nextSetting.duration.milliseconds, nextSetting.slideDuration.milliseconds, nextSetting.watermarkDelay.milliseconds, executed)

  private def getSchema(df: DataFrame): StructType = {
    new StructType(Array(
      df.schema.find(f => f.name == WindowColName).get,
      StructField(RawDurationColName, LongType), StructField(RawSlideDurationColName, LongType), StructField(RawWatermarkDelayColName, LongType),
      StructField(DurationColName, LongType), StructField(SlideDurationName, LongType), StructField(WatermarkDelayColName, LongType),
      StructField(ExecutedColName, BooleanType)
    ))
  }

  private def computeGroupCount(window: Window, instances: Iterator[Row]): Row = {
    val rqs = instances.size
    LoggerUtil.log.info(s"[WindowStreamResolver] Computing group count ($rqs instances)")

    val (rawSetting, setting, executed) = process(rqs) // Resolve new window settings
    buildRow(window, rawSetting, setting, executed) // Create row
  }

  // Overrides

  override def isActive: Boolean = sq.isDefined && sq.get.isActive

  override def start(callback: StreamResolverSignature => Unit): Unit = {
    LoggerUtil.log.info(s"[WindowStreamResolver] Starting resolver over $timeColName with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

    if (isActive) {
      LoggerUtil.log.info(s"[WindowStreamResolver] WindowStreamResolver already active")
      return
    }

    resolverCallback = Some(callback)

    val wwdf = buildWdf
    val kvgd = buildKvgd(wwdf)
    val mdf = buildMdf(kvgd, getSchema(wwdf))
    val sw = buildSw(mdf)

    val swsq = sw.start

    LoggerUtil.log.info(s"[WindowStreamResolver] Starting resolver: DONE")
    sq = Some(swsq)
  }

  override def stop: Unit = sq!(_.stop)
}

object WindowStreamResolver {
  class Apply(wdf: WindowedDataFrame, timeColName: String) {
    // Logs
    def apply(logsPath: String): WindowStreamResolver = {
      val wsr = new WindowStreamResolver(wdf, timeColName)
      wsr.setLogsPath(logsPath)
      wsr
    }
    // Sample size
    def apply(sampleSize: Int, logsPath: String): WindowStreamResolver = {
      val wsr = new WindowStreamResolver(wdf, timeColName)
      if (logsPath nonEmpty) wsr.setLogsPath(logsPath)
      wsr.setSampleSize(sampleSize)
      wsr
    }
    // Custom resolver
    def apply(resolver: (WindowSetting, Long) => WindowSetting, logsPath: String): WindowStreamResolver = {
      val wsr = new WindowStreamResolver(wdf, timeColName)
      if (logsPath nonEmpty) wsr.setLogsPath(logsPath)
      // wsr.setSampleSize(_) // TODO: Estimate initial sample size
      wsr.setResolver(resolver)
      wsr
    }
  }
//  implicit def applyToCall(a: Apply): WindowStreamResolver = a()
  def apply(wdf: WindowedDataFrame, timeColName: String): Apply = new Apply(wdf, timeColName)
}
