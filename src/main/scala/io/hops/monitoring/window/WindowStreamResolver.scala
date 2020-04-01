package io.hops.monitoring.window

import java.sql.Timestamp

import io.hops.monitoring.streams.manager.StreamManager
import io.hops.monitoring.streams.resolver.{StreamResolverBase, StreamResolverSignature, StreamResolverType}
import io.hops.monitoring.util.Constants.{File, Stats}
import io.hops.monitoring.util.Constants.Window._
import io.hops.monitoring.util.DataFrameUtil.Encoders
import io.hops.monitoring.util.{LoggerUtil, WindowUtil}
import io.hops.monitoring.util.RichOption._
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, Row}
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType, TimestampType}

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
//  private val countSchema: StructType = getCountSchema
//  private val countSchemaIndexMap = countSchema.fieldNames.zipWithIndex.toMap
  private val actionSchema: StructType = getActionSchema

  private var sampleSize: Option[Int] = None
  private var resolve: (WindowSetting, Long) => WindowSetting = defaultResolver
  private var resolverCallback: Option[StreamResolverSignature => Any] = None
  private var sq: Option[StreamingQuery] = None

  // Access methods

  private def setSampleSize(sampleSize: Int): Unit = this.sampleSize = Some(sampleSize)
  private def setResolver(resolver: (WindowSetting, Long) => WindowSetting): Unit = this.resolve = resolver
  private def setLogsPath(logsPath: String): Unit = this.logsPath = logsPath

  // Query

  private def buildWdf: DataFrame = {
    LoggerUtil.log.info(s"[WindowStreamResolver] Building WDF with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

    df.select(col(timeColName)) // shrink dataframe
      .withWatermark(timeColName, WindowUtil.durationToString(setting.watermarkDelay))
      .withColumn(WindowColName, window(col(timeColName), WindowUtil.durationToString(setting.duration), WindowUtil.durationToString(setting.slideDuration)))
  }
  private def buildKvgd(wwdf: DataFrame): KeyValueGroupedDataset[Window, Row] =
    wwdf.groupByKey[Window](selectWindow)(Encoders.windowEncoder)

  private def selectWindow: Row => Window = row =>
    rowToWindow(row.getAs[Row](WindowColName))

  private def rowToWindow(row: Row): Window =
    Window(row.getAs[Timestamp](0), row.getAs[Timestamp](1))

  private def buildSw(cdf: Dataset[(Window, Long)]): DataStreamWriter[(Window, Long)] = {
    cdf.writeStream
      .format("io.hops.monitoring.io.sink.BasicSinkProvider")
      .queryName(queryName)
      .outputMode(OutputMode.Append())
      .option(File.Path, s"$logsPath-parquet")
      .option(File.CheckpointLocation, s"$logsPath-parquet-checkpoint")
  }

  // Resolver

//  private def resolveCounts(df: DataFrame): DataFrame = {
//    val rowEncoder = Encoders.rowEncoder(actionSchema)
//    df.map(row => {
//      val (window, rqs) = (rowToWindow(row.getAs[Row](countSchemaIndexMap(WindowColName))), row.getAs[Long](countSchemaIndexMap(Stats.Count)))
//      val (nextSettingRaw, nextSetting, execute) = resolveCount(rqs)
//      buildActionRow(window, nextSettingRaw, nextSetting, execute)
//    })(rowEncoder)
//  }
//
//  private def resolveCount(rqs: Long): (WindowSetting, WindowSetting, Boolean) = {
//    val simple = new java.text.SimpleDateFormat("HH:mm:ss:SSS Z")
//    LoggerUtil.log.info(s"[WindowStreamResolver](${simple.format(new java.util.Date(System.currentTimeMillis()))}) Resolve count: RQS $rqs")
//
//    // estimate next window setting
//    val setting = wdf.getSetting
//    val nextSettingRaw = resolve(setting, rqs) // decide next window setting
//    val nextSetting = processSetting(setting, nextSettingRaw) // (smooth, ...)
//
//    // decide to take action
//    val execute = isActionRequired(setting, nextSetting)
//    if (execute) {
//      takeAction(nextSetting)
//    }
//
//    (nextSettingRaw, nextSetting, execute)
//  }
//
//  private def takeAction(setting: WindowSetting): Unit = {
//    wdf.setWindow(setting) // Update window settings
//    StreamManager.logState("(StreamResolverManager.takeAction)")
//    resolverCallback!(_(signature)) // Callback
//  }
//
//  private def processSetting(setting: WindowSetting, nextSettingRaw: WindowSetting): WindowSetting = {
//    // TODO: Process window (smooth, ...)
//    WindowSetting(setting.duration, setting.slideDuration, setting.watermarkDelay)
//  }
//
//  private def isActionRequired(setting: WindowSetting, nextSetting: WindowSetting): Boolean = {
//    // TODO: Decide to take action or not
//    true
//  }

  private def defaultResolver(setting: WindowSetting, rps: Long): WindowSetting = {
    assert(sampleSize isDefined)

    val duration = sampleSize.get / rps
    val slideDuration = duration * 0.1.toLong
    val watermarkDelay = duration * 0.4.toLong

    // Window estimation
    WindowSetting(Seconds(duration), Seconds(slideDuration), Seconds(watermarkDelay))
  }

  // Rows

//  private def buildCountRow(window: Window, rqs: Long): Row =
//    Row(Row(window.start, window.end), rqs)

  private def buildActionRow(window: Window, nextSettingRaw: WindowSetting, nextSetting: WindowSetting, executed: Boolean): Row =
    Row(Row(window.start, window.end),
      nextSettingRaw.duration.milliseconds, nextSettingRaw.slideDuration.milliseconds, nextSettingRaw.watermarkDelay.milliseconds,
      nextSetting.duration.milliseconds, nextSetting.slideDuration.milliseconds, nextSetting.watermarkDelay.milliseconds, executed)

  // Schemas

  private def getCountSchema: StructType = new StructType(Array(
    StructField(WindowColName, new StructType(Array(StructField(StartColName, TimestampType), StructField(EndColName, TimestampType)))),
    StructField(Stats.Count, LongType)))

  private def getActionSchema: StructType = new StructType(Array(
    StructField(WindowColName, new StructType(Array(StructField(StartColName, TimestampType), StructField(EndColName, TimestampType)))),
    StructField(RawDurationColName, LongType), StructField(RawSlideDurationColName, LongType), StructField(RawWatermarkDelayColName, LongType),
    StructField(DurationColName, LongType), StructField(SlideDurationName, LongType), StructField(WatermarkDelayColName, LongType),
    StructField(ExecutedColName, BooleanType)))

  // Overrides

  override def isActive: Boolean = sq.isDefined && sq.get.isActive

  override def start(callback: StreamResolverSignature => Unit): Unit = {
    StreamManager.logState("(WindowStreamResolver.start)")
    LoggerUtil.log.info(s"[WindowStreamResolver] Starting resolver over $timeColName with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

    if (isActive) {
      LoggerUtil.log.info(s"[WindowStreamResolver] WindowStreamResolver already active")
      return
    }

    resolverCallback = Some(callback)

    val wddf = buildWdf // windowed df
    val kvgd = buildKvgd(wddf) // key-value group
    val cdf = kvgd.count()
    val sw = buildSw(cdf) // stream writer

    val swsq = sw.start

    LoggerUtil.log.info(s"[WindowStreamResolver] Starting resolver: DONE")
    sq = Some(swsq)
  }

  override def stop(): Unit = sq!(_.stop)
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
