package io.hops.monitoring.window

import io.hops.monitoring.stats.StatsWindowedDataFrame
import io.hops.monitoring.util.Constants.Window._
import io.hops.monitoring.util.DataFrameUtil.Encoders
import io.hops.monitoring.util.{LoggerUtil, WindowUtil}
import io.hops.monitoring.streams.resolver.ResolvableDataFrame
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}
import java.sql.Timestamp

class WindowedDataFrame(val df: DataFrame, timeColName: String)
  extends ResolvableDataFrame with java.io.Serializable  {

  private var setting: WindowSetting = WindowUtil.defaultSetting

  LoggerUtil.log.info(s"[WindowedDataFrame] Created over column $timeColName with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

  // Access methods

  def getSetting: WindowSetting = setting

  def setWindow(setting: WindowSetting): Unit = {
    LoggerUtil.log.info(s"[WindowedDataFrame] Set window with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")
    this.setting = setting
  }

  // Window

  private def buildSdf(colNames: Seq[String]): DataFrame =
    df.select(col(timeColName) +: colNames.map(colName => col(colName)):_*)

  private def buildWdf(sdf: DataFrame): DataFrame = {
    LoggerUtil.log.info(s"[WindowedDataFrame] Building WDF with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

    val windowCol = window(col(timeColName), WindowUtil.durationToString(setting.duration), WindowUtil.durationToString(setting.slideDuration))
    sdf.withWatermark(timeColName, WindowUtil.durationToString(setting.watermarkDelay))
       .withColumn(WindowColName, windowCol)
  }
  private def buildKvgd(wdf: DataFrame): KeyValueGroupedDataset[Window, Row] =
    wdf.groupByKey[Window](selectWindow)(Encoders.windowEncoder)

  private def selectWindow: Row => Window = t => {
    val windowRow = t.getAs[Row](WindowColName)
    Window(windowRow.getAs[Timestamp](0), windowRow.getAs[Timestamp](1))
  }

  // Stats

  def stats(colNames: Seq[String], stats: Seq[String]): StatsWindowedDataFrame = {
    val sdf = buildSdf(colNames)
    val wdf = buildWdf(sdf)
    val kvgd = buildKvgd(wdf)
    new StatsWindowedDataFrame(kvgd, wdf.schema, colNames, stats, getSignatures)
  }
}

object WindowedDataFrame {
  class Apply(df: DataFrame, timeColName: String) {
    // Manual setting
    def apply(setting: WindowSetting): WindowedDataFrame = {
      val wdf = new WindowedDataFrame(df, timeColName)
      wdf.setWindow(setting)
      wdf
    }
    // Sample size
    def apply(sampleSize: Int, logsPath: String): WindowedDataFrame = {
      val wdf = new WindowedDataFrame(df, timeColName)
      wdf.addResolver(WindowStreamResolver(wdf, timeColName)(sampleSize, logsPath)) // Add resolver
      wdf
    }
    // Custom resolver
    def apply(resolver: (WindowSetting, Long) => WindowSetting, logsPath: String): WindowedDataFrame = {
      val wdf = new WindowedDataFrame(df, timeColName)
      wdf.addResolver(WindowStreamResolver(wdf, timeColName)(resolver, logsPath)) // Add resolver
      wdf
    }
    // Auto configurable
    def apply(logsPath: String): WindowedDataFrame = {
      val wdf = new WindowedDataFrame(df, timeColName)
      wdf.addResolver(WindowStreamResolver(wdf, timeColName)(logsPath)) // Add resolver
      wdf
    }
  }
//  implicit def applyToCall(a: Apply): WindowStreamResolver = a()
  def apply(df: DataFrame, timeColName: String): Apply = new Apply(df, timeColName)
}