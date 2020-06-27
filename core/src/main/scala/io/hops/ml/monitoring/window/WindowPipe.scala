package io.hops.ml.monitoring.window

import java.sql.Timestamp

import io.hops.ml.monitoring.drift.WindowDriftPipeJoint
import io.hops.ml.monitoring.outliers.WindowOutliersPipeJoint
import io.hops.ml.monitoring.stats.StatsPipeJoint
import io.hops.ml.monitoring.utils.Constants.Window._
import io.hops.ml.monitoring.utils.DataFrameUtil.Encoders
import io.hops.ml.monitoring.utils.{LoggerUtil, WindowUtil}
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

class WindowPipe(source: DataFrame, timestampCol: String, val setting: WindowSetting) extends StatsPipeJoint with WindowOutliersPipeJoint with WindowDriftPipeJoint {

  LoggerUtil.log.info(s"[WindowPipe] Created over column $timestampCol with duration ${setting.duration}, slide ${setting.slideDuration} and watermark ${setting.watermarkDelay}")

  // Window

  private def selectCols(cols: Seq[String]): DataFrame =
    source.select(col(timestampCol) +: cols.map(colName => col(colName)): _*)

  private def applyWindow(df: DataFrame): DataFrame = {
    val windowCol = window(col(timestampCol), WindowUtil.durationToString(setting.duration), WindowUtil.durationToString(setting.slideDuration))
    df.withWatermark(timestampCol, WindowUtil.durationToString(setting.watermarkDelay))
      .withColumn(WindowColName, windowCol)
  }

  private def applyGroupByKey(df: DataFrame): KeyValueGroupedDataset[Window, Row] =
    df.groupByKey[Window](selectWindow)(Encoders.windowEncoder)

  private def selectWindow: Row => Window = row => rowToWindow(row.getAs[Row](WindowColName))

  private def rowToWindow(row: Row): Window = Window(row.getAs[Timestamp](0), row.getAs[Timestamp](1))

  // Joints

  override def kvgd(cols: Seq[String]): (KeyValueGroupedDataset[Window, Row], StructType) = {
    val df = applyWindow(selectCols(cols))
    (applyGroupByKey(df), df.schema)
  }
}