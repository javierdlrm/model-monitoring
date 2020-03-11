package io.hops.monitoring

import util.DataFrameUtil.explodeColumn
import util.LoggerUtil
import window.{WindowedDataFrame, WindowSetting}
import streams.resolver.ResolvableDataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class MonitoredDataFrame(private var df: DataFrame) extends ResolvableDataFrame with java.io.Serializable {

  LoggerUtil.log.info(s"[MonitoredDataFrame] Created over dataframe with schema ${df.schema.json}")

  // Variables

  private var _schema: StructType = df.schema

  def schema: StructType = _schema

  // Instances in one Column

  def withSchema(colName: String, schema: StructType): MonitoredDataFrame = {
    LoggerUtil.log.info(s"[MonitoredDataFrame] Applying schema to $colName")

    df = df.explodeColumn(colName, schema)
    this
  }

  // Instances in multiple Column*

  def withSchema(schema: StructType): MonitoredDataFrame = {
    // TODO: Apply specific schema over df
    this._schema = schema
    this
  }

  // Window

  // manually configured
  def window(timeColName: String, setting: WindowSetting): WindowedDataFrame =
    WindowedDataFrame(df, timeColName)(setting)

  // with sample size
  def window(timeColName: String, sampleSize: Int): WindowedDataFrame = window(timeColName, sampleSize, "")
  def window(timeColName: String, sampleSize: Int, logsPath: String): WindowedDataFrame =
    WindowedDataFrame(df, timeColName)(sampleSize, logsPath)

  // with custom resolver
  def window(timeColName: String, windowResolver: (WindowSetting, Long) => WindowSetting): WindowedDataFrame = window(timeColName, windowResolver, "")
  def window(timeColName: String, windowResolver: (WindowSetting, Long) => WindowSetting, logsPath: String): WindowedDataFrame =
    WindowedDataFrame(df, timeColName)(windowResolver, logsPath)

  // auto configurable
  def window(timeColName: String): WindowedDataFrame = window(timeColName, "")
  def window(timeColName: String, logsPath: String): WindowedDataFrame =
    WindowedDataFrame(df, timeColName)(logsPath)
}
