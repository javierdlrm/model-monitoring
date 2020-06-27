package io.hops.ml.monitoring.monitor

import io.hops.ml.monitoring.drift.DriftPipeJoint
import io.hops.ml.monitoring.outliers.OutliersPipeJoint
import io.hops.ml.monitoring.utils.DataFrameUtil.explodeColumn
import io.hops.ml.monitoring.utils.LoggerUtil
import io.hops.ml.monitoring.window.WindowPipeJoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class MonitorPipe(source: DataFrame) extends WindowPipeJoint with OutliersPipeJoint with DriftPipeJoint {

  LoggerUtil.log.info(s"[MonitorPipe] Created over dataframe with schema ${source.schema.json}")

  // Variables

  private var _df: DataFrame = source

  // Accessors / Mutators

  def schema: StructType = _df.schema

  // Instance values in one Column

  def withSchema(colName: String, schema: StructType): MonitorPipe = {
    LoggerUtil.log.info(s"[MonitorPipe] Applying schema to $colName")

    _df = source.explodeColumn(colName, schema)
    this
  }

  // Instance values in multiple Column*

  def withSchema(schema: StructType): MonitorPipe = {
    // TODO: Apply specific schema over df
    this
  }

  // Window trait

  override def df: DataFrame = _df
}
