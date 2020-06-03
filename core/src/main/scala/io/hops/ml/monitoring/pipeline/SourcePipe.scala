package io.hops.ml.monitoring.pipeline

import io.hops.ml.monitoring.monitor.MonitorPipe
import io.hops.ml.monitoring.utils.RichOption._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SourcePipe(val spark: Option[SparkSession] = None, df: Option[DataFrame] = None) {

  private var dfs: Array[DataFrame] = Array()

  df ! _addDataFrame

  def _addDataFrame(df: DataFrame): SourcePipe = {
    dfs = dfs :+ df
    this
  }

  private def _reduceDFs: DataFrame = {
    dfs.reduce(_.union(_))
  }

  def monitor: MonitorPipe = new MonitorPipe(_reduceDFs)
}
