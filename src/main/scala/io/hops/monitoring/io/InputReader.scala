package io.hops.monitoring.io

import io.hops.monitoring.MonitoredDataFrame
import io.hops.monitoring.util.RichOption._
import org.apache.spark.sql.{DataFrame, SparkSession}

class InputReader (val spark: Option[SparkSession] = None, df: Option[DataFrame] = None) {

  private var dfs: Array[DataFrame] = Array()

  df!_addDataFrame

  def _addDataFrame(df: DataFrame): InputReader = {
    dfs = dfs :+ df
    this
  }

  private def _reduceDFs: DataFrame = {
    dfs.reduce(_.union(_))
  }

  def monitor: MonitoredDataFrame = new MonitoredDataFrame(_reduceDFs)
}
