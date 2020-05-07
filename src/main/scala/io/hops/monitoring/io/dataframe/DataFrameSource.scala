package io.hops.monitoring.io.dataframe

import io.hops.monitoring.monitor.MonitorPipe
import io.hops.monitoring.pipeline.SourcePipe
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameSource {

  implicit class ExtendedDataFrame(val df: DataFrame) extends AnyVal {

    def monitor: MonitorPipe = new MonitorPipe(df)

    def merge(spark: SparkSession): SourcePipe = new SourcePipe(spark = Some(spark), df = Some(df))
  }

  implicit class ExtendedSourcePipe(val sp: SourcePipe) extends AnyVal {

    def df(df: DataFrame): SourcePipe = sp._addDataFrame(df)
  }

}