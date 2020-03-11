package io.hops.monitoring.io.dataframe

import io.hops.monitoring.MonitoredDataFrame
import io.hops.monitoring.io.InputReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameReader {

  implicit class ExtendedDataFrame(val df: DataFrame) extends AnyVal {

    def monitor: MonitoredDataFrame = new MonitoredDataFrame(df)

    def merge(spark: SparkSession): InputReader = new InputReader(spark=Some(spark), df=Some(df))
  }

  implicit class ExtendedInputReader(val ir: InputReader) extends AnyVal {

    def df(df: DataFrame): InputReader = {
      ir._addDataFrame(df)
    }
  }
}