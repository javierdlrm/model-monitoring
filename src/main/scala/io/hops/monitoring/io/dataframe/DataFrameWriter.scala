package io.hops.monitoring.io.dataframe

import io.hops.monitoring.streams.writer.StreamWriter
import org.apache.spark.sql.DataFrame

object DataFrameWriter {
  implicit class ExtendedOutputDataFrame(val ow: StreamWriter) extends AnyVal {
    def df: DataFrame = ow.df
  }
}
