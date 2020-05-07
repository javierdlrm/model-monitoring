package io.hops.monitoring.io.dataframe

import io.hops.monitoring.pipeline.Pipeline
import org.apache.spark.sql.DataFrame

object DataFrameSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {
    def df: DataFrame = pipeline.df
  }

}
