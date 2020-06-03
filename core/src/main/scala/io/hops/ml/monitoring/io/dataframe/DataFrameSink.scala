package io.hops.ml.monitoring.io.dataframe

import io.hops.ml.monitoring.pipeline.Pipeline
import org.apache.spark.sql.DataFrame

object DataFrameSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {
    def df: DataFrame = pipeline.df
  }

}
