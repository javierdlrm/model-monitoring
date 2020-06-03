package io.hops.ml.monitoring.monitor

import io.hops.ml.monitoring.pipeline.SourcePipe
import org.apache.spark.sql.SparkSession

class Monitor(spark: SparkSession) {
  def source: SourcePipe = new SourcePipe(Some(spark))
}
