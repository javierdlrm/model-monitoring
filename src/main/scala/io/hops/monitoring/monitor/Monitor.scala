package io.hops.monitoring.monitor

import io.hops.monitoring.pipeline.SourcePipe
import org.apache.spark.sql.SparkSession

class Monitor(spark: SparkSession) {
  def source: SourcePipe = new SourcePipe(Some(spark))
}
