package io.hops.monitoring

import io.InputReader
import org.apache.spark.sql.SparkSession

class ModelMonitor(spark: SparkSession) {
  def input: InputReader = new InputReader(Some(spark))
}
