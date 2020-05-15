package io.hops.monitoring.stats.aggregators

import org.apache.spark.sql.types.{DataType, StructField}

trait StatAgregatorCompanion {
  def name: String

  def dataType: DataType

  def structField: StructField
}