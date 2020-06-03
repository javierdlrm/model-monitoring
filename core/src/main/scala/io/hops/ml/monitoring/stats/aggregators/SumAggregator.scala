package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.{StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class SumAggregator() extends StatSimpleAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  override def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue = {
    _value = StatDouble(sum(value, _value.getDouble))
    _value
  }

  private def sum(value: Double, sum: Double): Double = sum + value
}

object SumAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Sum

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = false)
}
