package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.{StatDouble, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class CountAggregator() extends StatSimpleAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  override def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue = {
    _value = StatDouble(add(_value.getDouble))
    _value
  }

  private def add(count: Double): Double = count + 1.0
}

object CountAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Count

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = false)
}

