package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.{StatDouble, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class MinAggregator() extends StatSimpleAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  private var _firstComputation: Boolean = true

  override def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue = {
    _value = StatDouble(
      if (_firstComputation) {
        _firstComputation = false
        value
      } else min(value, _value.getDouble)
    )
    _value
  }

  private def min(value: Double, min: Double): Double = math.min(value, min)
}

object MinAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Min

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = false)
}
