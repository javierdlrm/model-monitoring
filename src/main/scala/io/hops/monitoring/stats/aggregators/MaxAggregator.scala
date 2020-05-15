package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.{StatDouble, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class MaxAggregator() extends StatSimpleAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  private var _firstComputation: Boolean = true

  override def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue = {
    _value = StatDouble(
      if (_firstComputation) {
        _firstComputation = false
        value
      } else max(value, _value.getDouble)
    )
    _value
  }

  private def max(value: Double, max: Double): Double = math.max(value, max)
}

object MaxAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Max

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = false)
}

