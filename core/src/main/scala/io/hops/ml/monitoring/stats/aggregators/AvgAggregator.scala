package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.{StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class AvgAggregator() extends StatCompoundAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  override def value: StatValue = _value

  override def compute(stats: HashMap[String, StatAggregator]): StatValue = {
    val sum = stats(Descriptive.Sum).value.getDouble
    val count = stats(Descriptive.Count).value.getDouble
    _value = StatDouble(
      round(avg(sum, count))
    )
    _value
  }

  private def avg(sum: Double, count: Double): Double = sum / count
}

object AvgAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Avg

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = true)
}
