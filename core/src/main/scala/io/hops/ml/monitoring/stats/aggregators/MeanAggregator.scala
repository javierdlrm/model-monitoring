package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.{StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class MeanAggregator() extends StatCompoundAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  override def compute(stats: HashMap[String, StatAggregator]): StatValue = {
    val max = stats(Descriptive.Max).value.getDouble
    val min = stats(Descriptive.Min).value.getDouble
    val count = stats(Descriptive.Count).value.getDouble
    _value = StatDouble(
      round(mean(max, min, count))
    )
    _value
  }

  private def mean(max: Double, min: Double, count: Double): Double = (max - min) / count
}

object MeanAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Mean

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = true)
}
