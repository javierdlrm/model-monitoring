package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.definitions.Stddev
import io.hops.ml.monitoring.stats.definitions.Stddev.StddevType
import io.hops.ml.monitoring.stats.{StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types.{DataType, DoubleType, StructField}

import scala.collection.immutable.HashMap

case class StddevAggregator(stddev: Stddev) extends StatCompoundAggregator {

  private var _value: StatValue = StatDouble(0.0) // default
  def value: StatValue = _value

  override def compute(stats: HashMap[String, StatAggregator]): StatValue = {

    val pow2sum = stats(Descriptive.Pow2Sum).value.getDouble
    val count = stats(Descriptive.Count).value.getDouble
    val avg = stats(Descriptive.Avg).value.getDouble

    _value = StatDouble(
      round(
        stddev(stddev.`type`, pow2sum, count, avg)
      ))
    _value
  }

  def stddev(type_ : StddevType.Value, pow2Sum: Double, count: Double, avg: Double): Double = {
    val n = if (type_ == StddevType.SAMPLE) count - 1.0 else count
    math.sqrt((pow2Sum - (count * math.pow(avg, 2.0))) / n)
  }
}

object StddevAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Stddev

  override def dataType: DataType = DoubleType

  override def structField: StructField = StructField(name, dataType, nullable = true)
}