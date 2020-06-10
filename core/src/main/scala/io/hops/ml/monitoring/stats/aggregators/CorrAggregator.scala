package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.stats.definitions.Corr
import io.hops.ml.monitoring.stats.definitions.Corr
import io.hops.ml.monitoring.stats.definitions.Corr.CorrType
import io.hops.ml.monitoring.stats.{StatMap, StatValue}
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class CorrAggregator(corr: Corr, feature: String) extends StatMultipleAggregator {

  private var _value: mutable.HashMap[String, Double] = _

  def value: StatValue = StatMap(_value)

  private var _sums: mutable.HashMap[String, Double] = _

  private var _runs: Int = 0

  def runs: Int = _runs

  override def compute(values: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): StatValue = {

    // check residuals initialization
    if (_runs == 0)
      _sums = mutable.HashMap(values.keys.map(_ -> 0.0).toSeq: _*)

    // Compute corr
    _value = corr(corr.`type`, values, stats)

    // Increment runs
    _runs += 1

    this.value
  }

  private def corr(type_ : CorrType.Value, pairs: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): mutable.HashMap[String, Double] = {
    val n = if (type_ == CorrType.SAMPLE) _runs else _runs + 1 // Sample -> N - 1, Population -> N
    val featureStats = stats(feature)
    val x = pairs(feature)

    val corrPairs = pairs
      .filter(_._1 != feature) // filter out Corr(X,X)
      .map(pair => {

        val cofeatureStats = stats(pair._1)

        // check if corr already computed by co-feature
        val (cofCorr, cofSum) = checkCofeatureCorr(pair._1, cofeatureStats)

        val corr = if (!cofCorr.isNaN) { // if co-feature corr already computed
          _sums(pair._1) = cofSum
          cofCorr
        }
        else { // compute corr

          val y = pair._2
          val sum = _sums(pair._1) + (x * y) // sum(xy) agg
          _sums(pair._1) = sum
          val count = featureStats(Descriptive.Count).value.getDouble

          if (_runs + 1 == count) { // if last run

            val xAvg = featureStats(Descriptive.Avg).value.getDouble
            val yAvg = cofeatureStats(Descriptive.Avg).value.getDouble
            val xStddev = featureStats(Descriptive.Stddev).value.getDouble
            val yStddev = cofeatureStats(Descriptive.Stddev).value.getDouble

            val numerator = sum - (count * xAvg * yAvg) // sum(xy) - n * mean(x) * mean(y)
            val denominator = n * xStddev * yStddev // [n | n - 1] * stddev(x) * stddev(y)

            numerator / denominator
          } else {
            0.0
          }
        }

        // feature -> corr
        pair._1 -> round(corr, rounding = BigDecimal.RoundingMode.HALF_DOWN)
      })

    mutable.HashMap(corrPairs.toSeq: _*)
  }

  private def checkCofeatureCorr(partner: String, stats: HashMap[String, StatAggregator]): (Double, Double) = {
    val agg = stats(Descriptive.Corr).asInstanceOf[CorrAggregator]
    if (agg._runs > _runs + 1)
      (agg._value(feature), agg._sums(feature))
    else
      (Double.NaN, Double.NaN)
  }
}

object CorrAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Corr

  override def dataType: DataType = MapType(StringType, DoubleType)

  override def structField: StructField = StructField(name, dataType, nullable = false)
}