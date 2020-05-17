package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.definitions.Cov
import io.hops.monitoring.stats.definitions.Cov.CovType
import io.hops.monitoring.stats.{StatMap, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class CovAggregator(cov: Cov, feature: String) extends StatMultipleAggregator {

  private var _value: mutable.HashMap[String, Double] = _

  def value: StatValue = StatMap(_value)

  private var _residualsSums: mutable.HashMap[String, Double] = _

  private var _runs: Int = 0

  def runs: Int = _runs

  override def compute(values: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): StatValue = {

    // check residuals initialization
    if (_runs == 0)
      _residualsSums = mutable.HashMap(values.keys.map(_ -> 0.0).toSeq: _*)

    // Compute cov
    _value = cov(cov.type_, values, stats)

    // Increment runs
    _runs += 1

    this.value
  }

  private def cov(type_ : CovType.Value, pairs: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): mutable.HashMap[String, Double] = {
    val n = if (type_ == CovType.SAMPLE) _runs else _runs + 1 // Sample -> N - 1, Population -> N
    val featureStats = stats(feature)
    val xResidual = pairs(feature) - featureStats(Descriptive.Avg).value.getDouble // x - E[X]

    val covPairs = pairs
      .filter(_._1 != feature) // filter out Cov(X,X) -> Var(X) -> Stddev(X)^2
      .map(pair => {

        // check if cov already computed by co-feature
        val (cofCov, cofRS) = checkCofeatureCov(pair._1, stats)

        val cov = if (!cofCov.isNaN) { // if co-feature cov already computed
          _residualsSums(pair._1) = cofRS
          cofCov
        }
        else { // compute cov
          val yResidual = pair._2 - stats(pair._1)(Descriptive.Avg).value.getDouble // y - E[Y]
          val residualsProd = _residualsSums(pair._1) + (xResidual * yResidual) // (x - E[X])(y - E[Y])
          _residualsSums(pair._1) = _residualsSums(pair._1) + residualsProd // sum[residuals prod]
          residualsProd / n
        }

        // feature -> cov
        pair._1 -> round(cov, rounding = BigDecimal.RoundingMode.HALF_DOWN)
      })

    mutable.HashMap(covPairs.toSeq: _*)
  }

  private def checkCofeatureCov(partner: String, stats: HashMap[String, HashMap[String, StatAggregator]]): (Double, Double) = {
    val agg = stats(partner)(Descriptive.Cov).asInstanceOf[CovAggregator]
    if (agg._runs == _runs + 1) // next agg
    (agg._value(feature), agg._residualsSums(feature))
    else
    (Double.NaN, Double.NaN)
  }
}

object CovAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Cov

  override def dataType: DataType = MapType(StringType, DoubleType)

  override def structField: StructField = StructField(name, dataType, nullable = false)
}