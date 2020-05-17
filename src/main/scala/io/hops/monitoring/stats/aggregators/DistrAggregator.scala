package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.outliers.detectors.DescriptiveStatsDetector
import io.hops.monitoring.stats.definitions.Distr
import io.hops.monitoring.stats.definitions.Distr.BinningType
import io.hops.monitoring.stats.{StatMap, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.StatsUtil.round
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class DistrAggregator(distr: Distr, feature: String) extends StatSimpleAggregator {

  private var _bounds: Seq[String] = _

  private var _value: mutable.HashMap[String, Double] = _

  def value: StatValue = StatMap(_value)

  if (distr.bounds nonEmpty) {
    _bounds = distr.bounds(feature).sorted
    initializeValue()
  }

  override def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue = {

    // ensure bounds
    if (_bounds isEmpty) initialize(stats)

    // filter out upper outliers
    val binWidth = _bounds(1).toDouble - _bounds.head.toDouble
    if (value < (_bounds.last.toDouble + binWidth)) {
      // increment bin
      val index = _bounds.lastIndexWhere(_.toDouble <= value) // if value within bounds
      if (index >= 0) {
        val bound = _bounds(index)
        _value(bound) = _value(bound) + 1.0
      }
    }

    this.value
  }

  private def initialize(stats: HashMap[String, StatAggregator]): Unit = {
    // compute bounds
    val eps = 0.000001
    val numBins = getNumberOfBins(stats)
    val (max, min) = (stats(Descriptive.Max).value.getDouble, stats(Descriptive.Min).value.getDouble)
    val width = (max - min) / numBins + eps
    _bounds = (1 to numBins).map(i => min + width * i).sorted.map(_.toString)

    // initialize map
    initializeValue()
  }

  private def initializeValue(): Unit = {
    _value = mutable.HashMap(_bounds.map(_ -> 0.0): _*)
  }

  private def getNumberOfBins(stats: HashMap[String, StatAggregator]): Int = {
    val numBins = distr.binning match {
      case BinningType.STURGE => 1 + 3.322 * ((math.log(stats(Descriptive.Count).value.getDouble) / math.log(2.0)) + 1e-10) // 1 + 3.322 log2 N
    }
    round(numBins, decimals = 0, rounding = BigDecimal.RoundingMode.UP).toInt
  }
}

object DistrAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Distr

  override def dataType: DataType = MapType(StringType, DoubleType)

  override def structField: StructField = StructField(name, dataType, nullable = false)
}