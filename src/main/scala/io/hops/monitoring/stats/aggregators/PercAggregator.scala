package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.definitions.Perc
import io.hops.monitoring.stats.{StatMap, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.LoggerUtil
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class PercAggregator(perc: Perc) extends StatCompoundAggregator {

  // Assert valid percentiles
  assert(perc.percentiles.forall(p => 0.0 <= p && p <= 100.0))

  private var _value: mutable.HashMap[String, Double] = _

  def value: StatValue = StatMap(_value)

  override def compute(stats: HashMap[String, StatAggregator]): StatValue = {

    val distr = stats(Descriptive.Distr).value.getMap

    LoggerUtil.log.info(s"[PercAggregator] Distr $distr")
    _value = percentiles(distr)

    this.value
  }

  def percentiles(distr: mutable.HashMap[String, Double]): mutable.HashMap[String, Double] = {

    val filteredDistr = distr.filter(p => p._2 > 0)
    if (filteredDistr isEmpty)
      return mutable.HashMap[String, Double]()

    val count = filteredDistr.values.sum
    val sortedKeys = filteredDistr.keys.toSeq.sorted
    val numBins = sortedKeys.size
    val sortedPerc = (if (perc.iqr) iqrCompatible(perc.percentiles) else perc.percentiles).sorted
    val percBuffer: mutable.Buffer[(String, Double)] = mutable.Buffer()

    var percCount = 0.0
    var binIdx = 0
    var freqCount = filteredDistr(sortedKeys.head) // first frequency

    LoggerUtil.log.info(s"[PercAggregator] SortedPerc: [$sortedPerc], SortedKeys: [$sortedKeys]")
    LoggerUtil.log.info(s"[PercAggregator] NumBins: $numBins, Count: $count, Count2: ${filteredDistr.values.sum}")

    for (perc <- sortedPerc) {
      if (perc < 100) {
        percCount = perc / 100 * count
        while (freqCount <= percCount && binIdx < numBins) {
          LoggerUtil.log.info(s"[PercAggregator] FreqCount: $freqCount, PercCount: $percCount, binIdx: $binIdx, numBins: $numBins")
          binIdx += 1
          freqCount += filteredDistr(sortedKeys(binIdx))
        }
        percBuffer.append(perc.toString -> sortedKeys(binIdx).toDouble) // add {perc}th percentile
      } else {
        percBuffer.append(perc.toString -> sortedKeys.last.toDouble)  // add 100th percentile
      }
    }

    val percentiles = mutable.HashMap[String, Double](percBuffer: _*)
    if (perc.iqr)
      percentiles("iqr") = percentiles("75") - percentiles("25")
    percentiles
  }

  def iqrCompatible(percentiles: Seq[Int]): Seq[Int] = {
    val iqrPerc = Seq(25, 75)
    if (iqrPerc.forall(percentiles.contains))
      percentiles
    else {
      var iqrPercentiles = percentiles
      iqrPerc.foreach(p => {
        if (!iqrPercentiles.contains(p))
          iqrPercentiles = iqrPercentiles :+ p
      })
      iqrPercentiles
    }
  }
}

object PercAggregator extends StatAgregatorCompanion {
  override def name: String = Descriptive.Perc

  override def dataType: DataType = MapType(StringType, DoubleType)

  override def structField: StructField = StructField(name, dataType, nullable = false)
}