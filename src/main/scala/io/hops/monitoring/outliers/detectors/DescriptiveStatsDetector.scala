package io.hops.monitoring.outliers.detectors

import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.utils.Constants.Outliers.DescriptiveStats
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap

class DescriptiveStatsDetector(var statNames: Seq[String]) extends StatsOutlierDetector(statNames) {

  override def name: String = DescriptiveStats

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue] = {

    LoggerUtil.log.info(s"[DescriptiveStatsDetector] Detect in stats [${stats.mkString(", ")}], values: [$values]")

    // check outliers per each defined stat
    val outliers = stats.flatMap(stat => {
      val observed = values(stat)
      val reference = baseline(stat)
      val isOutlier = DescriptiveStatsDetector.isOutlier(stat, observed.getDouble, reference.getDouble, baseline)

      LoggerUtil.log.info(s"[DescriptiveStatsDetector] Outlier [$isOutlier], Stat [$stat], Observed [$observed], Reference [$reference]")

      if (isOutlier) Some(stat -> observed)
      else None
    })

    HashMap(outliers: _*)
  }
}

object DescriptiveStatsDetector {
  def isOutlier(stat: String, observed: Double, reference: Double, baseline: HashMap[String, StatValue]): Boolean = {
    stat match {
      case Descriptive.Max => observed > reference
      case Descriptive.Min => observed < reference
      case Descriptive.Mean => beyondStddevOutlier(observed, reference, baseline(Descriptive.Stddev).getDouble)
      case Descriptive.Avg => beyondStddevOutlier(observed, reference, baseline(Descriptive.Stddev).getDouble)
      case Descriptive.Stddev => observed > reference * 2
    }
  }

  private def beyondStddevOutlier(observed: Double, reference: Double, stddev: Double): Boolean = {
    val upperBound = reference + stddev * 2
    val lowerBound = reference - stddev * 2

    observed < lowerBound || observed > upperBound
  }
}