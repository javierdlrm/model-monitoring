package io.hops.ml.monitoring.outliers.detectors

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Outliers.DescriptiveStats
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

import scala.collection.immutable.HashMap

case class DescriptiveStatsDetector(var stats: Seq[String]) extends StatsOutliersDetector(stats) with OutliersDetector {

  override def name: String = DescriptiveStats

  override def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue] = {
    val outliers = stats.flatMap(stat => checkOutlier(values(stat), stat, baseline))
    HashMap(outliers: _*)
  }

  override def detect(value: StatValue, baseline: HashMap[String, StatValue]): HashMap[String, StatValue] = {
    val outliers = stats.flatMap(stat => checkOutlier(value, stat, baseline))
    HashMap(outliers: _*)
  }

  private def checkOutlier(observed: StatValue, stat: String, baseline: HashMap[String, StatValue]): Option[(String, StatValue)] = {
    val reference = baseline(stat)
    val isOutlier = DescriptiveStatsDetector.isOutlier(stat, observed.getDouble, reference.getDouble, baseline)
    if (isOutlier) Some(stat -> observed)
    else None
  }
}

object DescriptiveStatsDetector {

  val IntervalSigmas = 3 // certainty interval sigmas. Empirical rule: 68% (1-sigma), 95% (2-sigma), 99.7% (3-sigma)

  def isOutlier(stat: String, observed: Double, reference: Double, baseline: HashMap[String, StatValue]): Boolean = {
    stat match {
      case Descriptive.Max => observed > reference
      case Descriptive.Min => observed < reference
      case Descriptive.Mean => beyondStddevOutlier(observed, reference, baseline(Descriptive.Stddev).getDouble)
      case Descriptive.Avg => beyondStddevOutlier(observed, reference, baseline(Descriptive.Stddev).getDouble)
      case Descriptive.Stddev => observed > reference * IntervalSigmas
    }
  }

  private def beyondStddevOutlier(observed: Double, reference: Double, stddev: Double): Boolean = {
    val upperBound = reference + stddev * IntervalSigmas
    val lowerBound = reference - stddev * IntervalSigmas

    observed < lowerBound || observed > upperBound
  }
}