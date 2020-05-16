package io.hops.monitoring.outliers.detectors

import io.hops.monitoring.outliers.detectors.StatsOutlierDetector.StatsOutlierDetectorType
import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.utils.Constants.Outliers.DescriptiveStats

import scala.collection.immutable.HashMap

abstract class StatsOutlierDetector(var stats: Seq[String]) extends java.io.Serializable {
  def name: StatsOutlierDetectorType.Value

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}

object StatsOutlierDetector {

  object StatsOutlierDetectorType extends Enumeration {
    type Type = Value

    val DESCRIPTIVE: StatsOutlierDetectorType.Value = Value(DescriptiveStats)
  }

}
