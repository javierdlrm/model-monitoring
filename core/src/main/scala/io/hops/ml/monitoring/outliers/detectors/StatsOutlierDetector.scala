package io.hops.ml.monitoring.outliers.detectors

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

abstract class StatsOutlierDetector(var stats: Seq[String]) extends java.io.Serializable {
  def name: String

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}
