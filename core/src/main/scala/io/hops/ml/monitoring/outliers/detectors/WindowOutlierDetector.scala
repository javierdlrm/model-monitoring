package io.hops.ml.monitoring.outliers.detectors

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.stats.definitions.StatDefinition

import scala.collection.immutable.HashMap

abstract case class WindowOutlierDetector(var stats: Seq[StatDefinition]) extends java.io.Serializable {
  def name: String
  def detect(values: Seq[StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}

