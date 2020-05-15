package io.hops.monitoring.outliers.detectors

import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.stats.definitions.StatDefinition

import scala.collection.immutable.HashMap

abstract case class WindowOutlierDetector(var stats: Seq[StatDefinition]) extends java.io.Serializable {
  def name: String
  def detect(values: Seq[StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}

