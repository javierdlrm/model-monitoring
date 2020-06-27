package io.hops.ml.monitoring.drift.detectors

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait DriftDetector extends java.io.Serializable {
  def name: String

  def detect(value: StatValue, baseline: HashMap[String, StatValue]): Option[(StatValue, StatValue)]
}
