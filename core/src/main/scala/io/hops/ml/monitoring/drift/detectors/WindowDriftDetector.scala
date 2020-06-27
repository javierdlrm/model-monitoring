package io.hops.ml.monitoring.drift.detectors

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait WindowDriftDetector extends java.io.Serializable {
  def name: String

  def detect(values: Seq[StatValue], baseline: HashMap[String, StatValue]): Option[StatValue]
}

