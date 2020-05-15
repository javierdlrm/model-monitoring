package io.hops.monitoring.drift.detectors

import io.hops.monitoring.drift.detectors.StatsDriftDetector.StatsDriftDetectorType
import io.hops.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait StatsDriftDetector extends java.io.Serializable {
  def name: StatsDriftDetectorType.Value

  def stats: Seq[String]

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double]
}

object StatsDriftDetector {

  object StatsDriftDetectorType extends Enumeration {
    type Type = Value

    val WASSERSTEIN: StatsDriftDetectorType.Value = Value
  }
}

