package io.hops.ml.monitoring.outliers.detectors

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait OutliersDetector extends java.io.Serializable {
  def name: String

  def detect(value: StatValue, baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}
