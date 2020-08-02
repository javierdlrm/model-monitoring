package io.hops.ml.monitoring.outliers.detectors

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait WindowOutliersDetector extends java.io.Serializable {
  def name: String

  def detect(values: Seq[StatValue], baseline: HashMap[String, StatValue]): HashMap[String, StatValue]
}

