package io.hops.ml.monitoring.drift.detectors

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable

trait StatsDriftDetector extends java.io.Serializable {
  def name: String

  def stats: Seq[String]

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double]
}

object StatsDriftDetector {

  def getMatchedFrequencies(observed: mutable.HashMap[String, Double], baseline: mutable.HashMap[String, Double]): (Seq[Double], Seq[Double]) = {

    // get observed values count
    val observedCount = observed.values.sum
    val baselineCount = if (baseline.values.exists(_ > 1.0)) baseline.values.sum else 1.0 // In case it's prob distribution, use 1.0 as denominator

    // add missing values
    baseline.keys.foreach(key => {
      if (!observed.contains(key)) {
        observed(key) = 0.0
      }
    })

    // ensure bins match
    if (baseline.keySet.diff(observed.keySet) nonEmpty) {
      val msg = s"[StatsDriftDetector] getMatchedFrequencies: The distributions don't have same bins"
      LoggerUtil.log.error(msg)
      throw new Exception(msg) // TODO: Implement custom Exception wrappers
    }

    // sort frequencies
    val observedFreq: mutable.Buffer[Double] = mutable.Buffer()
    val baselineFreq: mutable.Buffer[Double] = mutable.Buffer()
    baseline.keys.toSeq.sorted.foreach(key => {
      observedFreq.append(observed(key) / observedCount) // add normalized observed frequency
      baselineFreq.append(baseline(key) / baselineCount) // add normalized baseline frequency
    })

    (observedFreq, baselineFreq)
  }
}

