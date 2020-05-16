package io.hops.monitoring.drift.detectors

import io.hops.monitoring.drift.detectors.StatsDriftDetector.StatsDriftDetectorType
import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.utils.Constants.Drift.{JensenShannon, KullbackLeibler, Wasserstein}
import io.hops.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable

trait StatsDriftDetector extends java.io.Serializable {
  def name: StatsDriftDetectorType.Value

  def stats: Seq[String]

  def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double]
}

object StatsDriftDetector {

  object StatsDriftDetectorType extends Enumeration {
    type Type = Value

    val WASSERSTEIN: StatsDriftDetectorType.Value = Value(Wasserstein)
    val KULLBACKLEIBLER: StatsDriftDetectorType.Value = Value(KullbackLeibler)
    val JENSENSHANNON: StatsDriftDetectorType.Value = Value(JensenShannon)
  }

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

