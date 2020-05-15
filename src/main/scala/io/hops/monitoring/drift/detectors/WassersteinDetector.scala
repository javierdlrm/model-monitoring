package io.hops.monitoring.drift.detectors

import io.hops.monitoring.drift.detectors.StatsDriftDetector.StatsDriftDetectorType
import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable

class WassersteinDetector(threshold: Double, showAll: Boolean = false) extends StatsDriftDetector {

  override def name: StatsDriftDetectorType.Value = StatsDriftDetectorType.WASSERSTEIN

  override def stats: Seq[String] = Seq(Descriptive.Distr)

  override def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double] = {

    // get frequencies from the distributions
    val (observedFreq, baselineFreq) = getFrequencies(values, baseline)

    // compute Wasserstein distance (also Earth Mover Distance)
    val distance = wasserstein(observedFreq, baselineFreq)

    // return distance if showAll or drift deteted
    val drift = distance > threshold
    if (showAll || drift)
      Some(distance)
    else
      None
  }

  def getFrequencies(observed: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): (Seq[Double], Seq[Double]) = {

    // get distributions
    val observedDistr = observed(Descriptive.Distr).getMap
    val baselineDistr = baseline(Descriptive.Distr).getMap

    // get observed values count
    val observedCount = observedDistr.values.sum
    val baselineCount = if (baselineDistr.values.exists(_ > 1.0)) baselineDistr.values.sum else 1.0 // In case it's prob distribution, use 1.0 as denominator

    // add missing values
    baselineDistr.keys.foreach(key => {
      if (!observedDistr.contains(key)) {
        observedDistr(key) = 0.0
      }
    })

    // ensure bins match
    if (baselineDistr.keySet.diff(observedDistr.keySet) nonEmpty) {
      val msg = s"[WassesteinDetector] The distributions don't have same bins"
      LoggerUtil.log.error(msg)
      throw new Exception(msg) // TODO: Implement custom Exception wrappers
    }

    // sort frequencies
    val observedFreq: mutable.Buffer[Double] = mutable.Buffer()
    val baselineFreq: mutable.Buffer[Double] = mutable.Buffer()
    baselineDistr.keys.toSeq.sorted.foreach(key => {
      observedFreq.append(observedDistr(key) / observedCount) // add normalized observed frequency
      baselineFreq.append(baselineDistr(key) / baselineCount) // add normalized baseline frequency
    })

    (observedFreq, baselineFreq)
  }

  def wasserstein(a: Seq[Double], b: Seq[Double]): Double = {
    LoggerUtil.log.info(s"[WassersteinDetector] Detecting drift over: A [$a] and B [$b]")

    var lastDistance: Double = 0.0
    var totalDistance: Double = 0.0
    for (i <- a.indices) {
      val currentDistance = (a(i) + lastDistance) - b(i)
      totalDistance += math.abs(currentDistance)
      lastDistance = currentDistance
    }
    totalDistance
  }
}