package io.hops.ml.monitoring.drift.detectors

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Drift.Wasserstein
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap

class WassersteinDetector(threshold: Double, showAll: Boolean = false) extends StatsDriftDetector {

  override def name: String = Wasserstein

  override def stats: Seq[String] = Seq(Descriptive.Distr)

  override def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double] = {

    // get frequencies from the distributions
    val (observedFreq, baselineFreq) = StatsDriftDetector.getMatchedFrequencies(values(Descriptive.Distr).getMap, baseline(Descriptive.Distr).getMap)

    // compute Wasserstein distance (also Earth Mover Distance)
    val distance = wasserstein(observedFreq, baselineFreq)

    // return distance if showAll or drift deteted
    val drift = distance > threshold
    if (showAll || drift)
      Some(distance)
    else
      None
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