package io.hops.monitoring.drift.detectors

import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.utils.Constants.Drift.KullbackLeibler
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap

class KullbackLeiblerDetector(threshold: Double, showAll: Boolean = false) extends StatsDriftDetector {

  override def name: String = KullbackLeibler

  override def stats: Seq[String] = Seq(Descriptive.Distr)

  override def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double] = {

    // get frequencies from the distributions
    val (observedFreq, baselineFreq) = StatsDriftDetector.getMatchedFrequencies(values(Descriptive.Distr).getMap, baseline(Descriptive.Distr).getMap)

    // compute KL distance
    val distance = KullbackLeiblerDetector.kullbackLeibler(observedFreq, baselineFreq)

    // return distance if showAll or drift deteted
    val drift = distance > threshold
    if (showAll || drift)
      Some(distance)
    else
      None
  }
}

object KullbackLeiblerDetector {
  def kullbackLeibler(p: Seq[Double], q: Seq[Double]): Double = {
    LoggerUtil.log.info(s"[KullbackLeiblerDetector] Detecting drift over: P [$p] and Q [$q]")

    p.zip(q).foldLeft(0.0)((sum, values) => {
      val (px, py) = values
      sum + (px * math.log(px / py)) // sum(p(x) ln (p(x) / p(y))
    })
  }
}