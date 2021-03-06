package io.hops.ml.monitoring.drift.detectors

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Drift.JensenShannon
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap

case class JensenShannonDetector(threshold: Double, showAll: Boolean = false) extends StatsDriftDetector {

  override def name: String = JensenShannon

  override def stats: Seq[String] = Seq(Descriptive.Distr)

  override def detect(values: HashMap[String, StatValue], baseline: HashMap[String, StatValue]): Option[Double] = {

    // get frequencies from the distributions
    val (observedFreq, baselineFreq) = StatsDriftDetector.getMatchedFrequencies(values(Descriptive.Distr).getMap, baseline(Descriptive.Distr).getMap)

    // compute JensenShannon distance
    val distance = jensenShannon(observedFreq, baselineFreq)

    // return distance if showAll or drift deteted
    val drift = distance > threshold
    if (showAll || drift)
      Some(distance)
    else
      None
  }

  def jensenShannon(p: Seq[Double], q: Seq[Double]): Double = {
    val m: Seq[Double] = p.zip(q).map { case (pi, qi) => (pi + qi) / 2.0 } // M = 1/2 (P + Q)
    (KullbackLeiblerDetector.kullbackLeibler(p, m) / 2.0) + (KullbackLeiblerDetector.kullbackLeibler(q, m) / 2.0) // JS(P||Q) = 1/2 KL(P||M) + 1/2 KL (Q||M)
  }
}