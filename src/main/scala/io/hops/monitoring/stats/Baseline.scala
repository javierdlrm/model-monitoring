package io.hops.monitoring.stats

import io.hops.monitoring.utils.Constants.Stats.Descriptive

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class Baseline(baseline: mutable.HashMap[String, HashMap[String, StatValue]]) extends mutable.HashMap[String, HashMap[String, StatValue]] {
  this ++= baseline

  val features: Seq[String] = this.keys.toSeq
  val stats: Seq[String] = if (this.nonEmpty) this.head._2.keys.toSeq else Seq()

  def getDistributionsBounds: HashMap[String, Seq[String]] = {
    val bounds = this.map(f => f._1 -> f._2(Descriptive.Distr).getMap.keys.toSeq)
    HashMap[String, Seq[String]](bounds.toSeq:_*)
  }
}

object Baseline {
  def apply(descriptiveStats: Map[String, Map[String, Double]] = Map(), featureDistributions: Map[String, Map[String, Double]] = Map()): Baseline = {

    // Transform into StatValues
    val descriptive = transformDoubles(descriptiveStats)
    val distributions = transformMaps(featureDistributions)

    // Combine maps
    descriptive.keys.foreach(feature => {
      descriptive(feature) += Descriptive.Distr -> distributions(feature)
    })

    Baseline(descriptive)
  }

  def transformDoubles(stats: Map[String, Map[String, Double]]): mutable.HashMap[String, HashMap[String, StatValue]] =
    mutable.HashMap[String, HashMap[String, StatValue]](
      stats.map(
        feature => feature._1 -> HashMap[String, StatValue](
          feature._2.map(
            stat => stat._1 -> StatDouble(stat._2)).toSeq: _*)).toSeq: _*)

  def transformMaps(stats: Map[String, Map[String, Double]]): mutable.HashMap[String, StatValue] =
    mutable.HashMap[String, StatValue](
      stats.map(
        feature => feature._1 -> StatMap(mutable.HashMap[String, Double](
          feature._2.map(
            stat => stat._1 -> stat._2).toSeq: _*))).toSeq: _*)
}
