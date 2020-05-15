package io.hops.monitoring.stats

import io.hops.monitoring.stats.aggregators.{StatAggregator, StatCompoundAggregator, StatMultipleAggregator, StatSimpleAggregator}
import io.hops.monitoring.stats.definitions.StatDefinition
import io.hops.monitoring.utils.StatsUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable

class Stats(val features: Seq[String], private var stats: Seq[StatDefinition]) {

  val statDefinitions: Seq[StatDefinition] = Stats.requiredStats(stats)
  private val _simple: Seq[StatDefinition] = statDefinitions.filter(d => StatsUtil.isSimple(d.name))
  private val _compound: Seq[StatDefinition] = statDefinitions.filter(d => StatsUtil.isCompound(d.name))
  private val _multiple: Seq[StatDefinition] = statDefinitions.filter(d => StatsUtil.isMultiple(d.name))

  private val _map: HashMap[String, HashMap[String, StatAggregator]] = Stats.buildMap(features, statDefinitions)

  // Methods

  def getFeatureStats(feature: String): Seq[StatAggregator] = {
    val featureStats = _map(feature)
    stats.map(s => featureStats(s.name))
  }

  def computeSimple(feature: String, value: Double): Unit = {
    val featureStats = _map(feature)

    _simple // Compute simple stats
      .foreach { stat => // for each stat
        featureStats(stat.name).asInstanceOf[StatSimpleAggregator].compute(value, featureStats)
      }
  }

  def computeCompound(feature: String): Unit = {
    val featureStats = _map(feature)

    _compound // Compute compound stats
      .foreach(stat => { // for each stat
        featureStats(stat.name).asInstanceOf[StatCompoundAggregator].compute(featureStats)
      })
  }

  def computeMultiple(feature: String, values: HashMap[String, Double]): Unit = {
    val featureStats = _map(feature)

    _multiple // Compute multiple-stat
      .foreach(stat => { // for each stat
        featureStats(stat.name).asInstanceOf[StatMultipleAggregator].compute(values, _map)
      })
  }
}

object Stats {

  private def requiredStats(stats: Seq[StatDefinition]): Seq[StatDefinition] = {

    val statsBuffer: mutable.Buffer[StatDefinition] = mutable.Buffer[StatDefinition](stats: _*)
    val requiredStats = stats.flatMap(_.require).distinct

    // add missing required stats
    requiredStats.foreach(stat =>
      if (!statsBuffer.exists(_.name == stat))
        statsBuffer.prepend(StatDefinition.get(stat)))

    statsBuffer
  }

  private def buildMap(features: Seq[String], statDefinitions: Seq[StatDefinition]): HashMap[String, HashMap[String, StatAggregator]] =
    HashMap[String, HashMap[String, StatAggregator]](
      features.map(
        f => f -> HashMap[String, StatAggregator](statDefinitions.map(d => d.name -> StatAggregator.get(d, f))(collection.breakOut): _*)): _*)
}
