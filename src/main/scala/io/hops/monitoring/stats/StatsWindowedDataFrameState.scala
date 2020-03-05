package io.hops.monitoring.stats

import io.hops.monitoring.util.Constants.Stats
import io.hops.monitoring.util.LoggerUtil

import scala.collection.mutable.HashMap

case class StatsWindowedDataFrameState(private val colNames: Seq[String], private var _stats: Seq[String]) extends java.io.Serializable {

  // Required stats
  val stats: Seq[String] = {
    if (_stats.contains(Stats.Avg) || _stats.contains(Stats.Mean)) {
      _stats = _stats ++ Seq(Stats.Sum, Stats.Count)
    }
    _stats.distinct
  }

  private var _statsMap: HashMap[String, HashMap[String, Option[Float]]] = HashMap[String, HashMap[String, Option[Float]]](
    colNames.map(
      col => col -> HashMap(
        stats.map(stat => stat -> (None: Option[Float])): _*)
    ): _*)
  val statsMap = _statsMap

  // Methods

  def update(updatedHashMap: HashMap[String, HashMap[String, Option[Float]]]): StatsWindowedDataFrameState = {
    _statsMap = updatedHashMap
    this
  }
}