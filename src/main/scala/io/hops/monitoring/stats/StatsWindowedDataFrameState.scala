package io.hops.monitoring.stats

import io.hops.monitoring.util.Constants.Stats
import scala.collection.mutable

case class StatsWindowedDataFrameState(private val colNames: Seq[String], private var _stats: Seq[String]) extends java.io.Serializable {

  // Required stats
  val stats: Seq[String] = {
    if (_stats.contains(Stats.Avg) || _stats.contains(Stats.Mean)) {
      _stats = _stats ++ Seq(Stats.Sum, Stats.Count)
    }
    _stats.distinct
  }

  private var _statsMap: mutable.HashMap[String, mutable.HashMap[String, Option[Float]]] = mutable.HashMap[String, mutable.HashMap[String, Option[Float]]](
    colNames.map(
      col => col -> mutable.HashMap(
        stats.map(stat => stat -> (None: Option[Float])): _*)
    ): _*)
  val statsMap: mutable.HashMap[String, mutable.HashMap[String, Option[Float]]] = _statsMap

  // Methods

  def update(updatedHashMap: mutable.HashMap[String, mutable.HashMap[String, Option[Float]]]): StatsWindowedDataFrameState = {
    _statsMap = updatedHashMap
    this
  }
}