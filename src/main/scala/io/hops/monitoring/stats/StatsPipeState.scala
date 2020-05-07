package io.hops.monitoring.stats

import io.hops.monitoring.utils.Constants.Stats.Descriptive.{Avg, Sum, Mean, Count, Stddev, Pow2Sum}
import scala.collection.mutable

case class StatsPipeState(private val cols: Seq[String], private var _stats: Seq[String]) extends java.io.Serializable {

  // Required stats
  val stats: Seq[String] = {
    if (_stats.contains(Avg) || _stats.contains(Mean)) {
      _stats = Seq(Sum, Count) ++ _stats
    }
    if (_stats.contains(Stddev)) {
      _stats = Seq(Pow2Sum) ++ _stats
    }
    _stats.distinct
  }

  private var _statsMap: mutable.HashMap[String, mutable.HashMap[String, Option[Double]]] = mutable.HashMap[String, mutable.HashMap[String, Option[Double]]](
    cols.map(
      col => col -> mutable.HashMap(
        stats.map(stat => stat -> (None: Option[Double])): _*)
    ): _*)
  val statsMap: mutable.HashMap[String, mutable.HashMap[String, Option[Double]]] = _statsMap

  // Methods

  def update(updatedHashMap: mutable.HashMap[String, mutable.HashMap[String, Option[Double]]]): StatsPipeState = {
    _statsMap = updatedHashMap
    this
  }
}