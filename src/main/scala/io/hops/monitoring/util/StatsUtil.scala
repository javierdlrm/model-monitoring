package io.hops.monitoring.util

import Constants.Stats
import io.hops.monitoring.util.Constants.Stats.{Avg, Count, Max, Mean, Min, Stddev, Sum, Pow2Sum}

import scala.collection.mutable

object StatsUtil {

  def isSimple(stat: String): Boolean =
    Stats.Simple.contains(stat)

  def isCompound(stat: String): Boolean =
    Stats.Compound.contains(stat)

  def defaultStat(stat: String, value: Float): Float =
    if (stat == Stats.Count) 1.0.toFloat else value

  // Compute stats

  object Compute {

    def simpleStat(stat: String, value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      stat match {
        case Max => Compute.max(value, stats)
        case Min => Compute.min(value, stats)
        case Count => Compute.count(stats)
        case Sum => Compute.sum(value, stats)
        case Pow2Sum => Compute.pow2Sum(value, stats)
      }
    def compoundStat(stat: String, stats: mutable.HashMap[String, Option[Float]]): Float = {
      stat match {
        case Avg => Compute.avg(stats)
        case Mean => Compute.mean(stats)
        case Stddev => Compute.stddevSam(stats)
      }
    }

    def mean(stats: mutable.HashMap[String, Option[Float]]): Float =
      (stats(Max).get - stats(Min).get) / stats(Count).get

    def avg(stats: mutable.HashMap[String, Option[Float]]): Float =
      stats(Sum).get / stats(Count).get

    def max(value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      math.max(value, stats(Max).get)

    def min(value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      math.min(value, stats(Min).get)

    def count(stats: mutable.HashMap[String, Option[Float]]): Float =
      stats(Count).get + 1.0.toFloat

    def sum(value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      value + stats(Sum).get

    def pow2Sum(value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      math.pow(value, 2).toFloat + stats(Pow2Sum).get

    def stddevSam(stats: mutable.HashMap[String, Option[Float]]): Float =
      math.sqrt((stats(Pow2Sum).get / (stats(Count).get - 1)) - math.pow(stats(Avg).get, 2)).toFloat

    def stddevPop(stats: mutable.HashMap[String, Option[Float]]): Float =
      math.sqrt((stats(Pow2Sum).get / stats(Count).get) - math.pow(stats(Avg).get, 2)).toFloat
  }
}
