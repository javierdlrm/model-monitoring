package io.hops.monitoring.util

import Constants.Stats
import io.hops.monitoring.util.Constants.Stats.{Avg, Count, Max, Mean, Min, Stddev, Sum}

import scala.collection.mutable
import scala.collection.mutable.HashMap

object StatsUtil {

  def isSimple(stat: String): Boolean =
    Stats.Simple.contains(stat)

  def isCompound(stat: String): Boolean =
    Stats.Compound.contains(stat)

  def isComplex(stat: String): Boolean =
    Stats.Complex.contains(stat)

  def needsIterate(stat: String): Boolean =
    Stats.Iterative.contains(stat)

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
      }
    def compoundStat(stat: String, stats: mutable.HashMap[String, Option[Float]]): Float = {
      stat match {
        case Avg => Compute.avg(stats)
        case Mean => Compute.mean(stats)
      }
    }
    def complexStat(stat: String, value: Float, stats: mutable.HashMap[String, Option[Float]]): Float = {
      stat match {
        case Stddev => Compute.stddev_residual(value, stats)
      }
    }
    def complexStat(stat: String, stats: mutable.HashMap[String, Option[Float]]): Float = {
      stat match {
        case Stddev => Compute.stddev_sam(stats)
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

    def stddev_residual(value: Float, stats: mutable.HashMap[String, Option[Float]]): Float =
      math.pow(value - stats(Avg).get, 2.0.toFloat).toFloat

    def stddev_sam(stats: mutable.HashMap[String, Option[Float]]): Float =
      math.sqrt(stats(Stddev).get / (stats(Count).get - 1)).toFloat

    def stddev_pop(stats: mutable.HashMap[String, Option[Float]]): Float =
      math.sqrt(stats(Stddev).get / stats(Count).get).toFloat
  }
}
