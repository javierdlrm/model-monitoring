package io.hops.monitoring.utils

import io.hops.monitoring.utils.Constants.Stats.Descriptive.{Avg, Count, Max, Mean, Min, Stddev, Sum, Pow2Sum, Simple, Compound}

import scala.collection.mutable

object StatsUtil {

  def isSimple(stat: String): Boolean =
    Simple.contains(stat)

  def isCompound(stat: String): Boolean =
    Compound.contains(stat)

  def defaultStat(stat: String, value: Double): Double =
    stat match {
      case Max => value
      case Min => value
      case Count => 1.0
      case Sum => value
      case Pow2Sum => math.pow(value, 2)
    }

  // Compute stats

  object Compute {

    def simpleStat(stat: String, value: Double, stats: mutable.HashMap[String, Option[Double]]): Double =
      stat match {
        case Max => Compute.max(value, stats)
        case Min => Compute.min(value, stats)
        case Count => Compute.count(stats)
        case Sum => Compute.sum(value, stats)
        case Pow2Sum => Compute.pow2Sum(value, stats)
      }

    def compoundStat(stat: String, stats: mutable.HashMap[String, Option[Double]]): Double = {
      stat match {
        case Avg => Compute.avg(stats)
        case Mean => Compute.mean(stats)
        case Stddev => Compute.stddevSam(stats)
      }
    }

    def mean(stats: mutable.HashMap[String, Option[Double]]): Double =
      (stats(Max).get - stats(Min).get) / stats(Count).get

    def avg(stats: mutable.HashMap[String, Option[Double]]): Double =
      stats(Sum).get / stats(Count).get

    def max(value: Double, stats: mutable.HashMap[String, Option[Double]]): Double =
      math.max(value, stats(Max).get)

    def min(value: Double, stats: mutable.HashMap[String, Option[Double]]): Double =
      math.min(value, stats(Min).get)

    def count(stats: mutable.HashMap[String, Option[Double]]): Double =
      stats(Count).get + 1.0

    def sum(value: Double, stats: mutable.HashMap[String, Option[Double]]): Double =
      value + stats(Sum).get

    def pow2Sum(value: Double, stats: mutable.HashMap[String, Option[Double]]): Double =
      math.pow(value, 2.0) + stats(Pow2Sum).get

    def stddevSam(stats: mutable.HashMap[String, Option[Double]]): Double =
      math.sqrt((stats(Pow2Sum).get - (stats(Count).get * math.pow(stats(Avg).get, 2.0))) / (stats(Count).get - 1.0))

    def stddevPop(stats: mutable.HashMap[String, Option[Double]]): Double =
      math.sqrt((stats(Pow2Sum).get - (stats(Count).get * math.pow(stats(Avg).get, 2.0))) / stats(Count).get)
  }

}
