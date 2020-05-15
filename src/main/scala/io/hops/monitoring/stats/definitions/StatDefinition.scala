package io.hops.monitoring.stats.definitions

import io.hops.monitoring.stats.{StatDouble, StatMap, StatValue}
import io.hops.monitoring.utils.Constants.Stats.Descriptive

trait StatDefinition {
  val name: String
  val require: Seq[String] = Seq()
  val flags: Map[String, Any] = Map()
}

object StatDefinition {
  def get(stat: String): StatDefinition =
    stat match {
      case Descriptive.Max => Max()
      case Descriptive.Min => Min()
      case Descriptive.Count => Count()
      case Descriptive.Sum => Sum()
      case Descriptive.Pow2Sum => Pow2Sum()
      case Descriptive.Avg => Avg()
      case Descriptive.Stddev => Stddev()
      case Descriptive.Mean => Mean()
      case Descriptive.Distr => Distr()
      case Descriptive.Corr => Corr()
      case Descriptive.Cov => Cov()
    }

  def toStatValue(stat: String, value: Any): StatValue = stat match {
    case Descriptive.Distr | Descriptive.Cov | Descriptive.Corr => StatMap.from(value)
    case _ => StatDouble.from(value)
  }
}