package io.hops.monitoring.stats.definitions

import io.hops.monitoring.stats.definitions.Cov.CovType
import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Cov(type_ : CovType.Value = CovType.SAMPLE) extends StatDefinition {
  override val name: String = Descriptive.Cov
  override val require: Seq[String] = Seq(Descriptive.Avg)
}

object Cov {

  object CovType extends Enumeration {
    type Type = Value
    val SAMPLE, POPULATION = Value
  }

}