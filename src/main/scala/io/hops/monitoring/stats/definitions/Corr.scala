package io.hops.monitoring.stats.definitions

import io.hops.monitoring.stats.definitions.Corr.CorrType
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.Constants.Stats.Descriptive.{Population, Sample}

case class Corr(type_ : CorrType.Value = CorrType.SAMPLE) extends StatDefinition {
  override val name: String = Descriptive.Corr
  override val require: Seq[String] = Seq(Descriptive.Avg, Descriptive.Stddev)
}

object Corr {
  object CorrType extends Enumeration {
    type Type = Value
    val SAMPLE: CorrType.Value = Value(Sample)
    val POPULATION: CorrType.Value  = Value(Population)
  }
}