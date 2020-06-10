package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.stats.definitions.Cov.CovType
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive.{Population, Sample}

case class Cov(`type` : CovType.Value = Cov.defaultType) extends StatDefinition {
  override val name: String = Descriptive.Cov
  override val require: Seq[String] = Seq(Descriptive.Avg)
}

object Cov {

  val defaultType: CovType.Value = CovType.SAMPLE

  object CovType extends Enumeration {
    type Type = Value
    val SAMPLE: CovType.Value = Value(Sample)
    val POPULATION: CovType.Value = Value(Population)
  }

}