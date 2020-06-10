package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.stats.definitions.Stddev.StddevType
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive.{Population, Sample}

case class Stddev(`type` : StddevType.Value = Stddev.defaultType) extends StatDefinition {
  override val name: String = Descriptive.Stddev
  override val require: Seq[String] = Seq(Descriptive.Count, Descriptive.Avg, Descriptive.Pow2Sum)
}

object Stddev {

  val defaultType: StddevType.Value = StddevType.SAMPLE

  object StddevType extends Enumeration {
    type Type = Value
    val SAMPLE: StddevType.Value = Value(Sample)
    val POPULATION: StddevType.Value = Value(Population)
  }

}