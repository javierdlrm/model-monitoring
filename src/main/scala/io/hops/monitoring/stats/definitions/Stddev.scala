package io.hops.monitoring.stats.definitions

import io.hops.monitoring.stats.definitions.Stddev.StddevType
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.Constants.Stats.Descriptive.{Population, Sample}

case class Stddev(type_ : StddevType.Value = Stddev.StddevType.SAMPLE) extends StatDefinition {
  override val name: String = Descriptive.Stddev
  override val require: Seq[String] = Seq(Descriptive.Count, Descriptive.Avg, Descriptive.Pow2Sum)
}

object Stddev {

  object StddevType extends Enumeration {
    type Type = Value
    val SAMPLE: StddevType.Value = Value(Sample)
    val POPULATION: StddevType.Value = Value(Population)
  }

}