package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

case class Perc(percentiles: Seq[Int], iqr: Boolean = false) extends StatDefinition {
  override val name: String = Descriptive.Perc
  override val require: Seq[String] = Seq(Descriptive.Distr, Descriptive.Sum)
}
