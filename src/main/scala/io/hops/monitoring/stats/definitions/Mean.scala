package io.hops.monitoring.stats.definitions

import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Mean() extends StatDefinition {
  override val name: String = Descriptive.Mean
  override val require: Seq[String] = Seq(Descriptive.Min, Descriptive.Max, Descriptive.Count)
}
