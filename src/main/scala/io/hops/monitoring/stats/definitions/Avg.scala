package io.hops.monitoring.stats.definitions

import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Avg() extends StatDefinition {
  override val name: String = Descriptive.Avg
  override val require: Seq[String] = Seq(Descriptive.Sum, Descriptive.Count)
}