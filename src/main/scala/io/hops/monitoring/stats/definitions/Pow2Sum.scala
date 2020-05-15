package io.hops.monitoring.stats.definitions

import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Pow2Sum() extends StatDefinition {
  override val name: String = Descriptive.Pow2Sum
}
