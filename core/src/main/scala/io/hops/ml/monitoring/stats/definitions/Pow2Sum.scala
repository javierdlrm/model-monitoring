package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

case class Pow2Sum() extends StatDefinition {
  override val name: String = Descriptive.Pow2Sum
}
