package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

case class Sum() extends StatDefinition {
  override val name: String = Descriptive.Sum
}
