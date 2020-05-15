package io.hops.monitoring.stats.definitions

import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Sum() extends StatDefinition {
  override val name: String = Descriptive.Sum
}
