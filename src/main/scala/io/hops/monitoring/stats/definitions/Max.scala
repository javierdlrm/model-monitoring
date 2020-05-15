package io.hops.monitoring.stats.definitions

import io.hops.monitoring.utils.Constants.Stats.Descriptive

case class Max() extends StatDefinition {
  override val name: String = Descriptive.Max
}
