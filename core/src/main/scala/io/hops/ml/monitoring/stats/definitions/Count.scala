package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

case class Count() extends StatDefinition {
  override val name: String = Descriptive.Count
}
