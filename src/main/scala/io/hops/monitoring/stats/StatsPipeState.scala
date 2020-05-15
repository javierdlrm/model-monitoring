package io.hops.monitoring.stats

import io.hops.monitoring.stats.definitions.StatDefinition

class StatsPipeState(cols: Seq[String], statDefinitions: Seq[StatDefinition]) extends java.io.Serializable {
  val stats: Stats = new Stats(cols, statDefinitions)
  def features: Seq[String] = stats.features
}