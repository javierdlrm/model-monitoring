package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait StatCompoundAggregator extends StatAggregator {
  def compute(stats: HashMap[String, StatAggregator]): StatValue
}
