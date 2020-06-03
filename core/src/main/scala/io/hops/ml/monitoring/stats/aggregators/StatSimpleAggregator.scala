package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait StatSimpleAggregator extends StatAggregator {
  def compute(value: Double, stats: HashMap[String, StatAggregator]): StatValue
}
