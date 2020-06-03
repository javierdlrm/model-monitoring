package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait StatMultipleAggregator extends StatAggregator {
  def compute(values: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): StatValue
}
