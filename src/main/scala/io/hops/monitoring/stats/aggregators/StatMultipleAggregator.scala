package io.hops.monitoring.stats.aggregators


import io.hops.monitoring.stats.StatValue

import scala.collection.immutable.HashMap

trait StatMultipleAggregator extends StatAggregator {
  def compute(values: HashMap[String, Double], stats: HashMap[String, HashMap[String, StatAggregator]]): StatValue
}
