package io.hops.monitoring.stats.aggregators

import io.hops.monitoring.stats.StatValue
import io.hops.monitoring.stats.definitions._
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import org.apache.spark.sql.types.StructField

trait StatAggregator extends java.io.Serializable {
  def value: StatValue
}

object StatAggregator {
  def get(stat: StatDefinition, feature: String): StatAggregator =
    stat.name match {
      case Descriptive.Max => MaxAggregator()
      case Descriptive.Min => MinAggregator()
      case Descriptive.Count => CountAggregator()
      case Descriptive.Sum => SumAggregator()
      case Descriptive.Pow2Sum => Pow2SumAggregator()
      case Descriptive.Distr => DistrAggregator(stat.asInstanceOf[Distr], feature)
      case Descriptive.Avg => AvgAggregator()
      case Descriptive.Mean => MeanAggregator()
      case Descriptive.Stddev => StddevAggregator(stat.asInstanceOf[Stddev])
      case Descriptive.Cov => CovAggregator(stat.asInstanceOf[Cov], feature)
      case Descriptive.Corr => CorrAggregator(stat.asInstanceOf[Corr], feature)
    }

  def getStructField(stat: String): StructField =
    stat match {
      case Descriptive.Max => MaxAggregator.structField
      case Descriptive.Min => MinAggregator.structField
      case Descriptive.Count => CountAggregator.structField
      case Descriptive.Sum => SumAggregator.structField
      case Descriptive.Pow2Sum => Pow2SumAggregator.structField
      case Descriptive.Distr => DistrAggregator.structField
      case Descriptive.Avg => AvgAggregator.structField
      case Descriptive.Mean => MeanAggregator.structField
      case Descriptive.Stddev => StddevAggregator.structField
      case Descriptive.Cov => CovAggregator.structField
      case Descriptive.Corr => CorrAggregator.structField
    }
}
