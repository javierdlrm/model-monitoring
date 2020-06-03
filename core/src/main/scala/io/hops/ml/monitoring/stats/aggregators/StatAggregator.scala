package io.hops.ml.monitoring.stats.aggregators

import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.stats.StatValue
import io.hops.ml.monitoring.stats.definitions._
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
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
      case Descriptive.Perc => PercAggregator(stat.asInstanceOf[Perc])
    }

  def getStructField(stat: String): StructField =
    (stat match {
      case Descriptive.Max => MaxAggregator
      case Descriptive.Min => MinAggregator
      case Descriptive.Count => CountAggregator
      case Descriptive.Sum => SumAggregator
      case Descriptive.Pow2Sum => Pow2SumAggregator
      case Descriptive.Distr => DistrAggregator
      case Descriptive.Avg => AvgAggregator
      case Descriptive.Mean => MeanAggregator
      case Descriptive.Stddev => StddevAggregator
      case Descriptive.Cov => CovAggregator
      case Descriptive.Corr => CorrAggregator
      case Descriptive.Perc => PercAggregator
    }).structField
}
