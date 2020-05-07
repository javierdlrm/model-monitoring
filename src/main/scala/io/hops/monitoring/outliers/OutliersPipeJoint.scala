package io.hops.monitoring.outliers

import io.hops.monitoring.stats.DatasetStats
import org.apache.spark.sql.DataFrame

trait OutliersPipeJoint extends java.io.Serializable {

  def df: DataFrame

  def stats: Seq[String]

  def outliers(cols: Seq[String], stats: Seq[String], baselineStats: DatasetStats): StatOutliersPipe =
    new StatOutliersPipe(df, cols, stats.intersect(this.stats), baselineStats)

}
