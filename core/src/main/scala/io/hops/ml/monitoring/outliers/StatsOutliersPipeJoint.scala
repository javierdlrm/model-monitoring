package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.StatsOutliersDetector
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.utils.LoggerUtil
import org.apache.spark.sql.DataFrame

trait StatsOutliersPipeJoint extends java.io.Serializable {

  def df: DataFrame

  def stats: Seq[StatDefinition]

  def outliers(detectors: Seq[StatsOutliersDetector], baseline: Baseline): StatsOutliersPipe = {

    val requiredStats: Seq[String] = detectors.flatMap(_.statNames).distinct

    // Check available stats
    val common = requiredStats.intersect(stats.map(_.name))
    val validDetectors = if (common.length != requiredStats.length) {
      val absent = requiredStats.diff(common)
      LoggerUtil.log.warn(s"Outliers cannot be detected in all the stats. Please, add the following to the stats pipe [${absent.mkString(", ")}]")
      LoggerUtil.log.info(s"[StatsOutliersPipeJoint] Missing stats. Continuing with detectors for [${common.mkString(", ")}]")

      // filter out detectors using absent stats
      detectors.flatMap(d => {
        d.statNames = d.statNames.filterNot(absent.contains)
        if (d.statNames nonEmpty) Some(d)
        else None
      })
    }
    else detectors

    new StatsOutliersPipe(df, validDetectors, baseline)
  }

}
