package io.hops.monitoring.drift

import io.hops.monitoring.drift.detectors.StatsDriftDetector
import io.hops.monitoring.stats.Baseline
import io.hops.monitoring.stats.definitions.StatDefinition
import io.hops.monitoring.utils.LoggerUtil
import org.apache.spark.sql.DataFrame

trait StatsDriftPipeJoint extends java.io.Serializable {

  def df: DataFrame

  def stats: Seq[StatDefinition]

  def drift(detectors: Seq[StatsDriftDetector], baseline: Baseline): StatsDriftPipe = {

    val requiredStats: Seq[String] = detectors.flatMap(_.stats).distinct

    // Check available stats
    val common = requiredStats.intersect(stats.map(_.name))
    val validDetectors = if (common.length != requiredStats.length) {
      val absent = requiredStats.diff(common)
      LoggerUtil.log.warn(s"Some drift detectors cannot be applied. Please, add the following to the stats pipe [${absent.mkString(", ")}]")
      LoggerUtil.log.info(s"[DriftPipeJoint] Missing stats. Continuing with detectors for [${common.mkString(", ")}]")

      // filter out detectors using absent stats
      detectors.filterNot(d => d.stats.exists(absent.contains))
    }
    else detectors

    new StatsDriftPipe(df, requiredStats, validDetectors, baseline)
  }
}
