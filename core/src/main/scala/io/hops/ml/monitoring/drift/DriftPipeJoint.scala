package io.hops.ml.monitoring.drift

import io.hops.ml.monitoring.drift.detectors.DriftDetector
import io.hops.ml.monitoring.stats.Baseline
import org.apache.spark.sql.DataFrame

trait DriftPipeJoint {
  def df: DataFrame

  def drift(cols: Seq[String], detectors: Seq[DriftDetector], baseline: Baseline): DriftPipe = {
    new DriftPipe(df, cols, detectors, baseline)
  }
}
