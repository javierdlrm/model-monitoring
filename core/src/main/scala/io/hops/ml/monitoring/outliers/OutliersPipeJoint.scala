package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.OutliersDetector
import io.hops.ml.monitoring.stats.Baseline
import org.apache.spark.sql.DataFrame

trait OutliersPipeJoint {
  def df: DataFrame

  def outliers(cols: Seq[String], detectors: Seq[OutliersDetector], baseline: Baseline): OutliersPipe = {
    new OutliersPipe(df, cols, detectors, baseline)
  }
}
