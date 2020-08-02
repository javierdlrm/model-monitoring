package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.WindowOutliersDetector
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{KeyValueGroupedDataset, Row}

trait WindowOutliersPipeJoint {

  def kvgd(cols: Seq[String]): (KeyValueGroupedDataset[Window, Row], StructType)

  def outliers(cols: Seq[String], detectors: Seq[WindowOutliersDetector], baseline: Baseline): WindowOutliersPipe = {
    val (df, schema) = kvgd(cols)
    new WindowOutliersPipe(df, schema, cols, detectors, baseline)
  }
}
