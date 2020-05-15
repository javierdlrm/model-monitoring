package io.hops.monitoring.outliers

import io.hops.monitoring.outliers.detectors.WindowOutlierDetector
import io.hops.monitoring.stats.Baseline
import io.hops.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{KeyValueGroupedDataset, Row}

trait WindowOutliersPipeJoint {

  def kvgd(cols: Seq[String]): (KeyValueGroupedDataset[Window, Row], StructType)

  def outliers(cols: Seq[String], detectors: Seq[WindowOutlierDetector], baseline: Baseline): WindowOutliersPipe = {
    val (df, schema) = kvgd(cols)
    new WindowOutliersPipe(df, schema, cols, detectors, baseline)
  }
}
