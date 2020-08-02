package io.hops.ml.monitoring.drift

import io.hops.ml.monitoring.drift.detectors.WindowDriftDetector
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{KeyValueGroupedDataset, Row}

trait WindowDriftPipeJoint {

  def kvgd(cols: Seq[String]): (KeyValueGroupedDataset[Window, Row], StructType)

  def drift(cols: Seq[String], detectors: Seq[WindowDriftDetector], baseline: Baseline): WindowDriftPipe = {
    val (df, schema) = kvgd(cols)
    new WindowDriftPipe(df, schema, cols, detectors, baseline)
  }
}
