package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.WindowOutliersDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

// TODO: Applicable on multiple instance values at once (i.e VAEOutlier, LinearRegression, ...)

class WindowOutliersPipe(source: KeyValueGroupedDataset[Window, Row], schema: StructType, cols: Seq[String], detectors: Seq[WindowOutliersDetector], baseline: Baseline) extends SinkPipeJoint {
  override def df: DataFrame = ???

  ???
}

