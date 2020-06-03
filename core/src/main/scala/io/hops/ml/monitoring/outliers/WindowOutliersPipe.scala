package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.WindowOutlierDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.window.Window
import io.hops.ml.monitoring.outliers.detectors.WindowOutlierDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.Baseline
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

// TODO: Applicable on instance values (i.e VAEOutlier, LinearRegression, ...)

class WindowOutliersPipe(source: KeyValueGroupedDataset[Window, Row], schema: StructType, cols: Seq[String], detectors: Seq[WindowOutlierDetector], baseline: Baseline) extends SinkPipeJoint {
  override def df: DataFrame = ???

  ???
}

