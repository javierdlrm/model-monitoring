package io.hops.ml.monitoring.job.config.storage

import io.circe.Decoder

case class AnalysisStorageConfig(stats: AnalysisSinkConfig, outliers: Option[SinkConfig], drift: Option[SinkConfig])

object AnalysisStorageConfig {
  implicit val decodeMonitoringConfig: Decoder[AnalysisStorageConfig] =
    Decoder.forProduct3("stats", "outliers", "drift")(AnalysisStorageConfig.apply)
}
