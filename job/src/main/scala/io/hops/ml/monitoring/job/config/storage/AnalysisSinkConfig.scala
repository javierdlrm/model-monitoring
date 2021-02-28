package io.hops.ml.monitoring.job.config.storage

import io.circe.Decoder

case class AnalysisSinkConfig(parquet: ParquetConfig)

object AnalysisSinkConfig {
  implicit val decodeJobSinkConfig: Decoder[AnalysisSinkConfig] = Decoder.forProduct1("parquet")(AnalysisSinkConfig
    .apply)
}