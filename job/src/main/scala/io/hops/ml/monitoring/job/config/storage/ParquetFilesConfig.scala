package io.hops.ml.monitoring.job.config.storage

import io.circe.Decoder

case class ParquetFilesConfig(prefix: String)

object ParquetFilesConfig {
  implicit val decodeParquetFilesConfig: Decoder[ParquetFilesConfig] =
    Decoder.forProduct1("prefix")(ParquetFilesConfig.apply)
}