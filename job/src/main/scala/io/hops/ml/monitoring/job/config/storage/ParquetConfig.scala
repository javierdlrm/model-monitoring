package io.hops.ml.monitoring.job.config.storage

import io.circe.Decoder

case class ParquetConfig(directory: String, files: ParquetFilesConfig)

object ParquetConfig {
  implicit val decodeParquetConfig: Decoder[ParquetConfig] =
    Decoder.forProduct2("directory", "files")(ParquetConfig.apply)
}