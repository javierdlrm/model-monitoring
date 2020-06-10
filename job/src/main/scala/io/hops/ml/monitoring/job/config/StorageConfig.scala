package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.config.storage.{AnalysisStorageConfig, SinkConfig}
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}
import io.hops.ml.monitoring.utils.LoggerUtil

case class StorageConfig(inference: SinkConfig, analysis: AnalysisStorageConfig)

object StorageConfig {
  implicit val decodeMonitoringConfig: Decoder[StorageConfig] =
    Decoder.forProduct2("inference", "analysis")(StorageConfig.apply)

  def getFromEnv: Option[StorageConfig] = {
    val storageConfigJson = Environment.getEnvVar(Constants.EnvVars.StorageConfig)
    if (storageConfigJson isEmpty) {
      LoggerUtil.log.error(s"Environment variable: ${Constants.EnvVars.StorageConfig} is missing")
      None
    }
    else Json.extract[StorageConfig](storageConfigJson.get)
  }
}
