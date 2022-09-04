package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Json}
import io.hops.ml.monitoring.utils.LoggerUtil

case class Config(modelInfo: ModelInfo, monitoringConfig: MonitoringConfig, storageConfig: StorageConfig, jobConfig: Option[JobConfig])

object Config {

  implicit val decodeConfig: Decoder[Config] =
    Decoder.forProduct4("modelInfo", "monitoringConfig", "storageConfig", "jobConfig")(Config.apply)

  def getFromEnv: Config = {

    LoggerUtil.log.info("[Monitor] Reading configuration from environment...")

    // Model info
    val modelInfoOpt = ModelInfo.getFromEnv
    val modelInfo = if (modelInfoOpt isEmpty) {
      throw new Exception(s"${Constants.EnvVars.ModelInfo} env var is required")
    } else modelInfoOpt.get

    // Monitoring config
    val monitoringConfigOpt = MonitoringConfig.getFromEnv
    val monitoringConfig = if (monitoringConfigOpt isEmpty) {
      throw new Exception(s"${Constants.EnvVars.MonitoringConfig} env var is required")
    } else monitoringConfigOpt.get

    // Storage config
    val storageConfigOpt = StorageConfig.getFromEnv
    val storageConfig = if (storageConfigOpt isEmpty) {
      throw new Exception(s"${Constants.EnvVars.StorageConfig} env var is required")
    } else storageConfigOpt.get

    // Job config
    val jobConfig = JobConfig.getFromEnv

    LoggerUtil.log.info(s"[Monitor] Configuration loaded")

    Config(modelInfo, monitoringConfig, storageConfig, jobConfig)
  }

  def getFromFile(file: String): Config = {

    LoggerUtil.log.info("[Monitor] Reading configuration from file...")

    val json = scala.io.Source.fromFile(file)
    val configOpt = Json.extract[Config](json.mkString)
    json.close()

    val config = if (configOpt isEmpty) {
      throw new Exception("Cannot extract the configuration")
    } else configOpt.get

    MonitoringConfig.prepare(config.monitoringConfig)

    LoggerUtil.log.info(s"[Monitor] Configuration loaded")
    config
  }
}
