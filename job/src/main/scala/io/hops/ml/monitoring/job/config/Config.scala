package io.hops.ml.monitoring.job.config

import io.hops.ml.monitoring.job.utils.Constants

case class Config(modelInfo: ModelInfo, monitoringConfig: MonitoringConfig, storageConfig: StorageConfig, jobConfig: Option[JobConfig])

object Config {

  def getFromEnv: Config = {

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

    Config(modelInfo, monitoringConfig, storageConfig, jobConfig)
  }
}
