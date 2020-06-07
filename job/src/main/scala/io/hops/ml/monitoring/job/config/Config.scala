package io.hops.ml.monitoring.job.config

import io.hops.ml.monitoring.job.utils.Constants

case class Config(modelInfo: ModelInfo, monitoringConfig: MonitoringConfig, jobConfig: JobConfig)

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

    // Job config
    val jobConfigOpt = JobConfig.getFromEnv
    val jobConfig = if (jobConfigOpt isEmpty) {
      throw new Exception(s"${Constants.EnvVars.JobConfig} env var is required")
    } else jobConfigOpt.get

    Config(modelInfo, monitoringConfig, jobConfig)
  }
}
