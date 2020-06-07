package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}

case class MonitoringConfig(trigger: TriggerConfig, stats: List[StatDefinition], outliers: Option[List[OutlierDefinition]], drift: Option[List[DriftDefinition]])

object MonitoringConfig {
  implicit val decodeMonitoringConfig: Decoder[MonitoringConfig] =
    Decoder.forProduct4("trigger", "stats", "outliers", "drift")(MonitoringConfig.apply)

  def getFromEnv: Option[MonitoringConfig] = {
    val monitoringConfigJson = Environment.getEnvVar(Constants.EnvVars.MonitoringConfig)
    if (monitoringConfigJson isEmpty) {
      println(s"Environment variable: ${Constants.EnvVars.MonitoringConfig} is missing")
      None
    }
    else Json.extract[MonitoringConfig](monitoringConfigJson.get)
  }
}