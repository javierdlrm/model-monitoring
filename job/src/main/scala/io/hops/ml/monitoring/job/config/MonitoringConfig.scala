package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.config.monitoring._
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}
import io.hops.ml.monitoring.stats.definitions.Distr
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.LoggerUtil

case class MonitoringConfig(trigger: TriggerConfig, stats: StatsConfig, baseline: Option[BaselineConfig], outliers: Option[OutliersConfig], drift: Option[DriftConfig])

object MonitoringConfig {
  implicit val decodeMonitoringConfig: Decoder[MonitoringConfig] =
    Decoder.forProduct5("trigger", "stats", "baseline", "outliers", "drift")(MonitoringConfig.apply)

  def getFromEnv: Option[MonitoringConfig] = {
    val monitoringConfigJson = Environment.getEnvVar(Constants.EnvVars.MonitoringConfig)
    if (monitoringConfigJson isEmpty) {
      LoggerUtil.log.error(s"Environment variable: ${Constants.EnvVars.MonitoringConfig} is missing")
      None
    }
    else {
      val opt = Json.extract[MonitoringConfig](monitoringConfigJson.get)

      // check if baseline is defined
      val config = if (opt.isEmpty || opt.get.baseline.isEmpty) return opt else opt.get

      // update distr with baseline bounds
      val distrIdx = config.stats.definitions.indexWhere(s => s.name == Descriptive.Distr)
      if (distrIdx > 0) {
        config.stats.definitions = config.stats.definitions.updated(distrIdx, Distr(config.baseline.get.map.getDistributionsBounds))
        Some(config)
      } else opt
    }
  }
}