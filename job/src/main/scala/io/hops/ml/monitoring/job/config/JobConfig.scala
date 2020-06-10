package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}

case class JobConfig(timeout: Option[Int])

object JobConfig {
  implicit val decodeJobConfig: Decoder[JobConfig] =
    Decoder.forProduct1("timeout")(JobConfig.apply)

  def getFromEnv: Option[JobConfig] = {
    val jobConfigJson = Environment.getEnvVar(Constants.EnvVars.JobConfig)
    if (jobConfigJson isEmpty) return None
    Json.extract[JobConfig](jobConfigJson.get)
  }
}
