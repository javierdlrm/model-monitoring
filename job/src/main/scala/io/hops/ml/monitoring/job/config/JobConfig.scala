package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}

case class JobConfig(timeout: Int, source: SourceConfig, sink: List[SinkConfig])

object JobConfig {
  implicit val decodeJobConfig: Decoder[JobConfig] =
    Decoder.forProduct3("timeout", "source", "sink")(JobConfig.apply)

  def getFromEnv: Option[JobConfig] = {
    val jobConfigJson = Environment.getEnvVar(Constants.EnvVars.JobConfig)
    if (jobConfigJson isEmpty) {
      println(s"Environment variable: ${Constants.EnvVars.JobConfig} is missing")
      None
    }
    else Json.extract[JobConfig](jobConfigJson.get)
  }
}
