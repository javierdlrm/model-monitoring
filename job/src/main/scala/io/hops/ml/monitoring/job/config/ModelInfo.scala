package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.config.model.InferenceSchemas
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}
import io.hops.ml.monitoring.utils.LoggerUtil

case class ModelInfo(name: String, id: Option[String], version: Option[Int], schemas: InferenceSchemas)

object ModelInfo {
  implicit val decodeModelInfo: Decoder[ModelInfo] =
    Decoder.forProduct4("name", "id", "version", "schemas")(ModelInfo.apply)

  def getFromEnv: Option[ModelInfo] = {
    val modelInfoJson = Environment.getEnvVar(Constants.EnvVars.ModelInfo)
    if (modelInfoJson isEmpty) {
      LoggerUtil.log.error(s"Environment variable: ${Constants.EnvVars.ModelInfo} is missing")
      None
    }
    else Json.extract[ModelInfo](modelInfoJson.get)
  }
}