package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}

case class ModelInfo(name: String, id: String, version: Int)


object ModelInfo {
  implicit val decodeModelInfo: Decoder[ModelInfo] = Decoder.forProduct3("name", "id", "version")(ModelInfo.apply)

  def getFromEnv: Option[ModelInfo] = {
    val modelInfoJson = Environment.getEnvVar(Constants.EnvVars.ModelInfo)
    if (modelInfoJson isEmpty) {
      println(s"Environment variable: ${Constants.EnvVars.ModelInfo} is missing")
      None
    }
    else Json.extract[ModelInfo](modelInfoJson.get)
  }
}