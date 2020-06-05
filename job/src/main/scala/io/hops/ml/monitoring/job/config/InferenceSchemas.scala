package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.{Constants, Environment, Json}

case class InferenceSchemas(request: String, response: String)

object InferenceSchemas {
  implicit val decodeInferenceSchemas: Decoder[InferenceSchemas] = Decoder.forProduct2("request", "response")(InferenceSchemas.apply)

  def getFromEnv: Option[InferenceSchemas] = {
    val decodeInferenceSchemas = Environment.getEnvVar(Constants.EnvVars.InferenceSchemas)
    if (decodeInferenceSchemas isEmpty) {
      println(s"Environment variable: ${Constants.EnvVars.InferenceSchemas} is missing")
      None
    }
    else Json.extract[InferenceSchemas](decodeInferenceSchemas.get)
  }
}