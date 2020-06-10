package io.hops.ml.monitoring.job.config.model

import io.circe.Decoder

case class InferenceSchemas(request: String, instance: String, response: String, prediction: String)

object InferenceSchemas {
  implicit val decodeInferenceSchemas: Decoder[InferenceSchemas] =
    Decoder.forProduct4("request", "instance", "response", "prediction")(InferenceSchemas.apply)
}