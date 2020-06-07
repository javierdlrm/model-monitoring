package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class InferenceSchemas(request: String, response: String)

object InferenceSchemas {
  implicit val decodeInferenceSchemas: Decoder[InferenceSchemas] =
    Decoder.forProduct2("request", "response")(InferenceSchemas.apply)
}