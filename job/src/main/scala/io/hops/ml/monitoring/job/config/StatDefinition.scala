package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class StatDefinition (name: String, params: Option[Map[String, String]])

object StatDefinition {
  implicit val decodeStatDefinition: Decoder[StatDefinition] =
    Decoder.forProduct2("name", "params")(StatDefinition.apply)
}
