package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class OutlierDefinition(name: String, params: Option[Map[String, String]])

object OutlierDefinition {
  implicit val decodeOutlierDefinition: Decoder[OutlierDefinition] =
    Decoder.forProduct2("name", "params")(OutlierDefinition.apply)
}
