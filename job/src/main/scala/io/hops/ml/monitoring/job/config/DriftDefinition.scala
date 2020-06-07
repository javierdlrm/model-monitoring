package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class DriftDefinition(name: String, threshold: Option[Long], showAll: Option[Boolean])

object DriftDefinition {
  implicit val decodeOutlierDefinition: Decoder[DriftDefinition] =
    Decoder.forProduct3("name", "threshold", "showAll")(DriftDefinition.apply)
}
