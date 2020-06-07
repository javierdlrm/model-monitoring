package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class SourceConfig(kafka: KafkaConfig)

object SourceConfig {
  implicit val decodeJobSourceConfig: Decoder[SourceConfig] =
    Decoder.forProduct1("kafka")(SourceConfig.apply)
}