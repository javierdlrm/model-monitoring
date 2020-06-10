package io.hops.ml.monitoring.job.config.storage

import io.circe.Decoder

case class SinkConfig(kafka: KafkaConfig)

object SinkConfig {
  implicit val decodeJobSinkConfig: Decoder[SinkConfig] = Decoder.forProduct1("kafka")(SinkConfig.apply)
}