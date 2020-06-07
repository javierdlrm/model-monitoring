package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class SinkConfig(pipe: String, kafka: KafkaConfig)

object SinkConfig {
  implicit val decodeJobSinkConfig: Decoder[SinkConfig] = Decoder.forProduct2("pipe", "kafka")(SinkConfig.apply)
}