package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class KafkaConfig(brokers: String, topic: KafkaTopicConfig)

object KafkaConfig {
  implicit val decodeKafkaConfig: Decoder[KafkaConfig] =
    Decoder.forProduct2("brokers", "topic")(KafkaConfig.apply)
}