package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class KafkaTopicConfig(name: String, partitions: Option[Int], replicationFactor: Option[Int])

object KafkaTopicConfig {
  implicit val decodeKafkaTopicConfig: Decoder[KafkaTopicConfig] =
    Decoder.forProduct3("name", "partitions", "replicationFactor")(KafkaTopicConfig.apply)
}