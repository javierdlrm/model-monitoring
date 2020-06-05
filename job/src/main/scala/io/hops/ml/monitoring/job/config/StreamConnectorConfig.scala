package io.hops.ml.monitoring.job.config

import io.circe.Decoder
import io.hops.ml.monitoring.job.utils.Constants.Kafka
import io.hops.ml.monitoring.utils.Constants

case class StreamConnectorConfig(format: String, params: Map[String, String])

object StreamConnectorConfig {
  implicit val streamConnectorConfig: Decoder[StreamConnectorConfig] = Decoder.forProduct2("format", "params")(StreamConnectorConfig.apply)

  implicit class ExtendStreamConnectorConfig(scc: StreamConnectorConfig) {
    def validate: Boolean = {
      scc.format match {
        case Constants.Kafka.Kafka => validateKafkaConnector(scc)
        case _ => throw new Exception(s"Stream connector format '${scc.format}' not recognized")
      }
    }

    def validateKafkaConnector(kc: StreamConnectorConfig): Boolean = {
      kc.params.contains(Kafka.Brokers) && kc.params.contains(Constants.Kafka.Topic)
    }
  }

}