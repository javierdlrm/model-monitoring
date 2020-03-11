package io.hops.monitoring.io.kafka

import io.hops.monitoring.util.Constants.Kafka
import io.hops.monitoring.streams.writer.{StreamWriter, StreamWriterBatch}

object KafkaWriter {
  implicit class KafkaOutputDataFrame(val ow: StreamWriter) extends AnyVal {

    def kafka(options: Map[String, String]): StreamWriter = {
      // Create batch writer
      val writer = new StreamWriterBatch(Kafka.Format, options)
      // Add writer
      ow.addWriter(writer)
    }
  }
}
