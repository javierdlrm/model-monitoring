package io.hops.monitoring.io.kafka

import io.hops.monitoring.pipeline.{Pipeline, SinkPipe}
import io.hops.monitoring.utils.Constants.Kafka

object KafkaSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {

    def kafka(settings: KafkaSettings): Pipeline = {
      val sink = new SinkPipe(Kafka.Format, settings.options)
      pipeline.addSink(sink)
    }
  }

}
