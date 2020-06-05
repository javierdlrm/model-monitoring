package io.hops.ml.monitoring.io.kafka

import io.hops.ml.monitoring.pipeline.Pipeline
import io.hops.ml.monitoring.utils.Constants.Kafka
import io.hops.ml.monitoring.pipeline.{Pipeline, SinkPipe}
import io.hops.ml.monitoring.utils.Constants.Kafka

object KafkaSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {

    def kafka(settings: KafkaSettings): Pipeline = {
      val sink = new SinkPipe(Kafka.Kafka, settings.options)
      pipeline.addSink(sink)
    }
  }

}
