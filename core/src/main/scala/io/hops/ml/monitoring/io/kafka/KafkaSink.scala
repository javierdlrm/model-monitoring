package io.hops.ml.monitoring.io.kafka

import io.hops.ml.monitoring.utils.Constants.File
import io.hops.ml.monitoring.pipeline.{Pipeline, SinkPipe}
import io.hops.ml.monitoring.utils.Constants.Kafka
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, to_json}

object KafkaSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {

    def kafka(settings: KafkaSettings, checkpoint: String): Pipeline = {
      val options = Map[String, String]((File.CheckpointLocation -> checkpoint) +: settings.options.toSeq:_*)

      val sink = new SinkPipe(Kafka.Kafka, options, Some(toKafkaFormat))
      pipeline.addSink(sink)
    }

    private def toKafkaFormat(df: DataFrame): DataFrame = {
        df.select(to_json(struct(df.columns.map(col): _*)) as Kafka.Value)
    }
  }

}
