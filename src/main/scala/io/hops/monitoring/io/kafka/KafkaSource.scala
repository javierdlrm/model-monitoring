package io.hops.monitoring.io.kafka

import io.hops.monitoring.pipeline.SourcePipe
import io.hops.monitoring.utils.Constants.Kafka

object KafkaSource {

  implicit class ExtendedSourcePipe(val sp: SourcePipe) extends AnyVal {

    def kafka(settings: KafkaSettings): SourcePipe = {

      assert(sp.spark isDefined, "Spark session is required")

      // create reader
      val reader = sp.spark.get.readStream.format(Kafka.Format)
      settings.options foreach { op => reader.option(op._1, op._2) }

      // add df
      val df = reader.load()
      sp._addDataFrame(df)
    }
  }

}
