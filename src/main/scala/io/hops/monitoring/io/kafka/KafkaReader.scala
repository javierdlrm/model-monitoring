package io.hops.monitoring.io.kafka

import io.hops.monitoring.io.InputReader
import io.hops.monitoring.util.Constants.Kafka

object KafkaReader {

  implicit class ExtendedKafka(val ir: InputReader) extends AnyVal {

    def kafka(options: Map[String, String]): InputReader = {

      assert(ir.spark isDefined, "Spark session is required")

      // create reader
      val reader = ir.spark.get.readStream.format(Kafka.Format)
      options foreach { op => reader.option(op._1, op._2) }

      // add df
      val df = reader.load()
      ir._addDataFrame(df)
    }
  }
}
