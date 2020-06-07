package io.hops.ml.monitoring.job

import java.util.UUID

import io.hops.ml.monitoring.job.config.{Config, KafkaConfig, SinkConfig}
import io.hops.ml.monitoring.utils.Constants
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingDataFrame {

  // Read

  def read(spark: SparkSession, config: Config): DataFrame = {
    readFromKafka(spark, config.jobConfig.source.kafka)
  }

  def readFromKafka(spark: SparkSession, kc: KafkaConfig): DataFrame = {
    spark.readStream.format(Constants.Kafka.Kafka)
      .option(Constants.Kafka.BootstrapServers, kc.brokers)
      .option(Constants.Kafka.Subscribe, kc.topic.name)
      .option(Constants.Kafka.StartingOffsets, "earliest")
      .load()
  }

  // Write

  def write(df: DataFrame, config: SinkConfig): StreamingQuery = {
    writeToKafka(df, config.kafka)
  }

  def writeToKafka(df: DataFrame, kc: KafkaConfig): StreamingQuery = {
    val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
    df.select(to_json(struct(df.columns.map(col): _*)) as 'value) // kafka value format
      .writeStream
      .outputMode(OutputMode.Append())
      .format(Constants.Kafka.Kafka)
      .option(Constants.File.CheckpointLocation, checkpointLocation)
      .option(Constants.Kafka.BootstrapServers, kc.brokers)
      .option(Constants.Kafka.Topic, kc.topic.name)
      .start()
  }

}
