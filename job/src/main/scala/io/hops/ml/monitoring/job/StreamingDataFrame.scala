package io.hops.ml.monitoring.job

import java.util.UUID

import io.hops.ml.monitoring.job.config.{Config, StreamConnectorConfig}
import io.hops.ml.monitoring.job.utils.Constants.Kafka
import io.hops.ml.monitoring.utils.Constants
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingDataFrame {

  // Read

  def read(spark: SparkSession, config: Config): DataFrame = {
    config.jobConfig.source.format match {
      case Constants.Kafka.Kafka => readFromKafka(spark, config.jobConfig.source)
      case _ => throw new Exception(s"Source format '${config.jobConfig.source.format}' not recognized")
    }
  }

  def readFromKafka(spark: SparkSession, scc: StreamConnectorConfig): DataFrame = {
    spark.readStream.format(Constants.Kafka.Kafka)
      .option(Constants.Kafka.BootstrapServers, scc.params(Kafka.Brokers))
      .option(Constants.Kafka.Subscribe, scc.params(Constants.Kafka.Topic))
      .option(Constants.Kafka.StartingOffsets, "earliest")
      .load()
  }

  // Write

  def write(df: DataFrame, config: Config): StreamingQuery = {
    config.jobConfig.sink.format match {
      case Constants.Kafka.Kafka => writeToKafka(df, config.jobConfig.sink)
      case _ => throw new Exception(s"Sink format '${config.jobConfig.sink.format}' not recognized")
    }
  }

  def writeToKafka(df: DataFrame, scc: StreamConnectorConfig): StreamingQuery = {
    val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
    df.select(to_json(struct(df.columns.map(col): _*)) as 'value) // kafka value format
      .writeStream
      .outputMode(OutputMode.Append())
      .format(Constants.Kafka.Kafka)
      .option(Constants.File.CheckpointLocation, checkpointLocation)
      .option(Constants.Kafka.BootstrapServers, scc.params(Kafka.Brokers))
      .option(Constants.Kafka.Topic, scc.params(Constants.Kafka.Topic))
      .start()
  }

}
