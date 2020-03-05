package io.hops.monitoring.examples

import io.hops.monitoring.ModelMonitor
import io.hops.monitoring.io.dataframe.DataFrameReader._
import io.hops.monitoring.io.file.FileWriter._
import io.hops.monitoring.io.file.FileReader._
import io.hops.monitoring.io.kafka.KafkaReader._
import io.hops.monitoring.streams.manager.StreamManager
import io.hops.monitoring.streams.writer.StreamWriter
import io.hops.monitoring.util.Constants.Stats._
import io.hops.monitoring.util.{DataFrameUtil, LoggerUtil}
import io.hops.monitoring.window.{WindowSetting, WindowStreamResolver}
import io.hops.util.Hops
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds}

/*
  |SPARK STREAMING APPROACHES|
  Spark Structured Streaming (Dataset[T]) (https://spark.apache.org/docs/2.4.3/structured-streaming-programming-guide.html)
  Spark Structured Streaming + Kafka (https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html)
  Spark Streaming (DStream: Seq(RDD)) (https://spark.apache.org/docs/2.4.3/streaming-programming-guide.html)
  Spark Streaming + Kafka (https://spark.apache.org/docs/2.4.3/streaming-kafka-0-10-integration.html)
*/

/**
 * Example that uses HopsUtil and Spark-Kafka direct streaming with Avro.
 * Runs a consumer that counts the number of predictions and write logs in parquet format
 */
object IrisMLMonitoringStructured {

  // constants
  private val ResourcesDir = "/Projects/" + Hops.getProjectName + "/Resources/Iris/"
  private val TrainDatasetName = "iris_train_dataset"

  def main(args: Array[String]): Unit = {

    LoggerUtil.log.info(s"[IrisMLMonitoringStructured] Starting job...")

    // Check arguments
    val (kfkTopic, jobTimeout, windowDuration, slideDuration, watermarkDelay) = checkArguments(args)

    // Streaming context
    val spark = createSparkSession()

    // Get topic schema
    val kfkSchema = Hops.getSchema(kfkTopic)

    // Get schemas
    // NOTE: This should be obtained from kafka topic schema
    val (kfkInstanceSchema, kfkReqSchema) = getSchemas
    val kfkReqStructSchema = DataFrameUtil.Schemas.structType(kfkReqSchema)
    val kfkInstanceStructSchema = DataFrameUtil.Schemas.structType(kfkInstanceSchema)

    // Manually read from Kafka
    val logsDF = createDF(spark, kfkTopic)
      .select(from_avro(col("value"), kfkSchema) as 'logs ) // parse avro

    // Manually parse schema:
    // NOTE: RequestTimestamp has to be divided by 1000. Issue in spark-avro: https://github.com/databricks/spark-avro/issues/229
    val instancesDF = logsDF
      .select(col("logs.requestTimestamp") as 'requestTimestamp, from_json(col("logs.inferenceRequest"), kfkReqStructSchema) as 'requests) // add timestamp and parse json request
      .select(col("requestTimestamp").divide(1000).cast("timestamp") as 'requestTimestampSecs, col("requests.instances") as 'instances) // extract instances
      .drop("requestTimestamp")
      .withColumn("instance", explode(col("instances"))).drop("instances") // explode instances

    // Parameters
    val timeColumnName = "requestTimestampSecs"
    val colNames = kfkInstanceStructSchema.map(_.name)
    val stats = Array(Min, Max, Mean, Avg, Count, Stddev)

    // Monitor
    val mdf = instancesDF.monitor
      .withSchema("instance", kfkInstanceStructSchema)  // parse request(Array(Double)) to request(Schema)

    // Windowed stats
    val sdf = mdf
      .window(timeColumnName, WindowSetting(windowDuration, slideDuration, watermarkDelay)) // Manual window
//      .window(timeColumnName, sampleSize = 100, logsPath=s"$ResourcesDir$kfkTopic") // Sample size based
//      .window(timeColumnName, defaultResolver, logsPath=s"$ResourcesDir$kfkTopic") // Auto
      .stats(colNames, stats)

    // Stream writer
    val ssw = sdf
      .output("StatsStreamingQuery")
      .parquet(kfkTopic, ResourcesDir)

    // Alerts
    val asw = sdf
      .watch(colNames)
      .unseen(TrainDatasetName)
      .output("AlertsStreamingQuery")
      .parquet(s"$kfkTopic-alerts", ResourcesDir)

    // Execute queries
    StreamManager
      .init(spark)
      .awaitAll(Seq(ssw, asw), jobTimeout)

    // Close session
    LoggerUtil.log.info("[IrisMLMonitoringStructured] Shutting down spark job...")
    spark.close()
  }

  private def defaultResolver: (WindowSetting, Long) => WindowSetting = (setting: WindowSetting, rps: Long) => {
    LoggerUtil.log.info(s"[IrisMLMonitoringStructured] Default resolver: Setting: [$setting] and RPS ($rps)")

//    val sampleSize = 200
//    val duration = sampleSize / rps
//    val slideDuration = duration * 0.1.toLong
//    val watermarkDelay = duration * 0.4.toLong

    val duration = 6
    val slideDuration = 3
    val watermarkDelay = 4

    // Window estimation
    LoggerUtil.log.info(s"[IrisMLMonitoringStructured] Default resolver: Decision ($duration, $slideDuration, $watermarkDelay)")
    WindowSetting(Seconds(duration), Seconds(slideDuration), Seconds(watermarkDelay))
  }

  def createDF(spark: SparkSession, kfk_topic: String): DataFrame = {
    spark.readStream.format("kafka").
      option("kafka.bootstrap.servers", Hops.getBrokerEndpoints).
      option("subscribe", kfk_topic).
      option("startingOffsets", "earliest").
      option("kafka.security.protocol", "SSL").
      option("kafka.ssl.truststore.location", Hops.getTrustStore).
      option("kafka.ssl.truststore.password", Hops.getKeystorePwd).
      option("kafka.ssl.keystore.location", Hops.getKeyStore).
      option("kafka.ssl.keystore.password", Hops.getKeystorePwd).
      option("kafka.ssl.key.password", Hops.getKeystorePwd).
      option("kafka.ssl.endpoint.identification.algorithm", "").
      load()
  }

  def createSparkSession(): SparkSession = {
    val sparkConf: SparkConf = new SparkConf()
    SparkSession
      .builder()
      .appName(Hops.getJobName)
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def checkArguments(args: Array[String]): (String, Duration, Duration, Duration, Duration) = {
    if (args.length != 5) {
      LoggerUtil.log.error("Arguments missing. Five arguments required: Topic name, job timeout (secs), window duration (ms), slide duration (ms), watermark delay (ms)")
      System.exit(1)
    }
    (args(0), Seconds(args(1).toLong), Milliseconds(args(2).toLong), Milliseconds(args(3).toLong), Milliseconds(args(4).toLong))
  }

  def getSchemas: (String, String) = {
    val kfkInstanceSchema = """{
                              |  "type" : "struct",
                              |  "fields" : [ {
                              |    "name" : "sepal_length",
                              |    "type" : "double",
                              |    "nullable" : true,
                              |    "metadata" : { }
                              |  }, {
                              |    "name" : "sepal_width",
                              |    "type" : "double",
                              |    "nullable" : true,
                              |    "metadata" : { }
                              |  }, {
                              |    "name" : "petal_length",
                              |    "type" : "double",
                              |    "nullable" : true,
                              |    "metadata" : { }
                              |  }, {
                              |    "name" : "petal_width",
                              |    "type" : "double",
                              |    "nullable" : true,
                              |    "metadata" : { }
                              |  } ]
                              |}""".stripMargin
    val kfkReqSchema = """{
                         |    "fields": [
                         |        {
                         |            "metadata": {},
                         |            "name": "signature_name",
                         |            "nullable": true,
                         |            "type": "string"
                         |        },
                         |        {
                         |            "metadata": {},
                         |            "name": "instances",
                         |            "nullable": true,
                         |            "type": {
                         |                "containsNull": true,
                         |                "elementType": {
                         |                    "containsNull": true,
                         |                    "elementType": "double",
                         |                    "type": "array"
                         |                },
                         |                "type": "array"
                         |            }
                         |        }
                         |    ],
                         |    "type": "struct"
                         |}""".stripMargin

    (kfkInstanceSchema, kfkReqSchema)
  }
}
