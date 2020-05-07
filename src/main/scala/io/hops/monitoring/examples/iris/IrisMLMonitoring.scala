package io.hops.monitoring.examples.iris

import io.hops.monitoring.utils.Constants.Stats.Descriptive._
import io.hops.monitoring.utils.{DataFrameUtil, LoggerUtil}
import io.hops.monitoring.window.WindowSetting
import io.hops.monitoring.io.dataframe.DataFrameSource._
import io.hops.monitoring.io.file.FileSink._
import io.hops.monitoring.stats.DatasetStats
import io.hops.monitoring.pipeline.PipelineManager
import io.hops.util.Hops
import org.apache.spark.SparkConf
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds}

/**
 * Example that uses HopsUtil and Spark-Kafka direct streaming with Avro.
 * Runs a consumer that counts the number of predictions and write logs in parquet format
 */
object IrisMLMonitoring {

  // constants
  private val ResourcesDir = "/Projects/" + Hops.getProjectName + "/Resources/Iris/"
  private val TrainDatasetName = "iris_train_dataset"

  def main(args: Array[String]): Unit = {

    LoggerUtil.log.info(s"[IrisMLMonitoring] Starting job...")

    // Check arguments
    val (kfkTopic, timeout, windowDuration, slideDuration, watermarkDelay) = checkArguments(args)

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
      .select(from_avro(col("value"), kfkSchema) as 'logs) // parse avro

    // Manually parse schema:
    // NOTE: RequestTimestamp has to be divided by 1000. Issue in spark-avro: https://github.com/databricks/spark-avro/issues/229
    val instancesDF = logsDF
      .select(col("logs.requestTimestamp") as 'requestTimestamp, from_json(col("logs.inferenceRequest"), kfkReqStructSchema) as 'requests) // add timestamp and parse json request
      .select(col("requestTimestamp").divide(1000).cast("timestamp") as 'requestTimestampSecs, col("requests.instances") as 'instances) // extract instances
      .drop("requestTimestamp")
      .withColumn("instance", explode(col("instances"))).drop("instances") // explode instances

    // Parameters
    val timeColumnName = "requestTimestampSecs"
    val cols = kfkInstanceStructSchema.map(_.name)
    val stats = Array(Min, Max, Mean, Avg, Count, Stddev)
    val outlierStats = Array(Min, Max, Mean, Stddev)

    // Monitor
    val mdf = instancesDF.monitor
      .withSchema("instance", kfkInstanceStructSchema) // parse request(Array(Double)) to request(Schema)

    // Windowed stats
    val wdf = mdf
      .window(timeColumnName, WindowSetting(windowDuration, slideDuration, watermarkDelay)) // Using manual settings
    //      .window(timeColumnName, WindowSetting(minSize = 200)) // Using min sample size

    val sdf = wdf.stats(cols, stats)

    // Stream writer
    val statsPipeline = sdf
      .output("StatsPipeline")
      .parquet(s"$kfkTopic-stats", ResourcesDir)

    // Outliers
    val outliersPipeline = sdf
      .outliers(cols, outlierStats, DatasetStats(getDescStats))
      .output("OutliersPipeline")
      .parquet(s"$kfkTopic-outliers", ResourcesDir)

    // Execute queries
    PipelineManager
      .init(spark)
      .awaitAll(Seq(statsPipeline, outliersPipeline), timeout)

    // Close session
    LoggerUtil.log.info("[IrisMLMonitoring] Shutting down spark job...")
    spark.close()
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

  //////////////////////////////
  // TEMPORARY HARD_CODE
  //////////////////////////////

  def getSchemas: (String, String) = {

    // TODO: Receive from kafka topic schema.

    val kfkInstanceSchema =
      """{
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
    val kfkReqSchema =
      """{
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

  def getDescStats: Map[String, Map[String, Double]] = {

    // TODO: Download from feature store.
    // NOTE: Issue with hops-util for scala

    Map(
      "species" -> Map(
        Count -> 120.0,
        Mean -> 1.0,
        Stddev -> 0.84016806,
        Min -> 0.0,
        Max -> 2.0
      ),
      "petal_width" -> Map(
        Count -> 120.0,
        Mean -> 1.1966667,
        Stddev -> 0.7820393,
        Min -> 0.1,
        Max -> 2.5
      ),
      "petal_length" -> Map(
        Count -> 120,
        Mean -> 3.7391667,
        Stddev -> 1.8221004,
        Min -> 1.0,
        Max -> 6.9
      ),
      "sepal_width" -> Map(
        Count -> 120.0,
        Mean -> 3.065,
        Stddev -> 0.42715594,
        Min -> 2.0,
        Max -> 4.4
      ),
      "sepal_length" -> Map(
        Count -> 120.0,
        Mean -> 5.845,
        Stddev -> 0.86857843,
        Min -> 4.4,
        Max -> 7.9
      )
    )
  }
}
