package io.hops.monitoring.experiments

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.{Analysis, InMemoryStateProvider, StandardDeviation, Size, Maximum, Minimum, Mean}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.CheckStatus
import io.hops.monitoring.io.dataframe.DataFrameSource._
import io.hops.monitoring.utils.Constants.File
import io.hops.monitoring.utils.Constants.Stats.Descriptive._
import io.hops.monitoring.utils.{DataFrameUtil, LoggerUtil}
import io.hops.monitoring.window.WindowSetting
import io.hops.util.Hops
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.{col, explode, from_json, lit, current_timestamp}
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
object IrisMLDeequStats {

  // constants
  private val ResourcesDir = "/Projects/" + Hops.getProjectName + "/Resources/Iris/"

  def main(args: Array[String]): Unit = {

    LoggerUtil.log.info(s"[DeequForeachBatch] Starting job...")

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
    val wdf = mdf
      .window(timeColumnName, WindowSetting(windowDuration, slideDuration, watermarkDelay))

    // Deequ

    // create a stateStore to hold our stateful metrics
    val stateStoreCurr = InMemoryStateProvider()
    val stateStoreNext = InMemoryStateProvider()

    // create the analyzer to run on the streaming data
    val analysis = Analysis()
      .addAnalyzers(Seq(Size(), Minimum(colNames.head), Maximum(colNames.head), Mean(colNames.head), StandardDeviation(colNames.head)))
      .addAnalyzers(Seq(Size(), Minimum(colNames(1)), Maximum(colNames(1)), Mean(colNames(1)), StandardDeviation(colNames(1))))
      .addAnalyzers(Seq(Size(), Minimum(colNames(2)), Maximum(colNames(2)), Mean(colNames(2)), StandardDeviation(colNames(2))))
      .addAnalyzers(Seq(Size(), Minimum(colNames(3)), Maximum(colNames(3)), Mean(colNames(3)), StandardDeviation(colNames(3))))

    // Stream writer
    val ssw = wdf
      .df(colNames)
      .writeStream
      .queryName("DeequStats")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
//        batchDF.persist()

        val fullPath = ResourcesDir + "deequestats" + "-" + File.ParquetFormat
        val checkpointLocation = fullPath + "-checkpoint"

        // reassign our current state to the previous next state
        val stateStoreCurr = stateStoreNext

        // run our analysis on the current batch, aggregate with saved state
        val metricsResult = AnalysisRunner.run(
          data = batchDF,
          analysis = analysis,
          aggregateWith = Some(stateStoreCurr),
          saveStatesWith = Some(stateStoreNext))

        // verify critical metrics for this microbatch
        val verificationResult = VerificationSuite()
          .onData(batchDF)
          .run()

        // if verification fails, write batch to bad records table
        if (verificationResult.status != CheckStatus.Success) {
          batchDF.withColumn("batchID", lit(batchId))
            .write
            .format(File.ParquetFormat)
            .option(File.Path, fullPath)
            .option(File.CheckpointLocation, checkpointLocation)
            .mode("append")
//            .saveAsTable("bad_records")
            .save()
        }

        // get the current metrics as a dataframe
        val metric_results = successMetricsAsDataFrame(spark, metricsResult)
          .withColumn("ts", current_timestamp())

        // write the current results into the metrics table
        metric_results.write
          .format(File.ParquetFormat)
          .option(File.Path, fullPath + "-results")
          .option(File.CheckpointLocation, checkpointLocation + "-results")
          .mode("Overwrite")
//          .saveAsTable("deequ_metrics")
          .save()

        batchDF.write
          .format(File.ParquetFormat)
          .option(File.Path, fullPath + "-raw")
          .option(File.CheckpointLocation, checkpointLocation + "-raw")
          .save()

//        batchDF.unpersist()
      })

    LoggerUtil.log.info("[DeequForeachBatch] Starting query...")
    ssw.start.awaitTermination(jobTimeout.milliseconds)

    // Close session
    LoggerUtil.log.info("[DeequForeachBatch] Shutting down spark job...")
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
