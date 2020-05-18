package io.hops.monitoring.examples.iris

import io.hops.monitoring.drift.detectors.{JensenShannonDetector, KullbackLeiblerDetector, WassersteinDetector}
import io.hops.monitoring.io.dataframe.DataFrameSource._
import io.hops.monitoring.io.file.FileSink._
import io.hops.monitoring.outliers.detectors.DescriptiveStatsDetector
import io.hops.monitoring.pipeline.PipelineManager
import io.hops.monitoring.stats.Baseline
import io.hops.monitoring.stats.definitions.Corr.CorrType
import io.hops.monitoring.stats.definitions.Cov.CovType
import io.hops.monitoring.stats.definitions.Stddev.StddevType
import io.hops.monitoring.stats.definitions._
import io.hops.monitoring.utils.Constants.Stats.Descriptive
import io.hops.monitoring.utils.{DataFrameUtil, LoggerUtil}
import io.hops.monitoring.window.WindowSetting
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
  //  private val TrainDatasetName = "iris_train_dataset"

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
    val (kfkInstanceSchema, kfkReqSchema) = IrisMLSchemas.getSchemas
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

    // Schema
    val timeColumnName = "requestTimestampSecs"
    val cols = kfkInstanceStructSchema.map(_.name)

    // Define baseline
    val baseline = Baseline(IrisMLBaseline.getDescriptiveStats, IrisMLBaseline.getFeatureDistributions)

    // Stats
    val stats = Seq(
      Min(), Max(), Mean(), Avg(), Count(),
      Stddev(StddevType.SAMPLE),
      Distr(baseline.getDistributionsBounds),
      Cov(CovType.SAMPLE),
      Corr(CorrType.SAMPLE),
      Perc(Seq(25,50,75), iqr = true))

    // Monitor
    val mdf = instancesDF.monitor
      .withSchema("instance", kfkInstanceStructSchema) // parse request(Array(Double)) to request(Schema)

    // Window pipe
    val wdf = mdf
      .window(timeColumnName, WindowSetting(windowDuration, slideDuration, watermarkDelay))

    // Stats pipe
    val sdf = wdf.stats(cols, stats)
    val statsPipeline = sdf
      .output("StatsPipeline")
      .parquet(s"$kfkTopic-stats", ResourcesDir)

    // Outlier pipe (from statistics)
    val outlierDetectors = Seq(new DescriptiveStatsDetector(Seq(Descriptive.Min, Descriptive.Max, Descriptive.Avg, Descriptive.Stddev)))
    val outliersPipeline = sdf
      .outliers(outlierDetectors, baseline)
      .output("OutliersPipeline")
      .parquet(s"$kfkTopic-outliers", ResourcesDir)

    // Drift pipe (from statistics)
    val driftDetectors = Seq( // define drift detectors
      new WassersteinDetector(threshold = 2.7, showAll = true),
      new KullbackLeiblerDetector(threshold = 1.3, showAll = true),
      new JensenShannonDetector(threshold = 0.5, showAll = true))
    val driftPipeline = sdf
      .drift(driftDetectors, baseline)
      .output("DriftPipeline")
      .parquet(s"$kfkTopic-drift", ResourcesDir)

    // Execute queries
    PipelineManager
      .init(spark)
      .awaitAll(Seq(statsPipeline, outliersPipeline, driftPipeline), timeout)

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
}
