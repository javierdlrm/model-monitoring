package io.hops.ml.monitoring.job

import java.util.UUID

import io.hops.ml.monitoring.io.dataframe.DataFrameSource._
import io.hops.ml.monitoring.io.kafka.KafkaSettings
import io.hops.ml.monitoring.job.config.Config
import io.hops.ml.monitoring.job.config.monitoring.{BaselineConfig, DriftConfig, OutliersConfig}
import io.hops.ml.monitoring.job.config.storage.{AnalysisSinkConfig, SinkConfig}
import io.hops.ml.monitoring.monitor.MonitorPipe
import io.hops.ml.monitoring.pipeline.{Pipeline, PipelineManager}
import io.hops.ml.monitoring.stats.StatsPipe
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.utils.RichOption._
import io.hops.ml.monitoring.utils.{Constants, DataFrameUtil, LoggerUtil}
import io.hops.ml.monitoring.window.{WindowPipe, WindowSetting}
import org.apache.spark.SparkConf
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Milliseconds, Seconds}

object Monitor {
  def main(args: Array[String]) {

    val confFile = parseArguments(args)

    // Prepare spark session and config
    val config = if (confFile.isDefined) Config.getFromFile(confFile.get) else Config.getFromEnv
    val spark = createSparkSession()

    // Shortcuts
    val trigger = config.monitoringConfig.trigger
    val stats = config.monitoringConfig.stats.definitions
    val outliers = config.monitoringConfig.outliers
    val drift = config.monitoringConfig.drift
    val baseline = config.monitoringConfig.baseline
    val inference = config.storageConfig.inference
    val analysis = config.storageConfig.analysis

    // Schemas
    val messageSchema = Hops.getSchema(inference.kafka.topic.name)
    val requestSchema = DataFrameUtil.Schemas.structType(config.modelInfo.schemas.request)
    val instanceSchema = DataFrameUtil.Schemas.structType(config.modelInfo.schemas.instance)
    val cols = instanceSchema.map(_.name) // all feature names

    // Load streaming df
    val logsDF = readDF(spark, inference)
      .select(from_avro(col("value"), messageSchema) as 'logs) // parse message

    // Requests df
    val timeColumnName = "timestamp"
//    val requestCondition = col(s"logs.messageType") === Schemas.Request
    val requestsDF = logsDF//.filter(requestCondition)
      .select(col(s"logs.requestTimestamp").divide(1000).cast("timestamp") as timeColumnName, from_json(col(s"logs" +
        s".inferenceRequest"), requestSchema) as 'requests) // add timestamp and parse json payload
      .select(col(timeColumnName), col("requests.instances") as 'instances) // extract requests
      .withColumn("instance", explode(col("instances"))).drop("instances") // explode instances

    // Initialize monitor
    val monitorPipe = requestsDF.monitor.withSchema("instance", instanceSchema)

    // Window pipe
    val windowPipe = monitorPipe.window(timeColumnName, WindowSetting(
      Milliseconds(trigger.window.duration),
      Milliseconds(trigger.window.slide),
      Milliseconds(trigger.window.watermarkDelay)))

    // Outlier pipe (distance-based)
    val outliersPipeline = buildOutliersPipeline(monitorPipe, cols, outliers, analysis.outliers, baseline)

    // Stats pipe
    val (statsPipe, statsPipeline) = buildStatsPipeline(windowPipe, cols, stats, analysis.stats)

    // Drift pipe (distribution-based)
    val driftPipeline = buildDriftPipeline(statsPipe, drift, analysis.drift, baseline)

    // All pipelines
    var pipelines = Seq(statsPipeline)
    outliersPipeline ! { op => pipelines = pipelines :+ op }
    driftPipeline ! { dp => pipelines = pipelines :+ dp }

    // Run and await all
    val manager = PipelineManager.init(spark)
    if (config.jobConfig.isDefined && config.jobConfig.get.timeout.isDefined) {
      manager.awaitAll(pipelines, Seconds(config.jobConfig.get.timeout.get))
    } else {
      manager.awaitAll(pipelines)
    }

    LoggerUtil.log.info("[Monitor] Shutting down spark job...")
    spark.stop()
    spark.close()
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

  def readDF(spark: SparkSession, config: SinkConfig): DataFrame = {
    spark.readStream.format(Constants.Kafka.Kafka).
      option("kafka.bootstrap.servers", Hops.getBrokerEndpoints).
      option("subscribe", config.kafka.topic.name).
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

  def buildStatsPipeline(windowPipe: WindowPipe, cols: Seq[String], stats: Seq[StatDefinition], sink: AnalysisSinkConfig): (StatsPipe, Pipeline) = {
    import io.hops.ml.monitoring.io.file.FileSink._

    val statsPipe = windowPipe.stats(cols, stats)
    val statsPipeline = statsPipe
      .output("StatsPipeline")
      .parquet(sink.parquet.files.prefix, s"/Projects/${Hops.getProjectName}${sink.parquet.directory}")

    (statsPipe, statsPipeline)
  }

  def buildOutliersPipeline(stats: MonitorPipe, cols: Seq[String], outliers: Option[OutliersConfig], sink: Option[SinkConfig], baseline: Option[BaselineConfig]): Option[Pipeline] = {
    import io.hops.ml.monitoring.io.kafka.KafkaSink._

    if (outliers.isEmpty || outliers.get.valuesBased.isEmpty) return None
    if (baseline.isEmpty || sink.isEmpty) {
      return None
    }

    Some(stats
      .outliers(cols, outliers.get.valuesBased, baseline.get.map)
      .output("OutliersPipeline")
      .kafka(KafkaSettings(
        bootstrapServers = Hops.getBrokerEndpoints,
        topic = Some(sink.get.kafka.topic.name),
        securityProtocol = Some("SSL"),
        sslTruststoreLocation = Some(Hops.getTrustStore),
        sslTruststorePassword = Some(Hops.getKeystorePwd),
        sslKeystoreLocation = Some(Hops.getKeyStore),
        sslKeystorePassword = Some(Hops.getKeystorePwd),
        sslKeyPassword = Some(Hops.getKeystorePwd),
        sslEndpointIdentificationAlgorithm = Some("")),
        s"/Projects/${Hops.getProjectName}/Resources/Sales/Checkpoints/checkpoint-outliers-" + UUID.randomUUID.toString))
  }

  def buildDriftPipeline(stats: StatsPipe, drift: Option[DriftConfig], sink: Option[SinkConfig], baseline: Option[BaselineConfig]): Option[Pipeline] = {
    import io.hops.ml.monitoring.io.kafka.KafkaSink._

    if (drift.isEmpty || drift.get.statsBased.isEmpty) return None
    if (baseline.isEmpty || sink.isEmpty) {
      return None
    }
    Some(stats
      .drift(drift.get.statsBased, baseline.get.map)
      .output("DriftPipeline")
      .kafka(KafkaSettings(
        bootstrapServers = Hops.getBrokerEndpoints,
        topic = Some(sink.get.kafka.topic.name),
        securityProtocol = Some("SSL"),
        sslTruststoreLocation = Some(Hops.getTrustStore),
        sslTruststorePassword = Some(Hops.getKeystorePwd),
        sslKeystoreLocation = Some(Hops.getKeyStore),
        sslKeystorePassword = Some(Hops.getKeystorePwd),
        sslKeyPassword = Some(Hops.getKeystorePwd),
        sslEndpointIdentificationAlgorithm = Some("")),
        s"/Projects/${Hops.getProjectName}/Resources/Sales/Checkpoints/checkpoint-drift-" + UUID.randomUUID.toString))
  }

  def parseArguments(args: Array[String]): Option[String] = {
    // Without arguments, configuration is read from the env vars
    // Only one keyed argument allowed: --conf hdfs_conf_file_path
    if (args.length == 2 && args.head == "--conf") {
      return Some(args.last)
    }
    if (args.length != 0) {
      LoggerUtil.log.error("Invalid arguments. None or one keyed argument allowed: '--conf hdfs_conf_file_path'")
      System.exit(1)
    }
    None
  }
}
