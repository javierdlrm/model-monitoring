package io.hops.ml.monitoring.job

import java.util.UUID

import io.hops.ml.monitoring.io.dataframe.DataFrameSource._
import io.hops.ml.monitoring.io.kafka.KafkaSettings
import io.hops.ml.monitoring.io.kafka.KafkaSink._
import io.hops.ml.monitoring.job.config.Config
import io.hops.ml.monitoring.job.config.monitoring.{BaselineConfig, DriftConfig, OutliersConfig}
import io.hops.ml.monitoring.job.config.storage.SinkConfig
import io.hops.ml.monitoring.job.utils.Constants.{Job, Schemas}
import io.hops.ml.monitoring.monitor.MonitorPipe
import io.hops.ml.monitoring.pipeline.{Pipeline, PipelineManager}
import io.hops.ml.monitoring.stats.StatsPipe
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.utils.RichOption._
import io.hops.ml.monitoring.utils.{Constants, DataFrameUtil, LoggerUtil}
import io.hops.ml.monitoring.window.{WindowPipe, WindowSetting}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Milliseconds, Seconds}

object Monitor {
  def main(args: Array[String]) {

    // Prepare spark session and config
    val config = Config.getFromEnv
    val spark = createSparkSession(config)

    // Shortcuts
    val trigger = config.monitoringConfig.trigger
    val stats = config.monitoringConfig.stats.definitions
    val outliers = config.monitoringConfig.outliers
    val drift = config.monitoringConfig.drift
    val baseline = config.monitoringConfig.baseline
    val inference = config.storageConfig.inference
    val analysis = config.storageConfig.analysis

    // Schemas
    val messageSchema = DataFrameUtil.Schemas.structType[InferenceLoggerMessage]()
    val requestSchema = DataFrameUtil.Schemas.structType(config.modelInfo.schemas.request)
    val instanceSchema = DataFrameUtil.Schemas.structType(config.modelInfo.schemas.instance)
    val cols = instanceSchema.map(_.name) // all feature names

    // Load streaming df
    val logsDF = readDF(spark, inference)
      .select(from_json(col("value").cast("string"), messageSchema) as 'logs) // parse message

    // Requests df
    val timeColumnName = "timestamp"
    val requestCondition = col(s"logs.${Schemas.TypeColName}") === Schemas.Request
    val requestsDF = logsDF.filter(requestCondition)
      .select(col(s"logs.${Schemas.TimeColName}").cast("timestamp") as timeColumnName, from_json(col(s"logs.${Schemas.PayloadColName}"), requestSchema) as 'requests) // add timestamp and parse json payload
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

  def createSparkSession(config: Config): SparkSession = {
    SparkSession
      .builder
      .appName(Job.defaultJobName(config.modelInfo.name))
      .getOrCreate()
  }

  def readDF(spark: SparkSession, config: SinkConfig): DataFrame = {
    spark.readStream.format(Constants.Kafka.Kafka)
      .option(Constants.Kafka.BootstrapServers, config.kafka.brokers)
      .option(Constants.Kafka.Subscribe, config.kafka.topic.name)
      .option(Constants.Kafka.StartingOffsets, "earliest")
      .load()
  }

  def buildStatsPipeline(windowPipe: WindowPipe, cols: Seq[String], stats: Seq[StatDefinition], sink: SinkConfig): (StatsPipe, Pipeline) = {
    val statsPipe = windowPipe.stats(cols, stats)
    val statsPipeline = statsPipe
      .output("StatsPipeline")
      .kafka(KafkaSettings(
        bootstrapServers = sink.kafka.brokers,
        topic = Some(sink.kafka.topic.name)),
        "/tmp/temporary-stats-" + UUID.randomUUID.toString)
    (statsPipe, statsPipeline)
  }

  def buildOutliersPipeline(stats: MonitorPipe, cols: Seq[String], outliers: Option[OutliersConfig], sink: Option[SinkConfig], baseline: Option[BaselineConfig]): Option[Pipeline] = {
    if (outliers.isEmpty || outliers.get.valuesBased.isEmpty) return None
    if (baseline isEmpty) {
      LoggerUtil.log.info("[Monitor] Ignoring outliers detection. Baseline is required")
      return None
    }
    if (sink isEmpty) {
      LoggerUtil.log.info("[Monitor] Ignoring outliers detection. Storage definition is required")
      return None
    }

    Some(stats
      .outliers(cols, outliers.get.valuesBased, baseline.get.map)
      .output("OutliersPipeline")
      .kafka(KafkaSettings(
        bootstrapServers = sink.get.kafka.brokers,
        topic = Some(sink.get.kafka.topic.name)),
        "/tmp/temporary-outliers-" + UUID.randomUUID.toString))
  }

  def buildDriftPipeline(stats: StatsPipe, drift: Option[DriftConfig], sink: Option[SinkConfig], baseline: Option[BaselineConfig]): Option[Pipeline] = {
    if (drift.isEmpty || drift.get.statsBased.isEmpty) return None
    if (baseline isEmpty) {
      LoggerUtil.log.info("[Monitor] Ignoring drift detection. Baseline is required")
      return None
    }
    if (sink isEmpty) {
      LoggerUtil.log.info("[Monitor] Ignoring drift detection. Storage definition is required")
      return None
    }
    Some(stats
      .drift(drift.get.statsBased, baseline.get.map)
      .output("DriftPipeline")
      .kafka(KafkaSettings(
        bootstrapServers = sink.get.kafka.brokers,
        topic = Some(sink.get.kafka.topic.name)),
        "/tmp/temporary-drift-" + UUID.randomUUID.toString))
  }
}
