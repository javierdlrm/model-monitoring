package io.hops.ml.monitoring.job

import io.hops.ml.monitoring.job.config.Config
import io.hops.ml.monitoring.job.utils.Constants.{Job, Schemas}
import io.hops.ml.monitoring.utils.DataFrameUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Monitor {
  def main(args: Array[String]) {

    // Prepare spark session and config
    val config = Config.getFromEnv
    val spark = createSparkSession(config)

    // Prepare schemas
    val schema = DataFrameUtil.Schemas.structType[InferenceLoggerSchema]()
    val requestSchema = DataFrameUtil.Schemas.structType(config.inferenceSchemas.request)

    // Load streaming df
    val logsDF = StreamingDataFrame.read(spark, config)
      .select(from_json(col("value").cast("string"), schema) as 'logs) // parse message

    // Filter requests df
    val requestCondition = col(s"logs.${Schemas.TypeColName}") === Schemas.Request
    val requestsDF = logsDF.filter(requestCondition)
      .select(col(s"logs.${Schemas.TimeColName}") as 'timestamp, from_json(col(s"logs.${Schemas.PayloadColName}"), requestSchema) as 'requests) // add timestamp and parse json payload
      .select(col("timestamp").cast("timestamp"), col("requests.instances") as 'instances) // extract requests
      .withColumn("instance", explode(col("instances"))).drop("instances") // explode instances
      .select(col("timestamp"), col("instance.*"))

    // Write requests to kafka
    val sq = StreamingDataFrame.write(requestsDF, config)
    sq.awaitTermination(config.jobConfig.timeout)

    spark.stop()
    spark.close()
  }

  def createSparkSession(config: Config): SparkSession = {
    SparkSession
      .builder
      .appName(Job.defaultJobName(config.modelInfo.name))
      .getOrCreate()
  }
}


