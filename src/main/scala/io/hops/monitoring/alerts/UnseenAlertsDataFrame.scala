package io.hops.monitoring.alerts

import io.hops.monitoring.streams.resolver.{ResolvableDataFrame, StreamResolverSignature}
import io.hops.monitoring.streams.writer.StreamWriter
import io.hops.monitoring.util.Constants.Stats.{Max, Mean, Min, StatColName, Stddev, Watchable}
import io.hops.monitoring.util.Constants.Watcher.{FeatureColName, ThresholdColName, WatchedColName}
import io.hops.monitoring.util.Constants.Window.WindowColName
import io.hops.monitoring.util.DataFrameUtil.Encoders
import io.hops.monitoring.util.LoggerUtil
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

class UnseenAlertsDataFrame(df: DataFrame, knownDataFrameName: String, colNames: Seq[String], stats: Seq[String], signatures: Option[Seq[StreamResolverSignature]])
  extends ResolvableDataFrame with java.io.Serializable {

  // Variables

  private val schema = df.schema
  private val schemaColIndexMap = (WindowColName +: schema.fieldNames.filter(fn => colNames.contains(fn)) :+ StatColName).zipWithIndex.toMap
  private val watchableStats = stats.filter(s => Watchable.contains(s)) // Filter watchable stats
  private val descStats: Map[String, Map[String, Float]] = getKnownDataFrameStats // 'count', 'mean', 'stddev', 'min', 'max'

  private val watcherSchemaFields = Seq(schema.find(field => field.name == WindowColName).get,
    StructField(FeatureColName, StringType),
    StructField(WatchedColName, FloatType), StructField(ThresholdColName, FloatType),
    StructField(StatColName, StringType))
  private val watcherSchema = StructType(watcherSchemaFields)

  // Stats

  def getKnownDataFrameStats(): Map[String, Map[String, Float]] = {
    LoggerUtil.log.info(s"[AlertsDataFrame] Get training dataset $knownDataFrameName")

    // Download descriptive stats
    //    val tds = Hops.getTrainingDataset(trainDatasetName)
    //    val stats = tds.getStatisticsDTO()
    //    val descStats = stats.getDescriptiveStatsDTO()
    //    val descStatsList = descStats.getDescriptiveStats()
    //    descStatsList.asScala

    // TEMPORARY PATCH
    val descStatsList = mockDescStatsMap

    // Filter by col names
    descStatsList.filter(stat => colNames.contains(stat._1))
  }

  private def buildCdf: DataFrame = {
    val rowEncoder = Encoders.rowEncoder(watcherSchema)
    df.select((col(WindowColName) +: colNames.map(colName => col(colName)) :+ col(StatColName)):_*) // select cols
      .filter(row => watchableStats.contains(row.getAs[String](schemaColIndexMap(StatColName)))) // filter stat rows
      .flatMap(row => {
        val rowStat = row.getAs[String](schemaColIndexMap(StatColName))
        if (watchableStats.contains(rowStat)) {
          descStats.flatMap(featureStats => checkMetric(row, rowStat, featureStats))
        } else { None }
      })(rowEncoder)
  }

  def checkMetric(row: Row, stat: String, featureStats: (String, Map[String, Float])): Option[Row] = {
    val colName = featureStats._1
    val statValue = featureStats._2.find(featureStat => featureStat._1 == stat).get._2
    val watchedStatValue = row.getAs[Float](schemaColIndexMap(colName))
    if (isUnseenStat(stat, statValue, watchedStatValue)) {
      LoggerUtil.log.info(s"[AlertsDataFrame] Alert for $colName: Stat $stat with threshold $statValue observed with value $watchedStatValue")
      val rowValues = Seq(row.getAs[Row](schemaColIndexMap(WindowColName)), colName, watchedStatValue, statValue, stat)
      Some(Row(rowValues:_*))
    } else { None }
  }

  def isUnseenStat(stat: String, statValue: Float, watchedStatValue: Float): Boolean = {
    stat match {
      case Min => watchedStatValue < statValue
      case Max => watchedStatValue > statValue
      case Mean => (watchedStatValue > statValue + statValue*0.2) || (watchedStatValue < statValue - statValue*0.2)
      case Stddev => (watchedStatValue > statValue + statValue*0.2) || (watchedStatValue < statValue - statValue*0.2)
    }
  }

  // Output

  def output(queryName: String): StreamWriter = {
    val cdf = buildCdf
    new StreamWriter(cdf, queryName, getSignatures)
  }

  //////////////////////////////
  // TEMPORARY HARD_CODE
  //////////////////////////////

  def mockDescStatsMap: Map[String, Map[String, Float]] = {
    Map(
      "species" -> Map(
        "count" -> 120.toFloat,
        "mean" -> 1.toFloat,
        "stddev" -> 0.84016806.toFloat,
        "min" -> 0.toFloat,
        "max" -> 2.toFloat
      ),
      "petal_width" -> Map(
        "count" -> 120.toFloat,
        "mean" -> 1.1966667.toFloat,
        "stddev" -> 0.7820393.toFloat,
        "min" -> 0.1.toFloat,
        "max" -> 2.5.toFloat
      ),
      "petal_length" -> Map(
        "count" -> 120.toFloat,
        "mean" -> 3.7391667.toFloat,
        "stddev" -> 1.8221004.toFloat,
        "min" -> 1.toFloat,
        "max" -> 6.9.toFloat
      ),
      "sepal_width" -> Map(
        "count" -> 120.toFloat,
        "mean" -> 3.065.toFloat,
        "stddev" -> 0.42715594.toFloat,
        "min" -> 2.toFloat,
        "max" -> 4.4.toFloat
      ),
      "sepal_length" -> Map(
        "count" -> 120.toFloat,
        "mean" -> 5.845.toFloat,
        "stddev" -> 0.86857843.toFloat,
        "min" -> 	4.4.toFloat,
        "max" -> 	7.9.toFloat
      )
    )
  }
}
