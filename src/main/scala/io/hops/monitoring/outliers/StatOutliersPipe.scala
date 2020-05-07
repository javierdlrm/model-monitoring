package io.hops.monitoring.outliers

import io.hops.monitoring.stats.{DatasetStats, FeatureStats}
import io.hops.monitoring.pipeline.SinkPipeJoint
import io.hops.monitoring.utils.Constants.Stats.Descriptive.{Max, Mean, Min, Stddev}
import io.hops.monitoring.utils.Constants.Stats.StatColName
import io.hops.monitoring.utils.Constants.Outliers.{FeatureColName, ThresholdColName, ValueColName}
import io.hops.monitoring.utils.Constants.Window.WindowColName
import io.hops.monitoring.utils.DataFrameUtil.Encoders
import io.hops.monitoring.utils.LoggerUtil

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class StatOutliersPipe(source: DataFrame, cols: Seq[String], stats: Seq[String], baselineStats: DatasetStats)
  extends SinkPipeJoint {

  LoggerUtil.log.info(s"[StatOutliersPipe] Created over columns [${cols.mkString(", ")}]")

  // Variables

  private val schemaColIndexMap = (WindowColName +: source.schema.fieldNames.filter(fn => cols.contains(fn)) :+ StatColName).zipWithIndex.toMap
  private val obsStats = stats.filter(s => baselineStats.stats.contains(s)) // Filter observable stats
  private val obsBaselineStats = baselineStats.filter(cols, obsStats)

  private val outliersSchemaFields = Seq(source.schema.find(field => field.name == WindowColName).get,
    StructField(FeatureColName, StringType),
    StructField(ValueColName, DoubleType), StructField(ThresholdColName, DoubleType),
    StructField(StatColName, StringType))
  private val outliersSchema = StructType(outliersSchemaFields)

  // Stats

  private def detectOutliers(df: DataFrame): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(outliersSchema)
    val statColIndex = schemaColIndexMap(StatColName)

    df.select(col(WindowColName) +: cols.map(colName => col(colName)) :+ col(StatColName): _*) // select cols
      .filter(row => obsStats.contains(row.getAs[String](statColIndex))) // filter stat rows
      .flatMap(row => {
        val rowStat = row.getAs[String](statColIndex)
        obsBaselineStats.featuresStats.flatMap(featureStats => checkStat(row, rowStat, featureStats))
      })(rowEncoder)
  }

  def checkStat(row: Row, stat: String, featureStats: FeatureStats): Option[Row] = {
    val colName = featureStats.name
    val statValue = featureStats.stats.find(featureStat => featureStat.name == stat).get.value
    val obsStatValue = row.getAs[Double](schemaColIndexMap(colName))
    if (isOutlier(stat, statValue, obsStatValue, featureStats)) {

      LoggerUtil.log.info(s"[StatOutliersPipe] Outlier detected on $colName: Stat $stat with threshold $statValue observed with value $obsStatValue")

      val rowValues = Seq(row.getAs[Row](schemaColIndexMap(WindowColName)), colName, obsStatValue, statValue, stat)
      Some(Row(rowValues: _*))
    } else {
      None
    }
  }

  def isOutlier(stat: String, statValue: Double, obsStatValue: Double, featureStats: FeatureStats): Boolean = {
    stat match {
      case Min => obsStatValue < statValue
      case Max => obsStatValue > statValue
      case Mean =>
        val stddev = featureStats.stats.find(featureStat => featureStat.name == Stddev)
        if (stddev isDefined)
          (obsStatValue > statValue + stddev.get.value * 2) || (obsStatValue < statValue - stddev.get.value * 2)
        else
          (obsStatValue > statValue + statValue * 2) || (obsStatValue < statValue - statValue * 2)
      case Stddev => obsStatValue > statValue * 2
    }
  }

  // Joints

  override def df: DataFrame = detectOutliers(source)
}
