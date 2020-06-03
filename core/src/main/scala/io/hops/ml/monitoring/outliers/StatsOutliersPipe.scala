package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.StatsOutlierDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.stats.{Baseline, StatValue}
import io.hops.ml.monitoring.utils.Constants.Outliers.OutlierColName
import io.hops.ml.monitoring.utils.Constants.Stats.StatColName
import io.hops.ml.monitoring.utils.Constants.Vars.{ValueColName, FeatureColName, TypeColName}
import io.hops.ml.monitoring.utils.Constants.Window.WindowColName
import io.hops.ml.monitoring.utils.DataFrameUtil.{Encoders, Schemas}
import io.hops.ml.monitoring.utils.LoggerUtil
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap

class StatsOutliersPipe(source: DataFrame, detectors: Seq[StatsOutlierDetector], baseline: Baseline)
  extends SinkPipeJoint {

  LoggerUtil.log.info(s"[StatsOutliersPipe] Created with detectors [${detectors.map(_.name).mkString(", ")}]")

  // Variables

  private val stats = detectors.flatMap(_.stats).distinct
  private val selectSchemaCols = WindowColName +: FeatureColName +: TypeColName +: stats
  private val schemaColsIndexedMap = selectSchemaCols.zipWithIndex.toMap

  private val outliersSchemaFields = Seq(
    StructField(WindowColName, Schemas.structType[Window]()),
    StructField(FeatureColName, StringType),
    StructField(OutlierColName, StringType),
    StructField(StatColName, StringType),
    StructField(ValueColName, DoubleType)
  )
  private val outliersSchema = StructType(outliersSchemaFields)

  // Stats

  private def detectOutliers(df: DataFrame): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(outliersSchema)

    df.select(selectSchemaCols.map(col): _*)
      .flatMap(row => {
        val feature = row.getAs[String](schemaColsIndexedMap(FeatureColName))
        val featureStats = baseline(feature)
        checkFeatureStats(feature, row, featureStats)
      })(rowEncoder)
  }

  def checkFeatureStats(feature: String, row: Row, featureStats: HashMap[String, StatValue]): Iterable[Row] = {
    val values = HashMap(stats.map(s => s -> StatDefinition.toStatValue(s, row.getAs[Any](schemaColsIndexedMap(s)))): _*)

    detectors.flatMap(detector => {
      val outliers = detector.detect(values, featureStats)
      if (outliers nonEmpty) {
        val window = row.getAs[Row](WindowColName)
        outliers.map(outlier => Row(Seq(window, feature, detector.name, outlier._1, outlier._2.getAny): _*))
      }
      else None
    })
  }

  // Joints

  override def df: DataFrame = detectOutliers(source)
}
