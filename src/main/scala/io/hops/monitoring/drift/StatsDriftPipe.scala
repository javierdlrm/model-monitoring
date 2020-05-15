package io.hops.monitoring.drift

import io.hops.monitoring.drift.detectors.StatsDriftDetector
import io.hops.monitoring.pipeline.SinkPipeJoint
import io.hops.monitoring.stats.definitions.StatDefinition
import io.hops.monitoring.stats.{Baseline, StatValue}
import io.hops.monitoring.utils.Constants.Drift.DriftColName
import io.hops.monitoring.utils.Constants.Vars.{DescriptionColName, FeatureColName, TypeColName}
import io.hops.monitoring.utils.Constants.Window.WindowColName
import io.hops.monitoring.utils.DataFrameUtil.{Encoders, Schemas}
import io.hops.monitoring.utils.LoggerUtil
import io.hops.monitoring.window.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap

class StatsDriftPipe(source: DataFrame, stats: Seq[String], detectors: Seq[StatsDriftDetector], baseline: Baseline)
  extends SinkPipeJoint {

  LoggerUtil.log.info(s"[DriftPipe] Created with detectors [${detectors.map(_.name).mkString(", ")}]")

  // Variables
  private val selectSchemaCols = WindowColName +: FeatureColName +: TypeColName +: stats

  private val driftSchemaFields = Seq(
    StructField(WindowColName, Schemas.structType[Window]()),
    StructField(FeatureColName, StringType),
    StructField(DriftColName, StringType),
    StructField(DescriptionColName, DoubleType)
  )
  private val driftSchema = StructType(driftSchemaFields)

  // Stats

  private def detectDrift(df: DataFrame): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(driftSchema)

    df.select(selectSchemaCols.map(col): _*)
      .flatMap(row => {
        val feature = row.getAs[String](FeatureColName)
        val featureStats = baseline(feature)
        checkFeatureDrift(feature, row, featureStats)
      })(rowEncoder)
  }

  def checkFeatureDrift(feature: String, row: Row, featureStats: HashMap[String, StatValue]): Iterable[Row] = {
    val window = row.getAs[Row](WindowColName)
    detectors.flatMap(detector => {
      // Prepare required values
      val values = HashMap(detector.stats.map(stat =>
        stat -> StatDefinition.toStatValue(stat,
          row.getAs[Any](stat))
      ): _*)

      // Run detector
      val drift = detector.detect(values, featureStats)

      if (drift isDefined)
        Some(Row(Seq(window, feature, detector.name, drift get): _*))
      else
        None
    })
  }

  // Joints

  override def df: DataFrame = detectDrift(source)
}
