package io.hops.ml.monitoring.drift

import io.hops.ml.monitoring.drift.detectors.DriftDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.{Baseline, StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Drift.DriftColName
import io.hops.ml.monitoring.utils.Constants.Vars.{FeatureColName, ThresholdColName, ValueColName}
import io.hops.ml.monitoring.utils.DataFrameUtil.Encoders
import io.hops.ml.monitoring.utils.LoggerUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap

class DriftPipe(source: DataFrame, cols: Seq[String], detectors: Seq[DriftDetector], baseline: Baseline)
  extends SinkPipeJoint {

  LoggerUtil.log.info(s"[DriftPipe] Created with detectors [${detectors.map(_.name).mkString(", ")}]")

  // Variables

  private val driftSchemaFields = Seq(
    StructField(FeatureColName, StringType),
    StructField(ValueColName, DoubleType),
    StructField(ThresholdColName, DoubleType),
    StructField(DriftColName, StringType))

  private val driftSchema = StructType(driftSchemaFields)

  // Drift

  private def selectCols: DataFrame =
    source.select(cols.map(colName => col(colName)): _*)

  private def detectDrift(df: DataFrame): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(driftSchema)

    df.flatMap(row => {
      cols.flatMap(col => {
        val value = StatDouble(row.getAs[Double](col))
        val featureStats = baseline(col)
        checkFeatureValue(col, value, featureStats)
      })
    })(rowEncoder)
  }

  def checkFeatureValue(feature: String, value: StatValue, featureStats: HashMap[String, StatValue]): Iterable[Row] = {
    detectors.flatMap(detector => {
      val drift = detector.detect(value, featureStats)
      if (drift nonEmpty) {
        Some(Row(Seq(feature, drift.get._1.getAny, drift.get._2.getAny, detector.name): _*))
      }
      else None
    })
  }

  // Joints

  override def df: DataFrame = detectDrift(selectCols)
}