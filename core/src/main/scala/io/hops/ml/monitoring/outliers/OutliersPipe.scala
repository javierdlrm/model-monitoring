package io.hops.ml.monitoring.outliers

import io.hops.ml.monitoring.outliers.detectors.OutliersDetector
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.{Baseline, StatDouble, StatValue}
import io.hops.ml.monitoring.utils.Constants.Outliers.OutlierColName
import io.hops.ml.monitoring.utils.Constants.Vars.{FeatureColName, ThresholdColName, TypeColName, ValueColName}
import io.hops.ml.monitoring.utils.DataFrameUtil.Encoders
import io.hops.ml.monitoring.utils.LoggerUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap

class OutliersPipe(source: DataFrame, cols: Seq[String], detectors: Seq[OutliersDetector], baseline: Baseline)
  extends SinkPipeJoint {

  LoggerUtil.log.info(s"[OutliersPipe] Created with detectors [${detectors.map(_.name).mkString(", ")}]")

  // Variables

  private val outliersSchemaFields = Seq(
    StructField(FeatureColName, StringType),
    StructField(ValueColName, DoubleType),
    StructField(ThresholdColName, DoubleType),
    StructField(TypeColName, StringType),
    StructField(OutlierColName, StringType))

  private val outliersSchema = StructType(outliersSchemaFields)

  // Outliers

  private def selectCols: DataFrame =
    source.select(cols.map(colName => col(colName)): _*)

  private def detectOutliers(df: DataFrame): DataFrame = {
    val rowEncoder = Encoders.rowEncoder(outliersSchema)

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
      val outliers = detector.detect(value, featureStats)
      if (outliers nonEmpty) {
        outliers.map(outlier => Row(Seq(feature, value.getAny, outlier._2.getAny, outlier._1, detector.name): _*))
      }
      else None
    })
  }

  // Joints

  override def df: DataFrame = detectOutliers(selectCols)
}