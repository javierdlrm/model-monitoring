package io.hops.ml.monitoring.utils

import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive._
import io.hops.ml.monitoring.utils.Constants.Vars.{CategoricalColName, NumericalColName}
import org.apache.spark.sql.types._

object StatsUtil {

  def isSimple(stat: String): Boolean =
    Simple.contains(stat)

  def isCompound(stat: String): Boolean =
    Compound.contains(stat)

  def isMultiple(stat: String): Boolean =
    Multiple.contains(stat)

  def getFeatureType(dataType: DataType): String = {
    dataType match {
      case StringType => CategoricalColName
      case _ => NumericalColName
    }
  }

  def round(value: Double, decimals: Int = 2, rounding: BigDecimal.RoundingMode.Value = BigDecimal.RoundingMode.HALF_UP): Double = {
    if (value.isNaN || value.isInfinite)
      value
    else {
      if (rounding == BigDecimal.RoundingMode.HALF_DOWN) LoggerUtil.log.info(s"[StatsUtil] value: $value...")
      val c = BigDecimal(value).setScale(decimals, rounding).toDouble
      if (rounding == BigDecimal.RoundingMode.HALF_DOWN) LoggerUtil.log.info(s"[StatsUtil] value: $value...  result: $c")
      c
    }
  }
}
