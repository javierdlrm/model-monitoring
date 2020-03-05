package io.hops.monitoring.util

import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.functions.col
import io.hops.monitoring.window.Window
import io.hops.monitoring.stats.StatsWindowedDataFrameState
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.immutable.ListMap

object DataFrameUtil {

  implicit def explodeColumn(df: DataFrame) = new {

    // Explode column based on schema
    def explodeColumn(colName: String, schema: StructType): DataFrame = {
      df.select(
        col("*") +:
          schema.zipWithIndex.map(
            tupl => col(colName).getItem(tupl._2).cast(tupl._1.dataType).as(tupl._1.name)): _*) // explode columns based on schema
        .drop(colName) // drop aggregated Column
    }
  }

  // Encoders

  object Encoders {

    def windowEncoder: Encoder[Window] = org.apache.spark.sql.Encoders.kryo[Window]

    def statsWindowedStateEncoder: Encoder[StatsWindowedDataFrameState] = org.apache.spark.sql.Encoders.kryo[StatsWindowedDataFrameState]

    def rowEncoder(jsonSchema: String): ExpressionEncoder[Row] = {
      val structType = DataFrameUtil.Schemas.structType(jsonSchema)
      rowEncoder(structType)
    }

    def rowEncoder(schema: StructType): ExpressionEncoder[Row] = {
      RowEncoder.apply(schema)
    }
  }

  object Schemas {

    def structType(jsonSchema: String): StructType = {
      DataType.fromJson(jsonSchema).asInstanceOf[StructType]
    }

  }
}
