package io.hops.monitoring.utils

import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.functions.col
import io.hops.monitoring.window.Window
import io.hops.monitoring.stats.StatsPipeState
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.reflect.runtime.universe.TypeTag

object DataFrameUtil {

  implicit def explodeColumn(df: DataFrame): Object {
    def explodeColumn(colName: String, schema: StructType): DataFrame} = new {

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

    def statsWindowedStateEncoder: Encoder[StatsPipeState] = org.apache.spark.sql.Encoders.kryo[StatsPipeState]

    def rowEncoder(jsonSchema: String): ExpressionEncoder[Row] = {
      val structType = DataFrameUtil.Schemas.structType(jsonSchema)
      rowEncoder(structType)
    }

    def rowEncoder(schema: StructType): ExpressionEncoder[Row] = RowEncoder.apply(schema)
  }

  object Schemas {

    def structType(jsonSchema: String): StructType = DataType.fromJson(jsonSchema).asInstanceOf[StructType]

    def structType[T: TypeTag](): StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  }

  object Types {

    def toScala(dataType: DataType): Any =
      dataType match {
        case ByteType => Byte
        case ShortType => Short
        case IntegerType => Int
        case LongType => Long
        case FloatType => Float
        case DoubleType => Double
        case BinaryType => scala.Array
        case BooleanType => Boolean
        case at: ArrayType => scala.collection.Seq
        case mt: MapType => scala.collection.Map
      }
  }

}
