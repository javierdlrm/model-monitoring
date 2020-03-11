package io.hops.monitoring.streams.writer

import io.hops.monitoring.util.LoggerUtil
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{DataFrame, Row}

class StreamWriterBatch(val format: String, options: Map[String, String], transformer: Option[DataFrame => DataFrame] = None) extends java.io.Serializable {

  def save(df: DataFrame): Unit = {
    val tdf = if (transformer.isDefined) transformer.get(df) else df
    val writer = tdf.write.format(format)
    options foreach { op => writer.option(op._1, op._2) }
    writer.save()
  }

  def create(df: DataFrame, queryName: String): DataStreamWriter[Row] = {
    LoggerUtil.log.info(s"[StreamWriterBatch] Create writeStream with name $queryName")
    val tdf = if (transformer.isDefined) transformer.get(df) else df

    val writer = tdf
      .writeStream
      .queryName(queryName)
      .format(format)
      .outputMode(OutputMode.Append())

    options foreach { op => writer.option(op._1, op._2) }
    writer
  }
}
