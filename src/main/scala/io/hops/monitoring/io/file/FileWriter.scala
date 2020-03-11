package io.hops.monitoring.io.file

import io.hops.monitoring.streams.writer.{StreamWriter, StreamWriterBatch}
import io.hops.monitoring.util.Constants.File
import org.apache.spark.sql.DataFrame

object FileWriter {
  implicit class FileOutputDataFrame(val ow: StreamWriter) extends AnyVal {

    def parquet(name: String, dirPath: String): StreamWriter = {
      val fullPath = dirPath + name + "-" + File.ParquetFormat
      val checkpointLocation = fullPath + "-checkpoint"

      val options = Map[String, String](
        (File.Path, fullPath),
        (File.CheckpointLocation, checkpointLocation))

      file(File.ParquetFormat, options)
    }

    def csv(name: String, dirPath: String): StreamWriter = {
      val fullPath = dirPath + name + "-" + File.CsvFormat
      val checkpointLocation = fullPath + "-checkpoint"

      val options = Map[String, String](
        (File.Path, fullPath),
        (File.CheckpointLocation, checkpointLocation))

      file(File.CsvFormat, options)
    }

    private def file(format: String, options: Map[String, String], transformer: Option[DataFrame => DataFrame] = None): StreamWriter = {
      // Create batch writer
      val writer = new StreamWriterBatch(format, options, transformer)
      // Add writer
      ow.addWriter(writer)
    }
  }
}