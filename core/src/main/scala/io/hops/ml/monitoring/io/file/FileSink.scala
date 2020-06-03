package io.hops.ml.monitoring.io.file

import io.hops.ml.monitoring.pipeline.{Pipeline, SinkPipe}
import io.hops.ml.monitoring.utils.Constants.File
import org.apache.spark.sql.DataFrame

object FileSink {

  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {

    def parquet(name: String, dirPath: String): Pipeline = {
      val fullPath = dirPath + name + "-" + File.ParquetFormat
      val checkpointLocation = fullPath + "-checkpoint"

      val options = Map[String, String](
        (File.Path, fullPath),
        (File.CheckpointLocation, checkpointLocation))

      file(File.ParquetFormat, options)
    }

    def csv(name: String, dirPath: String): Pipeline = {
      val fullPath = dirPath + name + "-" + File.CsvFormat
      val checkpointLocation = fullPath + "-checkpoint"

      val options = Map[String, String](
        (File.Path, fullPath),
        (File.CheckpointLocation, checkpointLocation))

      file(File.CsvFormat, options)
    }

    private def file(format: String, options: Map[String, String], transformer: Option[DataFrame => DataFrame] = None): Pipeline = {
      val sink = new SinkPipe(format, options, transformer)
      pipeline.addSink(sink)
    }
  }

}