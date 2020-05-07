package io.hops.monitoring.io.file

import io.hops.monitoring.pipeline.SourcePipe

object FileSource {

  implicit class ExtendedSourcePipe(val sp: SourcePipe) extends AnyVal {

    def parquet(path: String): SourcePipe = {
      // TODO: Read parquet and ir._addDataFrame(df)
      sp
    }

    def csv(path: String): SourcePipe = {
      // TODO: Read csv and ir._addDataFrame(df)
      sp
    }
  }

}
