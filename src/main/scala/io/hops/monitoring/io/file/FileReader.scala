package io.hops.monitoring.io.file

import io.hops.monitoring.io.InputReader

object FileReader {
  implicit class ExtendedInputReader(val ir: InputReader) extends AnyVal {

    def parquet(path: String): InputReader = {
      // TODO: Read parquet and ir._addDataFrame(df)
      ir
    }

    def csv(path: String): InputReader = {
      // TODO: Read csv and ir._addDataFrame(df)
     ir
    }
  }
}
