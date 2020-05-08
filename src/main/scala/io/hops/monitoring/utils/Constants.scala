package io.hops.monitoring.utils

import org.apache.spark.streaming.{Duration, Seconds}

object Constants {

  // Window

  object Window {
    val WindowColName = "window"
    val StartColName = "start"
    val EndColName = "end"
    val WindowColStartFieldName = s"$WindowColName.$StartColName"
    val WindowColEndFieldName = s"$WindowColName.$EndColName"

    val WindowStreamResolverQueryName = "WindowResolverStreamingQuery"
    val RawDurationColName = "rawDuration"
    val RawSlideDurationColName = "rawSlideDuration"
    val RawWatermarkDelayColName = "rawWatermarkDelay"
    val DurationColName = "duration"
    val SlideDurationName = "slideDuration"
    val WatermarkDelayColName = "watermarkDelay"
    val ExecutedColName = "executed"

    object Defaults {
      val Duration: Duration = Seconds(1)
      val SlideDuration: Duration = Seconds(1)
      val WatermarkDelay: Duration = Seconds(1)
    }

  }

  // Stats

  object Stats {
    val StatColName = "stat"

    object Descriptive {
      val Max = "max"
      val Min = "min"
      val Count = "count"
      val Sum = "sum"
      val Pow2Sum = "pow2Sum" // auxiliar

      val Avg = "avg"
      val Mean = "mean"
      val Stddev = "stddev"

      val Simple = Seq(Max, Min, Sum, Count, Pow2Sum) // one-pass
      val Compound = Seq(Avg, Mean, Stddev) // two-passes
      val All = Seq(Max, Min, Sum, Count, Pow2Sum, Avg, Mean, Stddev)
    }
  }

  // Watcher

  object Outliers {
    val FeatureColName = "feature"
    val ValueColName = "value"
    val ThresholdColName = "threshold"
  }

  // File

  object File {
    val ParquetFormat = "parquet"
    val CsvFormat = "csv"

    val Path = "path"
    val CheckpointLocation = "checkpointLocation"
  }

  // Kafka

  object Kafka {
    val Format = "format"

    val Bootstrap_Servers = "kafka.bootstrap.servers"
    val Subscribe = "subscribe"
    val StartingOffsets = "startingOffsets"
    val SecurityProtocol = "kafka.security.protocol"
    val SSLTruststoreLocation = "kafka.ssl.truststore.location"
    val SSLTruststorePassword = "kafka.ssl.truststore.password"
    val SSLKeystoreLocation = "kafka.ssl.keystore.location"
    val SSLKeystorePassword = "kafka.ssl.keystore.location"
    val SSLKeyPassword = "kafka.ssl.key.password"
    val SSLEndpointIdentificationAlgorithm = "kafka.ssl.endpoint.identification.algorithm"
  }

}