package io.hops.ml.monitoring.utils

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

  // Variable

  object Vars {
    val TimestampColName = "timestamp"

    val FeatureColName = "feature"
    val TypeColName = "type"

    val ValueColName = "value"
    val ThresholdColName = "threshold"

    val RequestTimeColName = "requestTime"
    val DetectionTimeColName = "detectionTime"

    val NumericalColName = "numerical"
    val CategoricalColName = "categorical"
  }

  // Stats

  object Stats {
    val StatColName = "stat"

    object Descriptive {
      // Simple
      val Max = "max"
      val Min = "min"
      val Count = "count"
      val Sum = "sum" // aux
      val Pow2Sum = "pow2Sum" // aux
      val Distr = "distr"

      // Compound
      val Avg = "avg"
      val Mean = "mean"
      val Stddev = "stddev"
      val Perc = "perc"

      // Multiple
      val Cov = "cov"
      val Corr = "corr"

      // Types
      val Sample = "sample"
      val Population = "population"

      // Distr
      val Sturge = "sturge"

      // Groups
      val Simple = Seq(Max, Min, Sum, Count, Pow2Sum, Distr) // one-pass
      val Compound = Seq(Avg, Mean, Stddev, Perc) // uses simple
      val Multiple = Seq(Cov, Corr) // multiple vars

      val All = Seq(Max, Min, Sum, Count, Pow2Sum, Distr, Avg, Mean, Stddev, Perc, Cov, Corr)
    }

  }

  // Outliers

  object Outliers {
    val OutlierColName = "outlier"

    val DescriptiveStats = "descriptiveStats"
  }

  // Drift

  object Drift {
    val AlgorithmColName = "algorithm"
    val DriftColName = "drift"

    val Wasserstein = "wasserstein"
    val KullbackLeibler = "kullbackLeibler"
    val JensenShannon = "jensenShannon"
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
    val Kafka = "kafka"

    val BootstrapServers = "kafka.bootstrap.servers"
    val Topic = "topic"
    val Subscribe = "subscribe"
    val StartingOffsets = "startingOffsets"
    val EndingOffsets = "endingOffsets"
    val KeyDeserializer = "kafka.key.deserializer"
    val ValueDeserializer = "kafka.value.deserializer"
    val SecurityProtocol = "kafka.security.protocol"
    val SSLTruststoreLocation = "kafka.ssl.truststore.location"
    val SSLTruststorePassword = "kafka.ssl.truststore.password"
    val SSLKeystoreLocation = "kafka.ssl.keystore.location"
    val SSLKeystorePassword = "kafka.ssl.keystore.location"
    val SSLKeyPassword = "kafka.ssl.key.password"
    val SSLEndpointIdentificationAlgorithm = "kafka.ssl.endpoint.identification.algorithm"

    val Value = "value"
  }

}
