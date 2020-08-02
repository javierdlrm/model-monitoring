package io.hops.ml.monitoring.job.config.monitoring

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.hops.ml.monitoring.job.utils.Constants.Outliers
import io.hops.ml.monitoring.outliers.detectors.{DescriptiveStatsDetector, OutliersDetector}

case class OutliersConfig(valuesBased: Seq[OutliersDetector])

object OutliersConfig {
  implicit val decodeOutlierConfig: Decoder[OutliersConfig] = Decoder.instance { c: HCursor =>
    var detectors = Seq[OutliersDetector]()
    c.get[Option[Seq[String]]](Outliers.Descriptive) match {
      case Right(stats) => if (stats isDefined) detectors = detectors :+ DescriptiveStatsDetector(stats.get)
      case Left(_) => throw new Exception(s"Cannot deserialize ${Outliers.Descriptive} outlier detector")
    }

    if (detectors isEmpty) Left(DecodingFailure("No detectors found", c.history))
    else Right(OutliersConfig(detectors))
  }
}
