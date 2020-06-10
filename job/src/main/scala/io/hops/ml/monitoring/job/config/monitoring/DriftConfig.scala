package io.hops.ml.monitoring.job.config.monitoring

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.hops.ml.monitoring.drift.detectors.{JensenShannonDetector, KullbackLeiblerDetector, StatsDriftDetector, WassersteinDetector}
import io.hops.ml.monitoring.utils.Constants.Drift

case class DriftConfig(statsBased: Seq[StatsDriftDetector])

object DriftConfig {

  implicit val defaultsConfig: Configuration = Configuration.default.withDefaults

  implicit val decodeDriftConfig: Decoder[DriftConfig] = Decoder.instance { c: HCursor =>
    var detectors = Seq[StatsDriftDetector]()

    c.get[Option[WassersteinDetector]](Drift.Wasserstein) match {
      case Right(d) => if (d isDefined) detectors = detectors :+ d.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Drift.Wasserstein} detector")
    }
    c.get[Option[KullbackLeiblerDetector]](Drift.KullbackLeibler) match {
      case Right(d) => if (d isDefined) detectors = detectors :+ d.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Drift.KullbackLeibler} detector")
    }
    c.get[Option[JensenShannonDetector]](Drift.JensenShannon) match {
      case Right(d) => if (d isDefined) detectors = detectors :+ d.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Drift.JensenShannon} detector")
    }

    if (detectors isEmpty) Left(DecodingFailure("No detectors found", c.history))
    else Right(DriftConfig(detectors))
  }

  // With optional params
  implicit val wassersteinDecoder: Decoder[WassersteinDetector] = deriveDecoder
  implicit val kullbackLeiblerDecoder: Decoder[KullbackLeiblerDetector] = deriveDecoder
  implicit val jensenShannonDecoder: Decoder[JensenShannonDetector] = deriveDecoder
}
