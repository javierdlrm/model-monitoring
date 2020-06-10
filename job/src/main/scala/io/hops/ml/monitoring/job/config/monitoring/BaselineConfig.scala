package io.hops.ml.monitoring.job.config.monitoring

import io.circe.parser._
import io.circe.{Decoder, HCursor}
import io.hops.ml.monitoring.job.utils.Constants
import io.hops.ml.monitoring.stats.Baseline

case class BaselineConfig(map: Baseline)

object BaselineConfig {
  implicit val decodeBaselineConfig: Decoder[BaselineConfig] = Decoder.instance { c: HCursor =>
    val descriptive = decodeString(c.get[Option[String]](Constants.Baseline.Descriptive)).getOrElse(Map())
    val distributions = decodeString(c.get[Option[String]](Constants.Baseline.Distributions)).getOrElse(Map())
    Right(BaselineConfig(Baseline(descriptive, distributions)))
  }

  private def decodeString(res: Decoder.Result[Option[String]]): Option[Map[String, Map[String, Double]]] = {
    res match {
      case Right(str) => decodeMap(str)
      case Left(_) => throw new Exception(s"Cannot deserialize descriptive baseline. A json string is required.")
    }
  }

  private def decodeMap(str: Option[String]): Option[Map[String, Map[String, Double]]] = {
    if (str isDefined) {
      decode[Map[String, Map[String, Double]]](str.get) match {
        case Right(m) => Some(m)
        case Left(_) => throw new Exception("Cannot deserialize descriptive baseline. Json does not follow the format Map[String, Map[String, Double]]")
      }
    } else None
  }
}