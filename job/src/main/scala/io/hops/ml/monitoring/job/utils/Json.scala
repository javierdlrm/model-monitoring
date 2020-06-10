package io.hops.ml.monitoring.job.utils

import io.circe._
import io.hops.ml.monitoring.utils.LoggerUtil

object Json {

  def extract[T](rawJson: String)(implicit decoder: Decoder[T]): Option[T] = {
    parser.decode[T](rawJson) match {
      case Left(failure) => LoggerUtil.log.info("[Json] Invalid JSON:" + failure.getMessage); None
      case Right(obj) => Some(obj)
    }
  }

}
