package io.hops.ml.monitoring.job.config

import io.circe.Decoder

case class WindowConfig(duration: Int, slide: Int, watermarkDelay: Int)

object WindowConfig {
  implicit val decodeWindowConfig: Decoder[WindowConfig] =
    Decoder.forProduct3("duration", "slide", "watermarkDelay")(WindowConfig.apply)
}
