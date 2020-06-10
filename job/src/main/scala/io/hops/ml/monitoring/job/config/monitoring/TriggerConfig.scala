package io.hops.ml.monitoring.job.config.monitoring

import io.circe.Decoder

case class TriggerConfig(window: WindowConfig)

object TriggerConfig {
  implicit val decodeTriggerConfig: Decoder[TriggerConfig] = Decoder.forProduct1("window")(TriggerConfig.apply)
}
