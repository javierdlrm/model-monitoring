package io.hops.ml.monitoring.window

import io.hops.ml.monitoring.utils.Constants.Window.Defaults
import io.hops.ml.monitoring.utils.Constants.Window.Defaults
import org.apache.spark.streaming.Duration

case class WindowSetting(duration: Duration, slideDuration: Duration, watermarkDelay: Duration, minSize: Option[Int])

object WindowSetting {
  def apply(duration: Duration,
            slideDuration: Duration,
            watermarkDelay: Duration): WindowSetting = WindowSetting(duration, slideDuration, watermarkDelay, None)

  def apply(minSize: Int): WindowSetting = WindowSetting(Defaults.Duration, Defaults.SlideDuration, Defaults.WatermarkDelay, Some(minSize))
}