package io.hops.monitoring.window

import org.apache.spark.streaming.Duration
import io.hops.monitoring.utils.Constants.Window.Defaults

case class WindowSetting(duration: Duration, slideDuration: Duration, watermarkDelay: Duration, minSize: Option[Int])

object WindowSetting {
  def apply(duration: Duration,
            slideDuration: Duration,
            watermarkDelay: Duration): WindowSetting = WindowSetting(duration, slideDuration, watermarkDelay, None)

  def apply(minSize: Int): WindowSetting = WindowSetting(Defaults.Duration, Defaults.SlideDuration, Defaults.WatermarkDelay, Some(minSize))
}