package io.hops.ml.monitoring.utils

import io.hops.ml.monitoring.utils.Constants.Window.Defaults
import io.hops.ml.monitoring.window.WindowSetting
import org.apache.spark.streaming.Duration

object WindowUtil {

  def durationToString(duration: Duration): String = s"${duration.milliseconds} milliseconds"

  def defaultSetting: WindowSetting = WindowSetting(Defaults.Duration, Defaults.SlideDuration, Defaults.WatermarkDelay)
}
