package io.hops.monitoring.window

import org.apache.spark.streaming.Duration

case class WindowSetting(duration: Duration, slideDuration: Duration, watermarkDelay: Duration)