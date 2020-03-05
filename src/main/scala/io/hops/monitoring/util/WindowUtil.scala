package io.hops.monitoring.util

import io.hops.monitoring.streams.resolver.StreamResolverSignature
import io.hops.monitoring.util.Constants.Window.{Defaults, WindowStreamResolverQueryName}
import io.hops.monitoring.window.WindowSetting
import io.hops.util.Hops
import org.apache.spark.streaming.Duration

object WindowUtil {

  def durationToString(duration: Duration): String = s"${duration.milliseconds} milliseconds"
  def windowStreamResolverQueryName(signature: StreamResolverSignature): String = s"$WindowStreamResolverQueryName-${signature.id}"
  def defaultSetting: WindowSetting = WindowSetting(Defaults.Duration, Defaults.SlideDuration, Defaults.WatermarkDelay)
  def defaultWindowStreamResolverLogsPath(signature: StreamResolverSignature): String =
    s"/Projects/${Hops.getProjectName}/Resources/$WindowStreamResolverQueryName/${signature.id}/window-resolver"
}
