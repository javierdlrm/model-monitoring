package io.hops.monitoring.util

object TypeUtil {

  def ~=(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

}
