package io.hops.ml.monitoring.utils

object TypeUtil {

  def ~=(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

}
