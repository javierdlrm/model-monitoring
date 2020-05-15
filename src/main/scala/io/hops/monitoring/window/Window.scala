package io.hops.monitoring.window

import org.apache.spark.sql.Row
import java.sql.Timestamp

case class Window(start: Timestamp, end: Timestamp) extends java.io.Serializable

object Window {
  def apply(row: Row): Window = Window(row.getAs[Timestamp](0), row.getAs[Timestamp](1))
}
