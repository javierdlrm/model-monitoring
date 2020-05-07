package io.hops.monitoring.window

import java.sql.Timestamp

case class Window(start: Timestamp, end: Timestamp) extends java.io.Serializable