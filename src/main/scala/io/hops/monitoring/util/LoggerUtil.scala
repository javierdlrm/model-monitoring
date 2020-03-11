package io.hops.monitoring.util

import org.apache.log4j.Logger

object LoggerUtil extends Serializable {
  @transient lazy val log: Logger = {
    val log = org.apache.log4j.Logger.getLogger(getClass.getName)
    log.setLevel(org.apache.log4j.Level.INFO)
    log
  }
}