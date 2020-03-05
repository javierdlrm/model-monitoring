package io.hops.monitoring.util

object LoggerUtil extends Serializable {
  @transient lazy val log = {
    val log = org.apache.log4j.Logger.getLogger(getClass.getName)
    log.setLevel(org.apache.log4j.Level.INFO)
    log
  }
}