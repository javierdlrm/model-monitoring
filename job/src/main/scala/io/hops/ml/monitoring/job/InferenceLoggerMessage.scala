package io.hops.ml.monitoring.job

import java.sql.Timestamp

case class InferenceLoggerMessage(`type` : String, id: String, time: Timestamp, payload: String)
