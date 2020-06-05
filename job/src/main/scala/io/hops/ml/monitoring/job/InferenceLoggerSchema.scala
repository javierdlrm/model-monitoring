package io.hops.ml.monitoring.job

import java.sql.Timestamp

case class InferenceLoggerSchema(`type` : String, id: String, time: Timestamp, payload: String)
