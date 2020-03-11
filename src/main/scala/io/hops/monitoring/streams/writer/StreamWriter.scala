package io.hops.monitoring.streams.writer

import java.util.UUID

import io.hops.monitoring.streams.resolver.{ResolvableDataFrame, StreamResolverSignature}
import io.hops.monitoring.util.LoggerUtil
import io.hops.monitoring.util.RichOption._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

class StreamWriter(val df: DataFrame, val queryName: String, val signatures: Option[Seq[StreamResolverSignature]])
  extends ResolvableDataFrame(signatures) with java.io.Serializable  {

  LoggerUtil.log.info(s"[StreamWriter] Created")

  // Variables

  private var obws: Array[StreamWriterBatch] = Array()
  private var sq: Option[StreamingQuery] = None
  private var restarting: Boolean = false

  // Access methods

  def id: Option[UUID] = if (sq isDefined) Some(sq.get.id) else None
  def runId: Option[UUID] = if (sq isDefined) Some(sq.get.runId) else None
  def isActive: Boolean = (sq.isDefined && sq.get.isActive) || restarting

  // Methods

  def addWriter(obw: StreamWriterBatch): StreamWriter = {
    LoggerUtil.log.info(s"[StreamWriter] Adding writer with format ${obw.format}")

    obws = obws :+ obw
    this
  }

  def start: (UUID, UUID) = {
    LoggerUtil.log.info(s"[StreamWriter] Starting query $queryName")

    val stq = if (obws.length == 1) {
      obws(0).create(df, queryName).start
    } else {
      df.writeStream
        .queryName(queryName)
        .foreachBatch((batchDF: DataFrame, batchId: Long) => {
          batchDF.persist()
          obws.foreach(obw => obw.save(batchDF))
          batchDF.unpersist()
      }).start
    }
    LoggerUtil.log.info(s"[StreamWriter] Starting query: DONE")

    sq = Some(stq)
    (stq.id, stq.runId)
  }

  def restart: (UUID, UUID) = {
    // NOTE: Avoid time leaks between queries. QueryManager is watching!

    restarting = true // keep streamWriter "isActive"
    stop() // stop query
    val (id, runId) = start // start query again
    restarting = false

    (id, runId)
  }

  def stop(): Unit = {
    sq!(_.stop)
  }
}
