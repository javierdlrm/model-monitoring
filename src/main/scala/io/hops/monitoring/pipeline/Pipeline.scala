package io.hops.monitoring.pipeline

import java.util.UUID

import io.hops.monitoring.utils.LoggerUtil
import io.hops.monitoring.utils.RichOption._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

class Pipeline(val df: DataFrame, val queryName: String) extends java.io.Serializable {

  LoggerUtil.log.info(s"[Pipeline] Created for query: $queryName")

  // Variables

  private var _sinks: Array[SinkPipe] = Array()
  private var _sq: Option[StreamingQuery] = None
  private var _restarting: Boolean = false

  // Access methods

  def id: Option[UUID] = if (_sq isDefined) Some(_sq.get.id) else None

  def runId: Option[UUID] = if (_sq isDefined) Some(_sq.get.runId) else None

  def isActive: Boolean = (_sq.isDefined && _sq.get.isActive) || _restarting

  // Methods

  def addSink(sink: SinkPipe): Pipeline = {
    LoggerUtil.log.info(s"[Pipeline] Adding sink with format ${sink.format}")

    _sinks = _sinks :+ sink
    this
  }

  def start: (UUID, UUID) = {
    LoggerUtil.log.info(s"[Pipeline] Starting query $queryName")

    val sq = if (_sinks.length == 1) {
      _sinks(0).create(df, queryName).start
    } else {
      df.writeStream
        .queryName(queryName)
        .foreachBatch((batchDF: DataFrame, _: Long) => {
          batchDF.persist()
          _sinks.foreach(sink => sink.save(batchDF))
          batchDF.unpersist()
        }).start
    }
    _sq = Some(sq)
    (sq.id, sq.runId)
  }

  def restart: (UUID, UUID) = {
    LoggerUtil.log.info(s"[Pipeline] Restarting query $queryName")
    _restarting = true // keep streamWriter "isActive"
    stop() // stop query
    val (id, runId) = start // start query again
    _restarting = false
    LoggerUtil.log.info(s"[Pipeline] Query $queryName restarted with id $id and runId $runId")

    (id, runId)
  }

  def stop(): Unit = {
    _sq ! (_.stop)
  }
}
