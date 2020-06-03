package io.hops.ml.monitoring.pipeline

import java.util.UUID

import io.hops.ml.monitoring.utils.LoggerUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.streaming.Duration

object PipelineManager extends java.io.Serializable {

  // Variables

  private var spark: Option[SparkSession] = None
  private var pipelines: Seq[Pipeline] = Seq()
  private var timeoutMs: Option[Duration] = None

  def logState(head: String): Unit = LoggerUtil.log.info(s"[PipelineManager]$head Pipelines (${pipelines.length}) - [${this.pipelines.map(p => s"${p.queryName}")}]")

  // Init

  def init(spark: SparkSession): PipelineManager.type = {
    LoggerUtil.log.info(s"[PipelineManager] Initializing")

    this.spark = Some(spark)
    this.spark.get.streams.addListener(listener)
    this
  }

  // Streams

  def awaitAll(streamWriters: Seq[Pipeline], timeoutMs: Duration): Unit = {
    this.timeoutMs = Some(timeoutMs)
    awaitAll(streamWriters)
  }

  def awaitAll(pipelines: Seq[Pipeline]): Unit = {
    LoggerUtil.log.info(s"[PipelineManager] Awaiting all pipelines (${pipelines.length})")

    assert(spark isDefined)
    this.pipelines = pipelines

    // Start queries asynchronously
    this.pipelines.foreach(_.start)

    // Await termination
    LoggerUtil.log.info(s"[PipelineManager] Waiting for pipelines: Any active (${pipelines.exists(_.isActive)})")
    var timeoutReached: Boolean = false
    while (this.pipelines.exists(_.isActive) && !timeoutReached) {
      LoggerUtil.log.info(s"[PipelineManager] Waiting for any pipeline termination: Active [${pipelines.filter(_.isActive).map(_.queryName).mkString(", ")}]")

      // Wait for the queries
      if (timeoutMs isDefined)
        timeoutReached = !spark.get.streams.awaitAnyTermination(timeoutMs.get.milliseconds) // return true if query.stop(), false otherwise
      else
      spark.get.streams.awaitAnyTermination()

      // Forget terminated queries
      spark.get.streams.resetTerminated
    }

    // Ensure all queries stopped
    if (timeoutReached) this.pipelines.foreach(sw => if (sw.isActive) sw.stop())
  }

  // Listener

  private def handleTerminatedQuery(id: UUID, runId: UUID, exception: Option[String]): Any = {
    val idx = this.pipelines.indexWhere(p => p.id.get == id && p.runId.get == runId)
    if (exception isDefined) {
      // TODO: Retry policy
      LoggerUtil.log.error(s"[PipelineManager] Pipeline with id $id and runId $runId terminated with exception $exception")
    } else {
      this.pipelines = this.pipelines.drop(idx) // Remove stream writer
    }
  }

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
      LoggerUtil.log.info(s"[PipelineManager](Listener) Pipeline started with id ${event.id}, runId ${event.runId} and name ${event.name}")

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
      LoggerUtil.log.info(s"[PipelineManager](Listener) Pipeline progressing: id ${event.progress.id}, runId ${event.progress.runId}")

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      LoggerUtil.log.info(s"[PipelineManager](Listener) Pipeline terminated with id ${event.id}, runId ${event.runId} and exception ${event.exception}")
      handleTerminatedQuery(event.id, event.runId, event.exception)
    }
  }
}
