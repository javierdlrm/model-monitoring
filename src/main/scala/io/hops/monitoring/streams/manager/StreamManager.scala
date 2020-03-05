package io.hops.monitoring.streams.manager

import java.util.UUID

import io.hops.monitoring.streams.resolver.{StreamResolverManager, StreamResolverSignature}
import io.hops.monitoring.streams.writer.StreamWriter
import io.hops.monitoring.util.LoggerUtil
import io.hops.monitoring.util.RichOption._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.streaming.Duration

object StreamManager extends java.io.Serializable {

  // Variables

  private var spark: Option[SparkSession] = None
  private var streamWriters: Seq[StreamWriter] = Seq()
  private var timeoutMs: Option[Duration] = None

  // Init

  def init(spark: SparkSession): StreamManager.type = {
    LoggerUtil.log.info(s"[StreamManager] Initializing")

    this.spark = Some(spark)
    this.spark.get.streams.addListener(listener)
    StreamResolverManager.setCallback(restart)
    this
  }

  // Streams

  private def restart(signature: StreamResolverSignature): Unit = {
    LoggerUtil.log.info(s"[StreamManager] Restarting queries with signature ${signature}")

    streamWriters.foreach(sw =>
      if (sw.signatures.isDefined && sw.signatures.get.contains(signature)) sw.restart)
  }

  private def start(sw: StreamWriter): Unit = {
    LoggerUtil.log.info(s"[StreamManager] Starting query with signatures [${sw.signatures.mkString(", ")}]")
    LoggerUtil.log.info(s"[StreamManager] Starting query: Resolvers")
    sw.signatures!startResolvers // Ensure resolvers are active
    LoggerUtil.log.info(s"[StreamManager] Starting query: Query")
    val (id, runId) = sw.start // Start query
    LoggerUtil.log.info(s"[StreamManager] Starting query: Done with id $id and runId $runId")
  }

  def awaitAll(streamWriters: Seq[StreamWriter], timeoutMs: Duration): Unit = {
    this.timeoutMs = Some(timeoutMs)
    awaitAll(streamWriters)
  }
  def awaitAll(streamWriters: Seq[StreamWriter]): Unit = {
    LoggerUtil.log.info(s"[StreamManager] Awaiting all queries (${streamWriters.length})")

    assert(spark isDefined)
    this.streamWriters = streamWriters

    // Start queries asynchronously
    streamWriters.foreach(start)

    // Await termination
    LoggerUtil.log.info(s"[StreamManager] Waiting for queries: Any active (${streamWriters.exists(_.isActive) })")
    var timeoutReached: Boolean = false
    while (streamWriters.exists(_.isActive) && !timeoutReached) {
      LoggerUtil.log.info(s"[StreamManager] Waiting for any query termination: Active [${streamWriters.filter(_.isActive).map(_.queryName).mkString(", ")}]")

      // Wait for the queries
      if (timeoutMs isDefined)
        timeoutReached = !spark.get.streams.awaitAnyTermination(timeoutMs.get.milliseconds) // return true if query.stop(), false otherwise
      else
        spark.get.streams.awaitAnyTermination()

      // Forget terminated queries
      spark.get.streams.resetTerminated
    }
  }

  // Resolvers

  private def startResolvers(signatures: Seq[StreamResolverSignature]): Unit = StreamResolverManager.start(signatures)
  private def stopResolvers(signatures: Seq[StreamResolverSignature]): Unit = {
    signatures.foreach(signature => {
      val used = streamWriters.exists(sw => sw.signatures.isDefined && sw.signatures.get.contains(signature))
      if (!used) StreamResolverManager.stop(Seq(signature))
    })
  }

  // Listener

  private def handleTerminatedQuery(id: UUID, runId: UUID, exception: Option[String]): Any = {
    LoggerUtil.log.info(s"[StreamManager](Listener) Streaming query terminated with id $id, runId $runId and exception $exception")

    val idx = streamWriters.indexWhere(sw => sw.id.get == id && sw.runId.get == runId)
    if (idx >= 0) { // query is StreamWriter
      if (exception isDefined) {
        // TODO: Retry policy
      } else {
        val signatures = streamWriters(idx).signatures
        streamWriters = streamWriters.drop(idx) // Remove stream writer
        signatures!stopResolvers // Stop unused resolvers
      }
    } else { // query is StreamResolver
      // TODO: Retry policy for resolvers
    }
  }

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
      LoggerUtil.log.info(s"[StreamManager](Listener) Streaming query started with id ${event.id}, runId ${event.runId} and name ${event.name}")

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
      LoggerUtil.log.info(s"[StreamManager](Listener) Streaming query progressing: id ${event.progress.id}, runId ${event.progress.runId}")

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
      handleTerminatedQuery(event.id, event.runId, event.exception)
  }
}
