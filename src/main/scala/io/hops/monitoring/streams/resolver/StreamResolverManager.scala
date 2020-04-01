package io.hops.monitoring.streams.resolver

import io.hops.monitoring.streams.manager.StreamManager
import io.hops.monitoring.util.LoggerUtil

object StreamResolverManager extends java.io.Serializable {

  private var resolvers: Seq[StreamResolverBase] = Seq()
  private val callback: StreamResolverSignature => Unit = (signature: StreamResolverSignature) => StreamManager.restart(signature)

//  def setCallback(callback: StreamResolverSignature => Unit): Unit = {
//    this.callback = Some(callback)
//  }

  def addResolver(resolver: StreamResolverBase): StreamResolverSignature = {
    LoggerUtil.log.info(s"[StreamResolverManager] Adding resolver with signature [${resolver.signature}]")
    resolvers = resolvers :+ resolver
    resolver.signature
  }

  def start(signatures: Seq[StreamResolverSignature]): Unit = {
    StreamManager.logState("(StreamResolverManager.start)")

//    assert(callback isDefined)
    LoggerUtil.log.info(s"[StreamResolverManager] Starting resolvers with signatures [${signatures.mkString(", ")}]")

    // Ensure resolvers are running
    val filteredResolvers = resolvers.filter(r => !r.isActive && signatures.contains(r.signature))
    LoggerUtil.log.info(s"[StreamResolverManager] Starting resolvers: Inactive resolvers [${filteredResolvers.map(_.signature.id).mkString(", ")}]")
    filteredResolvers.foreach(r => r.start(callback))
    LoggerUtil.log.info(s"[StreamResolverManager] Starting resolvers: DONE")
  }

  def stop(signatures: Seq[StreamResolverSignature]): Unit = {
    StreamManager.logState("(StreamResolverManager.stop)")
    LoggerUtil.log.info(s"[StreamResolverManager] Stopping resolvers with signatures [${signatures.mkString(", ")}]")

    resolvers
      .filter(r => r.isActive && signatures.contains(r.signature))
      .foreach(_.stop())
  }
}
