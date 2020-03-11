package io.hops.monitoring.streams.resolver

trait StreamResolverBase extends java.io.Serializable {
  val signature: StreamResolverSignature

  def isActive: Boolean
  def start(callback: StreamResolverSignature => Unit): Unit
  def stop(): Unit
}
