package io.hops.monitoring.streams.resolver

import scala.collection.immutable.HashMap

case class StreamResolverSignature(id: String, resolverType: StreamResolverType.Value, args: HashMap[String, String]) extends java.io.Serializable {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case signature: StreamResolverSignature => signature.hashCode == this.hashCode
      case _ => false
    }
  }
  override def hashCode(): Int = id.hashCode + resolverType.hashCode + args.hashCode
}

