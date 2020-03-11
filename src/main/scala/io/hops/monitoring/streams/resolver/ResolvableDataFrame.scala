package io.hops.monitoring.streams.resolver

case class ResolvableDataFrame(private var signatures: Option[Seq[StreamResolverSignature]] = None) extends java.io.Serializable {

  protected def getSignatures: Option[Seq[StreamResolverSignature]] = this.signatures

  protected def addResolver(resolver: StreamResolverBase): Unit = {
    // Add resolver
    val signature = StreamResolverManager.addResolver(resolver)

    // Register signature
    signatures = if (signatures isDefined)
      Some(signatures.get :+ signature)
    else
      Some(Seq(signature))
  }
}