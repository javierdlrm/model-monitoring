package io.hops.monitoring.io.kafka

import io.hops.monitoring.util.Constants.Kafka._
import io.hops.monitoring.util.RichOption._

import scala.collection.{immutable, mutable}

object KafkaSettings {

  def build(bootstrapServers: Option[String] = None,
           subscribe: Option[String] = None,
           startingOffsets: Option[String] = None,
           securityProtocol: Option[String] = None,
           sslTruststoreLocation: Option[String] = None,
           sslTruststorePassword: Option[String] = None,
           sslKeystoreLocation: Option[String] = None,
           sslKeystorePassword: Option[String] = None,
           sslKeyPassword: Option[String] = None,
           sslEndpointIdentificationAlgorithm: Option[String] = None)
           : immutable.Map[String, String] = {

    val options: mutable.Map[String, String] = mutable.Map[String, String]()

    bootstrapServers!(options += Bootstrap_Servers -> _)
    subscribe!(options += Subscribe -> _)
    startingOffsets!(options += StartingOffsets -> _)
    securityProtocol!(options += SecurityProtocol -> _)
    sslTruststoreLocation!(options += SSLTruststoreLocation -> _)
    sslTruststorePassword!(options += SSLTruststorePassword -> _)
    sslKeystoreLocation!(options += SSLKeystoreLocation -> _)
    sslKeystorePassword!(options += SSLKeystorePassword -> _)
    sslKeyPassword!(options += SSLKeyPassword -> _)
    sslEndpointIdentificationAlgorithm!(options += SSLEndpointIdentificationAlgorithm -> _)

    options.toMap
  }
}
