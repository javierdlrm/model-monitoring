package io.hops.ml.monitoring.io.kafka

import io.hops.ml.monitoring.utils.Constants.Kafka._
import io.hops.ml.monitoring.utils.RichOption._

import scala.collection.{immutable, mutable}

case class KafkaSettings(options: immutable.Map[String, String])

object KafkaSettings {

  def apply(bootstrapServers: String,
            subscribe: Option[String] = None,
            topic: Option[String] = None,
            startingOffsets: Option[String] = None,
            securityProtocol: Option[String] = None,
            sslTruststoreLocation: Option[String] = None,
            sslTruststorePassword: Option[String] = None,
            sslKeystoreLocation: Option[String] = None,
            sslKeystorePassword: Option[String] = None,
            sslKeyPassword: Option[String] = None,
            sslEndpointIdentificationAlgorithm: Option[String] = None)
  : KafkaSettings = {

    val options: mutable.Map[String, String] = mutable.Map[String, String]()

    options += BootstrapServers -> bootstrapServers
    subscribe ! (options += Subscribe -> _)
    topic ! (options += Topic -> _)
    startingOffsets ! (options += StartingOffsets -> _)
    securityProtocol ! (options += SecurityProtocol -> _)
    sslTruststoreLocation ! (options += SSLTruststoreLocation -> _)
    sslTruststorePassword ! (options += SSLTruststorePassword -> _)
    sslKeystoreLocation ! (options += SSLKeystoreLocation -> _)
    sslKeystorePassword ! (options += SSLKeystorePassword -> _)
    sslKeyPassword ! (options += SSLKeyPassword -> _)
    sslEndpointIdentificationAlgorithm ! (options += SSLEndpointIdentificationAlgorithm -> _)

    KafkaSettings(options.toMap)
  }
}
