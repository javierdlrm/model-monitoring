package io.hops.ml.monitoring.job.utils

import scala.collection.JavaConverters._

object Environment {

  private val _envVars = System.getenv().asScala

  def getEnvVars(names: Seq[String]): Map[String, String] = {
    _envVars.filter(envVar => names.contains(envVar._1)).toMap
  }

  def getEnvVar(name: String): Option[String] = _envVars.get(name)
}
