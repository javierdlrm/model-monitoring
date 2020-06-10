package io.hops.ml.monitoring.job.utils

object Constants {

  object EnvVars {
    val ModelInfo = "MODEL_INFO"
    val InferenceSchemas = "INFERENCE_SCHEMAS"
    val MonitoringConfig = "MONITORING_CONFIG"
    val StorageConfig = "STORAGE_CONFIG"
    val JobConfig = "JOB_CONFIG"
  }

  object Schemas {
    val Request = "request"
    val Response = "response"

    val TypeColName = "type"
    val IdColName = "id"
    val TimeColName = "time"
    val PayloadColName = "payload"
  }

  object Job {
    val JobNameSuffix = "monitoring-job"

    def defaultJobName(model: String): String = s"$model-$JobNameSuffix"
  }

  object Kafka {
    val Brokers = "brokers"
  }

  object Baseline {
    val Descriptive = "descriptive"
    val Distributions = "distributions"
  }

  object Outliers {
    val Descriptive = "descriptive"
  }
}
