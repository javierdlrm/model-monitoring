#FROM javierdlrm/spark:2.4.5
FROM gcr.io/spark-operator/spark:v2.4.5-gcs-prometheus

ARG JAR_FILE
ARG MODEL_MONITORING_JOB_DIR

COPY ${JAR_FILE} ${MODEL_MONITORING_JOB_DIR}

