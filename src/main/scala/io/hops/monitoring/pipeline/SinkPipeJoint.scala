package io.hops.monitoring.pipeline

import org.apache.spark.sql.DataFrame

trait SinkPipeJoint extends java.io.Serializable {

  def df: DataFrame

  def output(queryName: String): Pipeline = {
    new Pipeline(df, queryName)
  }
}
