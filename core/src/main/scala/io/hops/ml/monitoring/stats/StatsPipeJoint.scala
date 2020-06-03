package io.hops.ml.monitoring.stats

import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{KeyValueGroupedDataset, Row}

trait StatsPipeJoint extends java.io.Serializable {

  def kvgd(cols: Seq[String]): (KeyValueGroupedDataset[Window, Row], StructType)

  def stats(cols: Seq[String], stats: Seq[StatDefinition]): StatsPipe = {
    val (df, schema) = kvgd(cols)
    new StatsPipe(df, schema, cols, stats)
  }
}
