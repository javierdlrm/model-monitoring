package io.hops.monitoring.alerts

import io.hops.monitoring.streams.resolver.{ResolvableDataFrame, StreamResolverSignature}
import io.hops.monitoring.util.Constants.Stats._
import io.hops.monitoring.util.LoggerUtil
import org.apache.spark.sql.DataFrame

class AlertsDataFrame(df: DataFrame, colNames: Seq[String], private var stats: Seq[String], signatures: Option[Seq[StreamResolverSignature]])
  extends ResolvableDataFrame(signatures) with java.io.Serializable {

  LoggerUtil.log.info(s"[AlertsDataFrame] Created over columns [${colNames.mkString(", ")}]")

  // Unseen

  def unseen(trainDatasetName: String): UnseenAlertsDataFrame = unseen(trainDatasetName, Watchable)
  def unseen(trainDatasetName: String, stats: Seq[String]): UnseenAlertsDataFrame =
    new UnseenAlertsDataFrame(df, trainDatasetName, colNames, stats, getSignatures)
}
