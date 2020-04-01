package io.hops.monitoring.io.sink

import io.hops.monitoring.streams.manager.StreamManager
import io.hops.monitoring.util.LoggerUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.execution.streaming.Sink

class BasicSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): BasicSink = new BasicSink()
}

class BasicSink extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val simple = new java.text.SimpleDateFormat("HH:mm:ss:SSS Z")
    LoggerUtil.log.info(s"[BasicSink](${simple.format(new java.util.Date(System.currentTimeMillis()))}) Add Batch: batchId $batchId, data (${data.count()}): ${data.schema}")
    StreamManager.logState("(BasicSink)")
    LoggerUtil.log.info(s"[BasicSink] Spark session active queries: ${data.sparkSession.streams.active.length}")
    //    val batchDistinctCount = data.rdd.distinct.count()
    //    println(s"Batch ${batchId}'s distinct count is ${batchDistinctCount}")
  }
}