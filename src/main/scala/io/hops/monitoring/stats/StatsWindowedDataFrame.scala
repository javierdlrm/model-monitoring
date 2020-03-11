package io.hops.monitoring.stats

import io.hops.monitoring.alerts.AlertsDataFrame
import io.hops.monitoring.streams.resolver.{ResolvableDataFrame, StreamResolverSignature}
import io.hops.monitoring.streams.writer.StreamWriter
import io.hops.monitoring.util.Constants.Stats._
import io.hops.monitoring.util.Constants.Window.WindowColName
import io.hops.monitoring.util.DataFrameUtil.Encoders
import io.hops.monitoring.util.{LoggerUtil, StatsUtil}
import io.hops.monitoring.window.Window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

import scala.collection.mutable

class StatsWindowedDataFrame(kvgd: KeyValueGroupedDataset[Window, Row], schema: StructType, colNames: Seq[String], stats: Seq[String], signatures: Option[Seq[StreamResolverSignature]])
  extends ResolvableDataFrame(signatures) with java.io.Serializable {

  LoggerUtil.log.info(s"[StatsWindowedDataFrame] Created over columns [${colNames.mkString(", ")}] for stats [${stats.mkString(", ")}]")

  private val statsColFields = schema.filter(field => colNames.contains(field.name)).map(f => StructField(f.name, FloatType, f.nullable, f.metadata)) // Cast to FloatType
  private val statsFields = schema.find(field => field.name == WindowColName).get +: statsColFields :+ StructField(StatColName, StringType)
  private val statsSchema = StructType(statsFields)

  // Flat map groups with state

  private def buildMdf: DataFrame = {
    val rowEncoder = Encoders.rowEncoder(statsSchema)
    val stateEncoder = Encoders.statsWindowedStateEncoder
    kvgd.flatMapGroupsWithState[StatsWindowedDataFrameState, Row](OutputMode.Append(), GroupStateTimeout.EventTimeTimeout)(computeGroupStats)(stateEncoder, rowEncoder)
  }

  // Stats

  private def computeGroupStats(window: Window, instances: Iterator[Row], groupState: GroupState[StatsWindowedDataFrameState]): Iterator[Row] = {
    // Init state
    var state = if (!groupState.exists) StatsWindowedDataFrameState(colNames, stats) else groupState.get
    // Update stats
    state = updateState(state, instances)
    // Create stats row
    val statsRows = buildRow(window, state)
    // Check session
    if (groupState.hasTimedOut) { // If timed out, then remove session and send final update
      groupState.remove()
    } else {
      groupState.update(state)
    }
    // Return iterator of rows
    Iterator(statsRows:_*)
  }

  private def updateState(state: StatsWindowedDataFrameState, rows: Iterator[Row]): StatsWindowedDataFrameState = {
    val (instancesSimpleIt, instancesComplexIt) = rows.duplicate
    val stateStats = state.statsMap
    var n_instances = 0

    // simple stats (iteratively)
    instancesSimpleIt.foreach(instance => { // for each instance
      n_instances += 1
      stateStats.keys.foreach(colName => { // for each col
        val colStats = stateStats(colName)
        val instanceValue = instance.getAs[Double](colName).toFloat // get current value
        val updatedStats = updateColSimpleStats(colStats, instanceValue) // get updated stats
        stateStats(colName) = updatedStats // update state stats
      })
    })

    // compound stats (batch)
    stateStats.keys.foreach(colName => {
      val colStats = stateStats(colName)
      val updatedStats = updateColCompoundStats(colStats)
      stateStats(colName) = updatedStats
    })

    // complex stats (iterative)
    instancesComplexIt.foreach(instance => {
      stateStats.keys.foreach(colName => {
        val colStats = stateStats(colName)
        val instanceValue = instance.getAs[Double](colName).toFloat
        val updatedStats = updateColComplexStats(instanceValue, colStats)
        stateStats(colName) = updatedStats
      })
    })
    stateStats.keys.foreach(colName => { // complex stats (compound)
      val colStats = stateStats(colName)
      val updatedStats = updateColComplexStats(colStats)
      stateStats(colName) = updatedStats
    })

    // Update state
    state.update(stateStats)
  }

  private def updateColSimpleStats(colStats: mutable.HashMap[String, Option[Float]], instanceValue: Float): mutable.HashMap[String, Option[Float]] = {
    colStats.keys.filter(StatsUtil.isSimple)
      .foreach(stat => { // for each simple stat
        val statValue =
          if (colStats(stat) isEmpty) { // Check default simple stat
            StatsUtil.defaultStat(stat, instanceValue)
          } else {
            StatsUtil.Compute.simpleStat(stat, instanceValue, colStats)
          }
        colStats(stat) = Some(statValue) // update stat
      })
    colStats
  }

  private def updateColCompoundStats(colStats: mutable.HashMap[String, Option[Float]]): mutable.HashMap[String, Option[Float]] = {
    colStats.keys.filter(StatsUtil.isCompound)
      .foreach(stat => { // for each compound stat
        val statValue = StatsUtil.Compute.compoundStat(stat, colStats)
        colStats(stat) = Some(statValue)
      })
    colStats
  }

  private def updateColComplexStats(instanceValue: Float, colStats: mutable.HashMap[String, Option[Float]]): mutable.HashMap[String, Option[Float]] = {
    colStats.keys.filter(StatsUtil.isComplex)
      .foreach(stat => {
        val statValue = StatsUtil.Compute.complexStat(stat, instanceValue, colStats)
        colStats(stat) = Some(statValue)
      })
    colStats
  }
  private def updateColComplexStats(colStats: mutable.HashMap[String, Option[Float]]): mutable.HashMap[String, Option[Float]] = {
    colStats.keys.filter(StatsUtil.isComplex)
      .foreach(stat => {
        val statValue = StatsUtil.Compute.complexStat(stat, colStats)
        colStats(stat) = Some(statValue)
      })
    colStats
  }

  private def buildRow(window: Window, state: StatsWindowedDataFrameState): Seq[Row] = {
    state.stats.map(stat =>
      Row(Row(window.start, window.end) +: state.statsMap.map(colStat => colStat._2(stat).get).toArray :+ stat:_*))
  }

  // Alerts

  def watch: AlertsDataFrame = watch(colNames)
  def watch(colNames: Seq[String]): AlertsDataFrame = {
    assert(colNames.forall(colName => this.colNames.contains(colName))) // assert col stats are computed
    val mdf = buildMdf
    new AlertsDataFrame(mdf, colNames, stats, getSignatures)
  }

  // Output

  def output(queryName: String): StreamWriter = {
    val mdf = buildMdf
    new StreamWriter(mdf, queryName, getSignatures)
  }
}