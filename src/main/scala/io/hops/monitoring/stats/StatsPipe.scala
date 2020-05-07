package io.hops.monitoring.stats

import io.hops.monitoring.outliers.OutliersPipeJoint
import io.hops.monitoring.pipeline.SinkPipeJoint
import io.hops.monitoring.utils.Constants.Stats._
import io.hops.monitoring.utils.Constants.Window.WindowColName
import io.hops.monitoring.utils.DataFrameUtil.Encoders
import io.hops.monitoring.utils.{LoggerUtil, StatsUtil}
import io.hops.monitoring.window.Window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

import scala.collection.mutable

class StatsPipe(source: KeyValueGroupedDataset[Window, Row], schema: StructType, cols: Seq[String], override val stats: Seq[String])
  extends SinkPipeJoint with OutliersPipeJoint {

  LoggerUtil.log.info(s"[StatsPipe] Created over columns [${cols.mkString(", ")}] for stats [${stats.mkString(", ")}]")

  private var _df: Option[DataFrame] = None

  private val _statsColFields = schema.filter(field => cols.contains(field.name))
  private val _statsFields = schema.find(field => field.name == WindowColName).get +: _statsColFields :+ StructField(StatColName, StringType)
  private val _statsSchema = StructType(_statsFields)

  // Flat map groups with state

  private def applyFlatMapGroupsWithState(kvgd: KeyValueGroupedDataset[Window, Row]): DataFrame = {
    LoggerUtil.log.info(s"[StatsPipe] Apply flat map groups with state: kvgd -> ${kvgd != null}")

    val rowEncoder = Encoders.rowEncoder(_statsSchema)
    val stateEncoder = Encoders.statsWindowedStateEncoder
    // TODO: Check OutputMode. Use Update instead of Append? Are there intermediate duplicates in the results?
    kvgd.flatMapGroupsWithState[StatsPipeState, Row](OutputMode.Append(), GroupStateTimeout.EventTimeTimeout)(computeGroupStats)(stateEncoder, rowEncoder)
  }

  // Stats

  private def computeGroupStats(window: Window, instances: Iterator[Row], groupState: GroupState[StatsPipeState]): Iterator[Row] = {
    // Init state
    var state = if (!groupState.exists) StatsPipeState(cols, stats) else groupState.get
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
    Iterator(statsRows: _*)
  }

  private def updateState(state: StatsPipeState, rows: Iterator[Row]): StatsPipeState = {
    val stateStats = state.statsMap
    var n_instances = 0

    // simple stats (iteratively)
    rows.foreach(instance => { // for each instance
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

    // Update state
    state.update(stateStats)
  }

  private def updateColSimpleStats(colStats: mutable.HashMap[String, Option[Double]], instanceValue: Double): mutable.HashMap[String, Option[Double]] = {
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

  private def updateColCompoundStats(colStats: mutable.HashMap[String, Option[Double]]): mutable.HashMap[String, Option[Double]] = {
    colStats.keys.filter(StatsUtil.isCompound)
      .foreach(stat => { // for each compound stat
        val statValue = StatsUtil.Compute.compoundStat(stat, colStats)
        colStats(stat) = Some(statValue)
      })
    colStats
  }

  private def buildRow(window: Window, state: StatsPipeState): Seq[Row] = {
    stats.map(stat =>
      Row(Row(window.start, window.end) +: state.statsMap.map(colStat => colStat._2(stat).get).toArray :+ stat: _*))
  }

  // Joints

  override def df: DataFrame = {
    if (_df isEmpty) _df = Some(applyFlatMapGroupsWithState(source))
    _df get
  }
}