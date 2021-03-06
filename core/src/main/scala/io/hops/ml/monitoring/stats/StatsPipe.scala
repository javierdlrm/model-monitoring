package io.hops.ml.monitoring.stats

import java.sql.Timestamp

import io.hops.ml.monitoring.drift.StatsDriftPipeJoint
import io.hops.ml.monitoring.outliers.StatsOutliersPipeJoint
import io.hops.ml.monitoring.pipeline.SinkPipeJoint
import io.hops.ml.monitoring.stats.aggregators.StatAggregator
import io.hops.ml.monitoring.stats.definitions.StatDefinition
import io.hops.ml.monitoring.utils.Constants.Vars.{DetectionTimeColName, FeatureColName, TypeColName}
import io.hops.ml.monitoring.utils.Constants.Window.WindowColName
import io.hops.ml.monitoring.utils.DataFrameUtil.Encoders
import io.hops.ml.monitoring.utils.{DataFrameUtil, LoggerUtil, StatsUtil}
import io.hops.ml.monitoring.window.Window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row}

import scala.collection.immutable.HashMap

class StatsPipe(source: KeyValueGroupedDataset[Window, Row], schema: StructType, cols: Seq[String], override val stats: Seq[StatDefinition])
  extends SinkPipeJoint with StatsOutliersPipeJoint with StatsDriftPipeJoint {

  LoggerUtil.log.info(s"[StatsPipe] Created over columns [${cols.mkString(", ")}] for stats [${stats.mkString(", ")}]")

  private var _df: Option[DataFrame] = None

  private val _statsColFields = stats.map(s => StatAggregator.getStructField(s.name))
  private val _statsFields = StructField(WindowColName, DataFrameUtil.Schemas.structType[Window]()) +: StructField(DetectionTimeColName, TimestampType) +: StructField(FeatureColName, StringType) +: StructField(TypeColName, StringType) +: _statsColFields
  private val _statsSchema = StructType(_statsFields)

  // Flat map groups with state

  private def applyFlatMapGroupsWithState(kvgd: KeyValueGroupedDataset[Window, Row]): DataFrame = {

    val rowEncoder = Encoders.rowEncoder(_statsSchema)
    val stateEncoder = Encoders.statsWindowedStateEncoder
    // TODO: Check OutputMode. Use Update instead of Append? Are there intermediate duplicates in the results?
    // flatMapGroupsWithState in update mode is not supported with Append output mode on a streaming DataFrame/Dataset
    kvgd.flatMapGroupsWithState[StatsPipeState, Row](OutputMode.Append(), GroupStateTimeout.EventTimeTimeout)(computeGroupStats)(stateEncoder, rowEncoder)
  }

  // Stats

  // TODO: Revise. Only output the final stats row when timeout to ensure just one output in append mode.
  // TODO: The last window stats are not outputed when no new data arrives, since there's not more triggers.
  private def computeGroupStats(window: Window, instances: Iterator[Row], groupState: GroupState[StatsPipeState]): Iterator[Row] = {
    // Init state
    var state = groupState.getOption.getOrElse(new StatsPipeState(cols, stats))

    val rows = if (instances.isEmpty && groupState.hasTimedOut) { // if timeout, return stats
      LoggerUtil.log.info(s"[StatsPipe] TimedOut!")
      // Create rows from stats
      buildRows(window, state) // TODO: groupState.remove() ???
    } else {
      // Set timeout to current watermark (it needs to be set on each execution)
      groupState.setTimeoutTimestamp(window.end.getTime)
      // Update stats
      state = updateState(state, instances)
      // Update state
      groupState.update(state)
      // Return empty
      Seq.empty[Row]
    }
    Iterator(rows: _*)
  }

  private def updateState(state: StatsPipeState, rows: Iterator[Row]): StatsPipeState = {
    val (firstIterator, secondIterator) = rows.duplicate

    firstIterator.foreach(instance => { // for each instance
      state.stats.features.foreach(feature => { // for each feature
        val value = instance.getAs[Double](feature) // get current value
        state.stats.computeSimple(feature, value) // recompute with new feature value
      })
    })

    state.stats.features.foreach(feature => { // for each feature
      state.stats.computeCompound(feature) // compute compound stats with simple pre-computed stats
    })

    secondIterator.foreach(instance => { // for each instance
      val values = state.stats.features.map(f => instance.getAs[Double](f)) // same order: feature - value
      val pairs = HashMap(state.stats.features.zip(values): _*) // zip: feature - values
      state.stats.features.foreach(feature => { // for each feature
        state.stats.computeMultiple(feature, pairs)
      })
    })

    state
  }

  private def buildRows(window: Window, state: StatsPipeState): Seq[Row] = {
    state.features.map(feature => {
      val dataType = schema.find(_.name == feature).get.dataType
      val stats = state.stats.getFeatureStats(feature)
      Row(Row(window.start, window.end) +: new Timestamp(System.currentTimeMillis()) +: feature +: StatsUtil.getFeatureType(dataType) +: stats.map(_.value.getAny): _*)
    })
  }

  // Joints

  override def df: DataFrame = {
    if (_df isEmpty) _df = Some(applyFlatMapGroupsWithState(source))
    _df get
  }
}