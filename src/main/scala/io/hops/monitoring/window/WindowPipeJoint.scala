package io.hops.monitoring.window

import org.apache.spark.sql.DataFrame

trait WindowPipeJoint extends java.io.Serializable {

  def df: DataFrame

  def window(timestampCol: String, setting: WindowSetting): WindowPipe = {
    new WindowPipe(df, timestampCol, setting)
  }
}
