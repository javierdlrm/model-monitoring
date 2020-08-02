package io.hops.ml.monitoring.stats

import io.hops.ml.monitoring.utils.LoggerUtil

import scala.collection.immutable.HashMap
import scala.collection.mutable

// StatValue

sealed trait StatValue

object StatValue {

  implicit class ExtendedStatValue(val sv: StatValue) extends AnyVal {
    def getAny: Any = sv match {
      case double: StatDouble => double.get
      case map: StatMap => map.get
    }

    def getDouble: Double = sv.asInstanceOf[StatDouble].get

    def getMap: mutable.HashMap[String, Double] = sv.asInstanceOf[StatMap].get
  }

}

// StatDouble

final case class StatDouble(get: Double = Double.NaN) extends StatValue {
  override def toString: String = get.toString
}

object StatDouble {
  def from(value: Any): StatDouble = StatDouble(value.asInstanceOf[Double])
}


// StatMap

final case class StatMap(get: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]) extends StatValue {
  override def toString: String = get.toString
}

object StatMap {
  def from(value: Any): StatMap = value match {
    case mutableMap: mutable.HashMap[String, Double] => StatMap(mutableMap)
    case immutableMap: HashMap[String, Double] => StatMap(mutable.HashMap[String, Double](immutableMap.toSeq: _*))
  }
}




