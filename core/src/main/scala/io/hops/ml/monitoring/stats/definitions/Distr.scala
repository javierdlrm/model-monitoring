package io.hops.ml.monitoring.stats.definitions

import io.hops.ml.monitoring.stats.definitions.Distr.BinningType
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive.Sturge

import scala.collection.immutable.HashMap

case class Distr(bounds: HashMap[String, Seq[String]] = Distr.defaultBounds, binning: BinningType.Value = Distr.defaultBinning) extends StatDefinition {
  override val name: String = Descriptive.Distr
  override val require: Seq[String] = Seq(Descriptive.Min, Descriptive.Max)
}

object Distr {

  val defaultBounds: HashMap[String, Seq[String]] = HashMap()
  val defaultBinning: BinningType.Value = BinningType.STURGE

  object BinningType extends Enumeration {
    type Type = Value

    val STURGE: BinningType.Value = Value(Sturge) //  1 + 3. 322 logN

    // TODO: add auto-binning options: Doane’s rule, Scott’s Rule, Rice’s Rule, Freedman-Diaconis’s Rule
  }

}
