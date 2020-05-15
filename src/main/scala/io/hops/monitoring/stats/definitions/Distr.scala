package io.hops.monitoring.stats.definitions

import io.hops.monitoring.stats.definitions.Distr.BinningType
import io.hops.monitoring.utils.Constants.Stats.Descriptive

import scala.collection.immutable.HashMap

case class Distr(bounds: HashMap[String, Seq[String]] = HashMap(), binning: BinningType.Value = BinningType.STURGE) extends StatDefinition {
  override val name: String = Descriptive.Distr
  override val require: Seq[String] = Seq(Descriptive.Min, Descriptive.Max)
}

object Distr {

  object BinningType extends Enumeration {
    type Type = Value

    val STURGE: BinningType.Value = Value //  1 + 3. 322 logN

    // TODO: add auto-binning options: Doane’s rule, Scott’s Rule, Rice’s Rule, Freedman-Diaconis’s Rule
  }

}
