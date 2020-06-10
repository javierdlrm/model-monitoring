package io.hops.ml.monitoring.job.config.monitoring

import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.hops.ml.monitoring.stats.definitions.Corr.CorrType
import io.hops.ml.monitoring.stats.definitions.Cov.CovType
import io.hops.ml.monitoring.stats.definitions.Distr.BinningType
import io.hops.ml.monitoring.stats.definitions.Stddev.StddevType
import io.hops.ml.monitoring.stats.definitions._
import io.hops.ml.monitoring.utils.Constants.Stats.Descriptive

case class StatsConfig(var definitions: Seq[StatDefinition])

object StatsConfig {

  implicit val defaultsConfig: Configuration = Configuration.default.withDefaults

  implicit val decodeStatsConfig: Decoder[StatsConfig] = Decoder.instance { c: HCursor =>
    var stats = Seq[StatDefinition]()

    c.get[Option[Max]](Descriptive.Max) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Max} definition")
    }
    c.get[Option[Min]](Descriptive.Min) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Min} definition")
    }
    c.get[Option[Count]](Descriptive.Count) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Count} definition")
    }
    c.get[Option[Sum]](Descriptive.Sum) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Sum} definition")
    }
    c.get[Option[Pow2Sum]](Descriptive.Pow2Sum) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Pow2Sum} definition")
    }
    c.get[Option[Distr]](Descriptive.Distr) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Distr} definition")
    }
    c.get[Option[Avg]](Descriptive.Avg) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Avg} definition")
    }
    c.get[Option[Mean]](Descriptive.Mean) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Mean} definition")
    }
    c.get[Option[Stddev]](Descriptive.Stddev) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Stddev} definition")
    }
    c.get[Option[Perc]](Descriptive.Perc) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Perc} definition")
    }
    c.get[Option[Cov]](Descriptive.Cov) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Cov} definition")
    }
    c.get[Option[Corr]](Descriptive.Corr) match {
      case Right(s) => if (s isDefined) stats = stats :+ s.get
      case Left(_) => throw new Exception(s"Cannot deserialize ${Descriptive.Corr} definition")
    }

    if (stats isEmpty) Left(DecodingFailure("No stats found", c.history))
    else Right(StatsConfig(stats))
  }

  // Enums decoders
  implicit val binningTypeDecode: Decoder[BinningType.Value] = Decoder.decodeEnumeration(BinningType)
  implicit val stddevTypeDecode: Decoder[StddevType.Value] = Decoder.decodeEnumeration(StddevType)
  implicit val covDecode: Decoder[CovType.Value] = Decoder.decodeEnumeration(CovType)
  implicit val corrDecode: Decoder[CorrType.Value] = Decoder.decodeEnumeration(CorrType)

  // With optional params
  implicit val distrDecoder: Decoder[Distr] = deriveDecoder
  implicit val stddevDecoder: Decoder[Stddev] = deriveDecoder
  implicit val covDecoder: Decoder[Cov] = deriveDecoder
  implicit val corrDecoder: Decoder[Corr] = deriveDecoder
}
