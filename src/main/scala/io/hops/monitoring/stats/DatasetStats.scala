package io.hops.monitoring.stats

case class DatasetStats(featuresStats: Seq[FeatureStats]) {
  val stats: Seq[String] = featuresStats.head.stats.map(_.name)

  def filter(cols: Seq[String], stats: Seq[String]): DatasetStats =
    DatasetStats(
      featuresStats.flatMap(fs => {
        if (cols.contains(fs.name))
          Seq(FeatureStats(fs.name, fs.filter(stats)))
        else
          None
      }))
}

object DatasetStats {
  def apply(map: Map[String, Map[String, Double]]): DatasetStats =
    DatasetStats(
      map.map(f => FeatureStats(f._1, f._2.map(s =>
        DescStat(s._1, s._2)).toSeq)).toSeq)
}

case class FeatureStats(name: String, stats: Seq[DescStat]) {
  def filter(stats: Seq[String]): Seq[DescStat] = this.stats.filter(ds => stats.contains(ds.name))
}

case class DescStat(name: String, value: Double)
