package assessment

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

/**
  * Created by victor on 2/2/18.
  */
class MulticlassMetricsCalculator(predsAndLabels: RDD[(Double, Double)]) {

  private val multiMetrics = new MulticlassMetrics(predsAndLabels)

  private val labels = predsAndLabels.map(_._2).distinct().collect()

  private val numPredsPerCatTt = predsAndLabels
    .map{case(p, l) => (l, p.toString)}
    .countByKey()
    .toArray
    .map{case(l, n) => (l, n.toDouble)}

  private def getFscore(): Array[(Double, Double)] = labels.map(l => (l, multiMetrics.fMeasure(l)))

  private def getMaxFscore(): (Double, Double) = getFscore().sortBy(_._2).reverse.head

  private def getMinFscore(): (Double, Double) = getFscore().sortBy(_._2).head

  private def getMaxTrend = numPredsPerCatTt.sortBy(_._2).reverse.head

  private def getMinTrend = numPredsPerCatTt.sortBy(_._2).head

  def getGlobalAcc(): Double = multiMetrics.accuracy

  def getMaxFscoreValue(): Double = getMaxFscore()._2

  def getMaxFscoreClass(): Double = getMaxFscore()._1

  def getMinFscoreValue(): Double = getMinFscore()._2

  def getMinFscoreClass(): Double = getMinFscore()._1

  def getMaxTrendClass = getMaxTrend._1

  def getMaxTrendValue = getMaxTrend._2

  def getMinTrendClass = getMinTrend._1

  def getMinTrendValue = getMinTrend._2

  def getConfMatrix = multiMetrics.confusionMatrix.toString()

}
