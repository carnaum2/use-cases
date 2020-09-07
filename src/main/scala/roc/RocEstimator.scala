package roc

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD

object RocEstimator {

  // Estimating PDF

  def getPdf(x: RDD[Double], bw_method: String): (KernelDensity, Double, Double, Double) = {

    val bw = getOptimalBw(x, bw_method)

    val x_min = x.min()

    val x_max = x.max()

    (new KernelDensity()
      .setSample(x)
      .setBandwidth(bw), bw, x_min, x_max)

  }

  def getSampledPdf(x: RDD[Double], bw_method: String, num_points: Int): (Array[Double], Array[Double], Double, Double) = {

    val bw = getOptimalBw(x, bw_method)

    val x_min = x.min()

    val x_max = x.max()

    val lim_min = x_min - 5*bw

    val lim_max = x_max + 5*bw

    val delta = (lim_max - lim_min)/(num_points - 1).toDouble

    val x_samples = (1 to num_points).toArray.map(e => lim_min + (e-1)*delta)

    val pdf = new KernelDensity()
      .setSample(x)
      .setBandwidth(bw)

    val f_samples = pdf.estimate(x_samples)

    (f_samples, x_samples, x_min, x_max)

  }

  def getOptimalBw(x: RDD[Double], method: String): Double = {

    method match {
      case "silverman" => {

        val std_x = x.stdev()

        val nsamples = x.count()

        1.06*std_x*Math.pow(nsamples, -0.2)

      }

      case _ => 1.0
    }
  }

  // Computing probabilities from the PDF

  def getAreaUnderPdf(x: RDD[Double], p: Double, num_points: Int): Double = {

    val (pdf, bw, x_min, x_max) = getPdf(x, "silverman")

    // The probability is given by the area under the pdf from -Inf to p.
    // A point far enough from the minimum value of the original sample is taken as the smallest point

    val min_x = x_min - 5*bw

    val probability = if(p <= min_x) {
      0.0
    } else {

      // val num_points = 1000

      val delta = (p - min_x)/(num_points - 1).toDouble

      val x_samples = (1 to num_points).toArray.map(e => min_x + (e-1)*delta)

      val f_samples = pdf.estimate(x_samples)

      // To obtain the area under the pdf from -Inf to p, the value of the pdf in each of the points is obtained.
      // The mean value between consecutive points of the pdf is obtained and multiplied by the delta (distance between consecutive samples of x).
      // This product yields the area of the rectangle given by delta (width) and avg of pdf between x_n and x_n+1 (height).
      // Summing up all the rectangles gives an approximation to the area under the pdf function

      (f_samples.slice(0, num_points) zip f_samples.slice(1, num_points + 1))
        .map{case(f0, f1) => 0.5*f0 + 0.5*f1}
        .map(_*delta).sum

    }

    probability

  }

  def getAreaUnderPdf(sampled_pdf: (Array[Double], Array[Double], Double, Double), p: Double): Double = {

    // The probability is given by the area under the pdf from -Inf to p.
    // A point far enough from the minimum value of the original sample is taken as the smallest point

    val (f_samples, x_samples, f_min, f_max) = sampled_pdf

    val delta = x_samples(1) - x_samples(0)

    val probability = if(p <= f_min) {
      0.0
    } else {

      val idx_max = x_samples.zipWithIndex.filter(e => e._1 <= p).map(_._2).max

      val f_samples_p = f_samples.slice(0, idx_max + 1)

      val num_points = f_samples_p.length

      // To obtain the area under the pdf from -Inf to p, the value of the pdf in each of the points is obtained.
      // The mean value between consecutive points of the pdf is obtained and multiplied by the delta (distance between consecutive samples of x).
      // This product yields the area of the rectangle given by delta (width) and avg of pdf between x_n and x_n+1 (height).
      // Summing up all the rectangles gives an approximation to the area under the pdf function

      (f_samples_p.slice(0, num_points) zip f_samples_p.slice(1, num_points + 1))
        .map{case(f0, f1) => 0.5*f0 + 0.5*f1}
        .map(_*delta).sum

    }

    probability

  }

  def getAreaUnderPdf(x: RDD[Double], p1: Double, p2: Double, num_points: Int): Double = getAreaUnderPdf(x, p2, num_points) - getAreaUnderPdf(x, p1, num_points)

  def getRoc(xl: RDD[(Double, Double)], num_roc_points: Int, num_pdf_samples: Int): Array[(Double, Double)] = {

    // Sampling freq for the ROC curve

    val delta = 1.0/(num_roc_points - 1).toDouble

    val th = (1 to num_roc_points).toArray.map(e => 0.0 + (e-1)*delta)

    // PDF of the variable Y | true

    val x_true = xl.filter(_._2 == 1.0).map(_._1)

    val sampled_pdf_true = getSampledPdf(x_true, "silverman", num_pdf_samples)

    // PDF of the variable Y | false

    val x_false = xl.filter(_._2 == 0.0).map(_._1)

    val sampled_pdf_false = getSampledPdf(x_false, "silverman", num_pdf_samples)

    // Collection of pairs (FNR, TPR)

    val tpr = th.map(u => 1 - getAreaUnderPdf(sampled_pdf_true, u))

    val fnr = th.map(u => 1 - getAreaUnderPdf(sampled_pdf_false, u))

    fnr zip tpr

  }

  def getAuc(roc: Array[(Double, Double)]): Double = {

    val num_points = roc.length

    // Each point of the curve is a tuple of the form (coord_x, coord_y). We want to compute the area of the polygons
    // defined by two consecutive points

    // Each element of the input roc is a point. Consecutive points are paired


    val auc = (roc.slice(1, num_points + 1) zip roc.slice(0, num_points))
      .toSeq
      .map(p => Seq(p._2, p._1))
      .map(trapezoid(_))
      .sum

/*
    // Ordering by fnr: decreasing

    val order_roc = roc.sortBy(_._1).reverse

    // Compute array of delta's for fnr

    val order_fnr = order_roc.map(_._1)

    val deltas = (order_fnr.slice(1, num_points + 1) zip order_fnr.slice(0, num_points)).map(p => p._1 - p._2)

    // Compute corresponding array of avg tpr between consecutive points

    val order_tpr = order_roc.map(_._2)

    val avgtpr = (order_tpr.slice(1, num_points + 1) zip order_tpr.slice(0, num_points)).map(p => 0.5*p._1 + 0.5*p._2)

    val auc = (deltas zip avgtpr).map(p => p._1*p._2).sum
    */

    auc

  }

  def getAuc(xl: RDD[(Double, Double)], num_roc_points: Int, num_points_integral: Int): Double = {

    val roc = getRoc(xl, num_roc_points, num_points_integral)

    val auc = getAuc(roc)

    auc

  }

  def trapezoid(points: Seq[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (x._1 - y._1) * (y._2 + x._2) / 2.0
  }

}
