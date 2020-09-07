package utils

/**
  * Created by victor on 26/9/17.
  */
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, Row}

object Utils {

  def getServiceList(services: String) = services match {
    case "movil" => List("movil")

    case "fbb" => List("fixed", "fbb")

    case "movilandfbb" => List("movil", "fbb", "fixed")
  }

  def addMonth(yearmonth: String, n:Int): String = {

    val month = yearmonth.substring(4, 6)

    val (outyear, outmonth) = ((month.toInt + n) > 12) match {
      case true => {
        (yearmonth.substring(0, 4).toInt + 1, yearmonth.substring(4, 6).toInt + n - 12)
      }
      case false => {
        (yearmonth.substring(0, 4).toInt, yearmonth.substring(4, 6).toInt + n)
      }
    }

    val resultmonth = if(outmonth < 10) ("0" + outmonth.toString) else outmonth.toString
    val resultyear = outyear.toString

    (resultyear + resultmonth)

  }

  def substractMonth(yearmonth: String, n:Int): String = {

    val month = yearmonth.substring(4, 6)

    val (outyear, outmonth) = ((month.toInt - n) <= 0) match {
      case true => {
        (yearmonth.substring(0, 4).toInt - 1, yearmonth.substring(4, 6).toInt - n + 12)
      }
      case false => {
        (yearmonth.substring(0, 4).toInt, yearmonth.substring(4, 6).toInt - n)
      }
    }

    val resultmonth = if(outmonth < 10) ("0" + outmonth.toString) else outmonth.toString
    val resultyear = outyear.toString

    (resultyear + resultmonth)

  }

  def getMonthSeq(inimonth: String, endmonth: String): List[String] = {

    if(endmonth.compare(inimonth) == 0) List(inimonth)

    else {List(inimonth) ++ getMonthSeq(addMonth(inimonth, 1), endmonth)}
  }

  def getImportantFeaturesFromVector(features: Array[String], impv: Vector): Array[(String, Double)] = {

    impv.toArray.zip(features).map(_.swap).sortBy(-_._2)

  }

  def getLastDayOfMonth(month: String): String =
    month match {
      case "1" | "3" | "5" | "7" | "8" | "10" | "12" | "01" | "03" | "05" | "07" | "08" => "31"
      case "4" | "6" | "9" | "11" | "04" | "06" | "09" => "30"
      case "2" | "02" => "28"
      case _ => "Unknown month"
    }


  def getDeciles(dt: Dataset[Row]): Dataset[Row] = {

    println("\n[Info Utils] Computing deciles\n")

    val decileth = dt
      .stat
      .approxQuantile("score", (0.1 to 0.9).by(0.1).toArray, 0.0)

    val assignDecile = udf((p: Double) => {

      decileth.zipWithIndex.collectFirst{case(th, idx) if(th > p) => idx + 1} match {
        case None => 10
        case Some(n:Int) => n
      }

    })

    dt.withColumn("decile", assignDecile(col("score")))

  }

  def getDeciles(dt: Dataset[Row], column: String): Dataset[Row] = {

    println("\n[Info Utils] Computing deciles\n")

    val numnull = dt.filter(col(column).isNull or col(column).isNaN).count()

    println("\n[Info Utils] Computing lift - Num nulls of " + column + ": " + numnull + "\n")

    dt.describe(column).show()

    val decileth = dt
      .stat
      .approxQuantile(column, (0.1 to 0.9).by(0.1).toArray, 0.0)

    val assignDecile = udf((p: Double) => {

      decileth.zipWithIndex.collectFirst{case(th, idx) if(th > p) => idx + 1} match {
        case None => 10
        case Some(n:Int) => n
      }

    })

    val dtdecile = dt.withColumn("decile", assignDecile(col(column)))

    (1 to 10).toList.foreach(decile => {
      val numsamples = dtdecile.filter(col("decile") === decile).count()
      println("[Info Utils] Num samples in decile " + decile + ": " + numsamples + "\n")
    })

    dtdecile

  }

  // getDeciles2: to avoid the use of approxQuantile
  def getDeciles2(dt: Dataset[Row], column: String): Dataset[Row] = {

    println("\n[Info Utils] Computing deciles\n")

    val scorerdd = dt.select("model_score").rdd.map(r => r.getAs[Double](0))

    val decileth = computePercentile(scorerdd, (0.1 to 0.9).by(0.1).toArray)

    val assignDecile = udf((p: Double) => {

      decileth.zipWithIndex.collectFirst{case(th, idx) if(th > p) => idx + 1} match {
        case None => 10
        case Some(n:Int) => n
      }

    })

    val dtdecile = dt.withColumn("decile", assignDecile(col(column)))

    dtdecile

  }

  def getLift(dt: Dataset[Row]): Array[(Double, Double)] = {

    println("\n[Info Utils] Computing lift\n")

    val refprevalence = dt.select("label").rdd.map(r => r.getAs[Double](0)).mean()

    println("\n[Info Utils] Computing lift - Ref Prevalence for class 1: " + refprevalence + "\n")

    dt
      .groupBy("decile")
      .agg(avg("label").alias("prevalence"))
      .withColumn("refprevalence", lit(refprevalence))
      .withColumn("lift", col("prevalence")/col("refprevalence"))
      .select("lift", "decile")
      .show

    val result = dt
      .groupBy("decile")
      .agg(avg("label").alias("prevalence"))
      .withColumn("refprevalence", lit(refprevalence))
      .withColumn("lift", col("prevalence")/col("refprevalence"))
      .withColumn("decile", col("decile").cast(DoubleType))
      .select("decile", "lift")
      .rdd
      .map(r => (r.getAs[Double](0), r.getAs[Double](1)))
      .sortByKey(ascending=true)
      .collect()

    result

  }

  def computePercentile(data: RDD[Double], tile: Array[Double]): Array[Double] = {
    // NIST method; data to be sorted in ascending order
    val r = data.sortBy(x => x)
    val c = r.count()

    val th = tile.map(t => {
      if (c == 1) r.first()
      else {
        val n = t * (c + 1)
        val k = math.floor(n).toLong
        val d = n - k
        if (k <= 0) r.first()
        else {
          val index = r.zipWithIndex().map(_.swap)
          val last = c
          if (k >= c) {
            index.lookup(last - 1).head
          } else {
            index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
          }
        }
      }
    })

    th

  }


  def computePercentile(data: RDD[Double], tile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val r = data.sortBy(x => x)
    val c = r.count()
    if (c == 1) r.first()
    else {
      val n = tile * (c + 1)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
  }

  def getBalancedSet(df: Dataset[Row], key: String) : Dataset[Row] = {

    val spark = df.sparkSession

    val schema = df.schema

    val resultDf = df.dtypes.toMap.get(key) match  {

      case Some(x) => {

        val rddDf = df.rdd.keyBy(_.getAs[Double](key))

        val samplesByKey = rddDf.countByKey()

        val minCat = samplesByKey.minBy(_._2)._1

        val minSamples = samplesByKey.getOrElse(minCat, 0L)

        val fractions = samplesByKey.mapValues(v => minSamples.toDouble/(v.toDouble)).map(identity)

        fractions.foreach(m => println("\n[Info DataPreprocessing] Fraction - k: " + m._1 + " - v: " + m._2 + "\n"))

        val rowRdd = rddDf.sampleByKey(withReplacement = false, fractions = fractions).map(_._2)

        spark.createDataFrame(rowRdd, schema)

      }

      case None => {
        println("\n[Info DataPreprocessing] Non-valid column\n")

        df

      }
    }

    resultDf

  }

}
