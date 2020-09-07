package extraction

import labeling.ChurnDataLoader
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AmdocsCarToTableExtractor {

  def main(args: Array[String]): Unit = {

    val process_date = args(0)//"20180528"
    val horizon = args(1).toInt
    val numArgs = args.length
    val sources = args.slice(2, numArgs).toList

    val yearmonth = process_date.substring(0, 6)

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs CAR extractor")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    // Loading tables

    val car_crm_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_crm_" + process_date + "_processed")

    println("\n[Info Amdocs Car Preparation] Size of CAR(crm): " + car_crm_df.count() + "\n")

    val car_gnv_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_gnv_" + process_date + "_processed")

    println("\n[Info Amdocs Car Preparation] Size of CAR(gnv): " + car_gnv_df.count() + "\n")

    val car_camp_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_camp_" + process_date + "_processed")

    println("\n[Info Amdocs Car Preparation] Size of CAR(camp): " + car_camp_df.count() + "\n")

    val car_billing_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_billing_" + process_date + "_processed")

    println("\n[Info Amdocs Car Preparation] Size of CAR(billing): " + car_billing_df.count() + "\n")

    val car_df = car_crm_df
      .join(car_gnv_df, Seq("msisdn"), "inner")
      .join(car_camp_df, Seq("msisdn"), "inner")
      .join(car_billing_df, Seq("msisdn"), "inner")
      .withColumnRenamed("campo2", "msisdn_d")
      .withColumnRenamed("msisdn", "msisdn_a")

    println("\n[Info Amdocs Car Preparation] Size of CAR(all): " + car_df.count() + "\n")

    val targets = List("port", "masmovil", "orange", "movistar", "multiclass")

    val labeled_car_df = targets.foldLeft(car_df)((acc, t) => ChurnDataLoader.labelMsisdnDataFrame(spark, acc, yearmonth, horizon, t, "churn_" + t, "service"))

    labeled_car_df
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_complete_multilabel_" + process_date + "_processed")

    println("\n[Info Amdocs Car Preparation] Num rows inserted: " + labeled_car_df.count() + "\n")


  }

}
