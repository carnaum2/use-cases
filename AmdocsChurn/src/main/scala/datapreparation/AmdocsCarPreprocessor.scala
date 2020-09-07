package datapreparation

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AmdocsCarPreprocessor {

  def main(args: Array[String]): Unit = {

    val process_date = args(0)//"20180528"
    val numArgs = args.length
    val sources = args.slice(1, numArgs).toList

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs CAR preparation - Join")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    // Loading tables

    val car_crm_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_crm_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Size of CAR(crm): " + car_crm_df.count() + "\n")

    val car_gnv_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_gnv_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Size of CAR(gnv): " + car_gnv_df.count() + "\n")

    val car_camp_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_camp_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Size of CAR(camp): " + car_camp_df.count() + "\n")

    val car_billing_df = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_billing_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Size of CAR(billing): " + car_billing_df.count() + "\n")

    val car_df = car_crm_df
      .join(car_gnv_df, Seq("msisdn"), "inner")
      .join(car_camp_df, Seq("msisdn"), "inner")
      .join(car_billing_df, Seq("msisdn"), "inner")

    println("\n[Info Amdocs Car Preparation] Size of CAR(all): " + car_df.count() + "\n")

    car_df
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_complete_" + process_date + "_processed_new")

  }

}
