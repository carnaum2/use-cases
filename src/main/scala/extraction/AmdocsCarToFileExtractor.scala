package extraction

import labeling.{ChurnDataLoader, GeneralDataLoader}
import metadata.Metadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AmdocsCarToFileExtractor {

  def main(args: Array[String]): Unit = {

    val process_date = args(0)//"20180528"
    val horizon = args(1).toInt
    val complete = args(2)
    val segmentfilter = args(3)

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
      .withColumnRenamed("msisdn_d", "campo2")

    println("\n[Info Amdocs Car Preparation] Num of samples in the labeled CAR: " + labeled_car_df.count() + "\n")

    val segment_df = GeneralDataLoader
      .getServicesUnderAnalysis(spark, process_date, segmentfilter, List("movil"))
      .select("msisdn_a")
      .withColumnRenamed("msisdn_a", "msisdn")

    println("\n[Info Amdocs Car Preparation] Num of samples in the population under analysis: " + segment_df.count() + "\n")

    val feats = Metadata.getProcessedColumns() ++ targets.map("churn_" + _)

    val segment_labeled_car_df = labeled_car_df
      .join(segment_df, Seq("msisdn"), "inner")
      .select(feats.head, feats.tail:_*)

    println("\n[Info Amdocs Car Preparation] Num of samples in the labeled CAR once segment filter is applied: " + segment_labeled_car_df.count() + "\n")

    complete match {

      case "T" => {
        segment_labeled_car_df
          .coalesce(1)
          .write
          .format("csv")
          .option("header", true)
          .save("/user/jmarcoso/car_complete_" + segmentfilter + "_" + process_date)
      }


      case "F" => {
        segment_labeled_car_df
          .sample(false, 0.15)
          .coalesce(1)
          .write
          .format("csv")
          .option("header", true)
          .save("/user/jmarcoso/car_sample_" + segmentfilter + "_" + process_date)
      }

    }

  }


}
