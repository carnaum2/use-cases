package datapreparation

import metadata.Metadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean}

object AmdocsCarBillingPreprocessor {

  def main(args: Array[String]): Unit = {

    // Bill_N1_InvoiceCharges, Bill_N1_Amount_To_Pay, Bill_N1_Tax_Amount, Bill_N1_Debt_Amount, Bill_N1_Bill_Date

    val process_date = args(0)//"20180528"

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs CAR preparation")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    // val origin = "/user/hive/warehouse/tests_es.db/rbl_ids_srv_" + process_date
    val origin = "/user/hive/warehouse/tests_es.db/amdocs_ids_srv"
    //val origin = "/user/hive/warehouse/tests_es.db/jmarcoso_ids_test_srv_20180201_20180619"

    ////////////////// 2. Global variables //////////////////

    // 2.4. ____ Fields related to billing ____

    val billing_columns = Metadata.getInitialBillingColumns() :+ "msisdn"

    // 2.5. ____ Values used for null imputation ____

    val (
          avg_bill_n1_invoicecharges,
          avg_bill_n1_bill_amount,
          avg_bill_n1_tax_amount,
          avg_bill_n1_debt_amount,

          avg_bill_n2_invoicecharges,
          avg_bill_n2_bill_amount,
          avg_bill_n2_tax_amount,
          avg_bill_n2_debt_amount,

          avg_bill_n3_invoicecharges,
          avg_bill_n3_bill_amount,
          avg_bill_n3_tax_amount,
          avg_bill_n3_debt_amount,

          avg_bill_n4_invoicecharges,
          avg_bill_n4_bill_amount,
          avg_bill_n4_tax_amount,
          avg_bill_n4_debt_amount,

          avg_bill_n5_invoicecharges,
          avg_bill_n5_bill_amount,
          avg_bill_n5_tax_amount,
          avg_bill_n5_debt_amount) =
      spark
        .read
        .parquet(origin)
        .filter(col("ClosingDay") === process_date)
        .filter(col("clase_cli_cod_clase_cliente") === "RS"
          && col("cod_estado_general").isin("01", "09")
          && col("rgu") === "movil"
          && col("Bill_N1_InvoiceCharges").isNotNull
          && col("Bill_N1_Amount_To_Pay").isNotNull
          && col("Bill_N1_Tax_Amount").isNotNull
          && col("Bill_N1_Debt_Amount").isNotNull

          && col("Bill_N2_InvoiceCharges").isNotNull
          && col("Bill_N2_Amount_To_Pay").isNotNull
          && col("Bill_N2_Tax_Amount").isNotNull
          && col("Bill_N2_Debt_Amount").isNotNull

          && col("Bill_N3_InvoiceCharges").isNotNull
          && col("Bill_N3_Amount_To_Pay").isNotNull
          && col("Bill_N3_Tax_Amount").isNotNull
          && col("Bill_N3_Debt_Amount").isNotNull

          && col("Bill_N4_InvoiceCharges").isNotNull
          && col("Bill_N4_Amount_To_Pay").isNotNull
          && col("Bill_N4_Tax_Amount").isNotNull
          && col("Bill_N4_Debt_Amount").isNotNull

          && col("Bill_N5_InvoiceCharges").isNotNull
          && col("Bill_N5_Amount_To_Pay").isNotNull
          && col("Bill_N5_Tax_Amount").isNotNull
          && col("Bill_N5_Debt_Amount").isNotNull)
        .select(
          mean("Bill_N1_InvoiceCharges"),
          mean("Bill_N1_Amount_To_Pay"),
          mean("Bill_N1_Tax_Amount"),
          mean("Bill_N1_Debt_Amount"),

          mean("Bill_N2_InvoiceCharges"),
          mean("Bill_N2_Amount_To_Pay"),
          mean("Bill_N2_Tax_Amount"),
          mean("Bill_N2_Debt_Amount"),

          mean("Bill_N3_InvoiceCharges"),
          mean("Bill_N3_Amount_To_Pay"),
          mean("Bill_N3_Tax_Amount"),
          mean("Bill_N3_Debt_Amount"),

          mean("Bill_N4_InvoiceCharges"),
          mean("Bill_N4_Amount_To_Pay"),
          mean("Bill_N4_Tax_Amount"),
          mean("Bill_N4_Debt_Amount"),

          mean("Bill_N5_InvoiceCharges"),
          mean("Bill_N5_Amount_To_Pay"),
          mean("Bill_N5_Tax_Amount"),
          mean("Bill_N5_Debt_Amount")
          )
        .rdd
        .map(r => (r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getDouble(11), r.getDouble(12), r.getDouble(13), r.getDouble(14), r.getDouble(15), r.getDouble(16), r.getDouble(17), r.getDouble(18), r.getDouble(19) ))
        .first()

//    val (
//      avg_bill_n1_bill_amount,
//      avg_bill_n1_tax_amount,
//
//      avg_bill_n2_bill_amount,
//      avg_bill_n2_tax_amount,
//
//      avg_bill_n3_bill_amount,
//      avg_bill_n3_tax_amount,
//
//      avg_bill_n4_bill_amount,
//      avg_bill_n4_tax_amount,
//
//      avg_bill_n5_bill_amount,
//      avg_bill_n5_tax_amount
//      ) = spark
//      .read
//      .parquet(origin)
//      .filter(col("clase_cli_cod_clase_cliente") === "RS"
//        && col("cod_estado_general").isin("01", "09")
//        && col("rgu") === "movil"
//        && col("Bill_N1_Bill_Amount").isNotNull
//        && col("Bill_N1_Tax_Amount").isNotNull
//
//        && col("Bill_N2_Bill_Amount").isNotNull
//        && col("Bill_N2_Tax_Amount").isNotNull
//
//        && col("Bill_N3_Bill_Amount").isNotNull
//        && col("Bill_N3_Tax_Amount").isNotNull
//
//        && col("Bill_N4_Bill_Amount").isNotNull
//        && col("Bill_N4_Tax_Amount").isNotNull
//
//        && col("Bill_N5_Bill_Amount").isNotNull
//        && col("Bill_N5_Tax_Amount").isNotNull
//      )
//      .select(
//        mean("Bill_N1_Bill_Amount"),
//        mean("Bill_N1_Tax_Amount"),
//
//        mean("Bill_N2_Bill_Amount"),
//        mean("Bill_N2_Tax_Amount"),
//
//        mean("Bill_N3_Bill_Amount"),
//        mean("Bill_N3_Tax_Amount"),
//
//        mean("Bill_N4_Bill_Amount"),
//        mean("Bill_N4_Tax_Amount"),
//
//        mean("Bill_N5_Bill_Amount"),
//        mean("Bill_N5_Tax_Amount")
//      )
//      .rdd
//      .map(r => (r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9) ))
//      .first()

    ////////////////// 3. Preparing data (using cached tmp dataframes to break lineage and prevent "janino" errors (compiler) duting execution) //////////////////

    // Filter -- col("cod_estado_general").isin("01", "09") -- is not applied. All the services are processed and the filtered will be used in the process toimplement the model
    val tmp1 = spark
      .read
      .parquet(origin)
      .filter(col("ClosingDay") === process_date)
      .filter(col("clase_cli_cod_clase_cliente") === "RS" && col("rgu") === "movil")
      .select(billing_columns.head, billing_columns.tail:_*)
      // msisdn: pk
      .withColumn("msisdn", col("msisdn").cast("string"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp1: " + tmp1.count() + "\n")

    val bill_na_fill = Map(
      "Bill_N1_InvoiceCharges" -> avg_bill_n1_invoicecharges,
      "Bill_N1_Amount_To_Pay" -> avg_bill_n1_bill_amount,
      "Bill_N1_Tax_Amount" -> avg_bill_n1_tax_amount,
      "Bill_N1_Debt_Amount" -> avg_bill_n1_debt_amount,

      "Bill_N2_InvoiceCharges" -> avg_bill_n2_invoicecharges,
      "Bill_N2_Amount_To_Pay" -> avg_bill_n2_bill_amount,
      "Bill_N2_Tax_Amount" -> avg_bill_n2_tax_amount,
      "Bill_N2_Debt_Amount" -> avg_bill_n2_debt_amount,

      "Bill_N3_InvoiceCharges" -> avg_bill_n3_invoicecharges,
      "Bill_N3_Amount_To_Pay" -> avg_bill_n3_bill_amount,
      "Bill_N3_Tax_Amount" -> avg_bill_n3_tax_amount,
      "Bill_N3_Debt_Amount" -> avg_bill_n3_debt_amount,

      "Bill_N4_InvoiceCharges" -> avg_bill_n4_invoicecharges,
      "Bill_N4_Amount_To_Pay" -> avg_bill_n4_bill_amount,
      "Bill_N4_Tax_Amount" -> avg_bill_n4_tax_amount,
      "Bill_N4_Debt_Amount" -> avg_bill_n4_debt_amount,

      "Bill_N5_InvoiceCharges" -> avg_bill_n5_invoicecharges,
      "Bill_N5_Amount_To_Pay" -> avg_bill_n5_bill_amount,
      "Bill_N5_Tax_Amount" -> avg_bill_n5_tax_amount,
      "Bill_N5_Debt_Amount" -> avg_bill_n5_debt_amount
    )

    val tmp2 = tmp1
      .na
      .fill(bill_na_fill)
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp2: " + tmp2.count() + "\n")

    val tmp3 = (1 to 5).toArray.foldLeft(tmp2)((acc, m) => acc
      // target and camp type
      .withColumn("bill_n" + m + "_net", col("Bill_N" + m + "_Amount_To_Pay") - col("Bill_N" + m + "_Tax_Amount")  )
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp3: " + tmp3.count() + "\n")

    val tmp4 = tmp3
      .na
      .fill(-1.0)
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp4: " + tmp4.count() + "\n")

    tmp4
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_billing_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Car Billing cleaned and saved\n")

  }

}
