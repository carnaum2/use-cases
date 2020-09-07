package datapreparation

import metadata.Metadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object AmdocsCarCampPreprocessor {

  def main(args: Array[String]): Unit = {

    val process_date = args(0)//"20180528"

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs CAR preparation (CRM)")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    // val origin = "/user/hive/warehouse/tests_es.db/rbl_ids_srv_" + process_date
    val origin = "/user/hive/warehouse/tests_es.db/amdocs_ids_srv"
    //val origin = "/user/hive/warehouse/tests_es.db/jmarcoso_ids_test_srv_20180201_20180619"

    ////////////////// 2. Global variables //////////////////

    // 2.1. ____ Complete list of campaign feats describing msisdn services ____

    val camp_columns = Metadata.getInitialCampColumns() :+ "msisdn"

    val camp_types = Metadata.getCampTypes()

    val camp_channels = Metadata.getCampChannels()

    val camp_groups = Metadata.getCampGroups()

    ///////////// 3. Preparing data (using cached tmp dataframes to break lineage and prevent "janino" errors (compiler) duting execution) //////////////////

    // Filter -- col("cod_estado_general").isin("01", "09") -- is not applied. All the services are processed and the filter will be used in the process to implement the model
    val tmp1 = spark
      .read
      .parquet(origin)
      .filter(col("ClosingDay") === process_date)
      .filter(col("clase_cli_cod_clase_cliente") === "RS" && col("rgu") === "movil")
      .select(camp_columns.head, camp_columns.tail:_*)
      // msisdn: pk
      .withColumn("msisdn", col("msisdn").cast("string"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp1: " + tmp1.count() + "\n")

    val tmp2 = camp_groups.foldLeft(tmp1)((acc, g) => acc
      // Camps as target
      .withColumn("total_camps_" + g, camp_columns.filter(_.contains(g)).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("total_redem_" + g, camp_columns.filter(_.contains(g + "_1")).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("pcg_redem_" + g, when(col("total_camps_" + g) <= 0.0 || col("total_camps_" + g).isNull, -1.0).otherwise(col("total_redem_" + g).cast("double")/col("total_camps_" + g).cast("double")))
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp2: " + tmp2.count() + "\n")

    // type

    val tmp3 = camp_types.foldLeft(tmp2)((acc, t) => acc
      // Camps as target
      .withColumn("total_camps_" + t, camp_columns.filter(_.contains(t)).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("total_redem_" + t, camp_columns.filter(f => f.contains(t) && (f.contains("Target_1") || f.contains("Control_1") || f.contains("Universal_1"))).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("pcg_redem_" + t, when(col("total_camps_" + t) <= 0.0 || col("total_camps_" + t).isNull, -1.0).otherwise(col("total_redem_" + t).cast("double")/col("total_camps_" + t).cast("double")))
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp3: " + tmp3.count() + "\n")

    // channel

    val tmp4 = camp_channels.foldLeft(tmp3)((acc, ch) => acc
      // Camps as target
      .withColumn("total_camps_" + ch, camp_columns.filter(_.contains("_" + ch + "_")).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("total_redem_" + ch, camp_columns.filter(f => f.contains("_" + ch + "_") && (f.contains("Target_1") || f.contains("Control_1") || f.contains("Universal_1"))).map(f => col(f)).reduce((a,b) => a + b) )
      .withColumn("pcg_redem_" + ch, when(col("total_camps_" + ch) <= 0.0, -1.0).otherwise(col("total_redem_" + ch).cast("double")/col("total_camps_" + ch).cast("double")))
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp4: " + tmp4.count() + "\n")

    val tmp5 = tmp4
      .na
      .fill(-1.0)
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp5: " + tmp5.count() + "\n")

    tmp5
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_camp_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Car Campaigns cleaned and saved\n")

  }

}
