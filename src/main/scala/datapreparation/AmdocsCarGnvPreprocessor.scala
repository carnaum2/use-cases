package datapreparation

import metadata.Metadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AmdocsCarGnvPreprocessor {

  def main(args: Array[String]): Unit = {

    val process_date = args(0)//"20180528"

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs CAR preparation (CRM)")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    //val origin = "/user/hive/warehouse/tests_es.db/rbl_ids_srv_" + process_date
    val origin = "/user/hive/warehouse/tests_es.db/amdocs_ids_srv"
    //val origin = "/user/hive/warehouse/tests_es.db/jmarcoso_ids_test_srv_20180201_20180619"

    ////////////////// 2. Global variables //////////////////

    // 2.1. ____ Complete list of crm feats describing msisdn services ____

    val gnv_columns = Metadata.getInitialGnvColumns() :+ "msisdn"

    val usage_types = Metadata.getGnvUsageTypes()

    val hours = Metadata.getGnvHours()

    val gnv_aggs = Metadata.getNewGnvColumns()

    ///////////// 3. Preparing data (using cached tmp dataframes to break lineage and prevent "janino" errors (compiler) duting execution) //////////////////

    // Filter -- col("cod_estado_general").isin("01", "09") -- is not applied. All the services are processed and the filtered will be used in the process toimplement the model
    val tmp1 = spark
      .read
      .parquet(origin)
      .filter(col("ClosingDay") === process_date)
      .filter(col("clase_cli_cod_clase_cliente") === "RS" && col("rgu") === "movil")
      .select(gnv_columns.head, gnv_columns.tail:_*)
      // msisdn: pk
      .withColumn("msisdn", col("msisdn").cast("string"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp1: " + tmp1.count() + "\n")

    // GNV: null fields are set to -1; services with null fields tend to have all the usage fields as null

    // Total data consumption per hour during w and we; data per connection
    val tmp2 = hours.foldLeft(tmp1)(
      (acc, h) => acc
        //.withColumn("total_data_volume_w_" + h, usage_types.map(f => col("gnv_hour_" + h + "_w_" + f + "_data_volume_mb")).reduce(_+_))
        .withColumn("total_data_volume_w_" + h, usage_types.map(f => col("gnv_hour_" + h + "_w_" + f + "_data_volume_mb")).reduce((a,b) => a + b).cast("double") )
        //.withColumn("total_connections_w_" + h, usage_types.map(f => col("gnv_hour_" + h + "_w_" + f + "_num_of_connections")).reduce(_+_))
        .withColumn("total_connections_w_" + h, usage_types.map(f => col("gnv_hour_" + h + "_w_" + f + "_num_of_connections")).reduce((a,b) => a + b).cast("double") )
        .withColumn("data_per_connection_w_" + h, when(col("total_connections_w_" + h) <= 0.0, -1.0).otherwise(col("total_data_volume_w_" + h).cast("double")/col("total_connections_w_" + h).cast("double")) )
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp2: " + tmp2.count() + "\n")

    val tmp3 = tmp2
      .withColumn("total_data_volume_w", hours.map(h => col("total_data_volume_w_" + h)).reduce((a, b) => a + b).cast("double") )
      .withColumn("total_data_volume_w", when(col("total_data_volume_w") <= -1.0, -1.0).otherwise(col("total_data_volume_w")).cast("double") )
      .withColumn("total_connections_w", hours.map(h => col("total_connections_w_" + h)).reduce((a, b) => a + b).cast("double") )
      .withColumn("total_connections_w", when(col("total_connections_w") <= -1.0, -1.0).otherwise(col("total_connections_w")).cast("double") )
      .withColumn("data_per_connection_w", when(col("total_connections_w") <= 0.0, -1.0).otherwise(col("total_data_volume_w").cast("double")/col("total_connections_w").cast("double")) )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp3: " + tmp3.count() + "\n")

    val tmp4 = hours.foldLeft(tmp3)(
      (acc, h) => acc
        //.withColumn("total_data_volume_we_" + h, usage_types.map(f => col("gnv_hour_" + h + "_we_" + f + "_data_volume_mb")).reduce(_+_))
        .withColumn("total_data_volume_we_" + h, usage_types.map(f => col("gnv_hour_" + h + "_we_" + f + "_data_volume_mb")).reduce((a,b) => a + b).cast("double") )
        //.withColumn("total_connections_we_" + h, usage_types.map(f => col("gnv_hour_" + h + "_we_" + f + "_num_of_connections")).reduce(_+_))
        .withColumn("total_connections_we_" + h, usage_types.map(f => col("gnv_hour_" + h + "_we_" + f + "_num_of_connections")).reduce((a,b) => a + b).cast("double") )
        .withColumn("data_per_connection_we_" + h, when(col("total_connections_we_" + h) <= 0.0, -1.0).otherwise(col("total_data_volume_we_" + h).cast("double")/col("total_connections_we_" + h).cast("double")) )
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp4: " + tmp4.count() + "\n")

    val tmp5 = tmp4
      .withColumn("total_data_volume_we", hours.map(h => col("total_data_volume_we_" + h)).reduce((a, b) => a + b).cast("double") )
      .withColumn("total_data_volume_we", when(col("total_data_volume_we") <= -1.0, -1.0).otherwise(col("total_data_volume_we")).cast("double") )
      .withColumn("total_connections_we", hours.map(h => col("total_connections_we_" + h)).reduce((a, b) => a + b).cast("double") )
      .withColumn("total_connections_we", when(col("total_connections_we") <= -1.0, -1.0).otherwise(col("total_connections_we")).cast("double") )
      .withColumn("data_per_connection_we", when(col("total_connections_we") <= 0.0, -1.0).otherwise(col("total_data_volume_we").cast("double")/col("total_connections_we").cast("double")) )
      .withColumn("total_data_volume", col("total_data_volume_w").cast("double") + col("total_data_volume_we").cast("double") )
      .withColumn("total_data_volume", when(col("total_data_volume") <= -1.0, -1.0).otherwise(col("total_data_volume").cast("double")) )
      .withColumn("total_connections", col("total_connections_w").cast("double") + col("total_connections_we").cast("double") )
      .withColumn("total_connections", when(col("total_connections") <= -1.0, -1.0).otherwise(col("total_connections").cast("double")) )
      .withColumn("data_per_connection", when(col("total_connections") <= 0.0, -1.0).otherwise(col("total_data_volume").cast("double")/col("total_connections").cast("double")) )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp5: " + tmp5.count() + "\n")

    val tmp6 = hours.foldLeft(tmp5)(
      (acc, h) => acc
        .withColumn("gnv_hour_" + h + "_w_mou", col("gnv_hour_" + h + "_w_mou").cast("double"))
        .withColumn("gnv_hour_" + h + "_we_mou", col("gnv_hour_" + h + "_we_mou").cast("double"))
        .withColumn("gnv_hour_" + h + "_w_num_of_calls", col("gnv_hour_" + h + "_w_num_of_calls").cast("double"))
        .withColumn("gnv_hour_" + h + "_we_num_of_calls", col("gnv_hour_" + h + "_we_num_of_calls").cast("double"))
    )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp6: " + tmp6.count() + "\n")

    // Minutes per call
    val tmp7 = hours.foldLeft(tmp6)(
      (acc, h) => acc.withColumn("mou_per_call_w_" + h, when(col("gnv_hour_" + h + "_w_num_of_calls") <= 0.0, -1.0).otherwise(col("gnv_hour_" + h + "_w_mou")/col("gnv_hour_" + h + "_w_num_of_calls")).cast("double") )
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp7: " + tmp7.count() + "\n")

    val tmp8 = tmp7
      .withColumn("total_mou_w", hours.map(h => col("gnv_hour_" + h + "_w_mou")).reduce((a, b) => a + b).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp8: " + tmp8.count() + "\n")

    val tmp9 = hours.foldLeft(tmp8)(
      (acc, h) => acc.withColumn("mou_per_call_we_" + h, when(col("gnv_hour_" + h + "_we_num_of_calls") <= 0.0, -1.0).otherwise(col("gnv_hour_" + h + "_we_mou")/col("gnv_hour_" + h + "_we_num_of_calls")).cast("double") )
    ).cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp9: " + tmp9.count() + "\n")

    val tmp10 = tmp9
      .withColumn("total_mou_we", hours.map(h => col("gnv_hour_" + h + "_we_mou")).reduce((a, b) => a + b).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp10: " + tmp10.count() + "\n")

    val tmp11 = tmp10
      .na
      .fill(-1.0)
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp11: " + tmp11.count() + "\n")

    val findMaxPlace = udf((x: scala.collection.mutable.WrappedArray[Double]) => x.zipWithIndex.sortBy(_._1).reverse.map(_._2).head)

    val total_data_volume_w_array = hours.map(h => "total_data_volume_w_" + h)
    val total_connections_w_array = hours.map(h => "total_connections_w_" + h)
    val data_per_connection_w_array = hours.map(h => "data_per_connection_w_" + h)

    val tmp12 = tmp11
      // data_volume
      .withColumn("max_data_volume_w", greatest(total_data_volume_w_array.head, total_data_volume_w_array.tail:_*).cast("double") ) // hours.map(h => col("total_data_volume_w_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc))
      .withColumn("hour_max_data_volume_w", when(col("max_data_volume_w") === -1.0, -1.0).otherwise(findMaxPlace(array(total_data_volume_w_array.head, total_data_volume_w_array.tail:_*))).cast("double") )
      // connections
      .withColumn("max_connections_w", greatest(total_connections_w_array.head, total_connections_w_array.tail:_*)) //hours.map(h => col("total_connections_w_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc)))
      .withColumn("hour_max_connections_w", when(col("max_connections_w") === -1.0, -1.0).otherwise(findMaxPlace(array(total_connections_w_array.head, total_connections_w_array.tail:_*))).cast("double") )
      // data_per_connection
      .withColumn("max_data_per_connection_w", greatest(data_per_connection_w_array.head, data_per_connection_w_array.tail:_*)) //hours.map(h => col("data_per_connection_w_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc)))
      .withColumn("hour_max_data_per_connection_w", when(col("max_data_per_connection_w") === -1.0, -1.0).otherwise(findMaxPlace(array(data_per_connection_w_array.head, data_per_connection_w_array.tail:_*))).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp12: " + tmp12.count() + "\n")

    val total_data_volume_we_array = hours.map(h => "total_data_volume_we_" + h)
    val total_connections_we_array = hours.map(h => "total_connections_we_" + h)
    val data_per_connection_we_array = hours.map(h => "data_per_connection_we_" + h)

    val tmp13 = tmp12
      // data_volume
      .withColumn("max_data_volume_we", greatest(total_data_volume_we_array.head, total_data_volume_we_array.tail:_*).cast("double") ) // hours.map(h => col("total_data_volume_we_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc)))
      .withColumn("hour_max_data_volume_we", when(col("max_data_volume_we") === -1.0, -1.0).otherwise(findMaxPlace(array(total_data_volume_we_array.head, total_data_volume_we_array.tail:_*))).cast("double") )
      // connections
      .withColumn("max_connections_we", greatest(total_connections_we_array.head, total_connections_we_array.tail:_*).cast("double") ) // hours.map(h => col("total_connections_we_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc)))
      .withColumn("hour_max_connections_we", when(col("max_connections_we") === -1.0, -1.0).otherwise(findMaxPlace(array(total_connections_we_array.head, total_connections_we_array.tail:_*))).cast("double") )
      // data_per_connection
      .withColumn("max_data_per_connection_we", greatest(data_per_connection_we_array.head, data_per_connection_we_array.tail:_*).cast("double") ) // hours.map(h => col("data_per_connection_we_" + h)).foldLeft(lit(-1.0))((acc, c) => when(c > acc, c).otherwise(acc)))
      .withColumn("hour_max_data_per_connection_we", when(col("max_data_per_connection_we") === -1.0, -1.0).otherwise(findMaxPlace(array(data_per_connection_we_array.head, data_per_connection_we_array.tail:_*))).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp13: " + tmp13.count() + "\n")

    val mou_w_array = hours.map(h => "gnv_hour_" + h + "_w_mou")
    val num_calls_w_array = hours.map(h => "gnv_hour_" + h + "_w_num_of_calls")
    val mou_per_call_w_array = hours.map(h => "mou_per_call_w_" + h)

    val tmp14 = tmp13
      // mou
      .withColumn("max_mou_w", greatest(mou_w_array.head, mou_w_array.tail:_*).cast("double") )
      .withColumn("hour_max_mou_w", when(col("max_mou_w") === -1.0, -1.0).otherwise(findMaxPlace(array(mou_w_array.head, mou_w_array.tail:_*))).cast("double") )
      // num_calls
      .withColumn("max_num_calls_w", greatest(num_calls_w_array.head, num_calls_w_array.tail:_*).cast("double") )
      .withColumn("hour_max_num_calls_w", when(col("max_num_calls_w") === -1.0, -1.0).otherwise(findMaxPlace(array(num_calls_w_array.head, num_calls_w_array.tail:_*))).cast("double") )
      // mou_per_call
      .withColumn("max_mou_per_call_w", greatest(mou_per_call_w_array.head, mou_per_call_w_array.tail:_*).cast("double") )
      .withColumn("hour_max_mou_per_call_w", when(col("max_mou_per_call_w") === -1.0, -1.0).otherwise(findMaxPlace(array(mou_per_call_w_array.head, mou_per_call_w_array.tail:_*))).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp14: " + tmp14.count() + "\n")

    val mou_we_array = hours.map(h => "gnv_hour_" + h + "_we_mou")
    val num_calls_we_array = hours.map(h => "gnv_hour_" + h + "_we_num_of_calls")
    val mou_per_call_we_array = hours.map(h => "mou_per_call_we_" + h)

    val tmp15 = tmp14
      // mou
      .withColumn("max_mou_we", greatest(mou_we_array.head, mou_we_array.tail:_*).cast("double") )
      .withColumn("hour_max_mou_we", when(col("max_mou_we") === -1.0, -1.0).otherwise(findMaxPlace(array(mou_we_array.head, mou_we_array.tail:_*))).cast("double") )
      // num_calls
      .withColumn("max_num_calls_we", greatest(num_calls_we_array.head, num_calls_we_array.tail:_*).cast("double") )
      .withColumn("hour_max_num_calls_we", when(col("max_num_calls_we") === -1.0, -1.0).otherwise(findMaxPlace(array(num_calls_we_array.head, num_calls_we_array.tail:_*))).cast("double") )
      // mou_per_call
      .withColumn("max_mou_per_call_we", greatest(mou_per_call_we_array.head, mou_per_call_we_array.tail:_*).cast("double") )
      .withColumn("hour_max_mou_per_call_we", when(col("max_mou_per_call_we") === -1.0, -1.0).otherwise(findMaxPlace(array(mou_per_call_we_array.head, mou_per_call_we_array.tail:_*))).cast("double") )
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp15: " + tmp15.count() + "\n")

    tmp15
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_gnv_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Car GNV cleaned and saved\n")


  }

}
