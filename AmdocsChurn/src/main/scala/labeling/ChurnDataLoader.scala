package labeling

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import utils.Utils
import org.apache.spark.sql.expressions.Window

object ChurnDataLoader {

  // Collecting port-out requests observed during several months for both mobile and fixed services

  def getPortRequestsForMonthList(spark: SparkSession, yearmonths: List[String], modeltarget: String): Dataset[Row] = {

    // Returns a dataframe with the structure (msisdn, label=1.0) for the services (movil or fixed)
    // that have requested to be ported out during the months specified by the user

    def getPortRequestsForMonth(yearmonth: String): Dataset[Row] = {

      val year = yearmonth.substring(0, 4).toInt
      val month = yearmonth.substring(4, 6).toInt

      // Portability requests;

      val dfSolPor = modeltarget match {

        case "o2" => {

          val highvalue_msisdn = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/user/jmarcoso/data/mvno_active_customers_nike_20180710_0000_300000.txt")
            .union(spark
              .read
              .format("com.databricks.spark.csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", "|")
              .load("/user/jmarcoso/data/mvno_active_customers_nike_20180710_0001_106155.txt")
            )
            .filter(col("ProductName").contains("20GB 4G"))
            .select("MSISDN")
            .withColumn("MSISDN", col("MSISDN").substr(3, 9))
            .withColumnRenamed("MSISDN", "msisdn_d")
            .distinct()
            .withColumn("high_value_msisdn", lit(1.0))

          println("\n[Info DataLoader] highvalue_msisdn.count: " + highvalue_msisdn.count() + "\n")

          val highvalue_nif = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/user/jmarcoso/data/broadband_active_subscriptions_nike_20180710_0000_62082.txt")
            .withColumn("ref_date_start", from_unixtime(unix_timestamp(rpad(lit(yearmonth), 8, "01"), "yyyyMMdd")))
            .withColumn("ref_date_last", from_unixtime(unix_timestamp(rpad(lit(yearmonth), 8, Utils.getLastDayOfMonth(yearmonth.substring(4, 6))), "yyyyMMdd")))
            .withColumn("InstallationDate", from_unixtime(unix_timestamp(col("InstallationDate").substr(1, 10), "yyyy-MM-dd"))   )
            .filter(col("InstallationDate") >= col("ref_date_start") && col("InstallationDate") <= col("ref_date_last"))
            .select("NIF")
            .withColumnRenamed("NIF", "nif")
            .distinct()
            .withColumn("high_value_nif", lit(1.0))

          println("\n[Info DataLoader] highvalue_nif.count: " + highvalue_nif.count() + "\n")

          val call = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .load("/user/jmarcoso/data/Llamadas_1551_Amdocs.csv")
            .withColumn("ref_date_start", from_unixtime(unix_timestamp(rpad(lit(yearmonth), 8, "01"), "yyyyMMdd")))
            .withColumn("ref_date_last", from_unixtime(unix_timestamp(rpad(lit(yearmonth), 8, Utils.getLastDayOfMonth(yearmonth.substring(4, 6))), "yyyyMMdd")))
            .withColumn("FECHA_EVENTO", from_unixtime(unix_timestamp(col("FECHA_EVENTO").substr(1, 10), "yyyy-MM-dd"))   )
            .filter(col("FECHA_EVENTO") >= col("ref_date_start") && col("FECHA_EVENTO") <= col("ref_date_last"))
            .select("NUM_LLAMANTE")
            .withColumnRenamed("NUM_LLAMANTE", "msisdn_d")
            .distinct()
            .withColumn("call_o2_msisdn", lit(1.0))

          println("\n[Info DataLoader] call.count: " + call.count() + "\n")

          val mobporto2_tmp = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", "|")
            .load("/user/jmarcoso/data/" + yearmonth + ".csv")
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumnRenamed("SOPO_DS_CIF", "nif")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .select("msisdn_d", "nif", "portout_date", "SOPO_CO_RECEPTOR", "SOPO_CO_NRN_RECEPTORVIR")
            .distinct()

          println("\n[Info DataLoader] mobporto2_tmp.count: " + mobporto2_tmp.count() + "\n")

            val mobporto2 = mobporto2_tmp
            .join(highvalue_msisdn, Seq("msisdn_d"), "left_outer")
            .join(call, Seq("msisdn_d"), "left_outer")
            .join(highvalue_nif, Seq("nif"), "left_outer")
            .na
            .fill(Map("high_value_msisdn" -> 0.0, "high_value_nif" -> 0.0, "call_o2_msisdn" -> 0.0))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when(col("SOPO_CO_RECEPTOR") === "VIZZAVI" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"  && (col("high_value_msisdn") === 1.0 or col("high_value_nif") === 1.0 or col("call_o2_msisdn") === 1.0), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")

          println("\n[Info DataLoader] mobporto2.count: " + mobporto2.count() + "\n")

          // Port-out requests for fixed services are not included as Lowi is not identified among te set of operators of destination

          mobporto2

        }

        case "port" => {

          val mobport = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/" + yearmonth + ".csv")
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(1.0))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          val fixport = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/nodo_portas_fix_" + yearmonth + ".csv")
            .filter(col("INICIO_RANGO") === col("FIN_RANGO"))
            .withColumnRenamed("INICIO_RANGO", "msisdn_d")
            .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(1.0))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          mobport
            .union(fixport)
            .distinct()

        }

        case "masmovil" => {

          val mobportmm = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/" + yearmonth + ".csv")
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735014")
              or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735044")
              or (col("SOPO_CO_RECEPTOR") === "AIRTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "725303")
              or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735054")
              or (col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "715501")
              or (col("SOPO_CO_RECEPTOR") === "YOIGO" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          val fixportmm = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/nodo_portas_fix_" + yearmonth + ".csv")
            .filter(col("INICIO_RANGO") === col("FIN_RANGO"))
            .withColumnRenamed("INICIO_RANGO", "msisdn_d")
            .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when(col("OPERADOR_RECEPTOR").contains("Xtra Telecom"), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          mobportmm
            .union(fixportmm)
            .distinct()

        }

        case "orange" => {

          val mobportor = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/" + yearmonth + ".csv")
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
              or (col("SOPO_CO_RECEPTOR") === "JAZZTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
              or (col("SOPO_CO_RECEPTOR") === "EPLUS" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          val fixportor = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/nodo_portas_fix_" + yearmonth + ".csv")
            .filter(col("INICIO_RANGO") === col("FIN_RANGO"))
            .withColumnRenamed("INICIO_RANGO", "msisdn_d")
            .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when(col("OPERADOR_RECEPTOR").contains("France Telecom") or col("OPERADOR_RECEPTOR").contains("Jazz Telecom"), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          mobportor
            .union(fixportor)
            .distinct()
        }

        case "movistar" => {

          val mobportmv = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/" + yearmonth + ".csv")
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when(col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "0", 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          val fixportmv = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/nodo_portas_fix_" + yearmonth + ".csv")
            .filter(col("INICIO_RANGO") === col("FIN_RANGO"))
            .withColumnRenamed("INICIO_RANGO", "msisdn_d")
            .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .withColumn("label", lit(-1.0))
            .withColumn("label", when(col("OPERADOR_RECEPTOR").contains("NICA DE ESPA") or col("OPERADOR_RECEPTOR").contains("Telefonica Moviles"), 1.0).otherwise(col("label")))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          mobportmv
            .union(fixportmv)
            .distinct()
        }

        case "multiclass" => {

          val mobportmulti = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/" + yearmonth + ".csv")
            .withColumn("operator", lit("Other"))
            .withColumn("operator", when((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735014")
              or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735044")
              or (col("SOPO_CO_RECEPTOR") === "AIRTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "725303")
              or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735054")
              or (col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "715501")
              or (col("SOPO_CO_RECEPTOR") === "YOIGO" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"),"masmovil").otherwise(col("operator")))
            .withColumn("operator", when((col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
              or (col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "715401"),"movistar").otherwise(col("operator")))
            .withColumn("operator", when((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
              or (col("SOPO_CO_RECEPTOR") === "JAZZTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
              or (col("SOPO_CO_RECEPTOR") === "EPLUS" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"),"orange").otherwise(col("operator")))
            // take the last request if a given msisdn is associated with different destinations
            .withColumn("label", lit(4.0).cast("double"))
            .withColumn("label", when(col("operator") === "movistar", lit(1.0).cast("double")).otherwise(col("label")))
            .withColumn("label", when(col("operator") === "orange", lit(2.0).cast("double")).otherwise(col("label")))
            .withColumn("label", when(col("operator") === "masmovil", lit(3.0).cast("double")).otherwise(col("label")))
            .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
            .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          val fixportmulti = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load("/data/attributes/vf_es/sopo/nodo_portas_fix_" + yearmonth + ".csv")
            .filter(col("INICIO_RANGO") === col("FIN_RANGO"))
            .withColumn("operator", lit("Other"))
            .withColumn("operator", when(col("OPERADOR_RECEPTOR").contains("NICA DE ESPA") or col("OPERADOR_RECEPTOR").contains("Telefonica Moviles"), "movistar").otherwise(col("operator")) )
            .withColumn("operator", when(col("OPERADOR_RECEPTOR").contains("France Telecom") or col("OPERADOR_RECEPTOR").contains("Jazz Telecom"), "orange").otherwise(col("operator")) )
            .withColumn("operator", when(col("OPERADOR_RECEPTOR").contains("Xtra Telecom"), "masmovil").otherwise(col("operator")) )
            .withColumn("label", lit(4.0).cast("double"))
            .withColumn("label", when(col("operator") === "movistar", lit(1.0).cast("double")).otherwise(col("label")))
            .withColumn("label", when(col("operator") === "orange", lit(2.0).cast("double")).otherwise(col("label")))
            .withColumn("label", when(col("operator") === "masmovil", lit(3.0).cast("double")).otherwise(col("label")))
            .withColumnRenamed("INICIO_RANGO", "msisdn_d")
            .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
            .withColumn("portout_date", substring(col("portout_date"), 0, 10))
            .select("msisdn_d", "label", "portout_date")
            .distinct()

          mobportmulti
            .union(fixportmulti)
            .distinct()

        }

      }

      dfSolPor

    }

    if(yearmonths.isEmpty){

      val mySchema = StructType(StructField("msisdn_d", StringType, true) :: StructField("label", DoubleType, true) :: StructField("label", IntegerType, true) :: Nil)
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], mySchema)

    }

    else {

      (getPortRequestsForMonth(yearmonths.head) union getPortRequestsForMonthList(spark, yearmonths.tail, modeltarget))
      //.select("msisdn_d", "label")
      //.distinct()
      //.withColumn("label", lit(1.0))

    }
  }

  // Postprocessing the dataframe with port-out requests during several months

  def getPortLabels(spark: SparkSession, yearmonths: List[String], modeltarget: String): Dataset[Row] = {

    // The last day of the observed churn period is taken as reference

    val refyear = yearmonths.last.substring(0,4)

    val refmonth = yearmonths.last.substring(4,6)

    val refday = Utils.getLastDayOfMonth(refmonth)

    // To take the last request (asc) if a given msisdn is associated with different destinations,
    // rank will be computed and those with rank of 1 are retained.

    val window = Window
      .partitionBy("msisdn_d")
      .orderBy(asc("days_from_portout"))

    getPortRequestsForMonthList(spark, yearmonths, modeltarget)
      .withColumn("portout_date", from_unixtime(unix_timestamp(col("portout_date"), "yyyy-MM-dd")))
      .withColumn("ref_date", from_unixtime(unix_timestamp( concat(lit(refyear), lit(refmonth), lit(refday)), "yyyyMMdd")) )
      .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
      .withColumn("rank", row_number().over(window))
      .filter(col("rank") === 1)
      .select("msisdn_d", "label")
      .distinct()

  }

  // Labeling services (service_set) in a group of customers (targetdf) using the specified level (e.g., service,
  // num_cliente or nif_cliente). The input argument (car_date) specifies the CAR to be labeled

  def labelMsisdn(spark: SparkSession, targetdf: Dataset[Row], car_date: String, horizon: Int, modeltarget: String, service_set: List[String], level: String): Dataset[Row] = {

    // Only the id field and an additional column (label) is returned

    // Extracting year and month from car_date (yyyymmdd)

    val year = car_date.slice(0, 4)

    val month = car_date.slice(4, 6)

    val yearmonth = year + month

    // List of months considered to build the target variable

    val monthlist = (1 to horizon).toList.map(Utils.addMonth(yearmonth, _))

    // Loading port out requests

    val dfSolPor = getPortLabels(spark, monthlist, modeltarget)

    val numPort = dfSolPor.count()

    val numOperators = dfSolPor.select("label").distinct().count()

    println("\n[Info DataLoader] Number of port-out requests:" + numPort + "\n")

    println("\n[Info DataLoader] Number of distinct operators:" + numOperators + "\n")

    // Loading the CAR for the spaficied date

    val cardf = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_complete_" + car_date + "_processed_new")
      .join(targetdf, Seq("msisdn"), "inner")
      .select("msisdn", "campo2", "rgu", "num_cliente")
      .withColumnRenamed("msisdn", "msisdn_a")
      .withColumnRenamed("campo2", "msisdn_d")

    // Join between CAR and Port-out requests using "msisdn_d"

    val dfTar = level match {

      case "service" => cardf
        .join(dfSolPor, Seq("msisdn_d"), "left_outer")
        .withColumnRenamed("msisdn_a", "msisdn")
        .na
        .fill(0.0, Seq("label"))
        .filter(col("rgu").isin(service_set:_*))
        .select("msisdn", "label")
        .filter(col("label") >= 0)

      case "num_cliente" => {

        val window = Window
          .partitionBy("num_cliente")

        cardf
          .join(dfSolPor, Seq("msisdn_d"), "left_outer")
          .withColumnRenamed("msisdn_a", "msisdn")
          .na
          .fill(0.0, Seq("label"))
          .withColumn("sum_label", max("label").over(window))
          .withColumn("label", when(col("sum_label") >= 1.0, 1.0).otherwise(col("label")))
          .filter(col("rgu").isin(service_set:_*))
          .select("msisdn", "label")
          .filter(col("label") >= 0)

      }

      case "nif_cliente" => {

        val window = Window
          .partitionBy("nif_cliente")

        cardf
          .join(dfSolPor, Seq("msisdn_d"), "left_outer")
          .withColumnRenamed("msisdn_a", "msisdn")
          .na
          .fill(0.0, Seq("label"))
          .withColumn("sum_label", max("label").over(window))
          .withColumn("label", when(col("sum_label") >= 1.0, 1.0).otherwise(col("label")))
          .filter(col("rgu").isin(service_set:_*))
          .select("msisdn", "label")
          .filter(col("label") >= 0)

      }

    }

    val numLabels = dfTar
      .select("label")
      .distinct()
      .count()

    println("\n[Info DataLoader] Number of categories:" + numLabels + "\n")

    dfTar

  }

  // Unlike the previous function (labelMsisdn), this function adds a label
  // (the name is specified by the user through the argument labelname) to the CAR as given by the input argument df

  def labelMsisdnDataFrame(spark: SparkSession, df: Dataset[Row], yearmonth: String, horizon: Int, modeltarget: String, labelname: String, level: String): Dataset[Row] = {

    // The whole dataframe df with an additional column (label) is returned

    val monthlist = (1 to horizon).toList.map(Utils.addMonth(yearmonth, _))

    val dfSolPor = getPortLabels(spark, monthlist, modeltarget)

    val numPort = dfSolPor.count()

    val numOperators = dfSolPor.select("label").distinct().count()

    println("\n[Info DataLoader] Number of port-out requests:" + numPort + "\n")

    println("\n[Info DataLoader] Number of distinct operators:" + numOperators + "\n")

    val dfTar = level match {

      case "service" => df
        .join(dfSolPor, Seq("msisdn_d"), "left_outer")
        .withColumnRenamed("msisdn_a", "msisdn")
        .na
        .fill(0.0, Seq("label"))
        .withColumnRenamed("label", labelname)
        .filter(col("label") >= 0)

      case "num_cliente" => {

        val window = Window
          .partitionBy("num_cliente")

        df.join(dfSolPor, Seq("msisdn_d"), "left_outer")
          .withColumnRenamed("msisdn_a", "msisdn")
          .na
          .fill(0.0, Seq("label"))
          .withColumn("sum_label", max("label").over(window))
          .withColumn("label", when(col("sum_label") >= 1.0, 1.0).otherwise(col("label")))
          .drop("sum_label")
          .withColumnRenamed("label", labelname)
          .filter(col("label") >= 0)

      }

    }

    val numLabels = dfTar
      .select(labelname)
      .distinct()
      .count()

    println("\n[Info DataLoader] Number of categories:" + numLabels + "\n")

    dfTar

  }

  // Getting the CAR with the churn label

  def getLabeledCar(spark: SparkSession, yearmonth: String, horizon: Int, segmentfilter: String, modeltarget: String, service_set: List[String], feats: List[String], target: String, level: String): Dataset[Row] = {

    // Specific date of the CAR

    val car_date = yearmonth + Utils.getLastDayOfMonth(yearmonth.substring(4, 6))

    // 1. Population (num_cliente)

    val target_services = GeneralDataLoader.getServicesUnderAnalysis(spark, car_date, segmentfilter, service_set)

    println("\n[Info DataLoader] Number of target services before labeling: " + target_services.count() + "\n")

    // 2. Labeling with the specified target (modeltarget) and the specified level (level). Services (service_set) in the
    // specified segment (target_num_cliente) are labeled

    val labeled_services = labelMsisdn(spark, target_services, car_date, horizon, modeltarget, service_set, level)

    println("\n[Info Amdocs Car Preparation] Number of labeled services: " + labeled_services.count() + "\n")

    // 3. Joining feats and label for the targeted services

    val labelcardf = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_complete_" + car_date + "_processed_new")
      .join(labeled_services, Seq("msisdn"), "inner")

    val car_feats = feats :+ target

    val labelfeatcar = labelcardf
      .select(car_feats.head, car_feats.tail:_*)

    println("\n[Info Amdocs Car Preparation] Size of labelfeatcar: " + labelfeatcar.count() + "\n")

    labelfeatcar

  }

  // Getting an unalbeled vesion of the CAR

  def getUnlabeledCar(spark: SparkSession, yearmonth: String, segmentfilter: String, modeltarget: String, service_set: List[String], feats: List[String]): Dataset[Row] = {

    // Specific date of the CAR

    val car_date = if(yearmonth.length == 6) yearmonth + Utils.getLastDayOfMonth(yearmonth.substring(4, 6)) else yearmonth

    // 1. Population (services)

    val target_services = GeneralDataLoader
      .getServicesUnderAnalysis(spark, car_date, segmentfilter, service_set)

    println("\n[Info Amdocs Car Preparation] Size of labeled_services: " + target_services.count() + "\n")

    // 2. The CAR is filtered by the target_services

    val unlabelcardf = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_complete_" + car_date + "_processed_new")
      .join(target_services, Seq("msisdn"), "inner")

    val car_feats = feats

    val unlabelfeatcar = unlabelcardf
      .select(car_feats.head, car_feats.tail:_*)

    println("\n[Info Amdocs Car Preparation] Size of labelfeatcar: " + unlabelfeatcar.count() + "\n")

    unlabelfeatcar

  }

}
