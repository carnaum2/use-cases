package labeling

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils

object WinbackDataLoader {

  def getWinbackRequestsUnderAnalysis(spark: SparkSession, port_month: String, segmentfilter: String, service_set:List[String], competitor: String): Dataset[Row] = {

    // At the beginning of the month, mobile services found in the segment specified

    val prev_month = Utils.substractMonth(port_month, 1)

    val car_date = prev_month + Utils.getLastDayOfMonth(prev_month.substring(4,6))

    val segment = GeneralDataLoader.getServicesUnderAnalysis(spark, car_date, segmentfilter, service_set) // fields: msisdn_a, msisdn_d for the services in the set

    println("\n[Info Amdocs Winback Car Preparation] Number of services in the specified segment: " + segment.count() + "\n")

    val wb_ports = getWinbackLabels(spark, port_month, competitor) // fields: msisdn_d, label

    println("\n[Info Amdocs Winback Car Preparation] Number of port-out requests labeled: " + wb_ports.count() + "\n")

    // Only port-out requests found in the specified segment are retained

    val target_wb_ports = wb_ports
      .join(segment, Seq("msisdn_d"), "inner")
      .select("msisdn_a", "label")
      .withColumnRenamed("msisdn_a", "msisdn")

    println("\n[Info Amdocs Winback Car Preparation] Number of port-out requests labeled found in the target segment: " + target_wb_ports.count() + "\n")

    target_wb_ports

  }

  def getWinbackLabels(spark: SparkSession, yearmonth: String, competitor: String): Dataset[Row] = {

    val raw_ports = competitor match {

      case "masmovil" => {

        val mobportmm = spark
          .read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "|")
          .load("/user/jmarcoso/data/" + yearmonth + ".csv")
          .filter((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735014")
            or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735044")
            or (col("SOPO_CO_RECEPTOR") === "AIRTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "725303")
            or (col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "735054")
            or (col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "715501")
            or (col("SOPO_CO_RECEPTOR") === "YOIGO" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"))
          //.select( "SOPO_DS_MSISDN1")
          .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
          .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
          .withColumn("portout_date", substring(col("portout_date"), 0, 10))
          .withColumnRenamed("ESTA_CO_ESTADO", "estado")
          .withColumn("label", lit(0.0))
          .withColumn("label", when(col("estado") === lit("ACAN"), 1.0).otherwise(col("label")))
          .select("msisdn_d", "label", "portout_date")
          .distinct()

        // TODO: to build the state of the port-out request

        val fixportmm = spark
          .read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "|")
          // /user/jmarcoso/data/nodo_portas_fix_201806.csv
          .load("/user/jmarcoso/data/nodo_portas_fix_" + yearmonth + ".csv")
          .filter(col("INICIO_RANGO") === col("FIN_RANGO") && col("OPERADOR_RECEPTOR").contains("Xtra Telecom"))
          .withColumnRenamed("INICIO_RANGO", "msisdn_d")
          .withColumnRenamed("FECHA_INSERCION_SGP", "portout_date")
          .withColumn("portout_date", substring(col("portout_date"), 0, 10))
          .withColumn("label", lit(1.0))
          .select("msisdn_d", "label", "portout_date")
          .distinct()

        mobportmm
          .union(fixportmm)
          .distinct()

      }

      case "orange" => spark
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .load("/user/jmarcoso/data/" + yearmonth + ".csv")
        .filter((col("SOPO_CO_RECEPTOR") === "AMENA" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
          or (col("SOPO_CO_RECEPTOR") === "JAZZTEL" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
          or (col("SOPO_CO_RECEPTOR") === "EPLUS" && col("SOPO_CO_NRN_RECEPTORVIR") === "0"))
        //.select( "SOPO_DS_MSISDN1")
        .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
        .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
        .withColumn("portout_date", substring(col("portout_date"), 0, 10))
        .withColumnRenamed("ESTA_CO_ESTADO", "estado")
        .withColumn("label", lit(0.0))
        .withColumn("label", when(col("estado") === lit("ACAN"), 1.0).otherwise(col("label")))
        .select("msisdn_d", "label", "portout_date")
        .distinct()

      case "movistar" => spark
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .load("/user/jmarcoso/data/" + yearmonth + ".csv")
        .filter(col("SOPO_CO_RECEPTOR") === "MOVISTAR" && col("SOPO_CO_NRN_RECEPTORVIR") === "0")
        //.select( "SOPO_DS_MSISDN1")
        .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_d")
        .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
        .withColumn("portout_date", substring(col("portout_date"), 0, 10))
        .withColumnRenamed("ESTA_CO_ESTADO", "estado")
        .withColumn("label", lit(0.0))
        .withColumn("label", when(col("estado") === lit("ACAN"), 1.0).otherwise(col("label")))
        .select("msisdn_d", "label", "portout_date")
        .distinct()
    }

    // The last port-out is retained for each MSISDN

    val refday = Utils.getLastDayOfMonth(yearmonth)

    val window = Window
      .partitionBy("msisdn_d")
      .orderBy(asc("days_from_portout"))

    raw_ports
      .withColumn("portout_date", from_unixtime(unix_timestamp(col("portout_date"), "yyyy-MM-dd")))
      .withColumn("ref_date", from_unixtime(unix_timestamp( concat(lit(yearmonth), lit(refday)), "yyyyMMdd")) )
      .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
      .withColumn("rank", row_number().over(window))
      .filter(col("rank") === 1)
      .select("msisdn_d", "label")

  }

  def getLabeledWinbackCar(spark: SparkSession, port_month: String, segmentfilter: String, service_set:List[String], competitor: String, feats: List[String], target: String): Dataset[Row] = {

    // Loading the state of the services one month before

    val car_month = Utils.substractMonth(port_month, 2)

    val car_date = car_month + Utils.getLastDayOfMonth(car_month.substring(4,6))

    println("\n[Info Amdocs Winback Car Preparation] Date of the CAR: " + car_date + "\n")

    val cardf = spark
      .read
      .table("tests_es.jvmm_amdocs_car_mobile_complete_" + car_date + "_processed")

    println("\n[Info Amdocs Winback Car Preparation] Number of services in CAR of " + car_date + ": " + cardf.count() + "\n")

    // Loading port-out requests managed by Winback during port_month

    val labeled_ports = getWinbackRequestsUnderAnalysis(spark, port_month, segmentfilter, service_set, competitor)

    val labelcardf = cardf
      .join(labeled_ports, Seq("msisdn"), "inner")

    val car_feats = feats :+ target

    val labelfeatcar = labelcardf
      .select(car_feats.head, car_feats.tail:_*)

    println("\n[Info Amdocs Winback Car Preparation] Number of port-out requests characterised and labeled: " + labelfeatcar.count() + "\n")

    labelfeatcar

  }


}
