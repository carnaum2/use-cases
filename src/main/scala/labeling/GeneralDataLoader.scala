package labeling

import metadata.Metadata
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count}

object GeneralDataLoader {

  // List of input features given the name of the set

  def getInputFeats(feats: List[String]): List[String] = {

    def getFeatSingleSet(set: String): List[String] = {

      set match {

        case "crm_raw" => Metadata.getInitialCrmColumns()

        case "crm_new" => Metadata.getNewCrmColumns()

        case "gnv_raw" => Metadata.getInitialGnvColumns()

        case "gnv_new" => Metadata.getNewGnvColumns()

        case "gnv_all" => Metadata.getInitialGnvColumns().union(Metadata.getNewGnvColumns())

        case "camp_raw" => Metadata.getInitialCampColumns()

        case "camp_new" => Metadata.getNewCampColumns()

        case "camp_all" => Metadata.getInitialCampColumns().union(Metadata.getNewCampColumns())

        case "billing_raw" => Metadata.getInitialBillingColumns()

        case "billing_new" => Metadata.getNewBillingColumns()

        case "billing_all" => Metadata.getNewBillingColumns().union(Metadata.getInitialBillingColumns())

        case "all_reduced" => Metadata.getFeatSubset(List("gnv_raw", "camp_raw"))

        case "all" => Metadata.getFeatSubset(List[String]())
      }
    }

    if(feats.isEmpty) {
      List[String]()
    } else {
      getFeatSingleSet(feats.head) ++ getInputFeats(feats.tail)
    }
  }

  // Identifying num_cliente in the specified segment

  def getNumClienteUnderAnalysis(spark: SparkSession, car_date: String, segmentfilter: String): Dataset[Row] = {

    val targetIdDt = segmentfilter match {

      case "onlymob" => {

        // 1.1. Target population: num_cliente

        val target_cliente = spark
          .read
          .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
          .filter(col("ClosingDay") === car_date)
          .filter(col("clase_cli_cod_clase_cliente") === "RS"
            && col("cod_estado_general").isin("01", "09")
            && col("rgu").isNotNull)
          .groupBy("num_cliente")
          .pivot("rgu")
          .agg(count("*"))
          .na.fill(0)
          .filter(col("movil") >= 1 && col("fbb") === 0 && col("tv") === 0 && col("fixed") === 0 && col("bam") === 0 && col("bam-movil") === 0)
          .select("num_cliente")
          .distinct()

        println("\n[Info DataLoader] Target segment identified - Num customers in the segment: " + target_cliente.count() + "\n")

        target_cliente

      }

      case "onlymobmono" => {

        // 1.1. Target population: num_cliente

        val target_cliente = spark
          .read
          .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
          .filter(col("ClosingDay") === car_date)
          .filter(col("clase_cli_cod_clase_cliente") === "RS"
            && col("cod_estado_general").isin("01", "09")
            && col("rgu").isNotNull)
          .groupBy("num_cliente")
          .pivot("rgu")
          .agg(count("*"))
          .na.fill(0)
          .filter(col("movil") === 1 && col("fbb") === 0 && col("tv") === 0 && col("fixed") === 0 && col("bam") === 0 && col("bam-movil") === 0)
          .select("num_cliente")
          .distinct()

        println("\n[Info DataLoader] Target segment identified - Num customers in the segment: " + target_cliente.count() + "\n")

        target_cliente

      }

      case "onlymobmulti" => {

        // 1.1. Target population: num_cliente

        val target_cliente = spark
          .read
          .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
          .filter(col("ClosingDay") === car_date)
          .filter(col("clase_cli_cod_clase_cliente") === "RS"
            && col("cod_estado_general").isin("01", "09")
            && col("rgu").isNotNull)
          .groupBy("num_cliente")
          .pivot("rgu")
          .agg(count("*"))
          .na.fill(0)
          .filter(col("movil") > 1 && col("fbb") === 0 && col("tv") === 0 && col("fixed") === 0 && col("bam") === 0 && col("bam-movil") === 0)
          .select("num_cliente")
          .distinct()

        println("\n[Info DataLoader] Target segment identified - Num customers in the segment: " + target_cliente.count() + "\n")

        target_cliente

      }

      case "mobileandfbb" => {

        // spark.read.parquet("/user/hive/warehouse/tests_es.db/rbl_ids_srv_20180331").filter(col("clase_cli_cod_clase_cliente") === "RS" && col("cod_estado_general").isin(List("01", "09"):_*)).groupBy("num_cliente").pivot("rgu").agg(count("*")).na.fill(0).filter(col("movil") > 0 && col("fbb") > 0).count

        // 1.1. Target population: num_cliente

        val target_cliente = spark
          .read
          .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
          .filter(col("ClosingDay") === car_date)
          .filter(col("clase_cli_cod_clase_cliente") === "RS"
            && col("cod_estado_general").isin("01", "09")
            && col("rgu").isNotNull)
          .groupBy("num_cliente")
          .pivot("rgu")
          .agg(count("*"))
          .na.fill(0)
          .filter(col("movil") > 0 && col("fbb") > 0)
          .select("num_cliente")
          .distinct()

        println("\n[Info DataLoader] Target segment identified - Num customers in the segment: " + target_cliente.count() + "\n")

        target_cliente

      }

      case "allmob" => {

        // spark.read.parquet("/user/hive/warehouse/tests_es.db/rbl_ids_srv_20180331").filter(col("clase_cli_cod_clase_cliente") === "RS" && col("cod_estado_general").isin(List("01", "09"):_*)).groupBy("num_cliente").pivot("rgu").agg(count("*")).na.fill(0).filter(col("movil") > 0 && col("fbb") > 0).count

        // 1.1. Target population: num_cliente

        val target_cliente = spark
          .read
          .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
          .filter(col("ClosingDay") === car_date)
          .filter(col("clase_cli_cod_clase_cliente") === "RS"
            && col("cod_estado_general").isin("01", "09")
            && col("rgu").isNotNull)
          .groupBy("num_cliente")
          .pivot("rgu")
          .agg(count("*"))
          .na.fill(0)
          .filter(col("movil") > 0)
          .select("num_cliente")
          .distinct()

        println("\n[Info DataLoader] Target segment identified - Num customers in the segment: " + target_cliente.count() + "\n")

        target_cliente

      }

    }

    targetIdDt

  }

  // Identifying msisdn of interest (service_set) in the specified segment (segmentfilter)

  def getServicesUnderAnalysis(spark: SparkSession, car_date: String, segmentfilter: String, service_set: List[String]): Dataset[Row] = {

    // Initially, the customers in the specified segment
    val target_cliente = getNumClienteUnderAnalysis(spark, car_date, segmentfilter)

    // Services of interest (service_set) in the specified segment

    val services = spark
      .read
      .parquet("/user/hive/warehouse/tests_es.db/amdocs_ids_srv_v2")
      .filter(col("ClosingDay") === car_date)
      .filter(col("rgu").isin(service_set:_*)
        && col("clase_cli_cod_clase_cliente") === "RS"
        && col("cod_estado_general").isin("01", "09")
        && col("srv_basic").isin("MRSUI", "MPSUI") === false)
      .select("msisdn", "num_cliente", "campo2")

    println("\n[Info DataLoader] Number of services of active RS customers: " + services.count() + "\n")

    // Services in the specified segment

    val target_services = target_cliente
      .join(services, Seq("num_cliente"), "inner")
      .select("msisdn")
      //.withColumnRenamed("msisdn", "msisdn_a")
      //.withColumnRenamed("campo2", "msisdn_d")

    println("\n[Info DataLoader] Number of services of interest in the targeted segment:: " + target_services.count() + "\n")

    target_services

  }

}
