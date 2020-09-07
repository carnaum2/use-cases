package datapreparation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import metadata.Metadata

object AmdocsCarCrmPreprocessor {

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

    // 2.1. ____ Complete list of crm feats describing msisdn services ____

    val crm_columns = Metadata.getInitialCrmColumns() ++ Metadata.getIdFeats()

    // 2.2. ____ Values used for null imputation ____

    val avg_price_tariff = spark
      .read
      .parquet(origin)
      .filter(col("ClosingDay") === process_date)
      .filter(col("clase_cli_cod_clase_cliente") === "RS"
        && col("cod_estado_general").isin("01", "09")
        && col("rgu") === "movil"
        && col("price_tariff").isNotNull
      )
      .select(mean("price_tariff"))
      .rdd
      .map(r => r.getDouble(0))
      .first()

    ///////////// 3. Preparing data (using cached tmp dataframes to break lineage and prevent "janino" errors (compiler) duting execution) //////////////////

    // Filter -- col("cod_estado_general").isin("01", "09") -- is not applied. All the services are processed and the filtered will be used in the process toimplement the model
    val tmp1 = spark
      .read
      .parquet(origin)
      .filter(col("ClosingDay") === process_date)
      .filter(col("clase_cli_cod_clase_cliente") === "RS" && col("rgu") === "movil")
      .select(crm_columns.head, crm_columns.tail:_*)
      // msisdn: pk
      .withColumn("msisdn", col("msisdn").cast("string"))
      // codigo_postal:
      .withColumn("codigo_postal", col("codigo_postal").substr(0, 2))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp1: " + tmp1.count() + "\n")

    val tmp2 = tmp1
      // metodo_pago
      .withColumn("metodo_pago", when(col("metodo_pago").isin("", " "), "unknown").otherwise(col("metodo_pago")))
      .withColumn("metodo_pago", lower(col("metodo_pago")))
      .withColumn("metodo_pago", when(col("metodo_pago").contains("dom"), "domiciliado").otherwise(col("metodo_pago")))
      .withColumn("metodo_pago", when(col("metodo_pago").contains("subc"), "subcuenta").otherwise(col("metodo_pago")))
      .withColumn("metodo_pago", when(col("metodo_pago").contains("ventan"), "ventanilla").otherwise(col("metodo_pago")))
      .withColumn("metodo_pago", when(col("metodo_pago").contains("tarje"), "tarjeta").otherwise(col("metodo_pago")))
      .withColumn("metodo_pago", when(col("metodo_pago").isin("unknown", "domiciliado", "subcuenta", "ventanilla", "tarjeta") === false, "otros").otherwise(col("metodo_pago")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp2: " + tmp2.count() + "\n")

    val tmp3 = tmp2
      // cta_correo: gmail, hotmail, yahoo, otro, vacío (unknown)
      .withColumn("cta_correo", when(col("cta_correo").isin("", " "), "unknown").otherwise(col("cta_correo")))
      .withColumn("cta_correo", lower(col("cta_correo")))
      .withColumn("cta_correo", when(col("cta_correo").contains("gmail"), "gmail").otherwise(col("cta_correo")))
      .withColumn("cta_correo", when(col("cta_correo").contains("hotmail"), "hotmail").otherwise(col("cta_correo")))
      .withColumn("cta_correo", when(col("cta_correo").contains("yahoo"), "yahoo").otherwise(col("cta_correo")))
      .withColumn("cta_correo", when(col("cta_correo").isin("unknown", "gmail", "hotmail", "yahoo") === false, "otros").otherwise(col("cta_correo")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp3: " + tmp3.count() + "\n")

    val tmp4 = tmp3
      // factura_electronica: 1, 0, otros, vacío (unknown)
      .withColumn("factura_electronica", when(col("factura_electronica") === "1", "s").otherwise(col("factura_electronica")))
      .withColumn("factura_electronica", when(col("factura_electronica") === "0", "n").otherwise(col("factura_electronica")))
      .withColumn("factura_electronica", when(col("factura_electronica").isin("s", "n") === false, "unknown").otherwise(col("factura_electronica")))
      // superoferta: ON15, no ON15, otros, vacío (unknown)
      .withColumn("superoferta", when(col("superoferta").isin("", " "), "unknown").otherwise(col("superoferta")))
      .withColumn("superoferta", when(col("superoferta").isin("ON15", "unknown") === false, "otros").otherwise(col("superoferta")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp4: " + tmp4.count() + "\n")

    // tmp3.registerTempTable("qwerty")
    // val tmp3 = spark.table("qwerty")

    val tmp5 = tmp4
      // fecha_migracion: no migrado --> 1753-01-01; migrado --> fecha con formato yyyy-mm-dd
      .withColumn("fecha_migracion", from_unixtime(unix_timestamp(col("fecha_migracion"), "yyyy-MM-dd")))
      .withColumn("ref_date", from_unixtime(unix_timestamp(lit(process_date), "yyyyMMdd")))
      .withColumn("dias_desde_fecha_migracion", datediff(col("ref_date"), col("fecha_migracion")).cast("double"))
      .withColumn("dias_desde_fecha_migracion", when(col("dias_desde_fecha_migracion") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fecha_migracion")))
      .withColumn("cliente_migrado", when(col("dias_desde_fecha_migracion") === -1.0, "N").otherwise("S"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp5: " + tmp5.count() + "\n")

    val tmp6 = tmp5
      // tipo_documento: N.I.F., N.I.E., Pasaporte, Otros, vacío (unknown)
      .withColumn("tipo_documento", when(col("tipo_documento").isin("", " "), "unknown").otherwise(col("tipo_documento")))
      .withColumn("tipo_documento", when(col("tipo_documento").isin("N.I.F.", "DNI/NIF"), "nif").otherwise(col("tipo_documento")))
      .withColumn("tipo_documento", when(col("tipo_documento").isin("N.I.E.", "N.I.E.-Tar", "NIE"), "nie").otherwise(col("tipo_documento")))
      .withColumn("tipo_documento", when(col("tipo_documento").isin("Pasaporte", "PASAPORTE"), "pasaporte").otherwise(col("tipo_documento")))
      .withColumn("tipo_documento", when(col("tipo_documento").isin("unknown", "nif", "nie", "pasaporte") === false, "otros").otherwise(col("tipo_documento")))
      // x_publicidad_email: S, N, otro, unknown
      //.withColumn("x_publicidad_email", when(col("x_publicidad_email").isin("", " "), "unknown").otherwise(col("x_publicidad_email")))
      //.withColumn("x_publicidad_email", when(col("x_publicidad_email").isin("S", "N", "unknown") === false, "otros").otherwise(col("x_publicidad_email")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp6: " + tmp6.count() + "\n")

    val tmp7 = tmp6
      // nacionalidad_ Espa, Otros, vacío (unknown)
      .withColumn("nacionalidad", lower(col("nacionalidad")))
      .withColumn("nacionalidad", when(col("nacionalidad").isin("", " "), "unknown").otherwise(col("nacionalidad")))
      .withColumn("nacionalidad", when(col("nacionalidad").contains("espa"), "espana").otherwise(col("nacionalidad")))
      .withColumn("nacionalidad", when(col("nacionalidad").isin("unknown", "espana") === false, "otros").otherwise(col("nacionalidad")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp7: " + tmp7.count() + "\n")

    val tmp8 = tmp7
      // x_antiguedad_cuenta: <6 MESES, 1 A?O, 2 - 3 A?OS, 4 - 5 A?OS, 6 - 10 A?OS, >11 A?OS, vacío
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").isin("", " "), "unknown").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").contains("<6"), "upto6months").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when((col("x_antiguedad_cuenta").contains("1 A") === true) && (col("x_antiguedad_cuenta").contains("11") === false), "1year").otherwise(col("x_antiguedad_cuenta")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp8: " + tmp8.count() + "\n")

    val tmp9 = tmp8
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").contains("2 - 3"), "2to3years").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").contains("4 - 5"), "4to5years").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").contains("6 - 10"), "6to10years").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").contains(">11"), "gt11years").otherwise(col("x_antiguedad_cuenta")))
      .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").isin("unknown", "upto6months", "1year", "2to3years", "4to5years", "6to10years", "gt11years") === false, "otros").otherwise(col("x_antiguedad_cuenta")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp9: " + tmp9.count() + "\n")

    val tmp10 = tmp9
      // x_datos_navegacion: S, N, vacío
      .withColumn("x_datos_navegacion", when(col("x_datos_navegacion").isin("", " ") || col("x_datos_navegacion").isNull, "unknown").otherwise(col("x_datos_navegacion")))
      // x_datos_trafico: S, N, vacío
      .withColumn("x_datos_trafico", when(col("x_datos_trafico").isin("", " ") || col("x_datos_trafico").isNull, "unknown").otherwise(col("x_datos_trafico")))
      // x_cesion_datos: S, N, vacío
      .withColumn("x_cesion_datos", when(col("x_cesion_datos").isin("", " ") || col("x_cesion_datos").isNull, "unknown").otherwise(col("x_cesion_datos")))
      // x_user_facebook
      .withColumn("x_user_facebook", when(col("x_user_facebook").isin("", " ") || col("x_user_facebook").isNull, "n").otherwise("s"))
      // x_user_twitter
      .withColumn("x_user_twitter", when(col("x_user_twitter").isin("", " ") || col("x_user_twitter").isNull, "n").otherwise("s"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp10: " + tmp10.count() + "\n")

    val tmp11 = tmp10
      // marriage2hgbst_elm: Widowed, Divorced, Married, Couple, Single, Separated, vacío
      .withColumn("marriage2hgbst_elm", lower(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").isin("", " "), "unknown").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("wido") || col("marriage2hgbst_elm").contains("viud"), "viudo").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("divor"), "divorciado").otherwise(col("marriage2hgbst_elm")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp11: " + tmp11.count() + "\n")

    val tmp12 = tmp11
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("marr") || col("marriage2hgbst_elm").contains("casad"), "casado").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("coupl") || col("marriage2hgbst_elm").contains("parej"), "pareja").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("sing") || col("marriage2hgbst_elm").contains("solter"), "soltero").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").contains("separa"), "separado").otherwise(col("marriage2hgbst_elm")))
      .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").isin("unknown", "viudo", "divorciado", "casado", "pareja", "soltero", "separado") === false, "otros").otherwise(col("marriage2hgbst_elm")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp12: " + tmp12.count() + "\n")

    val tmp13 = tmp12
      // gender2hgbst_elm: Male, Female, vacío
      .withColumn("gender2hgbst_elm", lower(col("gender2hgbst_elm")))
      .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm").isin("", " "), "unknown").otherwise(col("gender2hgbst_elm")))
      .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm") === "male", "male").otherwise(col("gender2hgbst_elm")))
      .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm") === "female", "female").otherwise(col("gender2hgbst_elm")))
      .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm").isin("unknown", "male", "female") === false, "otros").otherwise(col("gender2hgbst_elm")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp13: " + tmp13.count() + "\n")

    val tmp14 = tmp13
      // flg_robinson: S, N, vacío
      .withColumn("flg_robinson", when(col("flg_robinson").isin("", " "), "unknown").otherwise(col("flg_robinson")))
      // x_formato_factura: PAPEL - ESTANDAR, ELECTRONICA - ESTANDAR, PAPEL - REDUCIDA, PAPEL - SEMIREDUCIDA, vacío
      .withColumn("x_formato_factura", when(col("x_formato_factura").isin("", " "), "unknown").otherwise(col("x_formato_factura")))
      .withColumn("x_formato_factura", when(col("x_formato_factura").isin("unknown", "PAPEL - ESTANDAR", "ELECTRONICA - ESTANDAR", "PAPEL - REDUCIDA", "PAPEL - SEMIREDUCIDA") === false, "otros").otherwise(col("x_formato_factura")))
      // x_idioma_factura: Castellano, Euskera, Gallego, Ingles, Catalan, vacío
      .withColumn("x_idioma_factura", when(col("x_idioma_factura").isin("", " "), "unknown").otherwise(col("x_idioma_factura")))
      .withColumn("x_idioma_factura", when(col("x_idioma_factura").isin("unknown", "Castellano", "Euskera", "Gallego", "Ingles", "Catalan") === false, "otros").otherwise(col("x_idioma_factura")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp14: " + tmp14.count() + "\n")

    val tmp15 = tmp14
      // bam_services, bam_fx_first: num bam services of NUM_CLIENTE, install date (yyyymmdd) of the first bam service; the date is null if bam_services = 0
      .withColumn("bam_fx_first", from_unixtime(unix_timestamp(col("bam_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_bam_fx_first", datediff(col("ref_date"), col("bam_fx_first")).cast("double"))
      .withColumn("dias_desde_bam_fx_first", when(col("dias_desde_bam_fx_first").isNull || col("dias_desde_bam_fx_first").isNaN, -1.0).otherwise(col("dias_desde_bam_fx_first")))
      // bam-movil_services, bam-movil_fx_first
      .withColumn("bam-movil_fx_first", from_unixtime(unix_timestamp(col("bam-movil_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_bam-movil_fx_first", datediff(col("ref_date"), col("bam-movil_fx_first")).cast("double"))
      .withColumn("dias_desde_bam-movil_fx_first", when(col("dias_desde_bam-movil_fx_first").isNull || col("dias_desde_bam-movil_fx_first").isNaN, -1.0).otherwise(col("dias_desde_bam-movil_fx_first")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp15: " + tmp15.count() + "\n")

    val tmp16 = tmp15
      // fbb_services, fbb_fx_first
      .withColumn("fbb_fx_first", from_unixtime(unix_timestamp(col("fbb_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_fbb_fx_first", datediff(col("ref_date"), col("fbb_fx_first")).cast("double"))
      .withColumn("dias_desde_fbb_fx_first", when(col("dias_desde_fbb_fx_first").isNull || col("dias_desde_fbb_fx_first").isNaN, -1.0).otherwise(col("dias_desde_fbb_fx_first")))
      // fixed_services, fixed_fx_first
      .withColumn("fixed_fx_first", from_unixtime(unix_timestamp(col("fixed_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_fixed_fx_first", datediff(col("ref_date"), col("fixed_fx_first")).cast("double"))
      .withColumn("dias_desde_fixed_fx_first", when(col("dias_desde_fixed_fx_first").isNull || col("dias_desde_fixed_fx_first").isNaN, -1.0).otherwise(col("dias_desde_fixed_fx_first")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp16: " + tmp16.count() + "\n")

    val tmp17 = tmp16
      // movil_services, movil_fx_first
      .withColumn("movil_fx_first", from_unixtime(unix_timestamp(col("movil_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_movil_fx_first", datediff(col("ref_date"), col("movil_fx_first")).cast("double"))
      .withColumn("dias_desde_movil_fx_first", when(col("dias_desde_movil_fx_first").isNull || col("dias_desde_movil_fx_first").isNaN, -1.0).otherwise(col("dias_desde_movil_fx_first")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp17: " + tmp17.count() + "\n")

    val tmp18 = tmp17
      // prepaid_services, prepaid_fx_first
      .withColumn("prepaid_fx_first", from_unixtime(unix_timestamp(col("prepaid_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_prepaid_fx_first", datediff(col("ref_date"), col("prepaid_fx_first")).cast("double"))
      .withColumn("dias_desde_prepaid_fx_first", when(col("dias_desde_prepaid_fx_first").isNull || col("dias_desde_prepaid_fx_first").isNaN, -1.0).otherwise(col("dias_desde_prepaid_fx_first")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp18: " + tmp18.count() + "\n")

    val tmp19 = tmp18
      // tv_services, tv_fx_first
      .withColumn("tv_fx_first", from_unixtime(unix_timestamp(col("tv_fx_first"), "yyyyMMdd")))
      .withColumn("dias_desde_tv_fx_first", datediff(col("ref_date"), col("tv_fx_first")).cast("double"))
      .withColumn("dias_desde_tv_fx_first", when(col("dias_desde_tv_fx_first").isNull || col("dias_desde_tv_fx_first").isNaN, -1.0).otherwise(col("dias_desde_tv_fx_first")))
      // total_num_services
      .withColumn("total_num_services", lit(0.0))
      .withColumn("total_num_services", col("bam_services") + col("bam-movil_services") + col("fbb_services") + col("fixed_services") + col("movil_services") + col("prepaid_services") + col("tv_services"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp19: " + tmp19.count() + "\n")

    val tmp20 = tmp19
      // fx_srv_basic: antiguedad del servicio básico
      .withColumn("fx_srv_basic", from_unixtime(unix_timestamp(col("fx_srv_basic"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_srv_basic", datediff(col("ref_date"), col("fx_srv_basic")).cast("double"))
      .withColumn("dias_desde_fx_srv_basic", when(col("dias_desde_fx_srv_basic").isNull || col("dias_desde_fx_srv_basic").isNaN, -1.0).otherwise(col("dias_desde_fx_srv_basic")))
      // tipo_sim: MTSIM, SIMVF, NULL (aplicable a movil y bam, principalmente)
      .withColumn("tipo_sim", when(col("tipo_sim").isin("", " ") || col("tipo_sim").isNull, "unknown").otherwise(col("tipo_sim")))
      .withColumn("tipo_sim", when(col("tipo_sim").isin("unknown", "MTSIM", "SIMVF") === false, "otros").otherwise(col("tipo_sim")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp20: " + tmp20.count() + "\n")

    val tmp21 = tmp20
      // tariff: 97 categorías diferentes (incluyendo NULL)
      // most popular tariffs for mobile services: TIL2G, T2C1G, TS215, TSP5M, TCP8M, TIS3G, TARSS, T2S1G, TN230, TIL4G, NULL, TPMNV, NTP1M, TJDSM, TA500, SYUTA, TSP1G, TN200, TIS2G, TN500
      .withColumn("tariff", when(col("tariff").isin("", " ") || col("tariff").isNull, "unknown").otherwise(col("tariff")))
      .withColumn("tariff", when(col("tariff").isin("unknown", "TIL2G", "T2C1G", "TS215", "TSP5M", "TCP8M", "TIS3G", "TARSS", "T2S1G", "TN230", "TIL4G", "TPMNV", "NTP1M", "TJDSM", "TA500", "SYUTA", "TSP1G", "TN200", "TIS2G", "TN500") === false, "otros").otherwise(col("tariff")))
      // 2 line
      .withColumn("segunda_linea", lit("n"))
      .withColumn("segunda_linea", when((col("desc_tariff").contains("2") && col("desc_tariff").contains("nea")) || (col("desc_tariff").contains("+")), "s").otherwise(col("segunda_linea")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp21: " + tmp21.count() + "\n")

    val tmp22 = tmp21
      // tariff_desc
      .withColumn("desc_tariff", when(col("desc_tariff").isin("", " ") || col("desc_tariff").isNull, "unknown").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("XS"), "xs").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Red M"), "redm").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Red L"), "redl").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Smart"), "smart").otherwise(col("desc_tariff")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp22: " + tmp22.count() + "\n")

    val tmp23 = tmp22
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Mini M"), "minim").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Tarifa Plana 200 min"), "plana200min").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("Tarifa Plana Minutos Ilimitados"), "planaminilim").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("+L") && col("desc_tariff").contains("neas Mini"), "maslineasmini").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").contains("egayuser"), "megayuser").otherwise(col("desc_tariff")))
      .withColumn("desc_tariff", when(col("desc_tariff").isin("unknown", "xs", "redm", "redl", "smart", "minim", "plana200min", "planaminilim", "maslineasmini", "megayuser") === false, "otros").otherwise(col("desc_tariff")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp23: " + tmp23.count() + "\n")

    val tmp24 = tmp23
      // fx_tariff
      .withColumn("fx_tariff", from_unixtime(unix_timestamp(col("fx_tariff"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_tariff", datediff(col("ref_date"), col("fx_tariff")).cast("double"))
      .withColumn("dias_desde_fx_tariff", when(col("dias_desde_fx_tariff").isNull || col("dias_desde_fx_tariff").isNaN || col("dias_desde_fx_tariff") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_tariff")))
      // price_tariff: null imputed as avg; ideally, random sample from the rest of non-null values
      .withColumn("price_tariff", when(col("price_tariff").isNull, avg_price_tariff).otherwise(col("price_tariff")))
      // voice_tariff: TV200, TVILI, null, TVPXM, TPDVZ
      .withColumn("voice_tariff", when(col("voice_tariff").isin("", " ") || col("voice_tariff").isNull, "unknown").otherwise(col("voice_tariff")))
      .withColumn("voice_tariff", when(col("voice_tariff").isin("unknown", "TV200", "TVILI", "TVPXM", "TPDVZ") === false, "otros").otherwise(col("voice_tariff")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp24: " + tmp24.count() + "\n")

    val tmp25 = tmp24
      // fx_voice_tariff
      .withColumn("fx_voice_tariff", from_unixtime(unix_timestamp(col("fx_voice_tariff"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_voice_tariff", datediff(col("ref_date"), col("fx_voice_tariff")).cast("double"))
      .withColumn("dias_desde_fx_voice_tariff", when(col("dias_desde_fx_voice_tariff").isNull || col("dias_desde_fx_voice_tariff").isNaN || col("dias_desde_fx_voice_tariff") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_voice_tariff")))
      // data (most popular categories): TD15G, TD2GB, null, TDA1G, T500M, TG300, T1GBD, TD6GB, TPDMD, TD100, TPDRD, TD800, otros
      .withColumn("data", when(col("data").isin("", " ") || col("data").isNull, "unknown").otherwise(col("data")))
      .withColumn("data", when(col("data").isin("unknown", "TD15G", "TD2GB", "TDA1G", "T500M", "TG300", "T1GBD", "TD6GB", "TPDMD", "TD100", "TPDRD", "TD800") === false, "otros").otherwise(col("data")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp25: " + tmp25.count() + "\n")

    val tmp26 = tmp25
      // fx_data
      .withColumn("fx_data", from_unixtime(unix_timestamp(col("fx_data"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_data", datediff(col("ref_date"), col("fx_data")).cast("double"))
      .withColumn("dias_desde_fx_data", when(col("dias_desde_fx_data").isNull || col("dias_desde_fx_data").isNaN || col("dias_desde_fx_data") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_data")))
      // dto_lev1: null, DTSCO, DM100, DTIOR, DTCR9, DTCR7, otros
      .withColumn("dto_lev1", when(col("dto_lev1").isin("", " ") || col("dto_lev1").isNull, "none").otherwise(col("dto_lev1")))
      .withColumn("dto_lev1", when(col("dto_lev1").isin("none", "DTSCO", "DM100", "DTIOR", "DTCR9", "DTCR7") === false, "otros").otherwise(col("dto_lev1")))
      // fx_dto_lev1
      .withColumn("fx_dto_lev1", from_unixtime(unix_timestamp(col("fx_dto_lev1"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_dto_lev1", datediff(col("ref_date"), col("fx_dto_lev1")).cast("double"))
      .withColumn("dias_desde_fx_dto_lev1", when(col("dias_desde_fx_dto_lev1").isNull || col("dias_desde_fx_dto_lev1").isNaN || col("dias_desde_fx_dto_lev1") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_dto_lev1")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp26: " + tmp26.count() + "\n")

    val tmp27 = tmp26
      // price_dto_lev1
      .withColumn("price_dto_lev1", when(col("price_dto_lev1").isin("", " ") || col("price_dto_lev1").isNull, 0.0).otherwise(col("price_dto_lev1")))
      // dto_lev2: null, DMXSG, DSC06, DSC04, DMTO3, DMTO2, DCRSM, otros
      .withColumn("dto_lev2", when(col("dto_lev2").isin("", " ") || col("dto_lev2").isNull, "none").otherwise(col("dto_lev2")))
      .withColumn("dto_lev2", when(col("dto_lev2").isin("none", "DMXSG", "DSC06", "DSC04", "DMTO3", "DMTO2", "DCRSM") === false, "otros").otherwise(col("dto_lev2")))
      // fx_dto_lev2
      .withColumn("fx_dto_lev2", from_unixtime(unix_timestamp(col("fx_dto_lev2"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_dto_lev2", datediff(col("ref_date"), col("fx_dto_lev2")).cast("double"))
      .withColumn("dias_desde_fx_dto_lev2", when(col("dias_desde_fx_dto_lev2").isNull || col("dias_desde_fx_dto_lev2").isNaN || col("dias_desde_fx_dto_lev2") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_dto_lev2")))
      // price_dto_lev2
      .withColumn("price_dto_lev2", when(col("price_dto_lev2").isin("", " ") || col("price_dto_lev2").isNull, 0.0).otherwise(col("price_dto_lev2")))
      // real_price = price_tariff - dtos
      .withColumn("real_price", col("price_tariff") + col("price_dto_lev1") + col("price_dto_lev1"))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp27: " + tmp27.count() + "\n")

    val tmp28 = tmp27
      // data_additional: null, TPEXC, 10G1A, S2OOB, otros
      .withColumn("data_additional", when(col("data_additional").isin("", " ") || col("data_additional").isNull, "none").otherwise(col("data_additional")))
      .withColumn("data_additional", when(col("data_additional").isin("none", "TPEXC", "10G1A", "S2OOB") === false, "otros").otherwise(col("data_additional")))
      // fx_data_additional
      .withColumn("fx_data_additional", from_unixtime(unix_timestamp(col("fx_data_additional"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_data_additional", datediff(col("ref_date"), col("fx_data_additional")).cast("double"))
      .withColumn("dias_desde_fx_data_additional", when(col("dias_desde_fx_data_additional").isNull || col("dias_desde_fx_data_additional").isNaN || col("dias_desde_fx_data_additional") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_data_additional")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp28: " + tmp28.count() + "\n")

    val tmp29 = tmp28
      // roam_zona_2: BSMS2, null, BVIL2, otors
      .withColumn("roam_zona_2", when(col("roam_zona_2").isin("", " ") || col("roam_zona_2").isNull, "unknown").otherwise(col("roam_zona_2")))
      .withColumn("roam_zona_2", when(col("roam_zona_2").isin("unknown", "BSMS2", "BVIL2") === false, "otros").otherwise(col("roam_zona_2")))
      // fx_roam_zona_2
      .withColumn("fx_roam_zona_2", from_unixtime(unix_timestamp(col("fx_roam_zona_2"), "yyyyMMdd")))
      .withColumn("dias_desde_fx_roam_zona_2", datediff(col("ref_date"), col("fx_roam_zona_2")).cast("double"))
      .withColumn("dias_desde_fx_roam_zona_2", when(col("dias_desde_fx_roam_zona_2").isNull || col("dias_desde_fx_roam_zona_2").isNaN || col("dias_desde_fx_roam_zona_2") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fx_roam_zona_2")))
      // sim_vf: 1 or NULL
      .withColumn("sim_vf", col("sim_vf").cast("string"))
      .withColumn("sim_vf", when(col("sim_vf").isin("", " ") || col("sim_vf").isNull, "0").otherwise(col("sim_vf")))
      .cache()

    println("\n[Info Amdocs Car Preparation] Size of tmp29: " + tmp29.count() + "\n")

    tmp29
      .write
      .format("parquet")
      .mode("overwrite")
      //.partitionBy("year","month")
      .saveAsTable("tests_es.jvmm_amdocs_car_mobile_crm_" + process_date + "_processed_new")

    println("\n[Info Amdocs Car Preparation] Car CRM cleaned and saved\n")

  }

}
