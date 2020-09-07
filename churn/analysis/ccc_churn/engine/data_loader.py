

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, concat_ws, \
    upper, sum as sql_sum, lpad, concat, asc

def _get_ccc_tipis_info(spark, ccc_end_date_, ccc_start_date_):
    from churn.resources.call_centre_calls import CallCentreCalls
    #from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls

    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date_, ccc_start_date_)
    # df_agg_original = ccc.add_l2_ccc_variables(ccc_end_date_)
    # df_agg_original = ccc.get_ccc_service_df(ccc_end_date_, ccc_start_date_, add_l2_vars=True)
    df_all = ccc.all_interactions
    df_all = df_all.withColumn("fx_interaction",
                               concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))


    # --- UCI
    fichero_tipi = "/data/udf/vf_es/churn/additional_data/tipis_uci.csv"
    df_tipis_uci = spark.read.option("delimiter", ";").option("header", True).csv(fichero_tipi)
    df_tipis_uci = df_tipis_uci.withColumn("tres_tipis", concat_ws("__", upper(col("tipo")), upper(col("subtipo")),
                                                                   upper(col("razon"))))
    df_tipis_uci = df_tipis_uci.withColumn("dos_tipis", concat_ws("__", upper(col("tipo")), upper(col("subtipo"))))
    lista_tipis = df_tipis_uci.select("tres_tipis").distinct().rdd.map(lambda x: x["tres_tipis"]).collect()
    lista_tipis_dos = df_tipis_uci.select("dos_tipis").distinct().rdd.map(lambda x: x["dos_tipis"]).collect()

    # --- ABONOS
    fichero_tipi_abonos = "/data/udf/vf_es/churn/additional_data/tipis_abonos.csv"
    df_tipis_abonos = spark.read.option("delimiter", ";").option("header", True).csv(fichero_tipi_abonos)
    df_tipis_abonos = df_tipis_abonos.withColumn("tres_tipis",
                                                 concat_ws("__", upper(col("tipo")), upper(col("subtipo")),
                                                           upper(col("razon"))))
    lista_tipis_abono = df_tipis_abonos.select("tres_tipis").distinct().rdd.map(lambda x: x["tres_tipis"]).collect()

    # --- TIPIS FACTURA
    fichero_tipi_factura = "/data/udf/vf_es/churn/additional_data/tipis_error_incidencias_factura.csv"
    df_tipis_factura = spark.read.option("delimiter", ";").option("header", True).csv(fichero_tipi_factura)
    df_tipis_factura = df_tipis_factura.withColumn("tres_tipis",
                                                   concat_ws("__", upper(col("INT_TIPO")), upper(col("INT_SUBTIPO")),
                                                             upper(col("INT_RAZON"))))
    lista_tipis_factura = df_tipis_factura.select("tres_tipis").distinct().rdd.map(lambda x: x["tres_tipis"]).collect()

    # --- TIPIS PERMANENCIA/COMPROMISO/DESCUENTOS
    fichero_tipi_permanencia_dctos = "/data/udf/vf_es/churn/additional_data/tipis_permanencia_compromiso_descuentos.csv"
    df_tipis_permanencia_dctos = spark.read.option("delimiter", ";").option("header", True).csv(
        fichero_tipi_permanencia_dctos)
    df_tipis_permanencia_dctos = df_tipis_permanencia_dctos.withColumn("tres_tipis",
                                                                       concat_ws("__", upper(col("INT_TIPO")),
                                                                                 upper(col("INT_SUBTIPO")),
                                                                                 upper(col("INT_RAZON"))))
    lista_tipis_perm_dctos = df_tipis_permanencia_dctos.select("tres_tipis").distinct().rdd.map(
        lambda x: x["tres_tipis"]).collect()

    df_all = df_all.withColumn("tres", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo")),
                                                 upper(col("INT_Razon"))))

    # la llamada esta en la lista de tipis que generan churn
    df_all = df_all.withColumn("TIPIS_UCI", when(col("tres").isin(lista_tipis), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_ABONOS", when(col("tres").isin(lista_tipis_abono), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_FACTURA", when(col("tres").isin(lista_tipis_factura), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_PERMANENCIA_DCTOS",
                               when(col("tres").isin(lista_tipis_perm_dctos), 1).otherwise(0))

    return df_all



def get_all_ports(spark, start_port, end_port, closing_day):
    df_port = get_port(spark, start_port, end_port)
    df_fixed_port = get_fixed_port(spark, start_port, end_port, closing_day)

    from pykhaos.utils.pyspark_utils import union_all
    df = union_all([df_port, df_fixed_port])
    df = df.drop_duplicates(["msisdn"])
    df = df.withColumn("SOPO", col("SOPO").cast("integer"))
    return df



def get_port(spark, start_port, end_port):

    from churn.datapreparation.general.data_loader import get_port_requests_table

    select_cols = ["msisdn_a", "label", "portout_date"]

    df_port = get_port_requests_table(spark, start_date=start_port, end_date=end_port, ref_date=None,
                                      select_cols=select_cols).withColumnRenamed("msisdn_a", "msisdn")
    df_port = df_port.withColumn("SOPO", lit(1.0)).drop("label")

    return df_port

def get_dx(spark, start_bajas, end_bajas):

    from churn.utils.general_functions import amdocs_table_reader

    service_cols = ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"]

    df_services_start = (amdocs_table_reader(spark, "service", start_bajas, new=False)
                  .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
                  .where(col("rgu").isNotNull())
                  .select(service_cols)
                  .withColumnRenamed("num_cliente", "num_cliente_service"))

    df_services_end = (amdocs_table_reader(spark, "service", end_bajas, new=False)
                  .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
                  .where(col("rgu").isNotNull())
                  .select(service_cols)
                  .withColumnRenamed("num_cliente", "num_cliente_service"))

    churn_base = df_services_start\
    .join(df_services_end.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn").withColumn("SOPO", lit(1.0))\
    .distinct()

    return churn_base



def get_fixed_port(spark, start_port, end_port, closing_day):
    '''

    :param spark:
    :param start_port:
    :param end_port:
    :param closing_day: base clients to obtain the msisdn from the msisdn_d
    :return:
    '''
    from pykhaos.utils.date_functions import convert_to_date

    df_fix_ports = spark.read.option("delimiter", "|").option("header", True).csv(
        "/data/udf/vf_es/churn/nodo_portas_fix")

    df_fix_ports = (df_fix_ports
                    .filter(col("INICIO_RANGO") == col("FIN_RANGO"))
                    .withColumnRenamed("INICIO_RANGO", "msisdn_d")
                    .select("msisdn_d", "FECHA_INSERCION_SGP")
                    .distinct()
                    .withColumn("SOPO", lit(1.0))
                    .where((col('FECHA_INSERCION_SGP') >= convert_to_date(start_port)) & (col('FECHA_INSERCION_SGP') <= convert_to_date(end_port)))
                    .withColumnRenamed('FECHA_INSERCION_SGP', 'portout_date'))

    from churn.datapreparation.general.data_loader import get_active_services

    df_services = (get_active_services(spark, closing_day, new=False,
                                              service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic",
                                                            "campo1"],
                                              customer_cols=["num_cliente", "nif_cliente"])
                          .withColumnRenamed("num_cliente_service", "num_cliente")
                          #.where(col("rgu").rlike("^mobile$|^movil$"))
                   ).withColumnRenamed("campo2", "msisdn_d")

    df_fix_ports = df_fix_ports.join(df_services.select("msisdn", "msisdn_d"), ["msisdn_d"], how="left").drop("msisdn_d")


    return df_fix_ports


def get_ccc_data(spark, ccc_end, ccc_start):
    df_ccc = _get_ccc_tipis_info(spark, ccc_end, ccc_start)
    df_ccc = df_ccc.select("nif", "msisdn", 'fx_interaction', 'TIPIS_UCI', 'TIPIS_ABONOS', 'TIPIS_FACTURA',
                           'TIPIS_PERMANENCIA_DCTOS', "INT_TIPO",
                                                      "INT_SUBTIPO",
                                                      "INT_RAZON")
    df_ccc = df_ccc.where(~col("INT_TIPO").rlike("PORTABILIDAD|DESACTIVACION|BAJA|CANCELACION"))

    df_ccc = df_ccc.withColumn("TIPIS_INFO", when(col("INT_TIPO").rlike("INFORMACION"), 1).otherwise(0))

    df_ccc = df_ccc.withColumn("TIPIS_INFO_NOCOMP", when( (col("INT_TIPO").rlike("INFORMACION")) & (col("INT_SUBTIPO")=="NO COMPLETADA"), 1).otherwise(0))
    df_ccc = df_ccc.withColumn("TIPIS_INFO_COMP", when( (col("INT_TIPO").rlike("INFORMACION")) & (col("INT_SUBTIPO")=="COMPLETADA"), 1).otherwise(0))

    # from pyspark.sql.window import Window
    # from pyspark.sql.functions import row_number

    # wndw_fx_interaction = Window().partitionBy("msisdn").orderBy(asc("fx_interaction"))
    # # In case of more than one interaction by msisdn, we keep the first one
    # df_ccc = (df_ccc.withColumn("rowNum", row_number().over(wndw_fx_interaction))
    #           .where(col('rowNum') == 1).drop("rowNum"))
    df_ccc = df_ccc.withColumn("CCC", lit(1))

    return df_ccc


def get_tgs(spark, tgs_closing_day, tgs_yyyymm):
    from churn.datapreparation.general.tgs_data_loader import get_tgs

    df_tgs = get_tgs(spark, tgs_closing_day, yyyymm=tgs_yyyymm, impute_nulls=False, force_keep_cols=["FECHA_FIN_DTO"])
    df_tgs = df_tgs.select("msisdn", "tgs_fecha_fin_dto")
    df_tgs = df_tgs.where(col("msisdn").isNotNull())
    df_tgs = df_tgs.drop_duplicates(["msisdn"])
    return df_tgs