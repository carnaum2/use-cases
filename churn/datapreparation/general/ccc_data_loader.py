# coding: utf-8

from pyspark.sql.functions import col

from churn.resources.call_centre_calls import CallCentreCalls

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, concat, concat_ws, date_format, dayofmonth, format_string, from_unixtime, length, \
	lit, lower, lpad, month, regexp_replace, translate, udf, unix_timestamp, year, when, upper, collect_set, collect_list, \
    count as sql_count, min as sql_min, max as sql_max, struct, size, coalesce, sum as sql_sum
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException
#
from pykhaos.utils.pyspark_utils import format_date, compute_diff_days, sum_horizontal
from collections import Counter
import time
from churn.datapreparation.general.data_loader import get_port_requests_table
from pykhaos.utils.date_functions import move_date_n_days
import re

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

CAT_PRECIO = "PRECIO"
CAT_TERMINAL = "TERMINAL"
CAT_CONTENIDOS = "CONTENIDOS"
CAT_COMERCIAL = "COMERCIAL"
CAT_NO_COMERCIAL = "NO_COMERCIAL"
CAT_SERV_ATENCION = "SERVICIO/ATENCION"
CAT_TECNICO = "TECNICO"
CAT_BILLING = "BILLING"
CAT_FRAUD = "FRAUD"
NO_PROB = "NO_PROB"
NA = "NA"

EXTRA_FEATS_PREFIX = "ccc_"

# ADD CATEGORIES TO CALLS
REASON1_PRECIO = ["Cliente quiere pagar menos", "Clte quiere pagar menos", "Ofrecimiento comercial", "Precios y Promociones", "Plan de precios", "Planes de precios"]

def __set_categories(df_all):

    start_time = time.time()

    df_all = df_all.withColumn("is_fraud", when(col("INT_RAZON").rlike("(?i)enga.o comercial|venta enga.osa") ,1).otherwise(0))
    df_all = df_all.withColumn("Queja_Trato", when(col("INT_SUBTIPO").isin(["Queja trato", "Quejas sevicios de atencion"])
                                    ,1).otherwise(0))

    df_all = df_all.withColumn("CATEGORY_2",
                               # - - - - - - - - - PRECIO
                               when(col('INT_TIPO').rlike(
                                   "(?i)cliente quiere pagar menos|clte quiere pagar menos|ofrecimiento comercial|precios y promociones|plan de precios|planes de precios"),
                                    CAT_PRECIO)
                               # - - - - - - - - - SERVICIO/ATENCION
                               .when(lower(col("INT_SUBTIPO")).isin
                                   (["Queja trato".lower(), "Quejas sevicios de atencion".lower()]) ,CAT_SERV_ATENCION)
                               # - - - - - - - - - CONTENIDOS
                               .when(col("INT_TIPO").rlike("(?i)productos"), CAT_CONTENIDOS)
                               # - - - - - - - - - ENGAÑO
                               .when(col("INT_RAZON").rlike("(?i)enga.o comercial|venta enga.osa"), CAT_FRAUD)
                               # - - - - - - - - - TECNICO
                               # FIXME "INT_TIPO" = Red/Asistencia Tecnica
                               .when(col('INT_TIPO').rlike('(?i)^Averia') ,CAT_TECNICO)
                               .when((col('INT_TIPO').rlike('(?i)^Transferencia') )&
                                   (col("INT_SUBTIPO").rlike('(?i)^Aver.as')), CAT_TECNICO)
                               .when(col('INT_TIPO').rlike(
                                   '(?i)^Inc Provis.*Neba|^Inc Provision Fibra|^Inc Provision DSL|^Incidencia%ecnica|^Incidencia%SGI|^Inc|^Incidencia'),
                                     CAT_TECNICO)
                               .when(col('INT_TIPO').rlike('(?i)^Consulta tec'), CAT_TECNICO)
                               .when(col("Raw_Resultado").rlike(
                                   "Raw_Resultado_Escalo|Raw_Resultado_Envio_tecnico|Raw_Resultado_Transferencia|Raw_Resultado_Reclamacion"),
                                     CAT_TECNICO)
                               # - - - - - - - - - FACTURA
                               .when(col('INT_TIPO').rlike('(?i)factura'), CAT_BILLING)
                               # - - - - - - - - - TERMINAL
                               .when(col("INT_TIPO") == "TERMINAL", CAT_TERMINAL)
                               # - - - - - - - - - NO_PROB :)
                               .when((col("INT_TIPO") == "INFORMACION") | ((col("INT_TIPO") == "TRANSFERENCIA") & (
                                           col("Bucket") == "Other customer information management")),
                                     NO_PROB).otherwise(NA)
                               )  # end

    df_all = df_all.na.fill(NA, subset=["CATEGORY_2"])

    df_all = (df_all.withColumn("CATEGORY_1",
                                when(col('CATEGORY_2').isin([CAT_PRECIO, CAT_TERMINAL, CAT_CONTENIDOS]), CAT_COMERCIAL)
                                .when(col('CATEGORY_2').isin([CAT_SERV_ATENCION, CAT_TECNICO, CAT_BILLING, CAT_FRAUD]),
                                      CAT_NO_COMERCIAL)
                                .otherwise(col('CATEGORY_2'))))

    print("Elapsed time _set_categories = {} minutes".format((time.time()-start_time)/60.0))
    # FALTA
    # col('Raw_Productos') hay que desglosarlo
    return df_all




def __aggregate_ccc_calls_by(df_all, process_date, agg_by="nif"):

    start_time = time.time()


    print("Calling to __aggregate_ccc_calls_by_{}.... {}".format(agg_by, process_date))

    df_agg = (df_all
              # .withColumn("fx_interaction",
              #             concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
              # .withColumn("tuples", struct(["fx_interaction", "Bucket"]))
              .groupby(agg_by)
              .agg(sql_count(lit(1)).alias("num_interactions"),
                   sql_min(col('fx_interaction')).alias("first_interaction"),
                   sql_max(col('fx_interaction')).alias("latest_interaction"),
                   # sql_max(col("tuples")).alias("tuples_max"),  # latest interaction: [max(fx_interaction), bucket]
                   # sql_min(col("tuples")).alias("tuples_min"),  # first interaction: [min(fx_interaction), bucket]
                   # collect_list('Bucket').alias("bucket_list"),
                   # collect_set('Bucket').alias("bucket_set"),
                   # sql_sum("Bucket_NA").alias("num_NA_buckets"),
                   sql_sum("IVR").alias("num_ivr_interactions"),
                   collect_list('CATEGORY_1').alias("cat1_list"),
                   collect_list('CATEGORY_2').alias("cat2_list"),
            ))

    df_agg = df_agg.withColumn("ref_date", format_date(lit(process_date)))
    # df_agg = (df_agg.withColumn("bucket_1st_interaction", col("tuples_min")["Bucket"])
    #                 .withColumn("bucket_latest_interaction", col("tuples_max")["Bucket"])
    #                 .withColumn("nb_diff_buckets", size("bucket_set"))
    #                .drop(*['tuples_max', 'tuples_min'])
    #           )

    for cc in ["first_interaction", "latest_interaction"]:
        df_agg = (df_agg.withColumn("fx_{}".format(cc), format_date(cc, filter_dates_1900=True))  # days before 1900 converted to None
                        .withColumn("days_since_{}".format(cc), compute_diff_days("fx_{}".format(cc), "ref_date")))

    df_agg = df_agg.drop("first_interaction", "latest_interaction", "ref_date")

    def get_mode_problems(lst):
        # filter NA y NO_PROB
        if not lst: return None
        lst=[ll for ll in lst if ll and ll!=NO_PROB and ll!="NA"]
        if not lst: return None
        dd = Counter(lst).most_common(2)
        return dd[0][0]
    get_mode_udf = udf(lambda lst: get_mode_problems(lst), StringType())

    df_agg = (df_agg.withColumn("CAT1_MODE", when(coalesce(size(col("cat1_list")), lit(0)) == 0, None).otherwise(get_mode_udf(col("cat1_list"))))
                    .withColumn("CAT2_MODE", when(coalesce(size(col("cat2_list")), lit(0)) == 0, None).otherwise(get_mode_udf(col("cat2_list")))))

    df_cat_1 = df_all.groupby(agg_by).pivot("CATEGORY_1", values=[CAT_COMERCIAL, CAT_NO_COMERCIAL]).agg(sql_count("*")).fillna(0)
    df_cat_2 = df_all.groupby(agg_by).pivot("CATEGORY_2", values=[CAT_PRECIO, CAT_TERMINAL, CAT_CONTENIDOS, CAT_SERV_ATENCION,
CAT_TECNICO, CAT_BILLING, CAT_FRAUD, NO_PROB]).agg(sql_count("*")).fillna(0)

    df_agg = df_agg.join(df_cat_1, [agg_by], how="left").join(df_cat_2,  [agg_by], how="left")

    # df_agg = (df_agg.withColumn("TOTAL_COMERCIAL", sum_horizontal([CAT_PRECIO, CAT_TERMINAL, CAT_CONTENIDOS]))
    #                 .withColumn("TOTAL_NO_COMERCIAL", sum_horizontal([CAT_SERV_ATENCION, CAT_TECNICO, CAT_BILLING, CAT_FRAUD])))


    print("Elapsed time __aggregate_ccc_calls_by_{} = {} minutes".format(agg_by, (time.time()-start_time)/60.0))

    return df_agg


# def __get_agg_ccc_nif(spark, ccc_start_model, ccc_end_model):
#     '''
#     msisdn_list a list of msisdn's to filter the calls. Usually, this list corresponds to portouts
#     '''
#     ccc = CallCentreCalls(spark)
#     ccc.prepareFeatures(ccc_end_model, ccc_start_model)
#     df_all = ccc.all_interactions
#     df_all = __set_categories(df_all)
#     df_agg = __aggregate_ccc_calls_by(df_all, ccc_end_model)
#     return df_agg


def get_calls_for_portouts_filename(config_obj): #FIXME

    port_start_date = config_obj.get_start_port()
    port_end_date = config_obj.get_end_port()
    n_ccc= config_obj.get_ccc_range_duration()
    agg_by = config_obj.get_agg_by()

    filename = '/user/csanc109/projects/churn/data/ccc_model/df_calls_for_portouts_nif_{}_{}_{}_{}'.format(
                                                                                                        port_start_date,
                                                                                                        port_end_date, abs(n_ccc), agg_by)
    return filename




def get_calls_for_portouts(spark, config_obj, ref_date, portout_select_cols=None, logger=None):
    '''
    This function returns CCC aggregates by NIF, for clients that has a portout in range
    '''

    n_ccc=config_obj.get_ccc_range_duration()
    agg_by = config_obj.get_agg_by()

    df_port = get_port_requests_table(spark, config_obj, ref_date=ref_date,
                                      select_cols=portout_select_cols).withColumnRenamed("msisdn_a", "msisdn")

    if logger: logger.info("df_port={}".format(df_port.count()))

    min_portout = df_port.select(sql_min("portout_date").alias("min_portout_date")).rdd.map(
        lambda x: x["min_portout_date"]).collect()[0]
    max_portout = df_port.select(sql_max("portout_date").alias("max_portout_date")).rdd.map(
        lambda x: x["max_portout_date"]).collect()[0]

    yyyymmdd_min_portout_date = str(min_portout.split(" ")[0].replace("-", ""))
    yyyymmdd_max_portout_date = str(max_portout.split(" ")[0].replace("-", ""))

    ccc_start_date = move_date_n_days(yyyymmdd_min_portout_date, n=n_ccc, str_fmt="%Y%m%d")
    ccc_end_date = yyyymmdd_max_portout_date

    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date, ccc_start_date)
    df_all = ccc.all_interactions
    df_all = df_all.withColumn("fx_interaction", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))

    df_all_port = df_all.join(df_port, ["msisdn"], how="inner")

    if logger: logger.info("df_all_port={}".format(df_all_port.count()))

    df_all_port = (df_all_port.withColumn("fx_fx_interaction", format_date("fx_interaction", filter_dates_1900=True))  # days before 1900 converted to None
                   .withColumn("days_since_fx_interaction", compute_diff_days("fx_fx_interaction", "portout_date")))


    df_all_port = df_all_port.where( (col("days_since_fx_interaction")>=0) & (col("days_since_fx_interaction")<=abs(n_ccc)))

    df_all_port = __set_categories(df_all_port)

    df_agg_all_ccc = __aggregate_ccc_calls_by(df_all_port, ccc_end_date, agg_by)

    if agg_by=="NIF":
        df_mapping = df_all_port.select("NIF", "portout_date").dropDuplicates(["NIF"]) #add portout date to every entry
        df_agg_all_ccc = df_agg_all_ccc.join(df_mapping, ["NIF"], how="left")
    else:
        df_mapping = df_all_port.select("msisdn", "portout_date").dropDuplicates(["msisdn"]) #add portout date to every entry
        df_agg_all_ccc = df_agg_all_ccc.join(df_mapping, ["msisdn"], how="left")


    if logger: logger.info("Number of rows {} (each row is a {})".format(df_agg_all_ccc.count(), agg_by))

    return df_agg_all_ccc




def get_ccc_profile(spark, ccc_start_date, ccc_end_date, agg_by, df_other=None, other_select_cols=None):

    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date, ccc_start_date)
    df_all = ccc.all_interactions
    df_all = df_all.withColumn("fx_interaction", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))

    if df_other is not None:
        other_select_cols = [agg_by] if not other_select_cols else other_select_cols
        df_all = df_all.join(df_other.select(other_select_cols), [agg_by], how="inner")

    df_all = __set_categories(df_all)
    df_agg_all_ccc = __aggregate_ccc_calls_by(df_all, ccc_end_date, agg_by)

    return df_agg_all_ccc





# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# EXTRA FEATS TABLE : CCC metrics (ccc, abonos, tripletas)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def get_ccc_metrics(spark, ccc_end_date_, ccc_start_date_, impute_nulls=True):

    import time
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES INFO
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before incidences")
    #from churn.others.add_indicadores_lista_scores_v2 import get_incidences_info

    start_time_incidences = time.time()
    df_ccc_incidencias = __get_incidences_info(spark, ccc_end_date_, ccc_start_date_)
    df_ccc_incidencias = df_ccc_incidencias.cache()
    df_ccc_incidencias = df_ccc_incidencias.fillna({'IND_TIPIF_UCI': 0})
    df_ccc_incidencias = df_ccc_incidencias.withColumnRenamed("campo2", "msisdn_d")  # deanonimized

    if logger: logger.info("Incidences - {} minutes".format( (time.time()-start_time_incidences)/60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #['msisdn', 'NUM_PBMA_SRV', 'NUM_ABONOS_TIPIS', 'IND_TIPIF_UCI']
    start_time = time.time()
    for col_ in df_ccc_incidencias.columns:
        # do not rename ids and cols that already start with EXTRA_FEATS_CCC_PREFIX
        if col_ in ["msisdn"]:
            continue
        elif col_.lower().startswith(EXTRA_FEATS_PREFIX.lower()):
            df_ccc_incidencias = df_ccc_incidencias.withColumnRenamed(col_, col_.lower())
        else:
            df_ccc_incidencias = df_ccc_incidencias.withColumnRenamed(col_, EXTRA_FEATS_PREFIX + col_.lower())
    df_ccc_incidencias = df_ccc_incidencias.cache()
    if logger: logger.info("Incidences - Renaming took {} minutes".format( (time.time()-start_time)/60.0))

    start_time = time.time()
    df_ccc_incidencias = df_ccc_incidencias.drop(*['ccc_bucket_list', 'ccc_bucket_set', 'ccc_year',  'ccc_month',
                         'ccc_day', 'ccc_partitioned_month', 'ccc_fx_first_interaction', 'ccc_first_interaction',
                        'ccc_latest_interaction', 'ccc_fx_latest_interaction'])

    for col_ in [col_ for col_ in df_ccc_incidencias.columns if re.match("^ccc_bucket_|^ccc_raw_|^ccc_num_", col_) or col_ in ['ccc_ind_tipif_uci']]:
        df_ccc_incidencias = df_ccc_incidencias.withColumn(col_, col(col_).cast("integer"))
    df_ccc_incidencias = df_ccc_incidencias.cache()
    if logger: logger.info("Incidences - Drop + Cast took {} minutes".format( (time.time()-start_time)/60.0))

    if impute_nulls:
        df_ccc_incidencias = impute_nulls_ccc(df_ccc_incidencias)


    return df_ccc_incidencias


def __get_incidences_info(spark, ccc_end_date_, ccc_start_date_):


    #from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls
    # FIXME This must point to new car when finished!!!
    from churn.resources.call_centre_calls import  CallCentreCalls

    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date_, ccc_start_date_)
    #df_agg_original = ccc.add_l2_ccc_variables(ccc_end_date_)
    df_agg_original = ccc.get_ccc_service_df(ccc_end_date_, ccc_start_date_, add_l2_vars=True)
    df_all = ccc.all_interactions

    from pyspark.sql.functions import concat_ws, upper, sum as sql_sum

    #fichero_tipi = "/user/csanc109/projects/churn/tipis_uci.csv"  # msisdn es msisdn_d
    fichero_tipi = "/data/udf/vf_es/churn/additional_data/tipis_uci.csv"
    df_tipis_uci = spark.read.option("delimiter", ";").option("header", True).csv(fichero_tipi)
    df_tipis_uci = df_tipis_uci.withColumn("tres_tipis", concat_ws("__", upper(col("tipo")),
                                                                   upper(col("subtipo")),
                                                                   upper(col("razon"))))
    df_tipis_uci = df_tipis_uci.withColumn("dos_tipis", concat_ws("__", upper(col("tipo")),
                                                                  upper(col("subtipo"))))

    lista_tipis = df_tipis_uci.select("tres_tipis").distinct().rdd.map(lambda x: x["tres_tipis"]).collect()
    lista_tipis_dos = df_tipis_uci.select("dos_tipis").distinct().rdd.map(lambda x: x["dos_tipis"]).collect()

    lista_tipis_abono = ["Factura__Abono__Abono no realizado", "Factura__Abono__Cambio metodo de pago",
                         "Factura__Abono__Consulta estado", "Factura__Abono__No conforme importe"]

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
    df_tipis_permanencia_dctos = df_tipis_permanencia_dctos.withColumn("tres_tipis", concat_ws("__", upper(col("INT_TIPO")),
                                                                                              upper(col("INT_SUBTIPO")),
                                                                                              upper(col("INT_RAZON"))))
    lista_tipis_perm_dctos = df_tipis_permanencia_dctos.select("tres_tipis").distinct().rdd.map(
        lambda x: x["tres_tipis"]).collect()

    df_all = df_all.withColumn("tres", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo")),
                                                 upper(col("INT_Razon"))))

    # la llamada esta en la lista de tipis que generan churn
    df_all = df_all.withColumn("IS_TIPIS_CHURN", when(col("tres").isin(lista_tipis), 1).otherwise(0))
    df_all = df_all.withColumn("ABONOS_TIPIS_UCI", when(col("tres").isin(lista_tipis_abono), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_FACTURA", when(col("tres").isin(lista_tipis_factura), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_PERMANENCIA_DCTOS", when(col("tres").isin(lista_tipis_perm_dctos), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_INFO", when(col("INT_Tipo").rlike("INFORMACION"), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_INFO_NOCOMP", when( (col("INT_Tipo").rlike("INFORMACION")) & (col("INT_Subtipo")=="NO COMPLETADA"), 1).otherwise(0))
    df_all = df_all.withColumn("TIPIS_INFO_COMP", when( (col("INT_Tipo").rlike("INFORMACION")) & (col("INT_Subtipo")=="COMPLETADA"), 1).otherwise(0))



    df_all = df_all.withColumn("dos", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo"))))

    df_agg = (df_all.groupby('msisdn')
              .agg(sql_sum(col("IS_TIPIS_CHURN")).alias("NUM_PBMA_SRV"),
                   collect_list('dos').alias("lista_de_dos"),
                   sql_sum(col("ABONOS_TIPIS_UCI")).alias("NUM_ABONOS_TIPIS"),
                   sql_sum(col("TIPIS_FACTURA")).alias("NUM_TIPIS_FACTURA"),
                   sql_sum(col("TIPIS_PERMANENCIA_DCTOS")).alias("NUM_TIPIS_PERM_DCTOS"),
                   sql_sum(col("TIPIS_INFO")).alias("NUM_TIPIS_INFO"),
                   sql_sum(col("TIPIS_INFO_NOCOMP")).alias("NUM_TIPIS_INFO_NOCOMP"),
                   sql_sum(col("TIPIS_INFO_COMP")).alias("NUM_TIPIS_INFO_COMP"),

                   ))

    df_agg = df_agg.fillna({'NUM_PBMA_SRV': 0, "NUM_TIPIS_FACTURA" :0, "NUM_ABONOS_TIPIS" :0,  "NUM_TIPIS_PERM_DCTOS" : 0,
                            "NUM_TIPIS_INFO": 0, "NUM_TIPIS_INFO_NOCOMP": 0, "NUM_TIPIS_INFO_COMP": 0})


    df_agg = df_agg.withColumn("IND_TIPIF_UCI", when(col("NUM_PBMA_SRV") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_ABONOS", when(col("NUM_ABONOS_TIPIS") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_FACTURA", when(col("NUM_TIPIS_FACTURA") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_PERM_DCTOS", when(col("NUM_TIPIS_PERM_DCTOS") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_INFO", when(col("NUM_TIPIS_INFO") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_INFO_NOCOMP", when(col("NUM_TIPIS_INFO_NOCOMP") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("IND_TIPIF_INFO_COMP", when(col("NUM_TIPIS_INFO_COMP") > 0, 1).otherwise(0))



    df_agg = df_agg.join(df_agg_original, on=["msisdn"], how="outer")

    df_agg = df_agg.drop(*["lista_de_dos", "ccc_fx_latest_interaction"])

    df_agg = df_agg.withColumnRenamed("nb_diff_buckets", "num_diff_buckets")

    return df_agg


def impute_nulls_ccc(df):

    fill_minus_one = ['ccc_most_common_bucket_interactions', 'ccc_days_since_first_interaction', 'ccc_days_since_latest_interaction']
    fill_unknown = ['ccc_most_common_bucket_with_ties', 'ccc_most_common_bucket']
    fill_zero = [col_ for col_ in df.columns if re.match("^ccc_bucket_|^ccc_raw_|^ccc_num_", col_) or col_ in ['ccc_ind_tipif_uci']]


    for col_ in fill_zero:
        df = df.withColumn(col_, col(col_).cast("integer"))

    df = df.fillna("UNKNOWN", subset=fill_unknown)
    df = df.fillna(0, subset=fill_zero)
    df = df.fillna(-1, subset=fill_minus_one)

    return df