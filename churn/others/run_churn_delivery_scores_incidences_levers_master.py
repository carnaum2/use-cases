# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, regexp_extract
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # jvmm_amdocs_prepared_car_mobile_complete_<closing_day> -- esta tabla la genera el proceso.
# 1) Lee las tablas de predicciones onlymob y mobileandfbb y las junta
# 2) Anade el decile
# 3) Anade la columna de incidencias
# 4) Escribe el fichero
# This file replaces the file add_indicadores_lista_scores_v2
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

logger=None
DECILE_COL = "comb_decile"

COLS_EXTENDED = ['msisdn', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                      'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'palanca',
                                      'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason',
                                      'top5_reason', 'Incertidumbre']
COLS_CLASSIC = ['msisdn', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                      'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'palanca']

COLS_REASONS = ['msisdn', 'comb_score', 'comb_decile', 'IND_TIPIF_UCI', 'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason',
                                      'top5_reason', 'Incertidumbre']

NAME_TABLE_DEANONYMIZED = 'tests_es.churn_team_delivery_{}_prepared'


def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        root_dir = "/var/SP/data/bdpmdses/deliveries_churn/"
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "ccc_data_preparation_main_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="", msg_format="%(asctime)s [%(levelname)-5.5s] CHURN|%(message)s")
    logger.info("Logging to file {}".format(logging_file))

    return logger


def get_columns_required(formats_list):
    cols = []
    for ff in formats_list:
        cols = cols + get_columns_by_format(ff)
    return list(set(cols))

def get_columns_by_format(format_file):
    if format_file == "extended":
        return COLS_EXTENDED
    if format_file in "classic":
        return COLS_CLASSIC
    if format_file in "reasons":
        return COLS_REASONS
    if logger: logger.info("Unknown file format {}".format(format_file))
    sys.exit()


def get_filename_by_format(format_file):
    from churn.utils.constants import DELIVERY_FILENAME_EXTENDED, DELIVERY_FILENAME_CLASSIC, DELIVERY_FILENAME_REASONS
    print("get_filename_by_format", format_file)
    if format_file == "extended":
        return DELIVERY_FILENAME_EXTENDED
    elif format_file == "classic":
        return DELIVERY_FILENAME_CLASSIC
    elif format_file == "reasons":
        return DELIVERY_FILENAME_REASONS
    else:
        if logger: logger.info("Unknown file format {}. Not possible to retrieve filename".format(format_file))
        sys.exit()

def want_reasons(columns):
    return "top0_reason" in columns

def want_indicators(columns):
    return "IND_PBMA_SRV" in columns

def want_levers(columns):
    return "palanca" in columns

def add_service_problems(spark, df_scores_incidencias, yyyymm_str, closing_day):
    '''

    :param df_scores_incidencias:
    :param yyyymm_str: YYYYMM for extracting the problems service
    :return:
    '''
    cocinado_jl = "/data/udf/vf_es/churn/jll_prob_srv_final/JLL_PROB_SERV_FINAL_{}.TXT".format(yyyymm_str)
    df_cocinado = spark.read.option("delimiter", "|").option("header", True).csv(cocinado_jl)
    df_cocinado = df_cocinado.withColumn("NUM_AVERIAS", col("NUM_AVERIAS").cast("integer"))
    df_cocinado = df_cocinado.withColumn("NUM_RECLAMACIONES", col("NUM_RECLAMACIONES").cast("integer"))
    df_cocinado = df_cocinado.withColumn("NUM_SOPORTE_TECNICO", col("NUM_SOPORTE_TECNICO").cast("integer"))
    # ['ANYOMES', 'NIF', 'NUM_AVERIAS', 'NUM_RECLAMACIONES', 'NUM_SOPORTE_TECNICO']

    df_map = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").load(
        "/data/udf/vf_es/churn/additional_data/amdocs_numcli_mapper_20190115.csv")
    df_map = df_map.drop_duplicates(["NUM_CLIENTE"])  # numero de num_cliente unicos: 15285229
    df_map = df_map.withColumnRenamed("NUM_CLIENTE", "NUM_CLIENTE_d").withColumnRenamed("NIF_CLIENTE", "NIF_CLIENTE_d")
    # ['NUM_CLIENTE', 'NIF_CLIENTE'] en claro

    from churn.datapreparation.general.data_loader import get_active_services
    df_services = get_active_services(spark, closing_day, new=False,
                                      service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic", "campo1"],
                                      customer_cols=["num_cliente", "nif_cliente"])
    # ['num_cliente_customer', 'cod_estado_general', 'clase_cli_cod_clase_cliente', 'msisdn', 'num_cliente_service', 'campo2', 'rgu', 'srv_basic', 'campo1']

    # add campo1 (NUM_CLIENTE desanonimiz) to scores_incidences
    df_scores_incidencias_2 = df_scores_incidencias.join(df_services.select("msisdn", "campo1"), on=["msisdn"],
                                                         how="left").withColumnRenamed("nif_cliente", "nif_cliente_a")

    # add nif desanonimiz using the mapper
    df_scores_incidencias_3 = df_scores_incidencias_2.join(df_map, on=df_scores_incidencias_2["campo1"] == df_map[
        "NUM_CLIENTE_d"], how="left")

    # add "cocinado" using NIF_CLIENTE desanonimiz as join column
    df_scores_incidencias_4 = df_scores_incidencias_3.join(df_cocinado,
                                                           on=df_scores_incidencias_3["NIF_CLIENTE_d"] == df_cocinado[
                                                               "NIF"], how="left")
    df_scores_incidencias_4 = df_scores_incidencias_4.fillna(0, subset=['NUM_AVERIAS', 'NUM_RECLAMACIONES',
                                                                        'NUM_SOPORTE_TECNICO'])

    df_scores_incidencias_4 = df_scores_incidencias_4.cache()

    # [fcarren: En principio la actual de IND_PBMA_SRV deberia ser un compendio de todas (averias, soporte tecnico,
    # reclamaciones, tipificaciones de interacciones (que es la que ahora lo informa). Podriamos tener algo asi:
    # IND_AVERIAS # IND_SOPORTE # IND_RECLAMACIONES # IND_DEGRAD_ADSL # IND_TIPIF_UCI # IND_PBMA_SRV # DETALLE_PBMA_SRV
    # Donde lo actual alimentaria al IND_TIPIF_UCI y el amarillo (IND_PBMA_SRV) seria 1 cuando alguno estuviera a 1.
    # El naranja estaria informado con lo actual si tiene algo y si no y tiene alguno de los 4 primeros indicadores a
    #  1 pondria lo que correspondiera por el orden ese por ej (averias, soporte, reclamaciones, degradacion adsl)

    from pyspark.sql.functions import concat_ws
    df_scores_incidencias_5 = (df_scores_incidencias_4.withColumnRenamed("IND_PBMA_SRV", "IND_TIPIF_UCI")
                               .withColumn("IND_AVERIAS", when(col("NUM_AVERIAS") > 0, 1).otherwise(0))
                               .withColumn("IND_SOPORTE", when(col("NUM_SOPORTE_TECNICO") > 0, 1).otherwise(0))
                               .withColumn("IND_RECLAMACIONES", when(col("NUM_RECLAMACIONES") > 0, 1).otherwise(0))
                               .withColumn("IND_DEGRAD_ADSL", lit(0))
                               .withColumn("IND_PBMA_SRV", when(col("IND_TIPIF_UCI") + col("IND_AVERIAS") + col('IND_SOPORTE') + col('IND_RECLAMACIONES') > 0, 1).otherwise(0))
                               .withColumn("DETALLE_PBMA_SRV", concat_ws("  ", col("DETALLE_PBMA_SRV").cast("string"),
                                                                         col("NUM_AVERIAS").cast("string"),
                                                                         col("NUM_SOPORTE_TECNICO").cast("string"),
                                                                         col("NUM_RECLAMACIONES").cast("string"))))

    df_scores_incidencias_5 = df_scores_incidencias_5.select(
        ['msisdn', 'msisdn_d', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES', 'IND_DEGRAD_ADSL',
         'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV'])

    return df_scores_incidencias_5

def get_incidences_info(spark, ccc_end_date_, ccc_start_date_):
    from churn.resources.call_centre_calls import CallCentreCalls
    print("get_incidences_info start={} __ end={}".format(ccc_start_date_, ccc_end_date_))
    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date_, ccc_start_date_)
    df_all = ccc.all_interactions

    from pyspark.sql.functions import concat_ws, upper, sum as sql_sum

    fichero_tipi = "/user/csanc109/projects/churn/tipis_uci.csv"  # msisdn es msisdn_d
    df_tipis_uci = spark.read.option("delimiter", ";").option("header", True).csv(fichero_tipi)
    df_tipis_uci = df_tipis_uci.withColumn("tres_tipis", concat_ws("__", upper(col("tipo")),
                                                                   upper(col("subtipo")),
                                                                   upper(col("razon"))))
    df_tipis_uci = df_tipis_uci.withColumn("dos_tipis", concat_ws("__", upper(col("tipo")),
                                                                  upper(col("subtipo"))))

    lista_tipis = df_tipis_uci.select("tres_tipis").distinct().rdd.map(lambda x: x["tres_tipis"]).collect()
    lista_tipis_dos = df_tipis_uci.select("dos_tipis").distinct().rdd.map(lambda x: x["dos_tipis"]).collect()

    df_all = df_all.withColumn("tres", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo")),
                                                 upper(col("INT_Razon"))))

    # la llamada esta en la lista de tipis que generan churn
    df_all = df_all.withColumn("IS_TIPIS_CHURN", when(col("tres").isin(lista_tipis), 1).otherwise(0))

    df_all = df_all.withColumn("dos", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo"))))

    df_agg = (df_all.groupby('msisdn')
              .agg(sql_sum(col("IS_TIPIS_CHURN")).alias("NUM_PBMA_SRV"),
                   collect_list('dos').alias("lista_de_dos"))
              )

    df_agg = df_agg.fillna({'NUM_PBMA_SRV': 0})

    from collections import Counter


    def get_mode(lst):
        if not lst: return None
        lst = [ll for ll in lst if ll in lista_tipis_dos]
        if not lst: return None
        dd = Counter(lst).most_common(2)
        return dd[0][0]  # if (len(dd) == 1 or dd[0][1] > dd[1][1]) else "TIE"


    get_mode_udf = udf(lambda lst: get_mode(lst), StringType())

    df_agg = df_agg.withColumn("IND_PBMA_SRV", when(col("NUM_PBMA_SRV") > 0, 1).otherwise(0))

    ## Do not compute DETALLE_PBMA_SRV when IND_PBMA_SRV=0
    df_agg = df_agg.withColumn("DETALLE_PBMA_SRV",
                               when( (coalesce(size(col("lista_de_dos")), lit(0)) == 0) | (col("IND_PBMA_SRV")==0), "None").otherwise(
                                   get_mode_udf(col("lista_de_dos"))))

    return df_agg


def get_scores_riesgo(spark, sql_onlymob, sql_mobileandfbb, logger=None):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # READ TABLES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("Running '{}'".format(sql_onlymob))
    df_onlymob = spark.sql(sql_onlymob).withColumn("segment", lit("onlymob"))
    df_onlymob = df_onlymob.withColumn("scoring", col("scoring").cast("float"))
    if logger: logger.info("Running '{}'".format(sql_mobileandfbb))
    df_mobileandfbb = spark.sql(sql_mobileandfbb).withColumn("segment", lit("mobileandfbb"))
    df_mobileandfbb = df_mobileandfbb.withColumn("scoring", col("scoring").cast("float"))

    from pykhaos.utils.pyspark_utils import union_all
    df_scores = union_all([df_onlymob, df_mobileandfbb]) # msisdn is anonymized | campo2 deanonymized
    # scores in 'scoring' columns
    df_scores = df_scores.cache()
    if logger: logger.info("*** df_scores {}".format(df_scores.count()))
    return df_scores 


def generate_scores_incidences(spark, sql_onlymob, sql_mobileandfbb, ccc_end_date_, ccc_start_date_, logger=None):

    import time

    df_scores = get_scores_riesgo(spark, sql_onlymob, sql_mobileandfbb, logger)


    from churn.datapreparation.general.data_loader import get_active_services

    df_active_services = (get_active_services(spark, closing_day=ccc_end_date_, new=False,
                                              service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic",
                                                            "campo1"],
                                              customer_cols=["num_cliente", "nif_cliente"])
                          .withColumnRenamed("num_cliente_service", "num_cliente")
                          )

    df_scores = df_scores.join(df_active_services.select("msisdn", "campo2"), on=["msisdn"], how="left")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES INFO
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before incidences")

    start_time_incidences = time.time()
    df_agg = get_incidences_info(spark, ccc_end_date_, ccc_start_date_)

    df_scores_incidencias = df_scores.join(df_agg, on=["msisdn"], how="left")

    df_scores_incidencias = df_scores_incidencias.fillna({'IND_PBMA_SRV': 0, 'DETALLE_PBMA_SRV': "None"})

    df_scores_incidencias = df_scores_incidencias.withColumnRenamed("campo2", "msisdn_d")  # deanonimized
    df_scores_incidencias = df_scores_incidencias.withColumnRenamed("scoring", "comb_score")

    if logger: logger.info("Incidences - {} minutes".format( (time.time()-start_time_incidences)/60.0))
    df_scores_incidencias = df_scores_incidencias.cache()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before add_decile")
    start_time_decile = time.time()
    df_scores_incidencias = __add_decile(df_scores_incidencias)
    if logger: logger.info("After add_decile - {} minutes".format( (time.time()-start_time_decile)/60))

    df_scores_incidencias = df_scores_incidencias.select(
        'msisdn', 'msisdn_d', 'comb_score', DECILE_COL, 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'segment')

    df_scores_incidencias = df_scores_incidencias.where(col('msisdn').isNotNull())

    return df_scores_incidencias



def __add_decile(df_scores_incidencias):
    import time
    # 2) se ordena de mayor a menor por score;
    df_scores_incidencias = df_scores_incidencias.withColumn("comb_score", col("comb_score").cast(FloatType()))
    df_scores_incidencias = df_scores_incidencias.orderBy('comb_score', ascending=False)

    df_scores_incidencias = df_scores_incidencias.cache()

    # 3) Add "risk" column: 1 for the 20% top. 0, for the rest
    qq = df_scores_incidencias.approxQuantile("comb_score", [0.8], 0.000001)[0]
    if logger: logger.info("quantile for 80% = {}".format(qq))
    df_scores_incidencias = df_scores_incidencias.withColumn("risk", when(col("comb_score") > qq, 1).otherwise(0))

    if DECILE_COL in df_scores_incidencias.columns:
        df_scores_incidencias = df_scores_incidencias.drop(DECILE_COL)

    # Conversion to DoubleType due to QuantileDiscretizer requirements
    df_risk = df_scores_incidencias.where(col("risk") == 1).withColumn("score_decile", col("comb_score").cast(DoubleType()))
    if logger: logger.info("Before QuantileDiscretizer")
    start_time_quantile = time.time()
    discretizer = QuantileDiscretizer(numBuckets=10, inputCol="score_decile", outputCol=DECILE_COL, relativeError=0)
    df_risk = discretizer.fit(df_risk).transform(df_risk)
    if logger: logger.info("After QuantileDiscretizer - {} minutes".format((time.time() - start_time_quantile)/60.0))
    df_risk = df_risk.drop("score_decile")

    df_norisk = df_scores_incidencias.where(col("risk") == 0).withColumn(DECILE_COL, lit(-1.0))
    from pykhaos.utils.pyspark_utils import union_all
    df_scores_incidencias = union_all([df_risk, df_norisk])
    df_scores_incidencias = df_scores_incidencias.withColumn(DECILE_COL, col(DECILE_COL) + 1)
    df_scores_incidencias = df_scores_incidencias.drop("risk")
    df_scores_incidencias = df_scores_incidencias.withColumn(DECILE_COL, col(DECILE_COL).cast(IntegerType()))

    return df_scores_incidencias


def get_churn_reasons(spark, closing_day):
    df_churn_reasons = spark.read.table("tests_es.asaezco_churn_reasons_full_{}".format(closing_day))
    # ['msisdn', 'Incertidumbre', 'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason', 'top5_reason']
    for ii in range(0, 6):
        df_churn_reasons = df_churn_reasons.withColumn("top{}_reason".format(ii),
                                                       regexp_extract(col("top{}_reason".format(ii)), "^\[(.*)\]$", 1))

    return df_churn_reasons

def create_delivery_table(spark, closing_day, sql_onlymob, sql_mobileandfbb, columns, logger=None):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT PARAMETERS - PREDICT MODEL (DO NOT MODIFY UNLESS YOU ARE SURE WHAT YOUR DOING) :)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    PREDICT_MODEL_NAME = "ccc_comercial_model"
    # Save the root path for savings
    SAVING_PATH = os.path.join("/var/SP/data/home/csanc109", "data", "churn", "ccc")
    #SAVING_PATH = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "churn", "ccc")
    PREDICT_MODEL_PATH = os.path.join(SAVING_PATH, "results", "ccc_comercial", "20181229_203318", "model")
    GENERATE_DELIVERY = True

    print("Program will use the model stored at '{}'".format(PREDICT_MODEL_PATH))

    import time
    start_time = time.time()
    from churn.models.ccc.delivery.delivery_manager import PROJECT_NAME, DIR_DELIVERY
    from pykhaos.utils.constants import WHOAMI

    from pykhaos.utils.date_functions import move_date_n_days
    ccc_start_date_ = move_date_n_days(closing_day, -30, str_fmt="%Y%m%d")

    delivery_table = NAME_TABLE_DEANONYMIZED.format(closing_day)

    reasons = want_reasons(columns)
    indicators = want_indicators(columns)
    levers = want_levers(columns)

    if logger: logger.info("Summary:")
    if logger: logger.info("\t CLOSING_DAY = {}".format(closing_day))
    if logger: logger.info("\tsql_onlymob = '{}'".format(sql_onlymob))
    if logger: logger.info("\tsql_mobileandfbb = '{}'".format(sql_mobileandfbb))
    #logger.info("\t incidencias filename = '{}'".format(SCORES_INCIDENCES_FILENAME))
    if logger: logger.info("\tModel stored at '{}'".format(PREDICT_MODEL_PATH))
    if logger: logger.info("\t table deanonymized   = '{}'".format(delivery_table))
    #if logger: logger.info("\t delivery filename    = '{}'".format(os.path.join(DIR_DELIVERY, name_file_delivery+".txt")))
    if logger: logger.info("\t ccc_range            = '{} - '{}'".format(ccc_start_date_, closing_day))
    if logger: logger.info("\t generate delivery    = '{}".format(GENERATE_DELIVERY))
    if logger: logger.info("\t cols           =  '{}'".format(",".join(columns)))
    if logger: logger.info("\t reasons = {}".format(reasons))
    if logger: logger.info("\t indicators = {}".format(indicators))
    if logger: logger.info("\t levers = {}".format(levers))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # LEVERS PREDICTIONS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if levers:
        from churn.models.ccc.delivery.delivery_manager import make_predictions
        if logger: logger.info("Make predictions")
        start_time_predictions = time.time()
        df_lever_predict_hidden = make_predictions(spark, PREDICT_MODEL_NAME, PREDICT_MODEL_PATH,
                                                   closing_day, h2o_port=54322)  # returns a column msisdn_a (anonymized)

        print("Prediction size {}".format(df_lever_predict_hidden.count()))

        if logger: logger.info("Make predictions - {} minutes".format( (time.time()-start_time_predictions)/60.0))
    else:
        df_lever_predict_hidden = None


    df_scores_incidencias = generate_scores_incidences(spark, sql_onlymob, sql_mobileandfbb, closing_day, ccc_start_date_, logger)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES + DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #if indicators:
    start_time_indicators = time.time()
    from pykhaos.utils.date_functions import move_date_n_yearmonths
    # Use the previous YYYYMM to get the service problems
    yyyymm_str = move_date_n_yearmonths(closing_day[:6], -1)
    if logger: logger.info("Using yyyymm={} to obtain the service_problems".format(yyyymm_str))
    df_scores_incidencias = add_service_problems(spark, df_scores_incidencias, yyyymm_str, closing_day)
    if logger: logger.info("Time for adding indicators {}".format((time.time()-start_time_indicators)/60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # GET CHURN REASONS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if reasons:
        df_churn_reasons = get_churn_reasons(spark, closing_day)
    else:
        df_churn_reasons = None
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MERGE PREDICTIONS WITH SCORES AND INCIDENCES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.models.ccc.delivery.delivery_manager import merging_process, prepare_delivery, DIR_DOWNLOAD

    try:
        start_merge = time.time()
        merging_process(df_lever_predict_hidden, df_scores_incidencias, df_churn_reasons, delivery_table, columns, save_table=GENERATE_DELIVERY)
        if logger: logger.info("merging_process - {} minutes".format( (time.time()-start_merge)/60.0))

    except Exception as e:
        if logger: logger.error("merging_process failed. Reason:")
        if logger: logger.error(e)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # END
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))
    if logger: logger.info("Process ended successfully. Enjoy :)")


