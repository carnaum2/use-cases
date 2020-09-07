
import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, count as sql_count, collect_set
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

HDFS_SERVICE_PROBLEMS_DIR = "/data/udf/vf_es/churn/prob_srv/"
EXTRA_FEATS_PREFIX = "pbms_srv_"



#################
##
#################

def get_cocinado_file(spark, closing_day):

    df_cocinado = None
    found = False

    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    #### First, try cocinado for exactly closing day - this must be the used option
    cocinado_jl = os.path.join(HDFS_SERVICE_PROBLEMS_DIR, "PROB_SERV_{}.TXT".format(closing_day))
    if check_hdfs_exists(cocinado_jl):
        df_cocinado = spark.read.option("delimiter", "|").option("header", True).csv(cocinado_jl)
        if logger: logger.info("Read service problems from file '{}'".format(cocinado_jl))
        found = True

    #### For historic reasons, I include the following two options. in the past, we only had the file with yyyymm
    cocinado_jl = os.path.join(HDFS_SERVICE_PROBLEMS_DIR, "PROB_SERV_{}.TXT".format(closing_day[:6]))
    if not found and check_hdfs_exists(cocinado_jl):
        df_cocinado = spark.read.option("delimiter", "|").option("header", True).csv(cocinado_jl)
        if logger: logger.info("Read service problems from file '{}'".format(cocinado_jl))
        found = True

    if not found:
        from pykhaos.utils.date_functions import move_date_n_yearmonths
        yyyymm = move_date_n_yearmonths(closing_day[:6], -1)
        # File for YYYYMM is generated on YYYY/MM+1/03
        cocinado_jl = os.path.join(HDFS_SERVICE_PROBLEMS_DIR, "PROB_SERV_{}.TXT".format(yyyymm))
        if check_hdfs_exists(cocinado_jl):
            df_cocinado = spark.read.option("delimiter", "|").option("header", True).csv(cocinado_jl)
            if logger: logger.info("Read service problems from file '{}'".format(cocinado_jl))
        else:
            if logger: logger.error("No found service problem file for closing_day = {}".format(closing_day))
            sys.exit()


    return df_cocinado



def get_service_problems(spark, closing_day, impute_nulls=True):
    '''
    :param df_scores_incidencias:
    :param yyyymm_str: YYYYMM for extracting the problems service
    :return:
    '''
    df_cocinado = get_cocinado_file(spark, closing_day)

    df_cocinado = (df_cocinado.withColumn("NUM_AVERIAS", col("NUM_AVERIAS").cast("integer"))
                    .withColumn("NUM_RECLAMACIONES", col("NUM_RECLAMACIONES").cast("integer"))
                    .withColumn("NUM_SOPORTE_TECNICO", col("NUM_SOPORTE_TECNICO").cast("integer"))
                    .withColumnRenamed("NIF", "NIF_CLIENTE_d")
                    .withColumn("num_reps_per_nif", sql_count("*").over(Window.partitionBy("NIF_CLIENTE_d")))
                    .where(col("num_reps_per_nif") == 1)
                    .drop("num_reps_per_nif"))
    # ['ANYOMES', 'NIF', 'NUM_AVERIAS', 'NUM_RECLAMACIONES', 'NUM_SOPORTE_TECNICO']

    df_map = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").load(
        "/data/udf/vf_es/churn/additional_data/amdocs_numcli_mapper_20190517.csv")
    df_map = df_map.drop_duplicates(["NUM_CLIENTE"])  # numero de num_cliente unicos: 15285229
    df_map = (df_map.withColumnRenamed("NUM_CLIENTE", "NUM_CLIENTE_d")
                   .withColumnRenamed("NIF_CLIENTE", "NIF_CLIENTE_d")
                    .filter((col("NUM_CLIENTE_d").isNotNull()) & (col("NIF_CLIENTE_d").isNotNull()))
                    .dropDuplicates())
    # ['NUM_CLIENTE', 'NIF_CLIENTE'] en claro

    from churn.datapreparation.general.data_loader import get_active_services
    df_services = (get_active_services(spark, closing_day, new=False,
                                      service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic", "campo1"],
                                      customer_cols=["num_cliente", "nif_cliente"])
                  .withColumnRenamed("num_cliente_service", "num_cliente")
                   .withColumnRenamed("campo1", "NUM_CLIENTE_d")
                   )
    # ['num_cliente_customer', 'cod_estado_general', 'clase_cli_cod_clase_cliente', 'msisdn', 'num_cliente', 'campo2', 'rgu', 'srv_basic', 'campo1']

    # # add campo1 (NUM_CLIENTE desanonimiz) to scores_incidences
    df_cocinado_2 = (df_cocinado.join(df_map, on=["NIF_CLIENTE_d"],
                                                          how="inner").withColumnRenamed("nif_cliente", "nif_cliente_a")
                    .withColumn("num_nifs_per_numcli", size(collect_set("NIF_CLIENTE_d").over(Window.partitionBy("NUM_CLIENTE_d"))))
                    .where(col("num_nifs_per_numcli") == 1)
                    .drop("num_nifs_per_numcli"))

    # add nif desanonimiz using the mapper
    df_cocinado_3 = (df_cocinado_2.join(df_services.select("msisdn", "NUM_CLIENTE_d", "campo2", "num_cliente", "nif_cliente"),
                                       on=["NUM_CLIENTE_d"],
                                       how="inner")
                    .where((col("num_cliente").isNotNull()) & (col("NUM_CLIENTE_d").isNotNull()))
                    .dropDuplicates())

    df_cocinado_3 = df_cocinado_3.withColumnRenamed("campo2", "msisdn_d")
    df_cocinado_3 = df_cocinado_3.fillna(0, subset=['NUM_AVERIAS', 'NUM_RECLAMACIONES', 'NUM_SOPORTE_TECNICO'])
    df_cocinado_3 = df_cocinado_3.cache()

    from pyspark.sql.functions import concat_ws
    df_cocinado_3 = (df_cocinado_3.withColumn("IND_AVERIAS", when(col("NUM_AVERIAS") > 0, 1).otherwise(0))
                                  .withColumn("IND_SOPORTE", when(col("NUM_SOPORTE_TECNICO") > 0, 1).otherwise(0))
                                  .withColumn("IND_RECLAMACIONES", when(col("NUM_RECLAMACIONES") > 0, 1).otherwise(0))
                                  .withColumn("IND_DEGRAD_ADSL", lit(0))
                    )

    df_cocinado_3 = df_cocinado_3.select(['msisdn', 'msisdn_d', 'num_cliente', 'NUM_CLIENTE_d', "NIF_CLIENTE_d", "nif_cliente",
                                          'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                          'IND_DEGRAD_ADSL', "NUM_AVERIAS", "NUM_SOPORTE_TECNICO", "NUM_RECLAMACIONES"])

    for col_ in df_cocinado_3.columns:
        if col_.lower().startswith("msisdn") or col_.lower().startswith("num_cliente") or col_.lower().startswith("nif_cliente"):
            continue
        df_cocinado_3 = df_cocinado_3.withColumnRenamed(col_, EXTRA_FEATS_PREFIX + col_)

    for col_ in df_cocinado_3.columns:
        df_cocinado_3 = df_cocinado_3.withColumnRenamed(col_, col_.lower())

    if impute_nulls:
        df_cocinado_3 = impute_nulls_pbma_srv(df_cocinado_3)

    return df_cocinado_3


def impute_nulls_pbma_srv(df):

    fill_zero = [col_ for col_ in df.columns if col_.lower().startswith(EXTRA_FEATS_PREFIX)]
    df = df.fillna(0, subset=fill_zero)

    return df