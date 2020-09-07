
import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, datediff,\
    to_date, from_unixtime, unix_timestamp, count as sql_count, collect_set
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

from pykhaos.utils.date_functions import move_date_n_yearmonths

HDFS_TGS_DIR = "/data/udf/vf_es/churn/tgs/"

EXTRA_FEATS_PREFIX = "tgs_"

MOST_COMMON_DISCOUNTS = ["PQC50", "DPR50", "CN003", "DMF15", "DPR40", "DPR25", "DPC50",
                         "PQC30", "DMR50", "CN002"]

def get_tgs(spark, closing_day, yyyymm, impute_nulls=True, force_keep_cols=None):
    '''

    :param spark:
    :param closing_day: yyyymmdd to compute days_since
    :param yyyymm: yearmonth to get the file
    :param impute_nulls:
    :param force_keep_cols: [Advanced users] list of columns to drop. These columns are always dropped, i.e., dates columns
              ["FECHA_FIN_DTO", "F_INICIO_BI", "F_FIN_BI", "F_INICIO_BI_EXP", "F_FIN_BI_EXP", "TARGET_SEGUIMIENTO_BI"]
    :return:
    '''

    # MATRIZ_PREVEN_BI_YYYYMM.TXT is generated on day 21
    # E.G MATRIZ_PREVEN_BI_201901.TXT is generated on 21-jan

    if not force_keep_cols:
        force_keep_cols = []


    to_drop = ["FECHA_FIN_DTO", "F_INICIO_BI", "F_FIN_BI", "F_INICIO_BI_EXP", "F_FIN_BI_EXP", "TARGET_SEGUIMIENTO_BI", 'MESES_FIN_DTO']
    to_drop = list(set(to_drop)-set(force_keep_cols))


    while True:

        tgs_dir_path = os.path.join(HDFS_TGS_DIR, "MATRIZ_PREVEN_BI_{}.TXT".format(yyyymm))

        if logger: logger.info("Reading TGs from file '{}'".format(tgs_dir_path))

        try: # BI generates the matrix when it required. In case the yyyymm file does not exist, we use the previous one...

            # Loading TG table and ensuring "nif_cliente_d" is unique identifier
            df_tgs = spark.read.option("delimiter", "|").option("header", True).csv(tgs_dir_path)
            break
        except Exception as e:
            if logger: logger.error("File '{}' does not exist".format(tgs_dir_path))
            if logger: logger.error(e)
            yyyymm = move_date_n_yearmonths(yyyymm, n=-1)
            if logger: logger.info("Trying to retrieve tgs info from yearmonth {}".format(yyyymm))


    df_tgs = (df_tgs.where(col("NIF").isNotNull())
                   .withColumnRenamed("NIF", "NIF_CLIENTE_d")
                   .withColumn("num_reps_per_nif", sql_count("*").over(Window.partitionBy("NIF_CLIENTE_d")))
                   .where(col("num_reps_per_nif") == 1)
                   .drop("num_reps_per_nif")
                   .withColumnRenamed("FECHA", "FECHA_FIN_DTO"))

    for col_ in ["F_INICIO_BI", "F_INICIO_BI_EXP"]:
        df_tgs = (df_tgs.withColumn("days_since_{}".format(col_), when(col(col_).isNotNull(),
                                                                   datediff(
                                                                       from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                                                                       from_unixtime(unix_timestamp(col_, "yyyyMMdd")),
                                                                       ).cast("double")).otherwise(-1)))

    for col_ in [ "F_FIN_BI",  "F_FIN_BI_EXP", "FECHA_FIN_DTO"]:
        df_tgs = (df_tgs.withColumn("days_until_{}".format(col_), when(col(col_).isNotNull(),
                                                                    datediff(from_unixtime(unix_timestamp(col_, "yyyyMMdd")),
                                                                             from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd"))
                                                                             ).cast("double")).otherwise(None)))

    df_tgs = (df_tgs.withColumn("HAS_DISCOUNT", when(col("DESCUENTO").isNotNull(), 1).otherwise(0))
                    .drop(*to_drop)
                    .where(col("NIF_CLIENTE_d").isNotNull()))

    for col_ in df_tgs.columns:
        if col_ in ["NIF_CLIENTE_d"]: continue
        df_tgs = df_tgs.withColumnRenamed(col_, EXTRA_FEATS_PREFIX + col_)

    for col_ in ['TGS_SUM_IND_UNDER_USE', 'TGS_SUM_IND_OVER_USE', 'TGS_BLINDA_BI_N2',
                 'TGS_BLINDA_BI_N4', 'TGS_BLINDAJE_BI_EXPIRADO']:
        df_tgs = df_tgs.withColumn(col_, when(col(col_).isNull(), 0).otherwise(col(col_).cast("integer")))

    # Loading MAPPER table and ensuring "num_cliente_d" is unique identifier
    df_map = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").load(
        "/data/udf/vf_es/churn/additional_data/amdocs_numcli_mapper_20190517.csv")
    df_map = df_map.drop_duplicates(["NUM_CLIENTE"])  # numero de num_cliente unicos: 15285229
    df_map = (df_map.withColumnRenamed("NUM_CLIENTE", "NUM_CLIENTE_d")
                    .withColumnRenamed("NIF_CLIENTE", "NIF_CLIENTE_d")
                    .filter( (col("NUM_CLIENTE_d").isNotNull()) & (col("NIF_CLIENTE_d").isNotNull()))
                    .dropDuplicates())

    from churn.datapreparation.general.data_loader import get_active_services
    df_services = (get_active_services(spark, closing_day, new=False,
                                      service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic", "campo1"],
                                      customer_cols=["num_cliente", "nif_cliente"])
                  .withColumnRenamed("num_cliente_service", "num_cliente")
                  .withColumnRenamed("campo1", "NUM_CLIENTE_d")
                  #.where(col("rgu").rlike("^mobile$|^movil$"))
                   )

    # Adding NUM_CLIENTE_D to TG table and ensuring the same NUM_CLIENTE_D is not associated with different NIF_CLIENTE_D
    # The resulting table has NUM_CLIENTE_D as unique identifier
    df_tgs_2 = ((df_tgs.join(df_map, on=["NIF_CLIENTE_d"], how="inner")
                       .withColumnRenamed("nif_cliente", "nif_cliente_a"))
                       .withColumn("num_nifs_per_numcli", size(collect_set("NIF_CLIENTE_d").over(Window.partitionBy("NUM_CLIENTE_d"))))
                       .where(col("num_nifs_per_numcli") == 1)
                       .drop("num_nifs_per_numcli"))

    # DF_SERVICES has MSISDN as unique identifier. Fields in TG table can be added by joining through NUM_CLIENTE_D/CAMPO1
    df_tgs_3 = (df_tgs_2.join(df_services.select("msisdn", "NUM_CLIENTE_d", "campo2", "num_cliente", "nif_cliente"),
                              on=["NUM_CLIENTE_d"],
                              how="inner")
                .where( (col("num_cliente").isNotNull()) & (col("NUM_CLIENTE_d").isNotNull()))
                .dropDuplicates()
                .withColumnRenamed("campo2", "msisdn_d")
                )

    for col_ in df_tgs_3.columns:
        df_tgs_3 = df_tgs_3.withColumnRenamed(col_, col_.lower())


    df_tgs_3 = df_tgs_3.withColumn("tgs_discount_proc",
                                   when(col("tgs_descuento").isNull(), "NO_DISCOUNT")
                                  .when(col("tgs_descuento").isin(MOST_COMMON_DISCOUNTS), col("tgs_descuento"))
                                  .otherwise("OTHERS"))


    from churn.metadata.metadata import Metadata
    expected_tgs_cols = Metadata.get_tgs_cols()
    for col_ in expected_tgs_cols:
        if col_ not in df_tgs_3.columns:
            # create the column, initialized with Nulls
            df_tgs_3 = df_tgs_3.withColumn(col_, lit(None))

    for col_ in ['tgs_stack', 'tgs_clasificacion_uso', 'tgs_blindaje_bi_expirado',
                    'tgs_blinda_bi_n4', 'tgs_blinda_bi_n2', 'tgs_decil', 'tgs_target_accionamiento']:  # be sure column is a string
        if col_ in df_tgs_3.columns:
            df_tgs_3 = df_tgs_3.withColumn(col_, col(col_).cast("string"))


    if impute_nulls:
        df_tgs_3 = impute_nulls_tgs(spark, df_tgs_3)

    ## COLS:
    # ['nif',
    #  'tgs_stack',
    #  'tgs_decil',
    #  'tgs_descuento',
    #  'tgs_meses_fin_dto',
    #  'tgs_sum_ind_under_use',
    #  'tgs_sum_ind_over_use',
    #  'tgs_clasificacion_uso',
    #  'tgs_blindaje_bi',
    #  'tgs_blinda_bi_n2',
    #  'tgs_blinda_bi_n4',
    #  'tgs_blindaje_bi_expirado',
    #  'tgs_target_accionamiento',
    #  'tgs_days_since_f_inicio_bi',
    #  'tgs_days_since_f_inicio_bi_exp',
    #  'tgs_days_until_f_fin_bi',
    #  'tgs_days_until_f_fin_bi_exp',
    #  'tgs_days_until_fecha_fin_dto',
    #  'tgs_has_discount',
    #  'num_cliente_d',
    #  'nif_cliente_d',
    #  'msisdn',
    #  'campo1',
    #  'msisdn_d',
    #  'num_cliente',
    #  'nif_cliente']

    return df_tgs_3



def impute_nulls_tgs(spark, df):

    from churn.metadata.metadata import Metadata
    impute_na = Metadata.get_tgs_impute_na()
    df_metadata = Metadata.create_metadata_df(spark, impute_na)
    return Metadata.apply_metadata(df, df_metadata)
