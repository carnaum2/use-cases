import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when
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
# This file replaces the file add_indicadores_lista_scores
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DECILE_COL = "comb_decile"

def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))


    ENGINE_SRC = "/var/SP/data/home/csanc109/src/devel/amdocs_informational_dataset/"
    if ENGINE_SRC not in sys.path:
        sys.path.append(ENGINE_SRC)
        print("Added '{}' to path".format(ENGINE_SRC))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger


def get_incidences_info(spark, ccc_end_date_, ccc_start_date_):
    from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls

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

# - - - - - - - -

def add_decile(df_scores_incidencias):
    import time
    # 2) se ordena de mayor a menor por score;
    df_scores_incidencias = df_scores_incidencias.withColumn("comb_score", col("comb_score").cast(FloatType()))
    df_scores_incidencias = df_scores_incidencias.orderBy('comb_score', ascending=False)

    # 3) Add "risk" column: 1 for the 20% top. 0, for the rest
    qq = df_scores_incidencias.approxQuantile("comb_score", [0.8], 0.005)[0]
    if logger: logger.info("quantile for 80% = {}".format(qq))
    df_scores_incidencias = df_scores_incidencias.withColumn("risk", when(col("comb_score") > qq, 1).otherwise(0))

    if DECILE_COL in df_scores_incidencias.columns:
        df_scores_incidencias = df_scores_incidencias.drop(DECILE_COL)

    # Conversion to DoubleType due to QuantileDiscretizer requirements
    df_risk = df_scores_incidencias.where(col("risk") == 1).withColumn("score_decile", col("comb_score").cast(DoubleType()))
    if logger: logger.info("Before QuantileDiscretizer")
    start_time_quantile = time.time()
    discretizer = QuantileDiscretizer(numBuckets=10, inputCol="score_decile", outputCol=DECILE_COL)
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


def generate_scores_incidences(spark, segments,ccc_end_date_, ccc_start_date_):

    import time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # READ TABLES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    df_list = []
    for segment, pred_name in segments.items():
        table_name = "tests_es.jvmm_amdocs_automated_churn_scores_{segment}".format(segment=segment)
        if logger: logger.info("Reading table '{}' for pred_name '{}'".format(table_name, pred_name))
        df = spark.read.table(table_name).where(col("pred_name")==pred_name)
        df = df.withColumn("segment", lit(segment))
        df_list.append(df)

    from pykhaos.utils.pyspark_utils import union_all
    df_scores = union_all(df_list) # msisdn is anonymized | campo2 deanonymized
    #logger.info("*** df_scores {}".format(df_scores.count()))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES INFO
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before incidences")

    start_time_incidences = time.time()
    df_agg = get_incidences_info(spark, ccc_end_date_, ccc_start_date_)
    df_scores_incidencias = df_scores.join(df_agg, on=["msisdn"], how="left")

    df_scores_incidencias = df_scores_incidencias.fillna({'IND_PBMA_SRV': 0, 'DETALLE_PBMA_SRV': "None"})
    df_scores_incidencias = df_scores_incidencias.withColumnRenamed("campo2", "msisdn_d")  # deanonimized
    df_scores_incidencias = df_scores_incidencias.withColumnRenamed("model_score", "comb_score")

    if logger: logger.info("Incidences - {} minutes".format( (time.time()-start_time_incidences)/60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before add_decile")
    start_time_decile = time.time()
    df_scores_incidencias = add_decile(df_scores_incidencias)
    logger.info("After add_decile - {} minutes".format( (time.time()-start_time_decile)/60))

    df_scores_incidencias = df_scores_incidencias.select(
        *['msisdn', 'msisdn_d', 'comb_score', DECILE_COL, 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'segment'])

    return df_scores_incidencias

def write_scores_incidences_file(df_scores_incidencias, closing_day):

    start_write = time.time()
    # HDFS
    OUTPUT_DIR_NAME = "/user/csanc109/projects/churn/incidencias/preds_comb_{}_all_incidences/".format(closing_day)

    # PROCESS MOVE AUTOMATICALLY THE OUTPUT TO THIS DIRECTORY
    OUTPUT_LOCAL_DIR = "/var/SP/data/home/csanc109/data/churn/amdocs/"

    # Be Sure this directory exists!!!!
    MYPC_DIR = "/Users/csanc109/Documents/Projects/Churn/Deliveries/{}".format(closing_day)

    df_scores_incidencias = df_scores_incidencias.repartition(200)

    logger.info("Before writing csv")
    start_time_csv = time.time()
    df_scores_incidencias.select(
        *['msisdn', 'msisdn_d', 'comb_score', DECILE_COL, 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV',
          'segment']).write.mode('overwrite').format(
        'csv').option('sep', '|').option('header', 'true').save(OUTPUT_DIR_NAME)

    logger.info("After writing - {} minutes".format((time.time() - start_time_csv) / 60))

    logger.info("Written {}".format(OUTPUT_DIR_NAME))

    from pykhaos.utils.hdfs_functions import move_hdfs_dir_to_local
    move_hdfs_dir_to_local(OUTPUT_DIR_NAME, OUTPUT_LOCAL_DIR)

    dir_name = os.path.basename(os.path.normpath(OUTPUT_DIR_NAME))
    OUTPUT_LOCAL_DIRNAME = os.path.join(OUTPUT_LOCAL_DIR, dir_name)

    scp_cmd = "scp -r milan-discovery-edge-387:{} {}".format(OUTPUT_LOCAL_DIRNAME, MYPC_DIR)
    print("SCP to move the file to your local pc, run:")
    print("\n\n '{}' \n\n".format(scp_cmd))

    logger.info("Process finished - {} minutes".format((time.time() - start_write) / 60.0))

    return OUTPUT_DIR_NAME

if __name__ == "__main__":

    ccc_start_date_ = "20181201"
    ccc_end_date_ = "20181231"
    running_time_onlymob = "preds_onlymob_for20181231_on20190109_083316"   # select distinct(pred_name) from tests_es.jvmm_amdocs_automated_churn_scores_onlymob order by pred_name desc limit 1
    running_time_mobileandfbb = "preds_mobileandfbb_for20181231_on20190109_142352"

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    print(ccc_start_date_, ccc_end_date_)

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    print("Process started...")

    import time
    start_time = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    from churn.utils.general_functions import init_spark
    spark = init_spark("add_indicadores_lista_scores_v2")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES + DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    segments = {"onlymob" : running_time_onlymob,
                "mobileandfbb" : running_time_mobileandfbb}

    df_scores_incidencias = generate_scores_incidences(spark, segments,ccc_end_date_, ccc_start_date_)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # WRITE DATAFRAME
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    output_dir = write_scores_incidences_file(df_scores_incidencias, ccc_end_date_)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))

    logger.info("Process ended successfully. Enjoy :)")


    # - - - - - - - - - - - -
    # PRINT SOME CHECKS
    # - - - - - - - - - - - -
'''
    from pyspark.sql.functions import count as sql_count
    logger.info("- - - - - - - - - - - - - - ")
    logger.info("CHECKS")
    logger.info("- - - - - - - - - - - - - - ")

    df_inci = spark.read.option("delimiter", "|").option("header", True).csv(output_dir)
    df_inci.where(col("IND_PBMA_SRV") == 0).groupby("IND_PBMA_SRV", "DETALLE_PBMA_SRV").agg(
        sql_count("*").alias("count")).show(1000, False)

    df_inci.select(DECILE_COL).groupBy([DECILE_COL]).agg(sql_count("*").alias("count")).show()
'''

