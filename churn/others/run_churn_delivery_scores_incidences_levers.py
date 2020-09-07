# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer

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

FINAL_COLS = ['msisdn', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                      'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'palanca']


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
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))


    print(sys.path)
    return logger


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

    cols_to_write = ['msisdn', 'msisdn_d', 'comb_score', DECILE_COL, 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                     'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'segment']

    df_scores_incidencias.select(*cols_to_write).write.mode('overwrite').format(
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



def generate_scores_incidences(spark, segments,ccc_end_date_, ccc_start_date_):

    import time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # READ TABLES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    df_list = []
    for segment, pred_name in segments.items():
        table_name = "tests_es.jvmm_amdocs_automated_churn_scores"
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
    df_scores_incidencias = df_scores_incidencias.cache()
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Before add_decile")
    start_time_decile = time.time()
    df_scores_incidencias = add_decile(df_scores_incidencias)
    if logger: logger.info("After add_decile - {} minutes".format( (time.time()-start_time_decile)/60))

    df_scores_incidencias = df_scores_incidencias.select(
        *['msisdn', 'msisdn_d', 'comb_score', DECILE_COL, 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'segment'])

    df_scores_incidencias = df_scores_incidencias.where(col('msisdn').isNotNull())

    return df_scores_incidencias



def add_decile(df_scores_incidencias):
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


if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT PARAMETERS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description='Merge scores, incidences and levers list. Run with: spark2-submit --conf spark.driver.port=58100 \
        --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 \
        --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 \
        --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 \
        --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 \
        --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 50G \
        --driver-memory 2G --conf spark.dynamicAllocation.maxExecutors=15 churn/others/run_churn_delivery_scores_incidences_levers.py',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-o', '--predname_onlymob', metavar='<predname_onlymob>', type=str, required=True,
                        help='Pred name for onlymob scores (assumed scores are in table "tests_es.jvmm_amdocs_automated_churn_scores"')
    parser.add_argument('-m', '--predname_mobileandfbb', metavar='<predname_mobileandfbb>', type=str, required=True,
                        help='Pred name for mobileandfbb scores (assumed scores are in table "tests_es.jvmm_amdocs_automated_churn_scores"')
    args = parser.parse_args()

    print(args)

    pred_name_onlymob = args.predname_onlymob
    pred_name_mobileandfbb = args.predname_mobileandfbb
    CLOSING_DAY = args.closing_day

    # select distinct(pred_name) from tests_es.jvmm_amdocs_automated_churn_scores order by pred_name desc limit 2
    #pred_name_onlymob = "preds_onlymob_for20190114_on20190123_084152"
    #pred_name_mobileandfbb = "preds_mobileandfbb_for20190114_on20190123_171550"
    #CLOSING_DAY = "20190114"

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

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -

    print("Process started...")


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    from churn.resources.call_centre_calls import CallCentreCalls

    from churn.utils.general_functions import init_spark
    spark = init_spark("predict_and_delivery")

    import time
    start_time = time.time()
    from churn.models.ccc.delivery.delivery_manager import PROJECT_NAME, DIR_DELIVERY
    from pykhaos.utils.constants import WHOAMI

    from pykhaos.utils.date_functions import move_date_n_days
    ccc_start_date_ = move_date_n_days(CLOSING_DAY, -30, str_fmt="%Y%m%d")

    NAME_TABLE_DEANONYMIZED = 'tests_es.{}_tmp_{}_{}_notprepared'.format(WHOAMI, PROJECT_NAME, CLOSING_DAY)
    name_file_delivery = '{}_delivery_{}_{}'.format(PROJECT_NAME, CLOSING_DAY,
                                                    dt.datetime.now().strftime("%Y%m%d_%H%M%S"))

    if logger: logger.info("Summary:")
    if logger: logger.info("\t CLOSING_DAY = {}".format(CLOSING_DAY))
    if logger: logger.info("\tpred_name onlymob = '{}'".format(pred_name_onlymob))
    if logger: logger.info("\tpred_name mobileandfbb = '{}'".format(pred_name_mobileandfbb))
    #logger.info("\t incidencias filename = '{}'".format(SCORES_INCIDENCES_FILENAME))
    if logger: logger.info("\tModel stored at '{}'".format(PREDICT_MODEL_PATH))
    if logger: logger.info("\t table deanonymized   = '{}'".format(NAME_TABLE_DEANONYMIZED))
    if logger: logger.info("\t delivery filename    = '{}'".format(os.path.join(DIR_DELIVERY, name_file_delivery+".txt")))
    if logger: logger.info("\t ccc_range            = '{} - '{}'".format(ccc_start_date_, CLOSING_DAY))
    if logger: logger.info("\t generate delivery    = '{}".format(GENERATE_DELIVERY))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MAKE PREDICTIONS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn.models.ccc.delivery.delivery_manager import make_predictions

    if logger: logger.info("Make predictions")
    start_time_predictions = time.time()
    df_lever_predict_hidden = make_predictions(spark, PREDICT_MODEL_NAME, PREDICT_MODEL_PATH,
                                               CLOSING_DAY, h2o_port=54322)  # returns a column msisdn_a (anonymized)

    print("Prediction size {}".format(df_lever_predict_hidden.count()))

    if logger: logger.info("Make predictions - {} minutes".format( (time.time()-start_time_predictions)/60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADD INCIDENCES + DECILE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    segments = {"onlymob": pred_name_onlymob,
                "mobileandfbb": pred_name_mobileandfbb}

    df_scores_incidencias = generate_scores_incidences(spark, segments, CLOSING_DAY, ccc_start_date_)

    from pykhaos.utils.date_functions import move_date_n_yearmonths

    # Use the previous YYYYMM to get the service problems
    yyyymm_str = move_date_n_yearmonths(CLOSING_DAY[:6], -1)
    if logger: logger.info("Using yyyymm={} to obtain the service_problems".format(yyyymm_str))
    df_scores_incidencias = add_service_problems(spark, df_scores_incidencias, yyyymm_str, CLOSING_DAY)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MERGE PREDICTIONS WITH SCORES AND INCIDENCES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    try:
        from churn.models.ccc.delivery.delivery_manager import merging_process_2, prepare_delivery, make_predictions, DIR_DOWNLOAD
        start_merge = time.time()
        merging_process_2(df_lever_predict_hidden, df_scores_incidencias, NAME_TABLE_DEANONYMIZED, FINAL_COLS, save_table=GENERATE_DELIVERY)
        if logger: logger.info("merging_process - {} minutes".format( (time.time()-start_merge)/60.0))
    except Exception as e:
        if logger: logger.error("merging_process_2 failed. Reason:")
        if logger: logger.error(e)
        #if logger: logger.info("Calling to write_scores_incidences_file with closing_day={} to save progress".format(CLOSING_DAY))
        #write_scores_incidences_file(df_scores_incidencias, CLOSING_DAY)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # PREPARE DELIVERY
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if GENERATE_DELIVERY:
        start_delivery = time.time()
        prepare_delivery(spark, PROJECT_NAME , CLOSING_DAY, NAME_TABLE_DEANONYMIZED, name_file_delivery, DIR_DOWNLOAD, DIR_DELIVERY)
        if logger: logger.info("merging_process - {} minutes".format( (time.time()-start_delivery)/60.0))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # END
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))
    if logger: logger.info("Process ended successfully. Enjoy :)")



