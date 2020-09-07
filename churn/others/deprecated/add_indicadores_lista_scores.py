import sys
import datetime as dt
import os


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




if __name__ == "__main__":

    ccc_start_date_ = "20181115"
    ccc_end_date_ = "20181214"

    ####
    # Move scores file sent by justshare to /var/SP/data/home/csanc109/data/churn/amdocs/:
    # $ scp preds_comb_YYYYMMDD_all.txt milan-discovery-edge-387:/var/SP/data/home/csanc109/data/churn/amdocs/
    ##############


    # First step, move the .txt file to hdfs
    HDFS_SRC_DIR = "/user/csanc109/projects/churn/data/incidencias/"

    #HDFS
    OUTPUT_DIR_NAME = "/user/csanc109/projects/churn/incidencias/preds_comb_{}_all_incidences/".format(ccc_end_date_)

    #OUTPUT_FILENAME = "/user/csanc109/projects/churn/incidencias/preds_comb_{}_all_incidences.csv".format(ccc_end_date_)
    # PROCESS MOVE AUTOMATICALLY THE OUTPUT TO THIS DIRECTORY
    OUTPUT_LOCAL_DIR = "/var/SP/data/home/csanc109/data/churn/amdocs/"

    # Be Sure this directory exists!!!!
    MYPC_DIR = "/Users/csanc109/Documents/Projects/Churn/Deliveries/{}".format(ccc_end_date_)

    print(ccc_start_date_, ccc_end_date_)

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    PREDS_COMB_FILENAME = "preds_comb_{}_all.txt".format(ccc_end_date_)
    HDFS_SRC_FILENAME = os.path.join(HDFS_SRC_DIR, PREDS_COMB_FILENAME)  # msisdn es msisdn_d
    SCORES_FILENAME = os.path.join("/var/SP/data/home/csanc109/data/churn/amdocs", PREDS_COMB_FILENAME)



    print("Process started...")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    from churn.utils.general_functions import init_spark
    spark = init_spark()

    from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls
    from amdocs_informational_dataset import engine
    from pyspark.sql.functions import collect_set, concat, size, coalesce, col, lpad, struct, count as sql_count, lit, \
        min as sql_min, max as sql_max, collect_list, udf, when
    from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
    from pyspark.sql.functions import array, regexp_extract
    from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType, FloatType
    import re



    from pykhaos.utils.hdfs_functions import move_local_file_to_hdfs
    # hdfs dfs - put preds_comb_20181130_all.txt /user/csanc109/projects/churn/
    move_local_file_to_hdfs(HDFS_SRC_DIR, SCORES_FILENAME)


    #fichero =
    #fichero = "/user/csanc109/projects/churn/preds_comb_20181031_all.txt"  # msisdn es msisdn_d

    closing_day = ccc_end_date_

    ccc = CallCentreCalls(spark)
    ccc.prepareFeatures(ccc_end_date_, ccc_start_date_)
    df_all = ccc.all_interactions

    from pyspark.sql.functions import array, regexp_extract, concat_ws, upper, sum as sql_sum

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
    df_all = df_all.withColumn("dos", concat_ws("__", upper(col("INT_Tipo")), upper(col("INT_Subtipo"))))

    # la llamada esta en la lista de tipis que generan churn
    df_all = df_all.withColumn("IS_TIPIS_CHURN", when(col("tres").isin(lista_tipis), 1).otherwise(0))

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

    df_agg = df_agg.withColumn("DETALLE_PBMA_SRV",
                               when(coalesce(size(col("lista_de_dos")), lit(0)) == 0, "None").otherwise(
                                   get_mode_udf(col("lista_de_dos"))))
    df_agg = df_agg.withColumn("IND_PBMA_SRV", when(col("NUM_PBMA_SRV") > 0, 1).otherwise(0))

    df_scores = spark.read.option("delimiter", "|").option("header", True).csv(HDFS_SRC_FILENAME)

    logger.info("*** df_scores {}".format(df_scores.count()))

    df_scores = df_scores.withColumnRenamed("msisdn", "msisdn_d_scores")

    hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/' #
    hdfs_partition_path = 'year=' + str(int(ccc_end_date_[:4])) + '/month=' + str(
        int(ccc_end_date_[4:6])) + '/day=' + str(int(ccc_end_date_[6:8]))
    path_service = hdfs_write_path_common + 'service/' + hdfs_partition_path
    df_service = (spark.read.load(path_service)).select("msisdn", "campo2").withColumnRenamed("msisdn",
                                                                                              "msisdn_a_service")

    df_service = df_service.dropDuplicates(['msisdn_a_service'])
    df_service = df_service.dropDuplicates(['campo2'])

    df_scores_join = df_scores.join(df_service, on=df_scores["msisdn_d_scores"] == df_service["campo2"], how="left")

    logger.info("*** df_scores_join {}".format(df_scores_join.count()))
    #logger.info("*** df_scores_join msisdn_a_service {}".format(df_scores_join.where(col("msisdn_a_service").isNotNull()).count()))


    df_scores_incidencias = df_scores_join.join(df_agg, on=df_scores_join["msisdn_a_service"] == df_agg["msisdn"],
                                                how="left")

    logger.info("*** df_scores_incidencias {}".format(df_scores_incidencias.count()))

    # pyspark cannot write arrays to csv's
    from pyspark.sql.functions import concat_ws

    # df_scores_incidencias = df_scores_incidencias.withColumn('bucket_list',
    #                                                          concat_ws(",", col('bucket_list'))).withColumn(
    #     'bucket_set', concat_ws(",", col('bucket_set')))

    df_scores_incidencias = df_scores_incidencias.fillna({'IND_PBMA_SRV': 0, 'DETALLE_PBMA_SRV': "None"})

    df_scores_incidencias = df_scores_incidencias.drop("msisdn")  # anonimized
    df_scores_incidencias = df_scores_incidencias.withColumnRenamed("msisdn_d_scores", "msisdn")  # deanonimized

    df_scores_incidencias = df_scores_incidencias.withColumn("comb_score", col("comb_score").cast(FloatType()))
    df_scores_incidencias = df_scores_incidencias.orderBy('comb_score', ascending=False)

    df_scores_incidencias = df_scores_incidencias.repartition(1)
    # df_scores_incidencias.select(*['msisdn', 'comb_score', 'comb_decile', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'num_averias', 'num_incidencias', 'num_interactions', 'days_since_first_interaction',
    #  'days_since_latest_interaction', 'num_ivr_interactions'] + cols_problemas).write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save('/tmp/csanc109/churn/preds_comb_20181031_all_incidences.csv.v13')


    df_scores_incidencias.select(
        *['msisdn', 'comb_score', 'comb_decile', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV']).write.mode('overwrite').format(
        'csv').option('sep', '|').option('header', 'true').save(OUTPUT_DIR_NAME)

    print("Written {}".format(OUTPUT_DIR_NAME))

    from pykhaos.utils.hdfs_functions import move_hdfs_dir_to_local
    move_hdfs_dir_to_local(OUTPUT_DIR_NAME, OUTPUT_LOCAL_DIR)

    dir_name = os.path.basename(os.path.normpath(OUTPUT_DIR_NAME))
    OUTPUT_LOCAL_DIRNAME = os.path.join(OUTPUT_LOCAL_DIR, dir_name)

    scp_cmd = "scp -r milan-discovery-edge-387:{} {}".format(OUTPUT_LOCAL_DIRNAME, MYPC_DIR)
    print("SCP to move the file to your local pc, run:")
    print("\n\n '{}' \n\n".format(scp_cmd))

    print("Process ended successfully. Enjoy :)")


