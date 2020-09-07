# coding: utf-8


import sys
import datetime as dt
import os
import time
import re

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# this script is intended for generating an unique file to use
# in the levers model.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=1G --executor-cores 8 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 10G --driver-memory 2G --conf spark.dynamicAllocation.maxExecutors=15 churn/others/merge_dp.py

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


def get_closing_day_from_filename(ff):
    gg = re.match(
        "^\/data\/udf\/vf_es\/churn\/ccc_model\/comercial\/df_[0-9]{8}_[0-9]{8}_c([0-9]{8})_n60_comercial_msisdn$",
        ff)
    return gg.group(1)

def _initialize( app_name, min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g"):
    import time
    start_time = time.time()

    print("_initialize spark")
    import pykhaos.utils.pyspark_configuration as pyspark_config
    sc, spark, sql_context = pyspark_config.get_spark_session(app_name=app_name, log_level="OFF", min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores,
                             executor_memory = executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark


if __name__ == "__main__":

    start_time = time.time()

    filenames = ["/data/udf/vf_es/churn/ccc_model/comercial/df_20180915_20181014_c20180914_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20180922_20181021_c20180921_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181001_20181031_c20180930_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181008_20181107_c20181007_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181015_20181114_c20181014_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181022_20181121_c20181021_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181101_20181130_c20181031_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181108_20181207_c20181107_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20181108_20181207_c20190107_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190101_20190131_c20181231_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190115_20190214_c20190114_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190122_20190221_c20190121_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190201_20190228_c20190131_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190208_20190307_c20190207_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190215_20190314_c20190214_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190222_20190321_c20190221_n60_comercial_msisdn",
                 "/data/udf/vf_es/churn/ccc_model/comercial/df_20190301_20190331_c20190228_n60_comercial_msisdn"]




    import re
    closing_day_list = []
    for ff in filenames:
        closing_day = get_closing_day_from_filename(ff)
        print(closing_day)
        if closing_day:
            closing_day_list.append(closing_day)
        else:
            print("Impossible to parse '{}'".format(ff))
            sys.exit()


    OUTPUT_FILENAME = "/data/udf/vf_es/churn/ccc_model/comercial/df_c{}_c{}_n60_comercial_msisdn".format(sorted(closing_day_list)[0],
                                                                                                         sorted(closing_day_list)[-1])


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    logger.info("Output filename will be '{}'".format(OUTPUT_FILENAME))

    logger.info("Process started...")

    from pykhaos.utils.pyspark_utils import union_all
    import pykhaos.utils.pyspark_configuration as pyspark_config
    import time

    spark = _initialize("merge-dp")

    logger.info("Ended spark session: {} secs".format(time.time() - start_time))


    proxy_cols = ["TOTAL_COMERCIAL", "TOTAL_NO_COMERCIAL", "COMERCIAL", "NO_COMERCIAL", "PRECIO", "TERMINAL",
                  "CONTENIDOS", "SERVICIO/ATENCION",
                  "TECNICO", "BILLING", "FRAUD", "NO_PROB"]
    useless_cols = ["DIR_LINEA1", "DIR_LINEA2", "DIR_LINEA3", "DIR_LINEA4", "DIR_FACTURA1", "x_user_facebook",
                    "x_user_twitter",
                    "DIR_FACTURA2", "DIR_FACTURA3", "DIR_FACTURA4", "num_cliente_car", "num_cliente", "NOMBRE",
                    "PRIM_APELLIDO",
                    "SEG_APELLIDO", "NOM_COMPLETO", "TRAT_FACT", 'NOMBRE_CLI_FACT', 'APELLIDO1_CLI_FACT',
                    'APELLIDO2_CLI_FACT',
                    'NIF_CLIENTE', 'DIR_NUM_DIRECCION', 'CTA_CORREO_CONTACTO', 'NIF_FACTURACION', 'CTA_CORREO', 'OBJID',
                    'TACADA',
                    'IMSI', 'OOB', 'CAMPO1', 'CAMPO2', 'CAMPO3', u'bucket_1st_interaction',
                    u'bucket_latest_interaction',
                    u'portout_date', 'codigo_postal_city', "PRICE_SRV_BASIC", "FBB_UPGRADE", "x_tipo_cuenta_corp",
                    "cta_correo_flag", "cta_correo_server", "DESC_TARIFF", "ROAM_USA_EUR", u'SUPEROFERTA2', u'rowNum',
                    u'age']

    from pyspark.sql.functions import col, lit

    # to_remove = imputed_avg  + useless_cols + proxy_cols + bucket_cols + novalen_penal_cols
    # to_remove = list(set(df_h2o.columns) & set(to_remove)) # intersection

    df_union = None
    for cc, ff in zip(closing_day_list, filenames):
        print(ff)
        df_tar1 = spark.read.option("delimiter", "|").option("header", True).csv(ff)
        df_tar1 = df_tar1.withColumn("closing_day", lit(cc))
        if df_union is None:
            df_union = df_tar1
        else:
            cols_union = list((set(df_union.columns) & set(df_tar1.columns)) - set(proxy_cols + useless_cols))
            df_union = union_all([df_union.select(cols_union), df_tar1.select(cols_union)])

    logger.info("Saving....")
    from pykhaos.utils.hdfs_functions import save_df_to_hdfs_csv

    df_union = df_union.repartition(1)
    save_df_to_hdfs_csv(df_union, OUTPUT_FILENAME)
    logger.info("end saving in file '{}'".format(OUTPUT_FILENAME))

    name_file = os.path.basename(os.path.normpath(OUTPUT_FILENAME))

    table_name = "tests_es.csanc109_{}".format(name_file)

    (df_union.where(col('msisdn').isNotNull())
     # .where(length(col('msisdn')) == 9)
     .write
     .format('parquet')
     .mode('overwrite')
     .saveAsTable(table_name))

    logger.info("Saved into table {}".format(table_name))


    logger.info("Process ended successfully. Enjoy :)")

    print("Process ended successfully. Enjoy :)")

