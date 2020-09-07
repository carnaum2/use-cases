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

    EXTERNAL_LIB = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src", "devel", "pykhaos", "external_lib")
    if EXTERNAL_LIB not in sys.path:
        sys.path.append(EXTERNAL_LIB)
        print("Added '{}' to path".format(EXTERNAL_LIB))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger


def prepare_delivery(hdfs_dir, start_yyyymmdd, end_yyyymmdd):
    from subprocess import Popen, PIPE
    filename = hdfs_dir.split("/")[-1]
    dir_download = "/var/SP/data/home/csanc109/data/download/monitoring/".format(start_yyyymmdd, end_yyyymmdd)
    dir_delivery = "/var/SP/data/home/csanc109/data/delivery/monitoring/".format(start_yyyymmdd, end_yyyymmdd)
    file_extension = "csv"
    local_dir_deliverables = None

    # Download the *.csv files to local
    print("Moving filename='{}' from dir_download={} to local".format(filename, dir_download))
    p = (Popen(['hadoop',
                'fs',
                '-copyToLocal',
                hdfs_dir,
                dir_download + '.'], stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print("Downloaded directory {}".format(os.path.join(dir_download, filename)))
    else:
        print(err)
        print("Something happen while trying to download the directory {}".format
              (os.path.join(dir_download, filename + "." + file_extension)))

    # Create dir download if does not exist
    if not os.path.exists(dir_delivery):
        print("Creating directory {}".format(dir_delivery))
        os.makedirs(dir_delivery)

    print("Merging {}".format(dir_download + filename + '/*.csv'))
    # Merge the files
    p = (Popen(' '.join(['awk',
                         "'FNR==1 && NR!=1{next;}{print}'",
                         dir_download + filename + '/*.csv',
                         '>',
                         # Hay que poner shell=True cuando se ejecuta una string
                         os.path.join(dir_delivery, filename + "." + file_extension)]), shell=True, stdin=PIPE,
               stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print("File {}.csv successfully created".format(os.path.join(dir_delivery, filename)))
    else:
        print("Something went wrong creating the file {}".format(os.path.join(dir_delivery, filename)))

    execute_command_in_local = \
        'scp {}@milan-discovery-edge-387:{}.{} {}'.format(os.getenv("USER"),
                                                          os.path.join(dir_delivery, filename),
                                                          file_extension,
                                                          local_dir_deliverables if local_dir_deliverables else "<local_directoy>")

    print \
        ("To download the file in your local computer, type from your local: \n\n {}".format(execute_command_in_local))

    print("Process ended successfully")


if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT PARAMETERS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls
    from pyspark.sql.functions import col, lit, asc, create_map
    # from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
    # from pyspark.sql.functions import array, regexp_extract
    from itertools import chain


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
                        help='End day YYYYMMDD for period')
    parser.add_argument('-s', '--starting_day', metavar='<STARTING_DAY>', type=str, required=True,
                        help='Start day YYYYMMDD for period')
    parser.add_argument('-w', '--write', action='store_true', help='Save df and create csv')
    args = parser.parse_args()

    import pprint
    pprint.pprint(args)

    STARTING_DAY = args.starting_day
    CLOSING_DAY = args.closing_day
    WRITE_DF = args.write

    if logger: logger.info("STARTING_DAY={}  CLOSING_DAY={}   WRITE_DF={}".format(STARTING_DAY, CLOSING_DAY, WRITE_DF))



    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT PARAMETERS - PREDICT MODEL (DO NOT MODIFY UNLESS YOU ARE SURE WHAT YOUR DOING) :)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    HDFS_DIR = "/user/csanc109/projects/car/data_monitoring/monitoring_{}_{}".format(STARTING_DAY, CLOSING_DAY)

    tables_dict = {
        "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.1/parquet": "netscout",  # "raw_es.netscout_subscriberuserplane"
        "/data/raw/vf_es/customerprofilecar/SERVICESOW/1.0/parquet": "services",
        "/data/raw/vf_es/priceplanstariffs/PLANPRIC_ONO/1.0/parquet": "services",
        "/data/raw/vf_es/customerprofilecar/ORDERCRMOW/1.1/parquet": "order",
        "/data/raw/vf_es/customerprofilecar/ORDERCLASOW/1.0/parquet": "order",
        "/data/raw/vf_es/cvm/CUSTOMERPENAL/1.0/parquet": "customer_penalties",  # "raw_es.cvm_customerpenal"
        "/data/raw/vf_es/customerprofilecar/SERVICESDIMOW/1.0/parquet": "customer",  # "raw_es.customerprofilecar_servicesdimow"
        "/data/raw/vf_es/campaign/MsisdnContactHist/1.0/parquet": "campaigns",
        "/data/raw/vf_es/campaign/MsisdnResponseHist/1.0/parquet": "campaigns",
        "/data/raw/vf_es/campaign/NifContactHist/1.0/parquet": "campaigns",
        "/data/raw/vf_es/campaign/NifResponseHist/1.0/parquet": "campaigns",
        "/data/raw/vf_es/billingtopsups/POSTBILLSUMOW/1.1/parquet": "geneva_traffic",
        "/data/raw/vf_es/billingtopsups/TRAFFICTARIFOW/1.0/parquet": "geneva_traffic",
        "/data/raw/vf_es/permsandprefs/Subscriber_Consent_Feed/1.0/parquet": "permsandprefs",
        "/data/raw/vf_es/devicecataloglocal/IMEI/1.0/parquet": "device_catalogue",  # raw_es.devicecataloglocal_imei"
        "/data/raw/vf_es/customerprofilecar/CUSTOMEROW/1.0/parquet": "customer_penalties",
        "/data/raw/vf_es/customerprofilecar/CUSTFICTICALOW/1.0/parquet" : "billing"
    }



    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -

    print("Process started...")


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()


    from churn.utils.general_functions import init_spark
    spark = init_spark("predict_and_delivery")

    import time
    start_time = time.time()


    from pykhaos.utils.date_functions import days_range
    mapping_expr = create_map([lit(x) for x in chain(*tables_dict.items())])

    dates_range = days_range(STARTING_DAY, CLOSING_DAY)
    df_join = None
    for date_ in dates_range:
        df_eval = spark.read.table("raw_es.evaluation").where(
            (col("interface").isin(tables_dict.keys())) & (col("year") == date_[:4]) & (
                    col("month") == str(int(date_[4:6]))) & (col("day") == str(int(date_[6:])))).withColumnRenamed(
            "num_registry", date_)
        df_eval = df_eval.drop_duplicates(["interface"])

        if df_join is None:
            df_join = df_eval.select("interface", date_)
        else:
            df_join = df_join.join(df_eval.select("interface", date_), on=["interface"], how="outer")
    df_join = df_join.withColumn('source', mapping_expr[df_join['interface']])
    df_join = df_join.select(["source", "interface"] + dates_range)
    df_join = df_join.sort(asc("source"))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SHOW DF
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    df_join.show(100, False)


    if WRITE_DF:
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # WRITE DF
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        df_join.write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save(HDFS_DIR)

        if logger: logger.info("Created file '{}'".format(HDFS_DIR))
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # CREATE CSV IN LOCAL
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        prepare_delivery(HDFS_DIR, STARTING_DAY, CLOSING_DAY)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # END
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))
    if logger: logger.info("Process ended successfully. Enjoy :)")



