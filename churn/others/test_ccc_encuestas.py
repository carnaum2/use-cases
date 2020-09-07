import sys
import datetime as dt
from pyspark.sql.functions import concat_ws, col


def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    try:
        USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))
    except:
        print("Run this script from use-cases, and specify the path to this file churn/others/test_ccc_encuestas.py")
        sys.exit()


    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("** Added USE_CASES '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("** Added DEVEL_SRC '{}' to path".format(DEVEL_SRC))



    ENGINE_SRC = os.path.join(DEVEL_SRC, "amdocs_informational_dataset/")
    if ENGINE_SRC not in sys.path:
        sys.path.append(ENGINE_SRC)
        print("** Added ENGINE '{}' to path".format(ENGINE_SRC))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stdout, logger_name="")
    print("Logging to file {}".format(logging_file))
    logger.info("Logging to file {}".format(logging_file))

    return logger


def get_encuestas_filename(config_obj): #FIXME

    port_start_date = config_obj.get_start_port()
    port_end_date = config_obj.get_end_port()
    n_ccc= config_obj.get_ccc_range_duration()

    filename = '/user/csanc109/projects/churn/data/ccc_model/df_calls_for_portouts_nif_encuestas_{}_{}_{}'.format(
                                                                                                        port_start_date,
                                                                                                        port_end_date, abs(n_ccc))
    return filename

if __name__ == "__main__":

    print("Process started...")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    from churn.utils.general_functions import init_spark
    spark = init_spark()
    FORCE = True
    DEBUG = False
    PORTOUT_TABLE_COLS = ["msisdn_a", "portout_date"]  # ["portout_date",  "msisdn_a", "label", "days_from_portout"]
    n_ccc = -60  # FIXME -abs(config_obj.['data_preparation'][60])
    by_agg_ccc = "nif" # do not change it! msisdn is not fully implemented

    import time


    from churn.config_manager.track_bajas_config_mgr import TrackBajasConfig
    config_obj = TrackBajasConfig(filename='/var/SP/data/home/csanc109/src/devel/use-cases/churn/input/dp_ccc_model.yaml',
                        check_args=False)

    port_end_date = config_obj.get_end_port()

    from churn.datapreparation.general.ccc_data_loader import get_calls_for_portouts, get_calls_for_portouts_filename
    calls_for_portouts_filename = get_calls_for_portouts_filename(config_obj)

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if not check_hdfs_exists(calls_for_portouts_filename) or FORCE:
        if logger: logger.info("'{}' does not exist!".format(calls_for_portouts_filename))
        df_agg_all_ccc = get_calls_for_portouts(spark, config_obj,
                                                port_end_date, portout_select_cols=PORTOUT_TABLE_COLS,
                                                logger=logger)
        # if logger: logger.info("[Info ccc_model_data_loader] Number of NIFs df_agg_all_ccc: df_agg_all_ccc={} ".format(df_agg_all_ccc.count()))


        start_time = time.time()
        if logger: logger.info("Prepared to write to {}".format(calls_for_portouts_filename))

        df_agg_all_ccc = df_agg_all_ccc.withColumn('cat1_list', concat_ws(",", col('cat1_list')))
        df_agg_all_ccc = df_agg_all_ccc.withColumn('cat2_list', concat_ws(",", col('cat2_list')))
        df_agg_all_ccc = df_agg_all_ccc.drop('bucket_list', 'bucket_set')

        df_agg_all_ccc.write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save(
            calls_for_portouts_filename)

        print("Elapsed {} minutes".format((time.time() - start_time) / 60.0))

        sys.exit(1)
    else:
        if logger: logger.info("'{}' exists! Loading!".format(calls_for_portouts_filename))
        df_agg_all_ccc = spark.read.option("delimiter", "|").option("header", True).csv(calls_for_portouts_filename)



    from churn.datapreparation.engine.bajas_data_loader import load_reasons_encuestas

    #df_bajas_complete = get_complete_info_encuestas(spark)
    df_bajas_complete = load_reasons_encuestas(spark, config_obj=None) # by nif

    #logger.info("df_bajas_complete={}".format(df_bajas_complete.count()))

    # Join encuestas with ccc
    df_agg_ccc_encuestas = df_agg_all_ccc.join(df_bajas_complete.select("NIF", "REASON_ENCUESTA"),
                                               on=["NIF"], how="outer")

    #logger.info("df_agg_ccc_encuestas={}".format(df_agg_ccc_encuestas.count()))
    #df_agg_ccc_encuestas = df_agg_ccc_encuestas.repartition(1)


    encuestas_filename = get_encuestas_filename(config_obj)
    logger.info("Prepared to write to {}".format(encuestas_filename))
    df_agg_ccc_encuestas.write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save(encuestas_filename)
    logger.info("written {}".format(encuestas_filename))
    logger.info("Process ended successfully. Enjoy :)")


