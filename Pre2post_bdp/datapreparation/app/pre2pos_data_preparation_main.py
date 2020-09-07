import sys
import datetime as dt
import time


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
        USE_CASES = dirname(os.path.abspath(imp.find_module('Pre2post_bdp')[1]))
    except:
        print("Run this script from use-cases, and specify the path to this file churn/others/test_ccc_encuestas.py")
        sys.exit()


    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))

    # ENGINE_SRC = os.path.join(DEVEL_SRC, "amdocs_informational_dataset/")
    # if ENGINE_SRC not in sys.path:
    #     sys.path.append(ENGINE_SRC)
    #     print("** Added ENGINE '{}' to path".format(ENGINE_SRC))


    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "ccc_data_preparation_main_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger


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


def run_data_preparation(spark, input_data, logger):

    import time

    start_time_cfg = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # GET UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from Pre2post_bdp.datapreparation.engine.pre2post_model_data_loader import get_labeled_or_unlabeled_car

    # process the object to obtain a labeled or unlabeled car
    df_car = get_labeled_or_unlabeled_car(spark, input_data, logger=logger)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SAVE UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from Pre2post_bdp.utils.constants import JSON_DATA_PREPARATION, JSON_SAVE

    if input_data[JSON_DATA_PREPARATION][JSON_SAVE]:
        logger.info("Starting repartition 1")
        df_car = df_car.repartition(1)
        from Pre2post_bdp.datapreparation.engine.pre2post_model_data_loader import save_results
        logger.info("Starting save_results")
        hdfs_dir = save_results(df_car,input_data, csv=True, logger=logger) # use csv. then, it is better to use h2o
        logger.info("Ended save_results - '{}'".format(hdfs_dir))
    else:
        print("[INFO] User selected not to save the car")


    print("CONFIG TIME {} hours".format( (time.time()-start_time_cfg)/3600))




if __name__ == "__main__":

    start_time = time.time()

    import argparse

    parser = argparse.ArgumentParser(
        description='CCC model. Run with: spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=50 --conf spark.driver.memory=16g --conf spark.executor.memory=16g data_preparation_main.py --starting-day <StartingDay> --closing-day <ClosingDay> --personal-usage <flag_personal_usage>',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-f', '--inputfile', metavar='<input_file>', type=str,
                        help='Path to yaml file with options')
    args = parser.parse_args()

    print("CSANC109 Process started...")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    spark = _initialize("data preparation main")

    import os
    print(os.environ["SPARK_COMMON_OPTS"])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # READ CONFIG
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Read config file (args.inputfile if not None or default file)

    import json
    with open(args.inputfile) as f:
        input_data = json.load(f, encoding='utf-8')

    from Pre2post_bdp.utils.constants import JSON_USER_CONFIG_FILE
    input_data[JSON_USER_CONFIG_FILE] = args.inputfile

    run_data_preparation(spark, input_data, logger=logger)

    print("TOTAL TIME {} hours".format( (time.time()-start_time)/3600))
    print("Process ended successfully. Enjoy :)")
