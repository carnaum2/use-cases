import sys
import datetime as dt
import time

logger = None



def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        from churn.utils.constants import CHURN_DELIVERIES_DIR
        root_dir = CHURN_DELIVERIES_DIR
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


def run_data_preparation(spark, config_obj, logger):
    import time

    start_time_cfg = time.time()
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # GET UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.engine.ccc_model_data_loader import get_labeled_or_unlabeled_car
    # process the object to obtain a labeled or unlabeled car
    df_car = get_labeled_or_unlabeled_car(spark, config_obj, logger=logger)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SAVE UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if config_obj.get_save_car():
        if logger: logger.info("Starting repartition 1")
        df_car = df_car.repartition(1)
        from churn.datapreparation.engine.ccc_model_data_loader import save_results
        if logger: logger.info("Starting save_results")
        hdfs_dir = save_results(df_car,config_obj, csv=True, logger=logger) # use csv. then, it is better to use h2o
        if logger: logger.info("Ended save_results")

        if df_car.rdd.getNumPartitions()>1:
            print"NUM PARTITIONS {}".format(df_car.rdd.getNumPartitions())
            num_partitions = "200"
            from pykhaos.utils.hdfs_functions import get_merge_hdfs, mv_dir, create_directory
            partitioned_dir = hdfs_dir+"_"+num_partitions
            mv_dir(hdfs_dir, partitioned_dir)
            create_directory(hdfs_dir)
            import time
            local_filename = '/var/SP/data/home/{}/tmp/{}.csv'.format(os.getenv("USER"), int(time.time()))
            get_merge_hdfs(partitioned_dir, local_filename, "*.csv", hdfs_dir)
    else:
        print("[INFO] User selected not to save the car")


    print("CONFIG TIME {} hours".format( (time.time()-start_time_cfg)/3600))


def start_process_config(spark, config):
    import yaml
    #from churn.config_manager.ccc_model_config_mgr import CCCmodelConfig
    from churn.utils.constants import YAML_ENABLED, YAML_CONFIGS, YAML_USER_CONFIG_FILE, YAML_CONFIG_NAME, YAML_CLOSING_DAY, YAML_SEGMENT_FILTER
    from churn.config_manager.ccc_model_config_mgr import CCCmodelConfig

    if YAML_CONFIGS in config: # more than one config
        for cfg_name, cfg_values in config[YAML_CONFIGS].items():
            if not cfg_values[YAML_ENABLED]:
                print("Skipping config {}".format(cfg_name))
                continue
            if logger: logger.info(" * * * * * * * * * * * * * * * ")
            if logger: logger.info(cfg_name)
            if logger: logger.info(" * * * * * * * * * * * * * * * ")
            config_obj = CCCmodelConfig(filename=None, check_args=False)
            config_obj.set_extra_info(cfg_values)

            auto_created_yaml = '/tmp/auto_build_config_for_delivery_{}_{}.yaml'.format(cfg_values[YAML_CLOSING_DAY], cfg_values[YAML_SEGMENT_FILTER])
            with open(auto_created_yaml, 'w') as outfile:
                yaml.dump(config, outfile, default_flow_style=False)
                if logger: logger.info("Created file {}".format(auto_created_yaml))
            config_obj.set_extra_info({YAML_USER_CONFIG_FILE : auto_created_yaml, YAML_CONFIG_NAME:cfg_name})
            config_obj.check_args()

            run_data_preparation(spark, config_obj, logger)
    else:
        if logger: logger.error("Config not build properly. Program will stop here!")
        sys.exit()



def start_process(spark, input_file, closing_day=None):
    import yaml
    from churn.config_manager.ccc_model_config_mgr import CCCmodelConfig
    from churn.utils.constants import YAML_ENABLED, YAML_CONFIGS, YAML_USER_CONFIG_FILE, YAML_CONFIG_NAME, YAML_CLOSING_DAY
    config = yaml.load(open(input_file))
    if YAML_CONFIGS in config: # more than one config
        for cfg_name, cfg_values in config[YAML_CONFIGS].items():
            if not cfg_values[YAML_ENABLED]:
                print("Skipping config {}".format(cfg_name))
                continue
            if logger: logger.info(" * * * * * * * * * * * * * * * ")
            if logger: logger.info(cfg_name)
            if logger: logger.info(" * * * * * * * * * * * * * * * ")
            config_obj = CCCmodelConfig(filename=None, check_args=False)
            config_obj.set_extra_info(cfg_values)
            config_obj.set_extra_info({YAML_USER_CONFIG_FILE : input_file, YAML_CONFIG_NAME:cfg_name})
            if closing_day:
                if logger: logger.info("closing_day={} provided by argument - overwritting file closing_day".format(closing_day))
                config_obj.set_extra_info({YAML_CLOSING_DAY: closing_day})
            config_obj.check_args()

            run_data_preparation(spark, config_obj, logger)
    else:
        config_obj = CCCmodelConfig(args.inputfile)
        run_data_preparation(spark, config_obj, logger)




if __name__ == "__main__":

    start_time = time.time()

    import argparse

    parser = argparse.ArgumentParser(
        description='CCC model. Run with: spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=50 --conf spark.driver.memory=16g --conf spark.executor.memory=16g data_preparation_main.py --starting-day <StartingDay> --closing-day <ClosingDay> --personal-usage <flag_personal_usage>',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-f', '--inputfile', metavar='<input_file>', type=str,
                        help='Path to yaml file with options')
    parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=False,
                        help='Closing day YYYYMMDD (same used for the car generation)')
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
    start_process(spark, args.inputfile, closing_day=args.closing_day)


    # import yaml
    # from churn.config_manager.ccc_model_config_mgr import CCCmodelConfig
    # from churn.utils.constants import YAML_ENABLED, YAML_CONFIGS, YAML_USER_CONFIG_FILE, YAML_CONFIG_NAME, YAML_CLOSING_DAY
    # config = yaml.load(open(args.inputfile))
    # if YAML_CONFIGS in config: # more than one config
    #     for cfg_name, cfg_values in config[YAML_CONFIGS].items():
    #         if not cfg_values[YAML_ENABLED]:
    #             print("Skipping config {}".format(cfg_name))
    #             continue
    #         logger.info(" * * * * * * * * * * * * * * * ")
    #         logger.info(cfg_name)
    #         logger.info(" * * * * * * * * * * * * * * * ")
    #         config_obj = CCCmodelConfig(filename=None, check_args=False)
    #         config_obj.set_extra_info(cfg_values)
    #         config_obj.set_extra_info({YAML_USER_CONFIG_FILE : args.inputfile, YAML_CONFIG_NAME:cfg_name})
    #         if args.closing_day:
    #             logger.info("closing_day={} provided by argument - overwritting file closing_day".format(args.closing_day))
    #             config_obj.set_extra_info({YAML_CLOSING_DAY: args.closing_day})
    #         config_obj.check_args()
    #
    #         run_data_preparation(spark, config_obj, logger)
    # else:
    #     config_obj = CCCmodelConfig(args.inputfile)
    #     run_data_preparation(spark, config_obj, logger)

    logger.info("TOTAL TIME {} hours".format( (time.time()-start_time)/3600))
    logger.info("Process ended successfully. Enjoy :)")
