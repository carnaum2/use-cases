import sys
import datetime as dt


def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os
    print(os.path.abspath(imp.find_module('churn')[1]))
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

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger




if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(
        description='DataPreparation. Run with: spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=50 --conf spark.driver.memory=16g --conf spark.executor.memory=16g data_preparation_main.py --starting-day <StartingDay> --closing-day <ClosingDay> --personal-usage <flag_personal_usage>',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-f', '--inputfile', metavar='<input_file>', type=str,
                        help='Path to yaml file with options')
    args = parser.parse_args()

    print("Process started...")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    logger = set_paths_and_logger()

    from churn.utils.general_functions import init_spark
    spark = init_spark(app_name="churn_data_preparation")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # READ CONFIG
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Read config file (args.inputfile if not None or default file)
    from churn.config_manager.config_mgr import Config
    import imp
    import os
    config_obj = Config(args.inputfile, internal_config_filename=os.path.join(imp.find_module('churn')[1], "config_manager", "config", "internal_config_churn.yaml"))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # GET UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.engine.churn_data_loader import get_labeled_or_unlabeled_car
    # process the object to obtain a labeled or unlabeled car
    df_car = get_labeled_or_unlabeled_car(spark, config_obj)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SAVE UNLABELED/LABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if config_obj.get_save_car():
        from churn.datapreparation.engine.churn_data_loader import save_results
        save_results(df_car,config_obj)

    print("Process ended successfully. Enjoy :)")

