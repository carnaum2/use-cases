import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, sum as sql_sum, count as sql_count, \
    datediff, unix_timestamp, from_unixtime


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
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "run_ccc_analysis_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger





if __name__ == "__main__":

    logger = set_paths_and_logger()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description='Generate table of extra feats',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    # parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=True,
    #                     help='Closing day YYYYMMDD (end of port period)')
    # parser.add_argument('-p', '--port_months_length', metavar='<port_months_length>', type=str, required=True,
    #                     help='length (in months) for the port period')
    # parser.add_argument('-b', '--by', metavar='<by>', type=str, required=False,
    #                     help='Aggregation parameter (msisdn/num_cliente). Default: msisdn')
    args = parser.parse_args()

    print(args)

    # CLOSING_DAY = args.closing_day
    # PORT_MONTHS_LENGTH = int(args.port_months_length)
    # BY = args.by

    # if not BY:
    #     BY = "msisdn"

    if logger: logger.info("Input params: CLOSING_DAY={} PORT_MONTHS_LENGTH={}".format(CLOSING_DAY, PORT_MONTHS_LENGTH))

    import time
    start_time = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SPARK
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn.utils.general_functions import init_spark
    spark = init_spark("generate_table_extra_feats")
    sc = spark.sparkContext

    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles, move_date_n_yearmonths
    from churn.analysis.ccc_churn.engine.data_loader import get_ccc_data, get_tgs, get_all_ports
    from churn.datapreparation.general.data_loader import get_active_services
    from churn.analysis.ccc_churn.engine.reporter import compute_results, SAVING_PATH, init_writer, print_sheet



    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))

    logger.info("Process ended successfully. Enjoy :)")

