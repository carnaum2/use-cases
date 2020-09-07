


import sys

def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import datetime as dt

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
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "generate_table_extra_feats_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    import logging
    logging.getLogger('matplotlib').setLevel(logging.WARNING)


    return logger


def get_metadata(spark):

    import time

    from churn.metadata.metadata import Metadata
    impute_na = Metadata.get_tgs_impute_na() + Metadata.get_ccc_impute_na() + \
                Metadata.get_pbma_srv_impute_na() + Metadata.get_devices_impute_na() + Metadata.get_others_impute_na() + \
                Metadata.get_scores_impute_na()
    df_metadata = Metadata.create_metadata_df(spark, impute_na)

    # ----
    from pykhaos.utils.scala_wrapper import convert_df_to_pyspark, get_scala_sc

    start_time_metadata = time.time()
    scala_sc = get_scala_sc(spark)
    sc = spark.sparkContext
    df_scala_metadata = sc._jvm.metadata.Metadata.generateMetadataTable(scala_sc)
    df_py_metadata = convert_df_to_pyspark(spark, df_scala_metadata)
    if logger: logger.info("Ended metadata - {} minutes".format( (time.time()-start_time_metadata)/60.0))
    #map_types = dict(df_py_metadata.rdd.map(lambda x: (x[0], float(x[3]) if x[2] == "double" else x[3])).collect())

    # - - - -
    from pykhaos.utils.pyspark_utils import union_all
    df_metadata = union_all([df_metadata, df_py_metadata])

    return df_metadata


if __name__ == "__main__":

    logger = set_paths_and_logger()

    import argparse

    parser = argparse.ArgumentParser(
        description='Generate table of extra feats',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-v', '--version', metavar='<VERSION>', type=str, required=True,
                        help='Version number to include in the path directory. Default=no version')
    parser.add_argument('-s', '--stable', action='store_true', help='Make the version <VERSION> stable. Overwrite the metadata directory (the stable directory)')
    args = parser.parse_args()

    version = args.version
    stable = args.stable
    if logger: logger.info("User input version='{}' and stable={}".format(version, stable))

    from churn.metadata.metadata import Metadata
    from churn.utils.general_functions import init_spark
    spark = init_spark("generate_metadata")
    sc = spark.sparkContext

    if not stable:
        metadata_dir = Metadata.get_metadata_path(version=version)
        if logger: logger.info("Metadata will be saved in hdfs directory '{}'".format(metadata_dir))

        df_metadata = get_metadata(spark)

        from churn.utils.general_functions import save_df
        save_df(df_metadata, metadata_dir, csv=True)
        if logger: logger.info("Saved df in '{}'".format(metadata_dir))

    else:
        stable_metadata_dir = Metadata.get_metadata_path(version="")
        df_metadata = Metadata.load_metadata_table(spark, version)
        from churn.utils.general_functions import save_df
        save_df(df_metadata, stable_metadata_dir, csv=True)
        if logger: logger.info("Saved df in '{}'".format(stable_metadata_dir))
