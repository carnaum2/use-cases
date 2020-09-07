# coding: utf-8

import sys
import datetime as dt
import os

logger = None
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when


MODULE_CCC = "ccc"
MODULE_TGS = "tgs"
MODULE_PRBMA_SRV = "pbma_srv"
MODULE_ORDERS = "order"
MODULE_DEVICES = "devices"
MODULE_DELTA = "delta"
MODULE_CAMPAIGN = "campaign"
MODULE_COMP_WEB = "comp_web"
MODULE_NETSCOUT = "netscout"
MODULE_ADDITIONAL = "additional"
MODULE_SCORES = "scores"


# name of the directory with the join of all modules
MODULE_JOIN = "extra_feats"
# tag for using in command line when the user wants to select all modules
ALL_MODULES_TAG = "all"

# HDFS_DIR_DATA/"extra_feats_mod"
EXTRA_FEATS_DIRECTORY = "extra_feats_mod"


PARTITION_DATE = "year={}/month={}/day={}"

def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re, datetime as dt

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print(pathname)
    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):

        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)

        #from churn.utils.constants import CHURN_DELIVERIES_DIR
        #root_dir = CHURN_DELIVERIES_DIR
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


    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))




    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "generate_extra_feats_new" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="", msg_format="%(asctime)s [%(levelname)-5.5s] CHURN|%(message)s")
    logger.info("Logging to file {}".format(logging_file))

    return logger


def impute_nulls(df, spark, metadata_version=""):
    import time

    start_time = time.time()
    from churn.metadata.metadata import Metadata

    metadata_path = Metadata.get_metadata_path(metadata_version)

    if logger: logger.info("impute_null function will read metadata from '{}'".format(metadata_path))
    df_metadata = spark.read.option("delimiter", "|").option("header", True).csv(metadata_path)

    from churn.metadata.metadata import Metadata
    df = Metadata.apply_metadata(df, df_metadata)

    if logger: logger.info("Elapssed time imputing nulls {} secs".format(time.time() - start_time))
    return df


def get_modules_list_by_version(metadata_version):
    module_names_list = [
        MODULE_CCC,
        MODULE_TGS,
        MODULE_PRBMA_SRV,
        MODULE_ORDERS,
        MODULE_DEVICES,
        MODULE_DELTA,
        MODULE_CAMPAIGN,
        MODULE_COMP_WEB,
        MODULE_NETSCOUT,
        MODULE_ADDITIONAL,
        # MODULE_SCORES
    ]

    if metadata_version > "1.1":
        module_names_list.append(MODULE_SCORES)

    return module_names_list


def generate_extra_feats(spark, closing_day, include_modules=None, metadata_version="", logger=None, time_logger=None):
    '''

    :param spark:
    :param closing_day:
    :param rgu: If none, no filter by rgu
    :param do_save_df:
    :param modules: If none, all modules will be generated
    :return:
    '''

    if not include_modules:
        if logger: logger.warning("Nothing to generate")
        return

    from churn.utils.constants import HDFS_DIR_DATA

    HDFS_EXTRA_FEATS_MOD_DIR = os.path.join(HDFS_DIR_DATA, EXTRA_FEATS_DIRECTORY)


    import time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    #HDFS_EXTRA_FEATS_DIR = os.path.join(HDFS_DIR_DATA, "extra_feats_mod", "extra_feats_{}".format(closing_day))
    year = closing_day[:4]
    month = closing_day[4:6]
    day = closing_day[6:]
    partition_date = PARTITION_DATE.format(int(year), int(month), int(day))


    if logger: logger.info("Extra feats dir will be {}".format(HDFS_EXTRA_FEATS_MOD_DIR))

    from pykhaos.utils.date_functions import move_date_n_days
    ccc_start_date_ = move_date_n_days(closing_day, -30, str_fmt="%Y%m%d")

    if logger: logger.info("ccc_start={} ccc_end={}".format(ccc_start_date_, closing_day))

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    if logger: logger.info("Process started...")

    # Use the previous YYYYMM
    from pykhaos.utils.date_functions import move_date_n_yearmonths
    prev_closing_yyyymm = move_date_n_yearmonths(closing_day[:6], -1)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # TGs
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_TGS in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_TGS))

        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_TGS, closing_day, start_time, -1)

        try:
            from churn.datapreparation.general.tgs_data_loader import get_tgs, EXTRA_FEATS_PREFIX as prefix_tgs
            df_tgs = get_tgs(spark, closing_day, prev_closing_yyyymm, impute_nulls=True)
            if logger: logger.info("Ended tgs")

            extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_TGS, partition_date)
            from churn.utils.general_functions import save_df
            save_df(df_tgs, extra_feats_path, csv=False)
            if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_TGS))
            if time_logger: time_logger.register_time(spark, MODULE_TGS, closing_day, start_time, time.time())


        except Exception as e:
            if logger: logger.error("Problem generating TGs module. {}".format(str(e)))
            if logger: logger.error("Skipping module...")

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_TGS))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CCC metrics (ccc, abonos, tripletas)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_CCC in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_CCC))

        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_CCC, closing_day, start_time, -1)


        from churn.datapreparation.general.ccc_data_loader import get_ccc_metrics
        df_ccc_metrics = get_ccc_metrics(spark, closing_day, ccc_start_date_, impute_nulls=True)
        #df_ccc_metrics = df_ccc_metrics.cache()
        if logger: logger.info("Ended ccc metrics - {}".format(df_ccc_metrics.count()))
        #df_ccc_metrics = impute_nulls(df_ccc_metrics, spark, metadata_version)
        #if logger: logger.info("Ended ccc null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_CCC, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_ccc_metrics, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_CCC))
        if time_logger: time_logger.register_time(spark, MODULE_TGS, closing_day, start_time, time.time())

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_CCC))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # services problems metrics
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_PRBMA_SRV in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_PRBMA_SRV))

        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_PRBMA_SRV, closing_day, start_time, -1)


        from churn.datapreparation.general.problemas_servicio_data_loader import get_service_problems, \
            EXTRA_FEATS_PREFIX as prefix_srv_pbms

        df_service_prbms = get_service_problems(spark, closing_day, impute_nulls=True)
        if logger: logger.info("Ended service problems")
        # df_service_prbms = impute_nulls(df_service_prbms, spark, metadata_version)
        # if logger: logger.info("Ended service problems null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_PRBMA_SRV, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_service_prbms, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_PRBMA_SRV))
        if time_logger: time_logger.register_time(spark, MODULE_TGS, closing_day, start_time, time.time())


    else:
        if logger: logger.info("Skipped module {}".format(MODULE_PRBMA_SRV))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Device metrics
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_DEVICES in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_DEVICES))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_DEVICES, closing_day, start_time, -1)

        from churn.utils.general_functions import amdocs_table_reader
        from churn.datapreparation.engine.ccc_model_auxiliar import prepare_devices
        df_devices = amdocs_table_reader(spark, table_name="device_catalogue",
                                         closing_day=closing_day, new=False, verbose=False)
        df_devices = prepare_devices(df_devices, closing_day, impute_nulls=True)

        if logger: logger.info("Ended prepare devices")
        # df_devices = impute_nulls(df_devices, spark, metadata_version)
        # if logger: logger.info("Ended devices null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_DEVICES, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_devices, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_DEVICES))
        if time_logger: time_logger.register_time(spark, MODULE_DEVICES, closing_day, start_time, time.time())


    else:
        if logger: logger.info("Skipped module {}".format(MODULE_DEVICES))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Scores metrics
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_SCORES in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_SCORES))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_SCORES, closing_day, start_time, -1)

        from churn.datapreparation.general.scores_data_loader import get_scores_metrics
        df_scores = get_scores_metrics(spark, closing_day, impute_nulls=True)

        if logger: logger.info("Ended prepare scores")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_SCORES, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_scores, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_SCORES))
        if time_logger: time_logger.register_time(spark, MODULE_SCORES, closing_day, start_time, time.time())


    else:
        if logger: logger.info("Skipped module {}".format(MODULE_SCORES))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SCALA
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from pykhaos.utils.scala_wrapper import convert_df_to_pyspark, get_scala_sc

    sc = spark.sparkContext

    scala_sc = get_scala_sc(spark)

    # 1. delta: computeDeltaFeatsDf - 153
    # 2. campanas: computeCampFeatsDf - 40
    # 3. navegacion por competidores: computeCompetitorsWebFeatsDf - 273
    # 4. netscout: computeNetscoutappFeatsDf - 606
    # 5. orders: computeOrderFeatsDf 6
    # 6. additional: computeAdditionalFeats 40

    # . . . . . . . . . . . . . . . . . . .
    # computeDeltaFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    if MODULE_DELTA in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_DELTA))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_DELTA, closing_day, start_time, -1)

        start_time_1 = time.time()
        df_scala_delta_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeDeltaFeatsDf(scala_sc, closing_day)
        df_py_delta_feats = convert_df_to_pyspark(spark, df_scala_delta_feats)
        if logger: logger.info("Ended scala1 - {} minutes".format((time.time() - start_time_1) / 60.0))
        df_py_delta_feats = impute_nulls(df_py_delta_feats, spark, metadata_version)
        if logger: logger.info("Ended delta null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_DELTA, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_delta_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_DELTA))
        if time_logger: time_logger.register_time(spark, MODULE_DELTA, closing_day, start_time, time.time())


    else:
        if logger: logger.info("Skipped module {}".format(MODULE_DELTA))


    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    if MODULE_CAMPAIGN in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_CAMPAIGN))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_CAMPAIGN, closing_day, start_time, -1)

        start_time_2 = time.time()
        df_scala_camp_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeCampFeatsDf(scala_sc, closing_day)
        df_py_camp_feats = convert_df_to_pyspark(spark, df_scala_camp_feats)
        if logger: logger.info("Ended scala2 - {} minutes".format((time.time() - start_time_2) / 60.0))
        df_py_camp_feats = impute_nulls(df_py_camp_feats, spark, metadata_version)
        if logger: logger.info("Ended camp null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_CAMPAIGN, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_camp_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_CAMPAIGN))
        if time_logger: time_logger.register_time(spark, MODULE_CAMPAIGN, closing_day, start_time, time.time())

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_CAMPAIGN))


    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    if MODULE_COMP_WEB in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_COMP_WEB))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_COMP_WEB, closing_day, start_time, -1)

        start_time_3 = time.time()
        df_scala_comp_web_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeCompetitorsWebFeatsDf(scala_sc,
                                                                                                             closing_day)
        df_py_comp_web_feats = convert_df_to_pyspark(spark, df_scala_comp_web_feats)
        if logger: logger.info("Ended scala3 - {} minutes".format((time.time() - start_time_3) / 60.0))
        df_py_comp_web_feats = impute_nulls(df_py_comp_web_feats, spark, metadata_version)
        if logger: logger.info("Ended comp web null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_COMP_WEB, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_comp_web_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_COMP_WEB))
        if time_logger: time_logger.register_time(spark, MODULE_COMP_WEB, closing_day, start_time, time.time())

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_COMP_WEB))


    # . . . . . . . . . . . . . . . . . . .
    # computeNetscoutappFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    if MODULE_NETSCOUT in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_NETSCOUT))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_NETSCOUT, closing_day, start_time, -1)

        start_time_4 = time.time()
        df_scala_netscout_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeNetscoutappFeatsDf(scala_sc,
                                                                                                          closing_day)
        df_py_netscout_feats = convert_df_to_pyspark(spark, df_scala_netscout_feats)
        if logger: logger.info("Ended scala4 - {} minutes".format((time.time() - start_time_4) / 60.0))
        df_py_netscout_feats = impute_nulls(df_py_netscout_feats, spark, metadata_version)
        if logger: logger.info("Ended netscout null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_NETSCOUT, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_netscout_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_NETSCOUT))
        if time_logger: time_logger.register_time(spark, MODULE_NETSCOUT, closing_day, start_time, time.time())


    else:
        if logger: logger.info("Skipped module {}".format(MODULE_NETSCOUT))


    # . . . . . . . . . . . . . . . . . . .
    # computeOrderFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    if MODULE_ORDERS in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_ORDERS))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_ORDERS, closing_day, start_time, -1)

        start_time_5 = time.time()
        df_scala_order_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeOrderFeatsDf(scala_sc, closing_day)
        df_py_order_feats = convert_df_to_pyspark(spark, df_scala_order_feats)
        if logger: logger.info("Ended scala5 - {} minutes".format((time.time() - start_time_5) / 60.0))
        df_py_order_feats = impute_nulls(df_py_order_feats, spark, metadata_version)
        if logger: logger.info("Ended order null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_ORDERS, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_order_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_ORDERS))
        if time_logger: time_logger.register_time(spark, MODULE_ORDERS, closing_day, start_time, time.time())

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_ORDERS))


    # . . . . . . . . . . . . . . . . . . .
    # computeAdditionalFeats
    # . . . . . . . . . . . . . . . . . . .

    if MODULE_ADDITIONAL in include_modules:
        if logger: logger.info("Started generation of module {}".format(MODULE_ADDITIONAL))
        start_time = time.time()
        if time_logger: time_logger.register_time(spark, MODULE_ADDITIONAL, closing_day, start_time, -1)

        start_time_6 = time.time()
        df_scala_additional_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeAdditionalFeats(scala_sc, closing_day)
        df_py_additional_feats = convert_df_to_pyspark(spark, df_scala_additional_feats)
        #if logger: logger.info("CSANC109 {} rows in additional".format(df_py_additional_feats.count()))
        if logger: logger.info("Ended scala6 - {} minutes".format((time.time() - start_time_6) / 60.0))
        df_py_additional_feats = impute_nulls(df_py_additional_feats, spark, metadata_version)
        if logger: logger.info("Ended additional null imputation")

        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_ADDITIONAL, partition_date)
        from churn.utils.general_functions import save_df
        save_df(df_py_additional_feats, extra_feats_path, csv=False)
        if logger: logger.info("EXTRA_FEATS MODULE = {} saved successfully".format(MODULE_ADDITIONAL))
        if time_logger: time_logger.register_time(spark, MODULE_ADDITIONAL, closing_day, start_time, time.time())

    else:
        if logger: logger.info("Skipped module {}".format(MODULE_ADDITIONAL))




def get_extra_feats_module_path(closing_day, module_name):
    year = closing_day[:4]
    month = closing_day[4:6]
    day = closing_day[6:]
    partition_date = PARTITION_DATE.format(int(year), int(month), int(day))
    from churn.utils.constants import HDFS_DIR_DATA
    HDFS_EXTRA_FEATS_MOD_DIR = os.path.join(HDFS_DIR_DATA, EXTRA_FEATS_DIRECTORY)
    return os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, module_name, partition_date)




def join_dataframes(spark, closing_day, include_modules, join_name=MODULE_JOIN, metadata_version="", time_logger=None):

    import time

    year = closing_day[:4]
    month = closing_day[4:6]
    day = closing_day[6:]
    partition_date = PARTITION_DATE.format(int(year), int(month), int(day))

    from churn.utils.constants import HDFS_DIR_DATA
    HDFS_EXTRA_FEATS_MOD_DIR = os.path.join(HDFS_DIR_DATA, EXTRA_FEATS_DIRECTORY)

    extra_feats_join_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, join_name, partition_date)
    if logger:logger.info("extra feats join will be saved in: '{}'".format(extra_feats_join_path))

    start_time_join_all = time.time()
    if time_logger: time_logger.register_time(spark, MODULE_JOIN, closing_day, start_time_join_all, -1)

    from churn.datapreparation.general.data_loader import get_active_services

    df_join = (get_active_services(spark, closing_day=closing_day, new=False,
                                              service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic",
                                                            "campo1"],
                                              customer_cols=["num_cliente", "nif_cliente"])
                          .withColumnRenamed("num_cliente_service", "num_cliente")
                          )


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # JOIN DATAFRAMES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if MODULE_CCC in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_CCC, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_ccc_metrics = spark.read.load(extra_feats_path)
        df_ccc_metrics = df_ccc_metrics.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_ccc_metrics, on=["msisdn"], how="left")
        df_join = df_join.cache()
        if logger: logger.info("df_join1.count() = {}".format(df_join.count()))
        if logger: logger.info("Ended join ccc - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CCC))


    if MODULE_TGS in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_TGS, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_tgs = spark.read.load(extra_feats_path)
        df_tgs = df_tgs.cache()

        from churn.datapreparation.general.tgs_data_loader import EXTRA_FEATS_PREFIX as prefix_tgs
        cols_tgs = [col_ for col_ in df_tgs.columns if col_ == "msisdn" or col_.startswith(prefix_tgs)]

        start_time_join = time.time()
        df_join = df_join.join(df_tgs.select(cols_tgs), on=["msisdn"], how="left")
        if logger: logger.info("Ended join tgs - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CCC))

    if MODULE_PRBMA_SRV in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_PRBMA_SRV, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_service_prbms = spark.read.load(extra_feats_path)
        df_service_prbms = df_service_prbms.cache()

        from churn.datapreparation.general.problemas_servicio_data_loader import EXTRA_FEATS_PREFIX as prefix_srv_pbms
        cols_pbms_srv = [col_ for col_ in df_service_prbms.columns if
                         col_ == "msisdn" or col_.startswith(prefix_srv_pbms)]


        start_time_join = time.time()
        df_join = df_join.join(df_service_prbms.select(cols_pbms_srv), on=["msisdn"], how="left")
        df_join = df_join.withColumn("others_ind_pbma_srv", when(col("ccc_ind_tipif_uci") +
                                                                 col("pbms_srv_ind_averias") +
                                                                 col('pbms_srv_ind_soporte') +
                                                                 col('pbms_srv_ind_reclamaciones') > 0, 1).otherwise(0))
        if logger: logger.info("Ended join pbma srv - {} minutes".format((time.time() - start_time_join) / 60.0))



    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CCC))

    if MODULE_DEVICES in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_DEVICES, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_devices = spark.read.load(extra_feats_path)
        df_devices = df_devices.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_devices, on=["msisdn"], how="left")
        if logger: logger.info("Ended join devices - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CCC))

    if MODULE_SCORES in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_SCORES, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_scores = spark.read.load(extra_feats_path)
        df_scores = df_scores.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_scores, on=["msisdn"], how="left")
        logger.info("Ended join scores - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CCC))

    if MODULE_CAMPAIGN in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_CAMPAIGN, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_camp_feats = spark.read.load(extra_feats_path)
        df_py_camp_feats = df_py_camp_feats.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_py_camp_feats, on=["num_cliente"], how="left")
        if logger: logger.info(
            "Ended join scala camp feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_CAMPAIGN))

    if MODULE_COMP_WEB in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_COMP_WEB, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_comp_web_feats = spark.read.load(extra_feats_path)
        df_py_comp_web_feats = df_py_comp_web_feats.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_py_comp_web_feats, on=["msisdn"], how="left")
        if logger: logger.info(
            "Ended join scala comp web feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_COMP_WEB))

    if MODULE_DELTA in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_DELTA, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_delta_feats = spark.read.load(extra_feats_path)
        df_py_delta_feats = df_py_delta_feats.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_py_delta_feats, on=["msisdn"], how="left")
        if logger: logger.info(
            "Ended join scala delta feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_DELTA))


    if MODULE_NETSCOUT in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_NETSCOUT, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_netscout_feats = spark.read.load(extra_feats_path)
        df_py_netscout_feats = df_py_netscout_feats.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_py_netscout_feats, on=["msisdn"], how="left")
        if logger:  logger.info(
            "Ended join scala netscout feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_NETSCOUT))

    if MODULE_ORDERS in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_ORDERS, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_order_feats = spark.read.load(extra_feats_path)
        df_py_order_feats = df_py_order_feats.cache()

        start_time_join = time.time()
        # order info is by num_cliente
        df_join = df_join.join(df_py_order_feats, on=["msisdn"], how="left")
        if logger: logger.info(
            "Ended join scala order feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_ORDERS))


    if MODULE_ADDITIONAL in include_modules:
        extra_feats_path = os.path.join(HDFS_EXTRA_FEATS_MOD_DIR, MODULE_ADDITIONAL, partition_date)
        if logger: logger.info("Reading {}".format(extra_feats_path))
        df_py_additional_feats = spark.read.load(extra_feats_path)
        df_py_additional_feats = df_py_additional_feats.cache()

        start_time_join = time.time()
        df_join = df_join.join(df_py_additional_feats, on=["num_cliente"], how="left")
        if logger: logger.info("Ended join scala additional feats - {} minutes".format((time.time() - start_time_join) / 60.0))
    else:
        if logger: logger.warning("Skipping {} from join".format(MODULE_ADDITIONAL))


    df_join = df_join.cache()
    if logger: logger.info("After the latest join df_join.count() = {}".format(df_join.count()))
    if logger: logger.info("Ended join()+cache()+count() {} minutes".format((time.time() - start_time_join_all) / 60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # IMPUTE_NULLS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    start_time_join = time.time()
    if logger: logger.info("About to call impute_nulls")
    df_join = impute_nulls(df_join, spark, metadata_version)
    if logger: logger.info("Ended call to impute_nulls")

    df_join = df_join.cache()

    if logger: logger.info("df_join.count() = {}".format(df_join.count()))

    if logger: logger.info("Elapsed time joining+imputing nulls {} minutes".format( (time.time() - start_time_join)/60.0))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # WRITE DATAFRAME
    start_time_join = time.time()
    from churn.utils.general_functions import save_df
    if logger: logger.info("Join will be saved in {}".format(extra_feats_join_path))
    save_df(df_join, extra_feats_join_path, csv=False)
    if logger: logger.info("HDFS EXTRA FEATS DIR '{}' saved successfully".format(extra_feats_join_path))
    if logger: logger.info("Elapsed time saving the join {} minutes".format( (time.time() - start_time_join)/60.0))
    if time_logger: time_logger.register_time(spark, MODULE_JOIN, closing_day, start_time_join_all, time.time())


    return df_join



if __name__ == "__main__":

    logger = set_paths_and_logger()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
                description="Generate table of extra feats  spark2-submit --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium \
                              --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G \
                              --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G \
                              --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 --jars /var/SP/data/bdpmdses/jmarcoso/apps/candidates/AutomChurn-assembly-0.5.jar \
                              churn/datapreparation/app/generate_table_extra_feats.py -c YYYYMMDD",
                epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation) or a csv list of closing_day')
    parser.add_argument('-s', '--starting_cycle', metavar='<YYYYMMDD>', type=str, required=False,
                        help='starting cycle YYYYMMDD. If present, the range of cycles [starting_day, closing_day] is generated')
    parser.add_argument('-e', '--exclude_modules', metavar='<modules_to_exclude>', type=str, required=False,
                        help='modules to exclude from the generation.') #FIXME Modules names are: {}'.format(",".join(MODULE_NAMES)))
    parser.add_argument('-i', '--include_modules', metavar='<modules_to_generate>', type=str, required=False,
                        help='modules to include from the generation. Use "--include_modules all" for all modules') #. Modules names are: {}'.format(",".join(MODULE_NAMES)))
    parser.add_argument('-j', '--join', action='store_true', help='Run the join for the closing day')
    parser.add_argument('-v', '--version', metavar='<VERSION>', type=str, required=False,
                        help='Version for metadata table. Default: no version')
    parser.add_argument('-n', '--join_name', metavar='<join_name>', type=str, required=False,
                        help='name for join module. Useful when creating a join for testing Default {}'.format(MODULE_JOIN))

    args = parser.parse_args()

    print(args)

    closing_day = args.closing_day
    starting_day = args.starting_cycle
    rgu = None
    exclude_modules = args.exclude_modules
    include_modules = args.include_modules
    do_join =  args.join
    metadata_version = args.version
    join_name = args.join_name

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ADJUSTING MODULE NAMES DEPENDING ON METADATA
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # if metadata_version > "1.1":
    #     global MODULE_NAMES
    #     MODULE_NAMES.append(MODULE_SCORES)
    MODULE_NAMES = get_modules_list_by_version(metadata_version)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if not closing_day:
        print("Closing day is a required argument")
        sys.exit()

    if starting_day:
        if "," in closing_day:
            print("closing_day cannot be a csv list when starting_day is specified")
            sys.exit()

        STARTING_DAY = starting_day
        CLOSING_DAY = closing_day

        if STARTING_DAY > CLOSING_DAY:
            logger.critical(
                "starting_day ({}) must not be later than closing_day ({})!!. Program finished".format(STARTING_DAY,
                                                                                                       CLOSING_DAY))
            sys.exit()

        from pykhaos.utils.date_functions import move_date_n_cycles

        CYCLES_TO_GENERATE = []
        if STARTING_DAY:
            c_day = STARTING_DAY
            while c_day <= CLOSING_DAY:
                CYCLES_TO_GENERATE.append(c_day)
                c_day = move_date_n_cycles(c_day, 1)


    else:
        #STARTING_DAY = closing_day
        if "," in closing_day:
            CYCLES_TO_GENERATE = closing_day.split(",")
        else:
            CYCLES_TO_GENERATE = [closing_day]

    if include_modules == ALL_MODULES_TAG:
        if logger: logger.info("User selected run all modules")
        include_modules = MODULE_NAMES
    elif not include_modules:
        if logger: logger.info("User selected run all modules")
        include_modules = MODULE_NAMES
    else:
        include_modules = include_modules.split(",")
        for mod_ in include_modules:
            if not mod_ in MODULE_NAMES:
                logger.error("Module name {} does not exist. Valid modules names are: {}".format(mod_, ",".join(MODULE_NAMES)))
                sys.exit()


    if exclude_modules == ALL_MODULES_TAG:
        if logger: logger.info("User selected exclude all modules")
        exclude_modules = MODULE_NAMES
    elif not exclude_modules:
        exclude_modules = []
    else:
        exclude_modules = exclude_modules.split(",")

    for mod_ in exclude_modules:
        if not mod_ in MODULE_NAMES:
            logger.error("Module name {} does not exist. Valid modules names are: {}".format(mod_, ",".join(MODULE_NAMES)))
            sys.exit()
        else:
            include_modules = list(set(include_modules) - set(exclude_modules))

    if not join_name:
        join_name = MODULE_JOIN


    from churn.utils.constants import HDFS_DIR_DATA

    HDFS_EXTRA_FEATS_MOD_DIR = os.path.join(HDFS_DIR_DATA, EXTRA_FEATS_DIRECTORY)

    if logger: logger.info("Input params:")
    if logger: logger.info("starting_day={}".format(starting_day))
    if logger: logger.info("closing_day={}".format(closing_day))
    if logger: logger.info("cycles={}".format(",".join(CYCLES_TO_GENERATE)))
    if logger: logger.info("include_modules={}".format(include_modules))
    if logger: logger.info("exclude_modules={}".format(exclude_modules))
    if logger: logger.info("do_join={}".format(do_join))
    if logger: logger.info("Saving dict={}".format(HDFS_EXTRA_FEATS_MOD_DIR))
    if logger: logger.info("metadata version={}".format(metadata_version))
    if logger: logger.info("join_name={}".format(join_name))
    if do_join:
        if logger: logger.info("join will include these modules: {}".format(",".join(MODULE_NAMES)))


    import time
    start_time = time.time()

    for c_day in CYCLES_TO_GENERATE:

        from churn.utils.general_functions import init_spark

        spark = init_spark("generate_table_extra_feats_new_{}".format(c_day), log_level="OFF")
        sc = spark.sparkContext

        start_time_c_day = time.time()

        if include_modules:
            if logger: logger.info("Computing extra feats for closing day {}".format(c_day))
            df_extra_feats = generate_extra_feats(spark, c_day, include_modules=include_modules, metadata_version=metadata_version)
            if logger: logger.info("c_day {} process finished - {} minutes".format(c_day, (time.time()-start_time)/60.0))
        else:
            if logger: logger.info("Nothing to generate. include modules is empty")

        if do_join:
            join_dataframes(spark, c_day, include_modules=MODULE_NAMES, join_name=join_name, metadata_version=metadata_version)
            if logger: logger.info("Ended join dataframes function")
        else:
            if logger: logger.info("Skipping join")

        spark.stop()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("TOTAL TIME Process finished - {} minutes".format( (time.time()-start_time)/60.0))

    if logger: logger.info("Process ended successfully. Enjoy :)")

