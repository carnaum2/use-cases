import sys
import datetime as dt
import os

logger = None
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when


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


def impute_nulls(df, spark):
    import time

    start_time = time.time()

    from churn.metadata.metadata import HDFS_METADATA_DIR
    df_metadata = spark.read.option("delimiter", "|").option("header", True).csv(HDFS_METADATA_DIR)

    from churn.metadata.metadata import Metadata
    df = Metadata.apply_metadata(df, df_metadata)

    if logger: logger.info("Elapssed time imputing nulls {} secs".format(time.time() - start_time))
    return df


def generate_extra_feats(spark, closing_day, rgu=None, do_save_df=True):


    import time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn.utils.constants import HDFS_DIR_DATA
    HDFS_EXTRA_FEATS_DIR = os.path.join(HDFS_DIR_DATA, "extra_feats", "extra_feats_{}".format(closing_day))

    if logger: logger.info("Extra feats dir will be {}".format(HDFS_EXTRA_FEATS_DIR))

    from pykhaos.utils.date_functions import move_date_n_days
    ccc_start_date_ = move_date_n_days(closing_day, -30, str_fmt="%Y%m%d")

    if logger: logger.info("ccc_start={} ccc_end={}".format(ccc_start_date_, closing_day))

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    if logger: logger.info("Process started...")



    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ACTIVE SERVICES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn.datapreparation.general.data_loader import get_active_services

    df_services = (get_active_services(spark, closing_day=closing_day, new=False,
                                              service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic",
                                                            "campo1"],
                                              customer_cols=["num_cliente", "nif_cliente"])
                          .withColumnRenamed("num_cliente_service", "num_cliente")
                          )

    if rgu:
        if logger: logger.info("filtering by rgu = {}".format(",".join(rgu)))
        df_services = (df_services.withColumn("rgu", col("rgu").isin(rgu)))

    df_services = df_services.cache()
    if logger: logger.info("df_services_mobile={}".format(df_services.count()))

    # Use the previous YYYYMM
    from pykhaos.utils.date_functions import move_date_n_yearmonths
    prev_closing_yyyymm = move_date_n_yearmonths(closing_day[:6], -1)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # TGs
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.general.tgs_data_loader import get_tgs, EXTRA_FEATS_PREFIX as prefix_tgs
    df_tgs = get_tgs(spark, closing_day, prev_closing_yyyymm, impute_nulls=False)
    if logger: logger.info("Ended tgs")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CCC metrics (ccc, abonos, tripletas)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.general.ccc_data_loader import get_ccc_metrics

    df_ccc_metrics = get_ccc_metrics(spark, closing_day, ccc_start_date_, impute_nulls=False)
    df_ccc_metrics = df_ccc_metrics.cache()
    if logger: logger.info("Ended ccc metrics - {}".format(df_ccc_metrics.count()))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CCC metrics
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.general.problemas_servicio_data_loader import get_service_problems, \
        EXTRA_FEATS_PREFIX as prefix_srv_pbms

    df_service_prbms = get_service_problems(spark, closing_day, prev_closing_yyyymm, impute_nulls=False)
    if logger: logger.info("Ended service problems")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Device metrics
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn.utils.general_functions import amdocs_table_reader
    from churn.datapreparation.engine.ccc_model_auxiliar import prepare_devices
    df_devices = amdocs_table_reader(spark, table_name="device_catalogue",
                                     closing_day=closing_day, new=False, verbose=False)
    df_devices = prepare_devices(df_devices, closing_day)

    if logger: logger.info("Ended prepare devices")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SCALA
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from pykhaos.utils.scala_wrapper import convert_df_to_pyspark, get_scala_sc, get_scala_sql_context

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
    start_time_1 = time.time()
    df_scala_delta_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeDeltaFeatsDf(scala_sc, closing_day)
    df_py_delta_feats = convert_df_to_pyspark(spark, df_scala_delta_feats)
    if logger: logger.info("Ended scala1 - {} minutes".format((time.time() - start_time_1) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    start_time_2 = time.time()
    df_scala_camp_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeCampFeatsDf(scala_sc, closing_day)
    df_py_camp_feats = convert_df_to_pyspark(spark, df_scala_camp_feats)
    if logger: logger.info("Ended scala2 - {} minutes".format((time.time() - start_time_2) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    start_time_3 = time.time()
    df_scala_comp_web_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeCompetitorsWebFeatsDf(scala_sc,
                                                                                                         closing_day)
    df_py_comp_web_feats = convert_df_to_pyspark(spark, df_scala_comp_web_feats)
    if logger: logger.info("Ended scala3 - {} minutes".format((time.time() - start_time_3) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    start_time_4 = time.time()
    df_scala_netscout_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeNetscoutappFeatsDf(scala_sc,
                                                                                                      closing_day)
    df_py_netscout_feats = convert_df_to_pyspark(spark, df_scala_netscout_feats)
    if logger: logger.info("Ended scala4 - {} minutes".format((time.time() - start_time_4) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # computeCampFeatsDf
    # . . . . . . . . . . . . . . . . . . .
    start_time_5 = time.time()
    df_scala_order_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeOrderFeatsDf(scala_sc, closing_day)
    df_py_order_feats = convert_df_to_pyspark(spark, df_scala_order_feats)
    if logger: logger.info("Ended scala5 - {} minutes".format((time.time() - start_time_5) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # computeAdditionalFeats
    # . . . . . . . . . . . . . . . . . . .
    # start_time_6 = time.time()
    # df_scala_additional_feats = sc._jvm.featureengineering.ExtraFeatGenerator.computeAdditionalFeats(scala_sc, closing_day)
    # df_py_additional_feats = convert_df_to_pyspark(spark, df_scala_additional_feats)
    # logger.info("Ended scala6 - {} minutes".format( (time.time()-start_time_6)/60.0))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # JOIN DATAFRAMES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    start_time_join = time.time()
    cols_pbms_srv = [col_ for col_ in df_service_prbms.columns if col_ == "msisdn" or col_.startswith(prefix_srv_pbms)]
    cols_tgs = [col_ for col_ in df_tgs.columns if col_ == "msisdn" or col_.startswith(prefix_tgs)]

    # . . . . . . . . . . . . . . . . . . .
    # ccc_metrics
    # . . . . . . . . . . . . . . . . . . .
    df_join1 = df_services.join(df_ccc_metrics, on=["msisdn"], how="left")
    df_join1 = df_join1.cache()
    if logger: logger.info("df_join1.count() = {}".format(df_join1.count()))
    if logger: logger.info("Ended join ccc - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # tgs
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join2 = df_join1.join(df_tgs.select(cols_tgs), on=["msisdn"], how="left")
    if logger: logger.info("Ended join tgs - {} minutes".format((time.time() - start_time_join) / 60.0))

    start_time_join = time.time()
    df_join = df_join2.join(df_service_prbms.select(cols_pbms_srv), on=["msisdn"], how="left")
    #FIXME this column name should be lower case
    df_join = df_join.withColumn("IND_PBMA_SRV", when(col("ccc_ind_tipif_uci") +
                                                      col("pbms_srv_ind_averias") +
                                                      col('pbms_srv_ind_soporte') +
                                                      col('pbms_srv_ind_reclamaciones') > 0, 1).otherwise(0))
    if logger: logger.info("Ended join pbma srv - {} minutes".format((time.time() - start_time_join) / 60.0))
    start_time_join = time.time()
    df_join = df_join.join(df_devices, on=["msisdn"], how="left")
    if logger: logger.info("Ended join devices - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # additional_feats
    # . . . . . . . . . . . . . . . . . . .
    # start_time_join = time.time()
    # df_join = df_join.join(df_py_additional_feats, on=["msisdn"], how="left")
    # logger.info("Ended join scala additional feats - {} minutes".format( (time.time()-start_time_join)/60.0))

    # . . . . . . . . . . . . . . . . . . .
    # camp_feats
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join = df_join.join(df_py_camp_feats, on=["msisdn"], how="left")
    if logger: logger.info("Ended join scala camp feats - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # comp_web_feats
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join = df_join.join(df_py_comp_web_feats, on=["msisdn"], how="left")
    if logger: logger.info("Ended join scala comp web feats - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # delta_feats
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join = df_join.join(df_py_delta_feats, on=["msisdn"], how="left")
    if logger: logger.info("Ended join scala delta feats - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # netscout_feats
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join = df_join.join(df_py_netscout_feats, on=["msisdn"], how="left")
    if logger:  logger.info("Ended join scala netscout feats - {} minutes".format((time.time() - start_time_join) / 60.0))

    # . . . . . . . . . . . . . . . . . . .
    # order_feats
    # . . . . . . . . . . . . . . . . . . .
    start_time_join = time.time()
    df_join = df_join.join(df_py_order_feats, on=["msisdn"], how="left")
    if logger: logger.info("Ended join scala order feats - {} minutes".format((time.time() - start_time_join) / 60.0))

    df_join = df_join.cache()
    if logger: logger.info("After the latest join df_join.count() = {}".format(df_join.count()))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # IMPUTE_NULLS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if logger: logger.info("About to call impute_nulls")
    df_join = impute_nulls(df_join, spark)
    if logger: logger.info("Ended call to impute_nulls")

    df_join = df_join.cache()

    if logger: logger.info("df_join.count() = {}", df_join.count())

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # WRITE DATAFRAME
    if do_save_df:
        from churn.utils.general_functions import save_df
        save_df(df_join, HDFS_EXTRA_FEATS_DIR, csv=False)
        if logger: logger.info("HDFS EXTRA FEATS DIR '{}' saved successfully".format(HDFS_EXTRA_FEATS_DIR))

    return df_join



if __name__ == "__main__":

    logger = set_paths_and_logger()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description='Generate table of extra feats',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-s', '--starting_day', metavar='<STARTING_DAY>', type=str, required=False,
                        help='starting cycle YYYYMMDD. If present, the range of cycles [starting_day, closing_day] is generated')
    parser.add_argument('-r', '--rgu', metavar='<rgu', type=str, required=False,
                        help='rgu list (csv) with the rgu required. If None, all rgu will be generated')
    args = parser.parse_args()

    print(args)

    closing_day = args.closing_day
    starting_day = args.starting_day
    rgu = args.rgu

    if logger: logger.info("Input params: starting_day={} closing_day={} rgu={}".format(starting_day, closing_day, rgu))

    import time
    start_time = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if not closing_day:
        print("Closing day is a required argument")
        sys.exit()
    CLOSING_DAY = closing_day
    if not starting_day:
        STARTING_DAY = closing_day
    else:
        STARTING_DAY = starting_day

    if STARTING_DAY > CLOSING_DAY:
        logger.critical("starting_day ({}) must not be later than closing_day ({})!!. Program finished".format(STARTING_DAY, CLOSING_DAY))

    if rgu:
        rgu = rgu.split(",")

    from churn.utils.general_functions import init_spark
    spark = init_spark("generate_table_extra_feats")
    sc = spark.sparkContext


    from pykhaos.utils.date_functions import move_date_n_cycles

    c_day = STARTING_DAY

    while c_day <= closing_day:

        start_time_c_day = time.time()

        if logger: logger.info("Computing extra feats for closing day {}".format(c_day))

        df_extra_feats = generate_extra_feats(spark, c_day, rgu)
        if logger: logger.info("c_day {} process finished - {} minutes".format(c_day, (time.time()-start_time)/60.0))

        c_day = move_date_n_cycles(c_day, 1)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))

    if logger: logger.info("Process ended successfully. Enjoy :)")

