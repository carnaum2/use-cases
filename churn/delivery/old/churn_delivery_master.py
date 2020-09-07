# coding: utf-8

import sys
import datetime as dt
import os

logger = None
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc
from pyspark.sql.utils import AnalysisException


def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

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
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging",
                                "churn_delivery_log_time_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    import logging
    logging.getLogger('matplotlib').setLevel(logging.WARNING)

    return logger, root_dir



def get_training_cycles(spark, closing_day, segmentfilter, horizon):
    # Check if there are some previous tr_cycle_
    from churn.datapreparation.general.model_outputs_manager import get_training_dates
    existing_models = get_training_dates(spark, closing_day, segmentfilter, horizon)

    if not existing_models:
        if logger: logger.info(
            "tr_cycle_ini and tr_cycle_end were not specified by user and there are not computed scores. They'll by computed automatically using horizon={}....".format(
                horizon))
        from pykhaos.utils.date_functions import move_date_n_cycles
        tr_cycle_ini = move_date_n_cycles(closing_day, n=-horizon)
        tr_cycle_end = tr_cycle_ini
        if logger: logger.info(
            "Computed automatically tr_cycle_ini={} tr_cycle_end={} using horizon={}....".format(tr_cycle_ini, tr_cycle_end, horizon))
    else:
        training_closing_date = existing_models[0]["training_closing_date"]
        tr_cycle_ini = training_closing_date[:8]
        tr_cycle_end = training_closing_date[10:]

        if logger: logger.info(
            "tr_cycle_ini and tr_cycle_end were not specified by user but we found some previous scores with tr_ini={} tr_end={}".format(
                tr_cycle_ini, tr_cycle_end))

    return tr_cycle_ini, tr_cycle_end


def run_checker(checkers_list):
    for checker_obj in checkers_list:
        if checker_obj:
            checker_obj.check_func()


def print_checkers(checkers_list):
    for checker_obj in checkers_list:
        if checker_obj:
            checker_obj.print_msg()


def check_all_ok(checkers_list):
    return all([checker_obj.status for checker_obj in checkers_list if checker_obj])


if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"

    logger, root_dir = set_paths_and_logger()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-s', '--starting_day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='starting day for car')
    parser.add_argument('--tr_ini', metavar='<YYYYMMDD>', type=str, required=False,
                        help='starting day for training cycle. If not specified, the script compute it automatically')
    parser.add_argument('--tr_end', metavar='<YYYYMMDD>', type=str, required=False,
                        help='ending day for training cycle. If not specified, the script compute it automatically')
    parser.add_argument('--check', action='store_true', help='Run the checks. Not compatible with delivery argument')
    parser.add_argument('--delivery', action='store_true', help='Run the delivery. Not compatible with check argument')
    parser.add_argument('-v', '--version', metavar='<VERSION>', type=str, required=False,
                        help='Version for metadata table. Default: no version')
    parser.add_argument('--horizon', metavar='<integer>', type=int, required=False, default=8,
                        help='prediction horizon')
    parser.add_argument('--deliv_file_format', metavar='<extended,classic,reasons, all>', type=str, required=False, default="all",
                        help="Delivery file format. ")
    parser.add_argument('--scores_segments', metavar='<onlymob,mobileandfbb,others,all>', type=str, required=False, default="onlymob,mobileandfbb",
                        help="Delivery file format. ")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    starting_day = args.starting_day
    tr_cycle_ini = args.tr_ini
    tr_cycle_end = args.tr_end
    check = args.check
    delivery = args.delivery
    metadata_version = args.version
    horizon = args.horizon
    deliv_file_format = args.deliv_file_format
    scores_segments = args.scores_segments



    if deliv_file_format == "all":
        deliv_file_format_list = ["reasons", "extended", "classic"]
    else:
        deliv_file_format_list =  deliv_file_format.split(",")

    if scores_segments == "all":
        scores_segments_list = ["onlymob", "mobileandfbb", "others"]
    else:
        scores_segments_list =  scores_segments.split(",")


    from pykhaos.utils.date_functions import move_date_n_cycles, move_date_n_days

    if delivery and check:
        if logger: logger.critical("Invalid input parameters: delivery={} check={}".format(delivery, check))
        sys.exit()

    if not closing_day:
        print("Closing day is a required argument")
        sys.exit()
    if not starting_day:
        print("starting_day will be computed automatically as: closing_day - 4 cycles")
        starting_day = move_date_n_days(move_date_n_cycles(closing_day, n=-4), n=1)
    else:
        starting_day = starting_day.split(" ")[0]

    if not metadata_version:
        metadata_version = "1.1"

    if not tr_cycle_ini and tr_cycle_end:
        if logger: logger.critical("If tr_cycle_end is specified, tr_cycle_ini must be specified. Program will finish here!")
        sys.exit()
    elif tr_cycle_ini  and not tr_cycle_end:
        if logger: logger.critical("If tr_cycle_ini is specified, tr_cycle_end must be specified. Program will finish here!")
        sys.exit()
    elif tr_cycle_ini and tr_cycle_end:
        tr_cycle_ini =tr_cycle_ini.split(" ")[0]
        tr_cycle_end = tr_cycle_end.split(" ")[0]

        # check input values
        if tr_cycle_ini < tr_cycle_end:
            if logger: logger.critical("tr_ini must be <= than tr_end. Program will finish here!")
            sys.exit()

        from pykhaos.utils.date_functions import move_date_n_cycles, count_nb_cycles
        #today_str = dt.datetime.today().strftime("%Y%m%d")
        if count_nb_cycles(tr_cycle_end, closing_day) < horizon:
            if logger: logger.critical("tr_end ({}) must be > horizon ({}). ".format(tr_cycle_end, horizon))
            if logger: logger.critical("    Hint: tr_end={} could be a valid value for a horizon={}".format(move_date_n_cycles(closing_day, n=-horizon), horizon))
            if logger: logger.critical("Program will finish here!")
            sys.exit()

    CLOSING_DAY = closing_day



    from pykhaos.utils.date_functions import move_date_n_cycles

    prev_closing_day = move_date_n_cycles(CLOSING_DAY, n=-1)

    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("- - - - - - - - - - - - - - - - - - - - - - - - ")
    if logger: logger.info("Input params:")
    if logger: logger.info("   closing_day='{}'".format(CLOSING_DAY))
    if logger: logger.info("   starting_day='{}'".format(starting_day))
    if logger: logger.info("   tr_ini_cycle={}".format(tr_cycle_ini))
    if logger: logger.info("   tr_end_cycle={}".format(tr_cycle_end))
    if logger: logger.info("   metadata_version='{}'".format(metadata_version))
    if logger: logger.info("   delivery={}".format(delivery))
    if logger: logger.info("   check={}".format(check))
    if logger: logger.info("   deliv_file_format={}".format(deliv_file_format_list))
    if logger: logger.info("   scores_segments={}".format(scores_segments_list))
    if logger: logger.info("- - - - - - - - - - - - - - - - - - - - - - - - ")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")

    from churn.delivery.time_logger import TimeLogger
    time_logger = TimeLogger(process_name="churn_delivery_{}".format(closing_day))

    from churn.utils.general_functions import init_spark

    spark = sc = scala_sc = None

    if delivery or check:

        spark = init_spark("churn_delivery")
        sc = spark.sparkContext
        from pykhaos.utils.scala_wrapper import get_scala_sc

        scala_sc = get_scala_sc(spark)

    from churn.delivery.checkers_class import check_car, check_car_preparado, check_extra_feats, check_sql, \
        check_scores_new, check_join_car, check_levers_model
    from churn.delivery.printers_class import format_table_msg, format_table_msg

    import time

    start_time_total = time.time()

    if check or delivery:

        from churn.others.run_churn_delivery_scores_incidences_levers_master import get_columns_required
        columns_required = get_columns_required(deliv_file_format_list)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # CHECK SECTION
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if logger: logger.info("Entering check section... \n")

        from churn.delivery.checkers_class import checker, check_join_car, check_extra_feats, check_scores_new, \
            check_levers_model, check_car, check_car_preparado, check_delivery_file, check_delivery_table
        from churn.delivery.printers_class import *  # print_msg, format_modules_msg, SECTION_CAR, format_table_msg
        from functools import partial

        checker_car_obj = checker(spark, partial(check_car, spark, closing_day), partial(format_modules_msg), closing_day,
                                  SECTION_CAR)
        checker_join_prev_car_obj = checker(spark, partial(check_join_car, spark, prev_closing_day),
                                            partial(format_table_msg), closing_day, SECTION_JOIN_CAR)
        checker_extra_feats_obj = checker(spark, partial(check_extra_feats, spark, closing_day),
                                          partial(format_modules_msg), closing_day, SECTION_EXTRA_FEATS)

        checker_prepared_car_obj = checker(spark, partial(check_car_preparado, spark, closing_day),
                                           partial(format_table_msg), closing_day, SECTION_CAR_PREPARADO)

        checker_scores_onlymob_obj = checker(spark, partial(check_scores_new, spark, closing_day, segment="onlymob",
                                                            tr_cycle_ini=tr_cycle_ini,
                                                            tr_cycle_end=tr_cycle_end), partial(format_table_msg),
                                             closing_day, SECTION_SCORES_NEW + " " + "onlymob")

        checker_scores_mobileandfbb_obj = checker(spark,
                                                  partial(check_scores_new, spark, closing_day, segment="mobileandfbb",
                                                          tr_cycle_ini=tr_cycle_ini,
                                                          tr_cycle_end=tr_cycle_end), partial(format_table_msg),
                                                  closing_day, SECTION_SCORES_NEW + " " + "mobileandfbb")

        checkers_list = [checker_car_obj, checker_join_prev_car_obj, checker_extra_feats_obj, checker_prepared_car_obj,
                         checker_scores_onlymob_obj, checker_scores_mobileandfbb_obj]

        if "others" in scores_segments_list:
            checker_scores_others_obj = checker(spark,
                                                      partial(check_scores_new, spark, closing_day, segment="others",
                                                              tr_cycle_ini=tr_cycle_ini,
                                                              tr_cycle_end=tr_cycle_end), partial(format_table_msg),
                                                      closing_day, SECTION_SCORES_NEW + " " + "others")
            checkers_list.append(checker_scores_others_obj)
        else:
            checker_scores_others_obj = None

        if "palanca" in columns_required:
            checker_dp_levers_obj = checker(spark, partial(check_levers_model, spark, closing_day), partial(format_table_msg),
                                            closing_day, SECTION_DP_LEVERS)

            checkers_list.append(checker_dp_levers_obj)
        else:
            checker_dp_levers_obj = None


        checker_delivery_table_obj = checker(spark, partial(check_delivery_table, spark, closing_day), partial(format_table_msg),
                                        closing_day, SECTION_DELIV_TABLE)

        checkers_list.append(checker_delivery_table_obj)


        checker_delivery_file_obj = checker(spark, partial(check_delivery_file, closing_day, tr_cycle_ini, tr_cycle_end, horizon, deliv_file_format_list),
                                            partial(format_table_msg), closing_day, SECTION_DELIV_FILE)

        checkers_list.append(checker_delivery_file_obj)

        if logger: logger.info("\n\n")
        if logger: logger.info("* " * 30)


        run_checker(checkers_list)
        print_checkers(checkers_list)


        message_ok = "\n\nEverything is ready! Enjoy :)"
        message_ko = "\n\nSome steps were missing :("

        if check_all_ok(checkers_list):
            logger.info(message_ok)
        else:
            logger.info(message_ko)

        if logger: logger.info("\n\n")
        if logger: logger.info("* " * 30)

        print(delivery)

    else:

        checker_scores_others_obj = None
        checker_extra_feats_obj = None
        checker_join_prev_car_obj = None
        checker_delivery_table_obj = None
        checker_churnreasons_table_obj = None
        checker_scores_onlymob_obj = None
        checker_scores_mobileandfbb_obj = None
        checker_dp_levers_obj = None
        checker_prepared_car_obj = None
        checker_car_obj = None
        checkers_list = []
        checker_delivery_file_obj = None
        columns_required = None
        sc = None
        scala_sc = None


    popen_list = []

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # DELIVERY SECTION
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    if delivery:

        if logger: logger.info("Entering delivery section... \n")

        # .   .   .   .   .   .
        # CAR
        # .   .   .   .   .   .
        # car_ok, car_msg, modules_dict = check_car(spark, closing_day, verbose=False)

        if not checker_car_obj.status:

            from churn.others.run_amdocs_car import amdocs_car_main_custom

            amdocs_car_main_custom(closing_day, starting_day, spark, checker_car_obj.extra_info, time_logger)

            checker_car_obj.check_func()

            if not checker_car_obj.status:
                if logger: logger.error("Car step was not finished! Stopping program")
                if logger: logger.error(checker_car_obj.print_msg())
                sys.exit()

        # .   .   .   .   .   .
        # EXTRA FEATS
        # .   .   .   .   .   .

        if not checker_extra_feats_obj.status:
            from churn.datapreparation.app.generate_table_extra_feats_new import generate_extra_feats, MODULE_JOIN, \
                join_dataframes, get_modules_list_by_version

            include_ef_modules = [mod for mod, doit in checker_extra_feats_obj.extra_info.items() if doit and mod != MODULE_JOIN]
            print("include_ef_modules {}".format(",".join(include_ef_modules)))

            generate_extra_feats(spark, closing_day, include_modules=include_ef_modules, metadata_version="1.1",
                                 logger=logger, time_logger=time_logger)
            if checker_extra_feats_obj.extra_info[MODULE_JOIN]:
                if logger: logger.info("Requested join of extra feats")
                modules_list = get_modules_list_by_version(metadata_version="1.1")
                if logger: logger.info("Join will include the following modules: {}".format(modules_list))
                join_dataframes(spark, closing_day, metadata_version="1.1", include_modules=modules_list)
            else:
                print("do not perform join")

            checker_extra_feats_obj.check_func()

            if not checker_extra_feats_obj.status:
                if logger: logger.error("Extra feats process was not finished! Stopping program")
                if logger: logger.error(checker_extra_feats_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # JOIN CAR of previous month
        # .   .   .   .   .   .
        if not checker_join_prev_car_obj.status:
            if logger: logger.error("Join car must exist at this point!! Generating it on the fly....")

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "generateCar", closing_day, start_time, -1)

            # "select * from tests_es.jvmm_amdocs_ids_" + process_date
            sql_join_car = sc._jvm.datapreparation.AmdocsCarPreprocessor.generateCar(scala_sc, closing_day)

            if time_logger: time_logger.register_time(spark, "generateCar", closing_day, start_time, time.time())

            # Check if the join of the amdocs car is ready tests_es.db/jvmm_amdocs_ids_20190321;'
            #join_car_ok, join_car_msg = check_join_car(spark, prev_closing_day, verbose=False)
            checker_join_prev_car_obj.check_func()

            if not checker_join_prev_car_obj.status:
                if logger: logger.error("join_car process was not finished! Stopping program")
                if logger: logger.error(checker_join_prev_car_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # CAR PREPARADO
        # .   .   .   .   .   .

        if not checker_prepared_car_obj.status:

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "storePreparedFeatures", closing_day, start_time, -1)

            sql_car_preparado = sc._jvm.datapreparation.AmdocsCarPreprocessor.storePreparedFeatures(scala_sc, closing_day)
            if time_logger: time_logger.register_time(spark, "storePreparedFeatures", closing_day, start_time, time.time())

            checker_prepared_car_obj.check_func(check_sql, spark, sql_car_preparado)

            if not checker_prepared_car_obj.status:
                if logger: logger.error("Car preparado process was not finished! Stopping program")
                if logger: logger.error(checker_prepared_car_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # SCORES ONLYMOB
        # .   .   .   .   .   .

        for segmentfilter in scores_segments:


            if segmentfilter == "onlymob":
                checker_scores_obj = checker_scores_onlymob_obj
            elif segmentfilter == "mobileandfbb":
                checker_scores_obj = checker_scores_mobileandfbb_obj
            elif segmentfilter == "others":
                checker_scores_obj = checker_scores_others_obj
            else:
                if logger: logger.info("Unknown segment. Impossible to compute scores {}".format(segmentfilter))
                sys.exit()

            if not checker_scores_obj.status:

                #segmentfilter = "onlymob"

                if not tr_cycle_ini and not tr_cycle_end and delivery:
                    tr_cycle_ini, tr_cycle_end = get_training_cycles(spark, closing_day, segmentfilter, horizon)

                start_time = time.time()
                extra_info = "closing_day={}|segmentfilter={}|tr_cycle_ini={}|tr_cycle_end={}".format(closing_day,
                                                                                                      segmentfilter, tr_cycle_ini, tr_cycle_end)

                if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, -1, extra_info)

                if logger: logger.info("Calling ChurnModelPredictor.getModelPredictions onlymob tr_cycle_ini={} tr_cycle_end={} closing_day={} segmentfilter={}".format(tr_cycle_ini,
                                                                                             tr_cycle_end, closing_day,
                                                                                             segmentfilter))
                sql_segment = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
                                                                                             tr_cycle_end, closing_day,
                                                                                             segmentfilter)

                if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, time.time(), extra_info)

                checker_scores_obj.check_func(check_sql, spark, sql_segment)

                if not checker_scores_obj.status:
                    if logger: logger.error("Scores {} process was not finished! Stopping program".format(segmentfilter))
                    if logger: logger.error(checker_scores_obj.status)

                    sys.exit()

        # # .   .   .   .   .   .
        # # SCORES MOBILEANDFBB
        # # .   .   .   .   .   .
        #
        # if not checker_scores_mobileandfbb_obj.status:
        #     segmentfilter = "mobileandfbb"
        #
        #     if not tr_cycle_ini and not tr_cycle_end and delivery:
        #         tr_cycle_ini, tr_cycle_end = get_training_cycles(spark, closing_day, segmentfilter, horizon)
        #
        #     extra_info = "closing_day={}|segmentfilter={}|tr_cycle_ini={}|tr_cycle_end={}".format(closing_day,
        #                                                                                           segmentfilter, tr_cycle_ini, tr_cycle_end)
        #
        #     start_time = time.time()
        #     if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, -1)
        #
        #     if logger: logger.info("Calling ChurnModelPredictor.getModelPredictions mobileandfbb tr_cycle_ini={} tr_cycle_end={} closing_day={} segmentfilter={}".format(tr_cycle_ini,
        #                                                                                  tr_cycle_end, closing_day,
        #                                                                                  segmentfilter))
        #
        #     sql_mobileandfbb = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
        #                                                                                       tr_cycle_end, closing_day,
        #                                                                                       segmentfilter)
        #
        #     if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, time.time())
        #
        #     checker_scores_mobileandfbb_obj.check_func(check_sql, spark, sql_mobileandfbb)
        #
        #     if not checker_scores_mobileandfbb_obj.status:
        #         if logger: logger.error("Scores mobileandfbb process was not finished! Stopping program")
        #         if logger: logger.error(checker_scores_mobileandfbb_obj.status)
        #         sys.exit()

        # .   .   .   .   .   .
        # DATA PREPARATION - LEVERS MODEL

        if checker_dp_levers_obj and not checker_dp_levers_obj.status:

            from churn.datapreparation.app.ccc_data_preparation_main import start_process_config
            from churn.config_manager.ccc_model_config_mgr import build_config_dict

            for segment, do_it in checker_dp_levers_obj.extra_info.items():
                if not do_it:
                    continue

                start_time = time.time()
                if time_logger: time_logger.register_time(spark, "dp_levers_{}".format(segment), closing_day, start_time, -1)

                my_config = build_config_dict(segment, agg_by='msisdn', ccc_days=-60, closing_day=closing_day, enabled=True,
                                  end_port='None', labeled=False, model_target='comercial', new_car_version=False,
                                  save_car=True,
                                  segment_filter=segment, start_port='None')

                start_process_config(spark, my_config)

                if time_logger: time_logger.register_time(spark, "dp_levers_{}".format(segment), closing_day, start_time, time.time())


            checker_dp_levers_obj.check_func()

            if not checker_dp_levers_obj.status:
                if logger: logger.error("Data preparation for levers model was not finished! Stopping program")
                if logger: logger.error(checker_dp_levers_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # DELIVERY TABLE

        if not checker_delivery_table_obj.status:

            from churn.others.run_churn_delivery_scores_incidences_levers_master import create_delivery_table, want_indicators, want_reasons

            extra_info = "deliv_file_format={}".format(",".join(checker_delivery_file_obj.extra_info))

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "create_delivery_table", closing_day, start_time, -1)


            create_delivery_table(spark, closing_day, checker_scores_onlymob_obj.extra_info, checker_scores_mobileandfbb_obj.extra_info, columns_required, logger)
            if logger: logger.info("Finished creation of the delivery table...")
            if time_logger: time_logger.register_time(spark, "create_delivery_table", closing_day, start_time, time.time())

            checker_delivery_table_obj.check_func()

        # .   .   .   .   .   .
        # FINAL STEP
        # .   .   .   .   .   .
        if check_all_ok(checkers_list[:-1]):#exclude delivery file checks

            if logger: logger.info("Ready to start the creation of the delivery file...")
            from churn.others.run_churn_delivery_scores_incidences_levers_master import get_columns_by_format, get_filename_by_format

            if not tr_cycle_ini and not tr_cycle_end and delivery:
                # FIXME can be tr_ini and tr_end different to onlymob and mobileandfbb?
                tr_cycle_ini, tr_cycle_end = get_training_cycles(spark, closing_day, "onlymob", horizon)


            from churn.models.ccc.delivery.delivery_manager import prepare_delivery, DIR_DOWNLOAD, \
                PROJECT_NAME, DIR_DELIVERY

            for format_file, do_it in checker_delivery_file_obj.extra_info.items():

                if not do_it:
                    if logger: logger.info("File for {} does not need to be generated. Skipping".format(format_file))
                    continue

                delivery_filename = get_filename_by_format(format_file)
                delivery_cols = get_columns_by_format(format_file)


                if logger: logger.info("delivery_cols: {}".format("|".join(delivery_cols)))
                delivery_filename = delivery_filename.format(closing_day, tr_cycle_ini, tr_cycle_end, horizon)

                start_delivery = time.time()
                from churn.others.run_churn_delivery_scores_incidences_levers_master import NAME_TABLE_DEANONYMIZED

                extra_info = "format_file={}".format(format_file)

                if time_logger: time_logger.register_time(spark, "create_delivery_file", closing_day, start_delivery, -1, extra_info)


                prepare_delivery(spark, PROJECT_NAME, closing_day, NAME_TABLE_DEANONYMIZED.format(closing_day), delivery_filename, DIR_DOWNLOAD,
                                 DIR_DELIVERY, columns=delivery_cols)

                if time_logger: time_logger.register_time(spark, "create_delivery_file", closing_day, start_delivery, time.time(), extra_info)

                if logger: logger.info("create_delivery_file - {} minutes".format((time.time() - start_delivery) / 60.0))
            if logger: logger.info("Finished the latest step of the delivery...")


        else:
            if logger: logger.error("something when wrong... please review...")
            run_checker(checkers_list)
            print_checkers(checkers_list)
            sys.exit()




    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("TOTAL TIME Process finished - {} minutes".format((time.time() - start_time_total) / 60.0))


    if logger: logger.info("Generating new entry in model evaluation")
    from pykhaos.utils.date_functions import move_date_n_cycles
    from churn.delivery.threads_manager import launch_process
    import os
    print("root_dir", root_dir)
    print("joineado", os.path.join(root_dir, "use-cases/churn/models/EvaluarModelosChurn/python_code/churn_Preds_quality.py"))
    popen = launch_process('spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=8 --conf spark.dynamicAllocation.maxExecutors=16 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 --conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 --driver-java-options="-Droot.logger=WARN,console"'.split(" ") +
                           [os.path.join(root_dir, "use-cases/churn/models/EvaluarModelosChurn/python_code/churn_Preds_quality.py")] +
                           [move_date_n_cycles(closing_day, n=-4), "30", "Yes"])

    returncode = popen.wait_until_end(delay=300)

    if returncode == 0:
        if logger: logger.info("Process ended successfully. Enjoy :)")
    else:
        if logger: logger.info("'{}' process ended with code {}".format(popen.running_script, returncode))


    if logger: logger.info("churn_delivery_master_threads ended")
