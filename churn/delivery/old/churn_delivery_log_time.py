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
        root_dir = re.match("^(.*)use-cases(.*)",
                 "/var/SP/data/bdpmdses/deliveries_churn/version1/use-cases/churn/delivery").group(1)

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

    return logger



def set_training_cycles(spark,closing_day, segmentfilter):
    # Check if there are some previous tr_cycle_
    from churn.datapreparation.general.model_outputs_manager import get_training_dates
    existing_models = get_training_dates(spark, closing_day, segmentfilter)

    if not existing_models:
        if logger: logger.info(
            "tr_cycle_ini and tr_cycle_end were not specified by user and there are not computed scores. They'll by computed automatically using horizon={}....".format(
                horizon))
        from pykhaos.utils.date_functions import move_date_n_cycles
        import datetime

        today_str = datetime.datetime.today().strftime("%Y%m%d")
        tr_cycle_ini = move_date_n_cycles(today_str, n=-(
                horizon + 1))  # first: go to the closest cycle (-1); then, move backwards 8 cycles = -9
        tr_cycle_end = tr_cycle_ini
    else:
        training_closing_date = existing_models[0]["training_closing_date"]
        tr_cycle_ini = training_closing_date[:8]
        tr_cycle_end = training_closing_date[10:]

        if logger: logger.info(
            "tr_cycle_ini and tr_cycle_end were not specified by user but we found some previous scores with tr_ini={} tr_end={".format(
                tr_cycle_ini, tr_cycle_end))
        # FIXME falta comprobar que cumplen con el horizon

    return tr_cycle_ini, tr_cycle_end





if __name__ == "__main__":

    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"

    logger = set_paths_and_logger()

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

    from pykhaos.utils.date_functions import move_date_n_cycles, move_date_n_days

    if delivery == check:
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
        today_str = dt.datetime.today().strftime("%Y%m%d")
        if count_nb_cycles(tr_cycle_end, closing_day) < horizon:
            if logger: logger.critical("tr_end ({}) must be > horizon ({}). ".format(tr_cycle_end, horizon))
            if logger: logger.critical("    Hint: tr_end={} could be a valid value for a horizon={}".format(move_date_n_cycles(today_str, n=-(horizon+1)), horizon))
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
    if logger: logger.info("- - - - - - - - - - - - - - - - - - - - - - - - ")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")

    from churn.delivery.time_logger import TimeLogger
    time_logger = TimeLogger(process_name="churn_delivery_{}".format(closing_day))

    from churn.utils.general_functions import init_spark

    spark = init_spark("churn_delivery")
    sc = spark.sparkContext
    from pykhaos.utils.scala_wrapper import get_scala_sc

    scala_sc = get_scala_sc(spark)

    from churn.delivery.checkers_class import check_car, check_car_preparado, check_extra_feats, check_sql, \
        check_scores_new, check_join_car, check_levers_model
    from churn.delivery.printers_class import format_table_msg, format_table_msg

    import time

    start_time_total = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CHECK SECTION
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("Entering check section... \n")

    from churn.delivery.checkers_class import checker, check_join_car, check_extra_feats, check_scores_new, \
        check_levers_model, check_car, check_car_preparado, check_delivery_file
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

    checker_dp_levers_obj = checker(spark, partial(check_levers_model, spark, closing_day), partial(format_table_msg),
                                    closing_day, SECTION_DP_LEVERS)


    checker_delivery_file_obj = checker(spark, partial(check_delivery_file, closing_day, tr_cycle_ini, tr_cycle_end, horizon),
                                        partial(format_table_msg), closing_day, SECTION_DELIV_FILE)

    checkers_list = [checker_car_obj, checker_join_prev_car_obj, checker_extra_feats_obj, checker_prepared_car_obj,
                     checker_scores_onlymob_obj, checker_scores_mobileandfbb_obj, checker_dp_levers_obj, checker_delivery_file_obj]

    if logger: logger.info("\n\n")
    if logger: logger.info("* " * 30)


    def run_checker(checkers_list):
        for checker_obj in checkers_list:
            checker_obj.check_func()


    def print_checkers(checkers_list):
        for checker_obj in checkers_list:
            checker_obj.print_msg()

    def check_all_ok(checkers_list):
        return all([checker_obj.status for checker_obj in checkers_list])


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

        if not checker_scores_onlymob_obj.status:
            segmentfilter = "onlymob"

            if not tr_cycle_ini and not tr_cycle_end and delivery:
                tr_cycle_ini, tr_cycle_end = set_training_cycles(spark, closing_day, segmentfilter)

            start_time = time.time()
            extra_info = "closing_day={}|segmentfilter={}|tr_cycle_ini={}|tr_cycle_end={}".format(closing_day,
                                                                                                  segmentfilter, tr_cycle_ini, tr_cycle_end)

            if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, -1, extra_info)

            sql_onlymob = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
                                                                                         tr_cycle_end, closing_day,
                                                                                         segmentfilter)

            if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, time.time(), extra_info)

            checker_scores_onlymob_obj.check_func(check_sql, spark, sql_onlymob)

            if not checker_scores_onlymob_obj.status:
                if logger: logger.error("Scores onlymob process was not finished! Stopping program")
                if logger: logger.error(checker_scores_onlymob_obj.status)
 
                sys.exit()

        # .   .   .   .   .   .
        # SCORES MOBILEANDFBB
        # .   .   .   .   .   .

        if not checker_scores_mobileandfbb_obj.status:
            segmentfilter = "mobileandfbb"

            if not tr_cycle_ini and not tr_cycle_end and delivery:
                tr_cycle_ini, tr_cycle_end = set_training_cycles(spark, closing_day, segmentfilter)

            extra_info = "closing_day={}|segmentfilter={}|tr_cycle_ini={}|tr_cycle_end={}".format(closing_day,
                                                                                                  segmentfilter, tr_cycle_ini, tr_cycle_end)

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, -1)

            sql_mobileandfbb = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
                                                                                              tr_cycle_end, closing_day,
                                                                                              segmentfilter)

            if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, time.time())

            checker_scores_mobileandfbb_obj.check_func(check_sql, spark, sql_mobileandfbb)

            if not checker_scores_mobileandfbb_obj.status:
                if logger: logger.error("Scores mobileandfbb process was not finished! Stopping program")
                if logger: logger.error(checker_scores_mobileandfbb_obj.status)
                sys.exit()

        # .   .   .   .   .   .
        # DATA PREPARATION - LEVERS MODEL

        if not checker_dp_levers_obj.status:

            from churn.datapreparation.app.ccc_data_preparation_main import start_process_config
            from churn.config_manager.ccc_model_config_mgr import build_config_dict

            for segment, do_it in checker_dp_levers_obj.extra_info.items():
                if not do_it:
                    continue

                start_time = time.time()
                if time_logger: time_logger.register_time(spark, "df_levers_{}".format(segment), closing_day, start_time, -1)

                my_config = build_config_dict(segment, agg_by='msisdn', ccc_days=-60, closing_day=closing_day, enabled=True,
                                  end_port='None', labeled=False, model_target='comercial', new_car_version=False,
                                  save_car=True,
                                  segment_filter=segment, start_port='None')

                start_process_config(spark, my_config)

                if time_logger: time_logger.register_time(spark, "df_levers_{}".format(segment), closing_day, start_time, time.time())


            checker_dp_levers_obj.check_func()

            if not checker_dp_levers_obj.status:
                if logger: logger.error("Data preparation for levers model was not finished! Stopping program")
                if logger: logger.error(checker_dp_levers_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # FINAL STEP
        # .   .   .   .   .   .
        if check_all_ok(checkers_list[:-1]):#exclude delivery file check
            if logger: logger.info("Ready to start the latest step of the delivery...")
            from churn.others.run_churn_delivery_scores_incidences_levers_master import run_delivery

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "run_delivery", closing_day, start_time, -1)

            from churn.utils.constants import DELIVERY_FILENAME
            delivery_filename = DELIVERY_FILENAME.format(closing_day, tr_cycle_ini, tr_cycle_end, horizon)
            run_delivery(spark, closing_day, checker_scores_onlymob_obj.extra_info, checker_scores_mobileandfbb_obj.extra_info, delivery_filename, logger)
            if logger: logger.info("Finished the latest step of the delivery...")
            if time_logger: time_logger.register_time(spark, "run_delivery", closing_day, start_time, time.time())

        else:
            if logger: logger.error("something when wrong... please review...")
            sys.exit()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("TOTAL TIME Process finished - {} minutes".format((time.time() - start_time_total) / 60.0))

    if logger: logger.info("Process ended successfully. Enjoy :)")
