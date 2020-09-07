# coding: utf-8

import sys
import datetime as dt
import os

logger = None
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc
from pyspark.sql.utils import AnalysisException

import time
import os


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



def get_training_cycles(closing_day, horizon):

    if logger: logger.info(
        "tr_cycle_ini and tr_cycle_end were not specified by user and there are not computed scores. They'll by computed automatically using horizon={}....".format(
            horizon))
    from pykhaos.utils.date_functions import move_date_n_cycles
    tr_cycle_ini = move_date_n_cycles(closing_day, n=-horizon)
    tr_cycle_end = tr_cycle_ini
    if logger: logger.info(
        "Computed automatically tr_cycle_ini={} tr_cycle_end={} using horizon={}....".format(tr_cycle_ini, tr_cycle_end, horizon))

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


    from churn.delivery.time_logger import TimeLogger
    from pykhaos.utils.date_functions import move_date_n_cycles
    from pykhaos.utils.threads_manager import launch_process, print_threads_status, any_running
    from churn.delivery.checkers_class import check_car, check_car_preparado, check_extra_feats, check_sql, \
        check_scores_new, check_join_car, check_levers_model
    from churn.delivery.printers_class import format_table_msg, format_table_msg
    from churn.delivery.delivery_constants import MODEL_EVALUATION_NB_DAYS, DELIVERY_TABLE_PREPARED
    from churn.delivery.model_outputs_formatter import build_pred_name, insert_to_model_outputs

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
    parser.add_argument('--deliv_file_format', metavar='<extended,classic,reasons, all>', type=str, required=False, default="reasons",
                        help="Delivery file format. ")
    parser.add_argument('--scores_segments', metavar='<onlymob,mobileandfbb,others,all>', type=str, required=False, default="all",
                        help="Delivery file format. ")
    parser.add_argument('--ref_date', metavar='YYYYMMDD', type=str, required=False, default=None,
                        help="When closing day is not a discriminatory parameters, we use this date to look for files created after or on this day")
    parser.add_argument('--historic', action='store_true', help='insert to model outputs in historic mode')

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
    historic = args.historic
    horizon = args.horizon
    deliv_file_format = args.deliv_file_format
    scores_segments = args.scores_segments
    ref_date = args.ref_date
    deliv_file_format_list = deliv_file_format.split(",")

    if deliv_file_format == "all":
        deliv_file_format_list = ["reasons", "extended", "classic"]
    else:
        deliv_file_format_list =  deliv_file_format.split(",")

    ALL_SEGMENTS = ["onlymob", "mobileandfbb", "others"]

    if scores_segments == "all":
        scores_segments_list = ALL_SEGMENTS
    else:
        scores_segments_list =  scores_segments.split(",")

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
        if count_nb_cycles(tr_cycle_end, today_str) <= horizon:
            if logger: logger.critical("tr_end ({}) + horizon ({}) > (today + ~4). ".format(tr_cycle_end, horizon))
            if logger: logger.critical("    Hint: tr_end={} could be a valid value for a horizon={}".format(move_date_n_cycles(closing_day, n=-horizon), horizon))
            if logger: logger.critical("Program will finish here!")
            sys.exit()
    elif not tr_cycle_ini and not tr_cycle_end and delivery:
        tr_cycle_ini, tr_cycle_end = get_training_cycles(closing_day, horizon)

    training_closing_date=None
    if tr_cycle_ini and tr_cycle_end:
        training_closing_date = tr_cycle_ini + "to" + tr_cycle_end

    CLOSING_DAY = closing_day

    from pykhaos.utils.date_functions import move_date_n_cycles

    prev_closing_day = move_date_n_cycles(CLOSING_DAY, n=-1)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # GET SPARK CONTEXT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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
    if logger: logger.info("   historic={}".format(historic))
    if logger: logger.info("   deliv_file_format={}".format(deliv_file_format_list))
    if logger: logger.info("   scores_segments={}".format(scores_segments_list))
    if logger: logger.info("- - - - - - - - - - - - - - - - - - - - - - - - ")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")
    if logger: logger.info("")



    time_logger = TimeLogger(process_name="churn_delivery_{}".format(closing_day))

    from churn.utils.general_functions import init_spark

    spark = sc = scala_sc = None

    if delivery or check:
        spark = init_spark("churn_delivery_{}".format(closing_day))
        sc = spark.sparkContext
        from pykhaos.utils.scala_wrapper import get_scala_sc

        scala_sc = get_scala_sc(spark)

    start_time_total = time.time()

    if check or delivery:

        from churn.others.run_churn_delivery_scores_incidences_levers_master import get_columns_required
        columns_required = get_columns_required(deliv_file_format_list)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # CHECK SECTION
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if logger: logger.info("Entering check section... \n")

        #FIXME refactoring!
        from churn.delivery.checkers_class import checker, check_join_car, check_extra_feats, check_scores_new, \
            check_levers_model, check_car, check_car_preparado, check_churn_reasons_table, check_delivery_file, check_delivery_table,\
        check_model_evaluation, check_model_output_entry
        from churn.others.run_amdocs_car import amdocs_car_main_custom
        from churn.delivery.printers_class import *  # print_msg, format_modules_msg, SECTION_CAR, format_table_msg
        from functools import partial
        from churn.datapreparation.app.generate_table_extra_feats_new import generate_extra_feats
        from churn.delivery.delivery_constants import MODEL_NAME_DELIV_CHURN

        checker_car_obj = checker(spark, partial(check_car, spark, closing_day), partial(format_modules_msg), closing_day,
                                  SECTION_CAR, partial(amdocs_car_main_custom, closing_day, starting_day, spark, time_logger=time_logger))
        checker_join_prev_car_obj = checker(spark, partial(check_join_car, spark, prev_closing_day),
                                            partial(format_table_msg), closing_day, SECTION_JOIN_CAR,
                                            #partial(sc._jvm.datapreparation.AmdocsCarPreprocessor.generateCar, scala_sc,prev_closing_day)
                                            )
        checker_extra_feats_obj = checker(spark, partial(check_extra_feats, spark, closing_day),
                                          partial(format_modules_msg), closing_day, SECTION_EXTRA_FEATS,
                                          partial(generate_extra_feats, spark, closing_day, metadata_version="1.1",
                                                  logger=logger, time_logger=time_logger))

        checker_prepared_car_obj = checker(spark, partial(check_car_preparado, spark, closing_day),
                                           partial(format_table_msg), closing_day, SECTION_CAR_PREPARADO,
                                           #partial(sc._jvm.datapreparation.AmdocsCarPreprocessor.storePreparedFeatures, scala_sc, closing_day)
                                           )

        checker_scores_onlymob_obj = checker(spark, partial(check_scores_new, spark, closing_day, segment="onlymob",
                                                            tr_cycle_ini=tr_cycle_ini,
                                                            tr_cycle_end=tr_cycle_end), partial(format_table_msg),
                                             closing_day, SECTION_SCORES_NEW + " " + "onlymob")

        checker_scores_mobileandfbb_obj = checker(spark,
                                                  partial(check_scores_new, spark, closing_day, segment="mobileandfbb",
                                                          tr_cycle_ini=tr_cycle_ini,
                                                          tr_cycle_end=tr_cycle_end), partial(format_table_msg),
                                                  closing_day, SECTION_SCORES_NEW + " " + "mobileandfbb",
                                                  #partial(sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions, scala_sc, tr_cycle_ini,
                                                  #                                           tr_cycle_end, closing_day,
                                                  #                                           "mobileandfbb")
                                                  )

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


        checker_churnreasons_table_obj = checker(spark, partial(check_churn_reasons_table, spark, closing_day, scores_segments_list, training_closing_date),
                                             partial(format_table_msg),
                                             closing_day, SECTION_DP_CHURN_REASONS)

        checkers_list.append(checker_churnreasons_table_obj)

        checker_delivery_table_obj = checker(spark, partial(check_delivery_table, spark, closing_day, training_closing_date), partial(format_table_msg),
                                        closing_day, SECTION_DELIV_TABLE)

        checkers_list.append(checker_delivery_table_obj)

        checker_mod_out_delivery_entry_obj = checker(spark, partial(check_model_output_entry, spark, MODEL_NAME_DELIV_CHURN, closing_day, training_closing_date),
                                            partial(format_table_msg), closing_day, SECTION_CHURN_DELIV_ENTRY)

        checkers_list.append(checker_mod_out_delivery_entry_obj)

        checker_delivery_file_obj = checker(spark, partial(check_delivery_file, closing_day, tr_cycle_ini, tr_cycle_end, horizon, deliv_file_format_list),
                                            partial(format_table_msg), closing_day, SECTION_DELIV_FILE)

        checkers_list.append(checker_delivery_file_obj)


        checker_model_evaluation_obj = checker(spark, partial(check_model_evaluation, move_date_n_cycles(closing_day, n=-4), MODEL_EVALUATION_NB_DAYS, ref_date),
                                            partial(format_table_msg), closing_day, SECTION_MODEL_EVALUATION)

        checkers_list.append(checker_model_evaluation_obj)

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
        checker_model_evaluation_obj = None
        sc = None
        scala_sc = None
        checker_mod_out_delivery_entry_obj = None


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

            checker_car_obj.run_func(modules_dict=checker_car_obj.extra_info)
            checker_car_obj.check_func()

            if not checker_car_obj.status:
                if logger: logger.error("Car step was not finished! Stopping program")
                if logger: logger.error(checker_car_obj.print_msg())
                sys.exit()

        # .   .   .   .   .   .
        # EXTRA FEATS
        # .   .   .   .   .   .

        # Not really necessary... except when extra feat process is run separately to reduce time....
        # TODO remove following line when this script is automated
        checker_extra_feats_obj.check_func()

        if not checker_extra_feats_obj.status:
            from churn.datapreparation.app.generate_table_extra_feats_new import generate_extra_feats, MODULE_JOIN, \
                join_dataframes, get_modules_list_by_version

            include_ef_modules = [mod for mod, doit in checker_extra_feats_obj.extra_info.items() if doit and mod != MODULE_JOIN]
            if logger: logger.info("include_ef_modules {}".format(",".join(include_ef_modules)))

            checker_extra_feats_obj.run_func(include_modules=include_ef_modules)

            if checker_extra_feats_obj.extra_info[MODULE_JOIN]:
                if logger: logger.info("Requested join of extra feats")
                modules_list = get_modules_list_by_version(metadata_version="1.1")
                if logger: logger.info("Join will include the following modules: {}".format(modules_list))
                join_dataframes(spark, closing_day, metadata_version="1.1", include_modules=modules_list)
            else:
                if logger: logger.info("do not perform join")

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
            sql_join_car = sc._jvm.datapreparation.AmdocsCarPreprocessor.generateCar(scala_sc, prev_closing_day) #checker_join_prev_car_obj.run_func() #

            if time_logger: time_logger.register_time(spark, "generateCar", closing_day, start_time, time.time())

            # Check if the join of the amdocs car is ready tests_es.db/jvmm_amdocs_ids_20190321;'
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
            sql_car_preparado = sc._jvm.datapreparation.AmdocsCarPreprocessor.storePreparedFeatures(scala_sc, closing_day) #checker_prepared_car_obj.run_func() #
            if time_logger: time_logger.register_time(spark, "storePreparedFeatures", closing_day, start_time, time.time())
            checker_prepared_car_obj.check_func(check_sql, spark, sql_car_preparado)

            if not checker_prepared_car_obj.status:
                if logger: logger.error("Car preparado process was not finished! Stopping program")
                if logger: logger.error(checker_prepared_car_obj.msg)
                sys.exit()

        # .   .   .   .   .   .
        # SCORES SECTION + CHURN_REASON
        # .   .   .   .   .   .

        for segmentfilter in scores_segments_list:

            if segmentfilter == "onlymob":
                checker_scores_obj = checker_scores_onlymob_obj
            elif segmentfilter == "mobileandfbb":
                checker_scores_obj = checker_scores_mobileandfbb_obj
            elif segmentfilter == "others":
                checker_scores_obj = checker_scores_others_obj
            else:
                if logger: logger.info("Unknown segment. Impossible to compute scores {}".format(segmentfilter))
                sys.exit()

            if not checker_scores_obj.status: # incomplete step

                start_time = time.time()
                extra_info = "closing_day={}|segmentfilter={}|tr_cycle_ini={}|tr_cycle_end={}".format(closing_day,
                                                                                                      segmentfilter, tr_cycle_ini, tr_cycle_end)

                if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, -1, extra_info)

                if logger: logger.info("Calling ChurnModelPredictor.getModelPredictions tr_cycle_ini={} tr_cycle_end={} closing_day={} segmentfilter={}".format(tr_cycle_ini,
                                                                                             tr_cycle_end, closing_day,
                                                                                             segmentfilter))
                sql_segment = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc,
                                                                                             tr_cycle_ini,
                                                                                             tr_cycle_end, closing_day,
                                                                                             segmentfilter)

                if time_logger: time_logger.register_time(spark, "getModelPredictions_{}".format(segmentfilter), closing_day, start_time, time.time(), extra_info)

                checker_scores_obj.check_func(check_sql, spark, sql_segment)

                if not checker_scores_obj.status:
                    if logger: logger.error("Scores {} process was not finished! Delivery will not be fully prepared but this script will try to advance as much as possible".format(segmentfilter))

            # launch in paralell the churn reasons corresponding to this segment
            # .   .   .   .   .   .
            # CHURN REASONS

            if not checker_churnreasons_table_obj.status:
                if logger: logger.info("About to launch churn reasons for segment {}".format(segmentfilter))
                if checker_churnreasons_table_obj.extra_info[segmentfilter]:
                    popen_list.append(launch_process(CMD_CHURN_REASONS.format(segment=segmentfilter,
                                                                              dir=root_dir if root_dir.endswith("/") else root_dir+"/",
                                                                              date=closing_day,
                                                                              pred_name=build_pred_name(spark,
                                                                                                        closing_day,
                                                                                                        segmentfilter,
                                                                                                        horizon))))

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
                if logger: logger.error("Data preparation for levers model was not finished! Delivery will not be fully prepared but this script will try to advance as much as possible")

        # Scores were finished and churn reasons process may be running. This block check the state using the popen object
        while any_running(popen_list): # and not checker_churnreasons_table_obj.status:
            if logger: logger.info("Not all process open from Popen are finished yet. Current state:")
            print_threads_status(popen_list)
            time.sleep(5 * 60) # wait 5 minutes
            checker_churnreasons_table_obj.check_func()
            checker_churnreasons_table_obj.print_msg()
            if logger: logger.info("\n-------------------\n")

        checker_churnreasons_table_obj.check_func()
        #checker_churnreasons_table_obj.print_msg()

        if not checker_churnreasons_table_obj.status:
            if logger: logger.critical("Churn reasons processes did not finish properly. Program will exit now.")
            checker_churnreasons_table_obj.print_msg()
            sys.exit()

        # Usage of a generic object above avoids to update the actual ones.
        #FIXME think how to avoid re run checkfun
        checker_scores_onlymob_obj.check_func()
        checker_scores_mobileandfbb_obj.check_func()
        if checker_scores_others_obj != None:
            checker_scores_others_obj.check_func()

        # We are ready to generate delivery table
        scores_sql_dict = {}
        scores_sql_dict["onlymob"] = checker_scores_onlymob_obj.extra_info
        scores_sql_dict["mobileandfbb"] = checker_scores_mobileandfbb_obj.extra_info
        if checker_scores_others_obj != None:
            scores_sql_dict["others"] = checker_scores_others_obj.extra_info

        # .   .   .   .   .   .
        # DELIVERY TABLE

        if not checker_delivery_table_obj.status or not checker_mod_out_delivery_entry_obj.status:

            extra_info = "deliv_file_format={}".format(",".join(checker_delivery_file_obj.extra_info))

            start_time = time.time()
            if time_logger: time_logger.register_time(spark, "build_delivery_df", closing_day, start_time, -1)
            from churn.delivery.delivery_table import build_delivery_df

            df_complete = build_delivery_df(spark, closing_day, scores_sql_dict, columns_required, training_closing_date, logger)
            if logger: logger.info("Finished creation of the delivery df...")

            if time_logger: time_logger.register_time(spark, "build_delivery_df", closing_day, start_time, time.time())
            #if logger: logger.info("merging_process - {} minutes".format((time.time() - start_merge) / 60.0))

            #TODO to be removed when return feed is working
            from churn.delivery.delivery_table import insert_to_delivery_table
            if not checker_delivery_table_obj.status:
                if logger: logger.info("About to call insert_to_delivery_table")
                insert_to_delivery_table(df_complete,
                                         name_table_prepared=DELIVERY_TABLE_PREPARED.format(closing_day, training_closing_date),
                                         columns_to_keep=columns_required)
                if logger: logger.info("Finished insertion into delivery table...")
                checker_delivery_table_obj.check_func()
                if logger: logger.info("hola2")

            if not checker_mod_out_delivery_entry_obj.status:
                if logger: logger.info("About to call insert_to_model_outputs")
                insert_to_model_outputs(spark, closing_day, df_complete, training_closing_date, historic)
                if logger: logger.info("Finished insertion into model_outputs...")

                checker_mod_out_delivery_entry_obj.check_func()
                if logger: logger.info("hola4")

        # .   .   .   .   .   .
        # FINAL STEP
        # .   .   .   .   .   .
        if check_all_ok(checkers_list[:-2]):#exclude delivery file and model evaluation
            #TODO to be removed when return feed is working

            if logger: logger.info("Ready to start the creation of the delivery file...")
            from churn.delivery.delivery_table import get_columns_by_format, get_filename_by_format
            from churn.models.ccc.delivery.delivery_manager import prepare_delivery, DIR_DOWNLOAD, \
                PROJECT_NAME, DIR_DELIVERY

            if not tr_cycle_ini and not tr_cycle_end and delivery:
                tr_cycle_ini, tr_cycle_end = get_training_cycles(closing_day, horizon)

            for format_file, do_it in checker_delivery_file_obj.extra_info.items():

                if not do_it:
                    if logger: logger.info("File for {} does not need to be generated. Skipping".format(format_file))
                    continue

                delivery_filename = get_filename_by_format(format_file)
                delivery_cols = get_columns_by_format(format_file)

                if logger: logger.info("delivery_cols: {}".format("|".join(delivery_cols)))
                delivery_filename = delivery_filename.format(closing_day, tr_cycle_ini, tr_cycle_end, horizon)

                start_delivery = time.time()
                #from churn.others.run_churn_delivery_scores_incidences_levers_master import NAME_TABLE_DEANONYMIZED

                extra_info = "format_file={}".format(format_file)

                if time_logger: time_logger.register_time(spark, "create_delivery_file", closing_day, start_delivery, -1, extra_info)

                prepare_delivery(spark, PROJECT_NAME, closing_day, DELIVERY_TABLE_PREPARED.format(closing_day, training_closing_date), delivery_filename, DIR_DOWNLOAD,
                                 DIR_DELIVERY, columns=delivery_cols)

                if time_logger: time_logger.register_time(spark, "create_delivery_file", closing_day, start_delivery, time.time(), extra_info)

                if logger: logger.info("create_delivery_file - {} minutes".format((time.time() - start_delivery) / 60.0))

            if logger: logger.info("Finished the latest step of the delivery...")

        else:
            if logger: logger.info("")
            if logger: logger.info("")
            if logger: logger.error("---------------------------------------------------")
            if logger: logger.error("something went wrong... Not all steps were completed.please review...")
            print_checkers(checkers_list)
            if logger: logger.info("")
            if logger: logger.info("")
            if logger: logger.info("")

            sys.exit()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("TOTAL TIME Process finished - {} minutes".format((time.time() - start_time_total) / 60.0))

    if delivery and not checker_model_evaluation_obj.status:

        if logger: logger.info("Generating new entry in model evaluation")

        cmd = CMD_MODEL_EVALUATION.format(os.path.join(root_dir, CMD_MODEL_EVALUATION_PATH_FILE), move_date_n_cycles(closing_day, n=-4), "30", "Yes").split(" ")
        if logger: logger.info("Command for model evaluation '{}'".format(cmd))
        popen = launch_process(cmd)
        returncode = popen.wait_until_end(delay=300)

        if logger: logger.info("'{}' process ended with code {}".format(popen.running_script, returncode))

        if returncode == 0:
            if logger: logger.info("Model Evaluation Process ended successfully. Enjoy :)")

    if logger: logger.info("churn_delivery_master_threads ended!")
