# coding: utf-8

import sys
import datetime as dt
import os

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


    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "churn_delivery_log_time_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    import logging
    logging.getLogger('matplotlib').setLevel(logging.WARNING)


    return logger



if __name__ == "__main__":

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
    parser.add_argument('-s', '--starting_day', metavar='<YYYYMMDD>', type=str, required=False, #only required in delivery mode
                        help='starting day for car')
    parser.add_argument('--tr_ini', metavar='<YYYYMMDD>', type=str, required=False,
                        help='starting day for training cycle. If not specified, the script compute it automatically for delivery mode')
    parser.add_argument('--tr_end', metavar='<YYYYMMDD>', type=str, required=False,
                        help='ending day for training cycle. If not specified, the script compute it automatically for delivery mode')
    parser.add_argument('--check', action='store_true', help='Run the checks. Not compatible with delivery argument')
    parser.add_argument('--delivery', action='store_true', help='Run the delivery. Not compatible with check argument')
    parser.add_argument('-v', '--version', metavar='<VERSION>', type=str, required=False,
                        help='Version for metadata table. Default: no version')

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
    elif not tr_cycle_ini and not tr_cycle_end and delivery: # only computes in delivery mode
        if logger: logger.info("tr_cycle_ini and tr_cycle_end were not specified by user. They'll by computed automatically....")
        import datetime
        today_str = datetime.datetime.today().strftime("%Y%m%d")
        tr_cycle_ini = move_date_n_cycles(today_str, n=-9) # first: go to the closest cycle (-1); then, move backwards 8 cycles = -9
        tr_cycle_end = tr_cycle_ini

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

    #from churn.delivery.time_logger import TimeLogger
    #time_logger = TimeLogger()
    time_logger = None

    from churn.utils.general_functions import init_spark
    spark = init_spark("churn_delivery")
    sc = spark.sparkContext
    from pykhaos.utils.scala_wrapper import get_scala_sc
    scala_sc = get_scala_sc(spark)

    from churn.delivery.old.checkers import check_car, check_car_preparado, check_extra_feats, check_sql, \
        check_scores_new, check_join_car, check_levers_model
    from churn.delivery.old.printers import print_msg_car, print_msg_car_preparado, print_msg_extra_feats_msg, \
        print_msg_scores, print_msg_join_car, print_msg_dp_levers

    import time
    start_time_total = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CHECK SECTION
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("Entering check section... \n")

    # Check if the amdocs car of current closing day is generated
    car_ok, car_msg, modules_dict, car_perc_completed = check_car(spark, closing_day, verbose=False)
    # check if previous prepared car is ready - delta feats need it
    car_preparado_ok, car_preparado_msg = check_car_preparado(spark, closing_day, verbose=False)

    # Check if the join of the amdocs car is ready tests_es.db/jvmm_amdocs_ids_20190321;'
    join_car_ok, join_car_msg = check_join_car(spark, prev_closing_day, verbose=False)

    #
    extra_feats_ok, extra_feats_msg, extrafeats_modules_dict, ex_feats_perc_completed = check_extra_feats(spark, closing_day, verbose=False)
    #     scores_onlymob_ok, scores_onlymob_msg = check_scores(spark, closing_day, segment="onlymob", verbose=False)
    #     scores_mobileandfbb_ok, scores_mobileandfbb_msg = check_scores(spark, closing_day, segment="mobileandfbb", verbose=False)
    scores_new_onlymob_ok, scores_new_onlymob_msg, sql_scores_onlymob = check_scores_new(spark, closing_day,
                                                                                         segment="onlymob",
                                                                                         tr_cycle_ini=tr_cycle_ini,
                                                                                         tr_cycle_end=tr_cycle_end,
                                                                                         verbose=False)
    scores_new_mobileandfbb_ok, scores_new_mobileandfbb_msg, sql_scores_mobileandfbb = check_scores_new(spark,
                                                                                                        closing_day,
                                                                                                        segment="mobileandfbb",
                                                                                                        tr_cycle_ini=tr_cycle_ini,
                                                                                                        tr_cycle_end=tr_cycle_end,
                                                                                                        verbose=False)

    dp_levers_ok, dp_levers_msg, dp_levers_dict, dp_levers_perc = check_levers_model(spark, closing_day)

    if logger: logger.info("\n\n")
    if logger: logger.info("* " * 30)

    print_msg_car(car_ok, car_msg, car_perc_completed)

    print_msg_join_car(join_car_ok, join_car_msg) # required by delta feats
    print_msg_car_preparado(car_preparado_ok, car_preparado_msg)
    print_msg_extra_feats_msg(extra_feats_ok, extra_feats_msg, ex_feats_perc_completed)
    print_msg_scores(scores_new_onlymob_ok, scores_new_onlymob_msg, "onlymob")
    print_msg_scores(scores_new_mobileandfbb_ok, scores_new_mobileandfbb_msg, "mobileandfbb")
    print_msg_dp_levers(dp_levers_ok, dp_levers_msg, dp_levers_perc)

    message_ok = "\n\nAll the datapreparation is done! Check if the delivery file is already generated. Enjoy :)"
    message_ko = "\n\nSome steps were missing :("

    if all([car_ok, extra_feats_ok, car_preparado_ok, scores_new_onlymob_ok, scores_new_mobileandfbb_ok]):
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
        #car_ok, car_msg, modules_dict = check_car(spark, closing_day, verbose=False)

        if not car_ok:
            #from amdocs_informational_dataset.partial_launcher_churn import amdocs_car_main_custom
            from churn.others.run_amdocs_car import  amdocs_car_main_custom

            amdocs_car_main_custom(closing_day, starting_day, spark, modules_dict, time_logger)
            car_ok, car_msg, modules_dict, car_perc_completed = check_car(spark, closing_day, verbose=False)

            if not car_ok:
                if logger: logger.error("Car step was not finished! Stopping program")
                if logger: logger.error(car_msg)
                sys.exit()

        # .   .   .   .   .   .
        # EXTRA FEATS
        # .   .   .   .   .   .
        #extra_feats_ok, extra_feats_msg = check_extra_feats(spark, closing_day, verbose=False)
        if not extra_feats_ok:
            from churn.datapreparation.app.generate_table_extra_feats_new import generate_extra_feats, MODULE_JOIN, join_dataframes, get_modules_list_by_version
            #FIXME: remove when scores module is validated
            include_ef_modules = [mod for mod,doit in extrafeats_modules_dict.items() if doit and mod != MODULE_JOIN]
            print("include_ef_modules {}".format(",".join(include_ef_modules)))
            generate_extra_feats(spark, closing_day, include_modules=include_ef_modules, metadata_version="1.1", logger=logger)
            if extrafeats_modules_dict[MODULE_JOIN]:
                if logger: logger.info("Requested join of extra feats")
                modules_list = get_modules_list_by_version(metadata_version="1.1")
                if logger: logger.info("Join will include the following modules: {}".format(modules_list))
                join_dataframes(spark, closing_day, metadata_version="1.1", include_modules=modules_list)
            else:
                print("do not perform join")

            extra_feats_ok, extra_feats_msg, extrafeats_modules_dict, ex_feats_perc_completed= check_extra_feats(spark, closing_day, verbose=False)

            if not extra_feats_ok:
                if logger: logger.error("Extra feats process was not finished! Stopping program")
                if logger: logger.error(extra_feats_msg)
                sys.exit()

        # .   .   .   .   .   .
        # JOIN CAR of previous month
        # .   .   .   .   .   .
        if not join_car_ok:
            if logger: logger.error("Join car must exist at this point!! Generating it on the fly....")
            #"select * from tests_es.jvmm_amdocs_ids_" + process_date
            sql_join_car = sc._jvm.datapreparation.AmdocsCarPreprocessor.generateCar(scala_sc,
                                                                                                    closing_day)
            # Check if the join of the amdocs car is ready tests_es.db/jvmm_amdocs_ids_20190321;'
            join_car_ok, join_car_msg = check_join_car(spark, prev_closing_day, verbose=False)

            if not join_car_ok:
                if logger: logger.error("join_car process was not finished! Stopping program")
                if logger: logger.error(car_preparado_msg)
                sys.exit()



        # .   .   .   .   .   .
        # CAR PREPARADO
        # .   .   .   .   .   .
        #car_preparado_ok, car_preparado_msg = check_car_preparado(spark, closing_day, verbose=False)

        if not car_preparado_ok:

            sql_car_preparado = sc._jvm.datapreparation.AmdocsCarPreprocessor.storePreparedFeatures(scala_sc, closing_day)

            car_preparado_ok, car_preparado_msg = check_car_preparado(spark, closing_day, verbose=False)

            if not car_preparado_ok:
                if logger: logger.error("Car preparado process was not finished! Stopping program")
                if logger: logger.error(car_preparado_msg)
                sys.exit()

        # .   .   .   .   .   .
        # SCORES ONLYMOB
        # .   .   .   .   .   .
        #scores_new_onlymob_ok, scores_onlymob_msg = check_scores_new(spark, closing_day, segment="onlymob",
        #                                                             tr_cycle_ini=tr_cycle_ini,
        #                                                             tr_cycle_end=tr_cycle_end, verbose=False)
        if not scores_new_onlymob_ok:
            segmentfilter = "onlymob"
            sql_onlymob = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini, tr_cycle_end, closing_day,
                                                                     segmentfilter)
            scores_new_onlymob_ok = check_sql(spark, sql_onlymob)
            # scores_onlymob_ok, scores_onlymob_msg = check_scores(spark, closing_day, segment="onlymob", verbose=False)
            if not scores_new_onlymob_ok:
                if logger: logger.error("Scores onlymob process was not finished! Stopping program")
                if logger: logger.error(scores_new_onlymob_msg)

                sys.exit()

        # .   .   .   .   .   .
        # SCORES MOBILEANDFBB
        # .   .   .   .   .   .
        # scores_new_mobileandfbb_ok, scores_mobileandfbb_msg, sql_scores_mobileandfbb = check_scores_new(spark, closing_day,
        #                                                                        segment="mobileandfbb",
        #                                                                        tr_cycle_ini=tr_cycle_ini,
        #                                                                        tr_cycle_end=tr_cycle_end, verbose=False)

        if not scores_new_mobileandfbb_ok:
            segmentfilter = "mobileandfbb"
            sql_mobileandfbb = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini, tr_cycle_end, closing_day,
                                                                          segmentfilter)
            # scores_mobileandfbb_ok, scores_mobileandfbb_msg = check_scores(spark, closing_day, segment="mobileandfbb", verbose=False)
            scores_new_mobileandfbb_ok = check_sql(spark, sql_mobileandfbb)

            if not scores_new_mobileandfbb_ok:
                if logger: logger.error("Scores mobileandfbb process was not finished! Stopping program")
                if logger: logger.error(scores_new_mobileandfbb_msg)
                sys.exit()


        # .   .   .   .   .   .
        # DATA PREPARATION - LEVERS MODEL

        if not dp_levers_ok:
            # from churn.datapreparation.app.ccc_data_preparation_main import start_process
            # dp_levers_yaml = "/var/SP/data/home/csanc109/src/devel/use-cases/churn/datapreparation/input/dp_ccc_model_predictions.yaml"
            # start_process(spark, dp_levers_yaml, closing_day=closing_day)
            # dp_levers_ok, dp_levers_msg, dp_levers_dict, dp_levers_perc = check_levers_model(spark, closing_day)
            #
            # if not dp_levers_ok:
            #     if logger: logger.error("Data preparation for levers model was not finished! Stopping program")
            #     if logger: logger.error(scores_new_mobileandfbb_msg)
            #     sys.exit()
            from churn.datapreparation.app.ccc_data_preparation_main import start_process_config
            from churn.config_manager.ccc_model_config_mgr import build_config_dict

            for segment, do_it in dp_levers_dict.items():
                if not do_it:
                    continue

                my_config = build_config_dict(segment, agg_by='msisdn', ccc_days=-60, closing_day=closing_day, enabled=True,
                                  end_port='None', labeled=False, model_target='comercial', new_car_version=False,
                                  save_car=True,
                                  segment_filter=segment, start_port='None')


                start_process_config(spark, my_config)

            dp_levers_ok, dp_levers_msg, dp_levers_dict, dp_levers_perc = check_levers_model(spark, closing_day)

            if not dp_levers_ok:
                if logger: logger.error("Data preparation for levers model was not finished! Stopping program")
                if logger: logger.error(dp_levers_msg)
                sys.exit()

        # .   .   .   .   .   .
        # FINAL STEP
        # .   .   .   .   .   .
        if all([car_ok, extra_feats_ok, car_preparado_ok, scores_new_mobileandfbb_ok, scores_new_mobileandfbb_ok, dp_levers_ok]):
            if logger: logger.info("Ready to start the lastest step of the delivery...")
            from churn.others.run_churn_delivery_scores_incidences_levers_master import run_delivery

            os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
            os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"
            run_delivery(spark, closing_day, sql_scores_onlymob, sql_scores_mobileandfbb, logger)
            if logger: logger.info("Finished the latest step of the delivery...")
        else:
            if logger: logger.error("something when wrong... please review...")
            sys.exit()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if logger: logger.info("TOTAL TIME Process finished - {} minutes".format( (time.time()-start_time_total)/60.0))

    if logger: logger.info("Process ended successfully. Enjoy :)")

