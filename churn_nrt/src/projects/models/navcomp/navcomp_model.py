#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time
from pyspark.sql.functions import (col, avg as sql_avg, concat, lit)

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


def __get_last_date(spark):

    from churn_nrt.src.data.customers_data import get_last_date as get_last_date_customer
    from churn_nrt.src.data.services_data import get_last_date as get_last_date_service
    from churn_nrt.src.data.navcomp_data import get_last_date as get_last_date_navcomp


    customer_last_date = get_last_date_customer(spark)
    service_last_date = get_last_date_service(spark)
    navcomp_last_date = get_last_date_navcomp(spark)


    last_date = str(min([navcomp_last_date, customer_last_date, service_last_date]))

    return last_date


if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("trigger_navcomp")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - algorithm: algorithm for training
    #      - mode_ : evaluation or prediction
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn_nrt.src.projects.models.navcomp.metadata import METADATA_STANDARD_MODULES, get_metadata
    from churn_nrt.src.utils.constants import MODE_EVALUATION, MODE_PRODUCTION
    from churn_nrt.src.data_utils.Metadata import Metadata
    from churn_nrt.src.projects.models.navcomp.constants import OWNER_LOGIN, MODEL_OUTPUT_NAME, EXTRA_INFO_COLS, INSERT_TOP_K, MASK_AS_RISK
    from churn_nrt.src.projects.models.navcomp.model_classes import TriggerNavCompModel
    import argparse

    parser = argparse.ArgumentParser(
        description="Run navcomp model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in test')

    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf",
                        help='model to be used for training de model')

    parser.add_argument('--sources', metavar='<YYYYMMDD>', type=str, required=False, default=METADATA_STANDARD_MODULES,
                        help='list of sources to be used for building the training dataset')

    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--day_to_insert', metavar='<day_to_insert>', type=int, required=False, default=4, help='day to insert in model outputs')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    model_ = args.model
    metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES
    mode_ = args.mode if args.mode else "evaluation"
    day_to_insert = args.day_to_insert

    print("day_to_insert", type(day_to_insert))
    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}' day_to_insert={}".format(tr_date_, tt_date_, model_, ",".join(metadata_sources), day_to_insert))

    print("Starting check of input params")

    if not tt_date_:
        if mode_==MODE_PRODUCTION:
            print("Not introduced a test date. Computing automatically...")
            tt_date_ = str(__get_last_date(spark))
            print("Computed tt_date_={}".format(tt_date_))
        else:
            print("Mode {} does not support empty --tt_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass


    if not tr_date_:
        if mode_ == MODE_PRODUCTION:
            print("Not introduced a training date. Computing automatically")
            from churn_nrt.src.utils.date_functions import move_date_n_days

            tr_date_ = move_date_n_days(tt_date_, n=-16)
            print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))
        else:
            print("Mode {} does not support empty --tr_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass

    print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    if mode_ == MODE_PRODUCTION:
        from churn_nrt.src.data_utils.model_outputs_manager import get_last_dates
        last_deliv_date, last_tr_date, last_tt_date = get_last_dates(spark, MODEL_OUTPUT_NAME)
        print("Last dates inserted in model_scores: deliv={} train={} test={}".format(last_deliv_date, last_tr_date, last_tt_date))

        if last_tt_date == tt_date_:
            print("")
            print("[ERROR] Test date is the same as the last one stored in model_outputs. Nothing will be generated")
            print("[ERROR] Program will exit here!")
            print("")
            import sys
            sys.exit(1)


    #################################################



    metadata_obj = Metadata(spark, get_metadata, ["msisdn"], metadata_sources)



    model_navcomp = TriggerNavCompModel(spark, tr_date_, mode_, model_, OWNER_LOGIN, metadata_obj)

    df_tr_preds, dict_tt_preds, _ = model_navcomp.run(tt_date=[tt_date_], do_calibrate_scores=0, handle_invalid ="skip")
    df_tt_preds = dict_tt_preds[tt_date_]

    model_navcomp.print_summary() # print a summary (optional)

    if mode_ == MODE_PRODUCTION:
        df_tt_preds = df_tt_preds\
            .withColumn("operator", concat(lit("operator="), col("most_consulted_operator")))

        from churn_nrt.src.data_utils.model_outputs_manager import add_decile

        df_tt_preds = add_decile(df_tt_preds, score="scoring", perc=MASK_AS_RISK)

        model_navcomp.insert_model_outputs(df_tt_preds,
                                           MODEL_OUTPUT_NAME,
                                           test_date=tt_date_,
                                           insert_top_k=INSERT_TOP_K,
                                           extra_info_cols=EXTRA_INFO_COLS,
                                           day_to_insert=day_to_insert)

        #model_navcomp.insert_model_outputs(df_tt_preds, MODEL_OUTPUT_NAME, test_date=tt_date_, insert_top_k=INSERT_TOP_K, extra_info_cols=EXTRA_INFO_COLS, day_to_insert=day_to_insert) # insert the complete df


    if mode_ != MODE_PRODUCTION:
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Obtain Churn Rate of reference base
        from churn_nrt.src.data.customer_base import CustomerBase

        base_df = CustomerBase(spark).get_module(tt_date_).filter(col('rgu') == 'mobile').select('msisdn')

        from churn_nrt.src.data.sopos_dxs import MobPort

        base_df = base_df\
            .join(MobPort(spark, churn_window=15).get_module(tt_date_, save=True).select('msisdn', 'label_mob'), ['msisdn'], 'left').na.fill(0.0)


        print('[Info evaluate_navcomp_model] Labeled base for {} - Size: {} - Num distinct msisdn: {}'.format(tt_date_,
                                                                                                            base_df.count(),
                                                                                                            base_df.select('msisdn').distinct().count()))

        tt_churn_ref = base_df.select(sql_avg('label_mob').alias('churn_ref')).rdd.first()['churn_ref']

        print('[Info evaluate_navcomp_model] Churn of the base for {}:{}'.format(tt_date_, tt_churn_ref))


    print("Process finished - Elapsed time: {} minutes ({} hours)".format( (time.time()-start_time_total)/60.0, (time.time()-start_time_total)/3600.0))

    import sys
    sys.exit(0)