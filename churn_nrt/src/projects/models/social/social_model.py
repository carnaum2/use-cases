#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import os
import time
# from pyspark.sql.window import Window
from pyspark.sql.functions import (
                                    col,
                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
                                     avg as sql_avg,
                                    concat,
                                    lit,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
                                    )
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# import datetime as dt



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


def __get_last_date(spark, sources):
    #TODO use sources parameter to get

    from churn_nrt.src.data.customers_data import get_last_date as get_last_date_customer
    from churn_nrt.src.data.services_data import get_last_date as get_last_date_service
    from churn_nrt.src.data.navcomp_data import get_last_date as get_last_date_navcomp
    from churn_nrt.src.data.myvf_data import get_last_date as get_last_date_myvf
    from churn_nrt.src.data.geneva_data import get_last_date as get_last_date_geneva



    customer_last_date = get_last_date_customer(spark)
    service_last_date = get_last_date_service(spark)
    gnv_last_date = get_last_date_geneva(spark)


    dates = [customer_last_date, service_last_date, gnv_last_date]

    if "navcomp" in sources:
        navcomp_last_date = get_last_date_navcomp(spark)
        dates.append(navcomp_last_date)

    if any([ss.startswith("myvf") for ss in sources]):
        myvf_last_date = get_last_date_myvf(spark, "app")
        dates.append(myvf_last_date)


    last_date = str(min(dates))

    return last_date


if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################



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

    from churn_nrt.src.projects.models.social.metadata import METADATA_STANDARD_MODULES, get_metadata
    from churn_nrt.src.utils.constants import MODE_PRODUCTION
    from churn_nrt.src.projects.models.social.constants import DEFAULT_N_DAYS_TARGET

    import argparse

    parser = argparse.ArgumentParser(
        description="Run social model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
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

    parser.add_argument('--target_days', metavar='<days for labeling>', type=int, required=False, default=DEFAULT_N_DAYS_TARGET, help='number of days for labeling')
    #parser.add_argument('--navig_days', metavar='<days for looking customers navigations>', type=int, required=False, default=DEFAULT_N_DAYS_NAVIGATION, help='days for looking customers navigations')
    #parser.add_argument('--platform', metavar='app/web', type=str, required=True, help='platform (web or app)')
    #parser.add_argument('--navig_sections', metavar='<sections to take into account for navigation>', type=str, required=False, default="None", help='sections to take into account for navigation')
    parser.add_argument('--filter_correlated_feats', metavar='<filter_correlated_feats>', type=int, required=True, default=0, help='Filter correlated feats')
    parser.add_argument('--balance_tr_df', metavar='<balance_tr_df>', type=int, required=False, default=0, help='balance tr df')
    parser.add_argument('--day_to_insert', metavar='<day_to_insert>', type=int, required=False, default=4, help='day to insert in model outputs')
    parser.add_argument('--segment_rule', metavar='<segment_rule>', type=int, required=False, default=2, help='nb of the rule to define the segment under analysis')



    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    model_ = args.model
    metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES
    mode_ = args.mode if args.mode else "evaluation"
    target_days = args.target_days
    balance_tr_df = True if args.balance_tr_df == 1 else False
    day_to_insert = args.day_to_insert
    segment_rule = args.segment_rule

    filter_correlated_feats = True if args.filter_correlated_feats == 1 else False
    #navig_sections = None if (navig_sections=="all" or not navig_sections or len(navig_sections.strip())==0) else navig_sections.split(",")
    #print(navig_sections)
    print(type(filter_correlated_feats), filter_correlated_feats)
    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}' target_days={}".format(tr_date_,
                                                                                                              tt_date_,
                                                                                                              model_,
                                                                                                              ",".join(metadata_sources),
                                                                                                              target_days
                                                                                                              ))
    print("INPUT ARGS filter_correlated_feats={} balance_tr_df={} day_to_insert={} segment_rule={}".format(
                                                                                          filter_correlated_feats,
                                                                                          balance_tr_df,
                                                                                          day_to_insert,
                                                                                          segment_rule
                                                                                          ))

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("trigger_social")
    sc.setLogLevel('WARN')

    print("Starting check of input params")

    from churn_nrt.src.data_utils.Metadata import Metadata
    metadata_obj = Metadata(spark, get_metadata, ["msisdn"], metadata_sources)


    if mode_ == "generation":
        from churn_nrt.src.utils.date_functions import days_range
        from churn_nrt.src.projects.models.social.model_classes import TriggerSocialData
        for c_day in days_range(tr_date_, tt_date_):
            print("Generating car for day {}...".format(c_day))
            df_tr_myvfdata = TriggerSocialData(spark, metadata_obj).get_module(c_day, force_gen=False, save_others=False, save=True)

    else:
        from churn_nrt.src.projects.models.social.constants import OWNER_LOGIN, MODEL_OUTPUT_NAME, EXTRA_INFO_COLS, INSERT_TOP_K, MARK_AS_RISK
        from churn_nrt.src.projects.models.social.model_classes import TriggerSocialModel

        if not tt_date_:
            if mode_==MODE_PRODUCTION:
                print("Not introduced a test date. Computing automatically...")
                tt_date_ = str(__get_last_date(spark, sources=metadata_sources))
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

                tr_date_ = move_date_n_days(tt_date_, n=-(target_days+1)) # avoid overlapping between train and test
                print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))
            else:
                print("Mode {} does not support empty --tr_date. Program will stop here!".format(mode_))
                sys.exit()
        else:
            # TODO check something?
            pass

        print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

        #################################################


        def get_model(model_name, label_col="label", featuresCol="features"):
            from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

            return {  # numtrees=500, minInstancesPerNode=20
                'rf': RandomForestClassifier(featuresCol=featuresCol, numTrees=800, maxDepth=10, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=100, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
                'rf2': RandomForestClassifier(featuresCol=featuresCol, numTrees=1000, maxDepth=5, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=150, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
                'rf3': RandomForestClassifier(featuresCol=featuresCol, numTrees=1300, maxDepth=5, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=200, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
                'rf4': RandomForestClassifier(featuresCol=featuresCol, numTrees=1500, maxDepth=5, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=250, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
                'gbt': GBTClassifier(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
                'gbt2': GBTClassifier(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=50, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
                'gbt3': GBTClassifier(featuresCol=featuresCol, labelCol=label_col, maxDepth=4, maxBins=32, minInstancesPerNode=100, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7)

            }[model_name]

        model_obj = get_model(model_, featuresCol="features", label_col="label",)

        model_social = TriggerSocialModel(spark, tr_date_, mode_, model_obj, OWNER_LOGIN, metadata_obj, target_days, segment_rule)
        df_tr_preds, dict_tt_preds, _ = model_social.run(tt_date=[tt_date_], do_calibrate_scores=0, filter_correlated_feats=filter_correlated_feats, balance_tr_df=balance_tr_df)
        df_tt_preds = dict_tt_preds[tt_date_]

        if mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
            import time
            start_time_social = time.time()
            print("About to save test df - /user/csanc109/projects/churn/data/social/df_{}".format(tt_date_))
            df_tt_preds.write.mode('overwrite').save("/user/csanc109/projects/churn/data/social/df_{}".format(tt_date_))
            print("Ended saving test df - /user/csanc109/projects/churn/data/social/df_{} (elapsed time {} minutes)".format(tt_date_, (time.time() - start_time_social)/60.0))



        print("----MODEL SOCIAL SUMMARY----")
        model_social.print_summary() # print a summary (optional)
        print("----END MODEL SOCIAL SUMMARY----")

        if mode_ == MODE_PRODUCTION:
            OPERATORS = ['JAZZTEL', 'YOIGO', 'DIGI_SPAIN', 'MAS_MOVIL', 'TELEFONICA', 'PEPEPHONE', 'R_CABLE', 'ORANGE', 'LEBARA', 'R', 'VODAFONE']

            from churn_nrt.src.data.geneva_data import PREFFIX_COLS
            df_tt_preds = df_tt_preds.select(*(df_tt_preds.columns + [when(col("{}_total_calls".format(PREFFIX_COLS)) > 0, (col('{}_mou_{}'.format(PREFFIX_COLS, op)) / col("{}_total_mou".format(PREFFIX_COLS)))).otherwise(-1).alias("{}_perc_mou_{}".format(PREFFIX_COLS, op)) for op in OPERATORS]))

            for op in OPERATORS:
                df_tt_preds = df_tt_preds.withColumn("CALLS_{}".format(op), concat(lit("CALLS_{}=".format(op)), col("{0}_perc_calls_{1}".format(PREFFIX_COLS, op))))
                df_tt_preds = df_tt_preds.withColumn("MOU_{}".format(op), concat(lit("MOU_{}=".format(op)), col("{0}_perc_mou_{1}".format(PREFFIX_COLS, op))))

            from churn_nrt.src.data_utils.model_outputs_manager import add_decile
            df_tt_preds = add_decile(df_tt_preds, score="scoring", perc=MARK_AS_RISK)

            model_social.insert_model_outputs(df_tt_preds, MODEL_OUTPUT_NAME, test_date=tt_date_, insert_top_k=INSERT_TOP_K, extra_info_cols=EXTRA_INFO_COLS, day_to_insert=day_to_insert) # insert the complete df



        else:
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Obtain Churn Rate of reference base

            from churn_nrt.src.data.customer_base import CustomerBase

            base_df = CustomerBase(spark).get_module(tt_date_).filter(col('rgu') == 'mobile').select('msisdn')

            from churn_nrt.src.data.sopos_dxs import MobPort

            base_df = base_df\
                .join(MobPort(spark, churn_window=target_days).get_module(tt_date_, save=True).select('msisdn', 'label_mob'), ['msisdn'], 'left').na.fill(0.0)


            print('[Info evaluate_social_model] Labeled base for {} - Size: {} - Num distinct msisdn: {}'.format(tt_date_,
                                                                                                                base_df.count(),
                                                                                                                base_df.select('msisdn').distinct().count()))

            tt_churn_ref = base_df.select(sql_avg('label_mob').alias('churn_ref')).rdd.first()['churn_ref']

            print('[Info evaluate_social_model] Churn of the base for {}:{}'.format(tt_date_, tt_churn_ref))


    print("Process finished - Elapsed time: {} minutes ({} hours)".format( (time.time()-start_time_total)/60.0, (time.time()-start_time_total)/3600.0))

    import sys
    sys.exit(0)