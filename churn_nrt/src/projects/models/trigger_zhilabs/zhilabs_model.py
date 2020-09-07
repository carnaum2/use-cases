#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
# from pyspark.sql.window import Window
# from pyspark.sql.functions import (
#                                    col,
#                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
#                                    avg as sql_avg,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
#                                    )
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

def __get_last_date(spark):

    from churn_nrt.src.data.customers_data import get_last_date as get_last_date_customer
    from churn_nrt.src.data.services_data import get_last_date as get_last_date_service

    from datetime import datetime
    # datetime object containing current date and time
    now = datetime.now()

    closing_day = now.strftime("%Y%m%d")

    customer_last_date = get_last_date_customer(spark)
    service_last_date = get_last_date_service(spark)
    from churn_nrt.src.utils.date_functions import move_date_n_days
    sources_last_date = int(move_date_n_days(closing_day, n=-3))

    print'Sources last date: ' + str(sources_last_date)

    last_date = str(min([sources_last_date, customer_last_date, service_last_date]))

    print'Last date: ' + str(last_date)

    return last_date



if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon

    spark, sc = get_spark_session_noncommon("trigger_zhilabs")
    #sc.setLogLevel('WARN')

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
    from churn_nrt.src.projects.models.trigger_zhilabs.metadata import METADATA_STANDARD_MODULES, METADATA_STANDARD_MODULES_DSL, METADATA_STANDARD_MODULES_HFC, get_metadata

    import argparse

    parser = argparse.ArgumentParser(description="Run navcomp model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=False, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=False, help='Date to be used in test')
    parser.add_argument('--critical', metavar='<YYYYMMDD>', type=str, required=False,  default="critical", help='Critical or warning threshold')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='model to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--tech', metavar='<ftth,hfc,dsl>', type=str, required=False, default='ftth', help='tbc')
    parser.add_argument('--sources', metavar='<YYYYMMDD>', type=str, required=False, default=METADATA_STANDARD_MODULES, help='list of sources to be used for building the training dataset')

    args = parser.parse_args()
    print(args)

    tech = args.tech
    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    model_ = args.model
    critical = args.critical

    print'Chosen tech: ' + str(tech)

    if tech == 'ftth':
        metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES
    elif tech == 'dsl':
        metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES_DSL
    else:
        metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES_HFC


    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))

    print("Starting check of input params")

    if not tt_date_:
        if mode_ == 'production':
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
        if mode_ == 'production':
            print("Not introduced a training date. Computing automatically")
            from churn_nrt.src.utils.date_functions import move_date_n_days

            tr_date_ = move_date_n_days(tt_date_, n=-35)
            print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))
        else:
            print("Mode {} does not support empty --tr_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass

    print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    from churn_nrt.src.data_utils.Metadata import Metadata
    metadata_obj = Metadata(spark, get_metadata, ["num_cliente"], metadata_sources)

    from churn_nrt.src.projects.models.trigger_zhilabs.constants import OWNER_LOGIN, MODEL_OUTPUT_NAME,MODEL_OUTPUT_NAME_HFC, MODEL_OUTPUT_NAME_DSL, EXTRA_INFO_COLS
    from churn_nrt.src.projects.models.trigger_zhilabs.model_classes import TriggerZhilabsModel

    model_zhilabs = TriggerZhilabsModel(spark, tr_date_, mode_, model_, OWNER_LOGIN, metadata_obj, tech, critical)

    from pyspark.sql.functions import when, col, desc

    df_tr_preds, dict_tt_preds, _ = model_zhilabs.run(tt_date=[tt_date_], do_calibrate_scores=0, filter_correlated_feats=True)
    df_tt_preds = dict_tt_preds[tt_date_]


    if mode_ == 'evaluation':
        from churn_nrt.src.projects_utils.models.modeler import get_metrics
        from pyspark.sql.types import DoubleType
        #model_zhilabs.print_summary()  # print a summary (optional)
        '''
        df_tr_preds = df_tr_preds.withColumn('scoring', col('model_score').cast(DoubleType()))
        path_to_save = '/data/attributes/vf_es/asaezco/incremental_warning/year={}/month={}/day={}'.format(tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
        if 'scoring' in df_tt_preds.columns:
            scoring = 'scoring'
        elif 'model_score' in df_tt_preds.columns:
            scoring = 'model_score'
        df_tt_preds.select('num_cliente', scoring).write.mode("overwrite").format("parquet").save(path_to_save)
        '''
        auc_test, _, _ = get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST", do_churn_rate_fix_step=True, score_col="scoring", label_col="label")
        print('AUC for TEST set: {}'.format(auc_test))

    if mode_ == 'production':

        if tech == 'ftth':
            from pyspark.sql.functions import concat, lit
            model_output_n = "zhilabs_ftth"
            top_k = 6000
            extra_info = EXTRA_INFO_COLS
            df_tt_preds = df_tt_preds.withColumn("cpe_model", concat(lit("Router="), col('cpe_model')))
            df_tt_preds = df_tt_preds.withColumn("network_access_type", concat(lit("Net_tech="), col('network_access_type')))
        elif tech == 'hfc':
            model_output_n = MODEL_OUTPUT_NAME_HFC
            top_k = 40000
            extra_info = []
        elif tech == 'dsl':
            model_output_n = MODEL_OUTPUT_NAME_DSL
            top_k = 10000
            extra_info = []

        from churn_nrt.src.data_utils.model_outputs_manager import add_decile

        df_insert = add_decile(df_tt_preds, "scoring", top_k)
        model_zhilabs.insert_model_outputs(df_insert.drop('nif_cliente'), model_output_n, test_date=tt_date_, insert_top_k=None, extra_info_cols=extra_info, day_to_insert=3)  # insert the complete df

        print"################## Finished Process ##################"





