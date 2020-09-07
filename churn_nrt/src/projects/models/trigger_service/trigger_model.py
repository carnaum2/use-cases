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

    spark, sc = get_spark_session_noncommon("trigger_service")
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
    from churn_nrt.src.projects.models.trigger_service.metadata import METADATA_STANDARD_MODULES, get_metadata
    import argparse

    parser = argparse.ArgumentParser(description="Run navcomp model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=False, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=False, help='Date to be used in test')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='model to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--sources', metavar='<YYYYMMDD>', type=str, required=False, default=METADATA_STANDARD_MODULES, help='list of sources to be used for building the training dataset')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES
    model_ = args.model

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

            tr_date_ = move_date_n_days(tt_date_, n=-32)
            print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))
        else:
            print("Mode {} does not support empty --tr_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass

    print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    from churn_nrt.src.data_utils.Metadata import Metadata
    metadata_obj = Metadata(spark, get_metadata, ["nif"], metadata_sources)


    from churn_nrt.src.projects.models.trigger_service.constants import OWNER_LOGIN, MODEL_OUTPUT_NAME, EXTRA_INFO_COLS, INSERT_TOP_K
    from churn_nrt.src.projects.models.trigger_service.model_classes import TriggerServiceModel

    model_service = TriggerServiceModel(spark, tr_date_, mode_, model_, OWNER_LOGIN, metadata_obj)

    from pyspark.sql.functions import when, col
    df_tr_preds, dict_tt_preds, _ = model_service.run(tt_date=[tt_date_], do_calibrate_scores=0 , filter_correlated_feats=False)
    df_tt_preds = dict_tt_preds[tt_date_]

    df_tt_preds = df_tt_preds.withColumn('flag_reimb_1w', when((col('Reimbursement_num_n1') > 0),1).otherwise(0)) \
    .withColumn('flag_fact_1w', when((col('num_tickets_tipo_tramitacion_w') > 0), 1).otherwise(0)) \
    .withColumn('flag_tickets_1w', when((col('num_tickets_tipo_incidencia_w')+ col('num_tickets_tipo_reclamacion_w')+ col('num_tickets_tipo_averia_w'))> 0, 1).otherwise(0))


    df_tt_preds = df_tt_preds.withColumn('flag_1w', when(col('flag_tickets_1w') + col('flag_reimb_1w') + col('nb_started_orders_last7') + col('flag_fact_1w') > 0,'flag1w=1').otherwise('flag1w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_1w_tickets',when(col('flag_tickets_1w') > 0, 'flag_tickets_1w=1').otherwise('flag_tickets_1w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_1w_orders', when(col('nb_started_orders_last7') > 0,'flag_orders_1w=1').otherwise('flag_orders_1w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_1w_billing',when(col('flag_fact_1w') > 0, 'flag_billing_1w=1').otherwise('flag_billing_1w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_1w_reimbursements', when(col('flag_reimb_1w') > 0,'flag_reimbursements_1w=1').otherwise('flag_reimbursements_1w=0'))




    df_tt_preds = df_tt_preds.withColumn('flag_reimb_2w', when((col('Reimbursement_num_n1')+ col('Reimbursement_num_n2') > 0),1).otherwise(0)) \
    .withColumn('flag_fact_2w', when((col('num_tickets_tipo_tramitacion_w2') > 0), 1).otherwise(0)) \
    .withColumn('flag_tickets_2w', when((col('num_tickets_tipo_incidencia_w2')+ col('num_tickets_tipo_reclamacion_w2')+ col('num_tickets_tipo_averia_w2'))> 0, 1).otherwise(0))


    df_tt_preds = df_tt_preds.withColumn('flag_2w', when(col('flag_tickets_2w') + col('flag_reimb_2w') + col('nb_started_orders_last14') + col('flag_fact_2w') > 0,'flag2w=1').otherwise('flag2w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_2w_tickets',when(col('flag_tickets_2w') > 0, 'flag_tickets_2w=1').otherwise('flag_tickets_2w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_2w_orders', when(col('nb_started_orders_last14') > 0,'flag_orders_2w=1').otherwise('flag_orders_2w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_2w_billing',when(col('flag_fact_2w') > 0, 'flag_billing_2w=1').otherwise('flag_billing_2w=0'))

    df_tt_preds = df_tt_preds.withColumn('flag_2w_reimbursements', when(col('flag_reimb_2w') > 0,'flag_reimbursements_2w=1').otherwise('flag_reimbursements_2w=0'))

    #model_service.print_summary()  # print a summary (optional)

    if mode_ == 'evaluation':
        from churn_nrt.src.projects_utils.models.modeler import get_metrics

        auc_train, _, _ = get_metrics(spark, df_tr_preds.fillna(0.0), title="TRAIN", do_churn_rate_fix_step=True, score_col="model_score", label_col="label")
        print('AUC for TRAINING set: {}'.format(auc_train))

        auc_test, _, _= get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST", do_churn_rate_fix_step=True, score_col="scoring", label_col="label")
        print('AUC for TEST set: {}'.format(auc_test))


    if mode_ == 'production':
        from churn_nrt.src.data_utils.model_outputs_manager import add_decile
        df_insert = add_decile(df_tt_preds, "scoring",INSERT_TOP_K)
        insert_cols = ["decil", "flag_propension"] + EXTRA_INFO_COLS
        model_service.insert_model_outputs(df_insert, MODEL_OUTPUT_NAME, test_date=tt_date_, insert_top_k=None, extra_info_cols=insert_cols, day_to_insert=3)  # insert the complete df

    print('########## Finished process ##########')





