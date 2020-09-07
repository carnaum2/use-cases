#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
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
from pyspark.sql.functions import col, count, when, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re, sys
    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset/')

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

if __name__ == "__main__":

    set_paths()
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("price_sensitivity")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - mode_ : evaluation or prediction
    #      - model :  algorithm for training
    #      - horizon : cycles horizon to predict sensitivity
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(description="Run price_sensitivity model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Alvaro <alvaro.saez@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in test')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='model to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--horizon', metavar='8', type=str, required=False, help='tbc')
    parser.add_argument('--verbose', metavar=0, type=int, required=False, help='tbc')
    parser.add_argument('--segment', metavar='<none,hard, soft>', type=str, required=True, help='selected bound segment')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    model_ = args.model
    horizon = int(args.horizon)
    verbose = False
    segment = args.segment

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))
    print("ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))

    ######################## Segment dfs ########################
    from churn_nrt.src.data_utils.ids_utils import get_ids_segments, get_ids

    df_train = get_ids_segments(spark, tr_date_, col_bi = "tgs_days_until_f_fin_bi", verbose= False)

    none_segment_train = df_train.where(col('blindaje') == 'none')
    soft_segment_train = df_train.where(col('blindaje') == 'soft')
    hard_segment_train = df_train.where(col('blindaje') == 'hard')

    df_test = get_ids_segments(spark, tt_date_, col_bi = "tgs_days_until_f_fin_bi", verbose= False)

    none_segment_test = df_test.where(col('blindaje') == 'none')
    soft_segment_test = df_test.where(col('blindaje') == 'soft')
    hard_segment_test = df_test.where(col('blindaje') == 'hard')


    df_ids_train = get_ids(spark, tr_date_).where(col('serv_rgu')=='mobile')
    df_ids_test = get_ids(spark, tt_date_).where(col('serv_rgu')=='mobile')


    if segment == "none":
        df_train_msisdn = none_segment_train.select('msisdn').join(df_ids_train, ['msisdn'], 'inner')
        df_test_msisdn = none_segment_test.select('msisdn').join(df_ids_test, ['msisdn'], 'inner')
    elif segment == "soft":
        df_train_msisdn = soft_segment_train.select('msisdn').join(df_ids_train, ['msisdn'], 'inner')
        df_test_msisdn = soft_segment_test.select('msisdn').join(df_ids_test, ['msisdn'], 'inner')
    elif segment == "hard":
        df_train_msisdn = hard_segment_train.select('msisdn').join(df_ids_train, ['msisdn'], 'inner')
        df_test_msisdn = hard_segment_test.select('msisdn').join(df_ids_test, ['msisdn'], 'inner')

    ######################## Target Train ########################
    from churn_nrt.src.data.sopos_dxs import Target
    target_msisdn_train = Target(spark, churn_window=(horizon) * 7, level='msisdn').get_module(tr_date_)
    df_train_labeled = df_train_msisdn.join(target_msisdn_train, ['msisdn'], 'inner')
    #df_train_labeled = df_train_labeled.cache()
    #print('Size of the labeled training df: {}'.format(df_train_labeled.count()))

    ######################## Target Test ########################
    if mode_ == 'evaluation':
        target_msisdn_test = Target(spark, churn_window=(horizon) * 7, level='msisdn').get_module(tt_date_)
        df_test_labeled = df_test_msisdn.join(target_msisdn_test, ['msisdn'], 'inner')
        #df_test_labeled = df_test_labeled.cache()
        #print('Size of the labeled test df: {}'.format(df_test_labeled.count()))

    else:
        df_test_pred = df_test_msisdn

    df_train_labeled = df_train_labeled.cache()
    print('Final prepared train df size: {}'.format(df_train_labeled.count()))
    df_train_labeled.groupBy('label').agg(count('*')).show()
    df_train_labeled.drop_duplicates(['nif_cliente']).groupBy('label').agg(count('*')).show()

    if mode_ == 'evaluation':
        df_test_labeled = df_test_labeled.cache()
        print('Final prepared test df size: {}'.format(df_test_labeled.count()))
        df_test_labeled.groupBy('label').agg(count('*')).show()
        df_test_labeled.drop_duplicates(['nif_cliente']).groupBy('label').agg(count('*')).show()
    else:
        df_test_pred = df_test_pred.cache()
        print('Final prepared test df size: {}'.format(df_test_pred.count()))

    '''
    ######################## Feature selection ########################
    print('Number of feats in the final df: {}'.format(len(df_train_labeled.columns) - 1))

    from churn_nrt.src.data_utils.ids_utils import get_ids_msisdn_feats, get_ids_nif_feats, get_ids_nc_feats

    numeric_nif = [f[0] for f in df_train_msisdn.dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]
    nif_feats = get_ids_nif_feats(numeric_nif)
    nc_feats = get_ids_nc_feats(df_train_msisdn.dtypes, True)

    feats = nif_feats + nc_feats

    if rgu == 'mobile':
        msisdn_feats = get_ids_msisdn_feats(df_train_msisdn.dtypes, True)
        feats = feats + msisdn_feats
    
    ######################## Feature selection ########################
    from churn_nrt.src.utils.pyspark_utils import count_nans

    null_cols = count_nans(df_train_labeled.select(feats)).keys()

    feats_ = list(set(feats) - set(null_cols))
    feats = [f[0] for f in df_train_msisdn.select(feats_).dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]
    print(feats)
    '''
    # Getting the metadata to identify numerical and categorical features
    from src.main.python.utils.general_functions import get_all_metadata
    from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features

    non_inf_features = get_noninf_features() + get_no_input_feats()
    final_map, categ_map, numeric_map, date_map = get_all_metadata(tr_date_)
    print "[Info] Metadata has been read"
    categorical_columns = list(set(categ_map.keys()) - set(non_inf_features))
    gnv_roam_columns = [c for c in numeric_map.keys() if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_map.keys() if ('address_' in c)]
    numeric_columns = list(set(numeric_map.keys()) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))

    ######################## Modeling ########################
    from churn_nrt.src.projects_utils.models.modeler import smart_fit, get_metrics, get_feats_imp, get_score
    # 3.3. Building the stages of the pipeline
    from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
    from pyspark.ml import Pipeline

    feats_ = numeric_columns
    stages = []

    assembler_inputs = feats_
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    print("[Info] Starting fit....")
    from churn_nrt.src.projects_utils.models.modeler import get_model

    mymodel = get_model("rf", "label")
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(df_train_labeled.fillna(0.0))
    ######################## Feature Importance ########################
    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    ######################## Train Evaluation ########################
    df_tr_preds = get_score(pipeline_model, df_train_labeled.fillna(0.0), calib_model=None, score_col="model_score")
    auc_train, _, _ = get_metrics(spark, df_tr_preds.fillna(0.0), title="TRAIN no calib", do_churn_rate_fix_step=True, score_col="model_score", label_col="label")
    print('AUC for TRAINING set: {}'.format(auc_train))

    if mode_ == 'evaluation':
    ######################## Test Evaluation ########################
        df_tt_preds = get_score(pipeline_model, df_test_labeled.fillna(0.0), calib_model=None, score_col="model_score")
        auc_test, _, _ = get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST no calib", do_churn_rate_fix_step=True, score_col="model_score", label_col="label")
        print('AUC for TEST set: {}'.format(auc_test))
    else:
    ######################## Test Predictions ########################
        df_tr_preds = get_score(pipeline_model, df_test_pred.fillna(0.0), calib_model=None, score_col="model_score")
        print'Generated predictions for test set.'

    print('########## Finished process ##########')





