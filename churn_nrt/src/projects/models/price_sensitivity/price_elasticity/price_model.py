#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
import sys
import time
from pyspark.sql.functions import lit

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re, sys

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
    from churn_nrt.src.projects.models.price_sensitivity.price_elasticity.model_classes import PriceSensitivity
    from churn_nrt.src.data_utils.Metadata import Metadata
    from churn_nrt.src.projects.models.price_sensitivity.price_elasticity.metadata import get_metadata

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

    parser = argparse.ArgumentParser(description="Run price_sensitivity model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
                                     epilog='Please report bugs and issues to Alvaro <alvaro.saez@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in test')
    parser.add_argument('--segment', metavar='<hard,soft,none>', type=str, required=True, help='Segment to be analyzed')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='model to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--price_q', metavar='<high,medium,low>', type=str, required=True, help='tbc')
    parser.add_argument('--horizon', metavar='8', type=str, required=False, help='tbc')
    parser.add_argument('--verbose', metavar=0, type=int, required=False, help='tbc')
    parser.add_argument('--gap_w', metavar='4', type=str, required=False, help='Number of cycles for gap window')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    model_ = args.model
    horizon = int(args.horizon)
    verbose = False
    gap_w = 1
    gap_label = 4
    segment = args.segment
    quantile = args.price_q

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))
    print("ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))

    metadata_obj = Metadata(spark, get_metadata, ["num_cliente"], ["ids"])

    model_price = PriceSensitivity(spark, tr_date_, mode_, model_, 'asaezco',metadata_obj, segment, quantile)

    if mode_ == 'evaluation':
        df_tr_preds, dict_tt_preds, val_df_preds = model_price.run(tt_date=[], do_calibrate_scores=2, filter_correlated_feats=True)

        path_to_save = '/data/attributes/vf_es/asaezco/price_val_scores/bill={}/segment={}/year={}/month={}/day={}'.format(quantile, segment, tr_date_[:4], int(tr_date_[4:6]), int(tr_date_[6:8]))
        if 'scoring' in val_df_preds.columns:
            scoring = 'scoring'
        elif 'model_score' in val_df_preds.columns:
            scoring = 'model_score'
        val_df_preds.select('num_cliente', scoring, 'label').write.mode("append").format("parquet").save(path_to_save)


    if mode_ == 'production':
        ########### ########### ########### ########### ########### ###########  CAMBIAMOS tt_date por tr_date en validación y train

        df_tr_preds, dict_tt_preds, val_df_preds = model_price.run(tt_date=[], do_calibrate_scores=2, filter_correlated_feats=False)
        '''
        ########### Training scores on 20191130 ###########
        #price_train_scores_corrected

        path_to_save = '/data/attributes/vf_es/asaezco/price_filtered/train_scores/bill={}/segment={}/year={}/month={}/day={}'.format(
                    quantile, segment, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))

        if 'scoring' in df_tr_preds.columns:
            scoring = 'scoring'
        elif 'model_score' in df_tr_preds.columns:
            scoring = 'model_score'
        df_tr_preds.select('num_cliente', scoring, 'label').write.mode("overwrite").format("parquet").save(path_to_save)
        '''


        ########### Validation scores 20191130 ###########
        path_to_save = '/data/attributes/vf_es/asaezco/price_filtered/val_scores/bill={}/segment={}/year={}/month={}/day={}'.format(quantile, segment, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
        #price_val_scores
        if 'scoring' in val_df_preds.columns:
            scoring = 'scoring'
        elif 'model_score' in val_df_preds.columns:
            scoring = 'model_score'
        print"Saving Validation df stage"
        val_df_preds.select('num_cliente', scoring, 'label').write.mode("overwrite").format("parquet").save(path_to_save)

        df_test_ = model_price.get_set(tt_date_, False)
        df_test_= df_test_.cache()
        df_test_.count()
        ########### Validation scores for inc=0 on 20191130 ###########
        for inc in [0]:
            #price_filtered_val_0
            df_test = model_price.get_set("20191131", False).withColumn("agg_sum_abs_inc", lit(inc))
            df_test = model_price.run_predict(tt_date_, df_tt_test=df_test)
            path_to_save = '/data/attributes/vf_es/asaezco/val_scores_0/price_inc={}/bill={}/segment={}/year={}/month={}/day={}'.format(inc,quantile,segment,tt_date_[:4],int(tt_date_[4:6]),int(tt_date_[6:8]))
            df_test.select('num_cliente', 'scoring').write.mode("overwrite").format("parquet").save(path_to_save)
            print"Saving Validation inc=0 df stage"
        ########### Test scores for several incs on test date ###########
        #df_test_ = model_price.get_set(tt_date_, False)
        for inc in [0,2,4,6,8,10]:
            df_test = df_test_.withColumn("agg_sum_abs_inc", lit(inc))
            df_test = model_price.run_predict(tt_date_, df_tt_test=df_test)
            path_to_save = '/data/attributes/vf_es/asaezco/price_filtered_test/price_inc={}/bill={}/segment={}/year={}/month={}/day={}'.format(inc,quantile,segment,tt_date_[:4],int(tt_date_[4:6]),int(tt_date_[6:8]))
            df_test.select('num_cliente', 'scoring').write.mode("overwrite").format("parquet").save(path_to_save)
            print"Saving test df stage"
            string_ = "Inc {}:".format(inc)
            print(string_)
        
        from pyspark.sql.functions import col
        df_m4m = spark.read.parquet('/data/raw/vf_es/cvm/SPS_BASE_M4M/1.0/parquet/year=2020/month=8/day=11')\
            .withColumn("agg_sum_abs_inc", col("SUBIDA_M4M").cast("double")).select('NUM_CLIENTE', 'agg_sum_abs_inc')
        #df_test = model_price.get_set(tt_date_, False)
        print"Number of customers before price increment join: {}".format(df_test.count())
        df_test = df_test_.join(df_m4m, ['num_cliente'], "inner")
        print"Number of customers after price increment join: {}".format(df_test.count())
        print"Number of distinct customers after price increment join: {}".format(df_test.select('NUM_CLIENTE').distinct().count())
        df_test = model_price.run_predict(tt_date_, df_tt_test=df_test)
        path_to_save = '/data/attributes/vf_es/asaezco/price_plan_b/bill={}/segment={}/year={}/month={}/day={}'.format(quantile, segment, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
        df_test.select('num_cliente', 'scoring').write.mode("overwrite").format("parquet").save(path_to_save)
        print"Saving M4M df stage"

        print("################ Finished process ################")







