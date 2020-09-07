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

def get_next_day_of_the_week(day_of_the_week):

    import datetime as dt

    idx = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    n = idx[day_of_the_week.lower()]

    # import datetime
    d = dt.date.today()

    while d.weekday() != n:
        d += dt.timedelta(1)

    year_= str(d.year)
    month_= str(d.month).rjust(2, '0')
    day_= str(d.day).rjust(2, '0')

    print 'Next ' + day_of_the_week + ' is day ' + str(day_) + ' of month ' + str(month_) + ' of year ' + str(year_)

    return year_+month_+day_

if __name__ == "__main__":

    set_paths()
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("revamp_mobile")
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

    df_train = get_ids_segments(spark, tr_date_, col_bi = "tgs_days_until_f_fin_bi", level='msisdn', verbose= False)

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
    target_msisdn_train = Target(spark, churn_window=(horizon) * 7, level='msisdn').get_module(tr_date_, save=False, save_others=False, force_gen=True)
    df_train_labeled = df_train_msisdn.join(target_msisdn_train, ['msisdn'], 'inner')

    ######################## Target Test ########################
    if mode_ == 'evaluation':
        target_msisdn_test = Target(spark, churn_window=(horizon) * 7, level='msisdn').get_module(tt_date_, save=False, save_others=False, force_gen=True)
        df_test_labeled = df_test_msisdn.join(target_msisdn_test, ['msisdn'], 'inner')

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

    mymodel = get_model(model_, "label")
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)

    ######################## Train/Calib ########################
    [tr_input_df, df_val] = df_train_labeled.randomSplit([0.7, 0.3], 1234)
    pipeline_model = pipeline.fit(tr_input_df.fillna(0.0))

    ######################## Feature Importance ########################
    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    ######################## Train Evaluation ########################
    df_tr_preds = get_score(pipeline_model, df_train_labeled.fillna(0.0), calib_model=None, score_col="model_score")
    df_val_preds = get_score(pipeline_model, df_val.fillna(0.0), calib_model=None, score_col="model_score")

    ######################## Validation df Storing ########################
    path_to_save = '/data/attributes/vf_es/revamp/validation_mob/segment={}/year={}/month={}/day={}'.format(segment,int(tr_date_[:4]), int(tr_date_[4:6]), int(tr_date_[6:8]))
    df_val_preds.select('msisdn', 'label', 'model_score').write.mode("overwrite").format("parquet").save(path_to_save)

    auc_train, _, _ = get_metrics(spark, df_tr_preds.fillna(0.0), title="TRAIN no calib", do_churn_rate_fix_step=True, score_col="model_score", label_col="label")
    print('AUC for TRAINING set: {}'.format(auc_train))

    if mode_ == 'evaluation':
    ######################## Test Evaluation ########################
        df_tt_preds = get_score(pipeline_model, df_test_labeled.fillna(0.0), calib_model=None, score_col="model_score")
        auc_test, _, _ = get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST no calib", do_churn_rate_fix_step=True, score_col="model_score", label_col="label")
        print('AUC for TEST set: {}'.format(auc_test))
        path_to_save = '/data/attributes/vf_es/revamp/scores_mob/segment={}/year={}/month={}/day={}'.format(segment,tt_date_[:4], tt_date_[4:6], tt_date_[6:8])
        df_tt_preds.select('msisdn', 'label', 'model_score').write.mode("append").format("parquet").save(path_to_save)
    elif(mode_ == 'production'):
    ######################## Test Predictions ########################
        df_tt_preds = get_score(pipeline_model, df_test_pred.fillna(0.0), calib_model=None, score_col="model_score")
        #df_tt_preds_calib = calibmodel[0].transform(df_tt_preds)
        print'Generated predictions for test set.'

        partition_date = "year={}/month={}/day={}".format(str(int(tt_date_[0:4])), str(int(tt_date_[4:6])),
                                                      str(int(tt_date_[6:8])))

        model_output_cols = ["model_name", \
                         "executed_at", \
                         "model_executed_at", \
                         "predict_closing_date", \
                         "msisdn", \
                         "client_id", \
                         "nif", \
                         "model_output", \
                         "scoring", \
                         "prediction", \
                         "extra_info", \
                         "year", \
                         "month", \
                         "day", \
                         "time"]

        partition_date = get_next_day_of_the_week('friday')
        partition_year = int(partition_date[0:4])
        partition_month = int(partition_date[4:6])
        partition_day = int(partition_date[6:8])

        import datetime as dt
        df_mo = df_tt_preds.select('msisdn', 'model_score')

        executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        from pyspark.sql.functions import split, from_unixtime, unix_timestamp
        model_name = "revamp_mobile_" + segment

        df_model_scores = df_mo \
        .withColumn("model_name", lit(model_name).cast("string")) \
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
        .withColumn("model_executed_at", col("executed_at").cast("string")) \
        .withColumn("client_id", lit("")) \
        .withColumn("nif", lit("")) \
        .withColumn("scoring", col("model_score").cast("float")) \
        .withColumn("model_output", lit("").cast("string")) \
        .withColumn("prediction", lit("").cast("string")) \
        .withColumn("extra_info", lit(""))\
        .withColumn("predict_closing_date", lit(tt_date_)) \
        .withColumn("year", lit(partition_year).cast("integer")) \
        .withColumn("month", lit(partition_month).cast("integer")) \
        .withColumn("day", lit(partition_day).cast("integer")) \
        .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer")) \
        .select(*model_output_cols)

        import pandas as pd

        df_pandas = pd.DataFrame({
            "model_name": [model_name],
            "executed_at": [executed_at],
            "model_level": ["nif"],
            "training_closing_date": [tr_date_],
            "target": [""],
            "model_path": [""],
            "metrics_path": [""],
            "metrics_train": [""],
            "metrics_test": [""],
            "varimp": ["-"],
            "algorithm": [model_],
            "author_login": ["asaezco"],
            "extra_info": [""],
            "scores_extra_info_headers": ["None"],
            "year": [partition_year],
            "month": [partition_month],
            "day": [partition_day],
            "time": [int(executed_at.split("_")[1])]})

        df_model_parameters = spark \
            .createDataFrame(df_pandas) \
            .withColumn("day", col("day").cast("integer")) \
            .withColumn("month", col("month").cast("integer")) \
            .withColumn("year", col("year").cast("integer")) \
            .withColumn("time", col("time").cast("integer"))

        df_model_scores.coalesce(400) \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save("/data/attributes/vf_es/model_outputs/model_scores/")

        df_model_parameters \
            .coalesce(1) \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save("/data/attributes/vf_es/model_outputs/model_parameters/")

        print("Inserted to model outputs")

    print('########## Finished process ##########')





