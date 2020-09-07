#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
import math
import re
from pyspark.sql.window import Window
from pyspark.sql.functions import (udf,
                                   col,
                                   decode,
                                   when,
                                   lit,
                                   lower,
                                   concat,
                                   translate,
                                   count,
                                   sum as sql_sum,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   count as sql_count,
                                   greatest,
                                   least,
                                   isnull,
                                   isnan,
                                   struct,
                                   substring,
                                   size,
                                   length,
                                   year,
                                   month,
                                   dayofmonth,
                                   unix_timestamp,
                                   date_format,
                                   from_unixtime,
                                   datediff,
                                   to_date,
                                   desc,
                                   asc,
                                   countDistinct,
                                   row_number,
                                   regexp_replace,
                                   upper,
                                   trim,
                                   array,
                                   create_map,
                                   randn,
                                   split)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType

#CAR_PATH = "/user/csanc109/projects/triggers/car_exploration/"
'''
CAR_PATH = "/data/udf/vf_es/churn/triggers/car_exploration_model1_withTickets/"
CAR_PATH_UNLABELED = "/data/udf/vf_es/churn/triggers/car_exploration_unlabeled_model1_withTickets/"

'''

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
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

if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn.utils.general_functions import init_spark

    spark = init_spark("trigger_segm")
    sc = spark.sparkContext

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    starting_day = sys.argv[1]

    closing_day = sys.argv[2]

    tech = sys.argv[3]

    from churn.analysis.fixed_qos_trigger.zhilabs_utils import get_zhilabs_ftth_att, get_zhilabs_hfc_att, get_zhilabs_adsl_att, \
        get_hfc_population, get_adsl_population, get_ftth_population

    from pykhaos.utils.date_functions import move_date_n_days
    end_port_1= move_date_n_days(starting_day, n=+30)
    end_port_2 = move_date_n_days(closing_day, n=+30)

    if tech == 'hfc':
        df_train = get_hfc_population(spark, starting_day, end_port_1)
        df_test = get_hfc_population(spark, closing_day, end_port_2)
    elif tech == 'ftth':
        df_train = get_ftth_population(spark, starting_day, end_port_1)
        df_test = get_hfc_population(spark, closing_day, end_port_2)
    elif tech == 'adsl':
        df_train = get_adsl_population(spark, starting_day, end_port_1)
        df_test = get_hfc_population(spark, closing_day, end_port_2)

    train_rate = df_train.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']
    test_rate = df_test.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']
    print'Churn rate for {} (num_cliente) for date {}: {}'.format(tech, starting_day, train_rate)
    print'Churn rate for {} (num_cliente) for date {}: {}'.format(tech, closing_day, test_rate)

    feats_ = [f for f in df_train.columns if 'max_' in f or 'min_' in f or 'mean_' in f or 'std_' in f]
    feats = [f for f in feats_ if 'enable' not in f]
    assembler = VectorAssembler(inputCols=feats, outputCol="features")
    from churn.analysis.poc_segments.poc_modeler import get_feats_imp, get_model, get_score, get_metrics

    stages = [assembler]
    mymodel = get_model('rf')
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(df_train.fillna(0.0))

    df_tr_preds = get_score(pipeline_model, df_train.fillna(0.0))
    get_metrics(df_tr_preds, title="TRAIN", nb_deciles=None, score_col="model_score", label_col="label", refprevalence=0.1)

    feat_importance_list = get_feats_imp(pipeline_model, feats)

    df_tt_car_preds = get_score(pipeline_model, df_test.fillna(0.0))

    get_metrics(df_tt_car_preds, title="TEST CAR", nb_deciles=None, score_col="model_score", label_col="label", refprevalence=0.1)

