#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql.functions import col, collect_set, when, udf
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline

def filter_and_label(df, top_ratio=0.25, column='model_score'):
    """
    Filters and labels a df based on the distribution (bottom and top quantiles) of a specified column, assigning 0's to
    the observations which belong to the bottom quantile and 1's to the observations which belong to the top quantile.
    :param df: Df for filtering and labeling
    :param top_ratio: Ratio for the calculation of the bottom and top quantiles
    :param column: Column for the calculation of the bottom and top quantiles
    :return: A filtered and labeled df
    """

    bottom, top = df.approxQuantile(column, probabilities=[top_ratio, 1 - top_ratio], relativeError=0)

    filtered_df = df.filter((col(column) <= bottom) | (col(column) >= top))

    labeled_df = filtered_df.withColumn('label', when(col(column) <= bottom, 0).otherwise(1))

    print('[Info] Data filtered and labeled with success')

    return labeled_df


def top_and_bottom_preparation(df, important_variables, score_col = 'model_score', top_ratio=0.25):
    """
    Prepares data to performance the explicability of a specified model.
    :param df: Df for preparing
    :param important_variables: List containing the name of the most important variables of the model
    :param top_ratio: Ratio for the calculation of the bottom and top quantiles
    :return: A Pandas df ready to performance the explicability and a list containing the name of the categorical
    columns that will be used in the next process
    """

    # Filter and label by probability distribution
    labeled_df = filter_and_label(df, top_ratio=top_ratio, column=score_col)\
        .select(important_variables + ['label'])

    # Select categorical columns
    cat_cols = [c for (c, c_type) in labeled_df.dtypes if c_type in ('string')]

    # Transform to Pandas df
    print('[Info] Pandas Dataframe transformation size: ' + str(labeled_df.count()*len(labeled_df.columns)*2) + ' bytes')

    prepared_df = labeled_df.toPandas()

    print('[Info] Data ready for explicability of the model')

    return prepared_df, cat_cols

def extract_rules_from_model(spark, df, cat_cols, method, threshold):
    """
    Function to select which method to use and then carry out
    :param spark: spark session
    :param df: dataframe that contains most important variables
    :param cat_cols: list of categorical variables
    :param method: method to use, 'dt' in the case of decision tree and 'lr' in the case of logistic regression
    :param threshold: threshold to use according to the method selected (number of samples in a single class in
        a node for decision tree and threshold for coefficients for logistic regression)
    :return: saved json files with results
    """

    if method == 'dt':
        print('[Info] Selecting top nodes')
        from churn_nrt.src.projects_utils.models.decision_tree_profiling import select_top_nodes, parse_dt_rules
        results = select_top_nodes(df, cat_cols, threshold=threshold)
        pattern = parse_dt_rules(spark, results)

    if method == 'lr':
        print('[Info] Selecting top coefficients')
        from churn_nrt.src.projects_utils.models.logistic_regression_profiling import select_top_coefficients, parse_lr_rules
        df = select_top_coefficients(df, cat_cols, threshold=threshold)
        pattern = parse_lr_rules(spark, df)

    return pattern


def get_model_pattern(spark, df, important_variables, method = 'dt', score_col ='model_score', top_ratio=0.15, threshold = 5):
    """
    Orchestrates processes for pattern capture
    :param spark: spark session
    :param df: training set (including the column with the prediction of the model for each of the training samples) of the model that is under analysis
    :param important_variables: List containing the name of the most important variables of the model under analysis
    :param method: method to use 'dt' (decision tree) or 'lr' (logistic regression) in pattern analysis
    :param score_col: column in df containing the model predictions
    :param top_ratio: Bottom and top percentages used for pattern analysis (the process is based on the comparison
    between top top_ratio% and bottom top_ratio%); top ratio must be in (0, 0.5):
    a higher value results in a larger number of samples for modelling but a lower resolution of the pattern
    :param threshold: Number of rules used to define the pattern (if < 1, it is interpreted as the percentage of rules to obtain as output out of the whole set of rules)
    """

    # Step 1: Prepare data to fit a explainable model
    prepared_df, cat_cols = top_and_bottom_preparation(df, important_variables, score_col = score_col, top_ratio = top_ratio)

    # Step 2: Fit an explainable model
    pattern = extract_rules_from_model(spark=spark, df=prepared_df, cat_cols=cat_cols, method=method, threshold=threshold)

    print("Process successfully finished")

    return pattern

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

if __name__ == '__main__':

    set_paths()

    ############### 0. Spark #################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("flow_analysis")

    method_ = sys.argv[1]

    ratio_ = float(sys.argv[2])

    num_imp_vars = int(sys.argv[3])

    threshold = float(sys.argv[4])

    # Loading IDS for mobile services

    date_ = "20200121"
    year_ = str(int(date_[0:4]))
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))

    print "[Info] Working with IDS: " + '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_

    ids = spark \
        .read \
        .parquet(
        '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
        .filter(col("serv_rgu") == 'mobile')

    # Getting numeric columns

    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

    from src.main.python.utils.general_functions import get_all_metadata

    final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

    numeric_cols = numeric_map.keys()

    l2_tgs_cols = [c for c in numeric_cols if (('_L2_' in c) | ('tgs' in c))]

    import random

    #l2_tgs_cols = random.sample(l2_tgs_cols, 10)

    sel_cols = ['msisdn'] + l2_tgs_cols

    # Labeling

    from churn_nrt.src.data.sopos_dxs import MobPort

    target_df = MobPort(spark, churn_window=60) \
        .get_module(date_, save=False, save_others=False, force_gen=True) \
        .withColumnRenamed('label_mob', 'label') \
        .select('msisdn', 'label')

    ids_lab = ids \
        .select(sel_cols) \
        .join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0}) \
        .sample(withReplacement=False, fraction=0.3)

    # Modelling

    stages = []

    assembler = VectorAssembler(inputCols=l2_tgs_cols, outputCol="features")
    stages += [assembler]

    model_ = RandomForestClassifier(featuresCol='features',
                                    numTrees=20,
                                    maxDepth=10,
                                    labelCol="label",
                                    seed=1234,
                                    maxBins=32,
                                    minInstancesPerNode=20,
                                    impurity='gini',
                                    featureSubsetStrategy='sqrt',
                                    subsamplingRate=0.7)

    stages += [model_]

    pipeline = Pipeline(stages=stages)

    pipeline_model = pipeline.fit(ids_lab)

    # Relevant feats

    from churn_nrt.src.projects_utils.models.modeler import getOrderedRelevantFeats

    feat_imp_nrt = getOrderedRelevantFeats(pipeline_model, l2_tgs_cols, "f")

    imp_vars = [f[0] for f in feat_imp_nrt][0:num_imp_vars]

    # Preds on tr data

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    tr_preds = pipeline_model \
        .transform(ids_lab) \
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType()))

    pattern = get_model_pattern(spark, tr_preds, imp_vars, method_, score_col='model_score', top_ratio=ratio_, threshold = threshold)

    for p in pattern:
        print "[Info] " + p
