#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import collect_set, col, count, sum as sql_sum, lit, log, log10, log2
from pyspark.sql import Window
from pyspark.ml import Pipeline
import numpy as np
import pandas as pd

# 1. Feature selection for numerical attributes

# 1.1. Feature selection based on AUC

def feature_selection_auc(df, threshold, cols_to_filter, label_col = 'label', pcg = 1.0):

    df = df.select(cols_to_filter + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_numeric_columns(df, label_col=label_col)

    input_cols = list(set(df.columns) - set([label_col]))

    for f in input_cols:
        df = df.withColumn(f, col(f).cast('double'))

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    from churn_nrt.src.projects_utils.models.modeler import get_auc

    auc_results = [(f, get_auc(df, score_col=f, label_col=label_col)) for f in input_cols]

    auc_results = sorted(auc_results, key=lambda tup: tup[1], reverse=True)

    for (f, r) in auc_results:
        print "[Info] " + f + ": " + str(r)

    n = threshold if (threshold >= 1) else round(threshold * len(auc_results))

    num_cols = [f[0] for f in auc_results][0:n]

    return num_cols

# 1.2. Removing correlated features

def feature_selection_correlation(df, cols_to_filter, threshold=0.8, label_col = 'label', pcg = 1.0):

    cols_to_filter = list(set(cols_to_filter) - set([label_col]))

    df = df.select(cols_to_filter).sample(withReplacement=False, fraction=pcg)

    df = only_numeric_columns(df)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    cols_to_filter = list(set(df.columns) - set([label_col]))

    from pyspark.ml.stat import Correlation

    print('[Info] Calculating correlation for ' + str(len(cols_to_filter)) + ' variables')

    assembler = VectorAssembler(inputCols=cols_to_filter, outputCol='features')

    output_data = assembler.transform(df)

    r1 = Correlation.corr(output_data, 'features')

    corr_matrix = r1.head()[0].toArray()

    corr_df = pd.DataFrame(data=corr_matrix, index=cols_to_filter, columns=cols_to_filter)

    corr_df = corr_df.abs()

    upper = corr_df.where(np.triu(np.ones(corr_df.shape), k=1).astype(np.bool))

    vars_to_drop = [c for c in upper.columns if any(upper[c] > threshold)]

    valid_vars = list(set(cols_to_filter) - set(vars_to_drop))

    return valid_vars

# 1.3. feature selection from varimp of a RF

def feature_selection_rf(df, threshold, cols_to_filter, label_col = 'label', pcg = 1.0):
    """
    Selects numerical variables based on their importance in a Random Forest model.
    :param df: IDS df
    :param threshold: Threshold importance. If importance < 1, this function will select the variables that exceed this
    importance threshold. If threshold > 1, then the N (N = threshold) most important variables will be selected
    :param cols_to_filter: List containing the name of the numerical variables to filter
    :param label_col: column containing the target
    :param pcg: percenage of the data (rows in df) to e used
    :return: A list with the name of the numerical variables that have passed the filter
    """
    print("[Info] Feature selection by Random Forest may take a long time")

    df = df.select(cols_to_filter + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_numeric_columns(df, label_col = label_col)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    input_cols = list(set(df.columns) - set([label_col]))

    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

    numTrees, maxDepth, minInstancesPerNode, maxBins, subsamplingRate, maxIter = param_selection(df)

    rf_model = RandomForestClassifier(numTrees=numTrees, maxDepth=maxDepth,
                                      minInstancesPerNode=minInstancesPerNode,
                                      maxBins=maxBins, featureSubsetStrategy='auto', minInfoGain=0.0,
                                      impurity='gini', subsamplingRate=subsamplingRate, labelCol = label_col)\

    pipeline = Pipeline(stages=[assembler, rf_model])

    pipeline_model = pipeline.fit(df)

    from churn_nrt.src.projects_utils.models.modeler import getOrderedRelevantFeats

    feat_imp_nrt = getOrderedRelevantFeats(pipeline_model, input_cols, "f")

    n = threshold if(threshold >=1) else round(threshold*len(feat_imp_nrt))

    num_cols = [f[0] for f in feat_imp_nrt][0:n]

    return num_cols

# 1.4. feature selection from varimp of a GBT

def feature_selection_gbt(df, threshold, cols_to_filter, label_col = 'label', pcg = 1.0):
    """
    Selects numerical variables based on their importance in a Gradient Boosting model.
    :param df: IDS df
    :param threshold: Threshold importance. If importance < 1, this function will select the variables that exceed this
    importance threshold. If threshold > 1, then the N (N = threshold) most important variables will be selected
    :param cols_to_filter: List containing the name of the numerical variables to filter
    :return: A list with the name of the numerical variables that have passed the filter
    """
    print("[Info] feature selection by Gradient Boosting may take a long time")

    df = df.select(cols_to_filter + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_numeric_columns(df, label_col=label_col)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    input_cols = list(set(df.columns) - set([label_col]))

    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

    numTrees, maxDepth, minInstancesPerNode, maxBins, subsamplingRate, maxIter = param_selection(df)

    gb_model = GBTClassifier(maxDepth=maxDepth, minInstancesPerNode=minInstancesPerNode, maxBins=maxBins,
                             subsamplingRate=subsamplingRate, maxIter=maxIter, stepSize=0.1,
                             minInfoGain=0.0, lossType='logistic', labelCol = label_col)\

    pipeline = Pipeline(stages=[assembler, gb_model])

    pipeline_model = pipeline.fit(df)

    from churn_nrt.src.projects_utils.models.modeler import getOrderedRelevantFeats

    feat_imp_nrt = getOrderedRelevantFeats(pipeline_model, input_cols, "f")

    n = threshold if (threshold >= 1) else round(threshold * len(feat_imp_nrt))

    num_cols = [f[0] for f in feat_imp_nrt][0:n]

    return num_cols

# 1.5. feature selection from the coeffs of a LR trained with L1 regularization

def feature_selection_lr(df, threshold, cols_to_filter, label_col='label', pcg = 1.0):
    """
    Selects numerical variables based on their coefficient obtained in a Linear Regression model.
    :param df: IDS df
    :param threshold: Threshold coefficient that have to be exceeded
    :param cols_to_filter: List containing the name of the numerical variables to filter
    :return: A list with the name of the numerical variables that have passed the filter
    """
    print("[WARNING] Feature selection by Logistic Regression may take a long time")

    df = df.select(cols_to_filter + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_numeric_columns(df, label_col=label_col)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    # Balancing the classes for LR training

    from churn_nrt.src.projects_utils.models.modeler import balance_df

    df = balance_df(df, label=label_col)

    input_cols = list(set(df.columns) - set([label_col]))

    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
    df = assembler.transform(df)
    model = LogisticRegression(elasticNetParam=1, standardization=True, labelCol=label_col).fit(df)

    coeffs = zip(input_cols, [abs(c) for c in model.coefficients.toArray()])

    coeffs = sorted(coeffs, key=lambda tup: tup[1], reverse=True)

    for (f, c) in coeffs:
        print "[Info] " + f + ": " + str(c)

    n = threshold if (threshold >= 1) else round(threshold * len(coeffs))

    num_cols = [f[0] for f in coeffs][0:n]

    return num_cols

# 2. Feature selection for categorical attributes (ChiSquare test and Information Gain)

# 2.1. Feature selection from Chi-square test

def feature_selection_chi_squared(df, string_cols, threshold = 0.05, label_col = 'label', pcg = 1.0):
    """
    Selects categorical variables based on a Chi-Squared test.
    :param df: IDS df
    :param threshold: Threshold p-value that do not have to be exceeded
    :param string_cols: List containing the name of the categorical variables to filter
    :return: A list with the name of the numerical categorical that have passed the filter
    """

    df = df.select(string_cols + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_categorical_columns(df, label_col=label_col)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    string_cols = list(set(df.columns) - set([label_col]))

    print('[INFO] Indexing categorical variables: ' + str(len(string_cols)))
    new_df = df
    stages = []
    for c in string_cols:
        new_df = new_df.withColumnRenamed(c, c + '_raw')  # store old columns with suffix _raw
        indexer = StringIndexer(inputCol=(c + '_raw'), outputCol=c)
        stages += [indexer]

    pipeline = Pipeline(stages=stages)
    new_df = pipeline.fit(new_df).transform(new_df)  # new_df contains also indexed string vars

    # output: valid_vars and not_pred_vars
    # Second pipeline: assembler for chi-squared
    assembler_input = string_cols
    assembler = VectorAssembler(inputCols=assembler_input, outputCol='features')
    stages = [assembler]

    pipeline = Pipeline(stages=stages)
    output_data = pipeline.fit(new_df).transform(new_df).select('features', label_col)

    # Third pipeline: chi-squared selector
    selector = ChiSqSelector(fpr=threshold, featuresCol='features', outputCol='selectedFeatures', labelCol=label_col)

    print('[INFO] Calculating Chi-squared test categorical variables: ' + str(len(string_cols)))
    result_df = selector.fit(output_data).transform(output_data)

    result_df.show()

    print "[Info] Result DF showed above"

    # Extract variables that related to label according to the test
    list_extract = []
    for i in result_df.schema['selectedFeatures'].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + result_df.schema['selectedFeatures'].metadata["ml_attr"]["attrs"][i]

    varlist = pd.DataFrame(list_extract)

    # Drop independent categorical variables
    if varlist.empty:
        cat_cols = []
    else:
        cat_cols = list(varlist['name'])

    return cat_cols

# 2.2. Feature selection based on Information Gain

def feature_selection_information_gain(df, string_cols, threshold = 0.01, label_col = 'label', pcg = 1.0):
    """
     Selects categorical variables based on their information gain.
     :param df: IDS df
     :param threshold: percentage of reduction wrt initial entropy Ht (Ht is small in clearly unbalanced datasets)
     :param string_cols: List containing the name of the categorical variables to filter
     :return: A list with the name of the numerical categorical that have passed the filter
     """

    df = df.select(string_cols + [label_col]).sample(withReplacement=False, fraction=pcg)

    df = only_categorical_columns(df, label_col=label_col)

    df.cache()

    print "[Info] Number of rows in the DF: " + str(df.count())

    string_cols = list(set(df.columns) - set([label_col]))

    # First pipeline: string indexer variables -> necessary to use them in models
    print('[INFO] Indexing categorical variables: ' + str(len(string_cols)))

    ig_df = information_gain(df=df, var_list=string_cols, label_col = label_col)

    cat_cols = ig_df\
        .filter(col('ig') >= (threshold)*col('init_entropy'))\
        .select('feature').rdd.map(lambda r: r['feature']).collect()

    # [ig[0] for ig in ig_results if (ig[1] >= threshold_abs)]

    return cat_cols

# Information gain utils

def single_entropy(df, var):
    """
    Calculate the entropy of one variable
    :param df: dataframe from IDS
    :param var: target variable over which to calculate entropy
    :return: entropy of the variable
    """

    entropy_ = df.groupBy(var).agg(count("*").alias('num_entries')) \
        .withColumn('all', lit('all')) \
        .withColumn('total_num_entries', sql_sum('num_entries').over(Window.partitionBy('all'))) \
        .withColumn('pcg', col('num_entries') / col('total_num_entries')) \
        .select(var, 'pcg') \
        .withColumn('entropy_term', -col('pcg') * log('pcg')) \
        .select(sql_sum('entropy_term').alias('entropy')).first()['entropy']

    return entropy_


def conditional_entropy(df, var, var_t):
    """
    Calculate the conditional entropy between two variables
    :param df: dataframe from IDS
    :param var: variable (not target) over which to compute entropy
    :param var_t: target variable
    :param ti: categories in target variable
    :param n: number of rows in df
    :param log_base: log base to use
    :return: conditional entropy of the two variables
    """
    row_list = df \
        .groupBy(var) \
        .agg(count("*").alias('num_entries')) \
        .withColumn('all', lit('all')) \
        .withColumn('total_num_entries', sql_sum('num_entries').over(Window.partitionBy('all'))) \
        .withColumn('pcg', col('num_entries') / col('total_num_entries')) \
        .select(var, 'pcg').collect()

    cat_and_weight = [(r[var], r['pcg']) for r in row_list]

    return sum([w * single_entropy(df=df.filter(col(var) == c), var=var_t) for (c, w) in cat_and_weight])


def information_gain(df, var_list, label_col='label'):
    """
    Calculate Information Gain between two variables
    :param df: dataframe from IDS
    :param var_list: list of variables over which to calculate entropy
    :param log_base: log base to use (default = 2)
    :return: list of variables the have higher information gain than threshold
    """

    df = df.select(var_list + [label_col])

    df.cache()

    print "[Info] Information gain - Cached DF for the computation of IG - Size: " + str(df.count())

    Ht = single_entropy(df=df, var=label_col)

    print "[Info] Information gain - Initial value of entropy: " + str(Ht)

    ig_results = [(v, Ht - conditional_entropy(df=df, var=v, var_t=label_col)) for v in var_list]

    for ig in ig_results:
        print "[Info] IG for variable " + ig[0] + ": " + str(ig[1])

    result_df = spark.createDataFrame(ig_results, ['feature', 'ig']).withColumn('init_entropy', lit(Ht))

    return result_df

# Other utils

def param_selection(df):
    """
    Selects parameters values based on the size of the dataset.
    :param df: IDS df
    :return: An array containing the values of the different parameters
    """
    n = df.count()
    numTrees = np.round(np.log10(n) * 100)
    maxDepth = np.round(np.log(n))
    minInstancesPerNode = np.round(np.log10(n) * (np.ceil(n / 500000) + 1))
    #maxBins = np.minimum(80, np.round(500 / np.log(n)))
    subsamplingRate = float(np.where(n > 500000, 0.6, 0.8))
    maxIter = np.round(np.log10(n) * 50)

    # minInstancesPerNode

    minInstancesPerNode = 200 if minInstancesPerNode > 200 else maxDepth
    minInstancesPerNode = 25 if minInstancesPerNode < 25 else minInstancesPerNode

    # maxDepth

    maxDepth = 15 if maxDepth > 15 else maxDepth
    maxDepth = 3 if maxDepth < 3 else maxDepth

    # maxIter applies to GBT

    maxIter = 200 if maxIter > 100 else maxIter
    maxIter = 50 if maxIter < 50 else maxIter

    # maxBins set to 32

    maxBins = 32

    print "[Info] numTrees: " + str(numTrees)
    print "[Info] maxDepth: " + str(maxDepth)
    print "[Info] minInstancesPerNode: " + str(minInstancesPerNode)
    print "[Info] maxBins: " + str(maxBins)
    print "[Info] subsamplingRate: " + str(subsamplingRate)
    print "[Info] maxIter: " + str(maxIter)

    return numTrees, maxDepth, minInstancesPerNode, maxBins, subsamplingRate, maxIter

def only_numeric_columns(df, label_col = None, id_cols = None):
    cols_and_types = df.dtypes

    num_types = ['int', 'bigint', 'long', 'double', 'float']

    num_cols = [c[0] for c in(cols_and_types) if c[1] in num_types]

    num_cols_and_label = list(set(num_cols + [label_col])) if(label_col) else num_cols

    sel_cols = (num_cols_and_label + id_cols) if(id_cols) else num_cols_and_label

    result_df = df.select(list(set(sel_cols)))

    return result_df

def only_categorical_columns(df, label_col = None, id_cols = None):
    cols_and_types = df.dtypes

    cat_types = ['string', 'bool']

    cat_cols = [c[0] for c in(cols_and_types) if c[1] in cat_types]

    cat_cols_and_label = list(set(cat_cols + [label_col])) if(label_col) else cat_cols

    sel_cols = (cat_cols_and_label + id_cols) if(id_cols) else cat_cols_and_label

    result_df = df.select(list(set(sel_cols)))

    return result_df

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

    if(method_.lower() == 'ig'):

        # Testing Information Gain

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
            .read \
            .parquet(
            '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
            .filter(col("serv_rgu") == 'mobile')

        # Getting a subset of 10 numeric columns: gnv_data_L2

        all_cols = ids.columns

        gnv_data = [c for c in all_cols if (c.startswith("GNV_Data_L2"))][0:10]

        # Categorical columns

        cat_cols = ['Cust_L2_SUPEROFERTA_proc', 'Cust_Agg_seg_pospaid_nif', 'Cust_L2_nacionalidad_is_spain']

        # Cols to select

        sel_cols = ['msisdn'] + gnv_data + cat_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort
        target_df = MobPort(spark, churn_window=60)\
            .get_module(date_, save=False, save_others=False, force_gen=True)\
            .withColumnRenamed('label_mob', 'label')\
            .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        #feats = information_gain(ids_lab, cat_cols, 5.0)

        feats = feature_selection_information_gain(ids_lab, cat_cols, threshold = 1.0, label_col='label')

        for f in feats:
            print "[Info] Testing IG - Feat to be retained: " + f

    if(method_.lower() == 'chi'):

        # Testing ChiSquare test

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
            .read \
            .parquet('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
            .filter(col("serv_rgu") == 'mobile')

        # Getting a subset of 10 numeric columns: gnv_data_L2

        all_cols = ids.columns

        gnv_data = [c for c in all_cols if (c.startswith("GNV_Data_L2"))][0:10]

        # Categorical columns

        cat_cols = ['Cust_L2_SUPEROFERTA_proc', 'Cust_Agg_seg_pospaid_nif', 'Cust_L2_nacionalidad_is_spain']

        # Cols to select

        sel_cols = ['msisdn'] + gnv_data + cat_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
            .get_module(date_, save=False, save_others=False, force_gen=True) \
            .withColumnRenamed('label_mob', 'label') \
            .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_chi_squared(ids_lab, cat_cols, threshold = 0.0005, label_col = 'label')

        for f in feats:
            print "[Info] Testing ChiSq - Feat to be retained: " + f

    if(method_.lower() == 'gbt'):


        # Testing GBT

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
            .read \
            .parquet(
            '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
            .filter(col("serv_rgu") == 'mobile')

        # Getting numeric columns

        sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

        from src.main.python.utils.general_functions import get_all_metadata

        date_ = "20200114"
        final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

        numeric_cols = numeric_map.keys()

        sel_cols = ['msisdn'] + numeric_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
            .get_module("20200114", save=False, save_others=False, force_gen=True) \
            .withColumnRenamed('label_mob', 'label') \
            .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_gbt(ids_lab, 50, numeric_cols, label_col = 'label', pcg=0.25)

        for f in feats:
            print "[Info] Testing RF - Feat to be retained: " + f

    if (method_.lower() == 'rf'):

        # Testing RF

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
            .read \
            .parquet('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
            .filter(col("serv_rgu") == 'mobile')

        # Getting numeric columns

        sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

        from src.main.python.utils.general_functions import get_all_metadata

        date_ = "20200114"
        final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

        numeric_cols = numeric_map.keys()

        sel_cols = ['msisdn'] + numeric_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
            .get_module("20200114", save=False, save_others=False, force_gen=True) \
            .withColumnRenamed('label_mob', 'label') \
            .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_rf(ids_lab, 50, numeric_cols, label_col='label', pcg=0.25)

        for f in feats:
            print "[Info] Testing RF - Feat to be retained: " + f

    if (method_.lower() == 'lr'):

        # Testing LR + regularization

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
            .read \
            .parquet('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
            .filter(col("serv_rgu") == 'mobile')

        # Getting numeric columns

        sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

        from src.main.python.utils.general_functions import get_all_metadata

        final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

        numeric_cols = numeric_map.keys()

        sel_cols = ['msisdn'] + numeric_cols

        #l2_tgs_cols = [c for c in numeric_cols if (('_L2_' in c) | ('tgs' in c))]

        #sel_cols = ['msisdn'] + l2_tgs_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
            .get_module(date_, save=False, save_others=False, force_gen=True) \
            .withColumnRenamed('label_mob', 'label') \
            .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_lr(ids_lab, 50, numeric_cols, label_col='label', pcg=0.25)

        for f in feats:
            print "[Info] Testing LR - Feat to be retained: " + f

    if (method_.lower() == 'auc'):

        # Testing AUC

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
                .read \
                .parquet(
                '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
                .filter(col("serv_rgu") == 'mobile')

        # Getting numeric columns

        sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

        from src.main.python.utils.general_functions import get_all_metadata

        date_ = "20200114"
        final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

        numeric_cols = numeric_map.keys()

        l2_tgs_cols = [c for c in numeric_cols if(('_L2_' in c) | ('tgs' in c))]

        sel_cols = ['msisdn'] + l2_tgs_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
                .get_module("20200114", save=False, save_others=False, force_gen=True) \
                .withColumnRenamed('label_mob', 'label') \
                .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_auc(ids_lab, 50, l2_tgs_cols, label_col = 'label', pcg = 0.25)

        for f in feats:
            print "[Info] Testing AUC - Feat to be retained: " + f

    if (method_.lower() == 'correlation'):

        # Testing AUC

        # Loading IDS for mobile services

        date_ = "20200114"
        year_ = str(int(date_[0:4]))
        month_ = str(int(date_[4:6]))
        day_ = str(int(date_[6:8]))

        ids = spark \
                .read \
                .parquet(
                '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/1.1.0/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
                .filter(col("serv_rgu") == 'mobile')

        # Getting numeric columns

        sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

        from src.main.python.utils.general_functions import get_all_metadata

        date_ = "20200114"
        final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata(date_)

        numeric_cols = numeric_map.keys()

        l2_tgs_cols = [c for c in numeric_cols if(('_L2_' in c) | ('tgs' in c))]

        sel_cols = ['msisdn'] + l2_tgs_cols

        # Labeling

        from churn_nrt.src.data.sopos_dxs import MobPort

        target_df = MobPort(spark, churn_window=60) \
                .get_module("20200114", save=False, save_others=False, force_gen=True) \
                .withColumnRenamed('label_mob', 'label') \
                .select('msisdn', 'label')

        ids_lab = ids.select(sel_cols).join(target_df, ['msisdn'], 'left').na.fill({'label': 0.0})

        feats = feature_selection_correlation(ids_lab, l2_tgs_cols, threshold=0.5)

        print "[Info] Initial set: " + str(len(l2_tgs_cols)) + " - Reduced set: " + str(len(feats))

        for f in feats:
            print "[Info] Testing Correlation - Feat to be retained: " + f

