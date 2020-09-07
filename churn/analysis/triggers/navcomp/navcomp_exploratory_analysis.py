
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
                                   randn)

from pyspark.sql.window import Window

def get_auc(spark, df, feat_cols):

    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    name_label = 'label'

    feature_list = []
    auc_list = []
    dir_list = []

    for name_ in df.select(feat_cols).columns:

        df = df.withColumn(name_, col(name_).cast('double'))

        evaluator = BinaryClassificationEvaluator(rawPredictionCol=name_, labelCol=name_label, metricName='areaUnderROC')
        auc_tmp = evaluator.evaluate(df)
        auc = (1 - auc_tmp) if (auc_tmp < 0.5) else auc_tmp
        direction = 'negative' if (auc_tmp < 0.5) else 'positive'

        feature_list.append(name_)
        auc_list.append(auc)
        dir_list.append(direction)
        print("[Info navcomp exploratory] Feature: {} - AUC: {}".format(name_, auc))

    data = {'feature': feature_list, 'auc': auc_list, 'direction': dir_list}

    import pandas as pd

    df = spark.createDataFrame(pd.DataFrame(data, columns=data.keys()))

    return df


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

    #sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    #sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    set_paths()

    # Reading from mini_ids. The following imports would be required for the automated process (computing features on the fly)
    #from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
    #from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_attributes_df

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    ##########################################################################################
    # 1. Getting input argument:
    #      - date_: reference date for the analysis
    ##########################################################################################

    date_ = sys.argv[1]

    from churn.analysis.triggers.navcomp.navcomp_utils import get_labeled_set_msisdn

    from churn.analysis.triggers.base_utils.base_utils import get_active_filter, get_disconnection_process_filter, get_churn_call_filter

    # Loading the initial set of customers who navigate through competitors websites

    initial_set_df = get_labeled_set_msisdn(spark, date_)

    from churn.analysis.triggers.navcomp.navcomp_model import filter_population

    initial_set_df = filter_population(spark, initial_set_df, 'comps')

    size_initial_set = initial_set_df.select('msisdn').distinct().count()

    # Modeling filters

    active_filter = get_active_filter(spark, date_, 90)

    disconnection_filter = get_disconnection_process_filter(spark, date_, 90)

    churn_call_filter = get_churn_call_filter(spark, date_, 90, 'msisdn')

    # Quantifying the effect of the modeling filters

    active_filter_df = initial_set_df.join(active_filter, ['msisdn'], 'inner')

    size_after_active_filter = active_filter_df.select('msisdn').distinct().count()

    disconnection_filter_df = initial_set_df.join(disconnection_filter, ['nif_cliente'], 'inner')

    size_after_disconnection_filter = disconnection_filter_df.select('msisdn').distinct().count()

    churn_call_filter_df = initial_set_df.join(churn_call_filter, ['msisdn'], 'inner')

    size_after_churn_call_filter = churn_call_filter_df.select('msisdn').distinct().count()

    print '[Info navcomp_exploratory] Initial set: ' + str(size_initial_set)

    print '[Info navcomp_exploratory] After active filter: ' + str(size_after_active_filter)

    print '[Info navcomp_exploratory] After disconnection filter: ' + str(size_after_disconnection_filter)

    print '[Info navcomp_exploratory] After churn call filter: ' + str(size_after_churn_call_filter)

    tr_set = initial_set_df\
        .join(active_filter, ['msisdn'], 'inner')\
        .join(disconnection_filter, ['nif_cliente'], 'inner')\
        .join(churn_call_filter, ['msisdn'], 'inner')

    tr_set = tr_set\
        .withColumn('blindaje', when(col('tgs_days_until_fecha_fin_dto') > 60, 'hard').otherwise(when((col('tgs_days_until_fecha_fin_dto') <= 60) & (col('tgs_days_until_fecha_fin_dto') > 0), 'soft').otherwise(lit('none'))))\

    tr_set.groupBy('blindaje').agg(sql_avg('label').alias('churn_rate'), count('*').alias('num_servs')).show()

    print '[Info navcomp_exploratory] Stats for blindaje segments showed above - Date: ' + str(date_)

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown"]

    for c in competitors:
        tr_set = tr_set.withColumn('flag_' + c, when(col(c + '_sum_count') > 0, 1).otherwise(lit(0)))

    tr_set.cache()

    global_churn_rate = tr_set.select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    group_cols = ['flag_' + c for c in competitors] + ['blindaje']

    #tr_set.withColumn('segment_all', lit('all')).withColumn('total_volume', count('*').over(Window.partitionBy('segment_all'))).groupBy(*group_cols).agg(sql_avg('label').alias('churn_rate'), count('*').alias('num_servs')).orderBy(desc('num_servs')).show()

    tr_set\
        .withColumn('segment_all', lit('all'))\
        .withColumn('total_volume', count('*').over(Window.partitionBy('segment_all')))\
        .withColumn('churn_rate', sql_avg('label').over(Window.partitionBy(*group_cols)))\
        .withColumn('num_servs', count('*').over(Window.partitionBy(*group_cols)))\
        .withColumn('segment_weight', col('num_servs').cast('double')/col('total_volume').cast('double'))\
        .withColumn('churn_weight', col('churn_rate')*col('segment_weight'))\
        .withColumn('total_churn', sql_avg('label').over(Window.partitionBy('segment_all')))\
        .select(group_cols + ['total_volume', 'total_churn', 'num_servs', 'churn_rate', 'segment_weight', 'churn_weight']).distinct()\
        .orderBy(desc('churn_weight')).show(500, False)

    print '[Info navcomp_exploratory] Stats for blindaje & competitor segments showed above - Date: ' + str(date_)

    # Individual evaluation of the input features

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    metadata = get_metadata(spark)

    numeric_columns = metadata.filter((col('type') == 'numeric') & (col('source').isin('customer', 'ccc', 'spinners', 'navcomp'))).rdd.map(lambda x: x['feature']).collect()

    print "[Info navcomp exploratory] Evaluating the individual performance of each feature - Total problem"

    auc_df = get_auc(spark, tr_set, numeric_columns)

    auc_df.orderBy(desc('auc')).show(200, False)

    '''

    ##########################################################################################
    # 2. Loading the dataset
    # ##########################################################################################

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    base_df = get_churn_target_nif(spark, date_, churn_window=30)

    nb_nifs_base = base_df.count()

    churn_base = base_df.select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    from churn.analysis.triggers.navcomp.navcomp_utils import get_labeled_set

    navcomp_df = get_labeled_set(spark, date_)

    nb_nifs_total = navcomp_df.count()

    churn_all = navcomp_df.select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_nocancel_ccc_w8 = navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).count()

    churn_nocancel_ccc_w8 = navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_cancel_ccc_w8 = navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).count()

    churn_cancel_ccc_w8 = navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    print "[Info navcomp exploratory] Number of NIFs (base): " + str(nb_nifs_base) + " - Churn rate (base): " + str(churn_base)

    print "[Info navcomp exploratory] Number of NIFs (total population): " + str(nb_nifs_total) + " - Churn rate (total population): " + str(churn_all)

    print "[Info navcomp exploratory] Number of NIFs (no cancel CCC w8): " + str(nb_nifs_nocancel_ccc_w8) + " - Churn rate (no cancel CCC w8): " + str(churn_nocancel_ccc_w8)

    print "[Info navcomp exploratory] Number of NIFs (cancel CCC w8): " + str(nb_nifs_cancel_ccc_w8) + " - Churn rate (cancel CCC w8): " + str(churn_cancel_ccc_w8)

    navcomp_onlycomps_df = navcomp_df.filter((col('sum_count_vdf')==0) & (col('sum_count_comps')>0))

    nb_nifs_onlycomps = navcomp_onlycomps_df.count()

    churn_onlycomps = navcomp_onlycomps_df.select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_onlycomps_nocancel_ccc_w8 = navcomp_onlycomps_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).count()

    churn_onlycomps_nocancel_ccc_w8 = navcomp_onlycomps_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_onlycomps_cancel_ccc_w8 = navcomp_onlycomps_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).count()

    churn_onlycomps_cancel_ccc_w8 = navcomp_onlycomps_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    print "[Info navcomp exploratory] Number of NIFs (only competitors): " + str(nb_nifs_onlycomps) + " - Churn rate (only competitors): " + str(churn_onlycomps)

    print "[Info navcomp exploratory] Number of NIFs (no cancel CCC w8, only competitors): " + str(nb_nifs_onlycomps_nocancel_ccc_w8) + " - Churn rate (no cancel CCC w8, only competitors): " + str(churn_onlycomps_nocancel_ccc_w8)

    print "[Info navcomp exploratory] Number of NIFs (cancel CCC w8, only competitors): " + str(nb_nifs_onlycomps_cancel_ccc_w8) + " - Churn rate (cancel CCC w8, only competitors): " + str(churn_onlycomps_cancel_ccc_w8)

    navcomp_onlyvdf_df = navcomp_df.filter((col('sum_count_vdf') > 0) & (col('sum_count_comps') == 0))

    nb_nifs_onlyvdf = navcomp_onlyvdf_df.count()

    churn_onlyvdf = navcomp_onlyvdf_df.select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_onlyvdf_nocancel_ccc_w8 = navcomp_onlyvdf_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).count()

    churn_onlyvdf_nocancel_ccc_w8 = navcomp_onlyvdf_df.filter(col('CHURN_CANCELLATIONS_w8') == 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    nb_nifs_onlyvdf_cancel_ccc_w8 = navcomp_onlyvdf_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).count()

    churn_onlyvdf_cancel_ccc_w8 = navcomp_onlyvdf_df.filter(col('CHURN_CANCELLATIONS_w8') > 0).select(sql_avg('label').alias('churn_rate')).first()['churn_rate']

    print "[Info navcomp exploratory] Number of NIFs (only vdf): " + str(nb_nifs_onlyvdf) + " - Churn rate (only vdf): " + str(churn_onlyvdf)

    print "[Info navcomp exploratory] Number of NIFs (no cancel CCC w8, only vdf): " + str(nb_nifs_onlyvdf_nocancel_ccc_w8) + " - Churn rate (no cancel CCC w8, only vdf): " + str(churn_onlyvdf_nocancel_ccc_w8)

    print "[Info navcomp exploratory] Number of NIFs (cancel CCC w8, only vdf): " + str(nb_nifs_onlyvdf_cancel_ccc_w8) + " - Churn rate (cancel CCC w8, only vdf): " + str(churn_onlyvdf_cancel_ccc_w8)
    '''

    '''

    # Individual evaluation of the input features

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    metadata = get_metadata(spark)

    numeric_columns = metadata.filter(col('type') == 'numeric').rdd.map(lambda x: x['feature']).collect()

    print "[Info navcomp exploratory] Evaluating the individual performance of each feature - Total problem"

    get_auc(navcomp_df, numeric_columns)

    print "[Info navcomp exploratory] Evaluating the individual performance of each feature - No cancel problem"

    get_auc(navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') == 0), numeric_columns)

    print "[Info navcomp exploratory] Evaluating the individual performance of each feature - Cancel problem"

    get_auc(navcomp_df.filter(col('CHURN_CANCELLATIONS_w8') > 0), numeric_columns)
    
    '''

