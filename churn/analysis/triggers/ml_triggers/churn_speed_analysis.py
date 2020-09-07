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


def get_lost_customers(df, basedf):

    lost_df = df \
        .select('nif') \
        .withColumnRenamed('nif', 'nif_cliente') \
        .join(basedf.withColumn('tmp', lit(1)), ['nif_cliente'], 'left') \
        .filter(isnull(col('tmp'))) \
        .select('nif_cliente')

    deliv_nifs = df.select('nif').distinct().count()

    lost_nifs = lost_df.select('nif_cliente').distinct().count()

    print "[Info churn_speed_analysis_trigger_ml] Total number of delivered NIFs: " + str(deliv_nifs)

    print "[Info churn_speed_analysis_trigger_ml] Total number of lost NIFs: " + str(lost_nifs)

    return (lost_nifs, deliv_nifs)

def lost_customer_analysis(spark, deliv_date_):

    deliv_year_ = str(int(deliv_date_[0:4]))

    deliv_month_ = str(int(deliv_date_[4:6]))

    deliv_day_ = str(int(deliv_date_[6:8]))

    ##########################################################################################
    # 2. Loading the delivery
    ##########################################################################################

    deliv_df = spark \
        .read \
        .parquet(
        "/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml/year=" + deliv_year_ + "/month=" + deliv_month_ + "/day=" + deliv_day_) \
        .withColumn('scoring', col('scoring') + lit(0.00001) * randn())

    picture_date_ = deliv_df.select("predict_closing_date").first()['predict_closing_date']

    print "[Info churn_speed_analysis_trigger_ml] Scorings delivered on " + str(
        deliv_date_) + " computed from the state on " + str(picture_date_)

    ##########################################################################################
    # 3. Comparing bases
    ##########################################################################################

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    deliv_base_df = get_customer_base(spark, deliv_date_).select('nif_cliente')

    pcgs = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

    from pyspark.sql import DataFrameStatFunctions

    ths = DataFrameStatFunctions(deliv_df).approxQuantile('scoring', pcgs, 0.00001)

    print ths

    result = [get_lost_customers(deliv_df.filter(col('scoring') >= x), deliv_base_df) for x in ths]

    for i in range(len(pcgs)):
        r = result[i]
        print "[Info churn_speed_analysis_trigger_ml] Delivery date: " + deliv_date_ + " - Top: " + str(
            pcgs[i]) + " - Threshold: " + str(ths[i]) + " - Total NIFs: " + str(r[1]) + " - Lost NIFs: " + str(r[0])

def days_to_churn_analysis(spark, deliv_date_):

    deliv_year_ = str(int(deliv_date_[0:4]))

    deliv_month_ = str(int(deliv_date_[4:6]))

    deliv_day_ = str(int(deliv_date_[6:8]))

    ##########################################################################################
    # 2. Loading the delivery
    ##########################################################################################

    deliv_df = spark \
        .read \
        .parquet("/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml/year=" + deliv_year_ + "/month=" + deliv_month_ + "/day=" + deliv_day_) \
        .withColumn('scoring', col('scoring') + lit(0.00001) * randn())

    picture_date_ = str(deliv_df.select("predict_closing_date").first()['predict_closing_date'])

    print "[Info churn_speed_analysis_trigger_ml] Scorings delivered on " + str(deliv_date_) + " computed from the state on " + str(picture_date_)

    #################################
    # Target
    #################################

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    target_df = get_churn_target_nif(spark, picture_date_, churn_window=30)


    lab_deliv_df = deliv_df.withColumnRenamed('nif', 'nif_cliente').join(target_df, ['nif_cliente'], 'inner') \
        .withColumn('days_to_port', datediff(col('portout_date'), from_unixtime(unix_timestamp(lit(picture_date_), 'yyyyMMdd'))).cast('double')) \

    lab_deliv_df.show()

    print "[Info churn_speed_analysis_trigger_ml] Labeled delivery DF showed above"

    lab_deliv_df.filter(col('label') == 1.0).describe('days_to_port').show()

    print "[Info churn_speed_analysis_trigger_ml] Statistical description of days_to_port showed above"

    lab_deliv_df.filter(col('label') == 1.0).groupBy('reason').agg(count('*')).show()

    lab_deliv_df.filter(col('label') == 1.0).filter(col('reason')=='mob').describe('days_to_port').show()

    print "[Info churn_speed_analysis_trigger_ml] Statistical description of days_to_port for mob showed above"

    lab_deliv_df.filter(col('label') == 1.0).filter(col('reason') == 'fix').describe('days_to_port').show()

    print "[Info churn_speed_analysis_trigger_ml] Statistical description of days_to_port for fix showed above"

    pcgs = [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5, 0.6, 0.75, 0.9, 1.0]

    from pyspark.sql import DataFrameStatFunctions

    lab_deliv_mob_fix_df = lab_deliv_df.filter(col('label') == 1.0).filter(col('reason').isin(['fix', 'mob'])).withColumn('days_to_port', col('days_to_port').cast('double'))

    lab_deliv_mob_fix_df.show()

    #ths = DataFrameStatFunctions(lab_deliv_mob_fix_df).approxQuantile('days_to_port', pcgs, 0.0)
    ths = lab_deliv_mob_fix_df.approxQuantile("days_to_port", pcgs, 0.0)

    print ths

    for i in range(len(pcgs)):
        print "[Info churn_speed_analysis_trigger_ml] " + str(100*pcgs[i]) + " per cent of the churners lost before " + str(ths[i]) + " days"

    h = lab_deliv_df \
        .filter(col('label') == 1.0) \
        .select('days_to_port') \
        .rdd \
        .map(lambda r: r['days_to_port']) \
        .histogram(20)

    bucket_edges = h[0]

    bucket_mid = []

    for i in range(1, len(bucket_edges)):
        l = bucket_edges[(i - 1):(i + 1)]
        bucket_mid.append(float(sum(l)) / float(len(l)))

    hist_count = h[1]

    for i in range(len(hist_count)):
        print '[Info histogram] Bucket: ' + str(bucket_mid[i]) + ' - Hist: ' + str(hist_count[i])


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
    # 0. Input args
    ##########################################################################################

    deliv_date_ = sys.argv[1]

    mode_ = sys.argv[2]


    ##########################################################################################
    # 1. Create Spark context with Spark configuration
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

    if mode_ == "lost":
        lost_customer_analysis(spark, deliv_date_)
    elif mode_ == "days":
        days_to_churn_analysis(spark, deliv_date_)










