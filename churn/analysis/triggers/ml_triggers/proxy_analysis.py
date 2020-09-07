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

    deliv_year_ = str(int(deliv_date_[0:4]))

    deliv_month_ = str(int(deliv_date_[4:6]))

    deliv_day_ = str(int(deliv_date_[6:8]))

    # 2. Taking delivery
    # - base, with cod_estado_general, for predict_closing_date
    # - label with FBB Dx
    # - for those with target=1 due to FBB:
    # --> number of services on predict_closing_date: it may occur the NIF only has one service and it corresponds to the FBB (customer partially disconnected)
    # --> state (cod_estado) of the customeron the target date used for base comparison

    deliv_df = spark \
        .read \
        .parquet(
        "/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml/year=" + deliv_year_ + "/month=" + deliv_month_ + "/day=" + deliv_day_) \
        .withColumn('scoring', col('scoring') + lit(0.00001) * randn()).withColumnRenamed('nif', 'nif_cliente')

    picture_date_ = str(deliv_df.select("predict_closing_date").first()['predict_closing_date'])

    print "[Info Proxy Analysis] Scorings delivered on " + str(deliv_date_) + " computed from the state on " + str(picture_date_) + " - Volume of the delivery: " + str(deliv_df.count())

    # Adding segment to NIFs in the delivery

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment

    base_predict_df = get_customer_base_segment(spark, picture_date_, ['cod_estado_general'], False).select(['nif_cliente', 'seg_pospaid_nif']).distinct()

    #print '[Info Proxy Analysis] Size: ' + str(base_predict_df.count()) + ' - Num distinct num_cliente: ' + str(base_predict_df.select('num_cliente').distinct().count()) + ' - Num distinct nif_cliente: ' + str(base_predict_df.select('nif_cliente').distinct().count())
    print '[Info Proxy Analysis] Size of the base: ' + str(base_predict_df.count()) + ' - Num distinct nif_cliente: ' + str(base_predict_df.select('nif_cliente').distinct().count())

    deliv_df = deliv_df.join(base_predict_df, ['nif_cliente'], 'inner')

    print "[Info Proxy Analysis] Volume of the delivery after the join with the base: " + str(deliv_df.count())

    # Adding label

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    target_df = get_churn_target_nif(spark, picture_date_)

    print "[Info Proxy Analysis] Volume of the target: " + str(target_df.count())

    deliv_df = deliv_df.join(target_df, ['nif_cliente'], 'inner')

    print "[Info Proxy Analysis] Volume of the delivery after the join with the target: " + str(deliv_df.count())

    # Churn rate for each segment

    target_df.join(base_predict_df, ['nif_cliente'], 'inner').groupBy('seg_pospaid_nif', 'label').agg(count('*').alias('num_customers')).show()

    deliv_df.groupBy('seg_pospaid_nif', 'label').agg(count('*').alias('num_customers')).show()

    #deliv_df.groupBy('seg_pospaid_nif').agg(sql_avg('label').alias('churn_rate')).show()

    #deliv_df.filter(col('label')==1).groupBy('seg_pospaid_nif').agg(count('*').alias('num_customers')).show()



