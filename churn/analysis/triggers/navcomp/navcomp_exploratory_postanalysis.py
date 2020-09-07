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

from pyspark.ml.feature import QuantileDiscretizer

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

    year_ = date_[0:4]

    month_ = str(int(date_[4:6]))

    day_ = str(int(date_[6:8]))

    ##########################################################################################
    # 2. Loading the delivery
    # ##########################################################################################

    deliv_df = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_navcomp/year=' + year_ + '/month=' + month_ + '/day=' + day_)

    num_nile = int(math.floor(float(deliv_df.count()) / 50000.0))

    discretizer = QuantileDiscretizer(numBuckets=num_nile, inputCol='scoring', outputCol="decile")
    top_deliv_df = discretizer.fit(deliv_df).transform(deliv_df).withColumn("decile", col("decile") + lit(1.0)).filter(col("decile")==1)

    picture_date_ = str(deliv_df.select("predict_closing_date").first()['predict_closing_date'])

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment

    base_df = get_customer_base_segment(spark, picture_date_).select('nif_cliente', 'seg_pospaid_nif').distinct()

    # Adding the segment

    top_deliv_df.withColumnRenamed('nif', 'nif_cliente').join(base_df, ['nif_cliente'], 'left').na.fill({'seg_pospaid_nif': 'unknown'}).groupBy('seg_pospaid_nif').agg(count('*').alias('nb_customers')).show()

    base_df.groupBy('seg_pospaid_nif').agg(count('*').alias('nb_customers')).show()






