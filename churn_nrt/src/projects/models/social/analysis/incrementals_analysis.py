#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import collect_set, concat, size, coalesce, col, trim, lpad, struct, upper, lower, avg as sql_avg, count as sql_count, lag, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum, length, concat_ws, regexp_replace, split


from pyspark.sql.functions import (
                                    col,
#                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
                                     avg as sql_avg,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
                                    )
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# import datetime as dt
# from pyspark.sql import Row, DataFrame, Column, Window
# from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import concat, size, coalesce, col, lpad, struct, count as sql_count, lag, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum, length, concat_ws, regexp_replace, split
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
# from pyspark.sql.functions import array, regexp_extract
# from itertools import chain
# from churn.datapreparation.general.data_loader import get_unlabeled_car, get_port_requests_table, get_numclients_under_analysis
# from churn.utils.constants import PORT_TABLE_NAME
# from churn.utils.udf_manager import Funct_to_UDF
# from pyspark.sql.functions import substring, datediff, row_number


#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/social/analysis/incrementals.py --day 20191231 --day2 20190930 --days_incremental 90 > /var/SP/data/home/csanc109/logging/social_incremental_20191231_20190930_90.log




import datetime as dt
# from pyspark.sql.functions import from_unixtime,unix_timestamp


import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/social/analysis/incrementals_analysis.py --day 20200220 --days_incremental 90 > /var/SP/data/home/csanc109/logging/social_incremental_analysis_20200220_90.log
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



if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################



    start_time_total = time.time()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(
        description="Run myvf model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--day2', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--days_incremental', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    args = parser.parse_args()
    print(args)

    closing_day = args.day
    #closing_day_2 = args.day2
    days_incremental = int(args.days_incremental)

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("analysis_ch_rate_section")
    sc.setLogLevel('WARN')

    from churn_nrt.src.utils.date_functions import move_date_n_days
    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists

    starting_day = move_date_n_days(closing_day, n=-days_incremental)
    #starting_day_2 = move_date_n_days(closing_day_2, n=-days_incremental)

    print("RANGE1 - {} to {}".format(starting_day, closing_day))
    #print("RANGE2 - {} to {}".format(starting_day_2, closing_day_2))

    #closing_day = "20191117"
    #days_incremental = 90

    from churn_nrt.src.data.customer_base import CustomerBase

    df_cust_base = CustomerBase(spark).get_module(closing_day).where(col("rgu") == "mobile")

    if closing_day == "20191231" and days_incremental == 30:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/social/incrementals_df_20191231_20191130_30")

    elif closing_day == "20191231" and days_incremental == 15:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/social/incrementals_df_20191231_20191215_15")

    elif closing_day == "20191231" and days_incremental == 60:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/myvf/social/incrementals_df_20191231_20191031_60")

    elif closing_day == "20191231" and days_incremental == 90:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/social/incrementals_df_20191231_20190930_90")



    elif closing_day == "20191117" and days_incremental == 90:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/social/incrementals_df_20191117_20190817_90")

    elif closing_day == "20200220" and days_incremental == 90:
        df_all = spark.read.load("/user/csanc109/projects/churn/data/social/incrementals_df_20200220_20191120_90")



    else:
        print("ERROR  closing_day={} days_incremental={}".format(closing_day, days_incremental))
        import sys

        sys.exit()

    df_all = df_all.join(df_cust_base.select("nif_cliente", "msisdn"), on=["msisdn"], how="inner")
    df_all.count()

    from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter

    print("[TriggerMyVfModel] get_set | - - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_active_filter = get_non_recent_customers_filter(spark, closing_day, 90)

    print("[TriggerMyVfModel] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_disconnection_filter = get_disconnection_process_filter(spark, closing_day, 90)

    print("[TriggerMyVfModel] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

    tr_churn_call_filter = get_churn_call_filter(spark, closing_day, 90, 'msisdn')

    tr_set = (df_all.join(tr_active_filter, ['msisdn'], 'inner')\
        .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tr_churn_call_filter, ['msisdn'], 'inner'))


    from pyspark.sql.functions import expr, avg as sql_avg
    ch_rate_ref_after_filters = tr_set.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    median_total_calls = tr_set.select(expr('percentile_approx(total_calls, 0.5)').alias("median_value")).rdd.first()["median_value"]

    print("median_total_calls", median_total_calls)

    print("before filtering", tr_set.count())

    df_all_filt_median = tr_set.where(col("total_calls") > median_total_calls)

    print("after filtering", df_all_filt_median.count())
    ch_rate_ref = df_all_filt_median.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']

    print("ch_rate_ref", ch_rate_ref)

    total_mou_0p75 = df_all_filt_median.select(expr('percentile_approx(total_mou, 0.75)').alias("0p75")).rdd.first()["0p75"]
    total_mou_0p9 = df_all_filt_median.select(expr('percentile_approx(total_mou, 0.9)').alias("0p9")).rdd.first()["0p9"]
    total_mou_0p99 = df_all_filt_median.select(expr('percentile_approx(total_mou, 0.99)').alias("0p99")).rdd.first()["0p99"]
    max_total_mou = df_all_filt_median.select(sql_max(col("total_mou")).alias("max_total_mou")).rdd.first()["max_total_mou"]

    print("total_mou q0.75", total_mou_0p75)
    print("total_mou q0.90", total_mou_0p9)
    print("total_mou q0.99", total_mou_0p99)
    print("max_total_mou", max_total_mou)

    total_calls_0p75 = df_all_filt_median.select(expr('percentile_approx(total_calls, 0.75)').alias("0p75")).rdd.first()["0p75"]
    total_calls_0p9 = df_all_filt_median.select(expr('percentile_approx(total_calls, 0.9)').alias("0p9")).rdd.first()["0p9"]
    total_calls_0p99 = df_all_filt_median.select(expr('percentile_approx(total_calls, 0.99)').alias("0p99")).rdd.first()["0p99"]
    max_total_calls = df_all_filt_median.select(sql_max(col("total_calls")).alias("max_total_calls")).rdd.first()["max_total_calls"]

    print("total_calls q0.75", total_calls_0p75)
    print("total_calls q0.90", total_calls_0p9)
    print("total_calls q0.99", total_calls_0p99)
    print("max_calls", max_total_calls)

    # Remove outliers
    df_all_filt = df_all_filt_median.where(col("total_calls") <= total_calls_0p99).where(col("total_mou") <= total_mou_0p99)
    ch_rate_ref = df_all_filt.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    print("ch_rate_ref", 100.0 * ch_rate_ref)

    # By operator
    import re

    # ch_rate_ref = df_all_filt.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    vol_total = df_all_filt.count()
    print("churn_rate ref", vol_total, 100.0 * ch_rate_ref)
    for col_ in ['num_calls_ORANGE', 'num_calls_MAS_MOVIL', 'num_calls_TELEFONICA']:
        print(re.sub("^num_calls_", "", col_), 100.0 * df_all_filt.where(col(col_) > 0).count() / vol_total,
              100.0 * df_all_filt.where(col(col_) > 0).select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref'])

    # By operator
    import re

    ch_rate_ref = df_all_filt.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    print("churn_rate ref", 100.0 * ch_rate_ref)
    for col_ in [  # 'inc_num_calls_ORANGE_d{}'.format(days_incremental),
        # 'inc_num_calls_MAS_MOVIL_d{}'.format(days_incremental),
        # 'inc_num_calls_TELEFONICA_d{}'.format(days_incremental),
        'inc_total_calls_other_operator_d{}'.format(days_incremental)
        # 'inc_total_calls_d{}'.format(days_incremental)
    ]:
        for inc_ in range(0, 251, 25):
            vol_ = df_all_filt.where(col(col_) > inc_).count()
            print("{0} > {1} | {2:.3f} % | vol={3}".format(re.sub("_d{}$".format(days_incremental), "", re.sub("^inc_num_calls_", "", col_)), inc_,
                                                           100.0 * df_all_filt.where(col(col_) > inc_).select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref'] if vol_ > 0 else 0, vol_))

    df_magic = (df_all_filt.where(  # (col('inc_num_calls_ORANGE_d{}'.format(days_incremental))>150) |
        (col('inc_num_calls_MAS_MOVIL_d{}'.format(days_incremental)) > 0) |  # .where(col('inc_num_calls_TELEFONICA_d{}'.format(days_incremental))>175)
        (col('inc_total_calls_other_operator_d90') > 175))# .where(col('inc_total_calls') > 50)
    )

    vol_ = df_magic.count()

    ch_rate_magic = df_magic.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    print("ch rate after 3 filters", ch_rate_ref_after_filters)
    print("ch rate after median and outliers filters", ch_rate_ref)

    print(vol_, 100.0 * ch_rate_magic, ch_rate_magic / ch_rate_ref_after_filters, ch_rate_magic / ch_rate_ref)

    print("Ended! - elapsed {} minutes".format( (time.time() - start_time_total)/60.0))
