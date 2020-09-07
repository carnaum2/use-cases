#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import (desc,
                                   asc,
                                   sum as sql_sum,
                                   avg as sql_avg,
                                   isnull,
                                   when,
                                   col,
                                   isnan,
                                   count,
                                   row_number,
                                   lit,
                                   coalesce,
                                   countDistinct,
                                   from_unixtime,
                                   unix_timestamp,
                                   concat,
                                   lpad)

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

    # Input arguments

    date_ = sys.argv[1]

    # Parsing year, month, day

    year_ = date_[0:4]

    month_ = str(int(date_[4:6]))

    day_ = str(int(date_[6:8]))

    # 30 days before

    from churn_nrt.src.utils.date_functions import move_date_n_days

    starting_date = move_date_n_days(date_, -30)

    ##################

    sc, sparkSession, _ = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    # Reading Netscout data

    df_all_app = spark \
        .read \
        .parquet("/data/udf/vf_es/netscout/dailyMSISDNApplicationName/") \
        .withColumn("event_date", from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')), "yyyyMMdd")))\
        .filter((col("event_date") >= from_unixtime(unix_timestamp(lit(starting_date), "yyyyMMdd"))) & (col("event_date") <= from_unixtime(unix_timestamp(lit(date_), "yyyyMMdd"))))\
        .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9))

    df_all_app.repartition(400)

    df_all_app.cache()

    print "[Info] Before filtering - Size of df_all_app: " + str(df_all_app.count())

    # Active base on 20191031

    from churn_nrt.src.data.customer_base import CustomerBase

    df_base_msisdn = CustomerBase(spark)\
        .get_module(date_, save=False)\
        .filter(col("rgu") == "mobile")\
        .select("msisdn")\
        .distinct()

    # Retaining only active customers

    df_app = df_all_app\
        .join(df_base_msisdn, ['msisdn'], 'inner')

    df_app.repartition(400)

    df_app.cache()

    print "[Info] After filtering - Size of df_app: " + str(df_app.count())

    from churn_nrt.src.data.sopos_dxs import MobPort

    df_sols_port = MobPort(spark)\
        .get_module("20191031", save=False, churn_window=30)\
        .select("msisdn", "label_mob")\
        .withColumnRenamed("label_mob", "label")\
        .distinct()

    total_num_users = df_app.select("msisdn").distinct().count()

    df_app_lab = df_app.join(df_sols_port, ['msisdn'], 'left').na.fill({'label': 0.0})

    # Adding total time (count) for every

    df_app_agg = df_app_lab\
        .groupBy("application_name")\
        .agg(countDistinct("msisdn").cast("double").alias("num_users"), sql_avg("label").alias("churn_rate"))\
        .withColumn("total_num_users", lit(total_num_users).cast("double"))\
        .withColumn("app_weight", col("num_users")/col("total_num_users"))\
        .withColumn("churn_weight", col("num_users")*col("churn_rate"))

    df_app_agg.repartition(400)

    df_app_agg.cache()

    df_app_agg.orderBy(desc("churn_rate")).show(100, False)

    print "[Info] df_app_agg sorted by desc(churn_rate) above"

    df_app_agg.orderBy(desc("churn_weight")).show(100, False)

    print "[Info] df_app_agg sorted by desc(churn_weight) above"

    df_app_agg.orderBy(desc("app_weight")).show(100, False)

    print "[Info] df_app_agg sorted by desc(app_weight) above"

    df_app_agg_filt = df_app_agg.filter(col("num_users") > 250000.0)

    df_app_agg_filt.repartition(400)

    df_app_agg_filt.cache()

    df_app_agg_filt.orderBy(desc("churn_rate")).show(100, False)

    print "[Info] df_app_agg_filt sorted by desc(churn_rate) above"

    df_app_agg_filt.orderBy(desc("churn_weight")).show(100, False)

    print "[Info] df_app_agg_filt sorted by desc(churn_weight) above"

    df_app_agg_filt.orderBy(desc("app_weight")).show(100, False)

    print "[Info] df_app_agg_filt sorted by desc(app_weight) above"




    '''

    from pyspark.sql import Window

    window = Window.partitionBy("application_name", "msisdn")

    # Only

    order_crm_sifd = class_order_df \
        .join(orders_crm_df, ['OBJID'], 'right') \
        .withColumn("row_nb", row_number().over(window)) \
        .filter(col("X_CLASIFICACION").isin('Instalacion', 'Reconexion', 'Aumento')) \
        .filter(
        (col("row_nb") == 1) & (col("WDQ01") == 0) & (col("WDQ02") == 1) & (coalesce(col("ESTADO"), lit("NA")) == "CP")) \
        .distinct()
        
    '''


    





