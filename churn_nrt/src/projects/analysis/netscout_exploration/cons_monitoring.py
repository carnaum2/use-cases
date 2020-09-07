#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import (udf, col, decode, when, lit, lower, concat,
                                   translate,
                                   count,
                                   sum as sql_sum,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   stddev as sql_std,
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
                                   first,
                                   to_timestamp,
                                   lpad,
                                   rpad,
                                   coalesce,
                                   udf,
                                   date_add,
                                   explode,
                                   collect_set,
                                   length,
                                   expr,
                                   split,
                                   hour,
                                   minute)
import datetime
import pandas as pd

path_netscout = "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/"

whatsapp_apps = ['whatsapp',
                 'whatsApp Media Message',
                 'whatsApp Voice Calling']

def get_hourly_agg_df(spark, date_, app_):
    """
    Function to obtain a uniform dataframe from netscout subscriber user plane
    Dates and applications are filtered to only collect the relevant information
    :return: dataframe netscout curated
    """
    #closing_day_date = datetime.datetime.strptime(closing_day, "%Y%m%d")
    #starting_day_date = datetime.datetime.strptime(starting_day, "%Y%m%d")
    year_ = date_[0:4]
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))

    from churn_nrt.src.data.customer_base import CustomerBase

    base_df = CustomerBase(spark) \
        .get_module(date_, save=False, save_others=False, force_gen=True) \
        .filter(col('rgu') == 'mobile') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)

    data_netscout_ori = (spark
                         .read
                         .parquet(path_netscout + "year=" + year_ + "/month=" + month_ + "/day=" + day_)
                         .where(lower(col('application_name')).contains(app_))
                         .where(~col('subscriber_msisdn').isNull())
                         .withColumn('msisdn', when(substring(col('subscriber_msisdn'), 1, 2) == '34', substring(col('subscriber_msisdn'), 3, 9)).otherwise(col('subscriber_msisdn')))
                         .select('msisdn', 'cal_timestamp_time', 'userplane_upload_bytes_count', 'userplane_download_bytes_count')
                         .join(base_df, ['msisdn'], 'inner')
                         .withColumn('cal_hour', hour(col('cal_timestamp_time')))
                         .withColumn('total_cons', (col('userplane_upload_bytes_count').cast('double') + col('userplane_download_bytes_count').cast('double')) / (1024.0*1024.0))
                         .groupBy(['cal_hour'])
                         .agg(sql_sum('total_cons').alias('sum_data'))
                         .withColumn('time', concat(lit(year_), lpad(lit(month_), 2, '0'), lpad(lit(day_), 2, '0'), lpad(col('cal_hour'), 2, '0')))
                        )

    pd_hourly_agg = data_netscout_ori.toPandas()

    pd_hourly_agg.to_csv('/var/SP/data/home/jmarcoso/tmp_app_consum_evolution_' + app_ + '_' + date_ + '.txt', sep='|', header=True, index=False)

    print "[Info] Saved df for " + app_ + " and " + date_ + " showed above"

    return pd_hourly_agg

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

    ############### 0. Spark ##################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("movility_analysis")

    app_ = sys.argv[1]

    date_ = sys.argv[2]

    num_days_ = int(sys.argv[3])

    from churn_nrt.src.utils.date_functions import move_date_n_days

    #date_ = "20200303"
    dates_ = [move_date_n_days(date_, d) for d in list(range(0, (num_days_ + 1)))]

    #dates_ = ["20200305", "20200319"]

    cons_curve_list = [get_hourly_agg_df(spark, d_, app_) for d_ in dates_]

    cons_curve_pd = pd.concat(cons_curve_list, ignore_index=True)

    '''
    
    from functools import reduce

    cons_curve_df = reduce(lambda x, y: x.union(y), cons_curve_list)

    cons_curve_df.cache()

    cons_curve_df.show(200, False)

    # Exporting to CSV

    cons_curve_pd = cons_curve_df.toPandas()
    
    '''

    # Exporting to CSV

    cons_curve_pd.to_csv('/var/SP/data/home/jmarcoso/app_consum_evolution_' + app_ + '_' + dates_[0] + '_' + dates_[-1] + '.txt', sep='|', header=True, index=False)