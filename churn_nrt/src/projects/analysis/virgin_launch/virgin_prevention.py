# coding=utf-8

from common.src.main.python.utils.hdfs_generic import *
import sys
import time
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
                                    skewness,
                                    kurtosis,
                                    concat_ws,
                                   array,
                                   lpad,
                                   split,
                                   regexp_replace)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, StructType, StructField
import pandas as pd


def get_virgin_navs(spark, starting_date, closing_day):

    path_to_read = "/data/udf/vf_es/netscout/dailyMSISDNApplicationName/"

    print("[Info] Reading from {}".format(path_to_read))

    repart_navigdf = spark.read.parquet(path_to_read)

    repart_navigdf = repart_navigdf \
        .withColumn('total_data', col("SUM_userplane_upload_bytes_count") + col("SUM_userplane_download_bytes_count")) \
        .filter((col("subscriber_msisdn").isNotNull()) & (col("application_name").contains('VIRGINTELCO')) & (col('total_data') > 524288)) \
        .withColumn("event_date", concat(col('year'), lit("-"), lpad(col('month'), 2, '0'), lit("-"), lpad(col('day'), 2, '0'))) \
        .withColumn("formatted_event_date", from_unixtime(unix_timestamp(col("event_date"), "yyyy-MM-dd"))) \
        .filter((col("formatted_event_date") >= from_unixtime(unix_timestamp(lit(starting_date), "yyyyMMdd"))) & (col("formatted_event_date") <= from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")))) \
        .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9)) \
        .groupBy('msisdn').agg(sql_sum("count").alias("sum_count"), countDistinct('event_date').alias('num_distinct_days'))\
        .select('msisdn', 'sum_count', 'num_distinct_days') \

    return repart_navigdf


def get_virgin_pop(spark, date_, n_cycles=12, verbose=False):

    from churn_nrt.src.data.customer_base import CustomerBase

    # Active mobile services

    pop_df = CustomerBase(spark)\
        .get_module(date_, save=False, save_others=False, force_gen=True) \
        .filter((col('rgu') == 'mobile') & (col("cod_estado_general").isin("01", "09")) & (col("srv_basic").isin("MRSUI", "MPSUI") == False))\
        .select('msisdn', 'num_cliente', 'nif_cliente')

    pop_df.cache()

    print '[Info] Initial - Initial volume: ' + str(pop_df.count()) + ' - Initial number of msisdn: ' + str(
        pop_df.select('msisdn').distinct().count()) + ' - Initial number of NIFs: ' + str(
        pop_df.select('nif_cliente').distinct().count()) + ' - Initial number of NCs: ' + str(
        pop_df.select('num_cliente').distinct().count())

    # Getting the date for n_cycles cycles before

    from churn_nrt.src.utils.date_functions import move_date_n_cycles, get_diff_days
    date_prev = move_date_n_cycles(date_, -n_cycles)
    diff_days = get_diff_days(date_prev, date_, format_date="%Y%m%d")

    # if filter_recent:
    from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter
    df_rec = get_non_recent_customers_filter(spark, date_, diff_days, level='nif', verbose=False)
    filt_pop_df = pop_df.join(df_rec.select('nif_cliente'), ['nif_cliente'], 'inner') \

    filt_pop_df.cache()

    print '[Info] Recent customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
        filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
        filt_pop_df.select('nif_cliente').distinct().count()) + ' - Number of NCs: ' + str(
        filt_pop_df.select('num_cliente').distinct().count())

    # if filter_disc:
    from churn_nrt.src.data_utils.base_filters import get_disconnection_process_filter
    df_disc = get_disconnection_process_filter(spark, date_, diff_days, verbose=verbose, level="nif_cliente")
    filt_pop_df = filt_pop_df.join(df_disc.select('nif_cliente'), ['nif_cliente'], 'inner')

    filt_pop_df.cache()

    print '[Info] DX customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
        filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
        filt_pop_df.select('nif_cliente').distinct().count()) + ' - Number of NCs: ' + str(
        filt_pop_df.select('num_cliente').distinct().count())

    # if filter_order:
    from churn_nrt.src.data_utils.base_filters import get_forbidden_orders_filter

    df_ord = get_forbidden_orders_filter(spark, date_, level='nif', verbose=False, only_active=True)

    filt_pop_df = filt_pop_df.join(df_ord.select('nif_cliente'), ['nif_cliente'], 'inner')

    filt_pop_df.cache()

    print '[Info] Order customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
        filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
        filt_pop_df.select('nif_cliente').distinct().count()) + ' - Number of NCs: ' + str(
        filt_pop_df.select('num_cliente').distinct().count())

    # if filter_ccc:
    from churn_nrt.src.data_utils.base_filters import get_churn_call_filter

    df_churn_ccc = get_churn_call_filter(spark, date_, 8 * 4, level='nif', filter_function=None, verbose=False)

    filt_pop_df = filt_pop_df.join(df_churn_ccc.select('nif_cliente'), ['nif_cliente'], 'inner')

    filt_pop_df.cache()

    print '[Info] CCC customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
        filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
        filt_pop_df.select('nif_cliente').distinct().count()) + ' - Number of NCs: ' + str(
        filt_pop_df.select('num_cliente').distinct().count())

    # filt no nav through virgin website in last 30 days

    from churn_nrt.src.utils.date_functions import move_date_n_days
    starting_date = move_date_n_days(date_, -30)

    virgin_df = get_virgin_navs(spark, starting_date, date_)\
        .select('msisdn')\
        .withColumn('tmp', lit(1))

    filt_pop_df = filt_pop_df\
    .join(virgin_df, ['msisdn'], 'left')\
    .filter(isnull(col('tmp')))\

    filt_pop_df.cache()

    print '[Info] No Virgin nav - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
        filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
        filt_pop_df.select('nif_cliente').distinct().count()) + ' - Number of NCs: ' + str(
        filt_pop_df.select('num_cliente').distinct().count())

    # Label

    label_date = move_date_n_days(date_, 15)

    label_df = get_virgin_navs(spark, date_, label_date)\
        .filter((col('sum_count') >= 2) | (col('num_distinct_days') >= 2))\
        .select('msisdn').distinct()\
        .withColumn('label', lit(1.0))

    agg_filt_pop_df = filt_pop_df\
        .join(label_df, ['msisdn'], 'left')\
        .na.fill({'label': 0.0})\
        .groupBy('label').agg(count('*').alias('volume'))\
        .withColumn('date', lit(date_))\

    agg_filt_pop_df.show()

    print '[Info] Result for date ' + str(date_) + ' shown above'

    return agg_filt_pop_df

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

    ############### 0. Spark ################

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon

    spark, sc = get_spark_session_noncommon("virgin_population")

    ############### 0. Input arguments #################

    dates_ = ['20200531', '20200607', '20200614', '20200621']

    virgin_pops_list = [get_virgin_pop(spark, d, n_cycles=12) for d in dates_]

    from functools import reduce

    virgin_pops_df = reduce(lambda a,b: a.union(b), virgin_pops_list)

    virgin_pops_df.orderBy(asc('date'), asc('label')).show()




