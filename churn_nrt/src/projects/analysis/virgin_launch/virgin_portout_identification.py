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


def get_port_data_agg_for_date(df, date_):
    year_ = str(int(date_[0:4]))
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))

    from churn_nrt.src.utils.date_functions import move_date_n_days

    target_date = move_date_n_days(date_, 30)

    cust_base_df = spark \
        .read \
        .parquet('/data/udf/vf_es/amdocs_inf_dataset/customer/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
        .filter(col("Cust_COD_ESTADO_GENERAL").isin("01", "09")) \
        .select("NUM_CLIENTE", "Cust_L2_codigo_postal_city") \
        .withColumnRenamed('Cust_L2_codigo_postal_city', 'cp') \
        .distinct()

    serv_base_df = spark \
        .read \
        .parquet('/data/udf/vf_es/amdocs_inf_dataset/service/year=' + year_ + '/month=' + month_ + '/day=' + day_) \
        .filter(col('Serv_RGU') == 'mobile') \
        .select('num_cliente', 'msisdn') \
        .distinct()

    base_df = cust_base_df.join(serv_base_df, ['num_cliente'], 'inner')

    from itertools import product

    operators = ['movistar', 'masmovil', 'orange', 'euskaltel', 'others']

    cps = cps = [str(e) if e >= 10 else ('0' + str(e)) for e in range(0, 52)] + ['unknown']

    combs = list(product(operators, cps))

    schema = StructType([
        StructField("target_operator", StringType(), True),
        StructField("cp", StringType(), True)
    ])

    ref_df = spark.createDataFrame(sc.parallelize(combs), schema)

    agg_df = df \
        .filter((col("portout_date_mob") >= from_unixtime(unix_timestamp(lit(date_), 'yyyyMMdd')))
                & (col("portout_date_mob") <= from_unixtime(unix_timestamp(lit(target_date), 'yyyyMMdd')))) \
        .join(base_df.select('msisdn', 'cp'), ['msisdn'], 'inner') \
        .groupBy("target_operator", "cp") \
        .agg(count("*").alias('vol_portas')) \
        .withColumn('tmp', lit(1))

    comp_df = ref_df.join(agg_df.select('target_operator', 'cp', 'tmp'), ['target_operator', 'cp'], 'left') \
        .filter(isnull(col('tmp'))).select('target_operator', 'cp').withColumn('vol_portas', lit(0))

    result_pd = agg_df \
        .select('target_operator', 'cp', 'vol_portas') \
        .union(comp_df) \
        .withColumn('all', lit('all')) \
        .withColumn('total', sql_sum(col('vol_portas')).over(Window.partitionBy('all'))) \
        .withColumn('weight', col('vol_portas') / col('total')) \
        .select('target_operator', 'vol_portas', 'total', 'weight', 'cp') \
        .withColumn('date', lit(date_)).toPandas()

    return result_pd


def get_port_volume_series(spark, start_date, num_cycles):
    from churn_nrt.src.utils.date_functions import move_date_n_cycles, get_diff_days, move_date_n_days

    dates_ = [move_date_n_cycles(start_date, n) for n in range(0, (num_cycles + 1))]

    num_days = get_diff_days(start_date, dates_[-1])

    end_date = move_date_n_days(start_date, 30 + num_days)

    print '[Info] Port-out requests from ' + start_date + ' to ' + end_date

    from churn_nrt.src.data.sopos_dxs import MobPort

    sol_port = MobPort(spark, churn_window=30 + num_days) \
        .get_module(start_date, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "portout_date_mob", "target_operator")

    # sol_port = get_port_data(spark, start_date, 30 + num_days).select("msisdn", "portout_date_mob", "target_operator")

    sol_port.cache()

    print "[Info] Size of sol_port: " + str(sol_port.count())

    # Filter to retain those corresponding to active customers in the base on each date

    ports_df = pd.concat([get_port_data_agg_for_date(sol_port, d) for d in dates_])

    return ports_df


def get_data_for_region(pd_df, cp_list, operator_list):
    filt_pd_df = pd_df[(pd_df.cp.isin(cp_list)) & (pd_df.target_operator.isin(operator_list))]

    vol_results_agg = filt_pd_df.groupby(['date'], axis=0, as_index=False).agg({"vol_portas": "sum"})

    return vol_results_agg

def get_ccaa():

    return {'galicia': ['15', '27', '32', '36'],
        'asturias': ['33'],
        'cantabria': ['39'],
        'paisvasco': ['01', '20', '48'],
        'navarra': ['31'],
        'aragon': ['22', '44', '50'],
        'cataluna': ['08', '17', '25', '43'],
        'cyl': ['09', '24', '34', '05', '37', '40', '47', '42', '49'],
        'rioja': ['26'],
        'extremadura': ['06', '10'],
        'madrid': ['28'],
        'clm': ['02', '16', '19', '13', '45'],
        'valencia': ['03', '12', '46'],
        'murcia': ['30'],
        'andalucia': ['21', '14', '11', '41', '23', '18', '04', '29'],
        'baleares': ['07'],
        'canarias': ['35', '38'],
        'cym': ['51', '52'],
        'otros': ['00', 'unknown']}

def get_euskaltel_area():

    ccaa = get_ccaa()

    eus_area = dict([(p[0], p[1]) for p in ccaa.items() if p[0] in ['galicia', 'asturias', 'paisvasco', 'navarra', 'rioja']])

    no_eus_area = dict([(p[0], p[1]) for p in ccaa.items() if p[0] not in ['galicia', 'asturias', 'paisvasco', 'navarra', 'rioja']])

    return (eus_area, no_eus_area)

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

    spark, sc = get_spark_session_noncommon("virgin_identification")

    ############### 0. Input arguments #################

    date_ = sys.argv[1]

    num_cycles = sys.argv[2]

    port_series = get_port_volume_series(spark, date_, num_cycles)

