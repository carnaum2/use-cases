#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import time
# Spark utils
from pyspark.sql.functions import (udf, col, decode, when, lit, lower, concat,
                                   translate, count, max, avg, min as sql_min,
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
                                   lpad,
                                   rpad,
                                   trim,
                                   split,
                                   coalesce,
                                   array)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
import datetime as dt
from pykhaos.utils.date_functions import *

from churn_nrt.src.data.customer_base import *




def get_fbb_dxs(spark, yearmonthday, yearmonthday_target):

    current_base = get_customer_base(spark, yearmonthday)\
        .filter(col('rgu')=='fbb')\
        .select("msisdn")\
        .distinct()\
        .repartition(400)

    target_base = get_customer_base(spark, yearmonthday_target) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)

    # It is not clear when the disconnection occurs. Thus, the nid point between both dates is assigned

    from pykhaos.utils.date_functions import move_date_n_days, get_diff_days
    portout_date = move_date_n_days(yearmonthday, get_diff_days(yearmonthday, yearmonthday_target)/2)

    churn_base = current_base\
    .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn")\
    .withColumn("label_dx", lit(1.0))\
    .distinct()\
    .withColumn('portout_date_dx', from_unixtime(unix_timestamp(lit(portout_date), 'yyyyMMdd')))

    print("[Info get_fbb_dxs] - DXs for FBB services during the period: " + yearmonthday + "-"+yearmonthday_target+": " + str(churn_base.count()))

    return churn_base

def get_fix_portout_requests(spark, yearmonthday, yearmonthday_target):
    '''
    ['msisdn', 'label_srv', 'portout_date_fix']
    :param spark:
    :param yearmonthday:
    :param yearmonthday_target:
    :return:
    '''
    # mobile portout
    window_fix = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

    fixport = spark.read.table("raw_es.portabilitiesinout_portafijo") \
        .filter(col("INICIO_RANGO") == col("FIN_RANGO")) \
        .withColumnRenamed("INICIO_RANGO", "msisdn") \
        .select("msisdn", "FECHA_INSERCION_SGP") \
        .distinct() \
        .withColumn("label_srv", lit(1.0)) \
        .withColumn("FECHA_INSERCION_SGP", substring(col("FECHA_INSERCION_SGP"), 0, 10))\
        .withColumn('FECHA_INSERCION_SGP', from_unixtime(unix_timestamp(col('FECHA_INSERCION_SGP'), "yyyy-MM-dd")))\
        .where((col('FECHA_INSERCION_SGP') >= from_unixtime(unix_timestamp(lit(yearmonthday), "yyyyMMdd"))) & (col('FECHA_INSERCION_SGP') <= from_unixtime(unix_timestamp(lit(yearmonthday_target), "yyyyMMdd"))))\
        .withColumnRenamed('FECHA_INSERCION_SGP', 'portout_date_fix') \
        .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(yearmonthday[:4]), lit(yearmonthday[4:6]), lit(yearmonthday[6:])), 'yyyyMMdd')))\
        .withColumn("days_from_portout", datediff(col("ref_date"), from_unixtime(unix_timestamp(col("portout_date_fix"), "yyyyMMdd"))).cast("int"))\
        .withColumn("rank", row_number().over(window_fix))\
        .where(col("rank") == 1)\
        .select("msisdn", "label_srv", "portout_date_fix")

    print("[Info get_fix_portout_requests] - Port-out requests for fixed services during period " + yearmonthday + "-"+yearmonthday_target+": " + str(fixport.count()))

    return fixport


def get_churn_target_nif(spark, closing_day, churn_window=30):

    start_port = closing_day
    from pykhaos.utils.date_functions import move_date_n_days
    end_port = move_date_n_days(closing_day, n=churn_window)

    # Getting portout requests for fix and mobile services, and disconnections of fbb services
    df_sopo_fix = get_fix_portout_requests(spark, closing_day, end_port)
    df_baja_fix = get_fbb_dxs(spark,closing_day, end_port)
    df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

    # The base of active aervices on closing_day
    df_services = get_customer_base_segment(spark, closing_day)

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn'], "left")
                    .na.fill({'label_srv': 0.0})
                    .join(df_baja_fix, ['msisdn'], "left")
                    .na.fill({'label_dx': 0.0})
                    .join(df_sol_port, ['msisdn'], "left")
                    .na.fill({'label_mob': 0.0})
                    .withColumn('tmp', when(    (col('label_srv')==1.0) | (col('label_dx')==1.0) | (col('label_mob')==1.0), 1.0).otherwise(0.0))
                    .withColumn('label', max('tmp').over(window_nc))
                    .drop("tmp"))

    def get_churn_reason(dates):

        reasons = ['mob', 'fix', 'fbb']

        sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

        sorted_reasons = [reasons[idx] for idx in sorted_dates if ((dates[idx] is not None) & (dates[idx] != '') & (dates[idx] != ' '))]

        if not sorted_reasons:
            reason = None
        else:
            reason = sorted_reasons[0]

        return reason


    get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'portout_date_mob', 'portout_date_fix', 'portout_date_dx')\
    .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc))\
    .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc))\
    .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc))\
    .select("nif_cliente", "label", 'min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx')\
    .distinct()\
    .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx'))\
    .withColumn('reason', get_churn_reason_udf(col('dates')))\
    .withColumn('reason', when(col('label')==0.0, '').otherwise(col('reason')))\
    .withColumn('portout_date', least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx')))\
    .select("nif_cliente", "label", 'portout_date', 'reason').drop_duplicates()

    return df_target_nifs

def get_mobile_portout_requests(spark, start_port, end_port):
    '''
    ['msisdn', 'label_mob', 'portout_date_mob']
    :param spark:
    :param start_port:
    :param end_port:
    :return:
    '''

    # mobile portout
    window_mobile = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

    PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"

    df_sol_port = (spark.read.table(PORT_TABLE_NAME)
        .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))
        .withColumn('sopo_ds_fecha_solicitud', from_unixtime(unix_timestamp(col('sopo_ds_fecha_solicitud'), "yyyy-MM-dd")))
        .where((col("sopo_ds_fecha_solicitud") >= from_unixtime(unix_timestamp(lit(start_port), 'yyyyMMdd'))) & (col("sopo_ds_fecha_solicitud") <= from_unixtime(unix_timestamp(lit(end_port), 'yyyyMMdd'))))
        .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn")
        .withColumnRenamed("sopo_ds_fecha_solicitud", "portout_date")
        .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(start_port[:4]), lit(start_port[4:6]), lit(start_port[6:])), 'yyyyMMdd')))
        .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
        .withColumn("rank", row_number().over(window_mobile))
        .where(col("rank") == 1))

    df_sol_port = df_sol_port\
        .withColumn("label_mob", lit(1.0))\
        .select("msisdn", "label_mob", "portout_date")\
        .withColumnRenamed("portout_date", "portout_date_mob")

    print("[Info get_mobile_portout_requests] - Port-out requests for mobile services during period " + start_port + "-" + end_port + ": " + str(df_sol_port.count()))

    return df_sol_port