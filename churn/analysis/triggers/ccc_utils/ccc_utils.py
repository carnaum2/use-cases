#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
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
                                    regexp_replace,
                                    upper,
                                    trim)
from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
import re


def get_bucket_info(spark):

    bucket =  spark\
    .read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .load("/data/udf/vf_es/ref_tables/amdocs_ids/Agrup_Buckets_unific.txt")\
    .withColumn('INT_Tipo', upper(col('INT_Tipo')))\
    .withColumn('INT_Subtipo', upper(col('INT_Subtipo')))\
    .withColumn('INT_Razon', upper(col('INT_Razon')))\
    .withColumn('INT_Resultado', upper(col('INT_Resultado')))\
    .withColumn("bucket", upper(col("bucket")))\
    .withColumn("sub_bucket", upper(col("sub_bucket")))\
    .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")))\

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%MI VODAFONE%'), 'MI_VODAFONE').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%BILLING - POSTPAID%'), 'BILLING_POSTPAID').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%DEVICE DELIVERY/REPAIR%'), 'DEVICE_DELIVERY_REPAIR').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%DSL/FIBER INCIDENCES AND SUPPORT%'), 'DSL_FIBER_INCIDENCES_AND_SUPPORT').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%INTERNET_EN_EL_MOVIL%'), 'INTERNET_EN_EL_MOVIL').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%DEVICE UPGRADE%'), 'DEVICE_UPGRADE').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%PRODUCT AND SERVICE MANAGEMENT%'), 'PRODUCT_AND_SERVICE_MANAGEMENT').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%INFOSATIS%'), 'OTHER_CUSTOMER_INFOSATIS_START_AVERIA').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%CHURN/CANCELLATIONS%'), 'CHURN_CANCELLATIONS').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%VOICE AND MOBILE DATA INCIDENCES AND SUPPORT%'), 'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%PREPAID BALANCE%'), 'PREPAID_BALANCE').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%COLLECTIONS%'), 'COLLECTIONS').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%QUICK CLOSING%'), 'QUICK_CLOSING').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%TARIFF MANAGEMENT%'), 'TARIFF_MANAGEMENT').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%NEW ADDS PROCESS%'), 'NEW_ADDS_PROCESS').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    bucket = bucket\
    .withColumn('bucket', when(col('bucket').like('%OTHER CUSTOMER INFORMATION MANAGEMENT%'), 'OTHER_CUSTOMER_INFORMATION_MANAGEMENT').otherwise(col('bucket')))

    bucket.cache()

    print '[Info get_bucket_info] Num entries in bucket: ' + str(bucket.count())

    return bucket

def get_ccc_attributes(spark, end_date, base):
    '''
    msisdn level
    :param spark:
    :param end_date:
    :param base:
    :return:
    '''

    from pykhaos.utils.date_functions import move_date_n_cycles

    # Generating dates

    w2 = move_date_n_cycles(end_date, -2)

    w4 = move_date_n_cycles(end_date, -4)

    w8 = move_date_n_cycles(end_date, -8)

    # Aggregated attributes


    agg_w2_ccc_attributes = get_ccc_period_attributes(spark, w2, end_date, base, "_w2")

    agg_w4_ccc_attributes = get_ccc_period_attributes(spark, w4, end_date, base, "_w4")

    agg_w8_ccc_attributes = get_ccc_period_attributes(spark, w8, end_date, base, "_w8")

    ccc_attributes = agg_w2_ccc_attributes.join(agg_w4_ccc_attributes, ['msisdn'], 'inner').join(agg_w8_ccc_attributes, ['msisdn'], 'inner')


    # Incremental attributes

    # Columns

    # Bucket

    bucket = get_bucket_info(spark)

    inc_cols = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()
    inc_cols.extend(['num_calls'])

    # Last 2 cycles vs previous 2 cycles

    #Remove "_w2" suffix
    new_cols_nosuffix = [re.sub(r"_w2$", "", col_) for col_ in agg_w2_ccc_attributes.columns if col_ != "msisdn"]

    #prevw2_cols = [(col(col_ + "_w4") - col(col_ + "_w2")).alias(col_ + "_prevw2") for col_ in new_cols_nosuffix]

    # w2 - [w4-w2]
    w2vsw2 = [(2.0 * col(col_ + "_w2") - col(col_ + "_w4")).alias("inc_" + col_ + "_w2vsw2") for col_ in new_cols_nosuffix]

    # Last 1 month vs previous 1 month
    #prevw4_cols = [(col(col_ + "_w8") - col(col_ + "_w4")).alias(col_ + "_prevw4") for col_ in new_cols_nosuffix]

    #inc_w4_ccc_attributes = get_incremental_attributes(spark, previous4w_ccc_attributes, last4w_ccc_attributes, inc_cols, "_prevw4", "_w4vsw4")
    w4vsw4 = [(2.0 * col(col_ + "_w4") - col(col_ + "_w8")).alias("inc_" + col_ + "_w4vsw4") for col_ in new_cols_nosuffix]

    ccc_attributes = ccc_attributes.select(*(ccc_attributes.columns + w2vsw2 + w4vsw4))

    return ccc_attributes


# TO BE DELETED
def get_ccc_attributes_deprecated(spark, end_date, base):

    from pykhaos.utils.date_functions import move_date_n_cycles

    # Generating dates

    w2 = move_date_n_cycles(end_date, -2)

    w4 = move_date_n_cycles(end_date, -4)

    w8 = move_date_n_cycles(end_date, -8)

    # Aggregated attributes


    agg_w2_ccc_attributes = get_ccc_period_attributes(spark, w2, end_date, base, "_w2")

    agg_w4_ccc_attributes = get_ccc_period_attributes(spark, w4, end_date, base, "_w4")

    agg_w8_ccc_attributes = get_ccc_period_attributes(spark, w8, end_date, base, "_w8")

    ccc_attributes = agg_w2_ccc_attributes.join(agg_w4_ccc_attributes, ['msisdn'], 'inner').join(agg_w8_ccc_attributes, ['msisdn'], 'inner')

    #print "[Info get_ccc_attributes] Size of agg_w2_ccc_attributes: " + str(agg_w2_ccc_attributes.count()) + " - Size of agg_w4_ccc_attributes: " + str(agg_w4_ccc_attributes.count()) + " - Size of agg_w8_ccc_attributes: " + str(agg_w8_ccc_attributes.count()) + " - Size of ccc_attributes: " + str(ccc_attributes.count())

    # Incremental attributes

    # Columns

    # Bucket

    bucket = get_bucket_info(spark)

    inc_cols = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()
    inc_cols.extend(['num_calls'])

    # Last 2 cycles vs previous 2 cycles

    #last2w_ccc_attributes = get_ccc_period_attributes(spark, w2, end_date, base, "")
    # Remove "_w2" suffix for computing incremental variables
    new_cols = [re.sub(r"_w2$", "", col_) for col_ in agg_w2_ccc_attributes.columns]
    last2w_ccc_attributes = agg_w2_ccc_attributes.toDF(*new_cols)

    previous2w_ccc_attributes = get_ccc_period_attributes(spark, w4, w2, base, "_prevw2")

    inc_w2_ccc_attributes = get_incremental_attributes(spark, previous2w_ccc_attributes, last2w_ccc_attributes, inc_cols, "_prevw2", "_w2vsw2")

    # Last 1 month vs previous 1 month

    #last4w_ccc_attributes = get_ccc_period_attributes(spark, w4, end_date, base, "")
    # Remove "_w4" suffix for computing incremental variables
    new_cols = [re.sub(r"_w4$", "", col_) for col_ in agg_w4_ccc_attributes.columns]
    last4w_ccc_attributes = agg_w4_ccc_attributes.toDF(*new_cols)

    previous4w_ccc_attributes = get_ccc_period_attributes(spark, w8, w4, base, "_prevw4")

    inc_w4_ccc_attributes = get_incremental_attributes(spark, previous4w_ccc_attributes, last4w_ccc_attributes, inc_cols, "_prevw4", "_w4vsw4")

    #print "[Info get_ccc_attributes] Size of inc_w2_ccc_attributes: " + str(inc_w2_ccc_attributes.count()) + " - Size of ccc_attributes: " + str(ccc_attributes.count())

    ccc_attributes = ccc_attributes.join(inc_w2_ccc_attributes, ['msisdn'], 'inner').join(inc_w4_ccc_attributes, ['msisdn'], 'inner')

    #print "[Info get_ccc_attributes] Size of ccc_attributes: " + str(ccc_attributes.count()) + " - number of distinct MSISDNs in ccc_attributes: " + str(ccc_attributes.select('msisdn').distinct().count())

    return ccc_attributes

def get_incremental_attributes(spark, prevdf, initdf, cols, suffix, name):

    initdf = initdf.join(prevdf, ['msisdn'], 'inner')

    for c in cols:
        initdf = initdf.withColumn("inc_" + c, col(c) - col(c + suffix))

    selcols = ["inc_" + c for c in cols]
    selcols.extend(['msisdn'])

    resultdf = initdf.select(selcols)

    inc_cols = ["inc_" + c for c in cols]

    for c in inc_cols:
        resultdf = resultdf.withColumnRenamed(c, c + name)

    return resultdf

def get_ccc_period_attributes(spark, start_date, end_date, base, suffix=""):

    # Bucket

    bucket = get_bucket_info(spark)

    bucket_list = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()

    interactions_ono = spark\
    .table('raw_es.callcentrecalls_interactionono')\
    .withColumn('formatted_date', from_unixtime(unix_timestamp(col("FX_CREATE_DATE"), "yyyy-MM-dd"))  )\
    .filter((col('formatted_date') >= from_unixtime(unix_timestamp(lit(start_date), "yyyyMMdd"))) & (col('formatted_date') <= from_unixtime(unix_timestamp(lit(end_date), "yyyyMMdd")))  )

    interactions_ono = interactions_ono.filter("DS_DIRECTION IN ('De entrada', 'Entrante')") # DS_DIRECTION IN ('De entrada', 'Entrante')
    interactions_ono = interactions_ono.filter(col("CO_TYPE").rlike('(?i)^Llamada Telefono$|^Telefonica$|$\.\.\.^'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^BO'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^B\.O'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Emis'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Gestion B\.O\.$|(?i)^Gestion casos$|(?i)^Gestion Casos Resueltos$|'\
        '(?i)^Gestion Casos Resueltos$|(?i)^Gestion documental$|(?i)^Gestion documental fax$|'\
        '(?i)^2ª Codificación$|(?i)^BAJ_BO Televenta$|(?i)^BAJ_B\.O\. Top 3000$|(?i)^BBOO$'\
        '(?i)^BO Fraude$|(?i)^B.O Gestion$|(?i)^B.O Portabilidad$|(?i)^BO Scoring$|'\
        '(?i)^Bo Scoring Permanencia$|(?i)^Consulta ficha$|(?i)^Callme back$|'\
        '(?i)^Consultar ficha$|(?i)^Backoffice Reclamaciones$|(?i)^BACKOFFICE$|(?i)^BackOffice Retención$|(?i)^NBA$|'
        '(?i)^Ofrecimiento comercial$|(?i)^No Ofrecimiento$|(?i)^Porta Salientes Emp Info$|(?i)^Porta Salientes Emp Movil$|'\
        '(?i)^Porta Salientes Emp Fijo$|(?i)^Callmeback$|(?i)^Caso Improcedente$|(?i)^Caso Resuelto$|(?i)^Caso Mal  Enviado BO 123$|(?i)^Gestion BO$'))

    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^CIERRE RAPID'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^SMS FollowUP Always Solv'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Ilocalizable'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Detractor'))

    interactions_ono = interactions_ono\
    .withColumnRenamed('DS_REASON_1', 'INT_Tipo')\
    .withColumnRenamed('DS_REASON_2', 'INT_Subtipo')\
    .withColumnRenamed('DS_REASON_3', 'INT_Razon')\
    .withColumnRenamed('DS_RESULT',   'INT_Resultado')\
    .withColumnRenamed('DS_DIRECTION', 'DIRECTION')\
    .withColumnRenamed('CO_TYPE', 'TYPE_TD')\
    .withColumn('INT_Tipo', upper(col('INT_Tipo')))\
    .withColumn('INT_Subtipo', upper(col('INT_Subtipo')))\
    .withColumn('INT_Razon', upper(col('INT_Razon')))\
    .withColumn('INT_Resultado', upper(col('INT_Resultado')))\
    .join(bucket.drop('Stack').distinct(), ['INT_Tipo', 'INT_Subtipo', 'INT_Razon', 'INT_Resultado'], 'left_outer')\
    .withColumnRenamed('DS_X_PHONE_CONSULTATION', 'msisdn')\
    .withColumnRenamed('DS_X_GROUP_WORK', 'grupo_trabajo')\
    .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")) & (~isnull(col('msisdn'))) & (~col('msisdn').isin("", " ")))\
    .select('msisdn', 'bucket')\

    #bucket_list = interactions_ono.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()

    print '[Info get_ccc_period_attributes] Period: ' + str(start_date) + ' - ' + str(end_date) + ' - Number of entries before the join with the base: ' + str(interactions_ono.count())
    
    interactions_ono = interactions_ono\
    .groupBy('msisdn')\
    .pivot('bucket', bucket_list)\
    .agg(count("*"))\
    .na.fill(0)\
    .withColumn('num_calls', sum([col(x) for x in bucket_list]))
    
    print '[Info get_ccc_period_attributes] Period: ' + str(start_date) + ' - ' + str(end_date) + ' - Number of entries after the join with the base: ' + str(interactions_ono.count())
    
    bucket_list.extend(['num_calls'])

    for c in bucket_list:
        interactions_ono = interactions_ono.withColumnRenamed(c, c + suffix)

    # interactions_ono.show()

    base = base.select('msisdn').distinct()

    interactions_ono = base.join(interactions_ono, ['msisdn'], 'left_outer').na.fill(0)

    return interactions_ono

def get_nif_ccc_period_attributes(spark, start_date, end_date, base, suffix=""):

    ccc_attributes = get_ccc_period_attributes(spark, start_date, end_date, base, suffix)

    
    print '[Info get_nif_ccc_period_attributes] printSchema of ccc_attributes below'

    ccc_attributes.printSchema()

    mapper = base\
    .select('msisdn', 'NIF_CLIENTE')\
    .filter((~isnull(col('msisdn'))) & (~isnull(col('NIF_CLIENTE'))) & (~col('msisdn').isin('', ' ')) & (~col('NIF_CLIENTE').isin('', ' ')))\
    .dropDuplicates()

    # Checking the mapper

    mapper_size = mapper.count()

    mapper_num_nifs = mapper.select('NIF_CLIENTE').distinct().count()

    print '[Info get_nif_ccc_period_attributes] Size of the mapper: ' + str(mapper_size) + ' - Num NIFs in the mapper: ' + str(mapper_num_nifs)

    ccc_attributes = ccc_attributes.join(mapper, ['msisdn'], 'inner')

    # Adding the NIF and computing the aggregates

    allcols = ccc_attributes.columns

    featcols = [c for c in allcols if (not (c.lower() in ['msisdn', 'nif_cliente', 'rgu', 'num_cliente']))]

    aggs = [sql_sum(col(c)).alias(c) for c in featcols]

    nif_ccc_attributes = ccc_attributes\
    .groupBy('NIF_CLIENTE')\
    .agg(*aggs)

    print '[Info get_nif_ccc_period_attributes] CCC attributes at NIF level computed for the period ' + str(start_date) + ' -> ' + str(end_date) + ' - Size of the output DF: ' + str(nif_ccc_attributes.count())

    return nif_ccc_attributes

def get_customer_base(spark, date_):

    # Customer base at the beginning of the period

    day_ = date_[6:8]

    month_ = date_[4:6]

    year_ = date_[0:4]

    customerDF = spark.read.option("mergeSchema", True)\
    .parquet("/data/udf/vf_es/amdocs_ids/customer/year=" + str(year_) + "/month=" + str(int(month_)) + "/day=" + str(int(day_)))

    serviceDF = spark.read.option("mergeSchema", True)\
    .parquet("/data/udf/vf_es/amdocs_ids/service/year=" + str(year_) + "/month=" + str(int(month_)) + "/day=" + str(int(day_)))\
    .withColumn("rgu", regexp_replace(col("rgu"), "bam_mobile", "bam-movil"))\
    .withColumn("rgu", regexp_replace(col("rgu"), "mobile", "movil"))

    mobilebase = customerDF\
    .join(serviceDF, "NUM_CLIENTE", "inner")\
    .select('msisdn')\
    .filter((~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')))\
    .dropDuplicates()

    print "[Info Trigger Identification] Number of mobile service in the base of " + date_ + ": " + str(mobilebase.count())

    return mobilebase

def get_nif_aggregates(spark, df, date_):

    day_ = str(int(date_[6:8]))

    month_ = str(int(date_[4:6]))

    year_ = date_[0:4]

    allcols = df.columns

    featcols = [c for c in allcols if c != 'msisdn']

    # Getting the mapper msisdn-NIF

    '''

    customerDF = spark.read.option("mergeSchema", True).parquet("/data/udf/vf_es/amdocs_ids/customer/year=" + year_ + "/month=" + month_ + "/day=" + day_)

    serviceDF = spark.read.option("mergeSchema", True).parquet("/data/udf/vf_es/amdocs_ids/service/year=" + year_ + "/month=" + month_ + "/day=" + day_)

    mapper = customerDF\
    .join(serviceDF, ["NUM_CLIENTE"], "inner")\
    .select('msisdn', 'NIF_CLIENTE')\
    .filter((~isnull(col('msisdn'))) & (~isnull(col('NIF_CLIENTE'))) & (~col('msisdn').isin('', ' ')) & (~col('NIF_CLIENTE').isin('', ' ')))\
    .dropDuplicates()
    
    '''

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    mapper = get_customer_base(spark, date_) \
        .select('msisdn', 'NIF_CLIENTE') \
        .filter((~isnull(col('msisdn'))) & (~isnull(col('NIF_CLIENTE'))) & (~col('msisdn').isin('', ' ')) & (~col('NIF_CLIENTE').isin('', ' '))) \
        .dropDuplicates()

    # Adding the NIF and computing the aggregates

    aggs = [sql_sum(col(c)).alias(c) for c in featcols]

    df = df\
    .join(mapper, ['msisdn'], 'inner')\
    .groupBy('NIF_CLIENTE')\
    .agg(*aggs)

    return df

def get_nif_ccc_attributes(spark, end_date):

    #sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    #sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    #from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment

    #base = get_customer_base_segment(spark, end_date).filter(col('rgu')=='mobile').select('msisdn').distinct()

    #base = get_customer_base(spark, end_date)

    # Interactions during the specified period

    #ccc_attributes = get_ccc_attributes(spark, end_date, base)

    #nif_ccc_attributes = get_nif_aggregates(spark, ccc_attributes, end_date)

    nif_ccc_attributes = get_nif_ccc_attributes_df(spark, end_date)

    year_ = str(end_date[0:4])
    month_ = str(int(end_date[4:6]))
    day_ = str(int(end_date[6:8]))

    nif_ccc_attributes\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/data/attributes/vf_es/trigger_analysis/ccc/year=" + year_ + "/month=" + month_ + "/day=" + day_)

    num_entries = nif_ccc_attributes.count()

    return num_entries

def get_nif_ccc_attributes_df(spark, end_date):

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment

    base = get_customer_base_segment(spark, end_date)\
        .filter(col('rgu')=='mobile')\
        .select('msisdn')\
        .distinct()

    print'Size of base' + str(base.count())

    #base = get_customer_base(spark, end_date)

    # Interactions during the spaecified period

    ccc_attributes = get_ccc_attributes(spark, end_date, base)
    print'Size of ccc_attributes' + str(ccc_attributes.count())

    nif_ccc_attributes = get_nif_aggregates(spark, ccc_attributes, end_date)
    print'Size of nif_ccc_attributes' + str(nif_ccc_attributes.count())

    return nif_ccc_attributes