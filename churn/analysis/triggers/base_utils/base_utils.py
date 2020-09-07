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
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import datetime as dt
from pykhaos.utils.date_functions import *

def get_service_last_date(spark):
    last_date = spark \
        .read \
        .parquet('/data/raw/vf_es/customerprofilecar/SERVICESOW/1.2/parquet/') \
        .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .select(max(col('mydate')).alias('last_date')) \
        .rdd \
        .first()['last_date']

    return int(last_date)


def get_customer_last_date(spark):
    last_date = spark \
        .read \
        .parquet('/data/raw/vf_es/customerprofilecar/CUSTOMEROW/1.0/parquet/') \
        .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .select(max(col('mydate')).alias('last_date')) \
        .rdd \
        .first()['last_date']

    return int(last_date)


def get_last_date(spark):
    last_srv = get_service_last_date(spark)

    last_cus = get_customer_last_date(spark)

    last_date = str(min([last_srv, last_cus]))

    return last_date


def _is_null_date(fecha):
    YEAR_NULL_TERADATA = 1753
    """
    As default null date in Teradata source is 1753, this function compares
    a given date with this value to identify null dates
    :param fecha:
    :return: True when provided date has 1753 as year
    """
    return year(fecha) == YEAR_NULL_TERADATA


def get_customers_insights(spark, date_, add_columns=None):
    closing_day = date_

    data_customer_fields = ['NUM_CLIENTE', \
                            'CLASE_CLI_COD_CLASE_CLIENTE', \
                            'COD_ESTADO_GENERAL', \
                            'NIF_CLIENTE', \
                            'X_CLIENTE_PRUEBA', \
                            'NIF_FACTURACION', \
                            'X_FECHA_MIGRACION', \
                            'SUPEROFERTA',\
                            'year', \
                            'month', \
                            'day']

    if add_columns:
        data_customer_fields = list(set(data_customer_fields) | set(add_columns))

    path_raw_base = '/data/raw/vf_es/'
    PATH_RAW_CUSTOMER = path_raw_base + 'customerprofilecar/CUSTOMEROW/1.0/parquet/'

    data_customer_ori = (spark.read.parquet(PATH_RAW_CUSTOMER).where(
        concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    # We use window functions to avoid duplicates of NUM_CLIENTE
    w = Window().partitionBy("NUM_CLIENTE").orderBy(desc("year"), desc("month"), desc("day"))
    data_customer = (data_customer_ori[data_customer_fields]
                     .where(data_customer_ori['COD_ESTADO_GENERAL'].isin('01', '03', '07', '09'))
                     .where(col("CLASE_CLI_COD_CLASE_CLIENTE").isin('NE', 'RV', 'DA', 'BA', 'RS'))
                     .where( ~((col("X_CLIENTE_PRUEBA") == '1') | (col("X_CLIENTE_PRUEBA") == 1) | ((col("NIF_CLIENTE").rlike('^999')) & (~(col("TIPO_DOCUMENTO").rlike("(?i)Pasaporte"))))))
                     .withColumnRenamed('X_FECHA_MIGRACION', 'FECHA_MIGRACION')
                     .withColumn("rowNum", row_number().over(w))
                     .drop('year', 'month', 'day')
                     )

    data_customer = data_customer.where(col('rowNum') == 1).drop('rowNum')

    # For those clients without NIF, take NIF_FACTURACION if exists
    data_customer = (data_customer.withColumn('NIF_CLIENTE',
                                              when((col('NIF_CLIENTE') == '') & (col('NIF_FACTURACION') != '0'),
                                                   col('NIF_FACTURACION')).otherwise(data_customer['NIF_CLIENTE']))
                     )

    return data_customer


def get_customers(spark, date_, add_columns=None):
    closing_day = date_

    data_customer_fields = ['NUM_CLIENTE', \
                            'CLASE_CLI_COD_CLASE_CLIENTE', \
                            'COD_ESTADO_GENERAL', \
                            'NIF_CLIENTE', \
                            'X_CLIENTE_PRUEBA', \
                            'NIF_FACTURACION', \
                            'X_FECHA_MIGRACION', \
                            'SUPEROFERTA',\
                            'year', \
                            'month', \
                            'day']

    if add_columns:
        data_customer_fields = list(set(data_customer_fields) | set(add_columns))

    path_raw_base = '/data/raw/vf_es/'
    PATH_RAW_CUSTOMER = path_raw_base + 'customerprofilecar/CUSTOMEROW/1.0/parquet/'

    data_customer_ori = (spark.read.parquet(PATH_RAW_CUSTOMER).where(
        concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    # We use window functions to avoid duplicates of NUM_CLIENTE
    w = Window().partitionBy("NUM_CLIENTE").orderBy(desc("year"), desc("month"), desc("day"))
    data_customer = (data_customer_ori[data_customer_fields]
                     .where(data_customer_ori['COD_ESTADO_GENERAL'].isin('01', '03', '07', '09'))
                     .where(col("CLASE_CLI_COD_CLASE_CLIENTE").isin('NE', 'RV', 'DA', 'BA', 'RS'))
                     .where((col("X_CLIENTE_PRUEBA").isNull()) | (col("X_CLIENTE_PRUEBA") != '1'))
                     .withColumnRenamed('X_FECHA_MIGRACION', 'FECHA_MIGRACION')
                     .withColumn("rowNum", row_number().over(w))
                     .drop('year', 'month', 'day')
                     )

    data_customer = data_customer.where(col('rowNum') == 1).drop('rowNum')

    # For those clients without NIF, take NIF_FACTURACION if exists
    data_customer = (data_customer.withColumn('NIF_CLIENTE',
                                              when((col('NIF_CLIENTE') == '') & (col('NIF_FACTURACION') != '0'),
                                                   col('NIF_FACTURACION')).otherwise(data_customer['NIF_CLIENTE']))
                     )

    return data_customer


def get_services(spark, date_):
    closing_day = date_

    path_raw_base = '/data/raw/vf_es/'
    LOC_RT_PATH = '/data/udf/vf_es/ref_tables/amdocs_ids/'
    LOC_RT_EXPORT_MAP = LOC_RT_PATH + 'RBL_EXPORT_MAP.TXT'
    LOC_RT_PARAM_OW_SERVICES = LOC_RT_PATH + 'PARAM_OW_SERVICES.TXT'
    PATH_RAW_SERVICE = path_raw_base + 'customerprofilecar/SERVICESOW/1.2/parquet/'
    PATH_RAW_SERVICE_PRICE = path_raw_base + 'priceplanstariffs/PLANPRIC_ONO/1.0/parquet/'

    data_mapper = spark.read.csv(LOC_RT_EXPORT_MAP, sep='|', header=True)

    data_service_ori = (spark.read.parquet(PATH_RAW_SERVICE)
                        .where(
        concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    data_serviceprice_ori = (spark.read.parquet(PATH_RAW_SERVICE_PRICE)
                             .where(
        concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
                             )

    w_srv = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

    data_service_ori_norm = (data_service_ori
                             .withColumn("rowNum", row_number().over(w_srv))
                             .where(col('rowNum') == 1)
                             )

    w_srv_price = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

    data_serviceprice_ori_norm = (data_serviceprice_ori
                                  .withColumn("rowNum", row_number().over(w_srv_price))
                                  .withColumnRenamed('OBJID', 'OBJID2PRICE')
                                  .withColumnRenamed('PRICE', 'PRICE2PRICE')
                                  .where(col('rowNum') == 1)
                                  )

    data_service_param = spark.read.csv(LOC_RT_PARAM_OW_SERVICES, sep='\t', header=True)
    data_service_param = (data_service_param.where(col('rgu').isNotNull())
                          .withColumn('rgu', when(data_service_param['rgu'] == 'bam-movil', 'bam_mobile')
                                      .when(data_service_param['rgu'] == 'movil', 'mobile')
                                      .otherwise(data_service_param['rgu']))
                          )

    ClosingDay_date = dt.date(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    yesterday = ClosingDay_date + dt.timedelta(days=-1)

    data_service_tmp1_basic = (data_service_ori_norm[
                                   'OBJID', 'NUM_CLIENTE', 'NUM_SERIE', 'COD_SERVICIO', 'ESTADO_SERVICIO', 'FECHA_INST', 'FECHA_CAMB', 'INSTANCIA', 'PRIM_RATE', 'SERVICIOS_ACTUALES2PRICE', 'year', 'month', 'day']
                               .where(
        ((to_date(col('FECHA_CAMB')) >= yesterday)) | (col('FECHA_CAMB').isNull()) | (_is_null_date(col('FECHA_CAMB'))))
                               .withColumn("Instancia_P", trim(split(data_service_ori_norm.INSTANCIA, '\\.')[0]))
                               .withColumn("Instancia_S", split(data_service_ori_norm.INSTANCIA, '\\.')[1])
                               .withColumn("TACADA",
                                           concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
                               .join(
        data_service_param['COD_SERVICIO', 'DESC_SERVICIO', 'RGU', 'RGU_MOBILE', 'RGU_BAM', 'TIPO', 'LEGACY'],
        ["COD_SERVICIO"], 'inner')
                               .join(data_serviceprice_ori_norm['OBJID2PRICE', 'PRICE2PRICE'],
                                     col('SERVICIOS_ACTUALES2PRICE') == col('OBJID2PRICE'), 'leftouter')
                               .withColumn('MSISDN',
                                           when((col('Instancia_S').isNull() & (col('COD_SERVICIO') == 'TVOTG')),
                                                concat(lit('FICT_TVOTG_'), data_service_ori_norm.NUM_CLIENTE))
                                           .when((col('Instancia_S').isNull() & (col('COD_SERVICIO') != 'TVOTG')),
                                                 data_service_ori_norm.NUM_SERIE)
                                           .otherwise(lit(None)))
                               .withColumn('SERV_PRICE',
                                           when((col('PRIM_RATE').isNull()) | (trim(col('PRIM_RATE')) == ''),
                                                data_serviceprice_ori_norm.PRICE2PRICE.cast('double'))
                                           .otherwise(data_service_ori_norm.PRIM_RATE.cast('double')))
                               )

    data_service_tmp2_basic = (data_service_tmp1_basic
        .groupBy('NUM_CLIENTE', 'Instancia_P')
        .agg(
        max(col('MSISDN')).alias("MSISDN")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("SRV_BASIC")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.DESC_SERVICIO)
              .otherwise(None)).alias("DESC_SRV_BASIC")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.OBJID)
              .otherwise(None)).alias("OBJID")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.TACADA)
              .otherwise(None)).alias("TACADA")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_SRV_BASIC")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_SRV_BASIC")
        , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.RGU)
              .otherwise(None)).alias("RGU")
        , max(when((col('TIPO') == 'SIM'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TIPO_SIM")
        , max(when((col('TIPO') == 'SIM'), data_service_tmp1_basic.NUM_SERIE)
              .otherwise(None)).alias("IMSI")
        , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TARIFF")
        , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TARIFF")
        , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.DESC_SERVICIO)
              .otherwise(None)).alias("DESC_TARIFF")
        , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TARIFF")
        , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("VOICE_TARIFF")
        , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_VOICE_TARIFF")
        , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_VOICE_TARIFF")
        , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DATA")
        , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DATA")
        , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DATA")
        , max(when((col('TIPO').isin('DTO_NIVEL1')), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DTO_LEV1")
        , max(when((col('TIPO').isin('DTO_NIVEL1')), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DTO_LEV1")
        , max(when((col('TIPO') == 'DTO_NIVEL1'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DTO_LEV1")
        , max(when((col('TIPO').isin('DTO_NIVEL2')), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DTO_LEV2")
        , max(when((col('TIPO').isin('DTO_NIVEL2')), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DTO_LEV2")
        , max(when((col('TIPO') == 'DTO_NIVEL2'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DTO_LEV2")
        , max(when((col('TIPO').isin('DTO_NIVEL3')), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DTO_LEV3")
        , max(when((col('TIPO').isin('DTO_NIVEL3')), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DTO_LEV3")
        , max(when((col('TIPO') == 'DTO_NIVEL3'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DTO_LEV3")
        , max(when((col('TIPO').isin('DATOS_ADICIONALES')), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DATA_ADDITIONAL")
        , max(when((col('TIPO').isin('DATOS_ADICIONALES')), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DATA_ADDITIONAL")
        , max(when((col('TIPO') == 'DATOS_ADICIONALES'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DATA_ADDITIONAL")
        , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("OOB")
        , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_OOB")
        , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_OOB")
        , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("NETFLIX_NAPSTER")
        , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_NETFLIX_NAPSTER")
        , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_NETFLIX_NAPSTER")
        , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("ROAMING_BASIC")
        , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_ROAMING_BASIC")
        , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_ROAMING_BASIC")
        , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("ROAM_USA_EUR")
        , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_ROAM_USA_EUR")
        , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_ROAM_USA_EUR")
        , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("ROAM_ZONA_2")
        , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_ROAM_ZONA_2")
        , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_ROAM_ZONA_2")
        , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("CONSUM_MIN")
        , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_CONSUM_MIN")
        , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_CONSUM_MIN")
        , max(when(col('COD_SERVICIO') == 'SIMVF', 1)
              .otherwise(0)).alias("SIM_VF")
        , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("HOMEZONE")
        , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_HOMEZONE")
        , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_HOMEZONE")
        , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.NUM_SERIE)
              .otherwise(None)).alias("MOBILE_HOMEZONE")
        , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("FBB_UPGRADE")
        , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_FBB_UPGRADE")
        , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_FBB_UPGRADE")
        , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DECO_TV")
        , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DECO_TV")
        , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DECO_TV")
        , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.NUM_SERIE)
              .otherwise(None)).alias("NUM_SERIE_DECO_TV")
        , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.OBJID)
              .otherwise(None)).alias("OBJID_DECO_TV")
        , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_CUOTA_ALTA")
        , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_CUOTA_ALTA")
        , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_CUOTA_ALTA")
        , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_TARIFF")
        , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_TARIFF")
        , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_TARIFF")
        , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_CUOT_CHARGES")
        , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_CUOT_CHARGES")
        , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_CUOT_CHARGES")
        , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_PROMO")
        , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_PROMO")
        , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_PROMO")
        , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_PROMO_USER")
        , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_PROMO_USER")
        , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_PROMO_USER")
        , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_ABONOS")
        , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_ABONOS")
        , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_ABONOS")
        , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_LOYALTY")
        , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_LOYALTY")
        , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_LOYALTY")
        , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TV_SVA")
        , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TV_SVA")
        , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TV_SVA")
        , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("FOOTBALL_TV")
        , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_FOOTBALL_TV")
        , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_FOOTBALL_TV")
        , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("MOTOR_TV")
        , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_MOTOR_TV")
        , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_MOTOR_TV")
        , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PVR_TV")
        , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PVR_TV")
        , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PVR_TV")
        , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("ZAPPER_TV")
        , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_ZAPPER_TV")
        , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_ZAPPER_TV")
        , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TRYBUY_TV")
        , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TRYBUY_TV")
        , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TRYBUY_TV")
        , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TRYBUY_AUTOM_TV")
        , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TRYBUY_AUTOM_TV")
        , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TRYBUY_AUTOM_TV")

        , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("CINE")
        , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_CINE")
        , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_CINE")

        , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("SERIES")
        , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_SERIES")
        , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_SERIES")

        , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("SERIEFANS")
        , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_SERIEFANS")
        , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_SERIEFANS")

        , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("SERIELOVERS")
        , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_SERIELOVERS")
        , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_SERIELOVERS")

        , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("CINEFANS")
        , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_CINEFANS")
        , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_CINEFANS")

        , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PEQUES")
        , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PEQUES")
        , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PEQUES")

        , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("DOCUMENTALES")
        , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_DOCUMENTALES")
        , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_DOCUMENTALES")

        , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("HBO")
        , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_HBO")
        , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_HBO")

        , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PROMO_HBO")
        , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PROMO_HBO")
        , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PROMO_HBO")

        , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("FILMIN")
        , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_FILMIN")
        , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_FILMIN")

        , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PROMO_FILMIN")
        , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PROMO_FILMIN")
        , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PROMO_FILMIN")

        # VODAFONE PASS
        , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("VIDEOHDPASS")
        , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_VIDEOHDPASS")
        , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_VIDEOHDPASS")

        , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("MUSICPASS")
        , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_MUSICPASS")
        , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_MUSICPASS")

        , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("VIDEOPASS")
        , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_VIDEOPASS")
        , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_VIDEOPASS")

        , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("SOCIALPASS")
        , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_SOCIALPASS")
        , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_SOCIALPASS")

        , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("MAPSPASS")
        , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_MAPSPASS")
        , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_MAPSPASS")

        , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("CHATPASS")
        , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_CHATPASS")
        , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_CHATPASS")

        , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("AMAZON")
        , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_AMAZON")
        , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_AMAZON")

        , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PROMO_AMAZON")
        , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PROMO_AMAZON")
        , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PROMO_AMAZON")

        , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("TIDAL")
        , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_TIDAL")
        , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_TIDAL")

        , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.COD_SERVICIO)
              .otherwise(None)).alias("PROMO_TIDAL")
        , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.FECHA_INST)
              .otherwise(None)).alias("FX_PROMO_TIDAL")
        , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.SERV_PRICE)
              .otherwise(None)).alias("PRICE_PROMO_TIDAL")
    )
    )

    cols_tv_charges = ['PRICE_SRV_BASIC', \
                       'PRICE_TV_CUOT_CHARGES', \
                       'PRICE_TV_CUOTA_ALTA', \
                       'PRICE_DECO_TV', \
                       'PRICE_TV_TARIFF', \
                       'PRICE_TV_PROMO', \
                       'PRICE_TV_PROMO_USER', \
                       'PRICE_TV_LOYALTY', \
                       'PRICE_TV_SVA', \
                       'PRICE_TV_ABONOS', \
                       'PRICE_TRYBUY_AUTOM_TV', \
                       'PRICE_TRYBUY_TV', \
                       'PRICE_ZAPPER_TV', \
                       'PRICE_PVR_TV', \
                       'PRICE_MOTOR_TV', \
                       'PRICE_FOOTBALL_TV', \
                       'PRICE_CINE', \
                       'PRICE_SERIES', \
                       'PRICE_SERIEFANS', \
                       'PRICE_SERIELOVERS', \
                       'PRICE_CINEFANS', \
                       'PRICE_PEQUES', \
                       'PRICE_DOCUMENTALES']

    cols_mobile_charges = ['PRICE_SRV_BASIC', \
                           'PRICE_TARIFF', \
                           'PRICE_DTO_LEV1', \
                           'PRICE_DTO_LEV2', \
                           'PRICE_DTO_LEV3', \
                           'PRICE_DATA', \
                           'PRICE_VOICE_TARIFF', \
                           'PRICE_DATA_ADDITIONAL', \
                           'PRICE_OOB', \
                           'PRICE_NETFLIX_NAPSTER', \
                           'PRICE_ROAM_USA_EUR', \
                           'PRICE_ROAMING_BASIC', \
                           'PRICE_ROAM_ZONA_2', \
                           'PRICE_CONSUM_MIN']

    data_service_tmp3_basic = (data_service_tmp2_basic
                               .withColumn('TV_TOTAL_CHARGES_PREV',
                                           sum(coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_tv_charges))
                               .withColumn('MOBILE_BAM_TOTAL_CHARGES_PREV', sum(
        coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_mobile_charges))
                               )

    w_srv_2 = Window().partitionBy("NUM_CLIENTE", "MSISDN").orderBy(desc("TACADA"))

    data_service_tmp4_basic = (data_service_tmp3_basic
                               .withColumn("rowNum", row_number().over(w_srv_2))
                               .where(col('rowNum') == 1)
                               )

    w_srv_3 = Window().partitionBy("MSISDN").orderBy(desc("TACADA"))

    data_service_tmp5_basic = (data_service_tmp4_basic
                               .withColumn("rowNum", row_number().over(w_srv_3))
                               .where(col('rowNum') == 1)
                               .join(data_mapper, (col("OBJID") == col("CAMPO3")), 'leftouter')
                               )

    data_service_tmp6_basic = (data_service_tmp5_basic
                               .withColumn('flag_msisdn_err',
                                           when(col('msisdn').like('% '), 1)
                                           .otherwise(0))
                               .withColumn('msisdn', trim(col('msisdn')))
                               .where(col('msisdn').isNotNull())
                               .where(col('msisdn') != '')
                               .withColumn('TV_TOTAL_CHARGES',
                                           when(col('rgu') == 'tv', data_service_tmp5_basic.TV_TOTAL_CHARGES_PREV)
                                           .otherwise(0))
                               .withColumn('MOBILE_BAM_TOTAL_CHARGES',
                                           when(col('rgu').isin('bam', 'bam_mobile', 'mobile'),
                                                data_service_tmp5_basic.MOBILE_BAM_TOTAL_CHARGES_PREV)
                                           .otherwise(0))
                               .drop(col('TV_TOTAL_CHARGES_PREV'))
                               .drop(col('MOBILE_BAM_TOTAL_CHARGES_PREV'))
                               .drop(col('rowNum'))
                               )

    return data_service_tmp6_basic

def get_active_services_insights(spark, closing_day, customer_cols=None, service_cols=None):
    '''

    :param spark:
    :param closing_day:
    :param new: not used; kept for compatibility with 'churn.datapreparation.general.data_loader.get_active_services'
    :param customer_cols: if not specified, ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"] are taken
    :param service_cols: if not specified, ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"] are taken
    :return:
    '''

    customer_cols_ = ["num_cliente", "nif_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"]
    service_cols_ = ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"]

    if not customer_cols:
        customer_cols = customer_cols_
    if not service_cols:
        service_cols = service_cols_

    sel_cols = list(set(customer_cols) | set(service_cols) | set(customer_cols_) | set(service_cols_))

    customers_df = get_customers_insights(spark, closing_day) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))

    services_df = get_services(spark, closing_day) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \

    base_df = customers_df \
        .join(services_df, 'NUM_CLIENTE', 'inner') \
        .filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente").isin('NE','RV','DA','BA','RS')) & (col("cod_estado_general").isin( '01','03','07','09')) & (col("srv_basic").isin("MRPD1", "MRSUI", "MPPD2", "MRIOE", "MPSUI", "MVPS3", "MPIOE"))) \
        .select(sel_cols) \
        .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \
        .distinct()

    # Some homezone are exluded by using the TARIFF code
    base_df = base_df.where(((col("TARIFF").isNull()) | ((~col("TARIFF").isin("DTVC2", "DTVC5")))))

    return base_df



def get_active_services(spark, closing_day, new, customer_cols=None, service_cols=None):
    '''

    :param spark:
    :param closing_day:
    :param new: not used; kept for compatibility with 'churn.datapreparation.general.data_loader.get_active_services'
    :param customer_cols: if not specified, ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"] are taken
    :param service_cols: if not specified, ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"] are taken
    :return:
    '''
    if not customer_cols:
        customer_cols = ["num_cliente", "nif_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"]
    if not service_cols:
        service_cols = ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"]

    sel_cols = list(set(customer_cols + service_cols))

    customers_df = get_customers(spark, closing_day) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))

    services_df = get_services(spark, closing_day) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \

    base_df = customers_df \
        .join(services_df, 'NUM_CLIENTE', 'inner') \
        .filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("srv_basic").isin("MRSUI", "MPSUI") == False)) \
        .select(sel_cols) \
        .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \
        .distinct()

    return base_df

    '''

    df_customer = (amdocs_table_reader(spark, "customer", closing_day, new)
                   .where(col("clase_cli_cod_clase_cliente") == "RS")  # customer
                   .where(col("cod_estado_general").isin(["01", "09"]))  # customer
                   .select(customer_cols)
                   .withColumnRenamed("num_cliente", "num_cliente_customer"))

    df_service = (amdocs_table_reader(spark, "service", closing_day, new)
                  .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
                  .where(col("rgu").isNotNull())
                  .select(service_cols)
                  .withColumnRenamed("num_cliente", "num_cliente_service"))

    df_services = df_customer.join(df_service,
                                   on=(df_customer["num_cliente_customer"] == df_service["num_cliente_service"]),
                                   how="inner")  # intersection

    #print("df_customer&df_service", df_services.count())
    
    

    return df_services
    '''


def get_customer_base(spark, date_, add_columns = None, active_filter = True, add_columns_customer=None, code = "99"):
    # Filtros aplicados en su generación:
        # COD_ESTADO_GENERAL IN('01', '03', '07', '09')
        # CLASE_CLI_COD_CLASE_CLIENTE IN('NE', 'RV', 'DA', 'BA', 'RS’)
        # X_CLIENTE_PRUEBA <> 1 OR CLI.X_CLIENTE_PRUEBA IS NULL
        # WHERE(X_CLIENTE_PRUEBA=1 OR(nif_cliente LIKE'999%' AND TIPO_DOCUMENTO <> 'PASAPORTE'))
        # TARIFA NOT IN('DTVC5', 'DTVC2')

    customers_df = get_customers(spark, date_, add_columns=add_columns_customer) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))

    print("Got columns from customers: '{}'".format(",".join(customers_df.columns)))

    services_df = get_services(spark, date_) \
        .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))).filter(col('rgu') != 'prepaid')

    # Build segment_nif at this point

    add_columns_customer = [] if not add_columns_customer else add_columns_customer
    add_columns = [] if not add_columns else add_columns

    add_columns = list(set(add_columns)  | set(add_columns_customer))

    print("Requested additional columns: '{}'".format(",".join(add_columns)))


    select_cols = ['NUM_CLIENTE', 'NIF_CLIENTE', 'msisdn', 'campo2', 'rgu'] + add_columns if add_columns else ['NUM_CLIENTE', 'NIF_CLIENTE', 'msisdn', 'campo2', 'rgu']

    base_tmp_df = customers_df \
        .join(services_df, 'NUM_CLIENTE', 'inner') \

    # Better with the structure base_df = base_tmp_df... if(condition) else base_tmp_df

    if(active_filter):
        base_df = base_tmp_df.filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09", code)) & (col("srv_basic").isin("MRSUI", "MPSUI") == False)) \
        .select(select_cols) \
        .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \
        .distinct()
    else:
        base_df = base_tmp_df.filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente") == "RS") & (col("srv_basic").isin("MRSUI", "MPSUI") == False)) \
            .select(select_cols) \
            .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' '))) \
            .distinct()

    return base_df



def get_customer_base_segment(spark, date_, add_columns = None, active_filter = True):
    VALUES_CUST_LEVEL_AGG = ['fbb', 'mobile', 'tv', 'prepaid', 'bam_mobile', 'fixed', 'bam']

    raw_base_df = get_customer_base(spark, date_, add_columns, active_filter)

    data_CAR_CUST_tmp2 = (raw_base_df
                          .groupby('NIF_CLIENTE')
                          .pivot('rgu', VALUES_CUST_LEVEL_AGG)
                          .agg(count(lit(1))).na.fill(0)
                          )

    for c in VALUES_CUST_LEVEL_AGG:
        data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.withColumnRenamed(c, c + '_services_nif')

    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2 \
        .join(raw_base_df.select('NIF_CLIENTE').distinct(), ['NIF_CLIENTE'], 'inner')

    # Flag Prepaid by NIF
    # data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.withColumn('flag_prepaid_nif', when(data_CAR_CUST_tmp2['prepaid_services_nif'] > 0, lit(1)).otherwise(lit(0)))

    # Pospaid segments by NIF
    # Postpaid mobile, and optionally with prepaid mobile, and no FBB
    mo_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] > 0) & (
                data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (
                                   data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (
                                   data_CAR_CUST_tmp2['fixed_services_nif'] == 0) & (
                                   data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Standalone FBB: FBB, and no postpaid mobile
    fbb_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (
                data_CAR_CUST_tmp2['fbb_services_nif'] > 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (
                                    data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (
                                    data_CAR_CUST_tmp2['fixed_services_nif'] >= 0) & (
                                    data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # FBB, and optionally with pre or postpaid mobile
    co_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] > 0) & (
                data_CAR_CUST_tmp2['fbb_services_nif'] > 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] >= 0) & (
                                   data_CAR_CUST_tmp2['bam_mobile_services_nif'] >= 0) & (
                                   data_CAR_CUST_tmp2['fixed_services_nif'] >= 0) & (
                                   data_CAR_CUST_tmp2['bam_services_nif'] >= 0)

    # Only fixed
    fixed_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (
                data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (
                                      data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (
                                      data_CAR_CUST_tmp2['fixed_services_nif'] > 0) & (
                                      data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Pure prepaid: Prepaid, and no postpaid
    pre_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (
                data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] > 0) & (
                                    data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (
                                    data_CAR_CUST_tmp2['fixed_services_nif'] == 0) & (
                                    data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Others

    data_CAR_CUST_tmp2 = (data_CAR_CUST_tmp2.withColumn('seg_pospaid_nif', when(mo_condition_nif,  'Mobile_only')
                                                                         .when(fbb_condition_nif, 'Standalone_FBB')
                                                                         .when(co_condition_nif,  'Convergent')
                                                                         .when(pre_condition_nif, 'Pure_prepaid')
                                                                        .when(fixed_condition_nif, 'Only_fixed')
                                                                        .otherwise('Other')))

    segment_base_df = raw_base_df \
        .join(data_CAR_CUST_tmp2.select('NIF_CLIENTE', 'seg_pospaid_nif', 'fbb_services_nif', 'mobile_services_nif', 'tv_services_nif', 'prepaid_services_nif', 'bam_mobile_services_nif', 'fixed_services_nif', 'bam_services_nif'), ['NIF_CLIENTE'], 'inner')

    segment_base_df = segment_base_df.withColumn('nb_rgus_nif', col('fbb_services_nif') + col('mobile_services_nif') + col('bam_mobile_services_nif') + col('fixed_services_nif') + col('bam_services_nif'))

    '''

    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2\
    .withColumn('seg_pospaid_nif', when(mo_condition_nif,  'Mobile_only')\
        .when(fbb_condition_nif, 'Standalone_FBB')\
        .when(co_condition_nif,  'Convergent')\
        .when(pre_condition_nif, 'Pure_prepaid')\
        .otherwise('Other'))
    '''

    return segment_base_df

# Copied to customer_base
def get_customer_base_attributes(spark, date_, days_before = 90, add_columns = None, active_filter = True, verbose = False):

    from pykhaos.utils.date_functions import move_date_n_days

    prev_date = move_date_n_days(date_, n=-days_before)

    sel_cols = ['NIF_CLIENTE', 'seg_pospaid_nif', 'nb_rgus_nif', 'fbb_services_nif', 'mobile_services_nif', 'tv_services_nif', 'prepaid_services_nif', 'bam_mobile_services_nif', 'fixed_services_nif', 'bam_services_nif']

    ref_base = get_customer_base_segment(spark, date_, add_columns, active_filter).select(sel_cols).distinct()

    if(verbose):
        print '[Info get_customer_base_attributes] Number of elements in ref_base is ' + str(ref_base.count()) + ' - Num distinct NIFs in ref_base is ' + str(ref_base.select('nif_cliente').distinct().count())

    prev_base = get_customer_base_segment(spark, prev_date, add_columns, active_filter).select(sel_cols).distinct()

    if(verbose):
        print '[Info get_customer_base_attributes] Number of elements in prev_base is ' + str(prev_base.count()) + ' - Num distinct NIFs in prev_base is ' + str(prev_base.select('nif_cliente').distinct().count())

    # Renaming prev_base columns

    ren_cols = list(set(prev_base.columns) - set(['NIF_CLIENTE']))

    for c in ren_cols:
        prev_base = prev_base.withColumnRenamed(c, 'prev_' + c)

    fill_na_map = {'prev_seg_pospaid_nif': 'no_prev_segment', 'prev_nb_rgus_nif': 0, 'prev_fbb_services_nif': 0, 'prev_mobile_services_nif': 0, 'prev_tv_services_nif': 0, 'prev_prepaid_services_nif': 0, 'prev_bam_mobile_services_nif': 0, 'prev_fixed_services_nif': 0, 'prev_bam_services_nif': 0}

    ref_base = ref_base\
        .join(prev_base, ['nif_cliente'], 'left')\
        .na.fill(fill_na_map)

    # Adding attributes

    for c in list(set(sel_cols) - set(['NIF_CLIENTE', 'seg_pospaid_nif'])):
        ref_base = ref_base.withColumn('inc_' + c, col(c) - col('prev_' + c))

    ref_base = ref_base\
        .withColumn('flag_segment_changed', when(col('seg_pospaid_nif') != col('prev_seg_pospaid_nif'), 1.0).otherwise(lit(0.0)))\
        .withColumn('flag_new_customer', when(col('prev_seg_pospaid_nif') == 'no_prev_segment', 1.0).otherwise(lit(0.0)))\
        .withColumn('flag_dx', when((col('inc_fbb_services_nif') < 0) | (col('inc_mobile_services_nif') < 0) | (col('inc_tv_services_nif') < 0), 1.0).otherwise(lit(0.0)))

    return ref_base

def get_service_base(spark, date_):
    base = get_customer_base(spark, date_).select('msisdn').distinct()

    return base


def get_nif_base(spark, date_):
    base = get_customer_base(spark, date_).select('nif_cliente').distinct()

    return base

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

def get_churn_target_nif_gap(spark, closing_day, churn_window=30, gap_window=5):

    start_port = closing_day
    from pykhaos.utils.date_functions import move_date_n_days
    end_port = move_date_n_days(closing_day, n=churn_window + gap_window)

    # Getting portout requests for fix and mobile services, and disconnections of fbb services
    print'Obtaining portouts between {} and {}'.format(start_port, end_port)
    df_sopo_fix = get_fix_portout_requests(spark, start_port, end_port)
    df_baja_fix = get_fbb_dxs(spark,start_port, end_port)
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

# Copied to use-cases/churn_nrt/src/data/sopos_dxs.py
def get_fbb_dxs(spark, yearmonthday, yearmonthday_target):
    #get_customer_base(spark, date_, add_columns=None, active_filter=True, add_columns_customer=None, code="99")

    current_base = get_customer_base(spark, yearmonthday)\
        .filter(col('rgu')=='fbb')\
        .select("msisdn")\
        .distinct()\
        .repartition(400)

    target_base = get_customer_base(spark, yearmonthday_target, add_columns=None, active_filter=True, add_columns_customer=None, code="03") \
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

# Copied to use-cases/churn_nrt/src/data/sopos_dxs.py
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

# Copied to use-cases/churn_nrt/src/data/sopos_dxs.py
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

#copied to customers_data
def get_active_filter(spark, date_, n_days, level='msisdn', verbose = False):

    if('nif' in level):
        level='nif_cliente'

    valid_rgus = ['mobile'] if level=='msisdn' else ['fbb', 'mobile', 'tv', 'bam_mobile', 'fixed', 'bam']

    # REMOVING RECENT CUSTOMERS (EARLY CHURN EFFECT IS REMOVED)

    # Only MSISDN active for at leats the last N days

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    from pykhaos.utils.date_functions import move_date_n_days

    prev_date_ = move_date_n_days(date_, -n_days)

    current_base_df = get_customer_base(spark, date_).filter(col('rgu').isin(valid_rgus)).select(level).distinct()

    size_current_base = current_base_df.count()

    prev_base_df = get_customer_base(spark, prev_date_).filter(col('rgu').isin(valid_rgus)).select(level).distinct()

    #size_prev_base = prev_base_df.count()

    active_base_df = current_base_df.join(prev_base_df, [level], 'inner')

    size_active_base = active_base_df.count()

    if(verbose):
        print '[Info get_active_filter] Services retained after the active filter for ' + str(date_) + ' and ' + str(n_days) + ' is ' + str(size_active_base) + ' out of ' + str(size_current_base)

    return active_base_df

#copied to customers_data
def get_disconnection_process_filter(spark, date_, n_days, verbose = False):

    # REMOVING CUSTOMERS WHO HAVE INITITED THE DISCONNECTION PROCESS

    base_df = get_customer_base_attributes(spark, date_, days_before=n_days)

    # Retaining NIFs with >= number of mobile & >= number of FBBs & >= number of TVs
    # 'fbb_services_nif', 'mobile_services_nif', 'tv_services_nif'

    non_churning_df = base_df.filter((col('inc_fbb_services_nif') >= 0) & (col('inc_mobile_services_nif') >= 0) & (col('inc_tv_services_nif') >= 0)).select('nif_cliente').distinct()

    if(verbose):
        print '[Info get_disconnection_process_filter] Customers retained after the disconnection process filter is ' + str(non_churning_df.count()) + ' out of ' + str(base_df.count())

    return non_churning_df

#copied to customers_data
def get_churn_call_filter(spark, date_, n_days, verbose=False):

    from churn.analysis.triggers.ccc_utils.ccc_utils import get_ccc_period_attributes

    from pykhaos.utils.date_functions import move_date_n_days

    prev_date_ = move_date_n_days(date_, -n_days)

    base = get_customer_base_segment(spark, date_).filter(col('rgu') == 'mobile').select('msisdn').distinct()

    ccc_df = get_ccc_period_attributes(spark, prev_date_, date_, base)\
        .filter(col('CHURN_CANCELLATIONS')==0)

    no_churn_call_df = ccc_df.select('msisdn').distinct()

    if (verbose):
        print '[Info get_disconnection_process_filter] Services retained after the churn call filter is ' + str(no_churn_call_df.count()) + ' out of ' + str(base.count())

    return no_churn_call_df

