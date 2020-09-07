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
                                  coalesce)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
#from pyspark.ml import Pipeline
#from pyspark.ml.classification import RandomForestClassifier
#from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
#from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
#from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import datetime as dt
from pykhaos.utils.date_functions import *
from churn.analysis.triggers.ccc_utils.ccc_utils import *

def get_service_last_date(spark):

  last_date = spark\
  .read\
  .parquet('/data/raw/vf_es/customerprofilecar/SERVICESOW/1.1/parquet/')\
  .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
  .select(max(col('mydate')).alias('last_date'))\
  .rdd\
  .first()['last_date']

  return int(last_date)

def get_customer_last_date(spark):

  last_date = spark\
  .read\
  .parquet('/data/raw/vf_es/customerprofilecar/CUSTOMEROW/1.0/parquet/')\
  .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
  .select(max(col('mydate')).alias('last_date'))\
  .rdd\
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

def get_customers(spark, date_):

    closing_day = date_

    data_customer_fields = ['NUM_CLIENTE',\
    'CLASE_CLI_COD_CLASE_CLIENTE',\
    'COD_ESTADO_GENERAL',\
    'NIF_CLIENTE',\
    'X_CLIENTE_PRUEBA',\
    'NIF_FACTURACION',\
    'X_FECHA_MIGRACION',\
    'year',\
    'month',\
    'day']

    path_raw_base = '/data/raw/vf_es/'
    PATH_RAW_CUSTOMER = path_raw_base + 'customerprofilecar/CUSTOMEROW/1.0/parquet/'

    data_customer_ori = (spark.read.parquet(PATH_RAW_CUSTOMER).where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

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
    PATH_RAW_SERVICE = path_raw_base + 'customerprofilecar/SERVICESOW/1.1/parquet/'
    PATH_RAW_SERVICE_PRICE = path_raw_base + 'priceplanstariffs/PLANPRIC_ONO/1.0/parquet/'

    data_mapper = spark.read.csv(LOC_RT_EXPORT_MAP, sep='|', header = True)

    data_service_ori = (spark.read.parquet(PATH_RAW_SERVICE)
                    .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    data_serviceprice_ori = (spark.read.parquet(PATH_RAW_SERVICE_PRICE)
                         .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
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

    data_service_param = spark.read.csv(LOC_RT_PARAM_OW_SERVICES, sep='\t', header = True)
    data_service_param = (data_service_param.where(col('rgu').isNotNull())
        .withColumn('rgu', when(data_service_param['rgu'] == 'bam-movil', 'bam_mobile')
            .when(data_service_param['rgu'] == 'movil', 'mobile')
            .otherwise(data_service_param['rgu']))
        )

    ClosingDay_date = dt.date(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    yesterday = ClosingDay_date + dt.timedelta(days=-1)

    data_service_tmp1_basic = (data_service_ori_norm['OBJID', 'NUM_CLIENTE', 'NUM_SERIE', 'COD_SERVICIO', 'ESTADO_SERVICIO', 'FECHA_INST', 'FECHA_CAMB', 'INSTANCIA', 'PRIM_RATE', 'SERVICIOS_ACTUALES2PRICE', 'year', 'month', 'day']
        .where(((to_date(col('FECHA_CAMB')) >= yesterday)) | (col('FECHA_CAMB').isNull()) | (_is_null_date(col('FECHA_CAMB'))))
        .withColumn("Instancia_P", trim(split(data_service_ori_norm.INSTANCIA, '\\.')[0]))
        .withColumn("Instancia_S", split(data_service_ori_norm.INSTANCIA, '\\.')[1])
        .withColumn("TACADA",concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
        .join(data_service_param['COD_SERVICIO', 'DESC_SERVICIO', 'RGU', 'RGU_MOBILE', 'RGU_BAM', 'TIPO', 'LEGACY'],["COD_SERVICIO"], 'inner')
        .join(data_serviceprice_ori_norm['OBJID2PRICE', 'PRICE2PRICE'], col('SERVICIOS_ACTUALES2PRICE') == col('OBJID2PRICE'), 'leftouter')
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

            #VODAFONE PASS
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

    cols_tv_charges = ['PRICE_SRV_BASIC',\
    'PRICE_TV_CUOT_CHARGES',\
    'PRICE_TV_CUOTA_ALTA',\
    'PRICE_DECO_TV',\
    'PRICE_TV_TARIFF',\
    'PRICE_TV_PROMO',\
    'PRICE_TV_PROMO_USER',\
    'PRICE_TV_LOYALTY',\
    'PRICE_TV_SVA',\
    'PRICE_TV_ABONOS',\
    'PRICE_TRYBUY_AUTOM_TV',\
    'PRICE_TRYBUY_TV',\
    'PRICE_ZAPPER_TV',\
    'PRICE_PVR_TV',\
    'PRICE_MOTOR_TV',\
    'PRICE_FOOTBALL_TV',\
    'PRICE_CINE',\
    'PRICE_SERIES',\
    'PRICE_SERIEFANS',\
    'PRICE_SERIELOVERS',\
    'PRICE_CINEFANS',\
    'PRICE_PEQUES',\
    'PRICE_DOCUMENTALES']

    cols_mobile_charges = ['PRICE_SRV_BASIC',\
    'PRICE_TARIFF',\
    'PRICE_DTO_LEV1',\
    'PRICE_DTO_LEV2',\
    'PRICE_DTO_LEV3',\
    'PRICE_DATA',\
    'PRICE_VOICE_TARIFF',\
    'PRICE_DATA_ADDITIONAL',\
    'PRICE_OOB',\
    'PRICE_NETFLIX_NAPSTER',\
    'PRICE_ROAM_USA_EUR',\
    'PRICE_ROAMING_BASIC',\
    'PRICE_ROAM_ZONA_2',\
    'PRICE_CONSUM_MIN']

    data_service_tmp3_basic = (data_service_tmp2_basic
                           .withColumn('TV_TOTAL_CHARGES_PREV', sum(coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_tv_charges))
                           .withColumn('MOBILE_BAM_TOTAL_CHARGES_PREV', sum(coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_mobile_charges))
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
                                       when(col('rgu').isin('bam', 'bam_mobile', 'mobile'),data_service_tmp5_basic.MOBILE_BAM_TOTAL_CHARGES_PREV)
                                       .otherwise(0))
                           .drop(col('TV_TOTAL_CHARGES_PREV'))
                           .drop(col('MOBILE_BAM_TOTAL_CHARGES_PREV'))
                           .drop(col('rowNum'))
                          )

    return data_service_tmp6_basic

def get_customer_base(spark, date_):

    customers_df = get_customers(spark, date_)\
    .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))

    services_df = get_services(spark, date_)\
    .filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))\

    # Build segment_nif at this point

    base_df = customers_df\
    .join(services_df, 'NUM_CLIENTE', 'inner')\
    .filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("srv_basic").isin("MRSUI", "MPSUI") == False))\
    .select('NUM_CLIENTE', 'NIF_CLIENTE', 'msisdn', 'campo2', 'rgu')\
    .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))\
    .distinct()

    return base_df

def get_customer_base_segment(spark, date_):

    VALUES_CUST_LEVEL_AGG = ['fbb', 'mobile', 'tv', 'prepaid', 'bam_mobile', 'fixed', 'bam']

    raw_base_df = get_customer_base(spark, date_)

    data_CAR_CUST_tmp2=(raw_base_df
                        .groupby('NIF_CLIENTE')
                        .pivot('rgu', VALUES_CUST_LEVEL_AGG)
                        .agg(count(lit(1))).na.fill(0)
                        )
    
    for c in VALUES_CUST_LEVEL_AGG:
        data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.withColumnRenamed(c, c + '_services_nif')
    
    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2\
    .join(raw_base_df.select('NIF_CLIENTE').distinct(), ['NIF_CLIENTE'], 'inner')

    # Flag Prepaid by NIF
    #data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.withColumn('flag_prepaid_nif', when(data_CAR_CUST_tmp2['prepaid_services_nif'] > 0, lit(1)).otherwise(lit(0)))

    # Pospaid segments by NIF
    # Postpaid mobile, and optionally with prepaid mobile, and no FBB
    mo_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] > 0) & (data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fixed_services_nif'] == 0) & (data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Standalone FBB: FBB, and no postpaid mobile
    fbb_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fbb_services_nif'] > 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fixed_services_nif'] >= 0) & (data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # FBB, and optionally with pre or postpaid mobile
    co_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] > 0) & (data_CAR_CUST_tmp2['fbb_services_nif'] > 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] >= 0) & (data_CAR_CUST_tmp2['bam_mobile_services_nif'] >= 0) & (data_CAR_CUST_tmp2['fixed_services_nif'] >= 0) & (data_CAR_CUST_tmp2['bam_services_nif'] >= 0)
    
    # Only fixed
    fixed_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] == 0) & (data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fixed_services_nif'] > 0) & (data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Pure prepaid: Prepaid, and no postpaid
    pre_condition_nif = (data_CAR_CUST_tmp2['mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fbb_services_nif'] == 0) & (data_CAR_CUST_tmp2['prepaid_services_nif'] > 0) & (data_CAR_CUST_tmp2['bam_mobile_services_nif'] == 0) & (data_CAR_CUST_tmp2['fixed_services_nif'] == 0) & (data_CAR_CUST_tmp2['bam_services_nif'] == 0)

    # Others
    
    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2\
    .withColumn('seg_pospaid_nif',\
        when(mo_condition_nif,  'Mobile_only').otherwise(\
            when(fbb_condition_nif, 'Standalone_FBB').otherwise(\
                when(co_condition_nif,  'Convergent').otherwise(\
                    when(pre_condition_nif, 'Pure_prepaid').otherwise('Other')))))

    segment_base_df = raw_base_df\
    .join(data_CAR_CUST_tmp2.select('NIF_CLIENTE', 'seg_pospaid_nif'), ['NIF_CLIENTE'], 'inner')
        
    '''

    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2\
    .withColumn('seg_pospaid_nif', when(mo_condition_nif,  'Mobile_only')\
        .when(fbb_condition_nif, 'Standalone_FBB')\
        .when(co_condition_nif,  'Convergent')\
        .when(pre_condition_nif, 'Pure_prepaid')\
        .otherwise('Other'))
    '''

    return segment_base_df

def get_service_base(spark, date_):

    base = get_customer_base(spark, date_).select('msisdn').distinct()

    return base

def get_nif_base(spark, date_):

    base = get_customer_base(spark, date_).select('nif_cliente').distinct()

    return base

def averias(spark, starting_day, closing_day):
    
    from pyspark.sql.functions import lpad
    from pyspark.sql.functions import year, month, dayofmonth
    from pyspark.sql.functions import countDistinct
    
    ga_tickets = spark.read.table('raw_es.callcentrecalls_ticketsow')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    ga_franquicia = spark.read.table('raw_es.callcentrecalls_ticketfranchiseow')
    ga_close_case = spark.read.table('raw_es.callcentrecalls_ticketclosecaseow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE')
    ga_tipo_tickets = spark.read.parquet('/data/raw/vf_es/cvm/GATYPETICKETS/1.0/parquet')

    
    
    tickets = (ga_tickets
                     #.withColumn('year', year('CREATION_TIME'))
                     #.withColumn('month', month('CREATION_TIME'))
                     #.withColumn('day', dayofmonth('CREATION_TIME'))
                     .where( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                            &(concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
 
                     .join(ga_tickets_detalle, ga_tickets.OBJID==ga_tickets_detalle.OBJID, 'left_outer')
                     .join(ga_franquicia, ga_tickets_detalle.OBJID==ga_franquicia.ID_FRANQUICIA, 'left_outer')
                     #.join(ga_centro_local, ga_tickets.CENTRO_LOCAL==ga_centro_local.ID_CENTRO_LOCAL, 'left_outer')
                     #.join(ga_centro_regional, ga_tickets.CENTRO_REGIONAL==ga_centro_local.ID_CENTRO_REGIONAL, 'left_outer')
                     .join(ga_close_case, ga_tickets.OBJID==ga_close_case.OBJID, 'left_outer')
                     .join(clientes, ga_tickets.CASE_REPORTER2YESTE==clientes.OBJID, 'left_outer')
                     #.join(ga_severidad, ga_tickets.ID_SEVERIDAD==ga_severidad.ID_SEVERIDAD, 'left_outer')
                     .join(ga_tipo_tickets, ga_tickets.ID_TIPO_TICKET==ga_tipo_tickets.ID_TIPO_TICKET, 'left_outer')
                     .filter(col('NIF_CLIENTE').isNotNull())
                     .filter('NIF_CLIENTE != ""')
                     .filter('NIF_CLIENTE != "7"')
          )
 
    averias = tickets.where("X_TIPO_OPERACION IN ('Averia')")
 
    averias_nif = averias.select('NIF_CLIENTE', 'ID_NUMBER').groupby('NIF_CLIENTE').agg(countDistinct('ID_NUMBER').alias('NUM_AVERIAS_NIF'))
     
    return averias_nif

def averias_with_dates(spark, starting_day, closing_day):
    
    from pyspark.sql.functions import lpad
    from pyspark.sql.functions import year, month, dayofmonth
    from pyspark.sql.functions import countDistinct
    
    ga_tickets = spark.read.table('raw_es.callcentrecalls_ticketsow')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    ga_franquicia = spark.read.table('raw_es.callcentrecalls_ticketfranchiseow')
    ga_close_case = spark.read.table('raw_es.callcentrecalls_ticketclosecaseow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE')
    ga_tipo_tickets = spark.read.parquet('/data/raw/vf_es/cvm/GATYPETICKETS/1.0/parquet')

    
    
    tickets = (ga_tickets
                     #.withColumn('year', year('CREATION_TIME'))
                     #.withColumn('month', month('CREATION_TIME'))
                     #.withColumn('day', dayofmonth('CREATION_TIME'))
                     .where( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                            &(concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
                     .withColumn('date', from_unixtime(unix_timestamp(concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0')), "yyyyMMdd"))    )
 
                     .join(ga_tickets_detalle, ga_tickets.OBJID==ga_tickets_detalle.OBJID, 'left_outer')
                     .join(ga_franquicia, ga_tickets_detalle.OBJID==ga_franquicia.ID_FRANQUICIA, 'left_outer')
                     #.join(ga_centro_local, ga_tickets.CENTRO_LOCAL==ga_centro_local.ID_CENTRO_LOCAL, 'left_outer')
                     #.join(ga_centro_regional, ga_tickets.CENTRO_REGIONAL==ga_centro_local.ID_CENTRO_REGIONAL, 'left_outer')
                     .join(ga_close_case, ga_tickets.OBJID==ga_close_case.OBJID, 'left_outer')
                     .join(clientes, ga_tickets.CASE_REPORTER2YESTE==clientes.OBJID, 'left_outer')
                     #.join(ga_severidad, ga_tickets.ID_SEVERIDAD==ga_severidad.ID_SEVERIDAD, 'left_outer')
                     .join(ga_tipo_tickets, ga_tickets.ID_TIPO_TICKET==ga_tipo_tickets.ID_TIPO_TICKET, 'left_outer')
                     .filter(col('NIF_CLIENTE').isNotNull())
                     .filter('NIF_CLIENTE != ""')
                     .filter('NIF_CLIENTE != "7"')
          )

    # from_unixtime(unix_timestamp(col("FX_CREATE_DATE"), "yyyy-MM-dd"))
 
    averias = tickets.where("X_TIPO_OPERACION IN ('Averia')")
 
    averias_nif = averias\
    .select('NIF_CLIENTE', 'ID_NUMBER', 'date')\
    .groupby('NIF_CLIENTE')\
    .agg(countDistinct('ID_NUMBER').alias('NUM_AVERIAS_NIF'), sql_min(col('date')).alias('first_issue'), max(col('date')).alias('last_issue'))
     
    return averias_nif

def get_next_day_of_the_week(day_of_the_week):

    idx = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    n = idx[day_of_the_week.lower()]

    # import datetime
    d = dt.date.today()

    while d.weekday() != n:
        d += dt.timedelta(1)

    year_= str(d.year)
    month_= str(d.month).rjust(2, '0')
    day_= str(d.day).rjust(2, '0')

    print 'Next ' + day_of_the_week + ' is day ' + str(day_) + ' of month ' + str(month_) + ' of year ' + str(year_)

    return year_+month_+day_
