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
from pyspark.sql import Row, DataFrame, Column, Window
import datetime
import pandas as pd

def load_locations(spark):

    locs_df = (
        spark \
        .read \
        .parquet("/data/udf/vf_es/locations/vodafone_analytics/vodafone_analytics_administrative_boundary_utm/")\
        .withColumn('location_name', when(col('location_name').like('%laga'), 'malaga')
                    .when(col('location_name').like('Badajoz'), 'badajoz')
                    .when(col('location_name').like('Madrid'), 'madrid')
                    .when(col('location_name').like('Asturias'), 'asturias')
                    .when(col('location_name').like('%Valencia'), 'valencia')
                    .when(col('location_name').like('Cuenca'), 'cuenca')
                    .when(col('location_name').like('Burgos'), 'burgos')
                    .when(col('location_name').like('Cantabria'), 'cantabria')
                    .when(col('location_name').like('Murcia'), 'murcia')
                    .when(col('location_name').like('Araba%'), 'alava')
                    .when(col('location_name').like('Ciudad Real'), 'ciudadreal')
                    .when(col('location_name').like('Zaragoza'), 'zaragoza')
                    .when(col('location_name').like('Ceuta'), 'ceuta')
                    .when(col('location_name').like('%vila'), 'avila')
                    .when(col('location_name').like('Illes Balears'), 'baleares')
                    .when(col('location_name').like('Tarragona'), 'tarragona')
                    .when(col('location_name').like('Zamora'), 'zamora')
                    .when(col('location_name').like('Segovia'), 'segovia')
                    .when(col('location_name').like('Granada'), 'granada')
                    .when(col('location_name').like('Barcelona'), 'barcelona')
                    .when(col('location_name').like('Toledo'), 'toledo')
                    .when(col('location_name').like('Guadalajara'), 'guadalajara')
                    .when(col('location_name').like('Lugo'), 'lugo')
                    .when(col('location_name').like('Gipuzkoa'), 'guipuzcoa')
                    .when(col('location_name').like('Albacete'), 'albacete')
                    .when(col('location_name').like('%rdoba'), 'cordoba')
                    .when(col('location_name').like('Palencia'), 'palencia')
                    .when(col('location_name').like('Ourense'), 'orense')
                    .when(col('location_name').like('Almer%'), 'almeria')
                    .when(col('location_name').like('Le%'), 'leon')
                    .when(col('location_name').like('Teruel'), 'teruel')
                    .when(col('location_name').like('Pontevedra'), 'pontevedra')
                    .when(col('location_name').like('Girona'), 'gerona')
                    .when(col('location_name').like('Navarra'), 'navarra')
                    .when(col('location_name').like('Lleida'), 'lerida')
                    .when(col('location_name').like('Huesca'), 'huesca')
                    .when(col('location_name').like('%ceres'), 'caceres')
                    .when(col('location_name').like('%licante'), 'alicante')
                    .when(col('location_name').like('Bizkaia'), 'vizcaya')
                    .when(col('location_name').like('%Palmas'), 'laspalmas')
                    .when(col('location_name').like('%diz'), 'cadiz')
                    .when(col('location_name').like('Sevilla'), 'sevilla')
                    .when(col('location_name').like('%Rioja'), 'larioja')
                    .when(col('location_name').like('%Coru'), 'lacoruna')
                    .when(col('location_name').like('%Tenerife'), 'tenerife')
                    .when(col('location_name').like('Salamanca'), 'salamanca')
                    .when(col('location_name').like('%Castell'), 'castellon')
                    .when(col('location_name').like('Ja%'), 'jaen')
                    .when(col('location_name').like('Soria'), 'soria')
                    .when(col('location_name').like('Huelva'), 'huelva')
                    .when(col('location_name').like('Melilla'), 'melilla')
                    .when(col('location_name').like('Valladolid'), 'valladolid')
                    .otherwise(col('location_name')))
               )

    '''
    
    +----------------------+
    |location_name         |
    +----------------------+
    |Málaga                |
    |Badajoz               |
    |Madrid                |
    |Asturias              |
    |València/Valencia     |
    |Cuenca                |
    |Burgos                |
    |Cantabria             |
    |Murcia                |
    |Araba/Álava           |
    |Ciudad Real           |
    |Zaragoza              |
    |Ceuta                 |
    |Ávila                 |
    |Illes Balears         |
    |Tarragona             |
    |Zamora                |
    |Segovia               |
    |Granada               |
    |Barcelona             |
    |Toledo                |
    |Guadalajara           |
    |Lugo                  |
    |Gipuzkoa              |
    |Albacete              |
    |Córdoba               |
    |Palencia              |
    |Ourense               |
    |Almería               |
    |León                  |
    |Teruel                |
    |Pontevedra            |
    |Girona                |
    |Navarra               |
    |Lleida                |
    |Huesca                |
    |Cáceres               |
    |Alacant/Alicante      |
    |Bizkaia               |
    |Las Palmas            |
    |Cádiz                 |
    |Sevilla               |
    |La Rioja              |
    |A Coruña              |
    |Santa Cruz de Tenerife|
    |Salamanca             |
    |Castelló/Castellón    |
    |Jaén                  |
    |Soria                 |
    |Huelva                |
    |Melilla               |
    |Valladolid            |
    +----------------------+
    
    '''

    return locs_df

def get_last_date_from_cell_data(spark, date_):

    return str(spark
               .read
               .parquet('/data/udf/vf_es/probability_location_given_cell/1.0/parquet/estimation_type=area_intersection/')
               .withColumn("date", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
               .filter(from_unixtime(unix_timestamp(col('date'), 'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(date_), 'yyyyMMdd')))
               .select(sql_max("date").alias('last_date')).first()['last_date'])

def get_last_date_from_age_data(spark, date_):

    return str(spark
               .read
               .parquet('/data/attributes/vf_es/mobileline_attributes/int/time_agg=monthly/attribute_name=MobileLineAge/')
               .withColumn("date", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
               .filter(from_unixtime(unix_timestamp(col('date'), 'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(date_), 'yyyyMMdd')))
               .select(sql_max("date").alias('last_date')).first()['last_date'])

def get_age_data(spark, date_):

    '''
    The function gets the estimated age from the source used in VF analytics (that is the reason why this functins is placed in mobility utils)
    :param spark:
    :param date_:
    :return:
    '''

    age_data_date = get_last_date_from_age_data(spark, date_)

    print "[Info] Getting age info available for date " + age_data_date

    year_age_ = str(int(age_data_date[0:4]))
    month_age_ = str(int(age_data_date[4:6]))
    day_age_ = str(int(age_data_date[6:8]))

    df = spark\
        .read\
        .parquet('/data/attributes/vf_es/mobileline_attributes/int/time_agg=monthly/attribute_name=MobileLineAge/year=' + year_age_ + '/month=' + month_age_ + '/day=' + day_age_)\
        .select('id', 'id_name', 'attribute_value')\
        .filter(col('attribute_value') >= 18)\
        .withColumn('age_group', when(col('attribute_value') <= 24, lit('18_24'))
                    .when((col('attribute_value') > 24) & (col('attribute_value') <= 34), lit('25_34'))
                    .when((col('attribute_value') > 34) & (col('attribute_value') <= 44), lit('35_44'))
                    .when((col('attribute_value') > 44) & (col('attribute_value') <= 54), lit('45_54'))
                    .when((col('attribute_value') > 54) & (col('attribute_value') <= 64), lit('55_64'))
                    .when((col('attribute_value') > 64), lit('65_')))\
        .filter(col('id_name')=='msisdn')\
        .withColumnRenamed('attribute_value', 'estimated_age')\
        .withColumnRenamed('id', 'msisdn')

    return df

def get_cells_location2(spark, date_):

    # location:

    # type=2, subtype=1, level=ElectoralDistrict (SC)
    # type=2, subtype=2, level=Neighbourhood (N)
    # type=2, subtype=3, level=Post - Code (CP)
    # type=2, subtype=4, level=Municipality (M)
    # type=2, subtype=5, level=Province (P)

    #levels = {'SC': (2, 1), 'N': (2, 2), 'CP': (2, 3), 'M': (2, 4), 'P': (2, 5)}
    #(type, subtype) = levels['SC']

    year_ = str(int(date_[0:4]))
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))

    # CIG + location id as unique identifier: one cell and several locations (each with a different probability)
    # When joining with Netscout (CIG), the location with the highest probability must be selected

    cell_window = Window\
        .partitionBy('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id', 'location_type', 'location_subtype')\
        .orderBy(desc('probability'))

    cell_data_date = get_last_date_from_cell_data(spark, date_)

    year_cell_ = str(int(cell_data_date[0:4]))
    month_cell_ = str(int(cell_data_date[4:6]))
    day_cell_ = str(int(cell_data_date[6:8]))

    cell_df = (spark \
        .read \
        .parquet(
        '/data/udf/vf_es/probability_location_given_cell/1.0/parquet/estimation_type=area_intersection/year=' + year_cell_ + '/month=' + month_cell_ + '/day=' + day_cell_)\
        .filter((col('location_type')==2) & (col("location_subtype").isin(1, 5, 4)))\
        .withColumn('cell_identifier', regexp_replace(col('cell_identifier'), '__', '_')) \
        .withColumn('cig', split(col('cell_identifier'), '_')) \
        .withColumn('cell_mcc', col("cig").getItem(0)) \
        .withColumn('cell_mnc', col("cig").getItem(1)) \
        .withColumn('cell_area', col("cig").getItem(2)) \
        .withColumn('cell_id', col("cig").getItem(3)) \
        .withColumn('cell_mcc', regexp_replace('cell_mcc', r'^[0]*', '')) \
        .withColumn('cell_mnc', regexp_replace('cell_mnc', r'^[0]*', '')) \
        .withColumn('cell_area', regexp_replace('cell_area', r'^[0]*', '')) \
        .withColumn('cell_id', regexp_replace('cell_id', r'^[0]*', '')) \
        .select('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id', 'location_type', 'location_subtype', 'location_id', 'probability')\
        .withColumn('order', row_number().over(cell_window))\
        .filter(col('order') == 1)\
        .select('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id', 'location_id', 'location_type', 'location_subtype'))

    # Location id as unique identifier

    locs_df = load_locations(spark)\
        .filter((col('location_type')==2) & (col("location_subtype").isin(1, 5, 4)))\

    # Adding location properties to cells

    cell_loc_df = cell_df.join(locs_df, ['location_id', 'location_type', 'location_subtype'], 'inner')\
        .withColumn('tmp', concat(col('location_type'), lit('_'), col('location_subtype')))\
        .groupBy('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id')\
        .pivot('tmp', ["2_5", "2_1", "2_4"])\
        .agg(first('location_name'))\
        .withColumnRenamed("2_1", "electoral_district")\
        .withColumnRenamed("2_5", "province")\
        .withColumnRenamed("2_4", "municipality")

    return cell_loc_df