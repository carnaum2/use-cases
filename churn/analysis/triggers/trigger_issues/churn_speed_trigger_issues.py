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
                                    trim,
                                    array,
                                    create_map)
from pyspark.sql import Row, DataFrame, Column, Window
#from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
#from pyspark.ml import Pipeline
#from pyspark.ml.classification import RandomForestClassifier
#from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
#from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
#from pyspark.mllib.evaluation import BinaryClassificationMetrics
#from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import datetime as dt
from itertools import chain
import re
from pykhaos.utils.date_functions import *
from trigger_issues import *

def amdocs_table_reader(spark, table_name, closing_day, new=True, verbose=False):
    if verbose:
        print("amdocs_table_reader", table_name, closing_day, new)
    path_car = "amdocs_inf_dataset" if new else "amdocs_ids"
    table_name = '/data/udf/vf_es/{}/{}/year={}/month={}/day={}'.format(path_car, table_name, int(closing_day[:4]),
                                                                                                    int(closing_day[4:6]),
                                                                                                    int(closing_day[6:]))
    if verbose:
        print("Loading {}".format(table_name))
    df_src = spark.read.load(table_name)
    return df_src

def get_active_services(spark, closing_day, new, customer_cols=None, service_cols=None):
    '''

    :param spark:
    :param closing_day:
    :param new:
    :param customer_cols: if not specified, ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"] are taken
    :param service_cols: if not specified, ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"] are taken
    :return:
    '''
    if not customer_cols:
        customer_cols = ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"]
    if not service_cols:
        service_cols = ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"]

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


def getFbbDxsForCycleList(spark, yearmonthday, yearmonthday_target):

    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    convert_to_date_udf = udf(lambda z: convert_to_date(z), StringType())

    portout_date = move_date_n_days(yearmonthday, 15)

    # If the day is the last one of the month we take the client list of the last cycle of the previous month
    if (yearmonthday[6:8]=='01'):
        yearmonthday= move_date_n_cycles(yearmonthday, n=-1)

    current_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday
    target_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday_target

    current_base = spark\
    .read\
    .parquet(current_source)\
    .filter(col("ClosingDay") == yearmonthday)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    target_base = spark\
    .read\
    .parquet(target_source)\
    .filter(col("ClosingDay") == yearmonthday_target)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    churn_base = current_base\
    .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn").withColumn("label_dx", lit(1.0))\
    .distinct()\
    .withColumn('portout_date_dx', from_unixtime(unix_timestamp(lit(portout_date), 'yyyyMMdd')))

    print("[Info getFbbDxsForMonth] " + time.ctime() + " DXs for FBB services during the period: " + yearmonthday + "-"+yearmonthday_target+": " + str(churn_base.count()))

    return churn_base

def getFixPortRequestsForCycleList(spark, yearmonthday, yearmonthday_target):

    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    yearmonthday_obj = convert_to_date(yearmonthday)
    yearmonthday_target_obj = convert_to_date(yearmonthday_target)

    fixport = spark.read.table("raw_es.portabilitiesinout_portafijo") \
        .filter(col("INICIO_RANGO") == col("FIN_RANGO")) \
        .withColumnRenamed("INICIO_RANGO", "msisdn") \
        .select("msisdn", "FECHA_INSERCION_SGP") \
        .distinct() \
        .withColumn("label_srv", lit(1.0)) \
        .withColumn("FECHA_INSERCION_SGP", substring(col("FECHA_INSERCION_SGP"), 0, 10))\
        .withColumn('FECHA_INSERCION_SGP', from_unixtime(unix_timestamp(col('FECHA_INSERCION_SGP'), "yyyy-MM-dd")))\
        .where((col('FECHA_INSERCION_SGP') >= from_unixtime(unix_timestamp(lit(yearmonthday), "yyyyMMdd"))) & (col('FECHA_INSERCION_SGP') <= from_unixtime(unix_timestamp(lit(yearmonthday_target), "yyyyMMdd"))))\
        .withColumnRenamed('FECHA_INSERCION_SGP', 'portout_date_fix')\
        .select("msisdn", "label_srv", "portout_date_fix")

    print("[Info getFixPortRequestsForMonth] " + time.ctime() + " Port-out requests for fixed services during period " + yearmonthday + "-"+yearmonthday_target+": " + str(fixport.count()))

    return fixport

def get_mobile_portout_requests(spark, start_port, end_port):

    # mobile portout
    window_mobile = Window.partitionBy("msisdn_a").orderBy(desc("days_from_portout"))  # keep the 1st portout

    #from churn.utils.udf_manager import Funct_to_UDF

    #start_date_obj = Funct_to_UDF.convert_to_date(start_port)
    #end_date_obj = Funct_to_UDF.convert_to_date(end_port)

    #convert_to_date_udf = udf(Funct_to_UDF.convert_to_date, StringType())

    start_date_obj = convert_to_date(start_port)
    end_date_obj = convert_to_date(end_port)

    convert_to_date_udf = udf(lambda z: convert_to_date_udf(z), StringType())

    PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"

    df_sol_port = (spark.read.table(PORT_TABLE_NAME)
        .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))
        .withColumn('sopo_ds_fecha_solicitud', from_unixtime(unix_timestamp(col('sopo_ds_fecha_solicitud'), "yyyy-MM-dd")))
        .where((col("sopo_ds_fecha_solicitud") >= from_unixtime(unix_timestamp(lit(start_port), 'yyyyMMdd'))) & (col("sopo_ds_fecha_solicitud") <= from_unixtime(unix_timestamp(lit(end_port), 'yyyyMMdd'))))
        .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_a")
        .withColumnRenamed("sopo_ds_fecha_solicitud", "portout_date")
        .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(start_port[:4]), lit(start_port[4:6]), lit(start_port[6:])), 'yyyyMMdd')))
        .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
        .withColumn("rank", row_number().over(window_mobile))
        .where(col("rank") == 1))

    df_sol_port = df_sol_port.withColumn("label_mob", lit(1.0)).withColumnRenamed("msisdn_a", "msisdn").select("msisdn", "label_mob", "portout_date").withColumnRenamed("portout_date", "portout_date_mob")

    return df_sol_port

def get_target(spark, closing_day):

    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    start_port = closing_day
    end_port = move_date_n_cycles(closing_day, n=4)

    #- Solicitudes de baja de fijo
    df_sopo_fix = getFixPortRequestsForCycleList(spark, closing_day, end_port)
    #- Porque dejen de estar en la lista de clientes
    df_baja_fix = getFbbDxsForCycleList(spark,closing_day, end_port)

    df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

    

    #from churn.datapreparation.general.data_loader import get_active_services
    df_services = get_active_services(spark, closing_day, new=False, customer_cols=["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "nif_cliente"], service_cols=None)\
    .withColumnRenamed("campo2", "msisdn_d")\
    .withColumnRenamed("num_cliente_customer", "num_cliente")
    #
    #     ['num_cliente_customer',
    #      'cod_estado_general',
    #      'clase_cli_cod_clase_cliente',
    #      'msisdn',
    #      'num_cliente_service',
    #      'campo2',
    #      'rgu',
    #      'srv_basic']

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn_d'], "left")
                    .na.fill({'label_srv': 0.0})
                    .join(df_baja_fix, ['msisdn'], "left")
                    .na.fill({'label_dx': 0.0})
                    .join(df_sol_port, ['msisdn'], "left")
                    .na.fill({'label_mob': 0.0})
                    .withColumn('tmp', when(    (col('label_srv')==1.0) | (col('label_dx')==1.0) | (col('label_mob')==1.0), 1.0).otherwise(0.0))
                    .withColumn('label', sql_max('tmp').over(window_nc))
                    .drop("tmp"))

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'portout_date_mob', 'portout_date_fix', 'portout_date_dx')\
    .withColumn('portout_date_min', least(col('portout_date_mob'), col('portout_date_fix'), col('portout_date_dx')))\
    .withColumn('portout_date', sql_min('portout_date_min').over(window_nc))\
    .select("nif_cliente", "label", 'portout_date').drop_duplicates()


    return df_target_nifs

def get_target2(spark, closing_day):

    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    start_port = closing_day
    end_port = move_date_n_cycles(closing_day, n=4)

    #- Solicitudes de baja de fijo
    df_sopo_fix = getFixPortRequestsForCycleList(spark, closing_day, end_port)
    #- Porque dejen de estar en la lista de clientes
    df_baja_fix = getFbbDxsForCycleList(spark,closing_day, end_port)

    df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

    

    #from churn.datapreparation.general.data_loader import get_active_services
    df_services = get_active_services(spark, closing_day, new=False, customer_cols=["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "nif_cliente"], service_cols=None)\
    .withColumnRenamed("campo2", "msisdn_d")\
    .withColumnRenamed("num_cliente_customer", "num_cliente")
    #
    #     ['num_cliente_customer',
    #      'cod_estado_general',
    #      'clase_cli_cod_clase_cliente',
    #      'msisdn',
    #      'num_cliente_service',
    #      'campo2',
    #      'rgu',
    #      'srv_basic']

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn_d'], "left")
                    .na.fill({'label_srv': 0.0})
                    .join(df_baja_fix, ['msisdn'], "left")
                    .na.fill({'label_dx': 0.0})
                    .join(df_sol_port, ['msisdn'], "left")
                    .na.fill({'label_mob': 0.0})
                    .withColumn('tmp', when(    (col('label_srv')==1.0) | (col('label_dx')==1.0) | (col('label_mob')==1.0), 1.0).otherwise(0.0))
                    .withColumn('label', sql_max('tmp').over(window_nc))
                    .drop("tmp"))

    def get_churn_reason(dates):

        reasons = ['mob', 'fix', 'fbb']

        sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

        reason = reasons[sorted_dates[0]]

        return reason


    get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'portout_date_mob', 'portout_date_fix', 'portout_date_dx')\
    .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc))\
    .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc))\
    .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc))\
    .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx'))\
    .withColumn('reason', get_churn_reason_udf(col('dates')))\
    .withColumn('reason', when(col('label')==0.0, '').otherwise(col('reason')))\
    .withColumn('portout_date_min', least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx')))\
    .withColumn('portout_date', sql_min('portout_date_min').over(window_nc))\
    .select("nif_cliente", "label", 'portout_date', 'reason').drop_duplicates()

    return df_target_nifs

def get_target3(spark, closing_day):

    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    start_port = closing_day
    end_port = move_date_n_cycles(closing_day, n=4)

    #- Solicitudes de baja de fijo
    df_sopo_fix = getFixPortRequestsForCycleList(spark, closing_day, end_port)
    #- Porque dejen de estar en la lista de clientes
    df_baja_fix = getFbbDxsForCycleList(spark,closing_day, end_port)

    df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)



    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
    df_services = get_customer_base_segment(spark, closing_day)

    #from churn.datapreparation.general.data_loader import get_active_services
    #df_services = get_active_services(spark, closing_day, new=False, customer_cols=["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "nif_cliente"], service_cols=None).withColumnRenamed("num_cliente_customer", "num_cliente")
    #
    #     ['num_cliente_customer',
    #      'cod_estado_general',
    #      'clase_cli_cod_clase_cliente',
    #      'msisdn',
    #      'num_cliente_service',
    #      'campo2',
    #      'rgu',
    #      'srv_basic']

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn'], "left")
                    .na.fill({'label_srv': 0.0})
                    .join(df_baja_fix, ['msisdn'], "left")
                    .na.fill({'label_dx': 0.0})
                    .join(df_sol_port, ['msisdn'], "left")
                    .na.fill({'label_mob': 0.0})
                    .withColumn('tmp', when(    (col('label_srv')==1.0) | (col('label_dx')==1.0) | (col('label_mob')==1.0), 1.0).otherwise(0.0))
                    .withColumn('label', sql_max('tmp').over(window_nc))
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

if __name__ == "__main__":

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession\
        .builder\
        .appName("Trigger identification")\
        .master("yarn")\
        .config("spark.submit.deployMode", "client")\
        .config("spark.ui.showConsoleProgress", "true")\
        .enableHiveSupport().getOrCreate())


    #date_ = get_last_date(spark)

    #date_ = sys.argv[1]

    date_ = '20190521'

    starting_date = move_date_n_days(date_, -30, str_fmt="%Y%m%d")

    ############## TRIGGER DF FOR THE SPECIFIED DATE ##############

    base_df = get_customer_base_segment(spark, date_)

    print '[Info trigger_issues] printSchema of base_df below'

    base_df.printSchema()

    averias_4w = averias_with_dates(spark, starting_date, date_)

    print '[Info trigger_issues] printSchema of averias_4w below'

    averias_4w.printSchema()

    ccc_4w = get_nif_ccc_period_attributes(spark, starting_date, date_, base_df)

    print '[Info trigger_issues] printSchema of ccc_4w below'

    ccc_4w.printSchema()

    nif_base_df = base_df.select('NIF_CLIENTE', 'seg_pospaid_nif').distinct()

    ids_tmp = nif_base_df.join(averias_4w, ['nif_cliente'], 'left_outer').na.fill(0)

    ids = ids_tmp.join(ccc_4w, ['nif_cliente'], 'left_outer').na.fill(0)

    print '[Info trigger_issues] Size of base_df: ' + str(base_df.count()) + ' - Number of NIFs in base_df: ' + str(base_df.select('NIF_CLIENTE').distinct().count())

    print '[Info trigger_issues] Size of nif_base_df: ' + str(nif_base_df.count()) + ' - Number of NIFs in nif_base_df: ' + str(nif_base_df.select('NIF_CLIENTE').distinct().count())

    print '[Info trigger_issues] Size of ids_tmp: ' + str(ids_tmp.count()) + ' - Number of NIFs in ids_tmp: ' + str(ids_tmp.select('NIF_CLIENTE').distinct().count())

    print '[Info trigger_issues] Size of ids: ' + str(ids.count()) + ' - Number of NIFs in ids: ' + str(ids.select('NIF_CLIENTE').distinct().count())

    ids.show()

    trigger_df = ids\
    .filter((col('CHURN_CANCELLATIONS')==0) & (col('seg_pospaid_nif').isin('Mobile_only', 'Convergent')) & (col('NUM_AVERIAS_NIF') >= 3))\
    .withColumn('scoring', col('NUM_AVERIAS_NIF'))

    trigger_df.printSchema()

    trigger_df.show()

    df_target = get_target3(spark, date_)

    df_target.show()

    print 'Volume of df_target: ' + str(df_target.count()) + ' - Num NIFs in df_target: ' + str(df_target.select('nif_cliente').distinct().count())

    lab_trigger_df = trigger_df.join(df_target, ['nif_cliente'], 'inner')\
    .withColumn('days_to_port', datediff(col('portout_date'), col('last_issue')))\

    lab_trigger_df.printSchema()

    lab_trigger_df.show()

    h = lab_trigger_df\
    .filter(col('label')==1.0)\
    .select('days_to_port')\
    .rdd\
    .map(lambda r: r['days_to_port'])\
    .histogram(20)

    bucket_edges = h[0]

    bucket_mid = []

    for i in range(1, len(bucket_edges)):
        l = bucket_edges[(i-1):(i+1)]
        bucket_mid.append(float(sum(l))/float(len(l)))

    hist_count = h[1]

    for i in range(len(hist_count)):
        print '[Info histogram] Bucket: ' + str(bucket_mid[i]) + ' - Hist: ' + str(hist_count[i])

    lab_trigger_df\
    .repartition(1)\
    .write\
    .csv('trigger_issues_speed_rate_' + date_, sep = '|', header = True)
    


    '''
    #########

    df78=spark.read.parquet('/data/attributes/vf_es/trigger_analysis/hclust_prediction').filter(col('prediction').isin(7,8)).select('nif_cliente')

    date_ = '20190414'

    df_target = get_target(spark, date_)

    df_target.show()

    print 'Volume: ' + str(df_target.count()) + ' - Num NIFs: ' + str(df_target.select('nif_cliente').distinct().count())

    df78_t = df78.join(df_target, ['nif_cliente'], 'inner')

    print 'Volume: ' +str(df78_t.count()) + ' - Num nulls: ' + str(df78_t.filter(~isnull('portout_date')).count())

    df78_t.withColumn('ref_date', from_unixtime(unix_timestamp(lit(date_), 'yyyyMMdd'))).withColumn('days_to_port', datediff(col('portout_date'), col('ref_date')).cast('int')).describe('days_to_port').show()
    '''











