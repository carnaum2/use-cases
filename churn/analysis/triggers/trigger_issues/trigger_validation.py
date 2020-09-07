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

    # print("df_customer&df_service", df_services.count())

    return df_services


def getFbbDxsForCycleList(spark, yearmonthday, yearmonthday_target):
    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    convert_to_date_udf = udf(lambda z: convert_to_date(z), StringType())

    portout_date = move_date_n_days(yearmonthday, 15)

    # If the day is the last one of the month we take the client list of the last cycle of the previous month
    if (yearmonthday[6:8] == '01'):
        yearmonthday = move_date_n_cycles(yearmonthday, n=-1)

    current_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday
    target_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday_target

    '''
    current_base = spark \
        .read \
        .parquet(current_source) \
        .filter(col("ClosingDay") == yearmonthday) \
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (
        col("rgu").isNotNull())) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn") \
        .distinct() \
        .repartition(400) 
    '''

    current_base = get_customer_base_segment(spark, yearmonthday) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)

    '''
    target_base = spark \
        .read \
        .parquet(target_source) \
        .filter(col("ClosingDay") == yearmonthday_target) \
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (
        col("rgu").isNotNull())) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)
    '''

    target_base = get_customer_base_segment(spark, yearmonthday_target) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)

    churn_base = current_base \
        .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left") \
        .filter(col("tmp").isNull()) \
        .select("msisdn").withColumn("label_dx", lit(1.0)) \
        .distinct() \
        .withColumn('portout_date_dx', from_unixtime(unix_timestamp(lit(portout_date), 'yyyyMMdd')))

    print("[Info getFbbDxsForMonth] " + time.ctime() + " DXs for FBB services during the period: " + yearmonthday + "-" + yearmonthday_target + ": " + str(churn_base.count()))

    return churn_base


def getFixPortRequestsForCycleList(spark, yearmonthday, yearmonthday_target):
    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    yearmonthday_obj = convert_to_date(yearmonthday)
    yearmonthday_target_obj = convert_to_date(yearmonthday_target)

    fixport = spark.read.table("raw_es.portabilitiesinout_portafijo") \
        .filter(col("INICIO_RANGO") == col("FIN_RANGO")) \
        .withColumnRenamed("INICIO_RANGO", "msisdn_d") \
        .select("msisdn_d", "FECHA_INSERCION_SGP") \
        .distinct() \
        .withColumn("label_srv", lit(1.0)) \
        .withColumn("FECHA_INSERCION_SGP", substring(col("FECHA_INSERCION_SGP"), 0, 10)) \
        .withColumn('FECHA_INSERCION_SGP', from_unixtime(unix_timestamp(col('FECHA_INSERCION_SGP'), "yyyy-MM-dd"))) \
        .where((col('FECHA_INSERCION_SGP') >= from_unixtime(unix_timestamp(lit(yearmonthday), "yyyyMMdd"))) & (
                col('FECHA_INSERCION_SGP') <= from_unixtime(unix_timestamp(lit(yearmonthday_target), "yyyyMMdd")))) \
        .withColumnRenamed('FECHA_INSERCION_SGP', 'portout_date_fix') \
        .select("msisdn_d", "label_srv", "portout_date_fix")

    print(
                "[Info getFixPortRequestsForMonth] " + time.ctime() + " Port-out requests for fixed services during period " + yearmonthday + "-" + yearmonthday_target + ": " + str(
            fixport.count()))

    return fixport


def get_mobile_portout_requests(spark, start_port, end_port):
    # mobile portout
    window_mobile = Window.partitionBy("msisdn_a").orderBy(desc("days_from_portout"))  # keep the 1st portout

    # from churn.utils.udf_manager import Funct_to_UDF

    # start_date_obj = Funct_to_UDF.convert_to_date(start_port)
    # end_date_obj = Funct_to_UDF.convert_to_date(end_port)

    # convert_to_date_udf = udf(Funct_to_UDF.convert_to_date, StringType())

    start_date_obj = convert_to_date(start_port)
    end_date_obj = convert_to_date(end_port)

    convert_to_date_udf = udf(lambda z: convert_to_date_udf(z), StringType())

    PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"

    df_sol_port = (spark.read.table(PORT_TABLE_NAME)
                   .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))
                   .withColumn('sopo_ds_fecha_solicitud',
                               from_unixtime(unix_timestamp(col('sopo_ds_fecha_solicitud'), "yyyy-MM-dd")))
                   .where(
        (col("sopo_ds_fecha_solicitud") >= from_unixtime(unix_timestamp(lit(start_port), 'yyyyMMdd'))) & (
                    col("sopo_ds_fecha_solicitud") <= from_unixtime(unix_timestamp(lit(end_port), 'yyyyMMdd'))))
                   .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_a")
                   .withColumnRenamed("sopo_ds_fecha_solicitud", "portout_date")
                   .withColumn("ref_date", from_unixtime(
        unix_timestamp(concat(lit(start_port[:4]), lit(start_port[4:6]), lit(start_port[6:])), 'yyyyMMdd')))
                   .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
                   .withColumn("rank", row_number().over(window_mobile))
                   .where(col("rank") == 1))

    df_sol_port = df_sol_port.withColumn("label_mob", lit(1.0)).withColumnRenamed("msisdn_a", "msisdn").select("msisdn",
                                                                                                               "label_mob",
                                                                                                               "portout_date").withColumnRenamed(
        "portout_date", "portout_date_mob")

    return df_sol_port

def get_target(spark, start_port):
    def convert_to_date(date_, str_fmt="%Y%m%d"):
        date_obj = dt.datetime.strptime(date_, str_fmt)
        return date_obj

    #start_port = closing_day
    #end_port = move_date_n_cycles(closing_day, n=4)
    end_port = move_date_n_days(start_port, 30, str_fmt="%Y%m%d")

    # - Solicitudes de baja de fijo
    df_sopo_fix = getFixPortRequestsForCycleList(spark, start_port, end_port)
    # - Porque dejen de estar en la lista de clientes
    df_baja_fix = getFbbDxsForCycleList(spark, start_port, end_port)

    df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

    # from churn.datapreparation.general.data_loader import get_active_services
    #df_services = get_active_services(spark, closing_day, new=False, customer_cols=["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "nif_cliente"], service_cols=None).withColumnRenamed("campo2", "msisdn_d").withColumnRenamed("num_cliente_customer", "num_cliente")

    df_services = get_customer_base_segment(spark, start_port).withColumnRenamed("campo2", "msisdn_d")

    #
    #     ['num_cliente_customer',]πø}~ §ø [
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
                      .withColumn('tmp',
                                  when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                       1.0).otherwise(0.0))
                      .withColumn('label', sql_max('tmp').over(window_nc))
                      .drop("tmp"))

    def get_churn_reason(dates):

        reasons = ['mob', 'fix', 'fbb']

        sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

        sorted_reasons = [reasons[idx] for idx in sorted_dates if
                          ((dates[idx] is not None) & (dates[idx] != '') & (dates[idx] != ' '))]

        if not sorted_reasons:
            reason = None
        else:
            reason = sorted_reasons[0]

        return reason

    get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'portout_date_mob', 'portout_date_fix',
                                           'portout_date_dx') \
        .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc)) \
        .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc)) \
        .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc)) \
        .select("nif_cliente", "label", 'min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx') \
        .distinct() \
        .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx')) \
        .withColumn('reason', get_churn_reason_udf(col('dates'))) \
        .withColumn('reason', when(col('label') == 0.0, '').otherwise(col('reason'))) \
        .withColumn('portout_date',
                    least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx'))) \
        .select("nif_cliente", "label", 'portout_date', 'reason').drop_duplicates()

    return df_target_nifs


def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))



if __name__ == "__main__":

    # sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    # sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    set_paths()

    from pykhaos.utils.date_functions import *
    from churn.analysis.triggers.trigger_issues.trigger_issues import *

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    # date_ = get_last_date(spark)

    model = sys.argv[1]

    date_ = sys.argv[2]

    print '[Info] Evaluating trigger_' + model + ' computed on ' + date_

    # date_ = '20190711'

    year_ = date_[0:4]

    month_ = date_[4:6]

    day_ = date_[6:8]

    starting_date = move_date_n_days(date_, -30, str_fmt="%Y%m%d")

    # Loading the table with the trigger info

    trigger_df = spark\
        .read\
        .parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_' + model + '/year=' + str(year_) + '/month=' + str(int(month_)) + '/day=' + str(int(day_)))\
        .select('nif')\
        .withColumnRenamed('nif', 'nif_cliente')

    # Label

    df_target = get_target(spark, date_).select('nif_cliente', 'label')

    print '[Info] Volume of df_target: ' + str(df_target.count()) + ' - Num NIFs in df_target: ' + str(df_target.select('nif_cliente').distinct().count())

    lab_trigger_df = trigger_df\
        .join(df_target, ['nif_cliente'], 'left_outer')\
        .na.fill({'label': 0.0})

    print '[Info] Volume of lab_trigger_df: ' + str(lab_trigger_df.count()) + ' - Num NIFs in lab_trigger_df: ' + str(lab_trigger_df.select('nif_cliente').distinct().count())

    # Churn rate

    print '[Info] Churn rate in the trigger: ' + str(lab_trigger_df.select(sql_avg(col('label')).alias('churn_rate')).rdd.first()['churn_rate'])

