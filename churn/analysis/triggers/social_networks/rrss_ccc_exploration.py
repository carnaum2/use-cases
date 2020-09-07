#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
import math
import re
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
                                   create_map,
                                   randn,
                                   lpad,
                                   to_timestamp)

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

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################
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


    #####################

    ga_tickets = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    ga_franquicia = spark.read.table('raw_es.callcentrecalls_ticketfranchiseow')
    ga_close_case = spark.read.table('raw_es.callcentrecalls_ticketclosecaseow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').filter(col('clase_cli_cod_clase_cliente') == 'RS').select('OBJID', 'NIF_CLIENTE').filter(col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')
    ga_tipo_tickets = spark.read.parquet('/data/raw/vf_es/cvm/GATYPETICKETS/1.0/parquet')

    starting_day = "20190601"
    closing_day = "20190630"

    # print'Starting day: ' + starting_day
    # print'Closing day: ' + closing_day
    # print("############ Loaded ticket sources ############")

    # from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    tickets = (ga_tickets
               .where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day) & (concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) >= starting_day))
               .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
               .withColumn('DURACION', regexp_replace('DURACION', ',', '.'))
               .join(ga_tickets_detalle.drop('year', 'month', 'day'), ga_tickets.OBJID == ga_tickets_detalle.OBJID,
                     'left_outer')
               .join(ga_franquicia.drop('year', 'month', 'day'),
                     ga_tickets_detalle.OBJID == ga_franquicia.ID_FRANQUICIA, 'left_outer')
               .join(ga_close_case.drop('year', 'month', 'day', 'X_GRUPO_TRABAJO'),
                     ga_tickets.OBJID == ga_close_case.OBJID, 'left_outer')
               .join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.OBJID, 'inner')
               .join(ga_tipo_tickets.drop('year', 'month', 'day'),
                     ga_tickets.ID_TIPO_TICKET == ga_tipo_tickets.ID_TIPO_TICKET, 'left_outer')
               .filter(col('NIF_CLIENTE').isNotNull())
               .filter(col("X_GRUPO_TRABAJO").isin("SM_Bosch_SAC", "SM_Konecta_SAC", "SM_ARVATO_RRSS", "VIS_EGIPTO_WEBCHAT", "SM_VIS_WEBCHAT"))
               )

    tickets.groupBy("X_GRUPO_TRABAJO").agg(countDistinct("NIF_CLIENTE").alias('num_nifs')).show()

    print '[Info rrss_ccc_exploration] Initial ticket dataset showed above'

    from churn.analysis.triggers.base_utils.base_utils import get_active_filter, get_disconnection_process_filter

    # Modeling filters

    active_filter = get_active_filter(spark, closing_day, 90, 'nif_cliente')

    disconnection_filter = get_disconnection_process_filter(spark, closing_day, 90)

    tickets = tickets\
        .join(active_filter, ['nif_cliente'], 'inner')\
        .join(disconnection_filter, ['nif_cliente'], 'inner')

    tickets.groupBy("X_GRUPO_TRABAJO").agg(countDistinct("NIF_CLIENTE").alias('num_nifs')).show()

    print '[Info rrss_ccc_exploration] Filtered ticket dataset showed above'

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    tickets = tickets.join(get_churn_target_nif(spark, closing_day, churn_window=30).select('nif_cliente', 'label'), ['nif_cliente'], 'inner')

    tickets.groupBy("X_GRUPO_TRABAJO").agg(countDistinct("NIF_CLIENTE").alias('num_nifs'), sql_avg('label').alias('churn_rate')).show()

    print '[Info rrss_ccc_exploration] Filtered and labeled ticket dataset showed above '



