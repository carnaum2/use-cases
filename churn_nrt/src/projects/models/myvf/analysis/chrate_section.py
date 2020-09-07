#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time
# from pyspark.sql.window import Window
from pyspark.sql.functions import (
                                    col,
#                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
                                     avg as sql_avg,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
                                    )
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# import datetime as dt
# from pyspark.sql import Row, DataFrame, Column, Window
# from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import concat, size, coalesce, col, lpad, struct, count as sql_count, lag, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum, length, concat_ws, regexp_replace, split
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
# from pyspark.sql.functions import array, regexp_extract
# from itertools import chain
# from churn.datapreparation.general.data_loader import get_unlabeled_car, get_port_requests_table, get_numclients_under_analysis
# from churn.utils.constants import PORT_TABLE_NAME
# from churn.utils.udf_manager import Funct_to_UDF
# from pyspark.sql.functions import substring, datediff, row_number

import datetime as dt
# from pyspark.sql.functions import from_unixtime,unix_timestamp


import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


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

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################



    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - algorithm: algorithm for training
    #      - mode_ : evaluation or prediction
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(
        description="Run myvf model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--closing_day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day


    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("ch_rate_section")
    sc.setLogLevel('WARN')


    from churn_nrt.src.utils.date_functions import move_date_n_days

    df_net = (spark.read.table("raw_es.customerprofilecar_adobe_sections")  # .where(col("msisdn").isin("34694136569", "34601472008", "34601053588"))
              .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
              .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) >= move_date_n_days(closing_day, n=-30))
              .drop("service_processed_at", "service_file_id", "year", "month","day")
              .withColumn("msisdn", col("MSISDN").substr(3, 9)))

    func_date = udf(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime("%Y%m%d"), StringType())

    df_net = df_net.withColumn('date_', func_date(col('Date'))).drop("Date")

    stages_name = ["section", "subsection", "page_name"]

    df_net = df_net.withColumn('Pages', regexp_replace('Pages', ', ', '_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', '-', '_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', ' ', '_'))
    # subsection exist with both names - join them
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'configuracion_notificacione_push', 'configuracion_notificaciones_push'))

    for ii, col_ in enumerate(stages_name):
        if col_ == "section":
            # Do not split if Pages is an url
            df_net = df_net.withColumn(col_, when(~col("Pages").rlike("^https://"), split("Pages", ":")[ii]).otherwise(col("Pages")))
        else:
            df_net = df_net.withColumn(col_, when(~col("Pages").rlike("^https://"), split("Pages", ":")[ii]).otherwise(None))

    df_net = df_net.withColumn('section', regexp_replace('section', ', ', '_'))
    df_net = df_net.withColumn('section', regexp_replace('section', ' ', '_'))  # remove spaces with underscore to avoid error when writing df
    df_net = df_net.withColumn('section', regexp_replace('section', 'https://m.vodafone.es/mves/', 'url_'))
    df_net = df_net.withColumn('section', regexp_replace('section', r'/|-', '_'))
    df_net = df_net.withColumn('section', when(col('section') == 'productsandservices', 'productos_y_servicios').otherwise(col('section')))
    df_net = df_net.withColumn("section_formatted", concat_ws("__", col("section"), col("subsection"), col("page_name")))
    df_net = df_net.withColumn("subsection_formatted", concat_ws("__", col("section"), col("subsection")))


    from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter
    from churn_nrt.src.data.sopos_dxs import MobPort
    from churn_nrt.src.data.customer_base import CustomerBase

    df_cust_base = CustomerBase(spark).get_module(closing_day)
    df_myvf_data = df_net.join(df_cust_base, on=["msisdn"], how="inner")

    target = MobPort(spark, churn_window=15).get_module(closing_day, save=True, force_gen=False).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')
    df_myvf_target = df_myvf_data.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

    print("- - - - - - - - navigated users for closing_day={} - - - - - - -".format(closing_day))

    # Navigating users
    # cols_sections_list = ["myvf_{}_nb_pages_last{}".format(sect, 14) for sect in NAVIG_SECTIONS]
    # print('[TriggerMyVfData] build_module | Tr set - Filtering myvf df for {}>0'.format("+".join(cols_sections_list)))
    # from churn_nrt.src.utils.pyspark_utils import sum_horizontal
    # df_tr_myvfdata = df_tr_myvfdata.withColumn("nb_pages_sections", sum_horizontal(cols_sections_list))

    df_myvf_target = df_myvf_target.cache()


    results = []

    print("- - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_active_filter = get_non_recent_customers_filter(spark, closing_day, 90)

    print("- - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_disconnection_filter = get_disconnection_process_filter(spark, closing_day, 90)

    print("- - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

    tr_churn_call_filter = get_churn_call_filter(spark, closing_day, 90, 'msisdn')

    df_myvf_target_filters = (df_myvf_target.join(tr_active_filter, ['msisdn'], 'inner')
                            .join(tr_disconnection_filter, ['nif_cliente'], 'inner')
                            .join(tr_churn_call_filter, ['msisdn'], 'inner'))

    df_myvf_target_filters = df_myvf_target_filters.cache()
    print("df_myvf_target_filters.count()={}".format(df_myvf_target_filters.count()))

    df_myvf_target_filters = df_myvf_target_filters.drop_duplicates(["msisdn", "section_formatted"])

    print("***Ch Rate ref****")
    chRateRef = df_myvf_target_filters.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    print("***Ch Rate ref****", chRateRef)


    vol_total = df_myvf_target_filters.count()
    print("Volumen total = {}".format(vol_total))


    #------------
    print("NUEVO ANALISIS")

    sect1 = "mi_cuenta__mis_contratos__resumen_de_mis_contratos"
    sect2 = "mi_cuenta__mis_permanencias__detalle_de_permanencias"

    trset1 = df_myvf_target_filters.withColumn("flag", when(col("section_formatted") == sect1, 1).otherwise(0)).where(col("flag") == 1)
    trset2 = df_myvf_target_filters.withColumn("flag", when(col("section_formatted") == sect2, 1).otherwise(0)).where(col("flag") == 1)

    print("sect1='{}' sect2='{}'".format(sect1, sect2))
    print("trset1={} trset2={} msisdns comunes={}".format(trset1.count(),
                                                 trset2.count(),
                                                 trset1.select("msisdn").join(trset2.select("msisdn"), on=["msisdn"], how="inner").count()))

    #-----------
    A = df_myvf_target_filters.select("section_formatted").distinct().rdd.map(lambda x: x[0]).collect()

    counter = 0
    print("There are {} sections".format(len(A)))

    for sect in A:

        start_time = time.time()

        tr_set = df_myvf_target_filters.withColumn("flag", when(col("section_formatted") == sect, 1).otherwise(0)).where(col("flag")==1)

        if tr_set.take(1):
            count_ = tr_set.count()
            chRate = tr_set.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
            print(chRate, type(chRate))
            weight_ = 1.0 * count_ / vol_total
            print(weight_, type(weight_))
            print(counter, sect, count_, chRate ,  weight_, weight_ * chRate, "{} seconds".format(time.time() - start_time))
            results.append([counter, sect, count_, chRate,  weight_, weight_ * chRate])
        else:
            try:
                print("Empty for section {}".format(sect))
            except:
                print("kk")

        counter=counter+1

    print(results)

    import pandas as pd
    df_results=pd.DataFrame(results, columns=["counter", "sect", "count_", "chRate ",  "weight_", "weight_  chRate"])

    print(df_results)

    import datetime as dt
    final_filename = "/var/SP/data/home/csanc109/data/ch_rate_results_{}_{}.xlsx".format(closing_day, dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
    writer = pd.ExcelWriter(final_filename, engine='xlsxwriter')
    workbook = writer.book
    df_results.to_excel(writer, sheet_name="RESULTS", startrow=7, startcol=0, index=False, header=True)


    #-----------

    df_myvf_target_filters = df_myvf_target_filters.drop_duplicates(["msisdn", "subsection_formatted"])

    vol_total = df_myvf_target_filters.count()
    print("Volumen total = {}".format(vol_total))

    B = df_myvf_target_filters.select("subsection_formatted").distinct().rdd.map(lambda x: x[0]).collect()
    results = []

    counter = 0
    print("There are {} subsections".format(len(B)))

    for sect in B:

        start_time = time.time()

        tr_set = df_myvf_target_filters.withColumn("flag", when(col("subsection_formatted") == sect, 1).otherwise(0)).where(col("flag")==1)

        if tr_set.take(1):
            count_ = tr_set.count()
            chRate = tr_set.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
            print(chRate, type(chRate))
            weight_ = 1.0 * count_ / vol_total
            print(weight_, type(weight_))
            print(counter, sect, count_, chRate ,  weight_, weight_ * chRate, "{} seconds".format(time.time() - start_time))
            results.append([counter, sect, count_, chRate,  weight_, weight_ * chRate])
        else:
            try:
                print("Empty for subsection {}".format(sect))
            except:
                print("kk")

        counter=counter+1

    print(results)

    import pandas as pd
    df_results=pd.DataFrame(results, columns=["counter", "subsect", "count_", "chRate ",  "weight_", "weight_  chRate"])

    print(df_results)

    import datetime as dt
    final_filename = "/var/SP/data/home/csanc109/data/ch_rate_results_subsection_{}_{}.xlsx".format(closing_day, dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
    writer = pd.ExcelWriter(final_filename, engine='xlsxwriter')
    workbook = writer.book
    df_results.to_excel(writer, sheet_name="RESULTS", startrow=7, startcol=0, index=False, header=True)

