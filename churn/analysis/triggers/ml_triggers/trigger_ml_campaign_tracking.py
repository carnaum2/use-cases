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
                                   randn)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType

def is_cycle(date_, str_fmt="%Y%m%d"):
    '''
    True if date_ is a cycle (valid date to be used in closing_day for car, deliveries, ...)
    :param date_:
    :param str_fmt:
    :return:
    '''
    import datetime as dt
    if isinstance(date_,str):
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_
    from pykhaos.utils.date_functions import get_next_cycle, get_previous_cycle
    return date_obj == get_next_cycle(get_previous_cycle(date_obj))

def get_tgs(spark, closing_day):

    df_tgs = spark.read.load("/data/udf/vf_es/churn/extra_feats_mod/tgs/year={}/month={}/day={}".format(int(closing_day[:4]),
                                                                             int(closing_day[4:6]),
                                                                             int(closing_day[6:])))

    #df_tgs = df_tgs.sort("nif_cliente", desc("tgs_days_until_fecha_fin_dto")).drop_duplicates(["nif_cliente"])
    df_tgs = df_tgs.sort("nif_cliente", desc("tgs_days_until_f_fin_bi")).drop_duplicates(["nif_cliente"])

    return df_tgs


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

    #sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    #sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    set_paths()

    # Reading from mini_ids. The following imports would be required for the automated process (computing features on the fly)
    #from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
    #from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_attributes_df

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

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    deliv_date = sys.argv[1]  # Día de la entrega de los resultados/lanzamiento de campaña
    deliv_year = str(int(deliv_date[0:4]))
    deliv_month = str(int(deliv_date[4:6]))
    deliv_day = str(int(deliv_date[6:8]))

    # 0. Getting the data

    # 0.1. Loading campaign data

    campaign = ['AUTOMSEM_PXXXT_TRIGG_SERVICIO']

    campaigndf = spark.read.table('raw_es.campaign_nifcontacthist')\
        .filter((col('year') == int(deliv_year)) & (col('month') == int(deliv_month)) & (col('CampaignCode').isin(campaign)))\
        .withColumn('Grupo', when(col('cellcode').startswith('CU'), 'Universal').when(col('cellcode').startswith('CC'), 'Control').otherwise('Target'))\
        .select('CIF_NIF', 'CampaignCode', 'Grupo', 'ContactDateTime', 'year', 'month', 'day')\
        .filter(col('day')==deliv_day)\
        .withColumnRenamed('CIF_NIF', 'nif_cliente')

    print '[Info Campaign Tracking] The number of entries in the campaigndf for ' + str(deliv_date) + ' is ' + str(campaigndf.count())

    campaigndf.groupBy('Grupo').agg(countDistinct('nif_cliente').alias('num_nifs')).show()

    print '[Info Campaign Tracking] Count on campaign groups for ' + str(deliv_date) + ' showed above'

    # 0.2. Loading labeled base

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    labeldf = get_churn_target_nif(spark, deliv_date, churn_window=30).select('nif_cliente', 'label').distinct()

    # 0.3. Loading the delivery

    all_delivdf = spark\
        .read\
        .parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml/year=' + deliv_year + '/month=' + deliv_month + '/day=' + deliv_day)

    car_date = str(all_delivdf.select('predict_closing_date').distinct().collect()[0]["predict_closing_date"])

    delivdf = all_delivdf\
        .select(['nif', 'scoring']).withColumn('scoring', col('scoring') + 0.0001 * randn())\
        .withColumnRenamed('nif', 'nif_cliente')

    # 1. Analysis

    # 1.1. Churn rates in the groups of the campaign

    campaigndf = campaigndf.select('nif_cliente', 'Grupo').distinct().join(labeldf, ['nif_cliente'], 'left').na.fill({'label': 0.0})

    campaigndf.groupBy('Grupo').agg(sql_avg('label').alias('churn_rate')).show()

    print '[Info Campaign Tracking] Churn rates for each group in the campaign for ' + str(deliv_date) + ' showed above'

    # 1.2. Lift curve of the delivery

    delivdf = delivdf.distinct().join(labeldf, ['nif_cliente'], 'left').na.fill({'label': 0.0})

    qds = QuantileDiscretizer(numBuckets=20, inputCol = "scoring", outputCol = "bucket", relativeError = 0.0)

    bucketed_delivdf = qds.fit(delivdf).transform(delivdf)

    # Num churners per bucket

    from pyspark.sql import Window

    bucketed_delivdf = bucketed_delivdf.groupBy('bucket').agg(count("*").alias("num_customers"), sql_sum('label').alias('num_churners')).withColumn('all_segment', lit('all'))

    windowval = (Window.partitionBy('all_segment').orderBy(desc('bucket')).rangeBetween(Window.unboundedPreceding, 0))
    result_df = bucketed_delivdf\
        .withColumn('cum_num_churners', sql_sum('num_churners').over(windowval))\
        .withColumn('cum_num_customers', sql_sum('num_customers').over(windowval))\
        .withColumn('lift', col('num_churners')/col('num_customers'))\
        .withColumn('cum_lift', col('cum_num_churners')/col('cum_num_customers'))

    result_df.orderBy(desc('bucket')).show(50, False)

    print '[Info Campaign Tracking] Churn rates for each bucket in the delivery for ' + str(deliv_date) + ' showed above'

    # 2. Creation of the campaign

    # 2.1. Range of indices/positions in the delivery for customers in the campaign

    import pandas as pd

    pd_delivdf = delivdf.toPandas()

    pd_delivdf = pd_delivdf.sort_values(by='scoring', ascending=False)

    pd_delivdf['position'] = range(1, len(pd_delivdf) + 1)

    ord_delivdf = spark.createDataFrame(pd_delivdf)

    ord_campaigndf = campaigndf\
        .select('nif_cliente', 'Grupo')\
        .distinct()\
        .join(ord_delivdf, ['nif_cliente'], 'inner')\
        .groupBy('Grupo')\
        .agg(sql_min('position').alias('min_position'), sql_max('position').alias('max_position'))

    ord_campaigndf.show()

    print '[Info Campaign Tracking] Min and max positions for each group in the campaign for ' + str(deliv_date) + ' showed above'

    # 2.2. Churn rate of those not included in the campaign

    max_position = ord_campaigndf\
        .select(sql_max('max_position').alias('max_position'))\
        .rdd\
        .map(lambda x: x["max_position"])\
        .collect()[0]

    nocamp_delivdf = ord_delivdf\
        .join(campaigndf.select('nif_cliente').withColumn('camp', lit(1)), ['nif_cliente'], 'left')\
        .na.fill({'camp': 0})\
        .filter((col('camp')==0) & (col('position') <= max_position))

    nocamp_delivdf.groupBy('label').agg(count("*").alias("num_nifs")).show()

    print '[Info Campaign Tracking] Num NIFs per label in top ' + str(max_position) + ' customers NOT included the campaign for ' + str(deliv_date) + ' showed above'

    # 2.3. Characterising those customers not included in the campaign. Attributes for "car_date"

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_attributes

    # Customer base attributes

    base_attrib_df = get_customer_base_attributes(spark, car_date, days_before=90, add_columns=None, active_filter=True, verbose=False)\
        .select('nif_cliente', 'seg_pospaid_nif', 'flag_segment_changed', 'flag_dx')

    # TGs: blindaje

    from pykhaos.utils.date_functions import move_date_n_cycles, get_previous_cycle

    # use closing_day if it is a cycle. Otherwise, get the previous one
    closing_day_tgs = car_date if is_cycle(car_date) else get_previous_cycle(car_date)

    df_tgs = None
    for ii in range(0, 3):
        try:
            df_tgs = get_tgs(spark, closing_day_tgs).select("nif_cliente", 'tgs_days_until_f_fin_bi').drop_duplicates()
            print("Found extra feats for tgs - {}".format(closing_day_tgs))

            break
        except Exception as e:
            print("Not found extra feats for tgs - {}".format(closing_day_tgs))
            print(e)
            print("trying with a previous cycle...")
            closing_day_tgs = move_date_n_cycles(closing_day_tgs, n=-1)

    if df_tgs is None:
        print("ERROR df_tgs could not be obtained. Please, review")
        sys.exit()

    df_tgs = df_tgs \
        .select("nif_cliente", "tgs_days_until_f_fin_bi") \
        .withColumn("blindaje", when(col("tgs_days_until_f_fin_bi") > 60, 'hard').otherwise(when((col("tgs_days_until_f_fin_bi") <= 60) & (col("tgs_days_until_f_fin_bi") > 0), 'soft').otherwise('none'))) \
        .select("nif_cliente", "blindaje")

    # Adding customer_base and TGs attributes to study the customer profile

    nocamp_delivdf\
        .join(base_attrib_df, ['nif_cliente'], 'left')\
        .na.fill({'seg_pospaid_nif': "unknown", 'flag_segment_changed': -1.0, 'flag_dx': -1.0})\
        .join(df_tgs, ['nif_cliente'], 'left')\
        .na.fill({'blindaje': 'unknown'})\
        .groupBy('seg_pospaid_nif', 'flag_segment_changed', 'flag_dx', 'blindaje').agg(count("*").alias("num_customers"), sql_avg("label").alias("churn_rate"))\
        .withColumn("segment_all", lit("all"))\
        .withColumn("total_num_customers", sql_sum("num_customers").over(Window.partitionBy('segment_all')))\
        .drop("segment_all")\
        .withColumn("segment_weight", col("num_customers")/col("total_num_customers"))\
        .orderBy(desc('segment_weight'))\
        .show(200, False)

    print '[Info Campaign Tracking] Profile of NIFs in top ' + str(max_position) + ' customers NOT included in the campaign for ' + str(deliv_date) + ' showed above'

    # 2.4. Characterising those customers included in the campaign. Attributes for "car_date"

    camp_delivdf = ord_delivdf \
        .join(campaigndf.select('nif_cliente').withColumn('camp', lit(1)), ['nif_cliente'], 'left') \
        .na.fill({'camp': 0}) \
        .filter((col('camp') == 1) & (col('position') <= max_position))

    camp_delivdf.groupBy('label').agg(count("*").alias("num_nifs")).show()

    print '[Info Campaign Tracking] Num NIFs per label in top ' + str(max_position) + ' customers included the campaign for ' + str(deliv_date) + ' showed above'

    camp_delivdf \
        .join(base_attrib_df, ['nif_cliente'], 'left') \
        .na.fill({'seg_pospaid_nif': "unknown", 'flag_segment_changed': -1.0, 'flag_dx': -1.0}) \
        .join(df_tgs, ['nif_cliente'], 'left') \
        .na.fill({'blindaje': 'unknown'}) \
        .groupBy('seg_pospaid_nif', 'flag_segment_changed', 'flag_dx', 'blindaje').agg(count("*").alias("num_customers"), sql_avg("label").alias("churn_rate")) \
        .withColumn("segment_all", lit("all")) \
        .withColumn("total_num_customers", sql_sum("num_customers").over(Window.partitionBy('segment_all'))) \
        .drop("segment_all")\
        .withColumn("segment_weight", col("num_customers") / col("total_num_customers")) \
        .orderBy(desc('segment_weight')) \
        .show(200, False)

    print '[Info Campaign Tracking] Profile of NIFs in top ' + str(max_position) + ' customers included in the campaign for ' + str(deliv_date) + ' showed above'




















