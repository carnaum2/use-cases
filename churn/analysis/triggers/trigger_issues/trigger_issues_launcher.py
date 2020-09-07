#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import os
import time
# Spark utils
from pyspark.sql.functions import (udf, col, decode, when, lit, lower, concat,
                                   translate, count, max, avg,
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
import datetime as dt


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
    from churn.analysis.triggers.ccc_utils.ccc_utils import *
    from churn.analysis.triggers.trigger_issues.trigger_issues import *

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, _ = run_sc()

    spark = (SparkSession\
        .builder\
        .appName("Trigger identification")\
        .master("yarn")\
        .config("spark.submit.deployMode", "client")\
        .config("spark.ui.showConsoleProgress", "true")\
        .enableHiveSupport().getOrCreate())

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    date_ = get_last_date(spark)

    starting_date = move_date_n_days(date_, -30, str_fmt="%Y%m%d")

    base_df = get_customer_base_segment(spark, date_)

    print '[Info trigger_issues] printSchema of base_df below'

    base_df.printSchema()

    averias_4w = averias(spark, starting_date, date_)

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
    .withColumn('scoring', col('NUM_AVERIAS_NIF'))\
    .select('NIF_CLIENTE', 'seg_pospaid_nif', 'scoring')

    print '[Info trigger_issues] Size of trigger_df: ' + str(trigger_df.count()) + ' - Number of NIFs in trigger_df: ' + str(trigger_df.select('NIF_CLIENTE').distinct().count())

    trigger_df.orderBy(desc('scoring')).show()

    ################ MODEL OUTPUTS #################

    model_output_cols = ["model_name",\
    "executed_at",\
    "model_executed_at",\
    "predict_closing_date",\
    "msisdn",\
    "client_id",\
    "nif",\
    "model_output",\
    "scoring",\
    "prediction",\
    "extra_info",\
    "year",\
    "month",\
    "day",\
    "time"]

    partition_date = get_next_day_of_the_week('monday')

    partition_year = int(partition_date[0:4])

    partition_month = int(partition_date[4:6])

    partition_day = int(partition_date[6:8])

    df_model_scores = trigger_df\
    .withColumn("model_name", lit("triggers_issues").cast("string"))\
    .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string"))\
    .withColumn("model_executed_at", col("executed_at").cast("string"))\
    .withColumn("test", lit(date_))\
    .withColumn("client_id", lit(""))\
    .withColumn("msisdn", lit(""))\
    .withColumnRenamed("NIF_CLIENTE", "nif")\
    .withColumn("scoring", col("scoring").cast("float"))\
    .withColumn("model_output", col("scoring").cast("string"))\
    .withColumn("prediction", lit("-").cast("string"))\
    .withColumn("extra_info", concat(lit("segment="), col("seg_pospaid_nif")).cast("string"))\
    .withColumn("predict_closing_date", lit(date_))\
    .withColumn("year", lit(partition_year).cast("integer"))\
    .withColumn("month", lit(partition_month).cast("integer"))\
    .withColumn("day", lit(partition_day).cast("integer"))\
    .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))\
    .select(*model_output_cols)

    #################### MODEL PARAMETERS ##################

    df_pandas = pd.DataFrame({
        "model_name": ['triggers_issues'],
        "executed_at": [executed_at],
        "model_level": ["nif"],
        "training_closing_date": ["-"],
        "target": [""],
        "model_path": [""],
        "metrics_path": [""],
        "metrics_train": [""],
        "metrics_test": [""],
        "varimp": [""],
        "algorithm": [""],
        "author_login": ["jmarcoso"],
        "extra_info": [""],
        "scores_extra_info_headers": ["-"],
        "year":  [partition_year],
        "month": [partition_month],
        "day":   [partition_day],
        "time": [int(executed_at.split("_")[1])]
    })

    df_model_parameters = spark\
    .createDataFrame(df_pandas)\
    .withColumn("day", col("day").cast("integer"))\
    .withColumn("month", col("month").cast("integer"))\
    .withColumn("year", col("year").cast("integer"))\
    .withColumn("time", col("time").cast("integer"))

    #######################

    df_model_scores\
    .write\
    .partitionBy('model_name', 'year', 'month', 'day')\
    .mode("append")\
    .format("parquet")\
    .save("/data/attributes/vf_es/model_outputs/model_scores/")

    # df_model_scores.coalesce(1) ...

    # /data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_issues

    df_model_parameters\
    .coalesce(1)\
    .write\
    .partitionBy('model_name', 'year', 'month', 'day')\
    .mode("append")\
    .format("parquet")\
    .save("/data/attributes/vf_es/model_outputs/model_parameters/")

    print("Inserted to model outputs")

    '''

    # Checking the trigger on past data

    label_df = df = spark.read.parquet('/data/attributes/vf_es/trigger_analysis/customer_master/year=2019/month=4/day=14').select('NIF_CLIENTE', 'label')

    label_trigger_df = trigger_df.join(label_df, ['NIF_CLIENTE'], 'inner')

    print '[Info trigger_issues] Size of label_trigger_df: ' + str(label_trigger_df.count()) + ' - Number of NIFs in label_trigger_df: ' + str(label_trigger_df.select('NIF_CLIENTE').distinct().count())

    label_trigger_df.select(count('*').alias('volume'), avg('label').alias('churn_rate')).show()

    '''
