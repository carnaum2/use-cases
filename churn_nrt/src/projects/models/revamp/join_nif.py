#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
import time
from pyspark.sql.functions import col, count, when, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce
from pyspark.sql.functions import mean as sql_avg, min as sql_min, max as sql_max, count as sql_count
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, regexp_extract
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.ml.feature import QuantileDiscretizer
import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)



def set_paths():
    import os, re, sys
    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset/')

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
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("revamp_mobile")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - mode_ : evaluation or prediction
    #      - model :  algorithm for training
    #      - horizon : cycles horizon to predict sensitivity
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(description="Run price_sensitivity model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Alvaro <alvaro.saez@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in test')
    parser.add_argument('--insert_day', metavar='<insert_day or None>', type=str, required=False, default="5", help='Day of the week where results will be stored')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    insert_day = int(args.insert_day)

    # Loading test/pred sets (stored with partition date given by insert_day)

    from churn_nrt.src.utils.date_functions import get_next_dow

    partition_date = ""
    if ((insert_day >= 1) & (insert_day <= 7)):
        import datetime as dt

        partition_date = get_next_dow(insert_day).strftime("%Y%m%d")
    elif (insert_day == -1):
        partition_date = tt_date_
    else:
        partition_date = str(insert_day)

    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[4:6])
    partition_day = int(partition_date[6:8])

    partition = 'year={}/month={}/day={}'.format(partition_year, partition_month, partition_day)

    ######################## Load customer base ########################
    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(tt_date_, force_gen=False).filter(col("rgu").isin('mobile', 'fbb')) \
        .select("msisdn", "num_cliente", "nif_cliente").distinct()

    ######################## Load model predictions ########################
    path_mobile = '/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_mobile/' + partition
    path_fbb = '/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_fbb/' + partition
    df_fbb = spark.read.load(path_fbb).withColumnRenamed('scoring', 'scoring_fbb').withColumnRenamed('client_id','num_cliente')
    df_mob = spark.read.load(path_mobile).withColumnRenamed('scoring', 'scoring_mob')

    ######################## Join scores with base ########################
    from pyspark.sql.functions import greatest
    joined_revamp = base_df.join(df_mob, ['msisdn'], 'left').join(df_fbb, ['num_cliente'], 'left') \
    .where((col('scoring_mob').isNotNull()) | (col('scoring_fbb').isNotNull())).withColumn('score_nif',greatest('scoring_fbb','scoring_mob'))

    join_reason = joined_revamp.groupBy("nif_cliente").agg(sql_max('scoring_mob').alias('Max_mob_score'),sql_max('scoring_fbb').alias('Max_fbb_score')) \
    .withColumn('max_score_service',when(col('Max_mob_score') >= col('Max_fbb_score'), "Risk=Mobile").otherwise("Risk=FBB"))

    nif_scores = joined_revamp.groupBy('nif_cliente').agg(sql_max('score_nif').alias('scoring'))
    nif_scores = nif_scores.join(join_reason.select("nif_cliente", "max_score_service"), ["nif_cliente"], "inner")

    from churn_nrt.src.data_utils.ids_utils import get_ids_nif

    df_ids = get_ids_nif(spark, tt_date_)
    df_bounds = df_ids.select('NIF_CLIENTE', 'tgs_days_until_f_fin_bi_agg_mean').distinct()
    col_bi = 'tgs_days_until_f_fin_bi_agg_mean'
    df_bounds = df_bounds.withColumn('blindaje', lit('bound=none')) \
        .withColumn('blindaje', when((col(col_bi) >= -60) & (col(col_bi) <= 60), 'bound=soft').otherwise(col('blindaje'))) \
        .withColumn('blindaje', when((col(col_bi) > 60), 'bound=hard').otherwise(col('blindaje')))

    nif_scores = nif_scores.join(df_bounds, ['nif_cliente'], 'inner')

    nif_scores = nif_scores.cache()
    print"Final NIF scores size: " + str(nif_scores.count())

    from churn_nrt.src.data_utils.model_outputs_manager import add_decile
    df_decile = add_decile(nif_scores,"scoring", 0.2, 10, 20)

    model_output_cols = ["model_name", \
                         "executed_at", \
                         "model_executed_at", \
                         "predict_closing_date", \
                         "msisdn", \
                         "client_id", \
                         "nif", \
                         "model_output", \
                         "scoring", \
                         "prediction", \
                         "extra_info", \
                         "year", \
                         "month", \
                         "day", \
                         "time"]

    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[4:6])
    partition_day = int(partition_date[6:8])

    import datetime as dt

    #df_mo = df_decile.select('nif_cliente', 'scoring', 'blindaje', 'decil', 'flag_propension', 'max_score_service')
    df_mo = df_decile.select('nif_cliente', 'scoring', 'blindaje', 'decil', 'flag_propension', 'max_score_service', 'percentile_norisk')

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    from pyspark.sql.functions import split, from_unixtime, unix_timestamp, concat

    model_name = "churn_score_nif"
    df_model_scores = df_mo \
        .withColumn("model_name", lit(model_name).cast("string")) \
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
        .withColumn("model_executed_at", col("executed_at").cast("string")) \
        .withColumn("client_id", lit("")) \
        .withColumn("msisdn", lit("")) \
        .withColumnRenamed("nif_cliente", "nif") \
        .withColumn("scoring", col("scoring").cast("float")) \
        .withColumn("model_output", lit("").cast("string")) \
        .withColumn("prediction", lit("").cast("string")) \
        .withColumn("extra_info", concat(df_mo.decil, lit(';'), df_mo.flag_propension, lit(';'), df_mo.blindaje, lit(';'), df_mo.max_score_service, lit(';'), df_mo.percentile_norisk)) \
        .withColumn("predict_closing_date", lit(tt_date_)) \
        .withColumn("year", lit(partition_year).cast("integer")) \
        .withColumn("month", lit(partition_month).cast("integer")) \
        .withColumn("day", lit(partition_day).cast("integer")) \
        .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer")) \
        .select(*model_output_cols)

    import pandas as pd

    df_pandas = pd.DataFrame({
        "model_name": [model_name],
        "executed_at": [executed_at],
        "model_level": ["nif"],
        "training_closing_date": [tr_date_],
        "target": [""],
        "model_path": [""],
        "metrics_path": [""],
        "metrics_train": [""],
        "metrics_test": [""],
        "varimp": ["-"],
        "algorithm": ["rf"],
        "author_login": ["asaezco"],
        "extra_info": [""],
        "scores_extra_info_headers": ["None"],
        "year": [partition_year],
        "month": [partition_month],
        "day": [partition_day],
        "time": [int(executed_at.split("_")[1])]})

    df_model_parameters = spark \
        .createDataFrame(df_pandas) \
        .withColumn("day", col("day").cast("integer")) \
        .withColumn("month", col("month").cast("integer")) \
        .withColumn("year", col("year").cast("integer")) \
        .withColumn("time", col("time").cast("integer"))

    df_model_scores.repartition(400)

    df_model_scores.cache()

    print '[Info] Model scores to be saved - Size: ' + str(df_model_scores.count()) + ' - Num NIFs: ' + str(df_model_scores.select('nif').distinct().count())

    df_model_scores\
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/data/attributes/vf_es/model_outputs/model_scores/")

    df_model_parameters \
        .coalesce(1) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/data/attributes/vf_es/model_outputs/model_parameters/")

    print('########## Inserted to MO ##########')

    print('########## Finished process ##########')





