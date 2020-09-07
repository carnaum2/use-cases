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
    parser.add_argument('--operator', metavar='<operator or None>', type=str, required=False, default="none", help='selected operator')
    parser.add_argument('--insert_day', metavar='<insert_day or None>', type=str, required=False, default="5", help='Day of the week where results will be stored')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    operator = None if args.operator == "none" else args.operator
    insert_day = int(args.insert_day)

    # Loading validation sets (stored with partition date given by tr_date)

    partition_date = tr_date_
    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[4:6])
    partition_day = int(partition_date[6:8])

    partition = 'year={}/month={}/day={}'.format(partition_year, partition_month, partition_day)

    #path_val_hard = '/data/attributes/vf_es/revamp/validation_mob/segment=hard/' + partition
    path_val_hard = ('/data/attributes/vf_es/revamp/validation_mob/segment=hard/' + partition) if operator == None else ('/data/attributes/vf_es/revamp/validation_mob/operator=' + operator + '/segment=hard/' + partition)
    #path_val_soft = '/data/attributes/vf_es/revamp/validation_mob/segment=soft/' + partition
    path_val_soft = ('/data/attributes/vf_es/revamp/validation_mob/segment=soft/' + partition) if operator == None else ('/data/attributes/vf_es/revamp/validation_mob/operator=' + operator + '/segment=soft/' + partition)
    #path_val_none = '/data/attributes/vf_es/revamp/validation_mob/segment=none/' + partition
    path_val_none = ('/data/attributes/vf_es/revamp/validation_mob/segment=none/' + partition) if operator == None else ('/data/attributes/vf_es/revamp/validation_mob/operator=' + operator + '/segment=none/' + partition)

    print "[Info] Loading validation for hard segment data from " + path_val_hard
    print "[Info] Loading validation for soft segment data from " + path_val_soft
    print "[Info] Loading validation for none segment data from " + path_val_none

    df_val_soft = spark.read.load(path_val_soft).withColumn('segment', lit('bound=soft')).withColumnRenamed('model_score', 'scoring')
    df_val_hard = spark.read.load(path_val_hard).withColumn('segment', lit('bound=hard')).withColumnRenamed('model_score', 'scoring')
    df_val_none = spark.read.load(path_val_none).withColumn('segment', lit('bound=none')).withColumnRenamed('model_score', 'scoring')

    df_val = df_val_soft.union(df_val_hard).union(df_val_none)

    # Loading test/pred sets (stored with partition date given by insert_day)

    from churn_nrt.src.utils.date_functions import get_next_dow

    partition_date = ""
    if((insert_day>=1) & (insert_day<=7)):
        import datetime as dt
        partition_date = get_next_dow(insert_day).strftime("%Y%m%d")
    elif(insert_day==-1):
        partition_date = tt_date_
    else:
        partition_date = insert_day

    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[4:6])
    partition_day = int(partition_date[6:8])

    partition = 'year={}/month={}/day={}'.format(partition_year, partition_month, partition_day)

    path_hard = ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_hard/' + partition) if operator==None else ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_hard_' + operator + '/' + partition)
    path_soft = ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_soft/' + partition) if operator == None else ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_soft_' + operator + '/' + partition)
    path_none = ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_none/' + partition) if operator == None else ('/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_none_' + operator + '/' + partition)
    #path_soft = '/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_soft/' + partition
    #path_none = '/data/attributes/vf_es/model_outputs/model_scores/model_name=revamp_mobile_none/' + partition

    print "[Info] Loading prediction for hard segment data from " + path_hard
    print "[Info] Loading prediction for soft segment data from " + path_soft
    print "[Info] Loading prediction for none segment data from " + path_none

    df_soft = spark.read.load(path_soft).withColumn('segment', lit('bound=soft'))
    df_hard = spark.read.load(path_hard).withColumn('segment', lit('bound=hard'))
    df_none = spark.read.load(path_none).withColumn('segment', lit('bound=none'))

    df_preds = df_soft.union(df_hard).union(df_none)

    df_preds = df_preds.cache()
    print"Size of prediction df: " + str(df_preds.count())

    df_val = df_val.cache()
    print"Size of validation df: " + str(df_val.count())

    from churn_nrt.src.projects_utils.models.modeler import get_model_combination
    df_merged = get_model_combination(spark, df_val, df_preds, num_buckets=301, ord_col = "scoring", verbose=True)


    DECILE_COL = "comb_decile"
    from churn_nrt.src.utils.date_functions import move_date_n_days

    ccc_start_date_ = move_date_n_days(tt_date_, -30)

    ############## Add pbmas information #############
    #df_agg = get_incidences_info(spark, tt_date_, ccc_start_date_)
    #df_scores_incidencias = df_merged.join(df_agg, on=["msisdn"], how="left")
    df_scores_incidencias = df_merged.withColumn('IND_PBMA_SRV', lit(0)).withColumn('DETALLE_PBMA_SRV', lit("None"))
    df_scores_incidencias = df_scores_incidencias.fillna({'IND_PBMA_SRV': 0, 'DETALLE_PBMA_SRV': "None"})
    df_scores_incidencias = df_scores_incidencias.cache()
    df_scores_incidencias.count()

    ############## Add deciles ##############
    from churn_nrt.src.data_utils.model_outputs_manager import add_decile
    df_scores_incidencias = add_decile(df_scores_incidencias, "norm_ord_score", 0.2)
    df_scores_incidencias = df_scores_incidencias

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

    import datetime as dt
    from pyspark.sql.functions import split, from_unixtime, unix_timestamp, concat
    df_scores_incidencias = df_scores_incidencias.withColumn('IND_PBMA_SRV', concat(lit("flag_pbmas="), col('IND_PBMA_SRV')))

    df_mo = df_scores_incidencias.select('msisdn', 'norm_ord_score', 'decil', 'IND_PBMA_SRV','segment', 'flag_propension')
    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    model_name = "churn_mobile" if(operator==None) else "churn_mobile_" + operator

    df_model_scores = df_mo \
        .withColumn("model_name", lit(model_name).cast("string")) \
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
        .withColumn("model_executed_at", col("executed_at").cast("string")) \
        .withColumn("client_id", lit("")) \
        .withColumn("scoring", col("norm_ord_score"))\
        .withColumn("nif", lit("")) \
        .withColumn("model_output", lit("").cast("string")) \
        .withColumn("prediction", lit("").cast("string")) \
        .withColumn("extra_info",concat_ws(';',df_mo.decil, df_mo.flag_propension, df_mo.IND_PBMA_SRV, df_mo.segment)) \
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

    df_model_scores.coalesce(400) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet")\
        .save("/data/attributes/vf_es/model_outputs/model_scores/")\
        #.save("/user/jmarcoso/jmarcoso_model_scores/")\

    df_model_parameters \
        .coalesce(1) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet")\
        .save("/data/attributes/vf_es/model_outputs/model_parameters/") \
        #.save("/user/jmarcoso/jmarcoso_model_parameters/")\

    print("Inserted to model outputs")

    print('########## Finished process ##########')





