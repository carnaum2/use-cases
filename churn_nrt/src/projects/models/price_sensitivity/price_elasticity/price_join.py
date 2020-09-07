#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import col,randn, count, desc, when, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce
from pyspark.sql.functions import mean as sql_avg, min as sql_min, max as sql_max, count as sql_count
from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, regexp_extract
import time
import sys
from pyspark.sql import Window

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)



def set_paths():
    import os, re, sys

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


def get_model_combination_9models(spark, df1, df2, num_buckets=300, ord_col="model_score", verbose=True):
    # From df1, the signature (ordered structure of (segment, bucket) elements) is obtained
    quantiles = ["high", "medium", "low"]
    segments = ["none", "soft", "hard"]

    dict_dfs = {'a_b': 100}
    dict_lifts = {'a_b': 100}
    for q in quantiles:
        for seg in segments:
            name_ = seg + "_" + q
            print(name_)
            df = df1.filter(col('segment') == seg).filter(col('ARPC') == q)
            dict_dfs[name_] = df
            lift_comb = get_cumulative_churn_rate_fix_step(spark, df, step_=1.0 / num_buckets, ord_col=ord_col, label_col="label", verbose=False, noise=0.000001).withColumn(
                'segment', lit(seg)).withColumn('ARPC', lit(q)).select('bucket', 'ARPC', 'cum_churn_rate', 'segment')
            dict_lifts[name_] = lift_comb
            if name_ == "none_high":
                lift_mob1_comb = lift_comb
            else:
                lift_mob1_comb = lift_mob1_comb.union(lift_comb)
    dict_lifts.pop('a_b')
    dict_dfs.pop('a_b')
    signature = lift_mob1_comb.toPandas()
    signature = signature.sort_values(by='cum_churn_rate', ascending=True)
    signature['position'] = range(1, len(signature) + 1)
    signature_df = spark.createDataFrame(signature)

    if (verbose):
        signature_df.show()
    # Combining: the set to be ordered (df2) is prepared by computing buckets
    # IMPORTANT: an equal number of buckets in both the signature and the structure (bucketed) df2 is required

    from pyspark.ml.feature import QuantileDiscretizer
    qds = QuantileDiscretizer(numBuckets=num_buckets, inputCol=ord_col, outputCol="bucket", relativeError=0.0)

    dict_dfs2 = {'a_b': 100}
    for q in quantiles:
        for seg in segments:
            name_ = seg + "_" + q
            df = df2.filter(col('segment') == seg).filter(col('ARPC') == q).withColumn(ord_col, col(ord_col) + lit(0.000001) * randn())
            bucketed = qds.fit(df).transform(df)
            dict_dfs2[name_] = bucketed
            if name_ == "none_high":
                bucketed_all = bucketed
            else:
                bucketed_all = bucketed_all.union(bucketed)
    dict_dfs2.pop('a_b')

    # Ordering df2 according to the obtained signature and generating an artificial score (ord_score) that is
    # mapped onto the [0, 1] interval
    ord_df = bucketed_all \
        .join(signature_df.select('bucket', 'segment', 'ARPC', 'position'), ['bucket', 'segment', 'ARPC'], 'inner') \
        .withColumn('ord_score', col('position') + col(ord_col)) \
        .withColumn('all', lit('all')) \
        .withColumn('max_ord_score', sql_max('ord_score').over(Window.partitionBy('all'))) \
        .withColumn('min_ord_score', sql_min('ord_score').over(Window.partitionBy('all'))) \
        .withColumn('norm_ord_score',
                    (col('ord_score') - col('min_ord_score')) / (col('max_ord_score') - col('min_ord_score')))
    if (verbose):
        ord_df.show()
    return ord_df

if __name__ == "__main__":

    set_paths()
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon

    spark, sc = get_spark_session_noncommon("price_join")
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
    insert_day = 5#int(args.insert_day)

    # Loading validation sets (stored with partition date given by tr_date)

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

    from churn_nrt.src.projects_utils.models.modeler import get_cumulative_churn_rate_fix_step, get_optimum_sens_spec_threshold
    from pyspark.sql import Window

    window = Window.partitionBy('all')
    key = 'label'
    name_ = "norm_score_inc"
    label = key

    quantiles = ["high", "medium", "low"]
    segments = ["none", "soft", "hard"]
    dict_increments = {'a_b': 100}
    dict_val = {'a_b': 100}
    dict_test_planb = {'a_b': 100}
    increments = [0, 2, 4, 6, 8, 10]

    for q in quantiles:
        for seg in segments:
            path_to_val = '/data/attributes/vf_es/asaezco/price_filtered/val_scores/bill={}/segment={}/year={}/month={}/day={}'.format(
                q, seg, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
            df_val = spark.read.load(path_to_val).drop_duplicates(["num_cliente"])
            path_to_val_0 = '/data/attributes/vf_es/asaezco/val_scores_0/price_inc=0/bill={}/segment={}/year={}/month={}/day={}'.format(
                q, seg, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
            df_val_0 = spark.read.load(path_to_val_0).drop('label').drop_duplicates(["num_cliente"])

            df_val_0 = df_val_0.withColumn("norm_scoring_0", col("scoring"))
            df_val = df_val.withColumn("norm_scoring", col("model_score"))
            df_val_dif = df_val.join(df_val_0, ['num_cliente'], 'inner').withColumn("inc", col("norm_scoring") - col(
                "norm_scoring_0")).withColumn("norm_score_inc", 100.0 * (col('inc') / col('norm_scoring_0')))

            th_tp = get_optimum_sens_spec_threshold(spark, df_val_dif, 1000, "norm_score_inc", "label", False,0.0000001)
            print" "
            print"Segment: " + seg
            print"ARPC: " + q

            opt_score_f1 = th_tp[0]
            opt_volume_f1 = df_val_dif.where(col("norm_score_inc") > opt_score_f1).count()
            path_to_test_0 = '/data/attributes/vf_es/asaezco/price_filtered_test/price_inc=0/bill={}/segment={}/year={}/month={}/day={}'.format(q, seg, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
            df_test_0 = spark.read.load(path_to_test_0).drop_duplicates(["num_cliente"])
            df_test_0 = df_test_0.withColumn("norm_scoring_0", col("scoring")).drop('scoring')

            df_aux = df_test_0.select("num_cliente")
            name_d = seg + '_' + q

            path_to_plan_m4m = '/data/attributes/vf_es/asaezco/price_plan_b/bill={}/segment={}/year={}/month={}/day={}'.format(
                q, seg, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))

            df_plan_m4m = spark.read.load(path_to_plan_m4m)

            for inc in increments[1:]:
                name_s = seg + '_' + q + '_' + str(inc)
                path_to_test = '/data/attributes/vf_es/asaezco/price_filtered_test/price_inc={}/bill={}/segment={}/year={}/month={}/day={}'.format(
                    inc, q, seg, tt_date_[:4], int(tt_date_[4:6]), int(tt_date_[6:8]))
                df_test = spark.read.load(path_to_test).drop_duplicates(["num_cliente"])
                df_test = df_test.withColumn("norm_scoring", col("scoring")).drop('scoring')

                df_test = df_test.join(df_test_0, ['num_cliente'], 'inner').withColumn("inc", col("norm_scoring") - col(
                    "norm_scoring_0")).withColumn("norm_score_inc", 100.0 * (col('inc') / col('norm_scoring_0')))

                df_f1 = df_test.withColumn("sensitive_inc_" + str(inc),
                                           when(col("norm_score_inc") > opt_score_f1, 1.0).otherwise(0.0))

                df_aux = df_aux.join(df_f1.select("num_cliente", "sensitive_inc_" + str(inc)), ["num_cliente"], "inner")

            df_aux = df_aux.withColumn("price_sens", when(col("sensitive_inc_2") > 0, "Sensitivity=2").otherwise(
                when(col("sensitive_inc_4") > 0, "Sensitivity=4").otherwise(
                    when(col("sensitive_inc_6") > 0, "Sensitivity=6").otherwise(
                        when(col("sensitive_inc_8") > 0, "Sensitivity=8").otherwise(
                            when(col("sensitive_inc_10") > 0, "Sensitivity=10").otherwise("Sensitivity=No")))))).select(
                "num_cliente", "price_sens")

            dict_val[name_d] = df_val_dif.withColumn('segment', lit(seg)).withColumn('ARPC', lit(q))
            dict_increments[name_d] = df_aux.join(df_test_0.select("num_cliente", "norm_scoring_0"), ["num_cliente"],
                                                  "inner")
            dict_test_planb[name_d] = df_plan_m4m.withColumn('segment', lit(seg)).withColumn('ARPC', lit(q))  # plan_b_preds

    dict_increments.pop('a_b')
    dict_val.pop('a_b')
    dict_test_planb.pop('a_b')

    df_mo1 = dict_increments["none_high"].withColumn('segment', lit("Segment=hard")).withColumn('ARPC',lit("ARPC=high")) \
        .select('num_cliente', 'segment', 'ARPC', 'norm_scoring_0', 'price_sens')
    df_mo2 = dict_val["none_high"]
    df_score = dict_test_planb["none_high"]

    for q in quantiles:
        for seg in segments:
            name_ = seg + "_" + q
            if not name_ == "none_high":
                ###### df_mo1 contains the customer's sensibility thresholds (euros) ######
                df_mo1 = df_mo1.union(dict_increments[name_].withColumn('segment', lit("Segment=" + seg)).withColumn('ARPC',lit("ARPC=" + q)) \
                .select('num_cliente', 'segment', 'ARPC', 'norm_scoring_0', 'price_sens'))
                ###### df_mo2 contains the joined validation dfs for scores ordering ######
                df_mo2 = df_mo2.union(dict_val[name_])
                ###### df_score contains the test scores for the new plan b euros increment ######
                df_score = df_score.union(dict_test_planb[name_])

    df_score = df_score.withColumnRenamed('scoring', 'norm_scoring')
    #norm_score_inc for deltaP
    #norm_scoring for P
    ordered_df = get_model_combination_9models(spark, df_mo2, df_score, num_buckets=300, ord_col="norm_scoring",verbose=False)

    print"Euros threshold customers volume: {}".format(df_mo1.count())
    print"New plan B volume: {}".format(df_score.count())
    df_mo = df_mo1.join(ordered_df.drop('ARPC', 'segment'),["num_cliente"],"inner")
    print"Joined outputs volume: {}".format(df_mo.count())

    #df_mo = df_mo.withColumn('ARPC', lit("segment=")+col('ARPC')).withColumn('segment', lit("bound=")+col('segment'))

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

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    model_name = "price_sensitivity"

    from churn_nrt.src.data_utils.model_outputs_manager import add_decile
    df_mo = add_decile(df_mo, "norm_ord_score", 0.17)

    df_model_scores = df_mo \
        .withColumn("model_name", lit(model_name).cast("string")) \
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
        .withColumn("model_executed_at", col("executed_at").cast("string")) \
        .withColumn("client_id", col("num_cliente")) \
        .withColumn("scoring", col("norm_ord_score"))\
        .withColumn("nif", lit("")) \
        .withColumn("msisdn", lit("")) \
        .withColumn("model_output", lit("").cast("string")) \
        .withColumn("prediction", lit("").cast("string")) \
        .withColumn("extra_info",concat_ws(';',df_mo.price_sens,df_mo.ARPC, df_mo.segment, df_mo.flag_propension, df_mo.decil)) \
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
            "model_level": ["client_id"],
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

    df_model_parameters \
        .coalesce(1) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet")\
        .save("/data/attributes/vf_es/model_outputs/model_parameters/") \

    print("Inserted to model outputs")

    print('########## Finished process ##########')





