from amdocs_informational_dataset.engine.call_centre_calls import CallCentreCalls
from pyspark.sql.functions import collect_set, concat, size, coalesce, col, lpad, struct, count as sql_count, lit, min as sql_min, max as sql_max, collect_list, udf, \
        desc, asc, to_date, create_map, sum as sql_sum, substring, sort_array, split, month, dayofmonth
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType, DateType
from pyspark.sql.functions import array, regexp_extract
from itertools import chain
import argparse
import csv
import re
import subprocess
import sys
import time
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.functions import concat_ws, date_format, from_unixtime, \
    length, lit, lower, lpad, month, regexp_replace, translate, udf, unix_timestamp, year, when, upper
from pyspark.sql.utils import AnalysisException
from engine.general_functions import format_date, compute_diff_days, sum_horizontal
from collections import Counter
from pyspark.sql.types import StringType
from pykhaos.utils.date_functions import get_last_day_of_month, move_date_n_days, move_date_n_cycles, move_date_n_yearmonths
from churn.analysis.ccc_churn.engine.data_loader import get_port, get_ccc_data, get_tgs, get_all_ports
from churn.datapreparation.general.data_loader import get_active_services
from churn.analysis.ccc_churn.engine.reporter import compute_results, SAVING_PATH, init_writer, print_sheet
from churn.analysis.ccc_churn.app.run_ccc_churn_analysis import join_dfs
from pyspark.sql.types import FloatType


N_HISTORIC = 12
EXTRA_FEATS_PREFIX = "scores_"



def get_scores_metrics(spark, closing_day, impute_nulls=True, force_keep_cols=None):
    df_onlymob = get_scores_metrics_segment(spark, closing_day, "onlymob", impute_nulls=impute_nulls, force_keep_cols=force_keep_cols)
    df_mobileandfbb = get_scores_metrics_segment(spark, closing_day, "mobileandfbb", impute_nulls=impute_nulls, force_keep_cols=force_keep_cols)

    from pykhaos.utils.pyspark_utils import union_all
    df = union_all([df_onlymob, df_mobileandfbb])
    return df


def get_scores_metrics_segment(spark, closing_day, segment, impute_nulls=True, force_keep_cols=None):

    #CLOSING_DAY = "20190131"

    df_scores = spark.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_{}".format(segment)).where(col("predict_closing_date")<closing_day)

    #FIXME check for duplicates on predict_closing_date
    df_scores_agg = df_scores.select("predict_closing_date", "executed_at", "year", "month", "day", "time").groupby("predict_closing_date", "executed_at", "year", "month", "day", "time").agg(sql_count("*").alias("count"))
    df_scores_agg = df_scores_agg.sort(desc("predict_closing_date"), desc("executed_at")).limit(N_HISTORIC)
    df_scores_agg.show(50,truncate=False)
    df_scores_agg_collect = df_scores_agg.collect()


    from churn.datapreparation.general.data_loader import get_active_services

    df_active = get_active_services(spark, closing_day, new=False, customer_cols=None, service_cols=None).select("msisdn")

    df_join = None

    fillna_dict = {}

    for i_row in range(0, min(N_HISTORIC, len(df_scores_agg_collect))):

        ii = i_row + 1

        #print("scoring{}".format(ii))

        row_dict = df_scores_agg_collect[i_row].asDict()
        #print(row_dict)
        df = spark.read.load(
            "/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_{}/year={}/month={}/day={}/time={}".format(
                segment,
                row_dict["year"],
                row_dict["month"],
                row_dict["day"],
                row_dict["time"]))
        df = df.select("msisdn", "scoring").withColumn("scoring", col("scoring").cast("float"))
        df = df.withColumnRenamed("scoring", "scoring{}".format(ii))

        df = df.join(df_active, on=["msisdn"], how="inner")

        # df = df.withColumnRenamed("executed_at", "executed_at{}".format(ii))

        fillna_dict["scoring{}".format(ii)] = -1

        if not df_join:
            df_join = df
        else:
            df_join = df_join.join(df, on=["msisdn"], how="full")

            df_join = (df_join.withColumn("diff_scoring_{}_{}".format(ii - 1, ii), when(
                (col("scoring{}".format(ii - 1)).isNotNull()) & (col("scoring{}".format(ii)).isNotNull()),
                col("scoring{}".format(ii - 1)) - col("scoring{}".format(ii))).otherwise(-1.0)))

            df_join = (df_join.withColumn("slope_scoring_{}_{}".format(ii - 1, ii),
                                          when(col("diff_scoring_{}_{}".format(ii - 1, ii)) > 0, 1.0).when(
                                              col("diff_scoring_{}_{}".format(ii - 1, ii)) < 0, -1.0).otherwise(0)))

            fillna_dict["diff_scoring_{}_{}".format(ii - 1, ii)] = 0
            fillna_dict["slope_scoring_{}_{}".format(ii - 1, ii)] = 0

    df_join = df_join.fillna(fillna_dict)

    if min(N_HISTORIC, len(df_scores_agg_collect)) < N_HISTORIC:
        ii = len(df_scores_agg_collect) + 1

        while ii <= N_HISTORIC:
            #print("EXTRA scoring{}".format(ii))

            df_join = df_join.withColumn("scoring{}".format(ii), lit(-1)).withColumn("scoring{}".format(ii), lit(-1))

            df_join = (df_join.withColumn("diff_scoring_{}_{}".format(ii - 1, ii), when(
                (col("scoring{}".format(ii - 1)).isNotNull()) & (col("scoring{}".format(ii)).isNotNull()),
                col("scoring{}".format(ii - 1)) - col("scoring{}".format(ii))).otherwise(-1.0)))

            df_join = (df_join.withColumn("slope_scoring_{}_{}".format(ii - 1, ii),
                                          when(col("diff_scoring_{}_{}".format(ii - 1, ii)) > 0, 1.0).when(
                                              col("diff_scoring_{}_{}".format(ii - 1, ii)) < 0, -1.0).otherwise(0)))

            fillna_dict["diff_scoring_{}_{}".format(ii - 1, ii)] = 0
            fillna_dict["slope_scoring_{}_{}".format(ii - 1, ii)] = 0

            ii += 1

    # Rellenar con el primero en lugar de con  -1 (primero siendo el mas antiguo)
    # 30/11/2018
    # 31/01/2019
    import numpy as np

    remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if
                                                     (not milista[ii] in [-1.0, None]) and (
                                                                 milista[ii] == milista[ii])]), ArrayType(FloatType()))
    # avg_udf = udf(lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())
    stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
    mean_udf = udf(lambda lista: float(np.mean(lista)), FloatType())
    max_udf = udf(lambda lista: float(np.max(lista)) if lista else None, FloatType())
    min_udf = udf(lambda lista: float(np.min(lista)) if lista else None, FloatType())

    mean_range_udf = udf(lambda lista, idx1, idx2: float(np.mean(lista[idx1:min(idx2, len(lista))])), FloatType())
    std_range_udf = udf(lambda lista, idx1, idx2: float(np.std(lista[idx1:min(idx2, len(lista))])), FloatType())

    pos_max_udf = udf(
        lambda milist: sorted([(vv, idx) for idx, vv in enumerate(milist)], key=lambda tup: tup[0], reverse=True)[0][1],
        IntegerType())
    pos_min_udf = udf(
        lambda milist: sorted([(vv, idx) for idx, vv in enumerate(milist)], key=lambda tup: tup[0], reverse=False)[0][
            1], IntegerType())

    from functools import partial

    def count_consecutive(mylist, nb):
        from itertools import groupby
        hola = [len(list(lista)) for vv, lista in groupby(mylist) if vv == nb]
        return hola[0] if hola else -1

    def count_consecutives_udf(mylist, nb):
        return udf(partial(count_consecutive), IntegerType())(mylist, nb)

    df_join = (
        df_join.withColumn("scoring_array", array([_col for _col in df_join.columns if _col.startswith("scoring")]))
        .withColumn("scoring_clean_array",
                    when(size(col("scoring_array")) > 0, remove_undefined_udf(col("scoring_array"))).otherwise(None))
        .withColumn("num_scores_historic",
                    when(col("scoring_clean_array").isNotNull(), size(col("scoring_clean_array"))).otherwise(0))
        .withColumn("mean_historic",
                    when(col("scoring_clean_array").isNotNull(), mean_udf(col("scoring_clean_array"))).otherwise(None))
        .withColumn("mean_1_4", when((col("num_scores_historic") > 0) & (col("num_scores_historic") > 0),
                                     mean_range_udf(col("scoring_clean_array"), lit(0), lit(4))).otherwise(-1))
        .withColumn("mean_5_8", when((col("num_scores_historic") > 0) & (col("num_scores_historic") >= 4),
                                     mean_range_udf(col("scoring_clean_array"), lit(4), lit(8))).otherwise(-1))
        .withColumn("mean_9_12", when((col("num_scores_historic") > 0) & (col("num_scores_historic") >= 8),
                                      mean_range_udf(col("scoring_clean_array"), lit(8), lit(12))).otherwise(-1))
        .withColumn("std_historic",
                    when(col("scoring_clean_array").isNotNull(), stddev_udf(col("scoring_clean_array"))).otherwise(
                        None))
        .withColumn("std_1_4", when((col("scoring_clean_array").isNotNull()) & (col("num_scores_historic") > 0),
                                    std_range_udf(col("scoring_clean_array"), lit(0), lit(4))).otherwise(-1))
        .withColumn("std_5_8", when((col("scoring_clean_array").isNotNull()) & (col("num_scores_historic") >= 4),
                                    std_range_udf(col("scoring_clean_array"), lit(4), lit(8))).otherwise(-1))
        .withColumn("std_9_12", when((col("scoring_clean_array").isNotNull()) & (col("num_scores_historic") >= 8),
                                     std_range_udf(col("scoring_clean_array"), lit(8), lit(12))).otherwise(-1))
        .withColumn("diff_scoring_array", array([_col for _col in df_join.columns if _col.startswith("diff_scoring")]))
        .withColumn("diff_scoring_clean_array", when(size(col("diff_scoring_array")) > 0,
                                                     remove_undefined_udf(col("diff_scoring_array"))).otherwise(None))
        .withColumn("size_diff", when(col("diff_scoring_clean_array").isNotNull(), size(col("diff_scoring_clean_array"))).otherwise(0))
        .withColumn("mean_diff_scoring", when(col("size_diff") > 0, mean_udf(col("diff_scoring_clean_array"))).otherwise(0))
        .withColumn("stddev_diff_scoring", when(col("size_diff") > 0, stddev_udf(col("diff_scoring_clean_array"))).otherwise(0))

        .withColumn("max_score", max_udf(col("scoring_clean_array")))
        .withColumn("min_score", min_udf(col("scoring_clean_array")))

        .withColumn("max_diff_score", when(col("size_diff") > 0, max_udf(col("diff_scoring_clean_array"))).otherwise(-1))
        .withColumn("min_diff_score", when(col("size_diff") > 0, min_udf(col("diff_scoring_clean_array"))).otherwise(-1))

        .withColumn("mean_q_array", array(["mean_1_4", "mean_5_8", "mean_9_12"]))  # 'q' stands for quarter
        .withColumn("mean_q_array", remove_undefined_udf(col("mean_q_array")))
        .withColumn("max_q_score",  when(col("mean_q_array").isNotNull(), max_udf(col("mean_q_array"))))  # max score by quarter
        .withColumn("pos_max_q_score", pos_max_udf(col("mean_q_array")))  # quarter of max score (1: 1-4 2: 5-8 3: 9-12)
        .withColumn("min_q_score",  when(col("mean_q_array").isNotNull(), min_udf(col("mean_q_array"))))  # min score by quarter
        .withColumn("pos_min_q_score", pos_min_udf(col("mean_q_array")))  # quarter of min score (1: 1-4 2: 5-8 3: 9-12)
        .withColumn("new_client", when(col("num_scores_historic") == 0, 1).otherwise(0))

        .withColumn("slope_scoring_array",
                    array([_col for _col in df_join.columns if _col.startswith("slope_scoring_")]))
        .withColumn("nb_cycles_same_trend", count_consecutives_udf(col("slope_scoring_array"), col("slope_scoring_1_2")))
        )

    for col_name, type_ in df_join.dtypes:
        if type_.startswith("array"):
            df_join = df_join.drop(col_name)


    for col_ in df_join.columns:
        if col_ in ["msisdn"]: continue
        df_join = df_join.withColumnRenamed(col_, EXTRA_FEATS_PREFIX + col_)

    if impute_nulls:
        df_join = impute_nulls_scores(spark, df_join)

    return df_join
    # df_join.select("msisdn", "diff_scoring_array", "diff_scoring_clean_array").distinct().show(200, truncate=False)



def impute_nulls_scores(spark, df):

    from churn.metadata.metadata import Metadata
    impute_na = Metadata.get_scores_impute_na()
    df_metadata = Metadata.create_metadata_df(spark, impute_na)
    return Metadata.apply_metadata(df, df_metadata)