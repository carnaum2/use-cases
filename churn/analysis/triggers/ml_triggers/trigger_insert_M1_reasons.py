#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, isnull,
                                   substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.functions import *
import matplotlib

matplotlib.use('Agg')


def create_model_output_dataframes(spark, predict_closing_day, df_model_scores, model_params_dict, model_name,
                                   extra_info_cols=None):
    from churn.delivery.delivery_constants import MODEL_OUTPUTS_NULL_TAG
    from churn.datapreparation.general.model_outputs_manager import ensure_types_model_scores_columns
    import datetime as dt
    executed_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S ")

    # TEMPORARY SOLUTION FOR RETURN FEED. PARTITION IS SET TO THE DATE OF NEXT WEDNESDAY
    from pykhaos.utils.date_functions import get_next_dow
    d = dt.date.today()
    # return_feed_execution = get_next_dow(weekday=3, from_date=dt.datetime.strptime(predict_closing_day, "%Y%m%d")).strftime("%Y%m%d")
    return_feed_execution = get_next_dow(weekday=3, from_date=d).strftime("%Y%m%d")

    day_partition = int(return_feed_execution[6:])
    month_partition = int(return_feed_execution[4:6])
    year_partition = int(return_feed_execution[:4])

    print("dataframes of model outputs set with values: year={} month={} day={}".format(year_partition, month_partition,
                                                                                        day_partition))

    '''
    MODEL PARAMETERS -
    '''

    model_params_dict.update({
        "model_name": [model_name],
        "executed_at": [executed_at],
        "year": [year_partition],
        "month": [month_partition],
        "day": [day_partition],
        "time": [int(executed_at.split(" ")[1].replace(":", ""))],
        "scores_extra_info_headers": [" "]
    })

    import pandas as pd
    df_pandas = pd.DataFrame(model_params_dict)

    df_parameters = spark.createDataFrame(df_pandas).withColumn("day", col("day").cast("integer")) \
        .withColumn("month", col("month").cast("integer")) \
        .withColumn("year", col("year").cast("integer")) \
        .withColumn("time", col("time").cast("integer"))

    '''
    MODEL SCORES
    '''
    if extra_info_cols:
        for col_ in extra_info_cols:
            df_model_scores = df_model_scores.withColumn(col_, when(coalesce(length(col(col_)), lit(0)) == 0,
                                                                    MODEL_OUTPUTS_NULL_TAG).otherwise(col(col_)))
        df_model_scores = (df_model_scores.withColumn("extra_info", concat_ws(";", *[col(col_name) for col_name in
                                                                                     extra_info_cols])).drop(
            *extra_info_cols))

    df_model_scores = (df_model_scores
                       .withColumn("prediction", lit("0"))
                       .withColumn("model_name", lit(model_name))
                       .withColumn("executed_at", lit(executed_at))
                       .withColumn("model_executed_at", lit(executed_at))
                       .withColumn("year", lit(year_partition).cast("integer"))
                       .withColumn("month", lit(month_partition).cast("integer"))
                       .withColumn("day", lit(day_partition).cast("integer"))
                       .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .withColumn("predict_closing_date", lit(closing_day))
                       .withColumn("model_output", lit(None))
                       )

    '''
    try:
        df_model_scores.show(20)
    except UnicodeEncodeError as e:
        print(e)

    except Exception as e:
        print(e)

    '''

    df_parameters.show()

    print '[Info] in create_model_output_dataframes function: df_parameters showed above'

    df_model_scores.show()

    print '[Info] in create_model_output_dataframes function: df_model_scores showed above'

    # df_model_scores = ensure_types_model_scores_columns(df_model_scores)

    #### Problems when calling the -ensure_types_model_scores_columns- function. Columns are treated in this function

    df_model_scores = df_model_scores.withColumn('msisdn', lit("")).withColumn('client_id', lit("")).withColumnRenamed(
        'NIF_CLIENTE', 'nif')

    df_model_scores = df_model_scores.withColumn("model_output", col("model_output").cast("string"))
    df_model_scores = df_model_scores.withColumn("scoring", col("scoring").cast("float"))
    df_model_scores = df_model_scores.withColumn("prediction", col("prediction").cast("string"))
    df_model_scores = df_model_scores.withColumn("extra_info", col("extra_info").cast("string"))
    df_model_scores = df_model_scores.withColumn("model_name", col("model_name").cast("string"))
    df_model_scores = df_model_scores.withColumn("time", col("time").cast("int"))
    df_model_scores = df_model_scores.withColumn("year", col("year").cast("int"))
    df_model_scores = df_model_scores.withColumn("month", col("month").cast("int"))
    df_model_scores = df_model_scores.withColumn("day", col("day").cast("int"))

    df_model_scores = df_model_scores.sort(desc("scoring"))

    df_model_scores.show()

    print '[Info] in create_model_output_dataframes function (after ensuring types): df_model_scores showed above'

    df_model_scores = (df_model_scores
                       .select(*(
        ["nif", "msisdn", "client_id", "executed_at", "model_executed_at", "predict_closing_date", "model_output",
         "scoring", "prediction", "extra_info", "time", "model_name", "year", "month", "day"])
                               )
                       )

    return df_parameters, df_model_scores


def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print(pathname)
    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):

        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)

        # from churn.utils.constants import CHURN_DELIVERIES_DIR
        # root_dir = CHURN_DELIVERIES_DIR
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

    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))

    return root_dir


TABLE_TRIGGER = '/data/udf/vf_es/churn/churn_reasons/trigger_reasons_results'

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar='<closing_d>', type=str, help='closing day', required=True)
    parser.add_argument('-s', '--starting_d', metavar='<starting_d>', type=str, help='starting day', required=True)
    args = parser.parse_args()

    set_paths_and_logger()

    from pykhaos.utils.date_functions import get_next_dow
    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *
    import pykhaos.utils.pyspark_configuration as pyspark_config

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="ticket_triggers_join", log_level="OFF",
                                                              min_n_executors=1, max_n_executors=15, n_cores=4,
                                                              executor_memory="32g", driver_memory="32g")
    print("############# Process Started ##############")

    starting_day = args.starting_d.split()[0]
    closing_day = args.closing_d.split()[0]

    starting_day = "20190918"
    closing_day = "20191018"

    # starting_day = sys.argv[1]

    # closing_day = sys.argv[2]

    # starting_day = '20190807'

    # closing_day = '20190907'

    from pyspark.sql.functions import collect_set, concat, size, coalesce, lpad, struct, count as sql_count, lit, \
        regexp_extract, greatest, col, \
        min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum

    closing_day_year = str(int(closing_day[0:4]))

    closing_day_month = str(int(closing_day[4:6]))

    closing_day_day = str(int(closing_day[6:8]))

    print '[Info] starting_day = ' + starting_day
    print '[Info] closing_day = ' + closing_day

    print '[Info triggers_join] closing_day = ' + closing_day + ' - len(closing_day) = ' + str(len(closing_day))

    print '[Info] Input dates (string): year = ' + closing_day[0:4] + ' - month = ' + closing_day[
                                                                                      4:6] + ' - day = ' + closing_day[
                                                                                                           6:8]
    print '[Info] Input dates (int): year = ' + str(int(closing_day[0:4])) + ' - month = ' + str(
        int(closing_day[4:6])) + ' - day = ' + str(int(closing_day[6:8]))

    partition_date = "year={}/month={}/day={}".format(str(int(closing_day[0:4])), str(int(closing_day[4:6])),
                                                      str(int(closing_day[6:8])))

    # model1 is Cristina's model
    df_model1 = spark.read.load("/data/udf/vf_es/churn/triggers/model1_50k_filt/" + partition_date) \
        .select(['NIF_CLIENTE', 'calib_model_score']) \
        .where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(
        ["NIF_CLIENTE"])
    # .withColumnRenamed("calib_model_score", "calib_model_score_model1") \

    print("STATS")  # just a check, count must be 50k in both cases
    print('\tNIFs model1 = {}'.format(df_model1.count()))


    # from churn.analysis.triggers.trigger_billing_ccc.trigger_smart_exploration import get_customer_master

    # _, volume_ref, churn_ref = get_customer_master(spark, closing_day)

    # print("Reference base: churn_rate={:.2f} volume={}".format(100.0 * churn_ref, volume_ref))


    df_all = df_model1

    df_all = df_all.cache()

    df_scores = df_all

    num_rows = df_all.count()

    print("Combination of both models has {} unique nifs".format(num_rows))

    path_to_save = "/data/udf/vf_es/churn/triggers/model_combined/"

    df_all = df_all.withColumn("day", lit(closing_day_day).cast('int'))
    df_all = df_all.withColumn("month", lit(closing_day_month).cast('int'))
    df_all = df_all.withColumn("year", lit(closing_day_year).cast('int'))

    print("Started saving - {} for closing_day={}".format(path_to_save, closing_day))
    (df_all.coalesce(1).write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print '[Info triggers_join] df_all saved - num rows: ' + str(df_all.count())
    load_reasons = '/data/udf/vf_es/churn/churn_reasons/trigger_reasons_results/year={}/month={}/day={}'.format(
        int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    df_reasons = spark.read.load(load_reasons)

    print '[Info triggers_join] df_reasons - num rows: ' + str(df_reasons.count())

    print '[Info triggers_join] closing_day = ' + closing_day + ' - len(closing_day) = ' + str(len(closing_day))

    df_cris_reasons = df_reasons.where(col('model') == 'csanc109')
    print'Loaded Reasons Dataframes'

    print '[Info triggers_join] df_cris_reasons - num rows: ' + str(df_cris_reasons.count())

    selcols = ['NIF_CLIENTE', 'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason',
               'Incertidumbre']

    EXTRA_INFO_COLS = ["top0_reason", "top1_reason", "top2_reason", "top3_reason", "top4_reason", "Incertidumbre"]


    df_full = df_scores.join(df_cris_reasons.select(selcols), ['NIF_CLIENTE'],
                                                                    'inner')

    print '[Info triggers_join] df_full_cris - num rows: ' + str(df_full.count())

    df_final = df_full

    print '[Info triggers_join] df_final - num rows: ' + str(df_final.count())

    for ii in range(0, 5):
        df_final = df_final.withColumn("top{}_reason".format(ii),
                                       regexp_extract(col("top{}_reason".format(ii)), "^\[(.*)\]$", 1))

    print '[Info triggers_join] After adding top_reasons - df_final - num rows: ' + str(df_final.count())

    save_dir = '/data/udf/vf_es/churn/triggers/scores_and_reasons/year={}/month={}/day={}'.format(int(closing_day[0:4]),
                                                                                                  int(closing_day[4:6]),
                                                                                                  int(closing_day[6:8]))
    df_final.repartition(300).write.save(save_dir, format='parquet', mode='append')

    print("Final df saved as")
    print(save_dir)

    print '[Info triggers_join] df_final saved - num rows: ' + str(df_final.count())

    print("Starting to save to model outputs")

    model_params_dict = {"model_level": ["nif"],
                         "training_closing_date": ['20190414'],
                         "target": ['full car'],
                         "model_path": [
                             " /data/udf/vf_es/churn/triggers/car_tr && /data/udf/vf_es/churn/triggers/car_exploration_model1"],
                         # car used to find the rule
                         "metrics_path": [""],
                         "metrics_train": ["vol={};churn_rate={};lift={}".format(84300, 18.50, 5.71)],
                         # volume after filters
                         "metrics_test": [""],
                         "varimp": ["/data/udf/vf_es/churn/triggers/feat_imp/year={}/month={}/day={}"],
                         "algorithm": ["ensembled rf"],
                         "author_login": ["ds_team"],
                         "extra_info": [
                             "top0_reason | top1_reason | top2_reason | top3_reason | top4_reason | Incertidumbre"],
                         }

    model_name = 'triggers_ml'

    df_model_scores = df_final.withColumnRenamed('calib_model_score', 'scoring').drop('executed_at', 'model',
                                                                                      'closing_day')

    df_model_scores.printSchema()

    df_parameters, df_model_scores = create_model_output_dataframes(spark, closing_day, df_model_scores,
                                                                    model_params_dict, model_name,
                                                                    extra_info_cols=EXTRA_INFO_COLS)

    df_parameters.show()

    print '[Info] df_parameters showed above'

    df_model_scores.show()

    print '[Info] df_model_scores showed above'

    ################# INSERTING INTO MODEL SCORES ##############

    df_model_scores \
        .coalesce(1) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/data/attributes/vf_es/model_outputs/model_scores/")

    df_parameters \
        .coalesce(1) \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/data/attributes/vf_es/model_outputs/model_parameters/")

    '''
    from churn.datapreparation.general.model_outputs_manager import insert_to_model_scores, insert_to_model_parameters

    insert_to_model_scores(df_model_scores)
    print("Inserted to model outputs scores")
    insert_to_model_parameters(df_parameters)
    print("Inserted to model outputs parameters")
    '''

    print'############ Finished Process ############'


