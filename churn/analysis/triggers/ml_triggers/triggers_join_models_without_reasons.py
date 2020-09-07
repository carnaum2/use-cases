#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, isnull,
                                   substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.functions import *
import matplotlib


matplotlib.use('Agg')

def get_next_day_of_the_week(day_of_the_week):

    import datetime as dt

    idx = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    n = idx[day_of_the_week.lower()]

    # import datetime
    d = dt.date.today()

    while d.weekday() != n:
        d += dt.timedelta(1)

    year_= str(d.year)
    month_= str(d.month).rjust(2, '0')
    day_= str(d.day).rjust(2, '0')

    print 'Next ' + day_of_the_week + ' is day ' + str(day_) + ' of month ' + str(month_) + ' of year ' + str(year_)

    return year_+month_+day_

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

    starting_day = "20190903"
    closing_day = "20191003"

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
    df_model1 = spark.read.load("/data/udf/vf_es/churn/triggers/model1_50k/" + partition_date) \
        .select(['NIF_CLIENTE', 'calib_model_score']).withColumnRenamed("calib_model_score", "calib_model_score_model1") \
        .where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(
        .where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(
        ["NIF_CLIENTE"])

    # model2 is Alvaro's model
    df_model2 = spark.read.load("/data/udf/vf_es/churn/triggers/model2_50k/" + partition_date).select(
        ['NIF_CLIENTE', 'calib_model_score']).withColumnRenamed("calib_model_score", "calib_model_score_model2") \
        .where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(["NIF_CLIENTE"])

    print("STATS")  # just a check, count must be 50k in both cases
    print('\tNIFs model1 = {}'.format(df_model1.count()))
    print('\tNIFs model2 = {}'.format(df_model2.count()))

    # compute common nifs
    df_inner = df_model1.join(df_model2, on=["nif_cliente"], how="inner")
    common_nifs = df_inner.count()
    print("\t Common nifs = {}".format(common_nifs))

    # NIFs in Cris' model not included in Alvaro's model
    df_model1_no_model2 = df_model2.select("nif_cliente").join(df_model1.select("nif_cliente"), ['nif_cliente'],
                                                               'right').where(df_model2['nif_cliente'].isNull())

    # NIFs in Alvaro's model not included in Cris' model
    df_model2_no_model1 = df_model1.select("nif_cliente").join(df_model2.select("nif_cliente"), ['nif_cliente'],
                                                               'right').where(df_model1['nif_cliente'].isNull())

    # Absurd print, since new NIFs are 50k - common, but good for checking
    print("NIFs nuevos model1 sobre model2 = {} [check={}]".format(df_model1_no_model2.count(),
                                                                   df_model1_no_model2.count() + common_nifs))
    print("NIFs nuevos model2 sobre model1 = {} [check={}]".format(df_model2_no_model1.count(),
                                                                   df_model2_no_model1.count() + common_nifs))

    # from churn.analysis.triggers.trigger_billing_ccc.trigger_smart_exploration import get_customer_master

    # _, volume_ref, churn_ref = get_customer_master(spark, closing_day)

    # print("Reference base: churn_rate={:.2f} volume={}".format(100.0 * churn_ref, volume_ref))

    pos_max_udf = udf(
        lambda milist: sorted([(vv, idx) for idx, vv in enumerate(milist)], key=lambda tup: tup[0], reverse=True)[0][1],
        IntegerType())

    df_all = df_model2.join(df_model1, on=["nif_cliente"], how="full").fillna(0)

    df_all = df_all.withColumn("calib_model_score", greatest(*["calib_model_score_model1", "calib_model_score_model2"]))

    df_all = df_all.withColumn("score_array", array(["calib_model_score_model1", "calib_model_score_model2"]))
    df_all = df_all.withColumn("pos_max", pos_max_udf(col("score_array")))

    df_all = df_all.where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(["NIF_CLIENTE"])

    df_all = df_all.withColumn("model",
                               when(col("pos_max") == 0, "csanc109").when(col("pos_max") == 1, "asaezco").otherwise(
                                   "unknown"))

    df_all = df_all.drop("pos_max", "score_array")
    df_all = df_all.sort(desc("calib_model_score"))

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

    ##########################################################################################
    # 6. Model outputs
    ##########################################################################################

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

    partition_date = get_next_day_of_the_week('wednesday')

    partition_year = int(partition_date[0:4])

    partition_month = int(partition_date[4:6])

    partition_day = int(partition_date[6:8])

    import datetime as dt

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    df_model_scores = df_all \
        .withColumn("model_name", lit("triggers_ml").cast("string")) \
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
        .withColumn("model_executed_at", col("executed_at").cast("string")) \
        .withColumn("client_id", lit("")) \
        .withColumn("msisdn", lit("")) \
        .withColumnRenamed("NIF_CLIENTE", "nif") \
        .withColumn("scoring", col("calib_model_score").cast("float")) \
        .withColumn("model_output", lit("").cast("string")) \
        .withColumn("prediction", lit("").cast("string")) \
        .withColumn("extra_info", lit(";".join(5*["-"])).cast("string")) \
        .withColumn("predict_closing_date", lit(closing_day)) \
        .withColumn("year", lit(partition_year).cast("integer")) \
        .withColumn("month", lit(partition_month).cast("integer")) \
        .withColumn("day", lit(partition_day).cast("integer")) \
        .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer")) \
        .select(*model_output_cols)

    ##########################################################################################
    # 7. Model parameters
    ##########################################################################################

    df_pandas = pd.DataFrame({
        "model_name": ['triggers_ml'],
        "executed_at": [executed_at],
        "model_level": ["nif"],
        "training_closing_date": [starting_day],
        "target": [""],
        "model_path": [""],
        "metrics_path": [""],
        "metrics_train": [""],
        "metrics_test": [""],
        "varimp": ["-"],
        "algorithm": ["rf"],
        "author_login": ["csanc109"],
        "extra_info": [""],
        "scores_extra_info_headers": [""],
        "year": [partition_year],
        "month": [partition_month],
        "day": [partition_day],
        "time": [int(executed_at.split("_")[1])]
    })

    df_model_parameters = spark \
        .createDataFrame(df_pandas) \
        .withColumn("day", col("day").cast("integer")) \
        .withColumn("month", col("month").cast("integer")) \
        .withColumn("year", col("year").cast("integer")) \
        .withColumn("time", col("time").cast("integer"))

    #######################

    df_model_scores \
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

    print("Inserted to model outputs")

    print'############ Finished Process ############'


