# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, isnull,substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.functions import *
from utils_trigger import get_trigger_minicar, get_billing_car, getIds, get_tickets_car, get_filtered_car, get_next_dow
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
import matplotlib
matplotlib.use('Agg')

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

        #from churn.utils.constants import CHURN_DELIVERIES_DIR
        #root_dir = CHURN_DELIVERIES_DIR
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
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= str, help= 'closing day', required=True)
    parser.add_argument('-s', '--starting_d', metavar = '<starting_d>', type= str, help= 'starting day', required=True)
    args = parser.parse_args()

    set_paths_and_logger()

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *
    import pykhaos.utils.pyspark_configuration as pyspark_config

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="ticket_triggers_join", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
    
    starting_day = args.starting_d
    closing_day = args.closing_d

    from pyspark.sql.functions import collect_set, concat, size, coalesce, lpad, struct, count as sql_count, lit, regexp_extract, greatest, col, \
    min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum

    partition_date = "year={}/month={}/day={}".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    # model1 is Cristina's model
    df_model1 = spark.read.load("/data/udf/vf_es/churn/triggers/model1_50k/" + partition_date)\
                 .select(['NIF_CLIENTE', 'calib_model_score']).withColumnRenamed("calib_model_score", "calib_model_score_model1")\
                 .where(col("NIF_CLIENTE").isNotNull()).drop_duplicates(["NIF_CLIENTE"])

    # model2 is Alvaro's model
    df_model2 = spark.read.load("/data/udf/vf_es/churn/triggers/model2_50k/" + partition_date).select(['NIF_CLIENTE', 'calib_model_score']).withColumnRenamed("calib_model_score", "calib_model_score_model2")\
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

    from churn.analysis.triggers.trigger_billing_ccc.trigger_smart_exploration import get_customer_master

    _, volume_ref, churn_ref = get_customer_master(spark, closing_day)

    print("Reference base: churn_rate={:.2f} volume={}".format(100.0 * churn_ref, volume_ref))

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

    df_all = df_all.withColumn("day", lit(int(closing_day[6:])))
    df_all = df_all.withColumn("month", lit(int(closing_day[4:6])))
    df_all = df_all.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {} for closing_day={}".format(path_to_save, closing_day))
    (df_all.coalesce(1).write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    df_reasons = spark.read.load(TABLE_TRIGGER)
    df_fil_reasons = df_reasons.where(col('closing_day') == closing_day)

    df_alv_reasons = df_fil_reasons.where(col('model') == 'asaezco')
    df_cris_reasons = df_fil_reasons.where(col('model') == 'csanc109')
    print'Loaded Reasons Dataframes'

    selcols = ['NIF_CLIENTE','top0_reason','top1_reason','top2_reason','top3_reason','top4_reason', 'Incertidumbre']

    df_full_alv = df_scores.where(col('model') == 'asaezco').join(df_alv_reasons.select(selcols), ['NIF_CLIENTE'], 'inner')
    df_full_cris = df_scores.where(col('model') == 'csanc109').join(df_cris_reasons.select(selcols), ['NIF_CLIENTE'], 'inner')
    df_final = df_full_cris.union(df_full_alv)

    for ii in range(0, 5):
        df_final = df_final.withColumn("top{}_reason".format(ii),regexp_extract(col("top{}_reason".format(ii)), "^\[(.*)\]$", 1))


    save_dir = '/data/udf/vf_es/churn/triggers/trigger_reasons/year={}/month={}/day={}'.format(int(closing_day[:4]),
                                                                                               int(closing_day[4:6]),
                                                                                               int(closing_day[6:8]))
    df_final.repartition(300).write.save(save_dir, format='parquet', mode='append')

    print("Final df saved as")
    print(save_dir)

    print("Starting to save final df into model outputs")

    

    print'############ Finished Process ############'
    
    
