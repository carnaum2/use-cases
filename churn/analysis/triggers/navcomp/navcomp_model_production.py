
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import os
import sys
import time
import math
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
                                   lpad,
                                   countDistinct,
                                   row_number,
                                   regexp_replace,
                                   upper,
                                   trim,
                                   array,
                                   create_map,
                                   randn,
                                   split)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType
import datetime as dt
import pandas as pd

def getOrderedRelevantFeats(model, featCols, pca="f"):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances
        return getImportantFeaturesFromVector(featCols, impFeats)

def getImportantFeaturesFromVector(featCols, impFeats):
    feat_and_imp=zip(featCols, impFeats.toArray())
    return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)

def get_last_date(spark):


    from churn.datapreparation.general.customer_base_utils import get_last_date

    base_last_date = get_last_date(spark)

    navcomp_last_date = spark\
    .read\
    .parquet('/data/attributes/vf_es/return_feed/data_navigation')\
    .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
    .select(sql_max(col('mydate')).alias('last_date'))\
    .rdd\
    .first()['last_date']

    last_date = str(min([navcomp_last_date, base_last_date]))

    return last_date


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

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################
    from churn.analysis.triggers.navcomp.metadata import METADATA_STANDARD_MODULES

    import argparse

    parser = argparse.ArgumentParser(
        description="Run navcomp model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in test')

    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf",
                        help='model to be used for training de model')

    parser.add_argument('--sources', metavar='<YYYYMMDD>', type=str, required=False, default=METADATA_STANDARD_MODULES,
                        help='list of sources to be used for building the training dataset')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    model_ = args.model
    metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    print("Starting check of input params")

    if not tt_date_:
        print("Not introduced a test date. Computing automatically...")
        tt_date_ = str(get_last_date(spark))
        print("Computed tt_date_={}".format(tt_date_))
    else:
        #TODO check something?
        pass


    if not tr_date_:
        print("Not introduced a training date. Computing automatically")
        from churn_nrt.src.utils.date_functions import move_date_n_days
        tr_date_ = move_date_n_days(tt_date_, n=-16)
        print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))

    else:
        #TODO check something?
        pass

    print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    #################################################


    #from churn.analysis.triggers.navcomp.metadata import get_metadata
    #get_metadata(spark, sources=metadata_sources).show()

    start_time_process =  time.time()

    # Parsing year, month, day

    tt_year_ = tt_date_[0:4]

    tt_month_ = str(int(tt_date_[4:6]))

    tt_day_ = str(int(tt_date_[6:8]))

    filter_ = 'comps'
    # Save modules
    save_ = True
    # Show debug traces
    verbose = True

    ##########################################################################################
    # 2. Loading tr data
    # ##########################################################################################

    from churn.analysis.triggers.navcomp.navcomp_utils import get_labeled_set_msisdn

    from churn.analysis.triggers.base_utils.base_utils import get_active_filter, get_disconnection_process_filter, get_churn_call_filter

    tr_set = get_labeled_set_msisdn(spark, tr_date_, sources=metadata_sources, save_ = save_, verbose = verbose)

    # Modeling filters

    tr_active_filter = get_active_filter(spark, tr_date_, 90)

    tr_disconnection_filter = get_disconnection_process_filter(spark, tr_date_, 90)

    tr_churn_call_filter = get_churn_call_filter(spark, tr_date_, 90, 'msisdn')

    tr_set = tr_set \
        .join(tr_active_filter, ['msisdn'], 'inner') \
        .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tr_churn_call_filter, ['msisdn'], 'inner')

    from churn.analysis.triggers.navcomp.navcomp_model import filter_population

    tr_set = filter_population(spark, tr_set, filter_)

    tr_set.groupBy('label').agg(count('*').alias('num_samples')).show()

    print "[Info navcomp_model_production] After all the filters - Label count on tr set showed above"

    ##########################################################################################
    # 3. Loading tt data
    # ##########################################################################################

    from churn.analysis.triggers.navcomp.navcomp_utils import get_unlabeled_set_msisdn

    tt_set = get_unlabeled_set_msisdn(spark, tt_date_, sources=metadata_sources, save_=save_, verbose=verbose)

    # Modeling filters

    tt_active_filter = get_active_filter(spark, tt_date_, 90)

    tt_disconnection_filter = get_disconnection_process_filter(spark, tt_date_, 90)

    tt_churn_call_filter = get_churn_call_filter(spark, tt_date_, 90, 'msisdn')

    tt_set = tt_set \
        .join(tt_active_filter, ['msisdn'], 'inner') \
        .join(tt_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tt_churn_call_filter, ['msisdn'], 'inner')

    tt_set = filter_population(spark, tt_set, filter_)

    print "[Info navcomp_model_production] After all the filters - Sie of the test set: " + str(tt_set.count()) + " - Number of distinct MSISDNs in the test set: " + str(tt_set.select('msisdn').distinct().count())

    ##########################################################################################
    # 4. Modeling
    ##########################################################################################

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    metadata = get_metadata(spark)

    categorical_columns = metadata.filter(col('type')=='categorical').rdd.map(lambda x: x['feature']).collect()
    stages = []

    for categorical_col in categorical_columns:
        print("Categorical: {}".format(categorical_col))
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    numeric_columns = metadata.filter(col('type')=='numeric').rdd.map(lambda x: x['feature']).collect()
    #numeric_columns = ['f1', 'f2', 'f3']
    assembler_inputs = [c + "_class_vec" for c in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    for cfeat in categorical_columns:
        print "[Info navcomp_model_production] Categorical column: " + cfeat

    for nfeat in numeric_columns:
        print "[Info navcomp_model_production] Numeric column: " + nfeat


    def get_model(x):
        return {
            'rf': RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol="label", seed=1234,
                                         maxBins=32, minInstancesPerNode=100, impurity='gini',
                                         featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'gbt': GBTClassifier(featuresCol='features', labelCol='label', maxDepth=5, maxBins=32,
                                 minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100,
                                 stepSize=0.1, seed=None, subsamplingRate=0.7),
        }[x]


    dt_model = get_model(model_)
    stages += [dt_model]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_set)

    feat_importance_list = getOrderedRelevantFeats(pipeline_model, assembler_inputs, pca="f")

    for fimp in feat_importance_list:
        print "[Info navcomp_model_production] Feat imp - " + fimp[0] + ": " + str(fimp[1])

    ##########################################################################################
    # 5. Prediction
    ##########################################################################################

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    predictions_df = pipeline_model \
        .transform(tt_set) \
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType())) \
        .withColumn('model_score', col('model_score') + lit(0.000001) * randn())

    ##########################################################################################
    # 6. Model outputs
    ##########################################################################################

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

    from churn_nrt.src.utils.date_functions import get_next_dow
    partition_date = get_next_dow(3) # get day of next wednesday

    partition_year = int(partition_date[0:4])

    partition_month = int(partition_date[4:6])

    partition_day = int(partition_date[6:8])

    # Marking top 20K at risk

    #pcg = 20000.0/float(predictions_df.count())
    pcg = 0.9

    th = predictions_df.approxQuantile('model_score', [pcg], 0.0)[0]

    print "[Info navcomp_model_production] Threshold to get top 10% at risk is " + str(th)

    df_model_scores = (predictions_df
        .withColumn("model_name", lit("triggers_navcomp").cast("string"))
        .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string"))
        .withColumn("model_executed_at", col("executed_at").cast("string"))
        .withColumn("client_id", lit(""))
        .withColumn("msisdn", col("msisdn").cast("string"))
        .withColumn("nif", lit(""))
        .withColumn("scoring", col("model_score").cast("float"))
        .withColumn("model_output", when(col("model_score") > th, "1").otherwise("0"))
        .withColumn("prediction", lit("").cast("string"))
        .withColumn("extra_info", col('most_consulted_operator').cast("string"))
        .withColumn("predict_closing_date", lit(tt_date_))
        .withColumn("year", lit(partition_year).cast("integer"))
        .withColumn("month", lit(partition_month).cast("integer"))
        .withColumn("day", lit(partition_day).cast("integer"))
        .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
        .select(*model_output_cols))
    #        .withColumn("msisdn", lit("")) \
    # .withColumnRenamed("NIF_CLIENTE", "nif") \
    ##########################################################################################
    # 7. Model parameters
    ##########################################################################################

    df_pandas = pd.DataFrame({
        "model_name": ['triggers_navcomp'],
        "executed_at": [executed_at],
        "model_level": ["msisdn"],
        "training_closing_date": [tr_date_],
        "target": [""],
        "model_path": [""],
        "metrics_path": [""],
        "metrics_train": [""],
        "metrics_test": [""],
        "varimp": [";".join([f[0] for f in feat_importance_list])],
        "algorithm": [model_],
        "author_login": ["jmarcoso"],
        "extra_info": [""],
        "scores_extra_info_headers": ["most_visited_operator"],
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

    print("[Info navcomp_model_production] Inserted to model outputs '/data/attributes/vf_es/model_outputs/model_scores/model_name={}/year={}/month={}/day={}'".format('triggers_navcomp',
                                                                                                                                                                       partition_year,
                                                                                                                                                                       partition_month,
                                                                                                                                                                       partition_day))


    print("ELAPSED TIME: {} HOURS".format( (time.time() - start_time_process)/3600.0))



