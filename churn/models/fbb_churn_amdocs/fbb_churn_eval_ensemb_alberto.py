# coding: utf-8

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
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
                                    row_number)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, LongType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from datetime import datetime
from itertools import chain
import numpy as np
from functools import reduce
from utils_general import *
from utils_model import *
from metadata_fbb_churn import *
from feature_selection_utils import *
import subprocess
#from date_functions import get_next_cycle

def set_paths():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and "dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    USE_CASES = "/var/SP/data/home/asaezco/src/devel2/use-cases"#dirname(os.path.abspath(imp.find_module('churn')[1]))

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))

####################################
### Creating Spark Session
###################################

def get_spark_session(app_name="default name", log_level='INFO', min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g"):
    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)


    setting_bdp(app_name=app_name, min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores, executor_memory = executor_memory, driver_memory=driver_memory)
    from common.src.main.python.utils.hdfs_generic import run_sc
    sc, spark, sql_context = run_sc(log_level=log_level)

    return sc, spark, sql_context


# set BDP parameters
def setting_bdp(min_n_executors = 1, max_n_executors = 15, n_cores = 8, executor_memory = "16g", driver_memory="8g",
                   app_name = "Python app", driver_overhead="1g", executor_overhead='3g'):

    MAX_N_EXECUTORS = max_n_executors
    MIN_N_EXECUTORS = min_n_executors
    N_CORES_EXECUTOR = n_cores
    EXECUTOR_IDLE_MAX_TIME = 120
    EXECUTOR_MEMORY = executor_memory
    DRIVER_MEMORY = driver_memory
    N_CORES_DRIVER = 1
    MEMORY_OVERHEAD = N_CORES_EXECUTOR * 2048
    QUEUE = "root.BDPtenants.es.medium"
    BDA_CORE_VERSION = "1.0.0"

    SPARK_COMMON_OPTS = os.environ.get('SPARK_COMMON_OPTS', '')
    SPARK_COMMON_OPTS += " --executor-memory %s --driver-memory %s" % (EXECUTOR_MEMORY, DRIVER_MEMORY)
    SPARK_COMMON_OPTS += " --conf spark.shuffle.manager=tungsten-sort"
    SPARK_COMMON_OPTS += "  --queue %s" % QUEUE

    # Dynamic allocation configuration
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.enabled=true"
    SPARK_COMMON_OPTS += " --conf spark.shuffle.service.enabled=true"
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.maxExecutors=%s" % (MAX_N_EXECUTORS)
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.minExecutors=%s" % (MIN_N_EXECUTORS)
    SPARK_COMMON_OPTS += " --conf spark.executor.cores=%s" % (N_CORES_EXECUTOR)
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.executorIdleTimeout=%s" % (EXECUTOR_IDLE_MAX_TIME)
    # SPARK_COMMON_OPTS += " --conf spark.ui.port=58235"
    SPARK_COMMON_OPTS += " --conf spark.port.maxRetries=100"
    SPARK_COMMON_OPTS += " --conf spark.app.name='%s'" % (app_name)
    SPARK_COMMON_OPTS += " --conf spark.submit.deployMode=client"
    SPARK_COMMON_OPTS += " --conf spark.ui.showConsoleProgress=true"
    SPARK_COMMON_OPTS += " --conf spark.sql.broadcastTimeout=1200"
    SPARK_COMMON_OPTS += " --conf spark.yarn.executor.memoryOverhead={}".format(executor_overhead)
    SPARK_COMMON_OPTS += " --conf spark.yarn.executor.driverOverhead={}".format(driver_overhead)
    SPARK_COMMON_OPTS += " --conf spark.shuffle.service.enabled = true"

    BDA_ENV = os.environ.get('BDA_USER_HOME', '')

    # Attach bda-core-ra codebase
    SPARK_COMMON_OPTS+=" --files {}/scripts/properties/red_agent/nodes.properties,{}/scripts/properties/red_agent/nodes-de.properties,{}/scripts/properties/red_agent/nodes-es.properties,{}/scripts/properties/red_agent/nodes-ie.properties,{}/scripts/properties/red_agent/nodes-it.properties,{}/scripts/properties/red_agent/nodes-pt.properties,{}/scripts/properties/red_agent/nodes-uk.properties".format(*[BDA_ENV]*7)


    os.environ["SPARK_COMMON_OPTS"] = SPARK_COMMON_OPTS
    os.environ["PYSPARK_SUBMIT_ARGS"] = "%s pyspark-shell " % SPARK_COMMON_OPTS
    #os.environ["SPARK_EXTRA_CONF_PARAMETERS"] = '--conf spark.yarn.jars=hdfs:///data/raw/public/lib_spark_2_1_0_jars_SPARK-18971/*'

def initialize(app_name, min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "16g", driver_memory="8g"):
    import time
    start_time = time.time()

    print("_initialize spark")
    #import pykhaos.utils.pyspark_configuration as pyspark_config
    sc, spark, sql_context = get_spark_session(app_name=app_name, log_level="OFF", min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores,
                             executor_memory = executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark

if __name__ == "__main__":

    set_paths()

    from pykhaos.utils.date_functions import *
    from utils_fbb_churn import *

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'

    global sqlContext

    spark = initialize("VF_ES AMDOCS FBB Churn Prediction ", executor_memory="16g", min_n_executors=10)
    print('Spark Configuration used', spark.sparkContext.getConf().getAll())

    selcols = getIdFeats() + getCrmFeats() + getBillingFeats() + getMobSopoFeats() + getOrdersFeats()
    now = datetime.now()

    date_name = str(now.year) + str(now.month).rjust(2, '0') + str(now.day).rjust(2, '0')
    origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'

    ## ARGUMENTS
    ###############
    parser = argparse.ArgumentParser(
        description='Generate score table for fbb model',
        epilog='Please report bugs and issues to Beatriz <beatriz.gonzalez2@vodafone.com>')

    parser.add_argument('-s', '--training_day', metavar='<TRAINING_DAY>', type=str, required=True,
                        help='Training day YYYYMMDD. Date of the CAR taken to train the model.')
    parser.add_argument('-p', '--prediction_day', metavar='<PREDICTION_DAY>', type=str, required=True,
                        help='Prediction day YYYYMMDD.')
    parser.add_argument('-o', '--horizon', metavar='<horizon>', type=int, required=True,
                        help='Number of cycles used to gather the portability requests from the training day.')
    args = parser.parse_args()

    print(args)

    # Cycle used for CAR and Extra Feats in the training set
    trcycle_ini = args.training_day# '20181130'  # Training data
    # Number of cycles to gather dismiss requests
    horizon = args.horizon #4
    # Cycle used for CAR and Extra Feats in the test set
    ttcycle_ini = args.prediction_day#'20181231'  # Test data

    tr_ttdates = trcycle_ini + '_' + ttcycle_ini

    ########################
    ### 1. TRAINING DATA ###
    ########################

    # 1.1. Loading training data

    inittrdf_ini = getFbbChurnLabeledCarCycles(spark, origin, trcycle_ini, selcols, horizon)
    #inittrdf_ini.repartition(200).write.save(path, format='parquet', mode='overwrite')

    ## Reading the Extra Features
    dfExtraFeat = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                         .format(int(trcycle_ini[0:4]), int(trcycle_ini[4:6]), int(trcycle_ini[6:8])))

    # Taking only the clients with a fbb service
    dfExtraFeatfbb = dfExtraFeat.join(inittrdf_ini, ["num_cliente"], "leftsemi")

    dfExtraFeatfbb = dfExtraFeatfbb.cache()
    print "[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats: ", dfExtraFeatfbb.count()

    # Taking the Extra Features of interest and adding their values for num_client when necessary
    dfExtraFeatSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb)

    print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeatSel.columns:
            dfillNa.pop(kkey, None)

    inittrdf = inittrdf_ini.join(dfExtraFeatSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)

    print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf to HDFS " +str(inittrdf.count())
    #inittrdf.repartition(200).write.save(path, format='parquet', mode='overwrite')

    #path1 = '/data/udf/vf_es/churn/fbb_tmp/unbaltrdf_' + tr_ttdates
    #path2 = '/data/udf/vf_es/churn/fbb_tmp/valdf_' + tr_ttdates
    #if (pathExist(path1)) and (pathExist(path2)):

        #print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path1) + " and " + str(path2) + " already exist. Reading them."

        #unbaltrdf = spark.read.parquet(path1)
        #valdf = spark.read.parquet(path2)
    #else:

        #print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the training set" + str(inittrdf.count())

    [unbaltrdf, valdf] = inittrdf.randomSplit([0.7, 0.3], 1234)

    #print "[Info Main FbbChurn] " + time.ctime() + " Stat description of the target variable printed above"

    unbaltrdf = unbaltrdf.cache()
    valdf = valdf.cache()
        #print "[Info Main FbbChurn] " + time.ctime() + " Saving unbaltrdf to HDFS " + str(unbaltrdf.count())
        #print "[Info Main FbbChurn] " + time.ctime() + " Saving valdf to HDFS " + str(valdf.count())

        #unbaltrdf.repartition(300).write.save(path1,format='parquet', mode='overwrite')
        #valdf.repartition(300).write.save(path2,format='parquet', mode='overwrite')

    # 1.2. Balanced df for training

    #path = "/data/udf/vf_es/churn/fbb_tmp/trdf_" + tr_ttdates
    #if (pathExist(path)):


    unbaltrdf.groupBy('label').agg(count('*')).show()

    print "[Info Main FbbChurn]" + time.ctime() + " Count on label column for unbalanced tr set showed above"
    trdf = balance_df2(unbaltrdf, 'label')

    trdf.groupBy('label').agg(count('*')).show()
        #print "[Info Main FbbChurn] " + time.ctime() + " Saving trdf to HDFS "

        #trdf.repartition(300).write.save(path, format='parquet',mode='overwrite')

    # 1.3. Feature selection

    allFeats = trdf.columns

    # Getting only the numeric variables
    catCols = [item[0] for item in trdf.dtypes if item[1].startswith('string')]
    numerical_feats = list(set(allFeats) - set(list(
        set().union(getIdFeats(), getIdFeats_tr(), getNoInputFeats(), catCols, [c + "_enc" for c in getCatFeatsCrm()],
                    ["label"]))))

    noninf_feats = getNonInfFeats(trdf, numerical_feats)

    for f in noninf_feats:
        print "[Info Main FbbChurn] Non-informative feat: " + f

    ####################
    ### 2. TEST DATA ###
    ####################

    #path = "/data/udf/vf_es/churn/fbb_tmp/ttdf_ini_" + tr_ttdates
    #if (pathExist(path)):

        #print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        #ttdf_ini = spark.read.parquet(path)

    #else:

    ttdf_ini = getFbbChurnLabeledCarCycles(spark, origin, ttcycle_ini, selcols,horizon)

    #print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf_ini to HDFS "
    #ttdf_ini.repartition(200).write.save(path,format='parquet', mode='overwrite')

        #ttdf_ini.describe('label').show()

    #path = "/data/udf/vf_es/churn/fbb_tmp/ttdf_" + tr_ttdates
    #if (pathExist(path)):

    #   print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
    #    ttdf = spark.read.parquet(path)

    #else:

    dfExtraFeat_tt = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                            .format(int(ttcycle_ini[0:4]), int(ttcycle_ini[4:6]), int(ttcycle_ini[6:8])))

    dfExtraFeatfbb_tt = dfExtraFeat_tt.join(ttdf_ini.select('num_cliente'), on='num_cliente', how='leftsemi')
    print(dfExtraFeatfbb_tt.select('num_cliente').distinct().count(), ttdf_ini.select('num_cliente').distinct().count())

    dfExtraFeatfbb_tt = dfExtraFeatfbb_tt.cache()
    print("[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats ", dfExtraFeatfbb_tt.count())

    dfExtraFeat_ttSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb_tt)

        #print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client in tt"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeat_ttSel.columns:
            dfillNa.pop(kkey, None)

    ttdf = ttdf_ini.join(dfExtraFeat_ttSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
    print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the test set " + str(ttdf.count())
    print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf to HDFS "

        #tdf = ttdf.repartition(300)
        #ttdf.repartition(300).write.save(path, format='parquet', mode='overwrite')

    ####################
    ### 3. MODELLING ###
    ####################

    featCols = list(set(numerical_feats) - set(noninf_feats))

    for f in featCols:
        print "[Info Main FbbChurn] Input feat: " + f

    assembler = VectorAssembler(inputCols=featCols, outputCol="features")

    classifier = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=15, \
                                        maxBins=32, \
                                        minInstancesPerNode=200, \
                                        impurity="gini", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.7, \
                                        numTrees=800, \
                                        seed=1234)

    pipeline = Pipeline(stages=[assembler, classifier])
    model = pipeline.fit(trdf)

    feat_importance = getOrderedRelevantFeats(model, featCols, 'f', 'rf')


    for fimp in feat_importance:
        print "[Info Main FbbChurn] Imp feat " + str(fimp[0]) + ": " + str(fimp[1])


    ##################
    ### EVALUATION ###
    ##################

    # Calibration
    calibmodel = get_calibration_function2(spark, model, valdf, 'label', 10)

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    # Train
    tr_preds_df = model.transform(trdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tr_calib_preds_df = calibmodel[0].transform(tr_preds_df)

    # Train evaluation
    trPredictionAndLabels = tr_calib_preds_df.select(['calib_model_score', 'label']).rdd.map(lambda r: (r['calib_model_score'], r['label']))
    trmetrics = BinaryClassificationMetrics(trPredictionAndLabels)

    # Test eval
    tt_preds_df = model.transform(ttdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tt_calib_preds_df = calibmodel[0].transform(tt_preds_df)

    # Evaluation
    ttPredictionAndLabels = tt_calib_preds_df.select(['calib_model_score', 'label']).rdd.map(lambda r: (r['calib_model_score'], r['label']))
    ttmetrics = BinaryClassificationMetrics(ttPredictionAndLabels)

    print(" Area under ROC(tr) = " + str(trmetrics.areaUnderROC))
    print(" Area under ROC(tt) = " + str(ttmetrics.areaUnderROC))

    print("[Info Main FbbChurn] " + time.ctime() + " Area under ROC(tr) = " + str(trmetrics.areaUnderROC) + " - Area under ROC(tt) = " + str(ttmetrics.areaUnderROC))

    lift = get_lift(tt_calib_preds_df, 'calib_model_score', 'label', 40)

    for d, l in lift:
        print str(d) + ": " + str(l)

    print('Summary of Evaluations')
    print('ttmetrics', ttmetrics.areaUnderROC)
    print('trmetrics', trmetrics.areaUnderROC)

    #########################
    ### ENSEMBLER ALBERTO ###
    #########################

    bad = tr_preds_df.where(col('label') != col('prediction')).drop(col('features'))
    good = tr_preds_df.where(col('label') == col('prediction')).drop(col('features'))

    bad_b = balance_df2(bad, 'label')
    good_b = balance_df2(good, 'label')

    #full_concat = tr_preds_df.union(tt_preds_df)

    assembler2 = VectorAssembler(inputCols=featCols, outputCol="features")

    classifier2 = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=15, \
                                        maxBins=32, \
                                        minInstancesPerNode=200, \
                                        impurity="gini", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.7, \
                                        numTrees=800, \
                                        seed=1234)

    pipeline2 = Pipeline(stages=[assembler2, classifier2])

    model_good = pipeline2.fit(good_b)
    model_bad  = pipeline2.fit(bad_b)

    calibmodel_good = get_calibration_function2(spark, model_good, valdf, 'label', 10)
    calibmodel_bad = get_calibration_function2(spark, model_bad, valdf, 'label', 10)

    tr_good = model_good.transform(good_b).drop(col('features'))
    tr_bad = model_bad.transform(bad_b).drop(col('features'))

    tr_preds_good = tr_good.withColumn("model_score_good_calib", getScore(col("probability")).cast(DoubleType()))
    tr_calib_preds_good = calibmodel_good[0].transform(tr_preds_good)
    tr_preds_bad = tr_bad.withColumn("model_score_bad_calib", getScore(col("probability")).cast(DoubleType()))
    tr_calib_preds_bad = calibmodel_bad[0].transform(tr_preds_bad)

    final_df = tr_preds_good.select('msisdn', "model_score_good_calib").join(tr_preds_bad.select('msisdn', "model_score_bad_calib"), ['msisdn'], 'inner')

    final_feats = ["model_score_good_calib", "model_score_bad_calib"]

    assembler3 = VectorAssembler(inputCols=final_feats, outputCol="features")

    classifier3 = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=15, \
                                        maxBins=32, \
                                        minInstancesPerNode=200, \
                                        impurity="gini", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.7, \
                                        numTrees=800, \
                                        seed=1234)

    pipeline3 = Pipeline(stages=[assembler3, classifier3])

    [tr_final, tt_final] = final_df.randomsplit([0.7, 0.3], 1234)

    model_f = pipeline3.fit(tr_final)

    calibmodel_final = get_calibration_function2(spark, model_f, valdf, 'label', 10)

    final_tr_df_with_scores = model_f.transform(tr_final).withColumn("final_score", getScore(col("probability")).cast(DoubleType()))
    final_tr_df_with_calib_scores = calibmodel_final[0].transform(final_tr_df_with_scores)
    tr_PredictionAndLabels = final_tr_df_with_calib_scores.select(['final_score_calib', 'label']).rdd.map(lambda r: (r['final_score_calib'], r['label']))

    final_tt_df_with_scores = model_f.transform(tt_final).withColumn("final_score", getScore(col("probability")).cast(DoubleType()))
    final_tt_df_with_calib_scores = calibmodel_final[0].transform(final_tt_df_with_scores)
    tt_PredictionAndLabels = final_tt_df_with_calib_scores.select(['final_score_calib', 'label']).rdd.map(lambda r: (r['final_score_calib'], r['label']))

    tr_metrics_final = BinaryClassificationMetrics(tr_PredictionAndLabels)
    tt_metrics_final = BinaryClassificationMetrics(tt_PredictionAndLabels)

    print 'Final AUC (tr): {}'.format(tr_metrics_final.areaUnderROC)
    print 'Final AUC (tt): {}'.format(tt_metrics_final.areaUnderROC)

    #Calibrar los scores dándoles un nuevo nombre para que sean independientes DONE
    #Combinarlos en un nuevo df DONE
    #Entrenar el ultimo modelo
    #Mucho ojo a que nombres espera la funcion de calibracion



    print(" ")
    print("[Info Main FbbChurn] Process completed")
    print(" ")

    spark.stop()
