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

def getFbbChurnLabeledCarCycles_both(spark, origin, yearmonthday, selcols, horizon = 4):

    cycle = 0
    fini_tmp = yearmonthday
    while cycle < horizon:
        yearmonthday_target = get_next_cycle(fini_tmp, str_fmt="%Y%m%d")
        cycle = cycle + 1
        fini_tmp = yearmonthday_target

    yearmonth = yearmonthday[0:6]

    trfeatdf = getCarNumClienteDf(spark, origin, yearmonthday)

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Samples for month " + yearmonthday + ": " + str(trfeatdf.count()))

    # Loading port-out requests and DXs
    # # labmonthlisttr = getMonthSeq(initportmonthtr, lastportmonthtr)

    # Las bajas de fibra pueden venir por:
    #- Solicitudes de baja de fijo
    fixporttr = getFixPortRequestsForCycleList(spark, yearmonthday, yearmonthday_target)
    #- Porque dejen de estar en la lista de clientes
    fixdxtr = getFbbDxsForCycleList(spark,yearmonthday, yearmonthday_target)

    # Labeling: FBB service is labeled as 1 if, during the next time window specified by the horizon, either the associated fixed service requested to be ported out or the FBB was disconnected
    window = Window.partitionBy("num_cliente")

    unbaltrdf = trfeatdf\
    .join(fixporttr, ['msisdn_d'], "left_outer")\
    .na.fill({'label_srv': 0.0})\
    .join(fixdxtr, ['msisdn'], "left_outer")\
    .na.fill({'label_dx': 0.0})\
    .withColumn('tmp', when((col('label_dx')==1.0), 1.0).otherwise(0.0))\
    .withColumn('label_bajas', sql_max('tmp').over(window))\
    .withColumn('tmp2', when((col('label_srv')==1.0), 1.0).otherwise(0.0))\
    .withColumn('label_port', sql_max('tmp2').over(window))\
    .filter(col("rgu")=="fbb")\
    .select(selcols + ['label_port', 'label_bajas'])

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Labeled samples for month " + yearmonth + ": " + str(unbaltrdf.count()))

    return unbaltrdf

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
        epilog='Please report bugs and issues to Álvaro <alvaro.saez@vodafone.com>')

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

    #path = '/data/udf/vf_es/churn/fbb_tmp/inittrdf_ini_' + tr_ttdates

    inittrdf_ini = getFbbChurnLabeledCarCycles_both(spark, origin, trcycle_ini, selcols, horizon)

    #print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf_ini to HDFS"

    #path = '/data/udf/vf_es/churn/fbb_tmp/inittrdf_' + tr_ttdates
    
    print'Dxs in training df: {}'.format(inittrdf_ini.where(col('label_bajas') > 0).count())
    print'Ports in training df: {}'.format(inittrdf_ini.where(col('label_port') > 0).count())

   ## Reading the Extra Features
    dfExtraFeat = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                             .format(int(trcycle_ini[0:4]), int(trcycle_ini[4:6]), int(trcycle_ini[6:8])))

        # Taking only the clients with a fbb service
    dfExtraFeatfbb = dfExtraFeat.join(inittrdf_ini, ["num_cliente"], "leftsemi")

    dfExtraFeatfbb = dfExtraFeatfbb.cache()
    #print "[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats: ", dfExtraFeatfbb.count()

        # Taking the Extra Features of interest and adding their values for num_client when necessary
    dfExtraFeatSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb)

    #print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeatSel.columns:
            dfillNa.pop(kkey, None)

    inittrdf = inittrdf_ini.join(dfExtraFeatSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
    
    #print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf to HDFS " +str(inittrdf.count())
    #path_p = '/data/udf/vf_es/churn/fbb_tmp/inittrdf_20181130_p'
    #inittrdf_port.repartition(200).write.save(path_p, format='parquet', mode='overwrite')

    [unbaltrdf_, valdf] = inittrdf.randomSplit([0.8, 0.2], 1234)
    [unbaltrdf, ensembdf] = unbaltrdf_.randomSplit([0.8, 0.2], 1234)
    
    
    unbaltrdf = unbaltrdf.cache()
    valdf = valdf.cache()
    
    unbaltrdf.groupBy('label_bajas').agg(count('*')).show()
    unbaltrdf.groupBy('label_port').agg(count('*')).show()

    trdf_port = balance_df2(unbaltrdf, 'label_port')
    trdf_dx = balance_df2(unbaltrdf, 'label_bajas')
    
    trdf_dx.groupBy('label_bajas').agg(count('*')).show()
    trdf_port.groupBy('label_port').agg(count('*')).show()
    
    allFeats = trdf_dx.columns

    # Getting only the numeric variables
    catCols = [item[0] for item in trdf_dx.dtypes if item[1].startswith('string')]
    numerical_feats = list(set(allFeats) - set(list(
            set().union(getIdFeats(), getIdFeats_tr(), getNoInputFeats(), catCols, [c + "_enc" for c in getCatFeatsCrm()],
                        ["label"]))))

    noninf_feats = getNonInfFeats(trdf_dx, numerical_feats)    

    #unbaltrdf.repartition(300).write.save(path1,format='parquet', mode='overwrite')
    #valdf.repartition(300).write.save(path2,format='parquet', mode='overwrite')

    # 1.2. Balanced df for training

    ####################
    ### 2. TEST DATA ###
    ####################
    
    ttdf_ini = getFbbChurnLabeledCarCycles_both(spark, origin, ttcycle_ini, selcols,horizon)
    
    #print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf_ini to HDFS "
    #ttdf_ini.repartition(200).write.save(path,format='parquet', mode='overwrite')

    #ttdf_ini.describe('label').show()

    #path = "/data/udf/vf_es/churn/fbb_tmp/ttdf_" + tr_ttdates

    dfExtraFeat_tt = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                            .format(int(ttcycle_ini[0:4]), int(ttcycle_ini[4:6]), int(ttcycle_ini[6:8])))
    dfExtraFeatfbb_tt = dfExtraFeat_tt.join(ttdf_ini.select('num_cliente'), on='num_cliente', how='leftsemi')
    #print(dfExtraFeatfbb_tt.select('num_cliente').distinct().count(), ttdf_ini.select('num_cliente').distinct().count())

    dfExtraFeatfbb_tt = dfExtraFeatfbb_tt.cache()
    #print("[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats ", dfExtraFeatfbb_tt.count())
    
    dfExtraFeat_ttSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb_tt)
    
    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeat_ttSel.columns:
            dfillNa.pop(kkey, None)

    ttdf = ttdf_ini.join(dfExtraFeat_ttSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)

    ####################
    ### 3. MODELLING ###
    ####################

    featCols = list(set(numerical_feats) - set(noninf_feats + ['label_bajas','label_port']))

    for f in featCols:
        print "[Info Main FbbChurn] Input feat: " + f

    assembler = VectorAssembler(inputCols=featCols, outputCol="features")

    classifier_port = RandomForestClassifier(featuresCol="features", \
                                            labelCol="label_port", \
                                            maxDepth=20, \
                                            maxBins=32, \
                                            minInstancesPerNode=100, \
                                            impurity="entropy", \
                                            featureSubsetStrategy="sqrt", \
                                            subsamplingRate=0.85, minInfoGain = 0.001, \
                                            numTrees=800, \
                                            seed=1234)  
    classifier_dx = RandomForestClassifier(featuresCol="features", \
                                            labelCol="label_bajas", \
                                            maxDepth=18, \
                                            maxBins=32, \
                                            minInstancesPerNode=90, \
                                            impurity="entropy", \
                                            featureSubsetStrategy="sqrt", \
                                            subsamplingRate=0.85, minInfoGain = 0.001, \
                                            numTrees=800, \
                                            seed=1234)     

    pipeline_port = Pipeline(stages=[assembler, classifier_port])
    pipeline_dx = Pipeline(stages=[assembler, classifier_dx])

    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    
    model_dx = pipeline_dx.fit(trdf_dx)
    calibmodel_dx = getCalibrationFunction(spark, model_dx, valdf, 'label_bajas', 10)
    
    # Calibration
    model_port = pipeline_dx.fit(trdf_port)
    calibmodel_port = getCalibrationFunction(spark, model_port, valdf, 'label_port', 10)
    
    feat_importance_port = getOrderedRelevantFeats(model_port, featCols, 'f', 'rf')
    feat_importance_dx = getOrderedRelevantFeats(model_dx, featCols, 'f', 'rf')
    
    port_imp, imp = zip(*feat_importance_port)
    dx_imp, imp_dx = zip(*feat_importance_dx)
    n = 200
    list_f = port_imp[:n] + dx_imp[:n]
    featCols_ensemb =  list(dict.fromkeys(list_f)) + ['calib_model_score_portas', 'calib_model_score_bajas']

    ##################
    ### EVALUATION ###
    ##################

    # Train
    tr_preds_df_dx = model_dx.transform(trdf_dx).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tr_calib_preds_df_dx = calibmodel_dx[0].transform(tr_preds_df_dx)
    
    trPredictionAndLabels_dx = tr_calib_preds_df_dx.select(['calib_model_score', 'label_bajas']).rdd.map(lambda r: (r['calib_model_score'], r['label_bajas']))
    trmetrics_dx = BinaryClassificationMetrics(trPredictionAndLabels_dx)    

    tt_preds_df_dx = model_dx.transform(ttdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tt_calib_preds_df_dx = calibmodel_dx[0].transform(tt_preds_df_dx)
    
    ttPredictionAndLabels_dx = tt_calib_preds_df_dx.select(['calib_model_score', 'label_bajas']).rdd.map(lambda r: (r['calib_model_score'], r['label_bajas']))
    ttmetrics_dx = BinaryClassificationMetrics(ttPredictionAndLabels_dx)
    
    print('Bajas:')
    print(" Area under ROC(tr) = " + str(trmetrics_dx.areaUnderROC))
    print(" Area under ROC(tt) = " + str(ttmetrics_dx.areaUnderROC))    
    print(" ")

    # Test eval
    tr_preds_df_port = model_port.transform(trdf_port).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tr_calib_preds_df_port = calibmodel_port[0].transform(tr_preds_df_port)

    trPredictionAndLabels_port = tr_calib_preds_df_port.select(['calib_model_score', 'label_port']).rdd.map(lambda r: (r['calib_model_score'], r['label_port']))
    trmetrics_port = BinaryClassificationMetrics(trPredictionAndLabels_port)  

    tt_preds_df_port = model_port.transform(ttdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tt_calib_preds_df_port = calibmodel_port[0].transform(tt_preds_df_port)
    
    ttPredictionAndLabels_port = tt_calib_preds_df_port.select(['calib_model_score', 'label_port']).rdd.map(lambda r: (r['calib_model_score'], r['label_port']))
    ttmetrics_port = BinaryClassificationMetrics(ttPredictionAndLabels_port)
    
    
    print('Portas:')
    print(" Area under ROC(tr) = " + str(trmetrics_port.areaUnderROC))
    print(" Area under ROC(tt) = " + str(ttmetrics_port.areaUnderROC))    
    print(" ")
    
    tt_calib_preds_df_port_ = tt_calib_preds_df_port.withColumnRenamed('calib_model_score', 'calib_model_score_portas')
    tt_calib_preds_df_dx_ = tt_calib_preds_df_dx.withColumnRenamed('calib_model_score', 'calib_model_score_bajas')
    
    joined = tt_calib_preds_df_port_.select('num_cliente', 'label_port','calib_model_score_portas').join(tt_calib_preds_df_dx_.select('num_cliente', 'label_bajas', 'calib_model_score_bajas'), ['num_cliente'], 'inner')
    
    from pyspark.sql.functions import greatest, least
    joined = joined.withColumn('label', greatest('label_port','label_bajas'))
    
    ensembled = joined.withColumn('max', greatest('calib_model_score_portas','calib_model_score_bajas')).withColumn('min', least('calib_model_score_portas','calib_model_score_bajas'))\
.withColumn('mean', (col('calib_model_score_portas')+col('calib_model_score_bajas'))/2)
    
    pred_max = ensembled.select(['max', 'label']).rdd.map(lambda r: (r['max'], r['label']))
    pred_max_metrics = BinaryClassificationMetrics(pred_max)
    pred_min = ensembled.select(['min', 'label']).rdd.map(lambda r: (r['min'], r['label']))
    pred_min_metrics = BinaryClassificationMetrics(pred_min)
    pred_mean = ensembled.select(['mean', 'label']).rdd.map(lambda r: (r['mean'], r['label']))
    pred_mean_metrics = BinaryClassificationMetrics(pred_mean)
    
    print(" Area under ROC(tt) max = " + str(pred_max_metrics.areaUnderROC)) 
    print(" Area under ROC(tt) min = " + str(pred_min_metrics.areaUnderROC))
    print(" Area under ROC(tt) mean = " + str(pred_mean_metrics.areaUnderROC)) 
    
    
    assembler_ensemb = VectorAssembler(inputCols=featCols_ensemb, outputCol="features")
    classifier_ensemb = RandomForestClassifier(featuresCol="features", \
                                            labelCol="label", \
                                            maxDepth=20, \
                                            maxBins=32, \
                                            minInstancesPerNode=90, \
                                            impurity="entropy", \
                                            featureSubsetStrategy="sqrt", \
                                            subsamplingRate=0.85, minInfoGain = 0.001, \
                                            numTrees=800, \
                                            seed=1234)    
    pipeline_ensemb = Pipeline(stages=[assembler_ensemb, classifier_ensemb])
    
    ensembdf = ensembdf.withColumn('label', greatest('label_port','label_bajas'))
    
    model_ensemb = model_dx.transform(ensembdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))

    model_ensemb_calib_bajas = calibmodel_dx[0].transform(model_ensemb).withColumnRenamed('calib_model_score','calib_model_score_bajas')
    model_ensemb = model_port.transform(model_ensemb_calib_bajas.drop('features').drop(col('probability')).drop(col('prediction')).drop(col('rawPrediction')).drop(col('model_score'))).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    model_ensemb_calib = calibmodel_port[0].transform(model_ensemb).withColumnRenamed('calib_model_score', 'calib_model_score_portas')
    
    model_ensemb_fit = pipeline_ensemb.fit(model_ensemb_calib.drop('features').drop(col('probability')).drop(col('prediction')).drop(col('rawPrediction')).drop(col('model_score')))
    
    ensemb_preds_tr = model_ensemb_fit.transform(model_ensemb_calib.drop('features').drop(col('probability')).drop(col('prediction')).drop(col('rawPrediction')).drop(col('model_score')))\
.withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    
    ensembler_PredAndLabs = ensemb_preds_tr.select(['model_score', 'label']).rdd.map(lambda r: (r['model_score'], r['label']))
    trmetrics_ensembled = BinaryClassificationMetrics(ensembler_PredAndLabs)
    
    print(" Area under ROC(tr-ensemb) = " + str(trmetrics_ensembled.areaUnderROC)) 
    
    ensembler_df = tt_calib_preds_df_port_.select('num_cliente','calib_model_score_portas').join(tt_calib_preds_df_dx_.drop('features').drop(col('rawPrediction')).drop(col('probability')), ['num_cliente'], 'inner')
    
    ensembler_df = ensembler_df.withColumn('label', greatest('label_port','label_bajas'))
    
    ensemb_preds_tt = model_ensemb_fit.transform(ensembler_df.drop('features').drop(col('probability')).drop(col('prediction')).drop(col('rawPrediction')).drop(col('model_score')))\
.withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    
    ensembler_PredAndLabs_tt = ensemb_preds_tt.select(['model_score', 'label']).rdd.map(lambda r: (r['model_score'], r['label']))
    ttmetrics_ensembled = BinaryClassificationMetrics(ensembler_PredAndLabs_tt)
    
    print(" Area under ROC(tt-ensemb) = " + str(ttmetrics_ensembled.areaUnderROC)) 
    
    spark.stop()