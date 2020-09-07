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
    num_tr_dfs = 5
    ########################
    ### 1. TRAINING DATA ###
    ########################

    inittrdf_ini = getFbbChurnLabeledCarCycles(spark, origin, trcycle_ini, selcols, horizon)

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

    #print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf to HDFS " +str(inittrdf.count())
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
    [tr_df, tr_df_final] = unbaltrdf.randomSplit([0.8, 0.2], 1234)

    print "[Info Main FbbChurn] " + time.ctime() + " Stat description of the target variable printed above"

    tr_df = tr_df.cache()
    valdf = valdf.cache()
    print "[Info Main FbbChurn] " + time.ctime() + " Saving unbaltrdf to HDFS " + str(unbaltrdf.count())
    print "[Info Main FbbChurn] " + time.ctime() + " Saving valdf to HDFS " + str(valdf.count())

    # 1.2. Balanced df for training

    unbaltrdf.groupBy('label').agg(count('*')).show()

    print "[Info Main FbbChurn]" + time.ctime() + " Count on label column for unbalanced tr set showed above"
    tr_1s = tr_df.where(col('label') == 1)
    tr_0s = tr_df.where(col('label') == 0)
    num_1s = tr_1s.count()
    ratio = num_1s / tr_0s.count()
    arr = np.ones(num_tr_dfs)
    vec_selected_0s = tr_0s.randomSplit(arr)

    '''
    vector_train = []
    print(num_1s)
    for i in range(0, num_tr_dfs):
        value = vec_selected_0s[i].count()
        tr = tr_1s.union(vec_selected_0s[i])
        print(value)
        vector_train.append(tr)
    

    num_1s = tr_1s.count()
    ratio = 1.0 * float(num_1s) / tr_0s.count()
    arr = np.full((1, num_tr_dfs + 1), ratio)
    arr = np.append(arr, 1 - ratio * num_tr_dfs)
    vec_selected_0s = tr_0s.randomSplit(arr)
    '''

    vector_train = []
    print(num_1s)
    for i in range(0, num_tr_dfs):
        tr = tr_1s.union(vec_selected_0s[i].limit(num_1s))
        vector_train.append(tr)

    # 1.3. Feature selection

    allFeats = vector_train[0].columns

    # Getting only the numeric variables
    catCols = [item[0] for item in vector_train[0].dtypes if item[1].startswith('string')]
    numerical_feats = list(set(allFeats) - set(list(
        set().union(getIdFeats(), getIdFeats_tr(), getNoInputFeats(), catCols, [c + "_enc" for c in getCatFeatsCrm()],
                    ["label"]))))

    noninf_feats_vec = []
    for el in vector_train:
        noninf_feats = getNonInfFeats(el, numerical_feats)
        noninf_feats_vec.append(noninf_feats)

    ####################
    ### 2. TEST DATA ###
    ####################


    ttdf_ini = getFbbChurnLabeledCarCycles(spark, origin, ttcycle_ini, selcols,horizon)

    print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf_ini to HDFS "

    dfExtraFeat_tt = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                            .format(int(ttcycle_ini[0:4]), int(ttcycle_ini[4:6]), int(ttcycle_ini[6:8])))

    dfExtraFeatfbb_tt = dfExtraFeat_tt.join(ttdf_ini.select('num_cliente'), on='num_cliente', how='leftsemi')
    print(dfExtraFeatfbb_tt.select('num_cliente').distinct().count(), ttdf_ini.select('num_cliente').distinct().count())

    dfExtraFeatfbb_tt = dfExtraFeatfbb_tt.cache()
    print("[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats ", dfExtraFeatfbb_tt.count())

    dfExtraFeat_ttSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb_tt)

    print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client in tt"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeat_ttSel.columns:
             dfillNa.pop(kkey, None)

    ttdf = ttdf_ini.join(dfExtraFeat_ttSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
    print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the test set " + str(ttdf.count())
    print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf to HDFS "

    ####################
    ### 3. MODELLING ###
    ####################

    vector_feats = []
    for i in range(0, num_tr_dfs):
        featCols = list(set(numerical_feats) - set(noninf_feats_vec[i]))
        vector_feats.append(featCols)

    assembler_vec = []
    for i in range(0, num_tr_dfs):
        assembler = VectorAssembler(inputCols=vector_feats[i], outputCol="features")
        assembler_vec.append(assembler)

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

    pipeline_vec = []
    for i in range(0, num_tr_dfs):
        pipeline = Pipeline(stages=[assembler_vec[i], classifier])
        pipeline_vec.append(pipeline)


    models_vec = []

    for i in range(0, num_tr_dfs):
        model = pipeline_vec[i].fit(vector_train[i])
        models_vec.append(model)
        print'Trained model {}'.format(i)
    #feat_importance = getOrderedRelevantFeats(model, featCols, 'f', 'rf')


    #for fimp in feat_importance:
        #print "[Info Main FbbChurn] Imp feat " + str(fimp[0]) + ": " + str(fimp[1])

    ##################
    ### EVALUATION ###
    ##################
    # EVALUATION
    # Calibration
    calibration_vec = []
    for i in range(0, num_tr_dfs):
        calibmodel = get_calibration_function_vec(spark, models_vec[i], valdf, 'label', 10, i)
        calibration_vec.append(calibmodel)
        print'Calibrated model {}'.format(i)

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    # Train
    tr_calib_vec = []
    for i in range(0, num_tr_dfs):
        tr_preds_df = models_vec[i].transform(vector_train[i]).withColumn("model_score_" + str(i),getScore(col("probability")).cast(DoubleType()))
        tr_calib_preds_df = calibration_vec[i][0].transform(tr_preds_df)
        tr_calib_vec.append(tr_calib_preds_df)
        print'Predictions and calibration for training subset of model {}'.format(i)

    tr_metrics_vec = []
    # Train evaluation
    # tr_calib_vec = []
    for i in range(0, num_tr_dfs):  # 'calib_model_score_' + str(num)
        trPredictionAndLabels = tr_calib_vec[i].select(['calib_model_score_' + str(i), 'label']).rdd.map(
            lambda r: (r['calib_model_score_' + str(i)], r['label']))
        trmetrics = BinaryClassificationMetrics(trPredictionAndLabels)
        tr_metrics_vec.append(trmetrics)
        print'Binary metrics for training subset of model {}'.format(i)

    # Test eval
    # Test eval
    df_aux = ttdf
    for i in range(0, num_tr_dfs):
        df_aux = models_vec[i].transform(df_aux).withColumn("model_score_" + str(i),
                                                            getScore(col("probability")).cast(DoubleType())).drop(
            col('probability'))
        # tt_preds_df
        df_aux = calibration_vec[i][0].transform(
            df_aux.drop(col('features')).drop(col('prediction')).drop(col('rawPrediction')))
        print'Test predictions for model {}'.format(i)

    save_dir = 'tests_es.asaezco_fbb_improved_ens_' + args.prediction_day
    df_aux.write.format('parquet').mode('overwrite').saveAsTable(save_dir)

    for i in range(0, num_tr_dfs):
        ttPredictionAndLabels = df_aux.select(['calib_model_score_' + str(i), 'label']).rdd.map(
            lambda r: (r['calib_model_score_' + str(i)], r['label']))
        ttmetrics = BinaryClassificationMetrics(ttPredictionAndLabels)
        # print(ttmetrics.areaUnderROC)
        print(i)
        print "Model {}: Area under ROC(tr) = {}".format(i, str(tr_metrics_vec[i].areaUnderROC))
        print "Model {}: Area under ROC(tt) = {}".format(i, str(ttmetrics.areaUnderROC))

    cols_scores = [name_ for name_ in df_aux.columns if name_.startswith('calib_model_')]

    from pyspark.sql.functions import greatest, least, mean, sum

    df_aux2 = df_aux.withColumn('maximo', greatest(*cols_scores)).withColumn('minimo', least(*cols_scores))

    for name in ['maximo', 'minimo']:
        tt_ensemble = df_aux2.select([name, 'label']).rdd.map(lambda r: (r[name], r['label']))
        ttmetrics = BinaryClassificationMetrics(tt_ensemble)
        print "Model {}: Area under ROC(tt) = {}".format(i, str(ttmetrics.areaUnderROC))

    features_ = cols_scores
    assembler = VectorAssembler(inputCols=features_ + featCols, outputCol="features")

    classifier = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=10, \
                                        maxBins=32, \
                                        minInstancesPerNode=200, \
                                        impurity="gini", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.7, \
                                        numTrees=800, \
                                        seed=1234, )

    pipeline = Pipeline(stages=[assembler, classifier])


    tr = balance_df2(tr_df_final, 'label')

    df_aux_tr = tr
    df_aux_val = valdf
    
    for i in range(0, num_tr_dfs):
        df_aux_tr = models_vec[i].transform(df_aux_tr).withColumn("model_score_" + str(i),
                                                              getScore(col("probability")).cast(DoubleType())).drop(col('probability'))
        
        df_aux_val = models_vec[i].transform(df_aux_val).withColumn("model_score_" + str(i),
                                                              getScore(col("probability")).cast(DoubleType())).drop(col('probability'))

        df_aux_tr = calibration_vec[i][0].transform(
            df_aux_tr.drop(col('features')).drop(col('prediction')).drop(col('rawPrediction')))
        df_aux_val = calibration_vec[i][0].transform(
            df_aux_val.drop(col('features')).drop(col('prediction')).drop(col('rawPrediction')))
        print(i)

    model_f = pipeline.fit(df_aux_tr)

    ensemble_ = model_f.transform(df_aux).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    
    calib_f = get_calibration_function2(spark, model_f, df_aux_val, 'label',  10)
    
    ensemble = calib_f[0].transform(ensemble_)
    
    ttPredictionAndLabels = ensemble.select(['calib_model_score', 'label']).rdd.map(
        lambda r: (r['calib_model_score'], r['label']))
    ttmetrics = BinaryClassificationMetrics(ttPredictionAndLabels)
    print'Ensembler AUC: {}'.format(ttmetrics.areaUnderROC)

    lift_vec = []
    for i in range(0, num_tr_dfs):
        lift = get_lift(df_aux, 'calib_model_score_' + str(i), 'label', 40)
        lift_vec.append(lift)

    for lift in lift_vec:
        for d, l in lift:
            print str(d) + ": " + str(l)



    print(" ")
    print("[Info Main FbbChurn] Process completed")
    print(" ")

    spark.stop()
