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
    from map_funct_fbb import getFeatGroups_fbb

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

    parser.add_argument('-s', '--training_day', metavar='<TRAINING_DAY>', type=str, required=False,
                        help='Training day YYYYMMDD. Date of the CAR taken to train the model.')
    parser.add_argument('-p', '--prediction_day', metavar='<PREDICTION_DAY>', type=str, required=False,
                        help='Prediction day YYYYMMDD.')
    parser.add_argument('-o', '--horizon', metavar='<horizon>', type=int, required=False,
                        help='Number of cycles used to gather the portability requests from the training day.')
    args = parser.parse_args()

    ########################
    ### 1. TRAINING DATA ###
    ########################

    # 1.1. Loading training data

    trdf = spark.read.load('/data/udf/vf_es/churn/fbb_datarobot/tr_df_full')
    
    allFeats = trdf.columns

    # Getting only the numeric variables
    catCols = [item[0] for item in trdf.dtypes if item[1].startswith('string')]
    numerical_feats = list(set(allFeats) - set(list(
        set().union(getIdFeats(), getIdFeats_tr(), getNoInputFeats(), catCols, [c + "_enc" for c in getCatFeatsCrm()],
                    ["label"]))))

    noninf_feats = getNonInfFeats(trdf, numerical_feats)

    #for f in noninf_feats:
    #    print "[Info Main FbbChurn] Non-informative feat: " + f

    ####################
    ### 2. TEST DATA ###
    ####################

    ttdf = spark.read.load('/data/udf/vf_es/churn/fbb_datarobot/tt_df_full')

        #tdf = ttdf.repartition(300)
        #ttdf.repartition(300).write.save(path, format='parquet', mode='overwrite')

    ####################
    ### 3. MODELLING ###
    ####################

    featCols = list(set(numerical_feats) - set(noninf_feats))
    
    assembler = VectorAssembler(inputCols=featCols, outputCol="features")
    
    classifier = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=20, \
                                        maxBins=32, \
                                        minInstancesPerNode=200, \
                                        impurity="gini", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.7, \
                                        numTrees=800, \
                                        minInfoGain = 0.01,\
                                        seed=1234)
    pipeline = Pipeline(stages=[assembler, classifier])
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    paramGrid = (ParamGridBuilder().addGrid(classifier.minInstancesPerNode, [100,200,300])\
             .addGrid(classifier.minInfoGain, [0.01, 0.005, 0.001])\
             .addGrid(classifier.numTrees, [800, 1000, 1200])\
             .addGrid(classifier.subsamplingRate, [0.85, 0.7, 0.6]).build())

    crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=BinaryClassificationEvaluator(), numFolds=5)
    
    cvModel = crossval.fit(trdf)
    
    predictions_tr = cvModel.transform(trdf)
    predictions_tt = cvModel.transform(ttdf)
    
    metrics = BinaryClassificationEvaluator(labelCol= 'label')
    
    scores_tr = metrics.evaluate(predictions_tr)
    scores_tt = metrics.evaluate(predictions_tt)
    
    print'AUC (Training data): {}'.format(scores_tr)
    print'AUC (Test data): {}'.format(scores_tt)
    
    model_f = cvModel.bestModel.stages[1]
    
    model_f.extractParamMap()
    
    print(model_f.extractParamMap())
    
    print('############ Finished process ############')
    
    spark.stop()
