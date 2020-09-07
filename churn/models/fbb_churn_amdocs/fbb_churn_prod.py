# coding: utf-8

import sys

#from common.src.main.python.utils.hdfs_generic import *
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
                                    row_number,
                                    skewness,
                                    kurtosis,
                                    concat_ws)

import re

from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, LongType,FloatType
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
# from general_model_trainer import GeneralModelTrainer

def set_paths():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and "dirs/dir1/"
    :return:
    '''
    import re
    from os.path import dirname
    import os

    churnFolder = [ii for ii in sys.path if 'churn' in ii]
    USE_CASES = dirname(re.match("(.*)use-cases/churn", churnFolder[0]).group())

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
    # HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    # if HOME_SRC not in sys.path:
    #     sys.path.append(HOME_SRC)
    #
    #
    # setting_bdp(app_name=app_name, min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores, executor_memory = executor_memory, driver_memory=driver_memory)
    # from common.src.main.python.utils.hdfs_generic import run_sc
    # sc, spark, sql_context = run_sc(log_level=log_level)
    from pyspark.sql import SparkSession, SQLContext, DataFrame
    spark = (SparkSession.builder.appName("churn_delivery_fbb")
             .master("yarn")
             .config("spark.submit.deployMode", "client")
             .config("spark.ui.showConsoleProgress", "true").enableHiveSupport().getOrCreate())

    sc = spark.sparkContext
    return sc, spark, None


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

def check_car_preparado_ready(closing_day):
    from pyspark.sql.utils import AnalysisException
    car_preparado_path = "tests_es.jvmm_amdocs_prepared_car_mobile_complete_" + closing_day
    try:
        spark.read.table(car_preparado_path)
        return True
    except AnalysisException as e:
        return False


if __name__ == "__main__":

    start_time = time.time()

    set_paths()

    ## ARGUMENTS
    ###############
    parser = argparse.ArgumentParser(
        description='Generate score table for fbb model',
        epilog='Please report bugs and issues to Beatriz <beatriz.gonzalez2@vodafone.com>')

    parser.add_argument('-s', '--training_day', metavar='<TRAINING_DAY>', type=str, required=False,
                        help='Training day YYYYMMDD. Date of the CAR taken to train the model.')
    parser.add_argument('-p', '--prediction_day', metavar='<PREDICTION_DAY>', type=str, required=True,
                        help='Prediction day YYYYMMDD.')
    parser.add_argument('-o', '--horizon', metavar='<horizon>', type=int, required=False, default=8,
                        help='Number of cycles used to gather the portability requests from the training day.')
    args = parser.parse_args()

    print(args)

    # Cycle used for CAR and Extra Feats in the training set
    trcycle_ini = args.training_day  # Training data
    # Number of cycles to gather dismiss requests
    horizon = args.horizon
    # Cycle used for CAR and Extra Feats in the test set
    ttcycle_ini = args.prediction_day  # Test data

    if not trcycle_ini:
        print("Computing trcycle_ini")
        from pykhaos.utils.date_functions import move_date_n_cycles
        trcycle_ini = move_date_n_cycles(ttcycle_ini, n=-horizon)

    print("Running script with arguments: training_day={} prediction_day={} horizon={}".format(trcycle_ini, ttcycle_ini, horizon))

    tr_ttdates = trcycle_ini + '_' + ttcycle_ini
    nowMoment=datetime.now()
    pred_name='prediction_fbb_tr'+trcycle_ini+'to'+trcycle_ini+'_tt'+ttcycle_ini+'_horizon'+str(horizon)+'_on'+nowMoment.strftime("%Y%m%d_%H%M%S")


    from pykhaos.utils.date_functions import *
    from utils_fbb_churn import *

    # Create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'

    global sqlContext

    spark = initialize("VF_ES AMDOCS FBB Churn Prediction ", executor_memory="16g", min_n_executors=10)
    print('Spark Configuration used', spark.sparkContext.getConf().getAll())

    selcols = getIdFeats() + getCrmFeats() + getBillingFeats() + getMobSopoFeats() + getOrdersFeats()

    date_name = str(nowMoment.year) + str(nowMoment.month).rjust(2, '0') + str(nowMoment.day).rjust(2, '0')
    origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'


    ########################
    ### 1. TRAINING DATA ###
    ########################

    # 1.1. Loading training data

    path = '/data/udf/vf_es/churn/fbb_tmp/inittrdf_ini_' + tr_ttdates
    if pathExist(path):

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        inittrdf_ini = spark.read.parquet(path)

    else:

        inittrdf_ini = getFbbChurnLabeledCarCycles(spark, origin, trcycle_ini, selcols, horizon)
        inittrdf_ini.repartition(200).write.save(path, format='parquet', mode='overwrite')

        print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf_ini to HDFS"


    path = '/data/udf/vf_es/churn/fbb_tmp/inittrdf_' + tr_ttdates
    if pathExist(path):

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        inittrdf = spark.read.parquet(path)

    else:

        ## Reading the Extra Features
        dfExtraFeat = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                         .format(int(trcycle_ini[0:4]), int(trcycle_ini[4:6]), int(trcycle_ini[6:8])))

        # Take only the clients with a fbb service
        dfExtraFeatfbb = dfExtraFeat.join(inittrdf_ini, ["num_cliente"], "leftsemi") # It takes the columns of the left table and the matching records

        dfExtraFeatfbb = dfExtraFeatfbb.cache()
        print "[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats: ", dfExtraFeatfbb.count()

        # Taking the Extra Features of interest and adding their values for num_client when necessary
        dfExtraFeatSel, selColumnas = addExtraFeats(dfExtraFeatfbb)

        print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client"

        #Fill the Nan existing in the dataframe
        dfillNa = fillNa(spark)
        for kkey in dfillNa.keys():
            if kkey not in dfExtraFeatSel.columns:
                dfillNa.pop(kkey, None)

        inittrdf = inittrdf_ini.join(dfExtraFeatSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)

        print "[Info Main FbbChurn] " + time.ctime() + " Saving inittrdf to HDFS " + str(inittrdf.count())
        inittrdf.repartition(200).write.save(path, format='parquet', mode='overwrite')

    # Split the dataframe into two groups one for validation and one for trainning
    path1 = '/data/udf/vf_es/churn/fbb_tmp/unbaltrdf_' + tr_ttdates
    path2 = '/data/udf/vf_es/churn/fbb_tmp/caldf_' + tr_ttdates
    path3 = '/data/udf/vf_es/churn/fbb_tmp/valdf_' + tr_ttdates

    if (pathExist(path1)) and (pathExist(path2)) and (pathExist(path3)) :

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path1) + " and " + str(path2) + " and " + str(path3) +  " already exist. Reading them."

        unbaltrdf = spark.read.parquet(path1)
        caldf = spark.read.parquet(path2)
        valdf = spark.read.parquet(path3)

    else:

        print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the training set" + str(inittrdf.count())

        [unbaltrdf, caldf, valdf] = inittrdf.randomSplit([0.50,0.25,0.25], 1234)

        print "[Info Main FbbChurn] " + time.ctime() + " Stat description of the target variable printed above"

        unbaltrdf = unbaltrdf.cache()
        caldf = caldf.cache()
        valdf = valdf.cache()
        print "[Info Main FbbChurn] " + time.ctime() + " Saving unbaltrdf to HDFS " + str(unbaltrdf.count())
        print "[Info Main FbbChurn] " + time.ctime() + " Saving caldf to HDFS " + str(caldf.count())
        print "[Info Main FbbChurn] " + time.ctime() + " Saving valdf to HDFS " + str(valdf.count())

        unbaltrdf.repartition(300).write.save(path1, format='parquet', mode='overwrite')
        caldf.repartition(300).write.save(path2, format='parquet', mode='overwrite')
        valdf.repartition(300).write.save(path3, format='parquet', mode='overwrite')


    # 1.2. Balanced df for training

    path = "/data/udf/vf_es/churn/fbb_tmp/trdf_" + tr_ttdates
    if (pathExist(path)):

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        trdf = spark.read.parquet(path)

    else:

        unbaltrdf.groupBy('label').agg(count('*')).show()

        print "[Info Main FbbChurn]" + time.ctime() + " Count on label column for unbalanced tr set showed above"
        trdf = balance_df2(unbaltrdf, 'label')

        trdf.groupBy('label').agg(count('*')).show()
        print "[Info Main FbbChurn] " + time.ctime() + " Saving trdf to HDFS "

        trdf.repartition(300).write.save(path, format='parquet', mode='overwrite')

    trdf.repartition(10).write.save('/data/udf/vf_es/churn/models/' + pred_name + '/training_set', format='parquet',
                                    mode='overwrite')

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

    path = "/data/udf/vf_es/churn/fbb_tmp/ttdf_ini_" + tr_ttdates
    if (pathExist(path)):

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        ttdf_ini = spark.read.parquet(path)

    else:

        ttdf_ini = getFbbChurnUnlabeledCar(spark, origin, ttcycle_ini, selcols)

        print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf_ini to HDFS "
        ttdf_ini.repartition(200).write.save(path, format='parquet', mode='overwrite')

    path = "/data/udf/vf_es/churn/fbb_tmp/ttdf_" + tr_ttdates
    if (pathExist(path)):

        print "[Info Main FbbChurn] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        ttdf = spark.read.parquet(path)

    else:

        dfExtraFeat_tt = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                            .format(int(ttcycle_ini[0:4]), int(ttcycle_ini[4:6]), int(ttcycle_ini[6:8])))

        dfExtraFeatfbb_tt = dfExtraFeat_tt.join(ttdf_ini, ["num_cliente"], "leftsemi")

        dfExtraFeatfbb_tt = dfExtraFeatfbb_tt.cache()
        print("[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats ", dfExtraFeatfbb_tt.count())

        dfExtraFeat_ttSel, selColumnas = addExtraFeats(dfExtraFeatfbb_tt)

        print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client in tt"

        dfillNa = fillNa(spark)
        for kkey in dfillNa.keys():
            if kkey not in dfExtraFeat_ttSel.columns:
                dfillNa.pop(kkey, None)

        ttdf = ttdf_ini.join(dfExtraFeat_ttSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
        print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the test set " + str(ttdf.count())
        print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf to HDFS "

        ttdf = ttdf.repartition(300)
        ttdf.repartition(300).write.save(path, format='parquet', mode='overwrite')

    ####################
    ### 3. MODELLING ###
    ####################

    #unpersist()  # Clear all the cache variables

    featCols = list(set(numerical_feats) - set(noninf_feats))

    for f in featCols:
        print "[Info Main FbbChurn] Input feat: " + f

    assembler = VectorAssembler(inputCols = featCols, outputCol = "features")

    classifier = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=20, \
                                        maxBins=32, \
                                        minInstancesPerNode=100, \
                                        impurity="entropy", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.85, minInfoGain=0.001, \
                                        numTrees=1200, \
                                        seed=1234)
    
    pipeline = Pipeline(stages= [assembler, classifier])

    model = pipeline.fit(trdf)

    feat_importance = getOrderedRelevantFeats(model, featCols, 'f', 'rf')

    featPandas = pd.DataFrame(feat_importance, columns=['features', 'importance'])
    cSchema = StructType([StructField("feature", StringType()),
                          StructField("importance", FloatType())])
    featPyspark = spark.createDataFrame(featPandas, schema=cSchema)
    featPyspark.repartition(10).write.save('/data/udf/vf_es/churn/models/' + pred_name + '/feat_importance',
                                           format='parquet', mode='overwrite')


    for fimp in feat_importance:
        print "[Info Main FbbChurn] Imp feat " + str(fimp[0]) + ": " +  str(fimp[1])

    ##################
    ### EVALUATION ###
    ##################

    # Calibration
    calibmodel = get_calibration_function2(spark, model, caldf, 'label', 10)

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

    # Test evaluation (Using the calibration set from the training data)
    tt_caldf = model.transform(caldf).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    tt_calib_caldf = calibmodel[0].transform(tt_caldf)

    ttcalPredictionAndLabels = tt_calib_caldf.select(['calib_model_score', 'label']).rdd.map(lambda r: (r['calib_model_score'], r['label']))
    ttmetrics = BinaryClassificationMetrics(ttcalPredictionAndLabels)


    print("[Info Main FbbChurn] " + time.ctime() + " Area under ROC(tr) = " + str(trmetrics.areaUnderROC))

    # We calculate an average model score for each client number (a client can have several fbb)
    tmpdf = tt_calib_preds_df.select(['num_cliente','calib_model_score']).groupBy('num_cliente').agg(sql_avg('calib_model_score').alias('model_score'))

    print "[Info Main FbbChurn] Output table prepared - Number of records: " + str(tmpdf.count())

    tmpdf.write.mode('overwrite').save('/data/udf/vf_es/churn/fbb_tmp/tmpdf_' + tr_ttdates, format='parquet', mode='append')

    #Adding the nif_cliente to the dataframe with the model scores
    tmpdf = tmpdf.join(tt_calib_preds_df.select(['num_cliente', 'nif_cliente']).distinct(), on=['num_cliente'], how='left')
    tmpdf = tmpdf.orderBy('model_score', ascending=False)

    # Identify the clients with risk of leaving (top 35% of the model scores) and divude them in 10 deciles depending on their risk level,
    # from Very high to Low
    tmpdfInd = add_column_index(spark, tmpdf, offset=1, colName="idx")
    print "[Info Main FbbChurn] Calculating the deciles of the predictions "
    df_scores = add_decile(tmpdfInd)

    ## Getting the deanonymized information
    clientCARdf = spark \
        .read \
        .parquet(origin + ttcycle_ini) \
        .filter(col("ClosingDay") == ttcycle_ini) \
        .repartition(400) \
        .cache()

    df_scores_d = df_scores.join(clientCARdf.select(['num_cliente', 'CAMPO1']).distinct(), on='num_cliente', how='inner')
    dfScores = df_scores_d.drop('num_cliente').withColumnRenamed("campo1", "num_cliente")
    dfScores = dfScores.na.drop()

    # Check the number of clients included in the final generated table
    from pyspark.sql.functions import col, count, isnan, lit, sum

    def count_not_null(c, nan_as_null=False):
        """Use conversion between boolean and integer
        - False -> 0
        - True ->  1
        """
        pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
        return sum(pred.cast("integer")).alias(c)

    dfScores.agg(*[count_not_null(c) for c in dfScores.columns]).show()

    #################
    ## Save results
    #################

    print "[Info Main FbbChurn] Saving results"

    # By num_cliente
    # Bring the results to the 387 node
    fecha = 'tr' + str(trcycle_ini) + '_h' + str(horizon) + '_tt' + str(ttcycle_ini)+''
    nowFecha=nowMoment.strftime("%Y%m%d")

    try:
        savingFolder = '/var/SP/data/bdpmdses/deliveries_churn/delivery_lists/fbb'
        dfScores.repartition(1).write.mode('overwrite').csv('/data/udf/vf_es/churn/fbb_model/' + fecha + '/', header=True,
                                                            sep='|')
        subprocess.call(["hadoop", "fs", "-copyToLocal", '/data/udf/vf_es/churn/fbb_model/' + fecha, savingFolder])
        for filename in os.listdir(savingFolder + '/' + fecha):
            os.rename(savingFolder + '/' + fecha + '/' + filename,
                      savingFolder + '/' + fecha + '/pred_fbb_' + fecha + '_' + nowFecha + '.csv')

        user = os.getenv("USER")
        execute_command_in_local = 'scp ' + user + '@milan-discovery-edge-387:' + savingFolder + '/' + fecha + '/pred_fbb_' + nowFecha + '.csv <local_directoy>'

        os.system('chmod +777 ' + savingFolder + '/' + fecha + '/pred_fbb_' + fecha + '_' + nowFecha + '.csv')
    except:
        print("[fbb_churn_prod] Error while trying to save the scores list locally...")

    elapsed_time = time.time() - start_time
    #    In the model output table (anonimize data)
    saveResults(tr_calib_preds_df, df_scores, trmetrics, ttmetrics, feat_importance,trcycle_ini,horizon,ttcycle_ini,spark,elapsed_time,nowMoment)

    print('******************************')
    print ("\n To download the file in your local computer, type from your local: \n {}".format(execute_command_in_local))
    print('******************************')

    print "[Info AmdocsChurnModel Predictor] Output table saved"

    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/inittrdf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/inittrdf_ini_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/trdf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/ttdf_ini_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/ttdf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/unbaltrdf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/valdf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/caldf_' + tr_ttdates])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/origindf_tmp_' + trcycle_ini])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/numclidf_tmp_' + trcycle_ini])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/origindf_tmp_' + ttcycle_ini])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/numclidf_tmp_' + ttcycle_ini])
    subprocess.call(["hadoop", "fs", "-rm", "-r", '/data/udf/vf_es/churn/fbb_tmp/tmpdf_'+ tr_ttdates])

    print(" ")
    print("[Info Main FbbChurn] Process completed!!")
    print(" ")




