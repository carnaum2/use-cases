# coding=utf-8

# Program to compare the churn model predictions with the portability applications:

# Input arguments:
# 1.- Initial day of the comparisson period
# 2.- Final day of the comparisson period

# Output
# - CSV with the values
# - PDF with graphs

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, substring, asc, udf, datediff, row_number, concat, when, desc,unix_timestamp, from_unixtime, date_format
from pyspark.sql.types import StringType,FloatType,DoubleType,IntegerType,StructType,StructField,LongType

from pyspark.mllib.evaluation import MulticlassMetrics

import os, sys, glob
from dateutil.relativedelta import relativedelta
from pyspark.ml.feature import QuantileDiscretizer

from pyspark.sql.functions import floor
# from datetime import datetime
from shutil import copyfile
import pandas as pd
import datetime
from datetime import timedelta

import re
import logging
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)

import matplotlib
matplotlib.use('Agg')  # Es necesario para evitar el error: TclError: no display name and no $DISPLAY environment variable
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import matplotlib.colors as colors

import time
from io import BytesIO

def set_paths():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and "dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os
    import sys

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
    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)


    setting_bdp(app_name=app_name, min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores, executor_memory = executor_memory, driver_memory=driver_memory)
    from common.src.main.python.utils.hdfs_generic import run_sc
    sc, spark, sql_context = run_sc(log_level=log_level)


    return sc, spark, sql_context


# set BDP parameters
def setting_bdp(min_n_executors=1, max_n_executors=15, n_cores=8, executor_memory="16g", driver_memory="8g",
                app_name="Python app", driver_overhead="1g", executor_overhead='3g'):
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

    BDA_ENV = os.environ.get('BDA_USER_HOME', '')

    # Attach bda-core-ra codebase
    SPARK_COMMON_OPTS += " --files {}/scripts/properties/red_agent/nodes.properties,{}/scripts/properties/red_agent/nodes-de.properties,{}/scripts/properties/red_agent/nodes-es.properties,{}/scripts/properties/red_agent/nodes-ie.properties,{}/scripts/properties/red_agent/nodes-it.properties,{}/scripts/properties/red_agent/nodes-pt.properties,{}/scripts/properties/red_agent/nodes-uk.properties".format(
        *[BDA_ENV] * 7)

    os.environ["SPARK_COMMON_OPTS"] = SPARK_COMMON_OPTS
    os.environ["PYSPARK_SUBMIT_ARGS"] = "%s pyspark-shell " % SPARK_COMMON_OPTS


def initialize(app_name, min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "16g", driver_memory="8g"):
    import time
    start_time = time.time()

    print("_initialize spark")
    sc, spark, sql_context = get_spark_session(app_name=app_name, log_level="OFF", min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores,
                             executor_memory = executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark


def getMovDxsForCycleList(spark, yearmonthday, yearmonthday_target):
    # If the day is the last one of the month we take the client list of the last cycle of the previous month
    if yearmonthday[6:8] == '01': yearmonthday = get_previous_cycle(yearmonthday)

    current_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday
    target_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday_target

    current_base = spark \
        .read \
        .parquet(current_source) \
        .filter(col("ClosingDay") == yearmonthday) \
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (
        col("rgu").isNotNull())) \
        .filter(col('rgu') == 'movil') \
        .select(["msisdn", 'NIF_CLIENTE']) \
        .distinct() \
        .repartition(400)

    current_base.columns

    target_base = spark \
        .read \
        .parquet(target_source) \
        .filter(col("ClosingDay") == yearmonthday_target) \
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (
        col("rgu").isNotNull())) \
        .filter(col('rgu') == 'movil') \
        .select(["msisdn"]) \
        .distinct() \
        .repartition(400)

    print(current_base.columns, target_base.columns)

    churn_base = current_base \
        .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left") \
        .filter(col("tmp").isNull()) \
        .select(["msisdn", 'NIF_CLIENTE']).withColumn("label_dx", lit(1.0)) \
        .distinct()

    print("[Info getFbbDxsForMonth] " + time.ctime() + " DXs for MOV services during the period: " + yearmonthday + "-" + yearmonthday_target + ": " + str(
            churn_base.count()))

    return churn_base

def obtenerTabla(spark, config_obj=None, start_date=None, end_date=None, ref_date=None, select_cols=None):
    print(start_date, end_date, ref_date)
    if not start_date:
        start_date = config_obj.get_start_port()
    if not end_date:
        end_date = config_obj.get_end_port()
    if not ref_date:
        ref_date = end_date

    if start_date == None or end_date == None:
        print("start_date and end_date should be different to None (Inserted {} and {})".format(start_date, end_date))
        sys.exit()

    print("Get port requests table: start={} end={} ref_date={}".format(start_date, end_date, ref_date))

    select_cols = ["msisdn", "label"] if not select_cols else select_cols

    window = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

    start_date_obj = convert_to_date(start_date)
    end_date_obj = convert_to_date(end_date)

    print(start_date_obj,end_date_obj)

    convert_to_date_udf = udf(convert_to_date, StringType())

    df_mobport = (spark.read.table(PORT_TABLE_NAME)
                  .where((col("sopo_ds_fecha_solicitud") >= start_date_obj) & (col("sopo_ds_fecha_solicitud") <= end_date_obj))
                  .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn")
                  .withColumnRenamed("SOPO_DS_CIF", "NIF")
                  .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date"))


    return df_mobport.select(["portout_date", "msisdn", "NIF"])


def getScores(start_date):
    df_scores_read = spark.read.parquet(
        '/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/').where(
        col('predict_closing_date') == start_date)

    if df_scores_read.select('executed_at').distinct().count() > 1:
        maxDate = df_scores_read.agg({"executed_at": 'max'}).collect()[0]["max(executed_at)"]
        df_scores_read = df_scores_read.filter(col('executed_at') == maxDate)

    CAR = spark \
        .read \
        .parquet('/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_' + start_date) \
        .select('msisdn', 'NIF_CLIENTE')

    df_scores = df_scores_read.join(CAR, on='msisdn', how='left').dropna(subset='NIF_CLIENTE')
    df_scores = df_scores.drop('nif').withColumnRenamed('NIF_CLIENTE', 'nif')
    # print(df_scores.columns,df_scoresMOV_prev.show())

    return df_scores


def estadisticas(lossType, portRequest, losses, bucketedData, clientType, nBuckets=100):

    percentiles = np.arange(0, nBuckets)

    F1_values = []
    Accuracy_values = []
    Sensitivity_values = []
    Specificity_values = []

    TP_values=[]
    FP_values=[]
    FN_values=[]
    TN_values=[]

    print('****',clientType,lossType, '****')

    if lossType == 'Portas':
        dfRequests = portRequest
    if lossType == 'Bajas':
        dfRequests = losses

    if clientType == 'MOV' and lossType == 'Juntas':
             dfRequests = portRequest.select('msisdn').union(losses.select('msisdn'))
             dfRequests = (dfRequests.na.drop())

    if clientType=='FBB' and lossType == 'Juntas':
            dfRequests = portRequest.select('NUM_CLIENTE', 'NIF_CLIENTE').union(losses.select('NUM_CLIENTE', 'NIF_CLIENTE'))
            dfRequests = (dfRequests.na.drop().withColumnRenamed("NUM_CLIENTE", "num_cliente").withColumnRenamed("NIF_CLIENTE","NIF"))

            dfRequests = (dfRequests.withColumnRenamed("NUM_CLIENTE", "num_cliente").withColumnRenamed("NIF_CLIENTE", "NIF"))

    for pp in percentiles:
        print('Percentil:', pp)

        df_scores_decile = bucketedData.withColumn('risk', when(col('bucketedFeatures') >= pp, 1).otherwise(0))
        if clientType == 'MOV':
            df_scores_decile_sel = dfRequests.join(df_scores_decile, on="msisdn", how="inner")

        if clientType == 'FBB':
            df_scores_decile = df_scores_decile.withColumnRenamed('client_id', 'num_cliente')
            df_scores_decile_sel = dfRequests.join(df_scores_decile, on="num_cliente", how="inner")

        df_scores_decile_sel = df_scores_decile_sel.withColumn('Reality', lit(1))

        if clientType == 'MOV':
            tabla_matrix = df_scores_decile.select(['msisdn', 'risk']).join(df_scores_decile_sel.select(['msisdn', 'Reality']), on='msisdn', how='left')

        if clientType == 'FBB':
            tabla_matrix = df_scores_decile.select(['num_cliente', 'risk']).join(df_scores_decile_sel.select(['num_cliente', 'Reality']),
                on='num_cliente', how='left')


        tabla_matrix = tabla_matrix.withColumn('Reality_fill', when((col('Reality').isNull()), 0).otherwise(1))
        tabla_matrix = tabla_matrix.drop('Reality').withColumnRenamed('Reality_fill', 'Reality')

        scoreAndLabels = tabla_matrix.select([tabla_matrix.risk.cast(DoubleType()), tabla_matrix.Reality.cast(DoubleType())]).rdd.map(tuple)

        metrics = MulticlassMetrics(scoreAndLabels)
        confusionMatrix = metrics.confusionMatrix().toArray()

        TP = confusionMatrix[1, 1]
        FP = confusionMatrix[0, 1]
        FN = confusionMatrix[1, 0]
        TN = confusionMatrix[0, 0]

        Acc = (TP + TN) / (TP + TN + FP + FN)
        F1Score = (2 * TP) / (2 * TP + FP + FN)
        Sensitivity = TP / (TP + FN)
        Specificity = TN / (TN + FP)

        F1_values.append(F1Score)
        Accuracy_values.append(Acc)
        Sensitivity_values.append(Sensitivity)
        Specificity_values.append(Specificity)

        TP_values.append(TP)
        FP_values.append(FP)
        FN_values.append(FN)
        TN_values.append(TN)

    datos = {'perc': percentiles, 'F1': F1_values, 'Accuracy': Accuracy_values, 'Sensitivity': Sensitivity_values,
             'Specificity': Specificity_values, 'TP': TP_values, 'FP': FP_values, 'FN': FN_values, 'TN': TN_values}

    df = pd.DataFrame(datos)

    return df


def estadisticasJuntas(lossType, portRequestMOV, portRequestFBB, lossesMOV, lossesFBB, bucketedData, nBuckets=100):

    percentiles = np.arange(0, nBuckets)

    F1_values = []
    Accuracy_values = []
    Sensitivity_values = []
    Specificity_values = []

    TP_values=[]
    FP_values=[]
    FN_values=[]
    TN_values=[]

    print('****', 'MOV_FBB', lossType, '****')

    if lossType == 'Portas':
        dfRequestsFBB = portRequestFBB
        dfRequestsMOV = portRequestMOV
    if lossType == 'Bajas':
        dfRequestsFBB = lossesFBB
        dfRequestsMOV = lossesMOV
        dfRequestsMOV = dfRequestsMOV.withColumnRenamed("NIF_CLIENTE", "NIF")
    if lossType == 'Juntas':
        dfRequestsFBB = portRequestFBB.select('NUM_CLIENTE', 'NIF_CLIENTE').union(
            lossesFBB.select('NUM_CLIENTE', 'NIF_CLIENTE'))
        dfRequestsFBB = (dfRequestsFBB.na.drop().withColumnRenamed("NUM_CLIENTE", "num_cliente")
                         .withColumnRenamed("NIF_CLIENTE", "NIF"))

        dfRequestsMOV = portRequestMOV.select(['msisdn', 'NIF']).union(lossesMOV.select('msisdn', 'NIF'))
        dfRequestsMOV = (dfRequestsMOV.na.drop())

    dfRequestsFBB = (
        dfRequestsFBB.withColumnRenamed("NUM_CLIENTE", "num_cliente").withColumnRenamed("NIF_CLIENTE", "NIF"))

    dfRequests = dfRequestsFBB.select('NIF').union(dfRequestsMOV.select('NIF'))
    dfRequests = dfRequests.distinct()



    for pp in percentiles:
        print('Percentil:', pp)

        df_scores_decile = bucketedData.withColumn('risk', when(col('bucketedFeatures') >= pp, 1).otherwise(0))

        df_scores_decile_sel = dfRequests.join(df_scores_decile, on="NIF", how="inner")
        df_scores_decile_sel = df_scores_decile_sel.withColumn('Reality', lit(1))

        tabla_matrix = df_scores_decile.select(['NIF', 'risk']).join(df_scores_decile_sel.select(['NIF', 'Reality']),
                                                                     on='NIF', how='left')

        tabla_matrix = tabla_matrix.withColumn('Reality_fill', when((col('Reality').isNull()), 0).otherwise(1))
        tabla_matrix = tabla_matrix.drop('Reality').withColumnRenamed('Reality_fill', 'Reality')
        scoreAndLabels = tabla_matrix.select(
            [tabla_matrix.risk.cast(DoubleType()), tabla_matrix.Reality.cast(DoubleType())]).rdd.map(tuple)

        metrics = MulticlassMetrics(scoreAndLabels)
        confusionMatrix = metrics.confusionMatrix().toArray()

        TP = confusionMatrix[1, 1]
        FP = confusionMatrix[0, 1]
        FN = confusionMatrix[1, 0]
        TN = confusionMatrix[0, 0]

        Acc = (TP + TN) / (TP + TN + FP + FN)
        F1Score = (2 * TP) / (2 * TP + FP + FN)
        Sensitivity = TP / (TP + FN)
        Specificity = TN / (TN + FP)

        F1_values.append(F1Score)
        Accuracy_values.append(Acc)
        Sensitivity_values.append(Sensitivity)
        Specificity_values.append(Specificity)

        TP_values.append(TP)
        FP_values.append(FP)
        FN_values.append(FN)
        TN_values.append(TN)

    datos = {'perc': percentiles, 'F1': F1_values, 'Accuracy': Accuracy_values, 'Sensitivity': Sensitivity_values,
             'Specificity': Specificity_values,'TP':TP_values,'FP':FP_values,'FN':FN_values,'TN':TN_values}


    df = pd.DataFrame(datos)

    return df

def plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80):

    fig = plt.figure(figsize=(8, 5), dpi=100)
    plt.plot((100 - dfPlot['perc'] * 100 / nBuckets), dfPlot['F1'], marker='o', label='F1 Score')
    plt.plot((100 - dfPlot['perc'] * 100 / nBuckets), dfPlot['Accuracy'], marker='o', label='Accuracy')
    plt.plot((100 - dfPlot['perc'] * 100 / nBuckets), dfPlot['Sensitivity'], marker='o', label='Sensitivity')
    plt.plot((100 - dfPlot['perc'] * 100 / nBuckets), dfPlot['Specificity'], marker='o', label='Specificity')
    plt.xlabel('Percentage of clients Labeled as 1')
    plt.legend()
    plt.title(lossType + ' ' + tipo)
    plt.savefig(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.jpeg', dpi=300)

    fig = plt.figure(figsize=(8, 5), dpi=100)
    plt.plot((100 - dfPlot['perc'][lim:] * 100 / nBuckets), dfPlot['F1'][lim:], marker='o', label='F1 Score')
    plt.xlabel('Percentage of clients Labeled as 1')
    plt.title(lossType + ' ' + tipo)
    plt.legend()
    # plt.show()

    plt.savefig(ruta + 'F1Max_' + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.jpeg', dpi=300)



if __name__ == "__main__":

    set_paths()

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *

    if len(sys.argv[1:]) != 3:

        print 'ERROR: This program takes 3 input arguments:', '\n', \
            '- Initial day of the comparisson period', '\n', \
            '- Lenght of the comparisson period (in cycles)', '\n', \
            '- Number of buckets to consider'

        sys.exit()

    else:

        # Starting Spark Session
        ########################
        spark = initialize("churn_quality_CM.py")

        start_date = sys.argv[1]
        lenght = sys.argv[2]
        nBuckets = int(sys.argv[3])


    cycle = 0
    fini_tmp = start_date
    while cycle < int(lenght) / 7:
        end_date = get_next_cycle(fini_tmp, str_fmt="%Y%m%d")
        cycle = cycle + 1
        fini_tmp = end_date

    yearmonth = end_date[0:6]

    ruta='/var/SP/data/home/bgmerin1/src/Repositorios/use-cases/churn/models/EvaluarModelosChurn/Outputs_CM/'

    caso = ['Portas', 'Bajas', 'Juntas']

    tipo_aux = 'MOVFBB'
    file1_aux = ruta + 'Portas_' + tipo_aux + '_' + start_date + '_' + end_date
    file2_aux = ruta + 'Bajas_' + tipo_aux + '_' + start_date + '_' + end_date
    file3_aux = ruta + 'Juntas_' + tipo_aux + '_' + start_date + '_' + end_date


    #############
    ### MOVIL ###
    #############

    tipo='MOV'

    print "[Info Churn Quality Confusion Matrix] " + time.ctime() + " Movil comparison."

    file1 = ruta + 'Portas_' + tipo +'_' + start_date + '_' + end_date
    file2 = ruta + 'Bajas_' + tipo + '_' + start_date + '_' + end_date
    file3 = ruta + 'Juntas_' + tipo +'_' + start_date + '_' + end_date

  #  if os.path.exists(file1_aux + '.csv') and os.path.exists(file2_aux + '.csv') and os.path.exists(file3_aux + '.csv'):

    df_scoresMOV_prev = getScores(start_date)

    PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"
    ref_date = None
    config_obj = None

    # - Solicitudes de baja de MOV
    movporttr = obtenerTabla(spark, config_obj, start_date, end_date, ref_date)
    # - Porque dejen de estar en la lista de clientes
    movdxtr = getMovDxsForCycleList(spark, start_date, end_date)

    movdxtr = movdxtr.withColumnRenamed('NIF_CLIENTE', 'NIF')

    movporttr = movporttr.orderBy('portout_date', ascending=True).drop_duplicates(subset=['msisdn'])
    # El msisdn_a es el msisdn_anonimizado y se corresponde con el msisdn de la tabla de servicios.
    # En esa misma tabla el valor de campo 2 es el que se corresponde con el msisdn de la tabla de Fede
    movporttr = movporttr.orderBy('portout_date', ascending=True)

    df_scoresMOV = df_scoresMOV_prev.orderBy('model_output', ascending=False)
    df_scoresMOV_idx = add_column_index(spark, df_scoresMOV, offset=1, colName="idx")

    df_scoresMOV_idx_buckets = df_scoresMOV_idx.withColumn('scoring_f',
                                                               col('scoring') * (rand() * 0.0001 + 1.00000).cast(
                                                                   FloatType()))
    bucketedDataMOV = QuantileDiscretizer(numBuckets=nBuckets, inputCol="scoring_f", outputCol="bucketedFeatures",
                                              relativeError=0).fit(df_scoresMOV_idx_buckets).transform(df_scoresMOV_idx_buckets)
    bucketedDataMOV = bucketedDataMOV.cache()

    caso = ['Portas', 'Bajas', 'Juntas']
    dCasos={}

    if os.path.exists(file1+'.csv') and os.path.exists(file2+'.csv') and os.path.exists(file3+'.csv'):
        if os.path.exists(file1 + '.jpeg') and os.path.exists(file2 + '.jpeg') and os.path.exists(file3 + '.jpeg'):
            print('The date used has already been processed for MOV. The results are ready.')

        else:

            for lossType in caso:
                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot,nBuckets,lossType,tipo,ruta,start_date,end_date,lim=80)

    else:

        for lossType in caso:

            print "[Info Churn Quality Confusion Matrix] " + time.ctime() + lossType

            if not os.path.exists(ruta+lossType+'_' + tipo + '_'+start_date+'_'+end_date+'.csv'):
                df = estadisticas(lossType, movporttr, movdxtr, bucketedDataMOV,tipo, nBuckets)
                dCasos[lossType] = df
                df.to_csv(ruta+lossType+'_' + tipo + '_'+start_date+'_'+end_date+'.csv')

                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)


            else:
                dfPlot = pd.read_csv(ruta+lossType+'_' + tipo + '_'+start_date+'_'+end_date+'.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)

    #############
    #### FBB ####
    #############

    tipo='FBB'

    print "[Info Churn Quality Confusion Matrix] " + time.ctime() + " FBB comparison."

    file1 = ruta + 'Portas_'+ tipo + '_' + start_date + '_' + end_date
    file2 = ruta + 'Bajas_'+ tipo + '_' + start_date + '_' + end_date
    file3 = ruta + 'Juntas_'+ tipo + '_' + start_date + '_' + end_date

   # if os.path.exists(file1_aux + '.csv') and os.path.exists(file2_aux + '.csv') and os.path.exists(file3_aux + '.csv'):

    # Getting Portability Requests
    ##############################################
    fixporttr = getFixPortRequestsForCycleList(spark, start_date, end_date)
    fixdxtr = getFbbDxsForCycleList(spark, start_date, end_date)

    ## Read the CAR to get the num_cliente
    origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'
    clientCARdf = spark \
            .read \
            .parquet(origin + start_date) \
            .filter(col("ClosingDay") == start_date) \
            .repartition(400) \
            .cache()

    fixporttr_numC = (fixporttr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'NIF_CLIENTE']).distinct(),
                                         on='msisdn', how='inner'))
    fixdxtr_numC = (fixdxtr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'NIF_CLIENTE']), on='msisdn',
                                     how='inner'))


    # # Getting churn model scores
    # ################################
    df_scoresFBB_prev = (
            spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_fbb')
            .where(col('predict_closing_date') == start_date)
            )

    df_scoresFBB = df_scoresFBB_prev.select(['client_id', 'nif', 'extra_info', 'model_output']).withColumn('prediction',
                                                                                                               df_scoresFBB_prev[
                                                                                                                   'extra_info'].substr(
                                                                                                                   0, 1))
    df_scoresFBB = df_scoresFBB.orderBy('model_output', ascending=False)
    df_scoresFBB_idx = add_column_index(spark, df_scoresFBB, offset=1, colName="idx")
    df_scoresFBB_idx = (df_scoresFBB_idx.na.drop().withColumnRenamed("client_id", "num_cliente"))

    df_scoresFBB_idx_buckets = df_scoresFBB_idx.withColumn('scoring_f',
                                                               col('model_output') * (rand() * 0.0001 + 1.00000).cast(
                                                                   FloatType()))
    bucketedDataFBB = QuantileDiscretizer(numBuckets=nBuckets, inputCol="scoring_f", outputCol="bucketedFeatures",
                                              relativeError=0).fit(df_scoresFBB_idx_buckets).transform(
            df_scoresFBB_idx_buckets)
    bucketedDataFBB = bucketedDataFBB.cache()

    dCasosFBB = {}

    if os.path.exists(file1+'.csv') and os.path.exists(file2+'.csv') and os.path.exists(file3+'.csv'):
        if os.path.exists(file1 + '.jpeg') and os.path.exists(file2 + '.jpeg') and os.path.exists(file3 + '.jpeg'):
            print('The date used has already been processed for FBB. The results are ready.')

        else:

            for lossType in caso:
                dfPlot = pd.read_csv(ruta + lossType + '_'+ tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)

    else:

        for lossType in caso:
            print "[Info Churn Quality Confusion Matrix] " + time.ctime() + ' ' +lossType

            if not os.path.exists(ruta + lossType + tipo + '_' + start_date + '_' + end_date + '.csv'):
               df = estadisticas(lossType, fixporttr_numC, fixdxtr_numC, bucketedDataFBB,tipo,nBuckets)
               dCasosFBB[lossType] = df
               df.to_csv(ruta + lossType + '_' + tipo +'_' + start_date + '_' + end_date + '.csv')

               dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

               plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)

            else:

                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)

    #############
    ## FBB+MOV ##
    #############

    tipo = 'MOVFBB'

    print "[Info Churn Quality Confusion Matrix] " + time.ctime() + " MOV+FBB comparison."

    file1 = ruta + 'Portas_' + tipo + '_' + start_date + '_' + end_date
    file2 = ruta + 'Bajas_' + tipo + '_' + start_date + '_' + end_date
    file3 = ruta + 'Juntas_' + tipo + '_' + start_date + '_' + end_date


    if os.path.exists(file1 + '.csv') and os.path.exists(file2 + '.csv') and os.path.exists(file3 + '.csv'):
        if os.path.exists(file1 + '.jpeg') and os.path.exists(file2 + '.jpeg') and os.path.exists(file3 + '.jpeg'):
            print('The date used has already been processed for FBB. The results are ready.')

        else:

            for lossType in caso:
                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)
    else:

        dfScoresMOV = (df_scoresMOV.select(['scoring', 'predict_closing_date', 'nif'])
                       .withColumnRenamed('scoring', 'scoringMOV')
                       # .withColumnRenamed('decile','decileMOV')
                       .withColumnRenamed('predict_closing_date', 'predict_closing_dateMOV'))

        dfScoresFBB = (df_scoresFBB_prev.select(['scoring', 'predict_closing_date', 'NIF'])
                       .withColumnRenamed('scoring', 'scoringFBB')
                       # .withColumnRenamed('decile','decileFBB')
                       .withColumnRenamed('predict_closing_date', 'predict_closing_dateFBB'))

        dfMediaMOV = dfScoresMOV.groupBy('NIF').agg(sql_max('scoringMOV'))
        dfMediaFBB = dfScoresFBB.groupBy('NIF').agg(sql_max('scoringFBB'))

        df = dfMediaFBB.join(dfMediaMOV, on='NIF', how='full')

        df = df.withColumn('scoringFBB_fill', when((col('max(scoringFBB)').isNull()), col('max(scoringMOV)')).otherwise(
            col('max(scoringFBB)')))
        df = df.withColumn('scoringMOV_fill', when((col('max(scoringMOV)').isNull()), col('max(scoringFBB)')).otherwise(
            col('max(scoringMOV)')))
        df=(df.drop('max(scoringFBB)').withColumnRenamed('scoringFBB_fill','scoringFBB')
          .drop('max(scoringMOV)').withColumnRenamed('scoringMOV_fill','scoringMOV')
             )

        dfScores = df.withColumn('scoreAvg', greatest(df.scoringFBB, df.scoringMOV))

        dfScores = dfScores.orderBy('scoreAvg', ascending=False)
        dfScores_idx = add_column_index(spark, dfScores, offset=1, colName="idx")

        dfScores_idx_buckets = dfScores_idx.withColumn('scoring_f',col('scoreAvg') * (rand() * 0.0001 + 1.00000).cast(FloatType()))
        bucketedData = QuantileDiscretizer(numBuckets=nBuckets, inputCol="scoring_f", outputCol="bucketedFeatures",
                                           relativeError=0).fit(dfScores_idx_buckets).transform(dfScores_idx_buckets)
        bucketedData = bucketedData.cache()

        dCasosMOVFBB = {}

        for lossType in caso:
            print "[Info Churn Quality Confusion Matrix] " + time.ctime() + ' '+lossType

            if not os.path.exists(ruta + lossType + '_' + tipo +'_' + start_date + '_' + end_date + '.csv'):
                df = estadisticasJuntas(lossType, movporttr, fixporttr_numC, movdxtr, fixdxtr_numC, bucketedData, nBuckets)
                dCasosMOVFBB[lossType] = df
                df.to_csv(ruta + lossType + '_' + tipo +'_' + start_date + '_' + end_date + '.csv')

                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo + '_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)

            else:
                dfPlot = pd.read_csv(ruta + lossType + '_' + tipo +'_' + start_date + '_' + end_date + '.csv')

                plotCurves(dfPlot, nBuckets, lossType, tipo, ruta, start_date, end_date, lim=80)



