# coding=utf-8

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

def get_spark_session(app_name="default name", log_level='INFO', min_n_executors=1, max_n_executors=15, n_cores=4,
                      executor_memory="32g", driver_memory="32g"):
    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)

    setting_bdp(app_name=app_name, min_n_executors=min_n_executors, max_n_executors=max_n_executors, n_cores=n_cores,
                executor_memory=executor_memory, driver_memory=driver_memory)
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
    # os.environ["SPARK_EXTRA_CONF_PARAMETERS"] = '--conf spark.yarn.jars=hdfs:///data/raw/public/lib_spark_2_1_0_jars_SPARK-18971/*'


def initialize(app_name, min_n_executors=1, max_n_executors=15, n_cores=4, executor_memory="16g", driver_memory="8g"):
    import time
    start_time = time.time()

    print("_initialize spark")
    sc, spark, sql_context = get_spark_session(app_name=app_name, log_level="OFF", min_n_executors=min_n_executors,
                                               max_n_executors=max_n_executors, n_cores=n_cores,
                                               executor_memory=executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark


def plotResults(lenght, OUTPUTFILE):
    # Leemos todos los archivos excel que tenemos para representarlos,
    # si no deseamos que se use alguno de ellos incluirle un prefijo en su nombre
    dfDraw = pd.DataFrame(data=None, columns=['CAR Date', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate', 'Lift'])
    for file in glob.glob(OUTPUTFILE + "Churn*.xlsx"):
        if file.split('/')[-1].split('_')[3] == str(lenght):
            print(file)
            dfs = pd.read_excel(file)
            dfs['CAR Date'] = file.split('/')[-1].split('_')[2]
            dfDraw = dfDraw.append(
                dfs[['CAR Date', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate', 'Lift']].dropna(0))

    dfDraw = dfDraw.sort_values('CAR Date')

    lRisk = ['Super High', 'High', 'Medium', 'Low', 'No Risk']

    colormap = cm.gist_rainbow
    colorlist = [colors.rgb2hex(colormap(i)) for i in np.linspace(0, 0.9, len(lRisk))]

    variables = ['Churn Rate', 'Total Churn', 'Lift']

    dfDraw['CAR Date'] = pd.to_datetime(dfDraw['CAR Date'], format='%Y-%m-%d')
    for var in variables:
        fig = plt.figure(figsize=(8, 5), dpi=100)

        for nn, risk in enumerate(lRisk):
            dt = [x.to_pydatetime() for x in dfDraw[dfDraw['Risk Label'] == risk]['CAR Date']]
            plt.plot(dt, dfDraw[dfDraw['Risk Label'] == risk][var], label=risk, color=colorlist[nn], marker='o',
                     linestyle='-')

        if var == 'Churn Rate': plt.title('Churn Rate: Portability Requests/Total of clients for each Risk section')
        if var == 'Total Churn': plt.title(
            'Total Churn: Portability Requests for each Risk Section/Total of Portability Requests')
        if var == 'Lift': plt.title('Lift: Churn rate in the segment/churn rate in the clients database')
        plt.ylabel(var)
        plt.xlabel('Date')
        plt.xticks(rotation=10)
        plt.legend(loc='best')

        if sys.argv[3] == 'Yes' or sys.argv[3] == 'yes' or sys.argv[3] == 'Y' or sys.argv[3] == 'y':
            plt.savefig(
                OUTPUTFILE + 'Analysis_' + var.replace(' ', '_') + '_' + str(int(lenght) / 7) + 'cycles_fbb.jpeg',
                dpi=300)

            imgdata = BytesIO()

            fig.savefig(imgdata)

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

    select_cols = ["msisdn_a", "label"] if not select_cols else select_cols

    window = Window.partitionBy("msisdn_a").orderBy(desc("days_from_portout"))  # keep the 1st portout

    start_date_obj = convert_to_date(start_date)
    end_date_obj = convert_to_date(end_date)

    print(start_date_obj,end_date_obj)

    convert_to_date_udf = udf(convert_to_date, StringType())

    df_mobport = (spark.read.table(PORT_TABLE_NAME)
                  .where(
        (col("sopo_ds_fecha_solicitud") >= start_date_obj) & (col("sopo_ds_fecha_solicitud") <= end_date_obj))
                  .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_a")
                  .withColumnRenamed("SOPO_DS_CIF", "NIF")
                  .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date"))

    return df_mobport.select(["portout_date", "msisdn_a", "NIF"])

def getScores(start_date):

    df_scores_onlymob = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_onlymob/').where(
        col('predict_closing_date') == start_date)
    df_scores_conv = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_mobileandfbb/').where(
        col('predict_closing_date') == start_date)
    df_scores_others = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_others/').where(
        col('predict_closing_date') == start_date)

    if df_scores_onlymob.select('executed_at').distinct().count() > 1:
        maxDate = df_scores_onlymob.agg({"executed_at": 'max'}).collect()[0]["max(executed_at)"]
        df_scores_onlymob = df_scores_onlymob.filter(col('executed_at') == maxDate)

    if df_scores_conv.select('executed_at').distinct().count() > 1:
        maxDate = df_scores_conv.agg({"executed_at": 'max'}).collect()[0]["max(executed_at)"]
        df_scores_conv = df_scores_conv.filter(col('executed_at') == maxDate)

    if df_scores_others.select('executed_at').distinct().count() > 1:
        maxDate = df_scores_others.agg({"executed_at": 'max'}).collect()[0]["max(executed_at)"]
        df_scores_others = df_scores_others.filter(col('executed_at') == maxDate)

    return df_scores_onlymob,df_scores_conv,df_scores_others


OUTPUTFILE = '/var/SP/data/bdpmdses/deliveries_churn/model_evaluation_nif/'
FILENAME_EXCEL = OUTPUTFILE + 'Churn_Predictions_{}_{}_{}.xlsx'

if __name__ == "__main__":

    set_paths()

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *

    if len(sys.argv[1:]) != 3:

        print 'ERROR: This program takes 3 input arguments:', '\n', \
            '- Initial day of the comparisson period', '\n', \
            '- Lenght of the comparisson period (in cycles)', '\n', \
            '- Would you like to save the results? Yes or No'

    else:
        end_date=sys.argv[1]
        lenght = sys.argv[2]  ## Days

        # We take portability requests for 30 days after the starting date provided as the comparisson period
        cycle = 0
        fini_tmp = end_date
        while cycle < int(lenght) / 7:
            start_date = get_previous_cycle(fini_tmp, str_fmt="%Y%m%d")
            cycle = cycle + 1
            fini_tmp = start_date

        print('Start Day:',start_date,'End Day:',end_date)
        today = time.strftime("%Y%m%d")

        print("***********************************************************************************************************")
        print("Comparing the Portability Requests from {} during a period of {} cycles".format(start_date, lenght))
        print('***********************************************************************************************************')

        # We check if there is an existing file with model comparissons or if the provided date has been processed already
        #####################################################################################################################

        filename_excel = FILENAME_EXCEL.format(str(start_date), str(lenght), str(today))
        column = ['CAR Date', 'Period Lenght', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate']

        doneDates = []
        for file in glob.glob(OUTPUTFILE + "*.xlsx"):
            doneDates.append(file.split('/')[-1].split('_')[2])
        print(doneDates)

        for date in doneDates:
            if str(date) == str(start_date):
                print('**** There is already a churn record for the starting date provided ****')

        doneDates.append(start_date)

        # Starting Spark Session
        #########################
        spark = initialize("churn_Preds_quality_fbb_NIF.py")

        # Saving files
        #################
        fileMatrix = open("ConfusionMatrix.txt", "a")

        print >> fileMatrix, '****************'
        print >> fileMatrix, '***'+end_date+'***'
        print >> fileMatrix, '****************'

        #############
        ### MOVIL ###
        #############

        # Getting Portability Requests
        ##################################
        # We consider a month after the input date (the day of the picture)

        PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"
        ref_date = None
        config_obj = None

        # We take portability requests for 30 days after the starting date provided as the comparisson period
        dfRequestsMOV = obtenerTabla(spark, config_obj, start_date, end_date, ref_date)

        dfRequestsMOV = dfRequestsMOV.orderBy('portout_date', ascending=True).drop_duplicates(subset=['msisdn_a'])
        # El msisdn_a es el msisdn_anonimizado y se corresponde con el msisdn de la tabla de servicios.
        # En esa misma tabla el valor de campo 2 es el que se corresponde con el msisdn de la tabla de Fede
        dfRequestsMOV = dfRequestsMOV.orderBy('portout_date', ascending=True)

        # # Getting churn model scores
        # ################################
        if datetime.strptime(start_date, '%Y%m%d') == datetime.strptime('20181221', '%Y%m%d'):
            unifiedT = 'No'

        elif datetime.strptime(start_date, '%Y%m%d') == datetime.strptime('20181231', '%Y%m%d'):
            unifiedT = 'No'

        else:
            unifiedT = 'Yes'

        df_scores_onlymob, df_scores_conv,df_scores_others = getScores(start_date)

        fecha_lim = datetime.strptime('20181214', '%Y%m%d')
        if datetime.strptime(start_date, '%Y%m%d') > fecha_lim:
            df_scores = union_all([df_scores_conv, df_scores_onlymob,df_scores_others])
            df_scores = df_scores.orderBy('model_output', ascending=False)
            df_scores = add_column_index(spark, df_scores, offset=1, colName="idx")
        else:
            df_scores = orderScores(df_scores_onlymob, df_scores_conv)

        ### Confusion Matrix MOVIL
        ################################

        print(df_scores.count())

        df_scores_decile_MOV = add_decile(df_scores)
        df_scores_decile_sel_MOV = dfRequestsMOV.join(df_scores_decile_MOV,
                                                      on=(dfRequestsMOV["msisdn_a"] == df_scores_decile_MOV["msisdn"]),
                                                      how="inner")

        df_scores_decile_sel_MOV = df_scores_decile_sel_MOV.withColumn('real_risk', lit(1))
        tabla_matrix_MOV = df_scores_decile_MOV.select(['msisdn', 'risk']).join(
            df_scores_decile_sel_MOV.select(['msisdn', 'real_risk']),
            on='msisdn', how='left')

        tabla_matrix_MOV = tabla_matrix_MOV.withColumn('real_risk_fill',
                                                       when((col('real_risk').isNull()), 0).otherwise(1))
        tabla_matrix_MOV = tabla_matrix_MOV.drop('real_risk').withColumnRenamed('real_risk_fill', 'real_risk')
        scoreAndLabels = tabla_matrix_MOV.select(
            [tabla_matrix_MOV.risk.cast(DoubleType()), tabla_matrix_MOV.real_risk.cast(DoubleType())]).rdd.map(tuple)

        metrics = MulticlassMetrics(scoreAndLabels)
        confusionMatrix_MOV = metrics.confusionMatrix().toArray()

        print >> fileMatrix, '** MOVIL **'

        #print('******* Confussion Matrix Movil *******')
        print >>fileMatrix,confusionMatrix_MOV

        #print('******* Accuracy: ')
        print >>fileMatrix, (confusionMatrix_MOV[0, 0] + confusionMatrix_MOV[1, 1]) / sum(sum(confusionMatrix_MOV)) * 100

        df_scoresMOV = df_scores.drop_duplicates(subset=['msisdn'])

        dfScoresMOV = (df_scoresMOV.select(['scoring', 'predict_closing_date', 'NIF'])
                       .withColumnRenamed('scoring', 'scoringMOV')
                       .withColumnRenamed('predict_closing_date', 'predict_closing_dateMOV'))

        dfMediaMOV = dfScoresMOV.groupBy('NIF').agg(sql_max('scoringMOV'))


        ###########
        ### FBB ###
        ###########

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

        fixporttr_numC = (fixporttr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'CAMPO1', 'CAMPO2', 'NIF_CLIENTE']).distinct(),
                           on=['msisdn'], how='inner'))
        fixdxtr_numC = (fixdxtr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'CAMPO1', 'CAMPO2', 'NIF_CLIENTE']), on='msisdn',
                         how='inner'))

        dfRequestsFBB = fixporttr_numC.select('NUM_CLIENTE', 'NIF_CLIENTE').union(fixdxtr_numC.select('NUM_CLIENTE', 'NIF_CLIENTE'))
        dfRequestsFBB = (dfRequestsFBB.na.drop().withColumnRenamed("NUM_CLIENTE", "num_cliente").withColumnRenamed("NIF_CLIENTE", "NIF"))

        # # Getting churn model scores
        # ################################
        df_scoresFBB = (spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_fbb')
                  .where(col('predict_closing_date') == start_date)
                  )

        dfScoresFBB = (df_scoresFBB.select(['scoring', 'predict_closing_date', 'NIF'])
                       .withColumnRenamed('scoring', 'scoringFBB')
                       .withColumnRenamed('predict_closing_date', 'predict_closing_dateFBB'))

        dfMediaFBB = dfScoresFBB.groupBy('NIF').agg(sql_max('scoringFBB'))

        ### Confusion Matrix FBB
        ################################

        df_scoresFBB = df_scoresFBB.orderBy('model_output', ascending=False)
        df_scoresFBB = add_column_index(spark, df_scoresFBB, offset=1, colName="idx")

        df_scores_decile_FBB = add_decile(df_scoresFBB)
        df_scores_decile_FBB = df_scores_decile_FBB.withColumnRenamed('client_id', 'num_cliente')

        df_scores_decile_sel_FBB = dfRequestsFBB.join(df_scores_decile_FBB, on="num_cliente", how="inner")
        df_scores_decile_sel_FBB = df_scores_decile_sel_FBB.withColumn('real_risk', lit(1))

        tabla_matrix_FBB = (df_scores_decile_FBB.select(['num_cliente', 'risk']).join(
            df_scores_decile_sel_FBB.select(['num_cliente', 'real_risk']),
            on='num_cliente', how='left'))

        tabla_matrix_FBB = tabla_matrix_FBB.withColumn('real_risk_fill',
                                                       when((col('real_risk').isNull()), 0).otherwise(1))
        tabla_matrix_FBB = tabla_matrix_FBB.drop('real_risk').withColumnRenamed('real_risk_fill', 'real_risk')
        scoreAndLabels = tabla_matrix_FBB.select(
            [tabla_matrix_FBB.risk.cast(DoubleType()), tabla_matrix_FBB.real_risk.cast(DoubleType())]).rdd.map(tuple)

        metrics = MulticlassMetrics(scoreAndLabels)
        confusionMatrix_FBB = metrics.confusionMatrix().toArray()

        print >> fileMatrix, '** FBB **'

        #print('******* Confussion Matrix FBB *******')
        print >> fileMatrix,confusionMatrix_FBB

        #print('******* Accuracy: ',)
        print >> fileMatrix,(confusionMatrix_FBB[0, 0] + confusionMatrix_FBB[1, 1]) / sum(sum(confusionMatrix_FBB)) * 100

        ### FBB and MOVIL together

        df = dfMediaFBB.join(dfMediaMOV, on='NIF', how='full')

        df = df.withColumn('scoringFBB_fill', when((col('avg(scoringFBB)').isNull()), col('avg(scoringMOV)')).otherwise(col('avg(scoringFBB)')))
        df = df.withColumn('scoringMOV_fill', when((col('avg(scoringMOV)').isNull()), col('avg(scoringFBB)')).otherwise(col('avg(scoringMOV)')))

        df = (df.drop('avg(scoringFBB)').withColumnRenamed('scoringFBB_fill', 'scoringFBB')
              .drop('avg(scoringMOV)').withColumnRenamed('scoringMOV_fill', 'scoringMOV')
              )

        dfScores = df.withColumn('scoreAvg', (col('scoringMOV') + col('scoringFBB')) / 2)

        # Save the results into a file

        if sys.argv[3] == 'Yes' or sys.argv[3] == 'yes' or sys.argv[3] == 'Y' or sys.argv[3] == 'y':

            year = start_date[0:4]
            month = start_date[4:6]
            day = start_date[6:]

            dfScores_Saving = (dfScores.withColumn('year', lit(year))
                               .withColumn('month', lit(int(month)))
                               .withColumn('day', lit(int(day)))
                               .withColumn('predictionDate', lit(start_date))
                               )

            dfScores_Saving = dfScores_Saving.coalesce(1)

            (dfScores_Saving.write.partitionBy('year', 'month', 'day').mode("append").format("parquet")
             .save('/data/attributes/vf_es/churn_model_scores'))

        ###################################

        dfScores = dfScores.select(['NIF', 'scoreAvg']).sort(col("scoreAvg").desc())
        df_scores = add_column_index(spark, dfScores, offset=1, colName="idx")
        df_scores_decile = add_decile(df_scores)

        dfRequests = dfRequestsFBB.select('NIF').union(dfRequestsMOV.select('NIF'))
        dfRequests = dfRequests.distinct()

        df_scores_decile_sel = dfRequests.join(df_scores_decile, on='NIF', how="inner")

        df_salida_num = df_scores_decile.groupby('decile').count().collect()
        df_salida_sel_num = df_scores_decile_sel.groupby('decile').count().collect()

        ### Confusion Matrix
        #######################

        df_scores_decile_sel = df_scores_decile_sel.withColumn('real_risk', lit(1))
        tabla_matrix = df_scores_decile.select(['NIF', 'risk']).join(df_scores_decile_sel.select(['NIF', 'real_risk']),
                                                                      on='NIF', how='left')

        tabla_matrix = tabla_matrix.withColumn('real_risk_fill', when((col('real_risk').isNull()), 0).otherwise(1))
        tabla_matrix = tabla_matrix.drop('real_risk').withColumnRenamed('real_risk_fill', 'real_risk')
        scoreAndLabels = tabla_matrix.select([tabla_matrix.risk.cast(DoubleType()), tabla_matrix.real_risk.cast(DoubleType())]).rdd.map(tuple)

        metrics = MulticlassMetrics(scoreAndLabels)
        confusionMatrix = metrics.confusionMatrix().toArray()

        print >> fileMatrix, '** GLOBAL **'

        #print('******* Confussion Matrix *******')
        print >> fileMatrix,confusionMatrix

        #print('******* Accuracy: ')
        print >> fileMatrix,(confusionMatrix[0, 0] + confusionMatrix[1, 1]) / sum(sum(confusionMatrix)) * 100

        # We add a label to gather the different risks of leaving
        ###########################################################
        column = ['CAR Date', 'Period Lenght', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate']

        dRisk = {'Super High': [8, 9, 10], 'High': [6, 7], 'Medium': [4, 5], 'Low': [1, 2, 3], 'No Risk': [0]}

        lenght = '30'

        nClients = df_scores_decile.count()
        nRequests = df_scores_decile_sel.count()

        dfChurn = pd.DataFrame(data=None, columns=column)

        for key in dRisk.keys():

            nClientes = 0
            nChurn = 0

            for riskLevel in dRisk[key]:

                for decile in df_salida_num:
                    if int(float(decile['decile'])) == riskLevel:
                        nClientes += decile['count']

                for decile2 in df_salida_sel_num:
                    if int(float(decile2['decile'])) == riskLevel:
                        nChurn += decile2['count']

                churnTotal = float(
                    nChurn) / nRequests * 100  # Portability requests in the Risk section / Total of Portability requests
                base = float(nClientes) / nClients * 100
                churnRate = float(
                    nChurn) / nClientes * 100  # Portability requests in the Risk section / Total of clients in the section


            dfChurn_tmp = pd.DataFrame([[start_date, lenght, key, churnTotal, base, churnRate]], columns=column)
            dfChurn = dfChurn.append(dfChurn_tmp)

        dfChurn['Valor'] = dfChurn['Base'] / 100 * dfChurn['Churn Rate'] / 100

        dfLift = pd.DataFrame(data=None)

        for rr in dfChurn['CAR Date'].unique():
            dfFin = dfChurn[dfChurn['CAR Date'] == rr]
            total = dfFin['Valor'].sum()

            dfFin['Lift'] = (dfFin['Churn Rate'] / total) / 100
            dfLift = dfLift.append(dfFin)

            print(dfLift)

        listas = [df_salida_sel_num, df_salida_num]
        dfNumeros = pd.DataFrame(data=None)

        for nLista, lista in enumerate(listas):

            if nLista == 1:
                colName = 'Num_Customers'
            else:
                colName = 'Num_Churners'

            df = pd.DataFrame(lista, columns=['Decile', colName])
            df.replace(0, -1, inplace=True)

            df_total = pd.DataFrame(np.array([[0, df[colName].sum()]]), columns=['Decile', colName])
            df_conj = df.append(df_total).sort_values('Decile')

            if nLista == 0:
                dfNumeros = pd.concat([dfNumeros, df_conj], axis=1)
            else:
                dfNumeros = pd.concat([dfNumeros, df_conj[colName]], axis=1)
        print(dfNumeros)

        # Saving the results into a csv
        ###################################
        if sys.argv[3] == 'Yes' or sys.argv[3] == 'yes' or sys.argv[3] == 'Y' or sys.argv[3] == 'y':
            writer = pd.ExcelWriter(filename_excel, engine='xlsxwriter')

            dfNumeros.to_excel(writer, startcol=0, index=False)
            dfLift.to_excel(writer, startcol=6, index=False)

            writer.close()

            os.system('chmod +777 ' + filename_excel)

        ## We plot the evolution of the results
        ##########################################

        plotResults(lenght, OUTPUTFILE)

        spark.stop()
        print('The comparisson has finished')

        fileMatrix.close()

