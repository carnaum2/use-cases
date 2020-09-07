from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, substring, asc, udf, datediff, row_number, concat, when, desc, substring_index
from pyspark.sql.types import StringType,FloatType,DoubleType,IntegerType,StructType,StructField,LongType

import os, sys, glob
from dateutil.relativedelta import relativedelta
from pyspark.ml.feature import QuantileDiscretizer

from pyspark.sql.functions import floor
# from datetime import datetime
from shutil import copyfile
import pandas as pd
import datetime

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
    sc, spark, sql_context = get_spark_session(app_name=app_name, log_level="OFF", min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores,
                             executor_memory = executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark


def plotResults(lenght, OUTPUTFILE):
    # Leemos todos los archivos excel que tenemos para representarlos,
    # si no deseamos que se use alguno de ellos incluirle un prefijo en su nombre
    dfDraw = pd.DataFrame(data=None,columns=['CAR Date', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate', 'Lift'])
    for file in glob.glob(OUTPUTFILE+"Churn*.xlsx"):
        if file.split('/')[-1].split('_')[3] == str(lenght):
            print(file)
            dfs = pd.read_excel(file)
            dfs['CAR Date'] = file.split('/')[-1].split('_')[2]
            dfDraw = dfDraw.append(dfs[['CAR Date', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate', 'Lift']].dropna(0))

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
        if var == 'Total Churn': plt.title('Total Churn: Portability Requests for each Risk Section/Total of Portability Requests')
        if var == 'Lift': plt.title('Lift: Churn rate in the segment/churn rate in the clients database')
        plt.ylabel(var)
        plt.xlabel('Date')
        plt.xticks(rotation=10)
        plt.legend(loc='best')

        if sys.argv[3] == 'Yes' or sys.argv[3] == 'yes' or sys.argv[3] == 'Y' or sys.argv[3] == 'y':
            plt.savefig(OUTPUTFILE+'Analysis_' + var.replace(' ', '_') + '_'+str(int(lenght)/7)+'cycles_fbb.jpeg',dpi=300)

            imgdata = BytesIO()

            fig.savefig(imgdata)

OUTPUTFILE= '/var/SP/data/bdpmdses/deliveries_churn/model_evaluation_fbb/'
FILENAME_EXCEL= OUTPUTFILE + 'Churn_Predictions_{}_{}_{}.xlsx'

if __name__ == "__main__":

    set_paths()

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *

    if len(sys.argv[1:]) != 3:

        print 'ERROR: This program takes 3 input arguments:', '\n', \
            '- Initial day of the comparisson period', '\n', \
            '- Lenght of the comparisson period (in cycles)', '\n', \
            '- Would you like to save the results? Yes or No'

    else:
        start_date = sys.argv[1]
        lenght = sys.argv[2]  ## Ciclos

        today = time.strftime("%Y%m%d")

        print("***********************************************************************************************************")
        print("Comparing the Portability Requests from {} during a period of {} cycles".format(start_date, lenght))
        print('***********************************************************************************************************')

        CAR_Date = start_date

        # We check if there is an existing file with model comparissons or if the provided date has been processed already
        #####################################################################################################################

        filename_excel = FILENAME_EXCEL.format(str(CAR_Date),str(lenght),str(today))
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
        spark = initialize("churn Preds quality fbb")

        cycle = 0
        fini_tmp = start_date
        while cycle < int(lenght)/7:

            yearmonthday_target = get_next_cycle(fini_tmp, str_fmt="%Y%m%d")
            cycle = cycle + 1
            fini_tmp = yearmonthday_target

        yearmonth = start_date[0:6]

        print('***************************',start_date,yearmonthday_target)

        # Getting Portability Requests
        ##############################################
        fixporttr = getFixPortRequestsForCycleList(spark, start_date, yearmonthday_target)
        fixdxtr = getFbbDxsForCycleList(spark,start_date, yearmonthday_target)

        ## Read the CAR to get the num_cliente
        origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'
        clientCARdf = spark \
                .read \
                .parquet(origin + start_date) \
                .filter(col("ClosingDay") == start_date) \
                .repartition(400) \
                .cache()

        fixporttr_numC = fixporttr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'CAMPO1', 'CAMPO2']).distinct(),
                                        fixporttr['msisdn_d'] == clientCARdf['CAMPO2'], how='inner')
        fixdxtr_numC = fixdxtr.join(clientCARdf.select(['msisdn', 'NUM_CLIENTE', 'CAMPO1', 'CAMPO2']), on='msisdn',
                                    how='inner')

        dfRequests = fixporttr_numC.select('NUM_CLIENTE').union(fixdxtr_numC.select('NUM_CLIENTE'))
        dfRequests = dfRequests.na.drop().withColumnRenamed("NUM_CLIENTE", "num_cliente")

        # # Getting churn model scores
        # ################################
        scores = (spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_fbb')
                  .where(col('predict_closing_date') == start_date)
                  )
        df_scores_deciles = scores.select(['scoring', 'extra_info', 'client_id']).withColumn('decile',
                                                                                             substring_index(scores.extra_info, ';',-1))


        df_score_decile_sel = dfRequests.join(df_scores_deciles, dfRequests["num_cliente"] == df_scores_deciles['client_id'],
                                              how="inner")


        df_salida_num = df_scores_deciles.groupby('decile').count().collect()
        df_salida_sel_num = df_score_decile_sel.groupby('decile').count().collect()

        # We add a label to gather the different risks of leaving
        ###########################################################
        dRisk = {'Super High': [8, 9, 10], 'High': [6, 7], 'Medium': [4, 5], 'Low': [1, 2, 3], 'No Risk': [0]}

        nClients = df_scores_deciles.count()
        nRequests = df_score_decile_sel.count()

        dfChurn = pd.DataFrame(data=None, columns=column)

        for key in dRisk.keys():

            nClientes = 0
            nChurn = 0

            for riskLevel in dRisk[key]:

                for decile in df_salida_num:
                    if int(float(decile['decile'])) == riskLevel:
                        nClientes += decile['count']
                        print(riskLevel, decile['count'])

                for decile2 in df_salida_sel_num:
                    if int(float(decile2['decile'])) == riskLevel:
                        nChurn += decile2['count']
                        print(riskLevel, decile2['count'])

                churnTotal = float(
                    nChurn) / nRequests * 100  # Portability requests in the Risk section / Total of Portability requests
                base = float(nClientes) / nClients * 100
                churnRate = float(
                    nChurn) / nClientes * 100  # Portability requests in the Risk section / Total of clients in the section

            print(nClientes, nChurn)
            print(key, '->', 'Churn Total:', churnTotal, 'Base:', base, 'Churn Rate:', churnRate)

            dfChurn_tmp = pd.DataFrame([[CAR_Date, lenght, key, churnTotal, base, churnRate]], columns=column)
            dfChurn = dfChurn.append(dfChurn_tmp)

        dfChurn['Valor'] = dfChurn['Base'] / 100 * dfChurn['Churn Rate'] / 100

        dfLift = pd.DataFrame(data=None)

        for rr in dfChurn['CAR Date'].unique():
            dfFin = dfChurn[dfChurn['CAR Date'] == rr]
            total = dfFin['Valor'].sum()

            dfFin['Lift'] = (dfFin['Churn Rate'] / total) / 100
            dfLift = dfLift.append(dfFin)

            print('*****', dfLift)

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




