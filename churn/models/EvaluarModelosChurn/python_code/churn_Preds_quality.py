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
from pyspark.sql.functions import col, lit, substring, asc, udf, datediff, row_number, concat, when, desc,floor
from pyspark.sql.types import StringType,FloatType,DoubleType,IntegerType,StructType,StructField,LongType

import os, sys, glob, stat
from dateutil.relativedelta import relativedelta
from pyspark.ml.feature import QuantileDiscretizer

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

##################################################
### Getting the Portability Requests table
##################################################
def get_end_port(self):
    if YAML_END_PORT in self.get_config_dict():
        return str(self.get_config_dict()[YAML_END_PORT])
    return None


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
    # .withColumn("portout_date", substring(col("portout_date"), 0, 10))
    # .select("msisdn_a", "sopo_ds_msisdn2", "sopo_msisdn_authorizada", "portout_date", "SOPO_CO_RECEPTOR")
    # .withColumn("portout_date", convert_to_date_udf(col("portout_date")))
    # .withColumn("ref_date",
    #          convert_to_date_udf(concat(lit(ref_date[:4]), lit(ref_date[4:6]), lit(ref_date[6:]))))
    # .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
    # .withColumn("rank", row_number().over(window))
    # .where(col("rank") == 1))

    # SOPO_DS_MSISDN1=>msisdn_a
    # SOPO_DS_MSISDN1=>NIF
    # SOPO_DS_FECHA_SOLICITUD=>portout_date

    return df_mobport.select(["portout_date", "msisdn_a", "NIF"])


######################################################################
### Getting the tables with the churn scores calculated by the model
######################################################################
def getScores(start_date):

    df_scores_onlymob = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_onlymob/').where(
        col('predict_closing_date') == start_date)
    df_scores_conv = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_mobileandfbb/').where(
        col('predict_closing_date') == start_date)
    df_scores_others = spark.read.parquet(
        '/data/attributes/vf_es/model_outputs/model_scores/model_name=churn_preds_others/').where(
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


## Fuction to mix the ordered score tables of onlymob and mobileandfbb: 3 record of onlymob + 1 record mobileandfbb
def orderScores(df_scores_onlymob,df_scores_conv):
#
    #pred_name = "preds_onlymob_for20181121_on20181201_121058"
    #table_name = "tests_es.jvmm_amdocs_automated_churn_scores_onlymob"
    #df_scores_onlymob = spark.read.table(table_name).where(col("pred_name") == preds_onlymob)
    df_scores_onlymob = df_scores_onlymob.withColumn("model_output", col("model_output").cast(FloatType()))
    df_scores_onlymob = df_scores_onlymob.orderBy('model_output', ascending=False)
    df_scores_onlymob = df_scores_onlymob.withColumn("segment", lit("onlymob"))
    df_scores_onlymob = add_column_index(spark, df_scores_onlymob, offset=1, colName="idx")
    df_scores_onlymob = df_scores_onlymob.withColumn("new_idx", 4 * col("idx"))

   # pred_name = "preds_mobileandfbb_for20181121_on20181201_230314"
    #table_name = "tests_es.jvmm_amdocs_automated_churn_scores_mobileandfbb"
    #df_scores_conv = spark.read.table(table_name).where(col("pred_name") == preds_mobileandfbb)
    df_scores_conv = df_scores_conv.withColumn("model_output", col("model_output").cast(FloatType()))
    df_scores_conv = df_scores_conv.orderBy('model_output', ascending=False)
    df_scores_conv = add_column_index(spark, df_scores_conv, offset=1, colName="idx")
    df_scores_conv = df_scores_conv.withColumn("segment", lit("conv"))
    df_scores_conv = df_scores_conv.withColumn("new_idx", when(col("idx") % 3 == 0,
                                                               col("idx") + floor(col("idx") / 3) - 1).otherwise(
        col("idx") + floor(col("idx") / 3)))

    # from pykhaos.utils.pyspark_utils import union_all
    df_scores_incidencias = union_all([df_scores_conv, df_scores_onlymob])
    df_scores_incidencias = df_scores_incidencias.drop("idx")
    df_scores_incidencias = df_scores_incidencias.orderBy('new_idx', ascending=True)
    df_scores_incidencias = add_column_index(spark, df_scores_incidencias, offset=1,
                                             colName="idx")  # compute a seq index after union
    df_scores_incidencias = df_scores_incidencias.drop("new_idx")

    df_scores_incidencias = df_scores_incidencias.withColumn("idx", col("idx").cast(DoubleType()))
    df_scores_incidencias = df_scores_incidencias.select("msisdn", "model_output", "idx", "segment")

    return df_scores_incidencias #.show(50, False)

#############################################################################################################################
### Divide the table with the score values fromt he model in 2 groups: Risk-No Risk. And the Risk group into 10 deciles
#############################################################################################################################
def add_decile(df, perc=0.35):
    df = df.withColumn('risk', lit(0))
    maximo = df.agg({"idx": "max"}).collect()[0][0]
    df = df.withColumn('risk', when(col('idx') < maximo * perc, 1).otherwise(0))

    df_risk = df.where(col('risk') == 1).withColumn('score_decile', col('idx').cast(DoubleType()))
    df_norisk = df.where(col('risk') == 0).withColumn(DECILE_COL, lit(-1.0))

    df_risk = df_risk.withColumn('idx2', df_risk.count() - df_risk['idx'])
    df_risk = df_risk.withColumn("idx2", col("idx2").cast(DoubleType()))

    discretizer = QuantileDiscretizer(numBuckets=10, inputCol='idx2', outputCol=DECILE_COL, relativeError=0)
    df_risk = discretizer.fit(df_risk).transform(df_risk)

    df_risk = df_risk.drop('score_decile')
    df_risk = df_risk.drop('idx2')

    df_scores = union_all([df_risk, df_norisk])
    df_scores = df_scores.withColumn(DECILE_COL, col(DECILE_COL) + 1)
    df_scores = df_scores.drop('risk')

    return df_scores

#######################
# General functions
#######################
def convert_to_date(dd_str):
    import datetime as dt
    if dd_str in [None, ""] or dd_str != dd_str: return None

    dd_obj = dt.datetime.strptime(dd_str.replace("-", "").replace("/", ""), "%Y%m%d")
    if dd_obj < dt.datetime.strptime("19000101", "%Y%m%d"):
        return None

    return dd_obj.strftime("%Y-%m-%d %H:%M:%S") if dd_str and dd_str == dd_str else dd_str

def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]

def add_column_index (spark, df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(colName,LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda (row,rowId): ([rowId +offset] + list(row)))

    return spark.createDataFrame(new_rdd, new_schema)


def plotResults(lenght,OUTPUTFILE):
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
            plt.savefig(OUTPUTFILE+'Analysis_' + var.replace(' ', '_') + '_'+str(lenght)+'days.jpeg',dpi=300)

            imgdata = BytesIO()

            fig.savefig(imgdata)

OUTPUTFILE= '/var/SP/data/bdpmdses/deliveries_churn/model_evaluation/'
FILENAME_EXCEL= OUTPUTFILE + 'Churn_Predictions_{}_{}_{}.xlsx'

if __name__ == "__main__":

    # python churn_Preds_quality.py 20181122 30 Yes


    if len(sys.argv[1:]) != 3:

        print 'ERROR: This program takes 3 input arguments:', '\n', \
            '- Initial day of the comparisson period', '\n', \
            '- Lenght of the comparisson period (in days)', '\n', \
            '- Would you like to save the results? Yes or No'

    #elif sys.argv[3].split('_')[2][3:]!=sys.argv[4].split('_')[2][3:]:
    #    print ('**** Warning ****  You are not using the same dates for the input files')

    else:
        start_date = sys.argv[1]
        lenght = sys.argv[2]

        today=time.strftime("%Y%m%d")

        print("******************************************************************************************")
        print("Comparing the Portability Requests from {} during a period of {} days".format(start_date, lenght))
        print('******************************************************************************************')

        CAR_Date = start_date

        # We check if there is an existing file with model comparissons or if the provided date has been processed already
        #####################################################################################################################

        filename_excel= FILENAME_EXCEL.format(str(CAR_Date),str(lenght),str(today))
        column = ['CAR Date','Period Lenght', 'Risk Label', 'Total Churn', 'Base', 'Churn Rate']

        doneDates=[]
        for file in glob.glob(OUTPUTFILE + "*.xlsx"):
            doneDates.append(file.split('/')[-1].split('_')[2])
        print(doneDates)

        for date in doneDates:
            if str(date) == str(start_date):
                print(date,'**** There is already a churn record for the starting date provided ****')
                sys.exit()

        doneDates.append(start_date)

        # Starting Spark Session
        #########################
        spark = initialize("churn Preds quality")

        # Getting Portability Requests
        ##################################
        # We consider a month after the input date (the day of the picture)

        PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"
        ref_date=None
        config_obj=None

        # We take portability requests for 30 days after the starting date provided as the comparisson period
        end_date = datetime.datetime.strptime(start_date, '%Y%m%d') + datetime.timedelta(int(lenght))
        end_date =str(datetime.datetime.strftime(end_date, '%Y%m%d'))

        dfRequests = obtenerTabla(spark, config_obj, start_date, end_date, ref_date)

        from pyspark.sql.functions import unix_timestamp, from_unixtime, date_format

        dfRequests = dfRequests.orderBy('portout_date', ascending=True).drop_duplicates(subset=['msisdn_a'])
        # El msisdn_a es el msisdn_anonimizado y se corresponde con el msisdn de la tabla de servicios.
        # En esa misma tabla el valor de campo 2 es el que se corresponde con el msisdn de la tabla de Fede
        dfRequests = dfRequests.orderBy('portout_date', ascending=True)

        # # Getting churn model scores
        # ################################
        if datetime.datetime.strptime(start_date, '%Y%m%d') == datetime.datetime.strptime('20181221', '%Y%m%d'):
            unifiedT='No'

        elif datetime.datetime.strptime(start_date, '%Y%m%d') == datetime.datetime.strptime('20181231', '%Y%m%d'):
            unifiedT='No'

        else: unifiedT='Yes'

        df_scores_onlymob, df_scores_conv,df_scores_others=getScores(start_date)#preds_onlymob,preds_mobileandfbb,unifiedT)

        # Before this date the tables of onlymob and mobileandfbb were mixed 3-1
        # after the scores are calibrated and the two tables can just be joined

        fecha_lim = datetime.datetime.strptime('20181214', '%Y%m%d')
        if datetime.datetime.strptime(start_date, '%Y%m%d') > fecha_lim:
            df_scores=union_all([df_scores_conv, df_scores_onlymob,df_scores_others])
            df_scores = df_scores.orderBy('model_output', ascending=False)
            df_scores=add_column_index(spark, df_scores, offset=1,colName="idx")
        else:
            df_scores = orderScores(df_scores_onlymob, df_scores_conv)

        df_scores = df_scores.drop_duplicates(subset=['msisdn'])

        # We classify the clients in deciles to measure its risk of leaving
        ######################################################################
        DECILE_COL = "comb_decile"

        df_scores_deciles = add_decile(df_scores)
        df_score_decile_sel = dfRequests.join(df_scores_deciles, on=(dfRequests["msisdn_a"] == df_scores_deciles["msisdn"]),
                                              how="inner")

        num = df_scores_deciles.groupby('comb_decile').count().collect()
        df_salida_num = df_scores_deciles.groupby('comb_decile').count().collect()
        df_salida_sel_num = df_score_decile_sel.groupby('comb_decile').count().collect()

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
                    if decile['comb_decile'] == riskLevel:
                        nClientes += decile['count']
                        print(riskLevel, decile['count'])

                for decile2 in df_salida_sel_num:
                    if decile2['comb_decile'] == riskLevel:
                        nChurn += decile2['count']
                        print(riskLevel, decile2['count'])

                churnTotal = float(nChurn) / nRequests * 100  #Portability requests in the Risk section / Total of Portability requests
                base = float(nClientes) / nClients * 100
                churnRate = float(nChurn) / nClientes * 100 #Portability requests in the Risk section / Total of clients in the section

            print(nClientes, nChurn)
            print(key, '->', 'Churn Total:', churnTotal, 'Base:', base, 'Churn Rate:', churnRate)

            dfChurn_tmp = pd.DataFrame([[CAR_Date,lenght,key,churnTotal,base,churnRate]], columns=column)
            dfChurn=dfChurn.append(dfChurn_tmp)

        dfChurn['Valor'] = dfChurn['Base'] / 100 * dfChurn['Churn Rate'] / 100

        dfLift = pd.DataFrame(data=None)

        for rr in dfChurn['CAR Date'].unique():
            dfFin = dfChurn[dfChurn['CAR Date'] == rr]
            total = dfFin['Valor'].sum()

            dfFin['Lift'] = (dfFin['Churn Rate'] / total) / 100
            dfLift = dfLift.append(dfFin)

        print('*****',dfLift)

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

            df_conj = df_conj.reset_index()

            if nLista == 0:
                dfNumeros = pd.concat([dfNumeros, df_conj], axis=1,ignore_index=True)
            else:
                dfNumeros = pd.concat([dfNumeros, df_conj[colName]], axis=1,ignore_index=True)

            dfNumeros = dfNumeros.reset_index()
        print(dfNumeros)

        # Saving the results into a csv
        ####################################
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

