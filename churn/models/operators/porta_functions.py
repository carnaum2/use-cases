from pykhaos.utils.date_functions import get_last_day_of_month, move_date_n_yearmonths,get_next_month, convert_to_date
from churn.analysis.triggers.base_utils.base_utils import get_customer_base, _is_null_date, get_customers, get_services
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, StructType, StructField, FloatType
from pyspark.sql.functions import col, when, min as sql_min, max as sql_max, row_number, avg as sql_avg
from churn_nrt.src.projects_utils.models.modeler import get_split
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler, Bucketizer
from pyspark.ml import Pipeline
import pandas as pd
from pyspark.ml.regression import IsotonicRegression
import numpy as np


def get_active_cust_three_months(date,spark):
    """
    Given a str date 'YYYMMDD', returns customers (msisdn) who were active three months ago
    """

    previous_year_month = move_date_n_yearmonths(date[0:6], -3)  # return 'YYYYMM' of three months ago

    if ((date[6] == '3') | (date[6:8] == '28')):  # if the day of the current date is the last day of the month

        previous_date = get_last_day_of_month(previous_year_month + '01')  # returns last day of three months ago

    else:

        previous_date = previous_year_month + date[6:8]  # returns same day of three months ago

    active_customers = get_customer_base(spark, previous_date).select('msisdn').distinct()  # get active clients three months ago

    return active_customers


#churn_customers=get_top_churn_customers('20190731',spark,100000)


def get_ids(date, spark):
    '''
    Extract complete ids of a given date
    '''

    year = date[0:4]

    if date[4] == '0':

        month = date[5]

    else:

        month = date[4:6]

    day = date[6:8]

    ids_complete = (spark.read.load(
        '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year + '/month=' + month + '/day=' + day))

    return ids_complete


def get_ids_filtered(date, spark):
    '''
    Extract ids for training set
    Filters: mobile, active for 3 months
    '''

    ids_complete = get_ids(date, spark)


    ids_filtered = ids_complete.filter((col('Serv_RGU') == 'mobile') &
                                       ((col('Cust_COD_ESTADO_GENERAL') == '01') | (
                                                   col('Cust_COD_ESTADO_GENERAL') == '09')))

    print('[Info]: IDS filtered by mobile and active customers')

    active_cust = get_active_cust_three_months(date,spark)  # get customers who were active three months ago

    ids_final = ids_filtered.join(active_cust, on='msisdn',
                                  how='inner')  # select customers who were active three months ago from the ids

    print('[Info]: IDS filtered by customers active 3 months')

    return ids_final


def get_ids_test_production(date, spark):
    '''
    Extract ids for test set: clients who are in the churn model scores table
    Extract scores table
    '''

    ids_complete = get_ids(date, spark)

    scores = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/')
    scores_test=scores.filter(col('predict_closing_date')==date)
    test_clients=scores_test.select('msisdn')
    ids_test = ids_complete.join(test_clients,on='msisdn',how='inner')

    print('[Info]: IDS for test set obtained')

    return ids_test, scores_test


def get_next_month_date(date):
    """
    Given a str date 'YYYMMDD', returns a str date 'YYYMMDD' of one month later
    """

    year_month = date[0:6]

    year_month_new = get_next_month(year_month)  # returns YYYMM of next month

    if ((date[6] == '3') | (date[6:8] == '28')):  # if the day of the current date is the last day of the month

        date_new = get_last_day_of_month(year_month_new + '01')  # returns last day of the next month

    else:
        date_new = year_month_new + date[6:8]  # returns same day of the next month

    return date_new


def get_portab_next_month(date, spark):
    """
    Given a date, returns portabilities over the next month
    """

    df_portab_complete = spark.read.table(
        'raw_es.portabilitiesinout_sopo_solicitud_portabilidad')  # lectura de tabla de portabilidad

    next_month_date = get_next_month_date(date)  # get date specified plus one month

    next_month_date = convert_to_date(next_month_date)  # convert date to datetime format

    current_date = convert_to_date(date)  # convert current date to datetime format

    df_portab_next_month = df_portab_complete.filter((col('SOPO_DS_FECHA_SOLICITUD') <= next_month_date) &
                                                     (col(
                                                         'SOPO_DS_FECHA_SOLICITUD') >= current_date))  # select portabilities between the period specified

    # If the same msisdn has requested a portability more than once, we drop the last request

    window = Window.partitionBy('SOPO_DS_MSISDN1').orderBy(col('SOPO_DS_FECHA_SOLICITUD').desc())
    df_portab_final = df_portab_next_month.withColumn("rank", row_number().over(window)).filter(col("rank") == 1)

    return df_portab_final


def get_portab_labels(portab_complete, df_no_labelled):
    portab_complete = portab_complete.withColumn('Operador_target', when(
        (col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735014")
        | (col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735044")
        | (col("SOPO_CO_RECEPTOR") == "AIRTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "725303")
        | (col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735054")
        | (col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "715501")
        | (col("SOPO_CO_RECEPTOR") == "YOIGO") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")
        | (col("SOPO_CO_RECEPTOR") == "AIRTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "725503")
        | (col("SOPO_CO_RECEPTOR") == "VIZZAVI") & (col("SOPO_CO_NRN_RECEPTORVIR") == "970513")
        | (col("SOPO_CO_RECEPTOR") == "AIRTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "725203")
        | (col("SOPO_CO_RECEPTOR") == "VIZZAVI") & (col("SOPO_CO_NRN_RECEPTORVIR") == "970213"), 1).otherwise(
        when((col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")
             | (col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "715401")
             | (col("SOPO_CO_RECEPTOR") == "TUENTI_MOVIL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), 2).otherwise(

            when((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")
                 | (col("SOPO_CO_RECEPTOR") == "JAZZTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")
                 | (col("SOPO_CO_RECEPTOR") == "EPLUS") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), 3).otherwise(4))))

    portab_final = portab_complete.select('SOPO_DS_MSISDN1', 'Operador_target')

    df_labelled = df_no_labelled.join(portab_final, on=portab_final.SOPO_DS_MSISDN1 == df_no_labelled.msisdn,
                                      how='left')

    df_labelled = df_labelled.fillna(0, subset='Operador_target')

    df_labelled = df_labelled.filter(col('Operador_target') != 0)

    df_labelled_complete = df_labelled.withColumn('masmovil',
                                                  when(df_labelled['Operador_target'] == 1, 1).otherwise(0)).withColumn(
        'movistar', when(df_labelled['Operador_target'] == 2, 1).otherwise(0)).withColumn('orange', when(
        df_labelled['Operador_target'] == 3, 1).otherwise(0)).withColumn('otros',
                                                                         when(df_labelled['Operador_target'] == 4,
                                                                              1).otherwise(0))

    return df_labelled_complete


def indexer_assembler(df_no_transformed, variables):

    assembler = VectorAssembler(inputCols=variables, outputCol="features")

    stages = [assembler]

    pipeline = Pipeline(stages=stages)

    pipeline_fit = pipeline.fit(df_no_transformed)

    df_transformed = pipeline_fit.transform(df_no_transformed)

    return df_transformed



def preparation(operator, train_set, test_set, var_elim):

    variables = [i for i in train_set.columns if i not in var_elim]  # cojo las variables predictoras para el assemble

    train_final = indexer_assembler(train_set, variables).withColumnRenamed(operator, 'label')

    print('[Info]: Assembler for train set finished')

    test = indexer_assembler(test_set, variables).withColumnRenamed(operator, 'label')

    print('[Info]: Assembler for test set finished')

    train_unbal, train, validation = get_split(train_final)

    print('[Info]: Balancing and splitting finished')

    return train_unbal, train, validation, test


def preparation_production(operator, train_set, test_set, var_elim):

    variables = [i for i in train_set.columns if i not in var_elim]  # cojo las variables predictoras para el assemble

    train_final = indexer_assembler(train_set, variables).withColumnRenamed(operator, 'label')

    print('[Info]: Assembler for train set finished')

    test = indexer_assembler(test_set, variables)

    print('[Info]: Assembler for test set finished')

    train_unbal, train, validation = get_split(train_final)

    print('[Info]: Balancing and splitting finished')

    return train_unbal, train, validation, test


def get_predictions(model,train_set,test_set,validation_set,getScore,operator):


    pred_train=model.transform(train_set).withColumn("score", getScore(col("probability")).cast(DoubleType())).orderBy('score',ascending=False).withColumnRenamed('label',operator)
    pred_test=model.transform(test_set).withColumn("score", getScore(col("probability")).cast(DoubleType())).orderBy('score',ascending=False).withColumnRenamed('label',operator)
    pred_validation=model.transform(validation_set).withColumn("score", getScore(col("probability")).cast(DoubleType())).orderBy('score',ascending=False).withColumnRenamed('label',operator)

    return pred_train,pred_test,pred_validation


# Cargamos predicciones guardadas y las ponemos como un dataframe de spark (las de agosto-sept)

def read_preds(spark, file_csv, operador):

    preds = pd.read_csv(file_csv)[["msisdn", "score", "prediction", operador]]

    schema = StructType([
        StructField("msisdn", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("prediction", DoubleType(), True),
        StructField(operador, IntegerType(), True)])

    preds = spark.createDataFrame(preds, schema=schema)

    return preds


def get_calibration_function(spark, valpredsdf, labelcol, numpoints=10):

    valpredsdf = valpredsdf.select(['score', labelcol])
    # min and max values of the obtained preds
    minprob = valpredsdf.select(sql_min('score').alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max('score').alias('max')).rdd.collect()[0]['max']

    # step size to discetize the model output by using the number of bins (points) specified in "numpoints"
    delta = float(maxprob - minprob) / float(numpoints)

    # calculating the splits for the bins
    splits = list(
        [-float("inf")] + [(minprob + i * delta) for i in list(np.arange(1, numpoints, step=1))] + [float("inf")])

    # calculating the mid points for the bins
    midpoints = {float(i): ((minprob + 0.5 * delta) + i * delta) for i in list(np.arange(0, numpoints, step=1))}

    # model_score_discrete: from 0 to numpoints - 1
    bucketizer_prob = Bucketizer(splits=splits, inputCol='score', outputCol='score_discrete')

    # dictionary --> bucket: percentage of samples with label=1; calculating the percentage of samples (probability) with label = 1 for each bin
    freqprob = bucketizer_prob.transform(valpredsdf).groupBy('score_discrete').agg(sql_avg(labelcol)).rdd.collectAsMap()
    for k in np.arange(0, numpoints):
        if not (k in freqprob):
            freqprob[k] = 0

    # samplepoints --> examples of the type: model_output (bin mid), proportion of target 1
    for (k, v) in freqprob.items():
        print "[Info Calibration] freqprob: key = " + str(k) + " - value = " + str(v)
    for (k, v) in midpoints.items():
        print "[Info Calibration] midpoints: key = " + str(k) + " - value = " + str(v)

    # in samplepoints, we have the pairs used to fit the calibration function
    samplepoints = [(float(midpoints[i]), float(freqprob[i])) for i in list(freqprob.keys())]
    for pair in samplepoints:
        print "[Info Calibration] " + str(pair[0]) + ", " + str(pair[1])

    # dataframe with the pairs
    sampledf = spark.createDataFrame(samplepoints, ['score', 'target_prob'])
    sampledf.show()

    # fitting an isotonic regression model to the obtained pairs
    ir = IsotonicRegression(featuresCol='score', labelCol='target_prob', predictionCol='calib_model_score')
    irmodel = ir.fit(sampledf)

    return (irmodel, samplepoints)



