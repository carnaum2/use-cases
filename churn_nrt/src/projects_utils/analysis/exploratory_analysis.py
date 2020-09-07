#!/usr/bin/env python
# -- coding: utf-8 --

import numpy as np
import pandas as pd
from datetime import datetime
import subprocess

from churn_nrt.src.utils.pyspark_utils import *

from pyspark.sql.functions import (col,
                                    randn,
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
                                    stddev,
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

from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, LongType,FloatType,TimestampType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics,MulticlassMetrics


def attributes_exploration (spark, df_data, columnas_sel, top_percentiles=None, saving_file=None, percentile_inc=5, nb_buckets=100):

    '''
    Function to get a general description of a variable
    - dfData: dataframe with the structure: Feature1,Feature2,Feature3,label
    - columnasSel: list of attributes in the dataFrame that will be analysed
    - topPercentiles: list of selected percentiles to save the parameters associated to them
    - savingfile: file to save the results.
    - percentileInc: increment in percentile when looking for the F1 value
    - nBuckets: number of buckets to distribute the data into

    '''

    if not top_percentiles:
        top_percentiles = [5,10,20,30]


    from collections import OrderedDict

    dfs = []

    for cc in columnas_sel:

        # OrderedDict so that the columns of the resulting dataframe keep the order
        feat_dict = OrderedDict()

        print(cc)

        feat_dict['Feature'] = cc
        feat_dict['Mean'] = df_data.agg(sql_avg(col(cc))).collect()[0]["avg({})".format(cc)]
        feat_dict['Std'] = df_data.agg(stddev(col(cc))).collect()[0]["stddev_samp({})".format(cc)]
        feat_dict['Skewness'] = df_data.agg(skewness(col(cc))).collect()[0]["skewness({})".format(cc)]
        feat_dict['Kurtosis'] = df_data.agg(kurtosis(col(cc))).collect()[0]["kurtosis({})".format(cc)]

        for ll in np.arange(2):
            feat_dict['Avg_label' + str(ll)] = df_data.filter(col('label') == ll).agg(sql_avg(col(cc))).collect()[0]["avg({})".format(cc)]

        scoreAndLabels = df_data.select([cc, 'label']).rdd.map(lambda r: (float(r[cc]), r['label']))
        AUC = BinaryClassificationMetrics(scoreAndLabels).areaUnderROC
        if AUC < 0.50:
            feat_dict['AUC'] = 1 - AUC
        else:
            feat_dict['AUC'] = AUC

        # Assumption: avg_label1 for cc > avg_label0 for cc. If not, multiply by -1 the column
        if feat_dict['Avg_label0'] > feat_dict['Avg_label1']:
            dfDataSel = df_data.select(['label', cc]).withColumn(cc + '_inv', col(cc) * (-1.0))
            dfDataSel = dfDataSel.drop(cc).withColumnRenamed(cc + '_inv', cc)
            feat_dict['Direction']='Negative'
        else:
            dfDataSel = df_data.select(['label', cc])
            feat_dict['Direction']='Positive'

        dfDataSelNoisy = dfDataSel.withColumn(cc + '_rand', col(cc) + (randn() * 0.0001).cast(FloatType()))

        discretizer = QuantileDiscretizer(numBuckets=100, inputCol=cc + '_rand', outputCol=cc + "_bucketed",relativeError=0)
        bucketedData = discretizer.fit(dfDataSelNoisy).transform(dfDataSelNoisy)
        bucketedData = bucketedData.cache()
        dfAgg_bucketed = bucketedData.groupBy(cc + "_bucketed").agg(sql_count("*").alias("num_customers"), sql_sum('label').alias('num_churners'))

        windowval = (Window.orderBy(desc(cc + '_bucketed')).rangeBetween(Window.unboundedPreceding, 0))
        result_df = (dfAgg_bucketed \
                     .withColumn('cum_num_churners', sql_sum('num_churners').over(windowval)) \
                     .withColumn('cum_num_customers', sql_sum('num_customers').over(windowval)) \
                     .withColumn('churn', col('num_churners') / col('num_customers')) \
                     .withColumn('cum_churn', col('cum_num_churners') / col('cum_num_customers')))

        feat_dict['churnTop10'] = result_df.filter(col(cc + '_bucketed') == 90).select('churn').collect()[0]['churn']
        feat_dict['cumChurnTop10'] = result_df.filter(col(cc + '_bucketed') == 90).select('cum_churn').collect()[0]['cum_churn']

        feat_dict['churnTop5'] = result_df.filter(col(cc + '_bucketed') == 95).select('churn').collect()[0]['churn']
        feat_dict['cumChurnTop5'] = result_df.filter(col(cc + '_bucketed') == 95).select('cum_churn').collect()[0]['cum_churn']

        if len(bucketedData.select(cc + '_bucketed').distinct().collect()) == nb_buckets:

            Volumen = []
            F1 = []
            Recall = []
            Precision = []

            percentiles = np.arange(nb_buckets, 0, -percentile_inc)

            for pp in percentiles:

                print('Percentil:', pp)

                dfDecile = bucketedData.withColumn(cc + '_risk', when(col(cc + '_bucketed') >= 100-pp, 1).otherwise(0))

                scoreAndLabels = dfDecile.select([dfDecile[cc + '_risk'].cast(DoubleType()), dfDecile.label.cast(DoubleType())]).rdd.map(tuple)

                metrics = MulticlassMetrics(scoreAndLabels)

                F1.append(metrics.fMeasure(1.0, 1.0))
                Recall.append(metrics.recall(1))
                Precision.append(metrics.precision(1))
                Volumen.append(dfDecile.filter(col(cc + '_risk') == 1).count())

            ind = F1.index(np.max(F1))

            feat_dict['F1_max'] = F1[ind]
            feat_dict['Percentil_F1_max'] = percentiles[ind]#100 - percentiles[ind]
            feat_dict['Recall_Topmax'] = Recall[ind]
            feat_dict['Precision_Topmax'] = Precision[ind]
            feat_dict['Volumen_Topmax'] = Volumen[ind]

            if percentiles[ind] == 100:
                dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand')
            else:
                if feat_dict['Avg_label0'] > feat_dict['Avg_label1']:
                    dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand').filter(col(cc + '_bucketed') == percentiles[ind])
                else:
                    dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand').filter(col(cc + '_bucketed') == 100-percentiles[ind])

            feat_dict['ValorVar_Topmax']=abs(dfDecileBucket.agg(sql_min(col(cc + '_rand'))).collect()[0]["min({})".format(cc + '_rand')])

            for vval in top_percentiles:

                ind = np.where(percentiles == (vval))[0][0]  # (100 - vval))[0][0]
                feat_dict['F1_Top' + str(vval)] = F1[ind]
                feat_dict['Percentil_F1_Top' + str(vval)] = percentiles[ind]  # 100 - percentiles[ind]
                feat_dict['Recall_Top' + str(vval)] = Recall[ind]
                feat_dict['Precision_Top' + str(vval)] = Precision[ind]
                feat_dict['Volumen_Top' + str(vval)] = Volumen[ind]

                if percentiles[ind] == 100:
                    dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand')
                else:
                    if feat_dict['Avg_label0'] > feat_dict['Avg_label1']:
                        dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand').filter(col(cc + '_bucketed') == percentiles[ind])
                    else:
                        dfDecileBucket = bucketedData.select(cc + '_bucketed', cc + '_rand').filter(col(cc + '_bucketed') == 100 - percentiles[ind])
                feat_dict['ValorVar_Top' + str(vval)] = abs(dfDecileBucket.agg(sql_min(col(cc + '_rand'))).collect()[0]["min({})".format(cc + '_rand')])

        dfs.append(pd.DataFrame([feat_dict]))


    # For performance, wait until all the dataframes are build to create the complete Pandas dataframe
    cols = feat_dict.keys()
    dfSalida = pd.concat(dfs)
    print(len(dfs))
    print(dfSalida)
    dfSalidaSpark = spark.createDataFrame(dfSalida).select(*cols)

    if saving_file:
        dfSalidaSpark.repartition(1).write.mode("overwrite").format("parquet").save(saving_file)
        print("Saved in '{}'".format(saving_file))
    else:
        print("Skipped saving in '{}'".format(saving_file))

    return dfSalidaSpark



if __name__ == "__main__":



    from churn.utils.general_functions import init_spark
    spark = init_spark("run_segment_orders")

    import numpy as np

    # - - - - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - - - - -

    gaussiana0 = np.random.normal(0, 1, 12000)
    gaussiana1 = np.random.normal(5, 1, 12000)

    gaus0 = spark.createDataFrame(pd.DataFrame(data=gaussiana0, columns=['Att1']))
    gaus1 = spark.createDataFrame(pd.DataFrame(data=gaussiana1, columns=['Att1']))

    gaus = gaus0.withColumn('label', lit(0.)).union(gaus1.withColumn('label', lit(1.)))
    gaus.show()

    dfSalidaGaus = attributes_exploration(spark, gaus, ['Att1'])

    # - - - - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - - - - -

    uniforme0 = np.random.uniform(3, 10, 12000)
    uniforme1 = np.random.uniform(0, 5, 12000)

    unif0 = spark.createDataFrame(pd.DataFrame(data=uniforme0, columns=['Att1']))
    unif1 = spark.createDataFrame(pd.DataFrame(data=uniforme1, columns=['Att1']))

    unif = unif0.withColumn('label', lit(0.)).union(unif1.withColumn('label', lit(1.)))
    unif.show()

    dfSalidaUnif = attributes_exploration(spark, unif, ['Att1'])

    # - - - - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - - - - -

    constante0 = np.full((1, 12000), 10)[0]
    constante1 = np.full((1, 12000), 10)[0]

    const0 = spark.createDataFrame(pd.DataFrame(data=constante0, columns=['Att1']))
    const1 = spark.createDataFrame(pd.DataFrame(data=constante1, columns=['Att1']))

    const = const0.withColumn('label', lit(0.)).union(const1.withColumn('label', lit(1.)))
    const.show()

    dfSalidaConst = attributes_exploration(spark, const, ['Att1'])

