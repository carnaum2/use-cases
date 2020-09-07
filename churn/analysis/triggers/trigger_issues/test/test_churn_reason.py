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
                                    regexp_replace,
                                    upper,
                                    trim,
                                    array)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import datetime as dt
from itertools import chain
import numpy as np
import pandas as pd
import re

def get_test_df(spark):

	data = {'msisdn': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'],
	'nif_cliente': ['nif1', 'nif2', 'nif3', 'nif1', 'nif2', 'nif2', 'nif1', 'nif3', 'nif1', 'nif4', 'nif5', 'nif4', 'nif6', 'nif5', 'nif7'],
	'portout_date_mob': ['20190206', '', '', '', '20190710', '', '', '', '', '', '', '', '', '', ''],
	'portout_date_fix': ['', '20190112', '', '20190506', '', '', '', '', '', '', '', '', '', '', ''],
	'portout_date_dx': ['20190405', '', '', '', '20190607', '', '', '', '', '', '', '', '', '', ''],
	'label': [1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}


	# 'portout_date_mob', 'portout_date_fix', 'portout_date_dx'

	pd_df = pd.DataFrame(data)

	df = spark.createDataFrame(pd_df)\
	.withColumn('portout_date_mob', from_unixtime(unix_timestamp(col('portout_date_mob'), 'yyyyMMdd')))\
	.withColumn('portout_date_fix', from_unixtime(unix_timestamp(col('portout_date_fix'), 'yyyyMMdd')))\
	.withColumn('portout_date_dx', from_unixtime(unix_timestamp(col('portout_date_dx'), 'yyyyMMdd')))

	return df

def get_target2(spark, df):


    def get_churn_reason(dates):

        reasons = ['mob', 'fix', 'fbb']

        sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

        reason = reasons[sorted_dates[0]]

        return reason


    get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = df.select("nif_cliente", 'portout_date_mob', 'portout_date_fix', 'portout_date_dx', 'label')\
    .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc))\
    .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc))\
    .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc))\
    .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx'))\
    .withColumn('reason', get_churn_reason_udf(col('dates')))\
    .withColumn('reason', when(col('label')==0.0, '').otherwise(col('reason')))\
    .withColumn('portout_date_min', least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx')))\
    .withColumn('portout_date', sql_min('portout_date_min').over(window_nc))\
    .select("nif_cliente", 'portout_date', 'reason').drop_duplicates()


    return df_target_nifs

if __name__ == "__main__":

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    spark = SparkSession.builder.appName('abc').getOrCreate()

    df = get_test_df(spark)

    df.show()

    target_df = get_target2(spark, df)

    target_df.show()





