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
                        rand,
                        countDistinct,
                        variance)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder, QuantileDiscretizer, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import IsotonicRegression
import math
import operator
import numpy as np

def balance_df(df=None, label=None):

    """
    Balance classes of the given DataFrame

    :param df: A DataFrame to balance classes in label column
    :type df: :class:`pyspark.sql.DataFrame`
    :param label: The label column to balance
    :type label: str

    :return: The given DataFrame balanced
    :rtype: :class:`pyspark.sql.DataFrame`
    """

    # Get counts by label
    #counts = df.groupBy(label).agg(count("*"))
        
    #counts_dict = dict(counts.rdd.map(lambda x: (x[label], x['count'])).collect())

    counts_dict = df.groupBy(label).agg(count('*')).rdd.collectAsMap()

    minor_cat_size = min(counts_dict.values())
        
    # Calculate the rate to apply to classes
    fractions_dict = {}
    for l in counts_dict.keys():
        fractions_dict[l] = float(minor_cat_size)/counts_dict[l]
    

    balanced = df.sampleBy(label, fractions=fractions_dict, seed=1234)
    
    return balanced

def balance_df2(df=None, label=None):

    """
    Balance classes of the given DataFrame

    :param df: A DataFrame to balance classes in label column
    :type df: :class:`pyspark.sql.DataFrame`
    :param label: The label column to balance
    :type label: str

    :return: The given DataFrame balanced
    :rtype: :class:`pyspark.sql.DataFrame`
    """

    counts_dict = df.groupBy(label).agg(count('*')).rdd.collectAsMap()

    minor_cat_size = min(counts_dict.values())
        
    # Calculate the rate to apply to classes
    fractions_dict = {}
    for l in counts_dict.keys():
        fractions_dict[l] = float(minor_cat_size)/counts_dict[l]
    
    balanced = reduce((lambda x, y: x.union(y)), [df.filter(col(label)==k).sample(False, fractions_dict[k]) for k in list(counts_dict.keys())])

    repartdf = balanced.repartition(400)

    print "[Info balance_df2] Dataframe has been balanced - Total number of rows is " + str(repartdf.count())

    # balanced = df.sampleBy(label, fractions=fractions_dict, seed=1234)
    
    return repartdf
    
def addMonth(yearmonth, n):

    month = yearmonth[4:6]

    year = yearmonth[0:4]

    idx = (float(month) + n) > 12

    options={True: ((float(year)+1), (float(month) + n - 12)), False: (float(year), float(month) + n)}

    outyear, outmonth = options[idx]

    resultmonth = ('0' + str(int(outmonth))) if outmonth < 10 else str(int(outmonth))

    resultyear = str(int(outyear))

    return (resultyear + resultmonth)

def substractMonth(yearmonth, n):

    month = yearmonth[4:6]
    
    year = yearmonth[0:4]

    idx = (float(month) - n) <= 0

    options={True: ((float(year) - 1), (float(month) - n + 12)), False: (float(year), float(month) - n)}

    outyear, outmonth = options[idx]

    resultmonth = ('0' + str(int(outmonth))) if outmonth < 10 else str(int(outmonth))

    resultyear = str(int(outyear))

    return (resultyear + resultmonth)

def getMonthSeq(inityearmonth, endyearmonth):

    initmonth = inityearmonth[4:6]

    # inityear = inityearmonth[0:4]

    endmonth = endyearmonth[4:6]

    # endyear = endyearmonth[0:4]

    if inityearmonth==endyearmonth:
        return [inityearmonth]
    else:
        return ([inityearmonth] + getMonthSeq(addMonth(inityearmonth, 1), endyearmonth))

def getLastDayOfMonth(m):
    options = {"1": "31", "3": "31", "5": "31", "7": "31", "8": "31",\
    "10": "31", "12": "31", "01": "31", "03": "31", "05": "31", "07": "31", "08": "31",\
    "4":"30", "6":"30", "9":"30", "11":"30", "04":"30", "06":"30", "09": "30", "2": "28", "02":"28"}

    return options[m]