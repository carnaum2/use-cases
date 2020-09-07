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
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from datetime import datetime
from itertools import chain
import numpy as np
from utils_general import *
from utils_model import *
from utils_fbb_churn import *
from metadata_fbb_churn import *
from feature_selection_utils import *

if __name__ == "__main__":

	# create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession\
        .builder\
        .appName("VF_ES AMDOCS FBB Churn Prediction")\
        .master("yarn")\
        .config("spark.submit.deployMode", "client")\
        .config("spark.ui.showConsoleProgress", "true")\
        .enableHiveSupport().getOrCreate())

    feats = ["total_tv_total_charges",\
    "total_price_football",\
    "football_services",\
    "max_dias_desde_fx_football_tv",\
    "max_dias_desde_fx_srv_basic",\
    "dias_desde_movil_fx_first",\
    "max_price_srv_basic",\
    "dias_desde_fixed_fx_first",\
    "max_price_tariff",\
    "total_price_tariff",\
    "mean_dias_desde_fx_srv_basic",\
    "mean_price_srv_basic",\
    "inc_bill_n1_n4_net",\
    "inc_Bill_N1_N4_Amount_To_Pay",\
    "inc_Bill_N1_N2_Amount_To_Pay",\
    "movil_services",\
    "inc_Bill_N1_N5_Amount_To_Pay",\
    "inc_bill_n1_n2_net",\
    "total_price_srv_basic",\
    "days_since_first_order",\
    "inc_bill_n1_n5_net",\
    "nif_min_days_since_port",\
    "dias_desde_fbb_fx_first",\
    "Bill_N1_InvoiceCharges",\
    "days_since_last_order",\
    "Bill_N1_Debt_Amount",\
    "Bill_N1_Tax_Amount",\
    "total_penal_srv_pending_n1_penal_amount",\
    "Bill_N1_Amount_To_Pay",\
    "nif_port_freq_per_day",\
    "bill_n1_net",\
    "min_price_srv_basic",\
    "min_dias_desde_fx_srv_basic",\
    "inc_bill_n1_n3_net",\
    "inc_Bill_N1_N3_Amount_To_Pay",\
    "dias_desde_tv_fx_first",\
    "Bill_N4_Amount_To_Pay",\
    "Bill_N4_InvoiceCharges",\
    "Bill_N2_InvoiceCharges",\
    "Bill_N4_Tax_Amount",\
    "Bill_N2_Amount_To_Pay",\
    "bill_n4_net",\
    "nif_avg_days_since_port",\
    "max_dias_desde_fx_pvr_tv",\
    "Bill_N2_Debt_Amount",\
    "dias_desde_fx_fbb_upgrade",\
    "total_min_dias_hasta_penal_cust_pending_end_date",\
    "num_tariff_unknown"]

    origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'

    trmonth = '201807'
    ttmonth='201809'
    horizon = 2

    selcols = selcols = getIdFeats() + feats

    trdf = getFbbChurnLabeledCar(spark, origin, trmonth, selcols, horizon)

    ttdf = getFbbChurnLabeledCar(spark, origin, ttmonth, selcols, horizon)

    trdf.coalesce(1)\
    .write\
    .format("com.databricks.spark.csv")\
    .option("header", "true")\
    .save("/user/jmarcoso/sample_tr")

    ttdf.coalesce(1)\
    .write\
    .format("com.databricks.spark.csv")\
    .option("header", "true")\
    .save("/user/jmarcoso/sample_tt")

