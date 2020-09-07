#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
                                    row_number,
                                    regexp_replace,
                                    upper,
                                    trim)
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
# from utils_general import *
# from date_functions import *
import re

def get_cat_cols():

	d = {'customer_master': ['segment_nif'], 'ccc': [], 'orders_sla': [], 'averias': [], 'billing': [], 'reimbursements': [], "reclamaciones" : []}


	return d

def get_sel_cols():

	d = {'customer_master': ['nif_cliente',\
	'label',\
	'nb_rgus',\
	'nb_tv_services_nif',\
	'segment_nif',\
	'nb_rgus_cycles_2',\
	'tgs_days_until_fecha_fin_dto',\
	'tgs_has_discount',\
	'diff_rgus_n_n2'],\
	'ccc': ['NIF_CLIENTE',\
	'DEVICE_DELIVERY_REPAIR_w2',\
	'INTERNET_EN_EL_MOVIL_w2',\
	'QUICK_CLOSING_w2',\
	'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w2',\
	'TARIFF_MANAGEMENT_w2',\
	'PRODUCT_AND_SERVICE_MANAGEMENT_w2',\
	'MI_VODAFONE_w2',\
	'CHURN_CANCELLATIONS_w2',\
	'DSL_FIBER_INCIDENCES_AND_SUPPORT_w2',\
	'PREPAID_BALANCE_w2',\
	'COLLECTIONS_w2',\
	'NEW_ADDS_PROCESS_w2',\
	'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w2',\
	'DEVICE_UPGRADE_w2',\
	'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w2',\
	'BILLING_POSTPAID_w2',\
	'BUCKET_w2',\
	'num_calls_w2',\
	'DEVICE_DELIVERY_REPAIR_w4',\
	'INTERNET_EN_EL_MOVIL_w4',\
	'QUICK_CLOSING_w4',\
	'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w4',\
	'TARIFF_MANAGEMENT_w4',\
	'PRODUCT_AND_SERVICE_MANAGEMENT_w4',\
	'MI_VODAFONE_w4',\
	'CHURN_CANCELLATIONS_w4',\
	'DSL_FIBER_INCIDENCES_AND_SUPPORT_w4',\
	'PREPAID_BALANCE_w4',\
	'COLLECTIONS_w4',\
	'NEW_ADDS_PROCESS_w4',\
	'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w4',\
	'DEVICE_UPGRADE_w4',\
	'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w4',\
	'BILLING_POSTPAID_w4',\
	'BUCKET_w4',\
	'num_calls_w4',\
	'DEVICE_DELIVERY_REPAIR_w8',\
	'INTERNET_EN_EL_MOVIL_w8',\
	'QUICK_CLOSING_w8',\
	'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w8',\
	'TARIFF_MANAGEMENT_w8',\
	'PRODUCT_AND_SERVICE_MANAGEMENT_w8',\
	'MI_VODAFONE_w8',\
	'CHURN_CANCELLATIONS_w8',\
	'DSL_FIBER_INCIDENCES_AND_SUPPORT_w8',\
	'PREPAID_BALANCE_w8',\
	'COLLECTIONS_w8',\
	'NEW_ADDS_PROCESS_w8',\
	'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w8',\
	'DEVICE_UPGRADE_w8',\
	'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w8',\
	'BILLING_POSTPAID_w8',\
	'BUCKET_w8',\
	'num_calls_w8',\
	'inc_DEVICE_DELIVERY_REPAIR_w2vsw2',\
	'inc_INTERNET_EN_EL_MOVIL_w2vsw2',\
	'inc_QUICK_CLOSING_w2vsw2',\
	'inc_VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w2vsw2',\
	'inc_TARIFF_MANAGEMENT_w2vsw2',\
	'inc_PRODUCT_AND_SERVICE_MANAGEMENT_w2vsw2',\
	'inc_MI_VODAFONE_w2vsw2',\
	'inc_CHURN_CANCELLATIONS_w2vsw2',\
	'inc_DSL_FIBER_INCIDENCES_AND_SUPPORT_w2vsw2',\
	'inc_PREPAID_BALANCE_w2vsw2',\
	'inc_COLLECTIONS_w2vsw2',\
	'inc_NEW_ADDS_PROCESS_w2vsw2',\
	'inc_OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w2vsw2',\
	'inc_DEVICE_UPGRADE_w2vsw2',\
	'inc_OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w2vsw2',\
	'inc_BILLING_POSTPAID_w2vsw2',\
	'inc_BUCKET_w2vsw2',\
	'inc_num_calls_w2vsw2',\
	'inc_DEVICE_DELIVERY_REPAIR_w4vsw4',\
	'inc_INTERNET_EN_EL_MOVIL_w4vsw4',\
	'inc_QUICK_CLOSING_w4vsw4',\
	'inc_VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w4vsw4',\
	'inc_TARIFF_MANAGEMENT_w4vsw4',\
	'inc_PRODUCT_AND_SERVICE_MANAGEMENT_w4vsw4',\
	'inc_MI_VODAFONE_w4vsw4',\
	'inc_CHURN_CANCELLATIONS_w4vsw4',\
	'inc_DSL_FIBER_INCIDENCES_AND_SUPPORT_w4vsw4',\
	'inc_PREPAID_BALANCE_w4vsw4',\
	'inc_COLLECTIONS_w4vsw4',\
	'inc_NEW_ADDS_PROCESS_w4vsw4',\
	'inc_OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w4vsw4',\
	'inc_DEVICE_UPGRADE_w4vsw4',\
	'inc_OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w4vsw4',\
	'inc_BILLING_POSTPAID_w4vsw4',\
	'inc_BUCKET_w4vsw4',\
	'inc_num_calls_w4vsw4'],\
	'orders_sla': ['NIF_CLIENTE',\
	'nb_started_orders_last30',\
	'nb_started_orders_last60',\
	'nb_started_orders_last90',\
	'nb_started_orders_last120',\
	'nb_started_orders_last180',\
	'nb_started_orders_last240',\
	'nb_started_orders_last365',\
	'nb_completed_orders_last30',\
	'nb_completed_orders_last60',\
	'nb_completed_orders_last90',\
	'nb_completed_orders_last120',\
	'nb_completed_orders_last180',\
	'nb_completed_orders_last240',\
	'nb_completed_orders_last365',\
	'nb_completed_orders_last30_1SLA',\
	'nb_completed_orders_last30_2SLA',\
	'nb_completed_orders_last30_3SLA',\
	'nb_completed_orders_last60_1SLA',\
	'nb_completed_orders_last60_2SLA',\
	'nb_completed_orders_last60_3SLA',\
	'nb_completed_orders_last90_1SLA',\
	'nb_completed_orders_last90_2SLA',\
	'nb_completed_orders_last90_3SLA',\
	'nb_completed_orders_last120_1SLA',\
	'nb_completed_orders_last120_2SLA',\
	'nb_completed_orders_last120_3SLA',\
	'nb_completed_orders_last180_1SLA',\
	'nb_completed_orders_last180_2SLA',\
	'nb_completed_orders_last180_3SLA',\
	'nb_completed_orders_last240_1SLA',\
	'nb_completed_orders_last240_2SLA',\
	'nb_completed_orders_last240_3SLA',\
	'nb_completed_orders_last365_1SLA',\
	'nb_completed_orders_last365_2SLA',\
	'nb_completed_orders_last365_3SLA',\
	'avg_days_bw_open_orders',\
	'mean_sla_factor_last30',\
	'mean_sla_factor_last60',\
	'mean_sla_factor_last90',\
	'mean_sla_factor_last120',\
	'mean_sla_factor_last180',\
	'mean_sla_factor_last240',\
	'mean_sla_factor_last365',\
	'nb_started_orders_traslados_last30',\
	'nb_started_orders_traslados_last60',\
	'nb_started_orders_traslados_last90',\
	'nb_started_orders_traslados_last120',\
	'nb_started_orders_traslados_last180',\
	'nb_started_orders_traslados_last240',\
	'nb_started_orders_traslados_last365',\
	'nb_completed_orders_traslados_last30',\
	'nb_completed_orders_traslados_last60',\
	'nb_completed_orders_traslados_last90',\
	'nb_completed_orders_traslados_last120',\
	'nb_completed_orders_traslados_last180',\
	'nb_completed_orders_traslados_last240',\
	'nb_completed_orders_traslados_last365',\
	'nb_completed_orders_traslados_last30_1SLA',\
	'nb_completed_orders_traslados_last30_2SLA',\
	'nb_completed_orders_traslados_last30_3SLA',\
	'nb_completed_orders_traslados_last60_1SLA',\
	'nb_completed_orders_traslados_last60_2SLA',\
	'nb_completed_orders_traslados_last60_3SLA',\
	'nb_completed_orders_traslados_last90_1SLA',\
	'nb_completed_orders_traslados_last90_2SLA',\
	'nb_completed_orders_traslados_last90_3SLA',\
	'nb_completed_orders_traslados_last120_1SLA',\
	'nb_completed_orders_traslados_last120_2SLA',\
	'nb_completed_orders_traslados_last120_3SLA',\
	'nb_completed_orders_traslados_last180_1SLA',\
	'nb_completed_orders_traslados_last180_2SLA',\
	'nb_completed_orders_traslados_last180_3SLA',\
	'nb_completed_orders_traslados_last240_1SLA',\
	'nb_completed_orders_traslados_last240_2SLA',\
	'nb_completed_orders_traslados_last240_3SLA',\
	'nb_completed_orders_traslados_last365_1SLA',\
	'nb_completed_orders_traslados_last365_2SLA',\
	'nb_completed_orders_traslados_last365_3SLA',\
	'avg_days_bw_open_orders_traslado',\
	'mean_sla_factor_traslados_last30',\
	'mean_sla_factor_traslados_last60',\
	'mean_sla_factor_traslados_last90',\
	'mean_sla_factor_traslados_last120',\
	'mean_sla_factor_traslados_last180',\
	'mean_sla_factor_traslados_last240',\
	'mean_sla_factor_traslados_last365'],\
	'averias': ['NIF_CLIENTE',\
	'NUM_AVERIAS_NIF_ini_w4',\
	'NUM_AVERIAS_NIF_prev_w4',\
	'NUM_AVERIAS_NIF_w4vsw4',\
	'NUM_AVERIAS_NIF_ini_w8',\
	'NUM_AVERIAS_NIF_prev_w8',\
	'NUM_AVERIAS_NIF_w8vsw8'],\
	'reclamaciones': ['NIF_CLIENTE',\
	'NUM_RECLAMACIONES_NIF_ini_w4',\
	'NUM_RECLAMACIONES_NIF_prev_w4',\
	'NUM_RECLAMACIONES_NIF_w4vsw4',\
	'NUM_RECLAMACIONES_NIF_ini_w8',\
	'NUM_RECLAMACIONES_NIF_prev_w8',\
	'NUM_RECLAMACIONES_NIF_w8vsw8'],\
	'billing': ['NIF_CLIENTE',\
	'Bill_N1_Amount_To_Pay',\
	'Bill_N2_Amount_To_Pay',\
	'Bill_N3_Amount_To_Pay',\
	'Bill_N4_Amount_To_Pay',\
	'Bill_N5_Amount_To_Pay',\
	'bill_n1_net',\
	'bill_n2_net',\
	'bill_n3_net',\
	'bill_n4_net',\
	'bill_n5_net',\
	'inc_bill_n1_n2_net',\
	'inc_bill_n1_n3_net',\
	'inc_bill_n1_n4_net',\
	'inc_bill_n1_n5_net',\
	'inc_Bill_N1_N2_Amount_To_Pay',\
	'inc_Bill_N1_N3_Amount_To_Pay',\
	'inc_Bill_N1_N4_Amount_To_Pay',\
	'inc_Bill_N1_N5_Amount_To_Pay'],\
	'reimbursements': ['NIF_CLIENTE',\
	'Reimbursement_adjustment_net',\
	'Reimbursement_adjustment_debt',\
	'Reimbursement_num',\
	'Reimbursement_num_n8',\
	'Reimbursement_num_n6',\
	'Reimbursement_days_since',\
	'Reimbursement_num_n4',\
	'Reimbursement_num_n5',\
	'Reimbursement_num_n2',\
	'Reimbursement_num_n3',\
	'Reimbursement_days_2_solve',\
	'Reimbursement_num_month_2',\
	'Reimbursement_num_n7',\
	'Reimbursement_num_n1',\
	'Reimbursement_num_month_1']}

	return d

def get_mini_ids(spark, sources, date_):

	day_ = str(int(date_[6:8]))

	month_ = str(int(date_[4:6]))

	year_ = str(int(date_[0:4]))

	# Metadata

	all_cols = get_sel_cols()

	cat_cols = get_cat_cols()

	# Base

	ids = get_customer_base(spark, date_)

	print '[Info Analysis] Number of NIFs in the customer base for ' + date_ + ': ' + str(ids.count())

	for s in sources:

		num_cols = list(set(all_cols[s]) - set(cat_cols[s]))

		df = spark\
		.read\
		.parquet('/data/attributes/vf_es/trigger_analysis/' + s + '/year=' + year_ + '/month=' + month_ + '/day=' + day_)\
		.select(num_cols)

		ids = ids.join(df, ['nif_cliente'], 'left_outer').na.fill(0)

		print '[Info Analysis] Number of NIFs in IDS built for ' + date_ + ' after adding ' + s + ' features: ' + str(ids.count())

	return ids


def get_customer_base(spark, date_):

	day_ = str(int(date_[6:8]))

	month_ = str(int(date_[4:6]))

	year_ = str(int(date_[0:4]))

	# Getting the mapper msisdn-NIF

	customerDF = spark\
	.read\
	.option("mergeSchema", True)\
	.parquet("/data/udf/vf_es/amdocs_ids/customer/year=" + year_ + "/month=" + month_ + "/day=" + day_)

	serviceDF = spark\
	.read\
	.option("mergeSchema", True)\
	.parquet("/data/udf/vf_es/amdocs_ids/service/year=" + year_ + "/month=" + month_ + "/day=" + day_)

	base = customerDF\
	.join(serviceDF, ["NUM_CLIENTE"], "inner")\
	.select('NIF_CLIENTE')\
	.filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')))\
	.dropDuplicates()

	return base

if __name__ == "__main__":

    # create Spark context with Spark configuration
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession\
        .builder\
        .appName("Trigger identification")\
        .master("yarn")\
        .config("spark.submit.deployMode", "client")\
        .config("spark.ui.showConsoleProgress", "true")\
        .enableHiveSupport().getOrCreate())

    # Inputs: date_ and sources
    # date_: 20190414, 20190421, 20190507
    # sources: 'customer_master', 'ccc', 'orders_sla', 'averias', 'reclamaciones', 'billing', 'reimbursements' 

    date_ = '20190414'
    sources = ['customer_master', 'ccc', 'orders_sla', 'averias', 'reclamaciones', 'billing', 'reimbursements']

    ids = get_mini_ids(spark, sources, date_)

    print '[Info Analysis] IDS of ' + date_ + ' ready for the analysis - Number of NIFs: ' + str(ids.count())

    schema = ids.dtypes

    non_numeric_cols = [p[0] for p in schema if p[1]=='string']

    for non_num_col in non_numeric_cols:
    	print non_num_col
