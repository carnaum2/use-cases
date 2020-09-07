# coding: utf-8

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
                                    variance,
                                    array,
                                    skewness,
                                    kurtosis,
                                    concat_ws)

from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, StructType, StructField,LongType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder, QuantileDiscretizer, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import IsotonicRegression
from datetime import datetime
import math
import operator
import numpy as np
import time
import itertools
from utils_general import *
from pykhaos.utils.date_functions import *
from utils_model import *
from metadata_fbb_churn import *
import subprocess
#from date_functions import get_next_cycle


def getFbbChurnLabeledCarCycles(spark, origin, yearmonthday, selcols, horizon = 4):

    cycle = 0
    fini_tmp = yearmonthday
    while cycle < horizon:
        yearmonthday_target = get_next_cycle(fini_tmp, str_fmt="%Y%m%d")
        cycle = cycle + 1
        fini_tmp = yearmonthday_target

    yearmonth = yearmonthday[0:6]

    trfeatdf = getCarNumClienteDf(spark, origin, yearmonthday)

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Samples for month " + yearmonthday + ": " + str(trfeatdf.count()))

    # Loading port-out requests and DXs
    # # labmonthlisttr = getMonthSeq(initportmonthtr, lastportmonthtr)

    # Las bajas de fibra pueden venir por:
    #- Solicitudes de baja de fijo
    fixporttr = getFixPortRequestsForCycleList(spark, yearmonthday, yearmonthday_target)
    #- Porque dejen de estar en la lista de clientes
    fixdxtr = getFbbDxsForCycleList(spark,yearmonthday, yearmonthday_target)

    # Labeling: FBB service is labeled as 1 if, during the next time window specified by the horizon, either the associated fixed service requested to be ported out or the FBB was disconnected
    window = Window.partitionBy("num_cliente")

    unbaltrdf = trfeatdf\
    .join(fixporttr, ['msisdn_d'], "left_outer")\
    .na.fill({'label_srv': 0.0})\
    .join(fixdxtr, ['msisdn'], "left_outer")\
    .na.fill({'label_dx': 0.0})\
    .withColumn('tmp', when((col('label_srv')==1.0) | (col('label_dx')==1.0), 1.0).otherwise(0.0))\
    .withColumn('label', sql_max('tmp').over(window))\
    .filter(col("rgu")=="fbb")\
    .select(selcols + ['label'])

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Labeled samples for month " + yearmonth + ": " + str(unbaltrdf.count()))

    return unbaltrdf

def getFbbChurnLabeledCar(spark, origin, yearmonth, selcols, horizon = 2):

    refdatetr = yearmonth + getLastDayOfMonth(yearmonth[4:6])

    initportmonthtr = addMonth(yearmonth, 1)
    lastportmonthtr = addMonth(yearmonth, horizon)

    trfeatdf = getCarNumClienteDf(spark, origin, refdatetr)

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Samples for month " + yearmonth + ": " + str(trfeatdf.count()))

    # Loading port-out requests and DXs

    labmonthlisttr = getMonthSeq(initportmonthtr, lastportmonthtr)

    fixporttr = getFixPortRequestsForMonthList(spark, labmonthlisttr)

    fixdxtr = getFbbDxsForMonthList(spark, [yearmonth], horizon)

    # Labeling: FBB service is labeled as 1 if, during the next time window specified by the horizon, either the associated fixed service requested to be ported out or the FBB was disconnected
    window = Window.partitionBy("num_cliente")

    unbaltrdf = trfeatdf\
    .join(fixporttr, ['msisdn_d'], "left_outer")\
    .na.fill({'label_srv': 0.0})\
    .join(fixdxtr, ['msisdn'], "left_outer")\
    .na.fill({'label_dx': 0.0})\
    .withColumn('tmp', when((col('label_srv')==1.0) | (col('label_dx')==1.0), 1.0).otherwise(0.0))\
    .withColumn('label', sql_max('tmp').over(window))\
    .filter(col("rgu")=="fbb")\
    .select(selcols + ['label'])

    print("[Info getFbbChurnLabeledCar] " + time.ctime() + " Labeled samples for month " + yearmonth + ": " + str(unbaltrdf.count()))

    return unbaltrdf

def getFbbChurnUnlabeledCar(spark, origin, yearmonthday, selcols):

    ttfeatdf = getCarNumClienteDf(spark, origin, yearmonthday)

    print("[Info getFbbChurnUnlabeledCar] " + time.ctime() + " Samples for " + yearmonthday + ": " + str(ttfeatdf.count()))

    unbalttdf = ttfeatdf\
    .filter(col("rgu")=="fbb")\
    .select(selcols)

    print("[Info getFbbChurnUnlabeledCar] " + time.ctime() + " Unlabeled samples for month " + yearmonthday + ": " + str(unbalttdf.count()))

    return unbalttdf

def getFixPortRequestsForCycleList(spark, yearmonthday, yearmonthday_target):

    yearmonthday_obj = convert_to_date(yearmonthday)
    yearmonthday_target_obj = convert_to_date(yearmonthday_target)

    fixport = spark.read.table("raw_es.portabilitiesinout_portafijo") \
        .filter(col("INICIO_RANGO") == col("FIN_RANGO")) \
        .withColumnRenamed("INICIO_RANGO", "msisdn_d") \
        .select("msisdn_d", "FECHA_INSERCION_SGP") \
        .distinct() \
        .withColumn("label_srv", lit(1.0)) \
        .where(
        (col('FECHA_INSERCION_SGP') >= yearmonthday_obj) & (col('FECHA_INSERCION_SGP') <= yearmonthday_target_obj))

    print("[Info getFixPortRequestsForMonth] " + time.ctime() + " Port-out requests for fixed services during period " + yearmonthday + "-"+yearmonthday_target+": " + str(fixport.count()))

    return fixport

def getFixPortRequestsForMonthList(spark, months):

    if (not months):
        myschema = StructType([StructField("msisdn_d", StringType(), True), StructField("label_srv", DoubleType(), True)])
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), myschema)
    else:
        return (getFixPortRequestsForMonth(spark, months[0]).union(getFixPortRequestsForMonthList(spark, months[1:])))

def getFbbDxsForMonth(spark, yearmonth, horizon = 2):

    #print('*******',yearmonthday)

    # cycle = 0
    # fini_tmp = yearmonthday
    # fechas = []
    # while cycle < horizon:
    #     trcycle_end = get_next_cycle(fini_tmp, str_fmt="%Y%m%d")
    #     cycle = cycle + 1
    #     fini_tmp = trcycle_end
    #     fechas.append(trcycle_end)

    current_date = str(yearmonth[0:6]) + str(getLastDayOfMonth(yearmonth[4:6]))
    current_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + current_date

    target_month = addMonth(yearmonth, horizon)
    target_date = str(target_month) + str(getLastDayOfMonth(target_month[4:6]))
    target_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + target_date

    print('DATES',current_date,target_date,'---',target_month,target_date)#,trcycle_end)

    current_base = spark\
    .read\
    .parquet(current_source)\
    .filter(col("ClosingDay") == current_date)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    target_base = spark\
    .read\
    .parquet(target_source)\
    .filter(col("ClosingDay") == target_date)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    churn_base = current_base\
    .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn").withColumn("label_dx", lit(1.0))\
    .distinct()

    print("[Info getFbbDxsForMonth] " + time.ctime() + " DXs for FBB services during month " + yearmonth + ": " + str(churn_base.count()))

    return churn_base


def getFbbDxsForCycleList(spark, yearmonthday, yearmonthday_target):

    # If the day is the last one of the month we take the client list of the last cycle of the previous month
    if yearmonthday[6:8]=='01': yearmonthday=get_previous_cycle(yearmonthday)

    current_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday
    target_source = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + yearmonthday_target

    current_base = spark\
    .read\
    .parquet(current_source)\
    .filter(col("ClosingDay") == yearmonthday)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    target_base = spark\
    .read\
    .parquet(target_source)\
    .filter(col("ClosingDay") == yearmonthday_target)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))\
    .filter(col('rgu')=='fbb')\
    .select("msisdn")\
    .distinct()\
    .repartition(400)

    churn_base = current_base\
    .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn").withColumn("label_dx", lit(1.0))\
    .distinct()

    print("[Info getFbbDxsForMonth] " + time.ctime() + " DXs for FBB services during the period: " + yearmonthday + "-"+yearmonthday_target+": " + str(churn_base.count()))

    return churn_base


def getFbbDxsForMonthList(spark, months, horizon = 2):

    if (not months):
        myschema = StructType([StructField("msisdn", StringType(), True), StructField("label_dx", DoubleType(), True)])
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), myschema)
    else:
        print(months[0])
        return (getFbbDxsForMonth(spark, months[0], horizon).union(getFbbDxsForMonthList(spark, months[1:], horizon)))

def get_billing_df(spark, origin, refdate):

    print "[Info get_billing_df] " + time.ctime() + " Starting the preparation of billing feats"

    # val origin = "/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_" + process_date

    #////////////////// 2. Global variables //////////////////

    #// 2.4. ____ Fields related to billing ____

    billing_columns = reduce((lambda x, y: x + y), [["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount", "Bill_N" + str(n) + "_Debt_Amount"] for n in range(1,6)]) + ["num_cliente"]

    # 3// 2.5. ____ Values used for null imputation ____

    repartdf = spark\
    .read\
    .parquet(origin + refdate)\
    .select(billing_columns + ["ClosingDay", "rgu", "clase_cli_cod_clase_cliente", "cod_estado_general"])\
    .repartition(400)\
    .cache()

    [avg_bill_n1_invoicecharges,\
    avg_bill_n1_bill_amount,\
    avg_bill_n1_tax_amount,\
    avg_bill_n1_debt_amount,\
    avg_bill_n2_invoicecharges,\
    avg_bill_n2_bill_amount,\
    avg_bill_n2_tax_amount,\
    avg_bill_n2_debt_amount,\
    avg_bill_n3_invoicecharges,\
    avg_bill_n3_bill_amount,\
    avg_bill_n3_tax_amount,\
    avg_bill_n3_debt_amount,\
    avg_bill_n4_invoicecharges,\
    avg_bill_n4_bill_amount,\
    avg_bill_n4_tax_amount,\
    avg_bill_n4_debt_amount,\
    avg_bill_n5_invoicecharges,\
    avg_bill_n5_bill_amount,\
    avg_bill_n5_tax_amount,\
    avg_bill_n5_debt_amount] = repartdf\
    .filter((col("ClosingDay") == refdate) & (col("rgu") == "fbb"))\
    .filter((col("clase_cli_cod_clase_cliente") == "RS")\
        & (col("cod_estado_general").isin("01", "09"))\
        & (col("Bill_N1_InvoiceCharges").isNotNull())\
        & (col("Bill_N1_Amount_To_Pay").isNotNull())\
        & (col("Bill_N1_Tax_Amount").isNotNull())\
        & (col("Bill_N1_Debt_Amount").isNotNull())\
        & (col("Bill_N2_InvoiceCharges").isNotNull())\
        & (col("Bill_N2_Amount_To_Pay").isNotNull())\
        & (col("Bill_N2_Tax_Amount").isNotNull())\
        & (col("Bill_N2_Debt_Amount").isNotNull())\
        & (col("Bill_N3_InvoiceCharges").isNotNull())\
        & (col("Bill_N3_Amount_To_Pay").isNotNull())\
        & (col("Bill_N3_Tax_Amount").isNotNull())\
        & (col("Bill_N3_Debt_Amount").isNotNull())\
        & (col("Bill_N4_InvoiceCharges").isNotNull())\
        & (col("Bill_N4_Amount_To_Pay").isNotNull())\
        & (col("Bill_N4_Tax_Amount").isNotNull())\
        & (col("Bill_N4_Debt_Amount").isNotNull())\
        & (col("Bill_N5_InvoiceCharges").isNotNull())\
        & (col("Bill_N5_Amount_To_Pay").isNotNull())\
        & (col("Bill_N5_Tax_Amount").isNotNull())\
        & (col("Bill_N5_Debt_Amount").isNotNull()))\
    .select(billing_columns)\
    .distinct()\
    .select(*[sql_avg("Bill_N1_InvoiceCharges").alias("avg_Bill_N1_InvoiceCharges"),\
        sql_avg("Bill_N1_Amount_To_Pay").alias("avg_Bill_N1_Amount_To_Pay"),\
        sql_avg("Bill_N1_Tax_Amount").alias("avg_Bill_N1_Tax_Amount"),\
        sql_avg("Bill_N1_Debt_Amount").alias("avg_Bill_N1_Debt_Amount"),\
        sql_avg("Bill_N2_InvoiceCharges").alias("avg_Bill_N2_InvoiceCharges"),\
        sql_avg("Bill_N2_Amount_To_Pay").alias("avg_Bill_N2_Amount_To_Pay"),\
        sql_avg("Bill_N2_Tax_Amount").alias("avg_Bill_N2_Tax_Amount"),\
        sql_avg("Bill_N2_Debt_Amount").alias("avg_Bill_N2_Debt_Amount"),\
        sql_avg("Bill_N3_InvoiceCharges").alias("avg_Bill_N3_InvoiceCharges"),\
        sql_avg("Bill_N3_Amount_To_Pay").alias("avg_Bill_N3_Amount_To_Pay"),\
        sql_avg("Bill_N3_Tax_Amount").alias("avg_Bill_N3_Tax_Amount"),\
        sql_avg("Bill_N3_Debt_Amount").alias("avg_Bill_N3_Debt_Amount"),\
        sql_avg("Bill_N4_InvoiceCharges").alias("avg_Bill_N4_InvoiceCharges"),\
        sql_avg("Bill_N4_Amount_To_Pay").alias("avg_Bill_N4_Amount_To_Pay"),\
        sql_avg("Bill_N4_Tax_Amount").alias("avg_Bill_N4_Tax_Amount"),\
        sql_avg("Bill_N4_Debt_Amount").alias("avg_Bill_N4_Debt_Amount"),\
        sql_avg("Bill_N5_InvoiceCharges").alias("avg_Bill_N5_InvoiceCharges"),\
        sql_avg("Bill_N5_Amount_To_Pay").alias("avg_Bill_N5_Amount_To_Pay"),\
        sql_avg("Bill_N5_Tax_Amount").alias("avg_Bill_N5_Tax_Amount"),\
        sql_avg("Bill_N5_Debt_Amount").alias("avg_Bill_N5_Debt_Amount")])\
    .rdd\
    .map(lambda r: [r["avg_Bill_N1_InvoiceCharges"], r["avg_Bill_N1_Amount_To_Pay"], r["avg_Bill_N1_Tax_Amount"], r["avg_Bill_N1_Debt_Amount"], r["avg_Bill_N2_InvoiceCharges"], r["avg_Bill_N2_Amount_To_Pay"], r["avg_Bill_N2_Tax_Amount"], r["avg_Bill_N2_Debt_Amount"], r["avg_Bill_N3_InvoiceCharges"], r["avg_Bill_N3_Amount_To_Pay"], r["avg_Bill_N3_Tax_Amount"], r["avg_Bill_N3_Debt_Amount"], r["avg_Bill_N4_InvoiceCharges"], r["avg_Bill_N4_Amount_To_Pay"], r["avg_Bill_N4_Tax_Amount"], r["avg_Bill_N4_Debt_Amount"], r["avg_Bill_N5_InvoiceCharges"], r["avg_Bill_N5_Amount_To_Pay"], r["avg_Bill_N5_Tax_Amount"], r["avg_Bill_N5_Debt_Amount"]])\
    .first()

    bill_na_fill = {"Bill_N1_InvoiceCharges": avg_bill_n1_invoicecharges,\
    "Bill_N1_Amount_To_Pay": avg_bill_n1_bill_amount,\
    "Bill_N1_Tax_Amount": avg_bill_n1_tax_amount,\
    "Bill_N1_Debt_Amount": avg_bill_n1_debt_amount,\
    "Bill_N2_InvoiceCharges": avg_bill_n2_invoicecharges,\
    "Bill_N2_Amount_To_Pay": avg_bill_n2_bill_amount,\
    "Bill_N2_Tax_Amount": avg_bill_n2_tax_amount,\
    "Bill_N2_Debt_Amount": avg_bill_n2_debt_amount,\
    "Bill_N3_InvoiceCharges": avg_bill_n3_invoicecharges,\
    "Bill_N3_Amount_To_Pay": avg_bill_n3_bill_amount,\
    "Bill_N3_Tax_Amount": avg_bill_n3_tax_amount,\
    "Bill_N3_Debt_Amount": avg_bill_n3_debt_amount,\
    "Bill_N4_InvoiceCharges": avg_bill_n4_invoicecharges,\
    "Bill_N4_Amount_To_Pay": avg_bill_n4_bill_amount,\
    "Bill_N4_Tax_Amount": avg_bill_n4_tax_amount,\
    "Bill_N4_Debt_Amount": avg_bill_n4_debt_amount,\
    "Bill_N5_InvoiceCharges": avg_bill_n5_invoicecharges,\
    "Bill_N5_Amount_To_Pay": avg_bill_n5_bill_amount,\
    "Bill_N5_Tax_Amount": avg_bill_n5_tax_amount,\
    "Bill_N5_Debt_Amount": avg_bill_n5_debt_amount}

    # 3. Preparing data (using cached tmp dataframes to break lineage and prevent "janino" errors (compiler) duting execution) //////////////////

    #// Filter -- col("cod_estado_general").isin("01", "09") -- is not applied. All the services are processed and the filter will be used in the process to implement the model
    billingdf = repartdf\
    .filter(col("ClosingDay") == refdate)\
    .filter((col("clase_cli_cod_clase_cliente") == "RS"))\
    .select(billing_columns)\
    .distinct()\
    .na\
    .fill(bill_na_fill)\
    .withColumn("bill_n1_net", col("Bill_N1_Amount_To_Pay") - col("Bill_N1_Tax_Amount")  )\
    .withColumn("bill_n2_net", col("Bill_N2_Amount_To_Pay") - col("Bill_N2_Tax_Amount")  )\
    .withColumn("bill_n3_net", col("Bill_N3_Amount_To_Pay") - col("Bill_N3_Tax_Amount")  )\
    .withColumn("bill_n4_net", col("Bill_N4_Amount_To_Pay") - col("Bill_N4_Tax_Amount")  )\
    .withColumn("bill_n5_net", col("Bill_N5_Amount_To_Pay") - col("Bill_N5_Tax_Amount")  )\
    .withColumn("inc_bill_n1_n2_net", col("bill_n1_net") - col("bill_n2_net")  )\
    .withColumn("inc_bill_n1_n3_net", col("bill_n1_net") - col("bill_n3_net")  )\
    .withColumn("inc_bill_n1_n4_net", col("bill_n1_net") - col("bill_n4_net")  )\
    .withColumn("inc_bill_n1_n5_net", col("bill_n1_net") - col("bill_n5_net")  )\
    .withColumn("inc_Bill_N1_N2_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N2_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N3_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N3_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N4_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N4_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N5_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N5_Amount_To_Pay")  )\
    .na\
    .fill(-1.0)

    print "[Info get_billing_df] " + time.ctime() + " Size of the df with billing feats: " + str(billingdf.count())

    return billingdf

def get_mobile_spinners_df(spark, closing_day, starting_day = "-"):

    # Feats derived from the previous behaviour (from 20020101 to closing_day) of the customer regarding port-out requests

    # Input:
    # - spark: entry point to Spark
    # - closing_day: feats will be computed from 20020101 to closing_day
    # - starting_day: not required (any string will be valid)
    # - verbose: True if traces/logs are need to be observed during the process

    # Output:
    # df: nif_cliente (primary key) and the set of feats described below

    # Feats (nif_level):
    # - nif_min_days_since_port, nif_max_days_since_port, nif_avg_days_since_port, nif_var_days_since_port, nif_distinct_msisdn, nif_port_freq_per_day, nif_port_freq_per_msisdn
    # - total_acan, total_apor, total_arec, total_movistar, total_simyo, total_orange, total_jazztel, total_yoigo, total_masmovil, total_pepephone, total_reuskal, total_unknown, total_otros, num_distinct_operators
    # - Number of port-out requests per operator and state. One column for each combination resulting from the two following sets:
    #     - destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown", "otros"]
    #     - estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

    repartdf = spark\
    .read\
    .table("raw_es.portabilitiesinout_sopo_solicitud_portabilidad")\
    .repartition(400)

    sopodf = repartdf\
    .select(["sopo_ds_cif", "sopo_ds_msisdn1", "sopo_ds_fecha_solicitud", "esta_co_estado", "sopo_co_solicitante"])\
    .withColumn("formatted_date", from_unixtime(unix_timestamp(col("sopo_ds_fecha_solicitud"), "yyyy-MM-dd")))\
    .filter(col("formatted_date") >= from_unixtime(unix_timestamp(lit("20020101"), "yyyyMMdd")))\
    .withColumn("ref_date", from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")))\
    .filter(col("formatted_date") < col("ref_date"))\
    .withColumn("destino", lit("otros"))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%telefonica%")) | (lower(col("sopo_co_solicitante")).like("%movistar%")), "movistar").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%eplus%")) | (lower(col("sopo_co_solicitante")).like("%e-plus%")), "simyo").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%amena%")), "orange").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%jazztel%")), "jazztel").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%xfera%")) | (lower(col("sopo_co_solicitante")).like("%yoigo%")), "yoigo").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%mas movil%")) | (lower(col("sopo_co_solicitante")).like("%masmovil%")) | (lower(col("sopo_co_solicitante")).like("%republica%")), "masmovil").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%pepephone%")), "pepephone").otherwise(col("destino")))\
    .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%mundor%")) | (lower(col("sopo_co_solicitante")).like("%euskaltel%")), "r_euskal").otherwise(col("destino")))\
    .withColumn("destino", when(((lower(col("sopo_co_solicitante")) == "")) | ((lower(col("sopo_co_solicitante")) == " ")), "unknown").otherwise(col("destino")))\
    .select(["sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "ref_date", "esta_co_estado", "destino"])\
    .cache()

    print "[Info get_mobile_spinners_df] " + time.ctime() + " Port-out data loaded for " + str(closing_day) + " with a total of " + str(sopodf.count()) + " rows and " + str(sopodf.select("sopo_ds_cif").distinct().count()) + " distinct NIFs"

    feats1df = sopodf\
    .select(["sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "ref_date"])\
    .distinct()\
    .withColumn("days_since_port", datediff(col("ref_date"), col("formatted_date")).cast("double"))\
    .groupBy("sopo_ds_cif")\
    .agg(*[count("*").alias("nif_port_number"), sql_min("days_since_port").alias("nif_min_days_since_port"), sql_max("days_since_port").alias("nif_max_days_since_port"), sql_avg("days_since_port").alias("nif_avg_days_since_port"), variance("days_since_port").alias("nif_var_days_since_port"), countDistinct("sopo_ds_msisdn1").alias("nif_distinct_msisdn")])\
    .withColumn("nif_var_days_since_port", when(isnan(col("nif_var_days_since_port")), 0.0).otherwise(col("nif_var_days_since_port")))\
    .withColumn("nif_port_freq_per_day", col("nif_port_number")/col("nif_max_days_since_port"))\
    .withColumn("nif_port_freq_per_day", when(isnan(col("nif_port_freq_per_day")), 0.0).otherwise(col("nif_port_freq_per_day")))\
    .withColumn("nif_port_freq_per_msisdn", col("nif_port_number")/col("nif_distinct_msisdn"))\
    .withColumn("nif_port_freq_per_msisdn", when(isnan(col("nif_port_freq_per_msisdn")), 0.0).otherwise(col("nif_port_freq_per_msisdn")))

    print "[Info get_mobile_spinners_df] " + time.ctime() + " Timing feats computed for a total of " + str(feats1df.select("sopo_ds_cif").distinct().count()) + " distinct NIFs"

    # To compute the feats related to the state and the destinations of the port-out requests:
    # destinos: all the possible destinations
    # estados: all the possible states
    # destino_estado: all the possible pairs
    # values: pair to string resulting in a list with all the possible levels of the combination
    
    destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown", "otros"]

    estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

    destino_estado = list(itertools.product(destinos, estados))

    values = [(str(x[0] + "_" + str(x[1]))) for x in destino_estado]

    feats2df = sopodf\
    .select(["sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "esta_co_estado", "destino"])\
    .distinct()\
    .withColumn("to_pivot", concat(col("destino"), lit("_"), col("esta_co_estado")))\
    .groupBy("sopo_ds_cif")\
    .pivot("to_pivot", values)\
    .agg(count("*"))\
    .na.fill(0)\
    .withColumn("total_acan", sum([col(x) for x in values if("ACAN" in x)]))\
    .withColumn("total_apor", sum([col(x) for x in values if("APOR" in x)]))\
    .withColumn("total_arec", sum([col(x) for x in values if("AREC" in x)]))\
    .withColumn("total_movistar", sum([col(x) for x in values if("movistar" in x)]))\
    .withColumn("total_simyo", sum([col(x) for x in values if("simyo" in x)]))\
    .withColumn("total_orange", sum([col(x) for x in values if("orange" in x)]))\
    .withColumn("total_jazztel", sum([col(x) for x in values if("jazztel" in x)]))\
    .withColumn("total_yoigo", sum([col(x) for x in values if("yoigo" in x)]))\
    .withColumn("total_masmovil", sum([col(x) for x in values if("masmovil" in x)]))\
    .withColumn("total_pepephone", sum([col(x) for x in values if("pepephone" in x)]))\
    .withColumn("total_reuskal", sum([col(x) for x in values if("reuskal" in x)]))\
    .withColumn("total_unknown", sum([col(x) for x in values if("unknown" in x)]))\
    .withColumn("total_otros", sum([col(x) for x in values if("otros" in x)]))\
    .withColumn("num_distinct_operators", sum([(col("total_" + x) > 0).cast("double") for x in destinos]))
    
    print "[Info get_mobile_spinners_df] " + time.ctime() + " Destinatination and state feats computed for a total of " + str(feats2df.select("sopo_ds_cif").distinct().count()) + " distinct NIFs"

    nifsopodf = feats1df.join(feats2df, "sopo_ds_cif", "inner").withColumnRenamed("sopo_ds_cif", "nif_cliente").repartition(1000)

    return nifsopodf

def get_orders_df(spark, origin, refdate):
    # Feats derived from orders up to refdate
    # If the model target is computed in a time window starting at refdate, feats derived from orders may include proxy effects
    # Example: churn model trained with pairs of the type (status of the customer at time t, port-out request during [t, t + window]).
    # The customer might have ordered the disconnection of the targeted service (or a different one) before t.
    # As a result, feats derived from orders will take into accout a gap of one month, i.e., they will correspond to the historic behaviour
    # of the customer up to refdate - 1 month

    ref_yearmonth = substractMonth(refdate[0:6], 1)
    newrefdate = ref_yearmonth + getLastDayOfMonth(ref_yearmonth[4:6])

    ref_year = newrefdate[0:4]

    ref_month = newrefdate[4:6]

    ref_day = newrefdate[6:8]

    # window = Window.partitionBy("num_cliente")

    repartdf = spark\
    .read\
    .parquet("/data/udf/vf_es/amdocs_ids/orders/year=" + str(ref_year) + "/month=" + str(int(ref_month)) + "/day=" + str(int(ref_day)))\
    .repartition(400)

    ordersdf = repartdf\
    .withColumn("refdate", from_unixtime(unix_timestamp(lit(newrefdate), 'yyyyMMdd')))\
    .withColumn("days_since_last_order", datediff("refdate", from_unixtime(unix_timestamp('order_n1_startdate', 'yyyyMMdd')))   )\
    .withColumn("order_n1_class", when(isnull(col("order_n1_class")), "unknown").otherwise(col("order_n1_class")))\
    .withColumn("days_since_first_order", greatest(datediff("refdate", from_unixtime(unix_timestamp('order_n1_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n2_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n3_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n4_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n5_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n6_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n7_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n8_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n9_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n10_startdate', 'yyyyMMdd')) )  )   )\
    .select(["num_cliente", "days_since_last_order", "days_since_first_order"]).repartition(1000)

    print "[Info get_orders_df] " + time.ctime() + " Orders feats up to " + newrefdate + " computed - Number of rows is " + str(ordersdf.count()) + " for a total of " + str(ordersdf.select("num_cliente").distinct().count()) + " distinct num_cliente"

    return ordersdf

def get_hfc_cons(spark, origin, refdate):

    #UPLOAD AND DOWNLOAD VOLUMES
    thot_smcnt_path='/data/raw/vf_es/fixnetprobes/THOT_RASTREO_SCMCNT/1.0/parquet/'

    thot_smcnt = spark\
    .read\
    .parquet(thot_smcnt_path)\
    .where(col('timestamp').between(StartingDay_date,ClosingDay_date))\
    .drop(*['service_processed_at', 'service_file_id'])\
    .dropDuplicates()

    thot_smcnt_df_tmp1 = thot_smcnt\
    .withColumn('MAC', col('MAC'))\
    .withColumn('cleanedMac', upper(regexp_replace('MAC', '\.', '')))\
    .withColumn('hour', hour(col('timestamp')))\
    .withColumn('tramo_horario', when(col('hour').isin(7, 8, 9, 10, 11, 12, 13, 14), 'morning').when(col('hour').isin(15, 16, 17, 18, 19, 20, 21, 22), 'evening').otherwise('night'))\
    .withColumn('IND_WEEKDAY', when(date_format(col('timestamp'), 'u').isin(6, 7), '_WE').otherwise('_W'))\
    .withColumn('to_pivot', concat(lit('THOT_Data_'),col('tramo_horario'), col('IND_WEEKDAY')))
    
    pivot_values = ['THOT_Data_morning_W','THOT_Data_evening_W','THOT_Data_night_W','THOT_Data_morning_WE','THOT_Data_evening_WE','THOT_Data_night_WE']
    
    thot_smcnt_df_tmp2 = thot_smcnt_df_tmp1\
    .groupBy('cleanedMac','year','month','day')\
    .pivot('to_pivot',pivot_values)\
    .agg((sql_round(sql_sum('US_BYTES')/1024/1024/1024,2)).alias('upload_GB'), (sql_round(sql_sum('DS_BYTES')/1024/1024/1024,2)).alias('download_GB'))

    thot_smcnt_df_tmp3_totals = thot_smcnt_df_tmp1\
    .groupBy('cleanedMac','year','month','day')\
    .agg((sql_round(sql_sum('US_BYTES')/1024/1024/1024,2)).alias('THOT_Data_Total_upload_GB'), (sql_round(sql_sum('DS_BYTES')/1024/1024/1024,2)).alias('THOT_Data_Total_download_GB'))

    thot_smcnt_df_tmp4 = thot_smcnt_df_tmp3_totals\
    .join(thot_smcnt_df_tmp2,['cleanedMac','year','month','day'],'inner')\
    .withColumnRenamed('cleanedMac','MAC')

    return thot_smcnt_df_tmp4

def getCarNumClienteDf(spark, origin, refdate):

    # Refdate es la fecha de referencia usada (si hemos dado el día uno del mes cogemos la del último ciclo)
    if refdate[6:8] == '01': refdate = get_previous_cycle(refdate)

 #   dfExtraFeat = spark.read.option("delimiter", "|").option("header", True).csv("/data/udf/vf_es/churn/extra_feats/extra_feats_20181231")
  #  dfExtraFeat = dfExtraFeat.drop_duplicates(subset=['msisdn'])
  #  dfExtraFeat = dfExtraFeat.drop_duplicates(subset=['num_cliente'])

  #  print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the original df for " + refdate + ": " + str(dfExtraFeat.count()))

    clientCARdf = spark\
    .read\
    .parquet(origin + refdate)\
    .filter(col("ClosingDay") == refdate)\
    .repartition(400)\
    .cache()

   # setLista = list(set(initCol).intersection(dfCol))
   # setLista.remove('msisdn')
   # for col in setLista:
   #     clientCARdf_mod = clientCARdf.withColumnRenamed(col, col + "_tr")

    repartdf = clientCARdf#_mod.join(dfExtraFeat, ["msisdn"], how="inner")

   # print('-------------',repartdf.count())

    # Values for imputation
    avg_price_srv_basic_dict = (repartdf\
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (isnull(col("price_srv_basic")) == False))\
        .groupBy("rgu")\
        .agg(sql_avg("price_srv_basic").alias("avg_price_srv_basic"))\
        .rdd\
        .map(lambda r: (r["rgu"], r["avg_price_srv_basic"]))\
        .collectAsMap())

    avg_price_tariff = (repartdf\
        .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (isnull(col("price_tariff")) == False) & (col("rgu") == "movil"))\
        .select(sql_avg("price_tariff").alias("avg_price_tariff"))\
        .rdd\
        .map(lambda r: r["avg_price_tariff"])\
        .collect()[0])

    # Data preparation Attributes: fbb service + num_cliente.

    # "nacionalidad", "x_cesion_datos", "x_publicidad_email", "cta_correo", "x_user_twitter", "gender2hgbst_elm", "x_datos_navegacion", "marriage2hgbst_elm", "x_datos_trafico", "x_idioma_factura", "factura_electronica", "x_antiguedad_cuenta", "metodo_pago", "x_formato_factura", "x_user_facebook", "flg_robinson", "superoferta", "cliente_migrado", "tipo_documento"

    origindf_tmp1 = repartdf\
    .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")))\
    .withColumn("ref_date", from_unixtime(unix_timestamp(lit(refdate), "yyyyMMdd")))\
    .withColumn("msisdn", col("msisdn").cast("string"))\
    .withColumn("metodo_pago", when(col("metodo_pago").isin("", " "), "unknown").otherwise(col("metodo_pago")))\
    .withColumn("metodo_pago", lower(col("metodo_pago")))\
    .withColumn("metodo_pago", when(col("metodo_pago").like("%dom%"), "domiciliado").otherwise(col("metodo_pago")))\
    .withColumn("metodo_pago", when(col("metodo_pago").like("%subc%"), "subcuenta").otherwise(col("metodo_pago")))\
    .withColumn("metodo_pago", when(col("metodo_pago").like("%ventan%"), "ventanilla").otherwise(col("metodo_pago")))\
    .withColumn("metodo_pago", when(col("metodo_pago").like("%tarje%"), "tarjeta").otherwise(col("metodo_pago")))\
    .withColumn("metodo_pago", when(col("metodo_pago").isin("unknown", "domiciliado", "subcuenta", "ventanilla", "tarjeta") == False, "otros").otherwise(col("metodo_pago")))

    origindf_tmp2 = origindf_tmp1\
    .withColumn("cta_correo", when(col("cta_correo").isin("", " "), "unknown").otherwise(col("cta_correo")))\
    .withColumn("cta_correo", lower(col("cta_correo")))\
    .withColumn("cta_correo", when(col("cta_correo").like("%gmail%"), "gmail").otherwise(col("cta_correo")))\
    .withColumn("cta_correo", when(col("cta_correo").like("%hotmail%"), "hotmail").otherwise(col("cta_correo")))\
    .withColumn("cta_correo", when(col("cta_correo").like("%yahoo%"), "yahoo").otherwise(col("cta_correo")))\
    .withColumn("cta_correo", when(col("cta_correo").isin("unknown", "gmail", "hotmail", "yahoo") == False, "otros").otherwise(col("cta_correo")))\
    .withColumn("factura_electronica", when(col("factura_electronica") == "1", "s").otherwise(col("factura_electronica")))\
    .withColumn("factura_electronica", when(col("factura_electronica") == "0", "n").otherwise(col("factura_electronica")))\
    .withColumn("factura_electronica", when(col("factura_electronica").isin("s", "n") == False, "unknown").otherwise(col("factura_electronica")))\
    .withColumn("superoferta", when(col("superoferta").isin("", " "), "unknown").otherwise(col("superoferta")))\
    .withColumn("superoferta", when(col("superoferta").isin("ON15", "unknown") == False, "otros").otherwise(col("superoferta")))\
    .withColumn("fecha_migracion", from_unixtime(unix_timestamp(col("fecha_migracion"), "yyyy-MM-dd")))\
    .withColumn("dias_desde_fecha_migracion", datediff(col("ref_date"), col("fecha_migracion")).cast("double"))\
    .withColumn("dias_desde_fecha_migracion", when(col("dias_desde_fecha_migracion") > 365.0*50.0, -1.0).otherwise(col("dias_desde_fecha_migracion")))\
    .withColumn("cliente_migrado", when(col("dias_desde_fecha_migracion") == -1.0, "N").otherwise("S"))\
    .withColumn("tipo_documento", when(col("tipo_documento").isin("", " "), "unknown").otherwise(col("tipo_documento")))\
    .withColumn("tipo_documento", when(col("tipo_documento").isin("N.I.F.", "DNI/NIF"), "nif").otherwise(col("tipo_documento")))\
    .withColumn("tipo_documento", when(col("tipo_documento").isin("N.I.E.", "N.I.E.-Tar", "NIE"), "nie").otherwise(col("tipo_documento")))\
    .withColumn("tipo_documento", when(col("tipo_documento").isin("Pasaporte", "PASAPORTE"), "pasaporte").otherwise(col("tipo_documento")))\
    .withColumn("tipo_documento", when(col("tipo_documento").isin("unknown", "nif", "nie", "pasaporte") == False, "otros").otherwise(col("tipo_documento")))

    origindf_tmp3 = origindf_tmp2\
    .withColumn("x_publicidad_email", when(col("x_publicidad_email").isin("", " "), "unknown").otherwise(col("x_publicidad_email")))\
    .withColumn("x_publicidad_email", when(col("x_publicidad_email").isin("S", "N", "unknown") == False, "otros").otherwise(col("x_publicidad_email")))\
    .withColumn("nacionalidad", lower(col("nacionalidad")))\
    .withColumn("nacionalidad", when(col("nacionalidad").isin("", " "), "unknown").otherwise(col("nacionalidad")))\
    .withColumn("nacionalidad", when(col("nacionalidad").like("%espa%"), "espana").otherwise(col("nacionalidad")))\
    .withColumn("nacionalidad", when(col("nacionalidad").isin("unknown", "espana") == False, "otros").otherwise(col("nacionalidad")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").isin("", " "), "unknown").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").like("%<6%"), "upto6months").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when((col("x_antiguedad_cuenta").like("%1 A%") == True) & (col("x_antiguedad_cuenta").like("%11%") == False), "1year").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").like("%2 - 3%"), "2to3years").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").like("%4 - 5%"), "4to5years").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").like("%6 - 10%"), "6to10years").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").like("%>11%"), "gt11years").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_antiguedad_cuenta", when(col("x_antiguedad_cuenta").isin("unknown", "upto6months", "1year", "2to3years", "4to5years", "6to10years", "gt11years") == False, "otros").otherwise(col("x_antiguedad_cuenta")))\
    .withColumn("x_datos_navegacion", when((col("x_datos_navegacion").isin("", " ")) | (col("x_datos_navegacion").isNull()), "unknown").otherwise(col("x_datos_navegacion")))\
    .withColumn("x_datos_trafico", when(col("x_datos_trafico").isin("", " ") | isnull(col("x_datos_trafico")), "unknown").otherwise(col("x_datos_trafico")))\
    .withColumn("x_cesion_datos", when(col("x_cesion_datos").isin("", " ") | col("x_cesion_datos").isNull(), "unknown").otherwise(col("x_cesion_datos")))\
    .withColumn("x_user_facebook", when(col("x_user_facebook").isin("", " ") | isnull(col("x_user_facebook")), "n").otherwise("s"))\
    .withColumn("x_user_twitter", when(col("x_user_twitter").isin("", " ") | isnull(col("x_user_twitter")), "n").otherwise("s"))\
    .withColumn("marriage2hgbst_elm", lower(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").isin("", " "), "unknown").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%wido%") | col("marriage2hgbst_elm").like("%viud%"), "viudo").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%divor%"), "divorciado").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%marr%") | col("marriage2hgbst_elm").like("%casad%"), "casado").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%coupl%") | col("marriage2hgbst_elm").like("%parej%"), "pareja").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%sing%") | col("marriage2hgbst_elm").like("%solter%"), "soltero").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").like("%separa%"), "separado").otherwise(col("marriage2hgbst_elm")))\
    .withColumn("marriage2hgbst_elm", when(col("marriage2hgbst_elm").isin("unknown", "viudo", "divorciado", "casado", "pareja", "soltero", "separado") == False, "otros").otherwise(col("marriage2hgbst_elm")))

    origindf_tmp4 = origindf_tmp3\
    .withColumn("gender2hgbst_elm", lower(col("gender2hgbst_elm")))\
    .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm").isin("", " "), "unknown").otherwise(col("gender2hgbst_elm")))\
    .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm") == "male", "male").otherwise(col("gender2hgbst_elm")))\
    .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm") == "female", "female").otherwise(col("gender2hgbst_elm")))\
    .withColumn("gender2hgbst_elm", when(col("gender2hgbst_elm").isin("unknown", "male", "female") == False, "otros").otherwise(col("gender2hgbst_elm")))\
    .withColumn("flg_robinson", when(col("flg_robinson").isin("", " "), "unknown").otherwise(col("flg_robinson")))\
    .withColumn("x_formato_factura", when(col("x_formato_factura").isin("", " "), "unknown").otherwise(col("x_formato_factura")))\
    .withColumn("x_formato_factura", when(col("x_formato_factura").isin("unknown", "PAPEL - ESTANDAR", "ELECTRONICA - ESTANDAR", "PAPEL - REDUCIDA", "PAPEL - SEMIREDUCIDA") == False, "otros").otherwise(col("x_formato_factura")))\
    .withColumn("x_idioma_factura", when(col("x_idioma_factura").isin("", " "), "unknown").otherwise(col("x_idioma_factura")))\
    .withColumn("x_idioma_factura", when(col("x_idioma_factura").isin("unknown", "Castellano", "Euskera", "Gallego", "Ingles", "Catalan") == False, "otros").otherwise(col("x_idioma_factura")))\
    .withColumn("bam_fx_first", from_unixtime(unix_timestamp(col("bam_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_bam_fx_first", datediff(col("ref_date"), col("bam_fx_first")).cast("double"))\
    .withColumn("dias_desde_bam_fx_first", when(isnull(col("dias_desde_bam_fx_first")) | isnan(col("dias_desde_bam_fx_first")), -1.0).otherwise(col("dias_desde_bam_fx_first")))\
    .withColumn("bam-movil_fx_first", from_unixtime(unix_timestamp(col("bam-movil_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_bam-movil_fx_first", datediff(col("ref_date"), col("bam-movil_fx_first")).cast("double"))\
    .withColumn("dias_desde_bam-movil_fx_first", when(isnull(col("dias_desde_bam-movil_fx_first")) | isnan(col("dias_desde_bam-movil_fx_first")), -1.0).otherwise(col("dias_desde_bam-movil_fx_first")))\
    .withColumn("fbb_fx_first", from_unixtime(unix_timestamp(col("fbb_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_fbb_fx_first", datediff(col("ref_date"), col("fbb_fx_first")).cast("double")).withColumn("dias_desde_fbb_fx_first", when(isnull(col("dias_desde_fbb_fx_first")) | isnan(col("dias_desde_fbb_fx_first")), -1.0).otherwise(col("dias_desde_fbb_fx_first")))\
    .withColumn("fixed_fx_first", from_unixtime(unix_timestamp(col("fixed_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_fixed_fx_first", datediff(col("ref_date"), col("fixed_fx_first")).cast("double"))\
    .withColumn("dias_desde_fixed_fx_first", when(isnull(col("dias_desde_fixed_fx_first")) | isnan(col("dias_desde_fixed_fx_first")), -1.0).otherwise(col("dias_desde_fixed_fx_first")))\
    .withColumn("movil_fx_first", from_unixtime(unix_timestamp(col("movil_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_movil_fx_first", datediff(col("ref_date"), col("movil_fx_first")).cast("double"))\
    .withColumn("dias_desde_movil_fx_first", when(isnull(col("dias_desde_movil_fx_first")) | isnan(col("dias_desde_movil_fx_first")), -1.0).otherwise(col("dias_desde_movil_fx_first")))\
    .withColumn("prepaid_fx_first", from_unixtime(unix_timestamp(col("prepaid_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_prepaid_fx_first", datediff(col("ref_date"), col("prepaid_fx_first")).cast("double"))\
    .withColumn("dias_desde_prepaid_fx_first", when(isnull(col("dias_desde_prepaid_fx_first")) | isnan(col("dias_desde_prepaid_fx_first")), -1.0).otherwise(col("dias_desde_prepaid_fx_first")))

    origindf_tmp5 = origindf_tmp4\
    .withColumn("tv_fx_first", from_unixtime(unix_timestamp(col("tv_fx_first"), "yyyyMMdd")))\
    .withColumn("dias_desde_tv_fx_first", datediff(col("ref_date"), col("tv_fx_first")).cast("double"))\
    .withColumn("dias_desde_tv_fx_first", when(isnull(col("dias_desde_tv_fx_first")) | isnan(col("dias_desde_tv_fx_first")), -1.0).otherwise(col("dias_desde_tv_fx_first")))\
    .withColumn("fx_football_tv", from_unixtime(unix_timestamp(col("fx_football_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_football_tv", datediff(col("ref_date"), col("fx_football_tv")).cast("double"))\
    .withColumn("dias_desde_fx_football_tv", when(isnull(col("dias_desde_fx_football_tv")) | isnan(col("dias_desde_fx_football_tv")), -1.0).otherwise(col("dias_desde_fx_football_tv")))\
    .withColumn("isfutbol", when(col("fx_football_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("fx_motor_tv", from_unixtime(unix_timestamp(col("fx_motor_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_motor_tv", datediff(col("ref_date"), col("fx_motor_tv")).cast("double"))\
    .withColumn("dias_desde_fx_motor_tv", when(isnull(col("dias_desde_fx_motor_tv")) | isnan(col("dias_desde_fx_motor_tv")), -1.0).otherwise(col("dias_desde_fx_motor_tv")))\
    .withColumn("ismotor", when(col("fx_motor_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("fx_pvr_tv", from_unixtime(unix_timestamp(col("fx_pvr_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_pvr_tv", datediff(col("ref_date"), col("fx_pvr_tv")).cast("double"))\
    .withColumn("dias_desde_fx_pvr_tv", when(isnull(col("dias_desde_fx_pvr_tv")) | isnan(col("dias_desde_fx_pvr_tv")), -1.0).otherwise(col("dias_desde_fx_pvr_tv")))\
    .withColumn("ispvr", when(col("fx_pvr_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("fx_zapper_tv", from_unixtime(unix_timestamp(col("fx_zapper_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_zapper_tv", datediff(col("ref_date"), col("fx_zapper_tv")).cast("double"))\
    .withColumn("dias_desde_fx_zapper_tv", when(isnull(col("dias_desde_fx_zapper_tv")) | isnan(col("dias_desde_fx_zapper_tv")), -1.0).otherwise(col("dias_desde_fx_zapper_tv")))\
    .withColumn("iszapper", when(col("fx_zapper_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("fx_trybuy_tv", from_unixtime(unix_timestamp(col("fx_trybuy_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_trybuy_tv", datediff(col("ref_date"), col("fx_trybuy_tv")).cast("double"))\
    .withColumn("dias_desde_fx_trybuy_tv", when(isnull(col("dias_desde_fx_trybuy_tv")) | isnan(col("dias_desde_fx_trybuy_tv")), -1.0).otherwise(col("dias_desde_fx_trybuy_tv")))\
    .withColumn("istrybuy", when(col("fx_trybuy_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("fx_trybuy_autom_tv", from_unixtime(unix_timestamp(col("fx_trybuy_autom_tv"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_trybuy_autom_tv", datediff(col("ref_date"), col("fx_trybuy_autom_tv")).cast("double"))\
    .withColumn("dias_desde_fx_trybuy_autom_tv", when(isnull(col("dias_desde_fx_trybuy_autom_tv")) | isnan(col("dias_desde_fx_trybuy_autom_tv")), -1.0).otherwise(col("dias_desde_fx_trybuy_autom_tv")))\
    .withColumn("istrybuy_autom", when(col("fx_trybuy_autom_tv").isNotNull(), 1.0).otherwise(0.0))\
    .withColumn("ishz", when(isnull(col("homezone"))==False, 1.0).otherwise(0.0))\
    .withColumn("total_num_services", lit(0.0))\
    .withColumn("total_num_services", col("bam_services") + col("bam-movil_services") + col("fbb_services") + col("fixed_services") + col("movil_services") + col("prepaid_services") + col("tv_services")).withColumn("fx_srv_basic", from_unixtime(unix_timestamp(col("fx_srv_basic"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_srv_basic", datediff(col("ref_date"), col("fx_srv_basic")).cast("double"))\
    .withColumn("dias_desde_fx_srv_basic", when(isnull(col("dias_desde_fx_srv_basic")) | isnan(col("dias_desde_fx_srv_basic")), -1.0).otherwise(col("dias_desde_fx_srv_basic")))\
    .withColumn("price_srv_basic", when(((isnull(col("price_srv_basic"))) | (isnan(col("price_srv_basic")))) & (col("rgu") == "movil"), avg_price_srv_basic_dict['movil']).otherwise(col("price_srv_basic")))\
    .withColumn("price_srv_basic", when(((isnull(col("price_srv_basic"))) | (isnan(col("price_srv_basic")))) & (col("rgu") == "bam"), avg_price_srv_basic_dict['bam']).otherwise(col("price_srv_basic")))\
    .withColumn("price_srv_basic", when(((isnull(col("price_srv_basic"))) | (isnan(col("price_srv_basic")))) & (col("rgu") == "fbb"), avg_price_srv_basic_dict['fbb']).otherwise(col("price_srv_basic")))\
    .withColumn("price_srv_basic", when(((isnull(col("price_srv_basic"))) | (isnan(col("price_srv_basic")))) & (col("rgu") == "fixed"), avg_price_srv_basic_dict['fixed']).otherwise(col("price_srv_basic")))\
    .withColumn("price_srv_basic", when(((isnull(col("price_srv_basic"))) | (isnan(col("price_srv_basic")))) & (col("rgu") == "tv"), avg_price_srv_basic_dict['tv']).otherwise(col("price_srv_basic")))

    origindf_tmp6 = origindf_tmp5\
    .withColumn("fx_dto_lev1", from_unixtime(unix_timestamp(col("fx_dto_lev1"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_dto_lev1", datediff(col("ref_date"), col("fx_dto_lev1")).cast("double"))\
    .withColumn("dias_desde_fx_dto_lev1", when(isnull(col("dias_desde_fx_dto_lev1")) | isnan(col("dias_desde_fx_dto_lev1")) | (col("dias_desde_fx_dto_lev1") > 365.0*50.0), -1.0).otherwise(col("dias_desde_fx_dto_lev1")))\
    .withColumn("price_dto_lev1", when(col("price_dto_lev1").isin("", " ") | isnull(col("price_dto_lev1")), 0.0).otherwise(col("price_dto_lev1")))\
    .withColumn("fx_dto_lev2", from_unixtime(unix_timestamp(col("fx_dto_lev2"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_dto_lev2", datediff(col("ref_date"), col("fx_dto_lev2")).cast("double"))\
    .withColumn("dias_desde_fx_dto_lev2", when(isnull(col("dias_desde_fx_dto_lev2")) | isnan(col("dias_desde_fx_dto_lev2")) | (col("dias_desde_fx_dto_lev2") > 365.0*50.0), -1.0).otherwise(col("dias_desde_fx_dto_lev2")))\
    .withColumn("price_dto_lev2", when(col("price_dto_lev2").isin("", " ") | isnull(col("price_dto_lev2")), 0.0).otherwise(col("price_dto_lev2")))\
    .withColumn("segunda_linea", lit(0.0))\
    .withColumn("segunda_linea", when((col("desc_tariff").like("%2%") & col("desc_tariff").like("%nea%")) | (col("desc_tariff").like("%+%")), 1.0).otherwise(col("segunda_linea")))\
    .withColumn("tariff_unknown", when((col("desc_tariff").isin("", " ")) | (isnull(col("desc_tariff"))), 1.0).otherwise(0.0))\
    .withColumn("tariff_xs", when(col("desc_tariff").like("%XS%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_redm", when(col("desc_tariff").like("%Red M%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_redl", when(col("desc_tariff").like("%Red L%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_smart", when(col("desc_tariff").like("%Smart%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_minim", when(col("desc_tariff").like("%Mini M%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_plana200min", when(col("desc_tariff").like("%Tarifa Plana 200 min%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_planaminilim", when(col("desc_tariff").like("%Tarifa Plana Minutos Ilimitados%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_maslineasmini", when((col("desc_tariff").like("%+L%")) & (col("desc_tariff").like("%neas Mini%")), 1.0).otherwise(0.0))\
    .withColumn("tariff_megayuser", when(col("desc_tariff").like("%egayuser%"), 1.0).otherwise(0.0))\
    .withColumn("tariff_otros", when((col("tariff_unknown") + col("tariff_xs") + col("tariff_redm") + col("tariff_redl") + col("tariff_smart") + col("tariff_minim") + col("tariff_plana200min") + col("tariff_planaminilim") + col("tariff_maslineasmini") + col("tariff_megayuser"))==0.0, 1.0).otherwise(0.0))\
    .withColumn("price_tariff", when((col("rgu").isin("movil", "bam", "bam-movil") == False), 0.0).otherwise(col("price_tariff")))\
    .withColumn("price_tariff", when((col("rgu").isin("movil", "bam", "bam-movil")) & (isnull(col("price_tariff"))), avg_price_tariff).otherwise(col("price_tariff")))\
    .withColumn("tv_total_charges", when(isnull(col("tv_total_charges")), 0.0).otherwise(col("tv_total_charges")))\
    .withColumn("dias_hasta_penal_cust_pending_n1_end_date", datediff(from_unixtime(unix_timestamp(col("penal_cust_pending_n1_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_cust_pending_n1_penal_amount", when(isnull(col("penal_cust_pending_n1_penal_amount")), 0.0).otherwise(col("penal_cust_pending_n1_penal_amount")))\
    .withColumn("dias_hasta_penal_cust_pending_n2_end_date", datediff(from_unixtime(unix_timestamp(col("penal_cust_pending_n2_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_cust_pending_n2_penal_amount", when(isnull(col("penal_cust_pending_n2_penal_amount")), 0.0).otherwise(col("penal_cust_pending_n2_penal_amount")))\
    .withColumn("dias_hasta_penal_cust_pending_n3_end_date", datediff(from_unixtime(unix_timestamp(col("penal_cust_pending_n3_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_cust_pending_n3_penal_amount", when(isnull(col("penal_cust_pending_n3_penal_amount")), 0.0).otherwise(col("penal_cust_pending_n3_penal_amount")))\
    .withColumn("dias_hasta_penal_cust_pending_n4_end_date", datediff(from_unixtime(unix_timestamp(col("penal_cust_pending_n4_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_cust_pending_n4_penal_amount", when(isnull(col("penal_cust_pending_n4_penal_amount")), 0.0).otherwise(col("penal_cust_pending_n4_penal_amount")))

    origindf_tmp = origindf_tmp6\
    .withColumn("dias_hasta_penal_cust_pending_n5_end_date", datediff(from_unixtime(unix_timestamp(col("penal_cust_pending_n5_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_cust_pending_n5_penal_amount", when(isnull(col("penal_cust_pending_n5_penal_amount")), 0.0).otherwise(col("penal_cust_pending_n5_penal_amount")))\
    .withColumn("dias_hasta_penal_srv_pending_n1_end_date", datediff(from_unixtime(unix_timestamp(col("penal_srv_pending_n1_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_srv_pending_n1_penal_amount", when(isnull(col("penal_srv_pending_n1_penal_amount")), 0.0).otherwise(col("penal_srv_pending_n1_penal_amount")))\
    .withColumn("dias_hasta_penal_srv_pending_n2_end_date", datediff(from_unixtime(unix_timestamp(col("penal_srv_pending_n2_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_srv_pending_n2_penal_amount", when(isnull(col("penal_srv_pending_n2_penal_amount")), 0.0).otherwise(col("penal_srv_pending_n2_penal_amount")))\
    .withColumn("dias_hasta_penal_srv_pending_n3_end_date", datediff(from_unixtime(unix_timestamp(col("penal_srv_pending_n3_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_srv_pending_n3_penal_amount", when(isnull(col("penal_srv_pending_n3_penal_amount")), 0.0).otherwise(col("penal_srv_pending_n3_penal_amount")))\
    .withColumn("dias_hasta_penal_srv_pending_n4_end_date", datediff(from_unixtime(unix_timestamp(col("penal_srv_pending_n4_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_srv_pending_n4_penal_amount", when(isnull(col("penal_srv_pending_n4_penal_amount")), 0.0).otherwise(col("penal_srv_pending_n4_penal_amount")))\
    .withColumn("dias_hasta_penal_srv_pending_n5_end_date", datediff(from_unixtime(unix_timestamp(col("penal_srv_pending_n5_end_date"), "yyyyMMdd")), col("ref_date") ).cast("double") )\
    .withColumn("penal_srv_pending_n5_penal_amount", when(isnull(col("penal_srv_pending_n5_penal_amount")), 0.0).otherwise(col("penal_srv_pending_n5_penal_amount")))\
    .withColumn("max_dias_hasta_penal_cust_pending_end_date", greatest(col("dias_hasta_penal_cust_pending_n1_end_date"), col("dias_hasta_penal_cust_pending_n2_end_date"), col("dias_hasta_penal_cust_pending_n3_end_date"), col("dias_hasta_penal_cust_pending_n4_end_date"), col("dias_hasta_penal_cust_pending_n5_end_date")) )\
    .withColumn("min_dias_hasta_penal_cust_pending_end_date", least(col("dias_hasta_penal_cust_pending_n1_end_date"), col("dias_hasta_penal_cust_pending_n2_end_date"), col("dias_hasta_penal_cust_pending_n3_end_date"), col("dias_hasta_penal_cust_pending_n4_end_date"), col("dias_hasta_penal_cust_pending_n5_end_date")))\
    .withColumn("max_dias_hasta_penal_srv_pending_end_date", greatest(col("dias_hasta_penal_srv_pending_n1_end_date"), col("dias_hasta_penal_srv_pending_n2_end_date"), col("dias_hasta_penal_srv_pending_n3_end_date"), col("dias_hasta_penal_srv_pending_n4_end_date"), col("dias_hasta_penal_srv_pending_n5_end_date")) )\
    .withColumn("min_dias_hasta_penal_srv_pending_end_date", least(col("dias_hasta_penal_srv_pending_n1_end_date"), col("dias_hasta_penal_srv_pending_n2_end_date"), col("dias_hasta_penal_srv_pending_n3_end_date"), col("dias_hasta_penal_srv_pending_n4_end_date"), col("dias_hasta_penal_srv_pending_n5_end_date")))\
    .withColumn("fbb_upgrade", when((col("fbb_upgrade").isin("", " ")) | (isnull(col("fbb_upgrade"))), "unknown").otherwise(col("fbb_upgrade")))\
    .withColumn("fbb_upgrade", when(col("fbb_upgrade").isin("unknown", "UP50M", "UP65M", "UP120", "UP030", "UP300") == False, "otros").otherwise(col("fbb_upgrade")))\
    .withColumn("fx_fbb_upgrade", from_unixtime(unix_timestamp(col("fx_fbb_upgrade"), "yyyyMMdd")))\
    .withColumn("dias_desde_fx_fbb_upgrade", datediff(col("ref_date"), col("fx_fbb_upgrade")).cast("double"))\
    .withColumn("dias_desde_fx_fbb_upgrade", when(isnull(col("dias_desde_fx_fbb_upgrade")) | isnan(col("dias_desde_fx_fbb_upgrade")), -1.0).otherwise(col("dias_desde_fx_fbb_upgrade")))\
    .na.fill({'price_football_tv': 0.0, 'price_motor_tv': 0.0, 'price_pvr_tv': 0.0, 'price_zapper_tv': 0.0, 'price_trybuy_tv': 0.0, 'price_trybuy_autom_tv': 0.0})\
    .filter(col("num_cliente").isNotNull())

    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the original df for " + refdate + ": " + str(origindf_tmp.count()))

    origindf_tmp.repartition(200).write.save('/data/udf/vf_es/churn/fbb_tmp/origindf_tmp_' + refdate, format='parquet', mode='overwrite')

    print "[Info FbbChurn] " + time.ctime() + " Saving origindf_tmp to HDFS"

    origindf = spark.read.parquet('/data/udf/vf_es/churn/fbb_tmp/origindf_tmp_' + refdate)

    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the origindf after load from HDFS: " + str(origindf.count()))
    
    # Counting on columns related to tariff should be done by using pivot (num_cliente aff count) and joining by num_cliente

    window = Window.partitionBy("num_cliente")

    numclidf_tmp = origindf\
    .withColumn("max_dias_desde_fx_football_tv", sql_max("dias_desde_fx_football_tv").over(window))\
    .withColumn("football_services", sql_sum("isfutbol").over(window))\
    .withColumn("total_price_football", sql_sum("price_football_tv").over(window))\
    .withColumn("max_dias_desde_fx_motor_tv", sql_max("dias_desde_fx_motor_tv").over(window))\
    .withColumn("motor_services", sql_sum("ismotor").over(window))\
    .withColumn("total_price_motor", sql_sum("price_motor_tv").over(window))\
    .withColumn("max_dias_desde_fx_pvr_tv", sql_max("dias_desde_fx_pvr_tv").over(window))\
    .withColumn("pvr_services", sql_sum("ispvr").over(window))\
    .withColumn("total_price_pvr", sql_sum("price_pvr_tv").over(window))\
    .withColumn("max_dias_desde_fx_zapper_tv", sql_max("dias_desde_fx_zapper_tv").over(window))\
    .withColumn("zapper_services", sql_sum("iszapper").over(window))\
    .withColumn("total_price_zapper", sql_sum("price_zapper_tv").over(window))\
    .withColumn("max_dias_desde_fx_trybuy_tv", sql_max("dias_desde_fx_trybuy_tv").over(window))\
    .withColumn("trybuy_services", sql_sum("istrybuy").over(window))\
    .withColumn("total_price_trybuy", sql_sum("price_trybuy_tv").over(window))\
    .withColumn("max_dias_desde_fx_trybuy_autom_tv", sql_max("dias_desde_fx_trybuy_autom_tv").over(window))\
    .withColumn("trybuy_autom_services", sql_sum("istrybuy_autom").over(window))\
    .withColumn("total_price_trybuy_autom", sql_sum("price_trybuy_autom_tv").over(window))\
    .withColumn("hz_services", sql_sum("ishz").over(window))\
    .withColumn("min_dias_desde_fx_srv_basic", sql_min("dias_desde_fx_srv_basic").over(window))\
    .withColumn("max_dias_desde_fx_srv_basic", sql_max("dias_desde_fx_srv_basic").over(window))\
    .withColumn("mean_dias_desde_fx_srv_basic", sql_avg("dias_desde_fx_srv_basic").over(window))\
    .withColumn("total_price_srv_basic", sql_sum("price_srv_basic").over(window))\
    .withColumn("mean_price_srv_basic", sql_avg("price_srv_basic").over(window))\
    .withColumn("max_price_srv_basic", sql_max("price_srv_basic").over(window))\
    .withColumn("min_price_srv_basic", sql_min("price_srv_basic").over(window))\
    .withColumn("min_dias_desde_fx_dto_lev1", sql_min("dias_desde_fx_dto_lev1").over(window))\
    .withColumn("min_dias_desde_fx_dto_lev2", sql_min("dias_desde_fx_dto_lev2").over(window))\
    .withColumn("max_dias_desde_fx_dto_lev1", sql_max("dias_desde_fx_dto_lev1").over(window))\
    .withColumn("max_dias_desde_fx_dto_lev2", sql_max("dias_desde_fx_dto_lev2").over(window))\
    .withColumn("total_price_dto_lev1", sql_sum("price_dto_lev1").over(window))\
    .withColumn("total_price_dto_lev2", sql_sum("price_dto_lev2").over(window))\
    .withColumn("num_2lins", sql_sum("segunda_linea").over(window))\
    .withColumn("num_tariff_unknown", sql_sum("tariff_unknown").over(window))\
    .withColumn("num_tariff_xs", sql_sum("tariff_xs").over(window))\
    .withColumn("num_tariff_redm", sql_sum("tariff_redm").over(window))\
    .withColumn("num_tariff_redl", sql_sum("tariff_redl").over(window))\
    .withColumn("num_tariff_smart", sql_sum("tariff_smart").over(window))\
    .withColumn("num_tariff_minim", sql_sum("tariff_minim").over(window))\
    .withColumn("num_tariff_plana200min", sql_sum("tariff_plana200min").over(window))\
    .withColumn("num_tariff_planaminilim", sql_sum("tariff_planaminilim").over(window))\
    .withColumn("num_tariff_maslineasmini", sql_sum("tariff_maslineasmini").over(window))\
    .withColumn("num_tariff_megayuser", sql_sum("tariff_megayuser").over(window))\
    .withColumn("num_tariff_otros", sql_sum("tariff_otros").over(window))\
    .withColumn("total_price_tariff", sql_sum("price_tariff").over(window))\
    .withColumn("max_price_tariff", sql_max("price_tariff").over(window))\
    .withColumn("min_price_tariff", sql_min("price_tariff").over(window))\
    .withColumn("total_tv_total_charges", sql_sum("tv_total_charges").over(window))\
    .withColumn("total_penal_cust_pending_n1_penal_amount", sql_max(col("penal_cust_pending_n1_penal_amount")).over(window))\
    .withColumn("total_penal_cust_pending_n2_penal_amount", sql_max(col("penal_cust_pending_n2_penal_amount")).over(window))\
    .withColumn("total_penal_cust_pending_n3_penal_amount", sql_max(col("penal_cust_pending_n3_penal_amount")).over(window))\
    .withColumn("total_penal_cust_pending_n4_penal_amount", sql_max(col("penal_cust_pending_n4_penal_amount")).over(window))\
    .withColumn("total_penal_cust_pending_n5_penal_amount", sql_max(col("penal_cust_pending_n5_penal_amount")).over(window))\
    .withColumn("total_penal_srv_pending_n1_penal_amount", sql_max(col("penal_srv_pending_n1_penal_amount")).over(window))\
    .withColumn("total_penal_srv_pending_n2_penal_amount", sql_max(col("penal_srv_pending_n2_penal_amount")).over(window))\
    .withColumn("total_penal_srv_pending_n3_penal_amount", sql_max(col("penal_srv_pending_n3_penal_amount")).over(window))\
    .withColumn("total_penal_srv_pending_n4_penal_amount", sql_max(col("penal_srv_pending_n4_penal_amount")).over(window))\
    .withColumn("total_penal_srv_pending_n5_penal_amount", sql_max(col("penal_srv_pending_n5_penal_amount")).over(window))\
    .withColumn("total_max_dias_hasta_penal_cust_pending_end_date", when(col("max_dias_hasta_penal_cust_pending_end_date").isNotNull() , sql_max(col("max_dias_hasta_penal_cust_pending_end_date")).over(window)).otherwise(10000.0))\
    .withColumn("total_min_dias_hasta_penal_cust_pending_end_date", when(col("min_dias_hasta_penal_cust_pending_end_date").isNotNull() , sql_min(col("min_dias_hasta_penal_cust_pending_end_date")).over(window)).otherwise(10000.0))\
    .withColumn("total_max_dias_hasta_penal_srv_pending_end_date", when(col("max_dias_hasta_penal_srv_pending_end_date").isNotNull() , sql_max(col("max_dias_hasta_penal_srv_pending_end_date")).over(window)).otherwise(10000.0))\
    .withColumn("total_min_dias_hasta_penal_srv_pending_end_date", when(col("min_dias_hasta_penal_srv_pending_end_date").isNotNull() , sql_min(col("min_dias_hasta_penal_srv_pending_end_date")).over(window)).otherwise(10000.0))\
    .withColumnRenamed('campo2', 'msisdn_d').filter(col("num_cliente").isNotNull())\
    .select(getIdFeats() + getCrmFeats())#.filter(col("rgu")=="fbb")
    #.filter((col('msisdn_d').isNotNull()) & (col('campo1').isNotNull()))


    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the numclidf for " + refdate + " - Num rows: " + str(numclidf_tmp.count()) + " - Num columns: " + str(len(numclidf_tmp.columns)))

    numclidf_tmp.repartition(200).write.save('/data/udf/vf_es/churn/fbb_tmp/numclidf_tmp_' + refdate, format='parquet', mode='overwrite')

    print "[Info FbbChurn] " + time.ctime() + " Saving numclidf_tmp to HDFS"

    numclidf = spark.read.parquet('/data/udf/vf_es/churn/fbb_tmp/numclidf_tmp_' + refdate)

    billingdf = get_billing_df(spark, origin, refdate)

    tmpdf = numclidf\
    .join(billingdf, ["num_cliente"], "left_outer")\
    .na.fill(-1.0)

    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the tmpdf for " + refdate + " - Num rows: " + str(tmpdf.count()) + " - Num columns: " + str(len(tmpdf.columns)))

    mobspinnersdf = get_mobile_spinners_df(spark, refdate)

    tmp2df = tmpdf\
    .join(mobspinnersdf, ["nif_cliente"], "left_outer")\
    .na.fill(0.0)

    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the featdf for " + refdate + " - Num rows: " + str(tmp2df.count()) + " - Num columns: " + str(len(tmp2df.columns)))

    ordersdf = get_orders_df(spark, origin, refdate)

    featdf = tmp2df\
    .join(ordersdf, ["num_cliente"], "left_outer")\
    .na.fill({"days_since_last_order": 10000, "days_since_first_order": 10000})

    print("[Info getCarNumClienteDf] " + time.ctime() + " Size of the featdf for " + refdate + " - Num rows: " + str(featdf.count()) + " - Num columns: " + str(len(featdf.columns)))


    return featdf

def addExtraFeatsBis(df):

    window = Window.partitionBy("num_cliente")
    columnSel = ['msisdn', 'num_cliente','rgu']

    colNum = df.columns

    for ccol in colNum:

        if 'netapps_ns_' in ccol:
            applic = ['whatsapp', 'instagram', 'facebook', 'googleplay', 'instagram']
            if any(app in ccol.lower() for app in applic):

                df = df.withColumn(ccol + '_total', sql_sum(ccol).over(window))
                columnSel.append(ccol+'_total')

        competitors = ['ORANGE', 'JAZZTEL', 'MOVISTAR', 'MASMOVIL', 'YOIGO', 'VODAFONE', 'LOWI', 'O2', 'PEPEPHONE']
        if any(app in ccol for app in competitors) and ('ns_' not in ccol):

            df = df.withColumn(ccol + '_total', sql_sum(ccol).over(window))
            columnSel.append(ccol + '_total')


        if 'tgs_' in ccol:
            columnSel.append(ccol)
        if 'orders_' in ccol:
            columnSel.append(ccol)
        if ('ccc_num' in ccol):
            df = df.withColumn(ccol + '_total', sql_sum(ccol).over(window))
            columnSel.append(ccol + '_total')
            columnSel.append(ccol)

    colFinal=[ccol for ccol in columnSel if 'netapps_ns_' not in ccol ]

    dfSelGrouped=df.select(columnSel)
    dfSelGrouped=dfSelGrouped.cache()

    dfSelGrouped = dfSelGrouped.filter(col('rgu') == 'fbb')
    print ("[Info FbbChurn] " + time.ctime() + " The module for the Extra Feats has been run. Count for the table", dfSelGrouped.count())

    return dfSelGrouped,columnSel


def addExtraFeats(df):

    columnSel = ['msisdn', 'num_cliente','rgu']

    colNum = df.columns
    operaciones=[]

    for ccol in colNum:

        if 'netapps_ns_' in ccol:
            applic = ['whatsapp', 'instagram', 'facebook', 'googleplay', 'instagram']
            if any(app in ccol.lower() for app in applic):

                operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
                columnSel.append(ccol)

        competitors = ['ORANGE', 'JAZZTEL', 'MOVISTAR', 'MASMOVIL', 'YOIGO', 'VODAFONE', 'LOWI', 'O2', 'PEPEPHONE']
        if any(app in ccol for app in competitors):
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            columnSel.append(ccol)

        if 'tgs_' in ccol:
            columnSel.append(ccol)
        if 'orders_' in ccol:
            columnSel.append(ccol)
        if ('ccc_num' in ccol):
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            columnSel.append(ccol)

    colFinal=[ccol for ccol in columnSel if 'netapps_ns_' not in ccol ]

    dfSelGrouped = df.select(columnSel).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped = dfSelGrouped.join(df.select(colFinal), on='num_cliente', how='inner')
    dfSelGrouped = dfSelGrouped.filter(col('rgu') == 'fbb')
    print ("[Info FbbChurn] " + time.ctime() + " The module for the Extra Feats has been run. Count for the table", dfSelGrouped.count())

    return dfSelGrouped,columnSel


def addExtraFeatsEvol(df):

    ### MODULO DE CCC
    dfCCC, selColumnasCCC = selectCCC(df)

    ### MODULO DE NS
    dfNS, selColumnasNS = selectNS(df)

    ### MODULO DE TGS
    dfTGS, selColumnasTGS = selectTGS(df)

    ### MODULO DE Orders
    dfOrder, selColumnasOrder = selectOrders(df)

    ### MODULO DE COMPETITORS
    dfComp, selColumnasComp = selectCompetitors(df)

    ### DEVICES
    dfDev, selColumnasDev = selectDev(df)

    ### PBMS
    dfPmbs, selColumnasPmbs = selectPbms(df)

    ### ADDITIONALS
    dfAdd, selColumnasAdd = selectAdd(df)

    ### CAMPAIGNS
    dfCamp, selColumnasCamp = selectCamp(df)

    ## INCREMENTAL
    dfIncr, selColumnasIncr= selectIncrem(df)


    dfTotal= dfCCC.join(dfNS, ["num_cliente",'msisdn','rgu'], how="inner")
    dfTotal = dfTotal.join(dfTGS, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfOrder, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfComp, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfDev, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfPmbs, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfAdd, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfCamp, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal = dfTotal.join(dfIncr, ["num_cliente", 'msisdn', 'rgu'], how="inner")
    dfTotal= dfTotal.cache()

    print ("[Info FbbChurn] " + time.ctime() + " The extra feats have been joined. Number of clientes ", dfTotal.count())

    selColumnas=[]
    selColumnas.extend(selColumnasCCC)
    selColumnas.extend(selColumnasNS)
    selColumnas.extend(selColumnasTGS)
    selColumnas.extend(selColumnasOrder)
    selColumnas.extend(selColumnasComp)
    selColumnas.extend(selColumnasDev)
    selColumnas.extend(selColumnasPmbs)
    selColumnas.extend(selColumnasAdd)
    selColumnas.extend(selColumnasCamp)
    selColumnas.extend(selColumnasIncr)

    return dfTotal,selColumnas



def selectCCC(df):

    print "[Info FbbChurn] " + time.ctime() +" Inside CCC module of the Extra Feats "

    varCliente=['num_cliente','msisdn','rgu']

    # Seleccionamos las columnas ccc
    totalColumns = [ccol for ccol in df.columns if 'ccc_' in ccol]
    dfSel = df.select(totalColumns + ['num_cliente', 'msisdn', 'rgu'])

    # Nos quedamos con las variables que son numéricas
    catCols=[item[0] for item in dfSel.dtypes if item[1].startswith('string')]
    colNum=list(set(dfSel.columns)-set(catCols))+varCliente

    print "[Info FbbChurn] " + time.ctime() +" Number of columns processed: " +str(len(colNum))

    dfSel = dfSel.select(colNum).cache()
    print "[Info FbbChurn] Number of rows: " +str(dfSel.count())

    selColumnas=varCliente
    window = Window.partitionBy("num_cliente")

    operaciones=[]

    for ccol in dfSel.columns:

        if 'ccc_' in ccol:

            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            selColumnas.append(ccol)

            if 'num' in ccol: selColumnas.append(ccol)

    dfSelGrouped=dfSel.select(selColumnas).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped=dfSelGrouped.join(dfSel.select(['num_cliente','msisdn','rgu']),'num_cliente','outer')
    dfSelGrouped = dfSelGrouped.filter(col('rgu') == 'fbb')

    dfSelGrouped=dfSelGrouped.cache()
    print "[Info FbbChurn] " + time.ctime() + " The module for CCC has been run. Number of clients ",dfSelGrouped.count()

    return dfSelGrouped,selColumnas


def selectNS(df):

    print("[Info FbbChurn] " + time.ctime() +" Inside NS module of the Extra Feats ")

    varCliente=['num_cliente','msisdn','rgu']

    # Seleccionamos las columnas NS
    totalColumns = [ccol for ccol in df.columns if 'netapps_ns_' in ccol]
    dfSel = df.select(totalColumns + ['num_cliente', 'msisdn', 'rgu']).cache()

    # Nos quedamos con las variables que son numéricas
    catCols=[item[0] for item in dfSel.dtypes if item[1].startswith('string')]
    colNum=list(set(dfSel.columns)-set(catCols))+varCliente

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(colNum))

    dfSel = dfSel.select(colNum)

    selColumnas=varCliente
    window = Window.partitionBy("num_cliente")

    operaciones = []

    for ccol in dfSel.columns:

        if 'netapps_ns_' in ccol:
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            selColumnas.append(ccol)

    dfSelGrouped = dfSel.select(selColumnas).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped = dfSelGrouped.join(dfSel.select(['num_cliente', 'msisdn', 'rgu']), 'num_cliente', 'inner')
    dfSelGrouped = dfSelGrouped.filter(col('rgu') == 'fbb')

    dfSelGrouped = dfSelGrouped.cache()
    print "[Info FbbChurn] " + time.ctime() + " The module for NS has been run. Number of clients ", dfSelGrouped.count()

    return dfSelGrouped, selColumnas

def selectTGS(df):

    print "[Info FbbChurn] " + time.ctime() + " Inside TGS module of the Extra Feats "

    # Seleccionamos las columnas TGS
    totalColumns = [ccol for ccol in df.columns if 'tgs' in ccol]
    totalColumns.append('num_cliente')
    totalColumns.append('msisdn')
    totalColumns.append('rgu')

    dfSel = df.select(totalColumns).cache()

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(totalColumns))

    dfSel = dfSel.select(totalColumns).filter(col('rgu') == 'fbb')
    print "[Info FbbChurn] " + time.ctime() + " The module for TGS has been run. Number of clientes ", dfSel.count()

    return dfSel, totalColumns

def selectOrders(df):

    print "[Info FbbChurn] " + time.ctime() + " Inside Orders module of the Extra Feats "

    # Seleccionamos las columnas Orders
    totalColumns = [ccol for ccol in df.columns if ('order' in ccol) and ('ccc_' not in ccol)]
    totalColumns.append('num_cliente')
    totalColumns.append('msisdn')
    totalColumns.append('rgu')

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(totalColumns))

    dfSel = df.select(totalColumns).cache()

    dfSel = dfSel.select(totalColumns).filter(col('rgu') == 'fbb')
    print "[Info FbbChurn] " + time.ctime() + " The module for Orders has been run. Number of clientes ", dfSel.count()

    return dfSel, totalColumns

def selectPbms(df):

    print("[Info FbbChurn] " + time.ctime() + " Inside Pbms module of the Extra Feats ")

    # Seleccionamos las columnas pbms
    totalColumns = [ccol for ccol in df.columns if ('pbms_' in ccol)]
    totalColumns.append('num_cliente')
    totalColumns.append('msisdn')
    totalColumns.append('rgu')

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(totalColumns))

    dfSel = df.select(totalColumns).cache()

    dfSel = dfSel.select(totalColumns).filter(col('rgu') == 'fbb')
    print "[Info FbbChurn] " + time.ctime() + " The module for Pbms has been run. Number of clientes ", dfSel.count()

    return dfSel, totalColumns


def selectAdd(df):

    print "[Info FbbChurn] " + time.ctime() + " Inside Additional module of the Extra Feats "

    # Seleccionamos las columnas Additional
    totalColumns = [ccol for ccol in df.columns if ('additional_' in ccol)]
    totalColumns.append('num_cliente')
    totalColumns.append('msisdn')
    totalColumns.append('rgu')

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(totalColumns))

    dfSel = df.select(totalColumns).cache()

    dfSel = dfSel.select(totalColumns).filter(col('rgu') == 'fbb')
    print "[Info FbbChurn] " + time.ctime() + " The module for Additional has been run. Number of clientes ", dfSel.count()

    return dfSel, totalColumns

def selectCamp(df):

    print "[Info FbbChurn] " + time.ctime() + " Inside Campaigns module of the Extra Feats "

    # Seleccionamos las columnas Additional
    totalColumns = [ccol for ccol in df.columns if ('campaigns_' in ccol)]
    totalColumns.append('num_cliente')
    totalColumns.append('msisdn')
    totalColumns.append('rgu')

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(totalColumns))

    dfSel = df.select(totalColumns).cache()

    dfSel = dfSel.select(totalColumns).filter(col('rgu') == 'fbb')
    print "[Info FbbChurn] " + time.ctime() + " The module for Campaigns has been run. Number of clientes ", dfSel.count()

    return dfSel, totalColumns



def selectCompetitors(df):

    print "[Info FbbChurn] " + time.ctime() + " Inside Competitors module of the Extra Feats "

    varCliente = ['num_cliente', 'msisdn', 'rgu']

    # Seleccionamos las columnas De Navegacion a competidores
    totalColumns = [ccol for ccol in df.columns if 'navigation' in ccol]
    dfSel = df.select(totalColumns + ['num_cliente', 'msisdn', 'rgu']).cache()

    # Nos quedamos con las variables que son numéricas
    catCols = [item[0] for item in dfSel.dtypes if item[1].startswith('string')]
    colNum = list(set(dfSel.columns) - set(catCols)) + varCliente

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(colNum))

    dfSel = df.select(colNum)

    operaciones = []

    selColumnas = varCliente
    window = Window.partitionBy("num_cliente")
    for ccol in dfSel.columns:

        if 'navigation_' in ccol:
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            selColumnas.append(ccol)

    dfSelGrouped = dfSel.select(selColumnas).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped = dfSelGrouped.join(dfSel.select(['num_cliente', 'msisdn', 'rgu']), 'num_cliente', 'outer')
    dfSelGrouped = dfSelGrouped.filter(col('rgu') == 'fbb')

    dfSelGrouped = dfSelGrouped.cache()
    print "[Info FbbChurn] " + time.ctime() + " The module for Competitors has been run. Number of clients ", dfSelGrouped.count()

    return dfSelGrouped, selColumnas


def selectDev(df):

    print "[Info FbbChurn] " + time.ctime() +" Inside Device module of the Extra Feats "

    varCliente=['num_cliente','msisdn','rgu']

    # Seleccionamos las columnas device
    totalColumns = [ccol for ccol in df.columns if ('device_' in ccol)]
    dfSel = df.select(totalColumns + ['num_cliente', 'msisdn', 'rgu'])

    # Nos quedamos con las variables que son numéricas
    catCols=[item[0] for item in dfSel.dtypes if item[1].startswith('string')]
    colNum=list(set(dfSel.columns)-set(catCols))+varCliente

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(colNum))

    dfSel = dfSel.select(colNum).cache()
    print "[Info FbbChurn] Number of rows: " +str(dfSel.count())

    selColumnas=varCliente
    window = Window.partitionBy("num_cliente")

    operaciones=[]

    for ccol in dfSel.columns:
        if ('device_' in ccol) and ('ccc_' not in ccol):
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            selColumnas.append(ccol)

    dfSelGrouped=dfSel.select(selColumnas).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped=dfSelGrouped.join(dfSel.select(['num_cliente','msisdn','rgu']),'num_cliente','outer')
    dfSelGrouped=dfSelGrouped.filter(col('rgu')=='fbb')

    dfSelGrouped=dfSelGrouped.cache()
    print "[Info FbbChurn] " + time.ctime() + " The module for Device has been run. Number of clients ",dfSelGrouped.count()

    return dfSelGrouped,selColumnas


def selectIncrem(df):

    print("[Info FbbChurn] " + time.ctime() +" Inside Incremental module of the Extra Feats ")

    varCliente=['num_cliente','msisdn','rgu']

    # Seleccionamos las columnas device
    totalColumns = [ccol for ccol in df.columns if ('incremental_' in ccol)]
    dfSel = df.select(totalColumns + ['num_cliente', 'msisdn', 'rgu'])

    # Nos quedamos con las variables que son numéricas
    catCols=[item[0] for item in dfSel.dtypes if item[1].startswith('string')]
    colNum=list(set(dfSel.columns)-set(catCols))+varCliente

    print "[Info FbbChurn] " + time.ctime() + " Number of columns processed: " + str(len(colNum))

    dfSel = dfSel.select(colNum).cache()
    print "[Info FbbChurn] Number of rows: " +str(dfSel.count())

    selColumnas=varCliente
    window = Window.partitionBy("num_cliente")

    operaciones=[]

    for ccol in dfSel.columns:
        if 'incremental_' in ccol:
            operaciones.append(sql_sum(ccol).alias(ccol + '_total'))
            selColumnas.append(ccol)

    dfSelGrouped=dfSel.select(selColumnas).groupby('num_cliente').agg(*operaciones)
    dfSelGrouped=dfSelGrouped.join(dfSel.select(['num_cliente','msisdn','rgu']),'num_cliente','outer')
    dfSelGrouped=dfSelGrouped.filter(col('rgu')=='fbb')

    dfSelGrouped=dfSelGrouped.cache()
    print "[Info FbbChurn] " + time.ctime() + " The module for Incremental has been run. Number of clients ",dfSelGrouped.count()

    return dfSelGrouped,selColumnas



def fillNa(spark):

    competitors = ['ORANGE', 'JAZZTEL', 'MOVISTAR', 'MASMOVIL', 'YOIGO', 'VODAFONE', 'LOWI', 'O2', 'PEPEPHONE']

    dfExtraFeatMeta = spark.read.option("delimiter", "|").csv("/data/udf/vf_es/churn/metadata/metadata_v1.1/",header=True)
    dfillNa = dict(dfExtraFeatMeta.rdd.map(lambda x: (x[0], float(x[3]) if x[2] == "double" else x[3])).collect())

    for dKey in dfillNa.keys():
        if ('ccc' in dKey) or ('NS' in dKey):
            dfillNa[dKey+'_total'] = 0

        if any(app.lower() in dKey.lower() for app in competitors):
            dfillNa[dKey + '_total'] = 0

    return dfillNa

def pathExist(path):

    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', path])
    proc.communicate()

    if proc.returncode != 0:
        #print "[Info FbbChurn] " + time.ctime() + " File " + str(path) +" doesn't exist"
        return  False
    else:
        #print "[Info FbbChurn] " + time.ctime() + " File " + str(path) + " exists"
        return True

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

def add_decile(df, perc=0.35):
    df = df.withColumn('risk', lit(0))
    maximo = df.agg({"idx": "max"}).collect()[0][0]

    df = df.withColumn('risk', when(col('idx') < maximo * perc, 1).otherwise(0))

    df_risk = df.where(col('risk') == 1).withColumn('score_decile', col('idx').cast(DoubleType()))
    df_norisk = df.where(col('risk') == 0).withColumn('decile', lit(-1.0))

    df_risk = df_risk.withColumn('model_score3', df_risk.count() - df_risk['idx'])
    df_risk = df_risk.withColumn("model_score3", col("model_score3").cast(DoubleType()))

    discretizer = QuantileDiscretizer(numBuckets=10, inputCol='model_score3', outputCol='decile', relativeError=0)
    df_risk = discretizer.fit(df_risk).transform(df_risk)

    df_risk = df_risk.drop('score_decile')
    df_risk = df_risk.drop('model_score3')

    df_scores = union_all([df_risk, df_norisk])
    df_scores = df_scores.withColumn("decile", col("decile") + 1)
    df_scores = df_scores.drop('idx')

    return df_scores

def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]


#def saveResults(tr_calib_preds_df,tt_calib_preds_df,trmetrics,feat_importance,trcycle_ini,horizon,ttcycle_ini,spark):
def saveResults(tr_calib_preds_df, df_scores, trmetrics, feat_importance, trcycle_ini, horizon, ttcycle_ini,spark):

    df_scores

    tr_roc = trmetrics.areaUnderROC
    tr_avgScore = tr_calib_preds_df.groupBy().mean('model_score').collect()[0].asDict()['avg(model_score)']
    tr_maxScore = tr_calib_preds_df.groupBy().max('model_score').collect()[0].asDict()['max(model_score)']
    tr_minScore = tr_calib_preds_df.groupBy().min('model_score').collect()[0].asDict()['min(model_score)']
    tr_skewScore = tr_calib_preds_df.agg(skewness("model_score")).collect()[0].asDict()['skewness(model_score)']
    tr_kurtosisScore = tr_calib_preds_df.agg(kurtosis("model_score")).collect()[0].asDict()['kurtosis(model_score)']

    tt_avgScore = df_scores.groupBy().mean('model_score').collect()[0].asDict()['avg(model_score)']
    tt_maxScore = df_scores.groupBy().max('model_score').collect()[0].asDict()['max(model_score)']
    tt_minScore = df_scores.groupBy().min('model_score').collect()[0].asDict()['min(model_score)']
    tt_skewScore = df_scores.agg(skewness("model_score")).collect()[0].asDict()['skewness(model_score)']
    tt_kurtosisScore = df_scores.agg(kurtosis("model_score")).collect()[0].asDict()['kurtosis(model_score)']

    # Model Parameters
    ###################
    feat_importance.sort(key=lambda tup: tup[1], reverse=True)
    feat_importance_lst = [tup[0] + ':' + str(tup[1]) for tup in feat_importance[:30]]

    executedAt = datetime.now()
    executedAt_time = str(executedAt.hour) + str(executedAt.minute) + str(executedAt.second)
    date_name=str(executedAt.year)+str(executedAt.month)+str(executedAt.day)

    modelParam = [{'model_name': 'churn_preds_fbb',
                   'executed_at': executedAt.strftime('%Y-%m-%d %H:%M:%S'),
                   'model_level': 'Client Number',
                   'training_closing_date': trcycle_ini,
                   'target': 'port',
                   'model_path': '/data/attributes/vf_es/model_outputs/fbb_churn/model/',
                   'metrics_path': '/data/attributes/vf_es/model_outputs/fbb_churn/metrics/',
                   'metrics_train': ';'.join(["roc=" + str(tr_roc),
                                              "avg_score=" + str(tr_avgScore),
                                              "skewness_score=" + str(tr_skewScore),
                                              "kurtosis_score=" + str(tr_kurtosisScore),
                                              "min_score=" + str(tr_minScore),
                                              "max_score=" + str(tr_maxScore)]),
                   'metrics_test': ';'.join(["avg_score=" + str(tt_avgScore),
                                             "skewness_score=" + str(tt_skewScore),
                                             "kurtosis_score=" + str(tt_kurtosisScore),
                                             "min_score=" + str(tt_minScore),
                                             "max_score=" + str(tt_maxScore)]),
                   'varimp': feat_importance_lst,
                   'algorithm': 'MLLib RandomForestRegressor',
                   'author_login': 'csanc109;asaezco;bgmerin1;jmarcoso',
                   'extra_info': ';'.join(["horizon","risk","decile"]),
                   'scores_extra_info_headers': '-',
                   'year': datetime.strptime(date_name, '%Y%m%d').year,
                   'month': datetime.strptime(date_name, '%Y%m%d').month,
                   'day': datetime.strptime(date_name, '%Y%m%d').day,
                   'time': int(executedAt_time)}]

    schema = StructType([StructField('model_name', StringType(), True),
                         StructField('executed_at', StringType(), True),
                         StructField('model_level', StringType(), True),
                         StructField('training_closing_date', StringType(), True),
                         StructField('target', StringType(), True),
                         StructField('model_path', StringType(), True),
                         StructField('metrics_path', StringType(), True),
                         StructField('metrics_train', StringType(), True),
                         StructField('metrics_test', StringType(), True),
                         StructField('varimp', StringType(), True),
                         StructField('algorithm', StringType(), True),
                         StructField('author_login', StringType(), True),
                         StructField('extra_info', StringType(), True),
                         StructField('scores_extra_info_headers', StringType(), True),
                         StructField('year', IntegerType(), True),
                         StructField('month', IntegerType(), True),
                         StructField('day', IntegerType(), True),
                         StructField('time', IntegerType(), True)])

    dfModelParameters = spark.createDataFrame(modelParam, schema)

    dfModelParameters = dfModelParameters.coalesce(1)

    (dfModelParameters.write.partitionBy('model_name', 'year', 'month', 'day').mode("append").format("parquet")
            .save('/data/attributes/vf_es/model_outputs/model_parameters'))

    # Model Scores
    ##################

    tt_predsOut = (df_scores.withColumn('model_name', lit('churn_preds_fbb'))
                   .withColumn('msisdn', lit('-'))
                   .withColumn('executed_at', lit(executedAt))
                   .withColumn('model_score', df_scores['model_score'].cast('float'))
                   .withColumnRenamed("num_cliente", "client_id")
                   .withColumnRenamed("nif_cliente", "nif")
                   .withColumnRenamed("model_score", "scoring")
                   .withColumn("model_output", col("scoring").cast("string"))
                   .withColumn('model_executed_at', lit(executedAt))
                   .withColumn('predict_closing_date', lit(ttcycle_ini))
                   .withColumn('year', lit(datetime.strptime(date_name, '%Y%m%d').year))
                   .withColumn('month', lit(datetime.strptime(date_name, '%Y%m%d').month))
                   .withColumn('day', lit(datetime.strptime(date_name, '%Y%m%d').day))
                   .withColumn('time', lit(int(str(executedAt.hour) + str(executedAt.minute) + str(executedAt.second))))
                   .withColumn('prediction', lit('-'))
                   .withColumn('extra_info', concat(lit(str(horizon)),lit(';'),df_scores.risk,lit(';'),df_scores.decile)))

    tt_predsOutSave=tt_predsOut.drop('risk')
    tt_predsOutSave=tt_predsOutSave.drop('decile')

    model_name_partition = "churn_preds_fbb"

    tt_predsOutSave = tt_predsOutSave.coalesce(1)

    (tt_predsOutSave.write.partitionBy('model_name', 'year', 'month', 'day').mode("append").format("parquet")
        .save('/data/attributes/vf_es/model_outputs/model_scores/'))