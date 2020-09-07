
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
import math
import re
from pyspark.sql.window import Window
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
                                   count as sql_count,
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
                                   array,
                                   create_map,
                                   randn)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType

#CAR_PATH = "/user/csanc109/projects/triggers/car_exploration/"
CAR_PATH = "/data/udf/vf_es/churn/triggers/car_ini_exploration_model1/"
CAR_PATH_UNLABELED = "/data/udf/vf_es/churn/triggers/car_ini_exploration_unlabeled_model1/"

PARTITION_DATE = "year={}/month={}/day={}"


def get_deciles(df, column, nile = 40):
    print "[Info get_deciles] Computing deciles"
    discretizer = QuantileDiscretizer(numBuckets=nile, inputCol=column, outputCol="decile")
    dtdecile = discretizer.fit(df).transform(df).withColumn("decile", col("decile") + lit(1.0))
    return dtdecile

def get_lift(df, score_col, label_col, nile = 40, refprevalence = None):

    print "[Info get_lift] Computing lift"

    if(refprevalence == None):
        refprevalence = df.select(label_col).rdd.map(lambda r: r[label_col]).mean()

    print "[Info get_lift] Computing lift - Ref Prevalence for class 1: " + str(refprevalence)

    dtdecile = get_deciles(df, score_col, nile)

    result = dtdecile\
    .groupBy("decile")\
    .agg(sql_avg(label_col).alias("prevalence"))\
    .withColumn("refprevalence", lit(refprevalence))\
    .withColumn("lift", col("prevalence")/col("refprevalence"))\
    .withColumn("decile", col("decile").cast("double"))\
    .select("decile", "lift")\
    .rdd\
    .map(lambda r: (r["decile"], r["lift"]))\
    .collect()

    result_ord = sorted(result, key=lambda tup: tup[0], reverse=True)

    return result_ord

def get_customer_master(spark, closing_day):

    from churn.analysis.triggers.orders.customer_master import get_customer_master
    tr_base = (get_customer_master(spark, closing_day, unlabeled=False)
               .filter(col('segment_nif') != 'Pure_prepaid')
               .withColumn('blindaje', lit('none'))
               .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') >= 0) & (col('tgs_days_until_fecha_fin_dto') <= 60), 'soft').otherwise(col('blindaje')))
               .withColumn('blindaje',  when((col('tgs_days_until_fecha_fin_dto') > 60), 'hard').otherwise(col('blindaje')))
               .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'rgus_list', 'segment_nif', 'label'))

    tr_base = tr_base.where(col("segment_nif").isNotNull())

    tr_base = tr_base.drop_duplicates(["nif_cliente"])
    tr_churn_ref = tr_base.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tr_volume_ref = tr_base.count()

    return tr_base, tr_volume_ref, tr_churn_ref


def get_customer_master_unlabeled(spark, closing_day):

    from churn.analysis.triggers.orders.customer_master import get_customer_master
    tr_base = (get_customer_master(spark, closing_day, unlabeled=True)
               .filter(col('segment_nif') != 'Pure_prepaid')
               .withColumn('blindaje', lit('none'))
               .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') >= 0) & (col('tgs_days_until_fecha_fin_dto') <= 60), 'soft').otherwise(col('blindaje')))
               .withColumn('blindaje',  when((col('tgs_days_until_fecha_fin_dto') > 60), 'hard').otherwise(col('blindaje')))
               .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'rgus_list', 'segment_nif', 'label'))

    tr_base = tr_base.where(col("segment_nif").isNotNull())

    tr_base = tr_base.drop_duplicates(["nif_cliente"])

    tr_volume_ref = tr_base.count()

    return tr_base, tr_volume_ref, -1



def build_exploration_car(spark, closing_day, select_cols, labeled=True):

    from churn.analysis.triggers.ml_triggers.metadata_triggers import get_billing_metadata, get_tickets_metadata, \
        get_reimbursements_metadata, get_ccc_metadata, get_customer_metadata, get_orders_metadata, \
        get_full_billing_metadata

    partition_date = "year={}/month={}/day={}".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    # CCC attributes
    ccc_path = '/data/attributes/vf_es/trigger_analysis/ccc/' + partition_date

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(ccc_path) == False:
        from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_attributes
        get_nif_ccc_attributes(spark, closing_day)

    tr_ccc_all = spark.read.parquet(ccc_path)

    print('[Info build_exploration_car] CCC info loaded')

    ######################################

    bill_cols = [c for c in tr_ccc_all.columns if 'billing' in c.lower()]

    ############ CUSTOMER MASTER ###############
    if labeled:
        tr_base, tr_volume_ref, tr_churn_ref = get_customer_master(spark, closing_day)
    else:
        tr_base, tr_volume_ref, tr_churn_ref = get_customer_master_unlabeled(spark, closing_day)

    from operator import add
    from functools import reduce
    import pyspark.sql.functions as F

    from pyspark.sql.functions import max as _max
    max_rgus = tr_base.select(_max(col('nb_rgus')).alias('max')).collect()

    expr_fbb = reduce(add,
                      [F.when(F.col('rgus_list').getItem(x) == 'fbb', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_mobile = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'mobile', 1).otherwise(0) for x in
                               range(max_rgus[0][0])])
    expr_tv = reduce(add,
                     [F.when(F.col('rgus_list').getItem(x) == 'tv', 1).otherwise(0) for x in range(max_rgus[0][0])])

    tr_base = tr_base.withColumn('nb_rgus_fbb', expr_fbb)
    tr_base = tr_base.withColumn('nb_rgus_tv', expr_tv)
    tr_base = tr_base.withColumn('nb_rgus_mobile', expr_mobile)

    base_ori = tr_base.count()
    from pykhaos.utils.date_functions import move_date_n_days
    closing_day_3m = move_date_n_days(closing_day, n=-90)
    tr_base_3m, tr_volume_ref_3m, tr_churn_ref_3m = get_customer_master_unlabeled(spark, closing_day_3m)

    from pyspark.sql.functions import max as _max
    max_rgus = tr_base_3m.select(_max(col('nb_rgus')).alias('max')).collect()

    expr_fbb = reduce(add,
                      [F.when(F.col('rgus_list').getItem(x) == 'fbb', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_mobile = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'mobile', 1).otherwise(0) for x in
                               range(max_rgus[0][0])])
    expr_tv = reduce(add,
                     [F.when(F.col('rgus_list').getItem(x) == 'tv', 1).otherwise(0) for x in range(max_rgus[0][0])])

    tr_base_3m = tr_base_3m.withColumn('nb_rgus_fbb_3m', expr_fbb)
    tr_base_3m = tr_base_3m.withColumn('nb_rgus_tv_3m', expr_tv)
    tr_base_3m = tr_base_3m.withColumn('nb_rgus_mobile_3m', expr_mobile)

    base_3m = tr_base_3m.count()

    tr_base_ori = tr_base

    # Take only customers who have been in the base for over 3 months
    tr_base = tr_base_3m.select('nif_cliente', 'nb_rgus_fbb_3m', 'nb_rgus_tv_3m', 'nb_rgus_mobile_3m').join(tr_base, [
        'nif_cliente'], 'inner')

    tr_base_ori_filt_rgus = tr_base_ori.join(tr_base_3m.select('nif_cliente', 'nb_rgus_fbb_3m', 'nb_rgus_tv_3m', 'nb_rgus_mobile_3m'), ['nif_cliente'], 'inner').fillna(0)

    base_join = tr_base.count()

    print('[Info build_exploration_car] Number of customers in the base: ') + str(base_ori)
    print('[Info build_exploration_car] Number of customers in the (M-3) base: ') + str(base_3m)
    print('[Info build_exploration_car] Number of stable customers in the base: ') + str(base_join)

    print('[Info build_exploration_car] Number of discarded customers because of early churn: ') + str(
        base_ori - base_join)

    tr_base = tr_base.withColumn('inc_fbb_rgus', col('nb_rgus_fbb') - col('nb_rgus_fbb_3m')).withColumn('inc_tv_rgus',
                                                                                                        col('nb_rgus_tv') - col(
                                                                                                            'nb_rgus_tv_3m')).withColumn(
        'inc_mobile_rgus', col('nb_rgus_mobile') - col('nb_rgus_mobile_3m'))

    tr_base_ori_filt_rgus = tr_base_ori_filt_rgus.withColumn('inc_fbb_rgus', col('nb_rgus_fbb') - col('nb_rgus_fbb_3m')).withColumn('inc_tv_rgus',
                                                                                                        col('nb_rgus_tv') - col(
                                                                                                            'nb_rgus_tv_3m')).withColumn(
        'inc_mobile_rgus', col('nb_rgus_mobile') - col('nb_rgus_mobile_3m'))

    filt_rgus = tr_base_ori_filt_rgus.count()
    base_rgu = tr_base.count()
    tr_base = tr_base.where((col('inc_fbb_rgus') >= 0) & (col('inc_tv_rgus') >= 0) & (col('inc_mobile_rgus') >= 0))
    base_filtered = tr_base.count()
    filtered_rgus = tr_base_ori_filt_rgus.where((col('inc_fbb_rgus') >= 0) & (col('inc_tv_rgus') >= 0) & (col('inc_mobile_rgus') >= 0)).count()


    print('[Info build_exploration_car] Number of customers without any rgu decrements (sequential): ') + str(base_filtered)

    print('[Info build_exploration_car] Number of discarded customers because of rgu decrements (sequential): ') + str(base_rgu - base_filtered)

    print('[Info build_exploration_car] Number of customers without any rgu decrements (full base): ') + str(filtered_rgus)

    print('[Info build_exploration_car] Number of discarded customers because of rgu decrements (full base): ') + str(filt_rgus - filtered_rgus)

    metadata_ccc = get_ccc_metadata(spark)
    ccc_map_tmp = metadata_ccc.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    ccc_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in ccc_map_tmp if x[0] in tr_ccc_all.columns])

    tr_ccc_base = tr_base.join(tr_ccc_all, ['nif_cliente'], 'left').na.fill(ccc_map)

    tr_input_df = tr_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label', 'segment_nif'])
    #tr_input_df = tr_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0) & (col('BILLING_POSTPAID_w4') > 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label'])

    print('[Info build_exploration_car] CUSTOMER MASTER info loaded')

    ################ ORDERS #####################

    # orders_path = '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/' + partition_date
    #
    # if check_hdfs_exists(orders_path) == False:
    #     from churn.datapreparation.general.sla_data_loader import get_orders_module, save_module
    #     df_orders = get_orders_module(spark, closing_day, None)
    #     save_module(df_orders, closing_day, '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/')
    # tr_orders = spark.read.load(orders_path)
    from churn_nrt.src.data.orders_sla import OrdersSLA
    tr_orders = OrdersSLA(spark).get_module(closing_day).select("nif_cliente", 'nb_started_orders_last30',
                                                                'nb_running_last30_gt5', "has_forbidden_orders_last90")


    orders_meta = get_orders_metadata(spark, tr_orders)
    orders_map_tmp = orders_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    orders_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in orders_map_tmp if x[0] in tr_orders.columns])

    print('[Info build_exploration_car] ORDERS info loaded')

    tr_input_df = tr_input_df.join(tr_orders, on=["nif_cliente"], how="left").na.fill(orders_map)

    print("Filtering clients with forbidden orders last 90 days")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~(col("has_forbidden_orders_last90")>0))
    after_cust = tr_input_df.count()
    print("Filtered NIFs with forbidden orders before={} after={} eliminados={}".format(before_cust, after_cust, before_cust-after_cust))

    print'Number of NIFs discarded because of forbidden orders (full base): ' + str(tr_orders.where(col('has_forbidden_orders_last90')>0).count())

    tr_segment_churn_ref = tr_input_df.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tr_segment_volume = tr_input_df.count()

    print('[closing_day={}] Volume={}  ChurnRate={} | Segment: Volume={} ChurnRate={}'.format(closing_day,
                                                                                              tr_volume_ref,
                                                                                              tr_churn_ref,
                                                                                              tr_segment_volume,
                                                                                              tr_segment_churn_ref))

    ############ REIMBURSEMENTS ##############
    # Adding reimbursement features
    # If the module does not exist, it must be created
    reimb_path = '/data/attributes/vf_es/trigger_analysis/reimbursements/' + partition_date
    if check_hdfs_exists(reimb_path) == False:
        from churn.analysis.triggers.billing.reimbursements_funct import get_reimbursements_car
        get_reimbursements_car(spark, closing_day)
        ###########################################################
        # Calling to the function that creates the module
        ###########################################################

    tr_reimb_df = spark.read.parquet('/data/attributes/vf_es/trigger_analysis/reimbursements/' + partition_date).withColumnRenamed("NIF_CLIENTE", "nif_cliente")
    tr_reimb_df = tr_reimb_df.drop_duplicates(["nif_cliente"])

    print('[Info build_exploration_car] REIMBURSEMENTS info loaded')

    tr_input_df = tr_input_df.join(tr_reimb_df, ['nif_cliente'], 'left').na.fill(reimb_map)


    ############ BILLING ####################
    # Adding billing attributes
    #tr_billing_df = spark.read.parquet('/user/csanc109/projects/triggers/trigger_billing_car/' + partition_date)
    from churn.analysis.triggers.billing.run_segment_billing import get_billing_module
    tr_billing_df, _ = get_billing_module(spark, closing_day, labeled_mini_ids=labeled)
    # This function generate the module if it does not exist or return the stored one from path (/user/csanc109/projects/triggers/trigger_billing_car/)
    tr_billing_df = tr_billing_df.drop("label").drop(*['DEVICE_DELIVERY_REPAIR_w8',
                     'INTERNET_EN_EL_MOVIL_w8',
                     'QUICK_CLOSING_w8',
                     'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w8',
                     'TARIFF_MANAGEMENT_w8',
                     'PRODUCT_AND_SERVICE_MANAGEMENT_w8',
                     'MI_VODAFONE_w8',
                     'CHURN_CANCELLATIONS_w8',
                     'DSL_FIBER_INCIDENCES_AND_SUPPORT_w8',
                     'PREPAID_BALANCE_w8',
                     'COLLECTIONS_w8',
                     'NEW_ADDS_PROCESS_w8',
                     'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w8',
                     'DEVICE_UPGRADE_w8',
                     'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w8',
                     'BILLING_POSTPAID_w8',
                     'BUCKET_w8',
                     'Reimbursement_days_since',
                     'segment_nif'
                     ])
    tr_billing_df = tr_billing_df.drop_duplicates(["nif_cliente"])

    print('[Info build_exploration_car] BILLING info loaded')

    metadata_billing = get_full_billing_metadata(spark, tr_billing_df)

    billing_map_tmp = metadata_billing.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    billing_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in billing_map_tmp if x[0] in tr_billing_df.columns])



    tr_input_df = tr_input_df.join(tr_billing_df.drop("num_cliente"), ['nif_cliente'], 'left').na.fill(billing_map)


    ################# TICKETS ###################
    # Add tickets
    #FIXME from churn.analysis.triggers.base_utils.utils_trigger import get_tickets_car
    # Move function locally. I received following error when a run the code:
    # File "/var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn/analysis/triggers/base_utils/utils_trigger.py", line 47
    # SyntaxError: Non - ASCII character '\xc3' in file / var / SP / data / bdpmdses / deliveries_churn / latest / use - cases / churn / analysis / triggers / base_utils / utils_trigger.py
    # on line 47, but no encoding declared; see http: // python.org / dev / peps / pep - 0263 / for details
    tickets = get_tickets_car(spark, closing_day)
    window = Window.partitionBy("NIF_CLIENTE")

    #df_fact = tickets.where((col('X_TIPO_OPERACION') == 'Tramitacion'))
    #df_fact = (df_fact.withColumn('tmp_fact', when((col('TIPO_TICKET').isNull()), 1.0).otherwise(0.0))\
    #                  .withColumn('NUM_TICKETS_TIPO_FACT', sql_sum('tmp_fact').over(window)))
    #df_tickets = df_fact.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT').drop_duplicates(subset= ['NIF_CLIENTE']).withColumnRenamed("NIF_CLIENTE", "nif_cliente")

    df_fact = (tickets
         .withColumn('tmp_fact', when((col('X_TIPO_OPERACION') == 'Tramitacion'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_FACT', sql_sum('tmp_fact').over(window))
         .withColumn('tmp_rec', when((col('X_TIPO_OPERACION') == 'Reclamacion'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_REC', sql_sum('tmp_rec').over(window))
         .withColumn('tmp_pet', when((col('X_TIPO_OPERACION') == 'Peticion'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_PET', sql_sum('tmp_pet').over(window))
         .withColumn('tmp_av', when((col('X_TIPO_OPERACION') == 'Averia'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_V', sql_sum('tmp_av').over(window))
         .withColumn('tmp_inc', when((col('X_TIPO_OPERACION') == 'Incidencia'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_INC', sql_sum('tmp_inc').over(window))
         .withColumn('tmp_inf', when((col('X_TIPO_OPERACION') == 'Informacion'), 1.0).otherwise(0.0))
         .withColumn('NUM_TICKETS_TIPO_INF', sql_sum('tmp_inf').over(window))
         .withColumn('NUM_TICKETS', sql_count("*").over(window)))
    df_tickets = (df_fact.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT', 'NUM_TICKETS')\
                   .drop_duplicates(subset= ['NIF_CLIENTE'])
                   .withColumnRenamed("NIF_CLIENTE", "nif_cliente")) # necessary?

    metadata_tickets = get_tickets_metadata(spark)
    tickets_map_tmp = metadata_tickets.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    tickets_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in tickets_map_tmp if x[0] in df_tickets.columns])

    tr_input_df = tr_input_df.join(df_tickets.drop("num_cliente"), ['nif_cliente'], 'left').na.fill(tickets_map)

    if select_cols:
        tr_input_df = tr_input_df.select(*(select_cols+["label"]))

    save_car(tr_input_df, closing_day, labeled)

    return tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref


def save_car(df, closing_day, labeled=True):

    path_to_save = CAR_PATH if labeled else CAR_PATH_UNLABELED

    df = df.withColumn("day", lit(int(closing_day[6:])))
    df = df.withColumn("month", lit(int(closing_day[4:6])))
    df = df.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {}".format(path_to_save))
    (df.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved {} for closing_day {}".format(path_to_save, closing_day))

def get_exploration_car(spark, closing_day, exploration_cols):
    car_path  = CAR_PATH+PARTITION_DATE.format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(car_path):
        print("Found already a car saved in path - '{}'".format(car_path))
        tr_input_df = spark.read.parquet(car_path)
        tr_input_df = tr_input_df.where(col("segment_nif").isNotNull())

        _, tr_volume_ref, tr_churn_ref = get_customer_master(spark, closing_day)
        tr_segment_churn_ref = tr_input_df.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']
        #tr_segment_volume = tr_input_df.count()
    else:
        start_time = time.time()
        tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref = build_exploration_car(spark, closing_day, exploration_cols)
        print("Build_exploration_car {} minutes".format((time.time() - start_time)/60.0))

    print("Filtering clients with standalone_fbb+diff_rgus_d_d98<0")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~((col('diff_rgus_d_d98')<0) & (col("segment_nif")=="Standalone_FBB")))
    after_cust = tr_input_df.count()
    print("Filtered before={} after={}".format(before_cust, after_cust))
    tr_segment_volume = tr_input_df.count()

    return tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref


def get_exploration_car_unlabeled(spark, closing_day, exploration_cols):

    car_path  = CAR_PATH_UNLABELED+PARTITION_DATE.format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(car_path):
        print("Found already a car saved in path - '{}'".format(car_path))
        tr_input_df = spark.read.parquet(car_path)
        tr_input_df = tr_input_df.where(col("segment_nif").isNotNull())
    else:
        start_time = time.time()
        tr_input_df, _, _ ,_ = build_exploration_car(spark, closing_day, exploration_cols, False)
        print("Build_exploration_car {} minutes".format((time.time() - start_time)/60.0))

    print("Filtering clients with standalone_fbb+diff_rgus_d_d98<0")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~((col('diff_rgus_d_d98')<0) & (col("segment_nif")=="Standalone_FBB")))
    after_cust = tr_input_df.count()
    print("Filtered before={} after={}".format(before_cust, after_cust))

    return tr_input_df





def get_tickets_car(spark, closing_day):
    ga_tickets = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    ga_franquicia = spark.read.table('raw_es.callcentrecalls_ticketfranchiseow')
    ga_close_case = spark.read.table('raw_es.callcentrecalls_ticketclosecaseow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(
        col('NIF_CLIENTE') != '7')
    ga_tipo_tickets = spark.read.parquet('/data/raw/vf_es/cvm/GATYPETICKETS/1.0/parquet')
    print("############ Loaded ticket sources ############")

    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles
    starting_day = move_date_n_cycles(closing_day, n=-4)

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad
    tickets = (ga_tickets.where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day) & (
                concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) >= starting_day)).withColumn('CREATION_TIME',
                                                                                                                       to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')).withColumn(
        'DURACION', regexp_replace('DURACION', ',', '.')).join(ga_tickets_detalle, ga_tickets.OBJID == ga_tickets_detalle.OBJID, 'left_outer').join(ga_franquicia,
                                                                                                                                                    ga_tickets_detalle.OBJID == ga_franquicia.ID_FRANQUICIA,
                                                                                                                                                    'left_outer').join(ga_close_case,
                                                                                                                                                                       ga_tickets.OBJID == ga_close_case.OBJID,
                                                                                                                                                                       'left_outer').join(clientes,
                                                                                                                                                                                          ga_tickets.CASE_REPORTER2YESTE == clientes.OBJID,
                                                                                                                                                                                          'left_outer').join(
        ga_tipo_tickets, ga_tickets.ID_TIPO_TICKET == ga_tipo_tickets.ID_TIPO_TICKET, 'left_outer').filter(col('NIF_CLIENTE').isNotNull()))

    return tickets

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


def get_feat_auc(spark, df, feats, label):
    for name_ in df.select(feats).columns:
        df = df.withColumn(name_, col(name_).cast('double'))
        evaluator = BinaryClassificationEvaluator(rawPredictionCol=name_, labelCol=label, metricName='areaUnderROC')
        auc = evaluator.evaluate(df)
        auc_f = (1 - auc) if (auc < 0.5) else auc
        print("Feature {} AUC: {}".format(name_, auc_f))

if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn.utils.general_functions import init_spark
    from churn.models.fbb_churn_amdocs.utils_general import *
    from churn.models.fbb_churn_amdocs.utils_model import *

    spark = init_spark("trigger_segm")
    sc = spark.sparkContext

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    tr_date_ = sys.argv[1]

    tt_date_ = sys.argv[2]

    model_ = sys.argv[3]



    ##########################################################################################
    # The model is built on the segment of customers with calls related to billing to the call centre during the last month.
    # Thus, additional feats are added to this segment by using left_outer + na.fill
    ##########################################################################################

    reimb_map = {'Reimbursement_adjustment_net': 0.0,\
                 'Reimbursement_adjustment_debt': 0.0,\
                 'Reimbursement_num': 0.0,\
                 'Reimbursement_num_n8': 0.0,\
                 'Reimbursement_num_n6': 0.0,\
                 'Reimbursement_days_since': 1000.0,\
                 'Reimbursement_num_n4': 0.0,\
                 'Reimbursement_num_n5': 0.0,\
                 'Reimbursement_num_n2': 0.0,\
                 'Reimbursement_num_n3': 0.0,\
                 'Reimbursement_days_2_solve': -1.0,\
                 'Reimbursement_num_month_2': 0.0,\
                 'Reimbursement_num_n7': 0.0,\
                 'Reimbursement_num_n1': 0.0,\
                 'Reimbursement_num_month_1': 0.0}

    # List of columns to be used in the model
    EXPLORATION_COLS = list({'BILLING_POSTPAID_w2',
                        'BILLING_POSTPAID_w4',
                        'BILLING_POSTPAID_w8',
                        'nif_cliente',
                        'num_calls_w8',
                        'billing_std',
                        'billing_mean',
                        'nb_rgus',
                        'nb_started_orders_last30',
                        'nb_running_last30_gt5',
                        'blindaje',
                        'nb_superofertas',
                         'segment_nif',
                         'nb_rgus_d98',
                         'diff_rgus_d_d98',
                         'flag_decremento_rgus',
                         'flag_incremento_rgus',
                         'flag_no_inc_rgus',
                         'flag_keep_rgus',
                         'billing_nb_last_bills',
                         'billing_avg_days_bw_bills',
                         'billing_max_days_bw_bills',
                         'billing_min_days_bw_bills',
                         'nb_invoices',
                         'greatest_diff_bw_bills',
                         'least_diff_bw_bills',
                         'billing_current_debt',
                         'billing_current_vf_debt',
                         'billing_current_client_debt',
                         'Reimbursement_days_2_solve',
                         'Reimbursement_adjustment_net',
                         'NUM_TICKETS_TIPO_FACT',
                         'NUM_TICKETS'
                             })




    ##########################################################################################
    # 2. Loading tr data
    ##########################################################################################
    df_tr_all, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref = get_exploration_car(spark, tr_date_, EXPLORATION_COLS)

    print("COUNT NANS")
    from pykhaos.utils.pyspark_utils import count_nans
    A = count_nans(df_tr_all)
    print(A)

    [tr_input_df, df_val] = df_tr_all.randomSplit([0.8, 0.2], 1234)

    tr_input_df.groupBy('label').agg(count('*').alias('nb')).show()

    feats_num = list(set(EXPLORATION_COLS) - {"blindaje", "nif_cliente", "segment_nif"})
    get_feat_auc(spark, tr_input_df, feats_num,'label')

    tr_input_df = balance_df2(tr_input_df, 'label')

    tr_input_df.groupBy('label').agg(count('*').alias('nb')).show()


    ##########################################################################################
    # 3. Loading tt data
    ##########################################################################################

    # tt_input_df, tt_segment_churn_ref, tt_segment_volume, tt_churn_ref = get_exploration_car(spark, tt_date_, EXPLORATION_COLS)
    df_tt_all, tt_segment_churn_ref, tt_segment_volume, tt_churn_ref = get_exploration_car(spark, tt_date_, EXPLORATION_COLS)

    #unlab_tt_input_df = get_exploration_car_unlabeled(spark, tt_date_, EXPLORATION_COLS)

    ##########################################################################################
    # 4. Modeling
    ##########################################################################################

    categorical_columns = ['blindaje', 'segment_nif']

    stages = []

    start_time_modeling = time.time()
    for categorical_col in categorical_columns:
        if not categorical_col in EXPLORATION_COLS:
            continue
        print("StringIndexer+OneHotEncoderEstimator '{}'".format(categorical_col))
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    numeric_columns = list(set(EXPLORATION_COLS) - {"blindaje", "nif_cliente", "segment_nif"})
    assembler_inputs = [c + "_class_vec" for c in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    print("assembler_inputs [{}] -> {}".format(len(assembler_inputs), ",".join(assembler_inputs)))


    print("Starting fit....")
    from churn.analysis.poc_segments.poc_modeler import get_feats_imp, get_model

    mymodel = get_model(model_)
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_input_df)

    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    print("Elapsed modeling time {} minutes".format((time.time() - start_time_modeling)/60.0))

    ##########################################################################################
    # 5. Evaluation
    ##########################################################################################

    start_time_evaluation = time.time()
    from churn.analysis.poc_segments.poc_modeler import get_score, get_metrics
    df_tr_preds = get_score(pipeline_model, tr_input_df)

    get_metrics(df_tr_preds, title="TRAIN", nb_deciles=None, score_col="model_score", label_col="label", refprevalence=tr_churn_ref)

    print("Elapsed time evaluating train {} minutes".format((time.time() - start_time_evaluation)/60.0))

    start_time_evaluation = time.time()

    df_tt_preds = get_score(pipeline_model, df_tt_all)
    df_tt_preds = df_tt_preds.sort(desc("model_score"))

    # get_metrics(df_tt_preds, title="TEST", nb_deciles=None, score_col="model_score", label_col="label", refprevalence=tt_churn_ref)

    print("Elapsed time evaluating test {} minutes".format((time.time() - start_time_evaluation)/60.0))

    from churn.models.fbb_churn_amdocs.utils_model import get_calibration_function2

    # Split train en train y valdf
    #calibmodel = get_calibration_function2(spark, pipeline_model, df_val, 'label', 10)
    predicted_tt_calib = df_tt_preds #calibmodel[0].transform(df_tt_preds)

    print("num_columns df original={} df calibrado={}".format(len(df_tt_preds.columns), len(predicted_tt_calib.columns)))
    print("Columnas: {}".format(",".join(predicted_tt_calib.columns)))

    # since i am testing, I add a time partition to store different executions
    #import datetime as dt
    #partition_time = int(dt.datetime.now().strftime("%H%m%S"))
    #predicted_tt_calib = predicted_tt_calib.withColumn("time", lit(partition_time))
    predicted_tt_calib = predicted_tt_calib.withColumn("day", lit(int(tt_date_[6:])))
    predicted_tt_calib = predicted_tt_calib.withColumn("month", lit(int(tt_date_[4:6])))
    predicted_tt_calib = predicted_tt_calib.withColumn("year", lit(int(tt_date_[:4])))
    path_to_save = "/data/udf/vf_es/churn/triggers/model1_balanced_scores/"
    (predicted_tt_calib.coalesce(1).write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))


