# !/usr/bin/env python
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

CAR_PATH = "/data/udf/vf_es/churn/triggers/car_exploration_model1_segment_tickets_evol/"
CAR_PATH_UNLABELED = "/data/udf/vf_es/churn/triggers/car_exploration_unlabeled_model1_segment_tickets_evol/"

PARTITION_DATE = "year={}/month={}/day={}"

def get_tickets_car(spark, closing_day):
    from pykhaos.utils.date_functions import move_date_n_days
    closing_day_w = move_date_n_days(closing_day, n=-7)
    closing_day_2w = move_date_n_days(closing_day, n=-15)
    closing_day_4w = move_date_n_days(closing_day, n=-30)
    closing_day_8w = move_date_n_days(closing_day, n=-60)

    tickets_w = get_module_tickets(spark, closing_day_w, closing_day)
    tickets_w2 = get_module_tickets(spark, closing_day_2w, closing_day)
    tickets_w4 = get_module_tickets(spark, closing_day_4w, closing_day)
    tickets_w8 = get_module_tickets(spark, closing_day_8w, closing_day)
    tickets_w4w2 = get_module_tickets(spark, closing_day_4w, closing_day_2w)
    tickets_w8w4 = get_module_tickets(spark, closing_day_8w, closing_day_4w)

    base_cols = [col_ for col_ in tickets_w8.columns if col_.startswith('num_tickets_tipo_')]

    for col_ in base_cols:
        tickets_w = tickets_w.withColumnRenamed(col_, col_ + '_w')
        tickets_w8 = tickets_w8.withColumnRenamed(col_, col_ + '_w8')
        tickets_w4 = tickets_w4.withColumnRenamed(col_, col_ + '_w4')
        tickets_w2 = tickets_w2.withColumnRenamed(col_, col_ + '_w2')
        tickets_w4w2 = tickets_w4w2.withColumnRenamed(col_, col_ + '_w4w2')
        tickets_w8w4 = tickets_w8w4.withColumnRenamed(col_, col_ + '_w8w4')

    df_tickets = tickets_w8.join(tickets_w4, ['NIF_CLIENTE'], 'left').join(tickets_w2, ['NIF_CLIENTE'], 'left') \
        .join(tickets_w4w2, ['NIF_CLIENTE'], 'left').join(tickets_w8w4, ['NIF_CLIENTE'], 'left').join(tickets_w, ['NIF_CLIENTE'], 'left').fillna(0)

    for col_ in base_cols:
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w4w4', col(col_ + '_w4') - col(col_ + '_w8w4'))

    return df_tickets.fillna(0)



def get_customer_master(spark, closing_day):
    from churn.analysis.triggers.orders.customer_master import get_customer_master
    tr_base = (get_customer_master(spark, closing_day, unlabeled=False)
               .filter(col('segment_nif') != 'Pure_prepaid')
               .withColumn('blindaje', lit('none'))
               .withColumn('blindaje', when(
        (col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60), 'soft').otherwise(
        col('blindaje')))
               .withColumn('blindaje',
                           when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje')))
               .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'rgus_list', 'segment_nif','tgs_days_until_f_fin_bi', 'tgs_has_discount', 'label'))

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
               .withColumn('blindaje', when(
        (col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60), 'soft').otherwise(
        col('blindaje')))
               .withColumn('blindaje',
                           when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje')))
               .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'rgus_list', 'segment_nif','tgs_days_until_f_fin_bi', 'tgs_has_discount', 'label'))

    tr_base = tr_base.where(col("segment_nif").isNotNull())

    tr_base = tr_base.drop_duplicates(["nif_cliente"])

    tr_volume_ref = tr_base.count()

    return tr_base, tr_volume_ref, -1


def build_exploration_car(spark, closing_day, select_cols, labeled=True):
    from churn.analysis.triggers.ml_triggers.metadata_triggers import get_billing_metadata, get_tickets_metadata, \
        get_reimbursements_metadata, get_ccc_metadata, get_customer_metadata, get_orders_metadata, \
        get_full_billing_metadata, get_tickets_additional_metadata_dict

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

    expr_fbb = reduce(add,[F.when(F.col('rgus_list').getItem(x) == 'fbb', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_mobile = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'mobile', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_tv = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'tv', 1).otherwise(0) for x in range(max_rgus[0][0])])

    tr_base = tr_base.withColumn('nb_rgus_fbb', expr_fbb)
    tr_base = tr_base.withColumn('nb_rgus_tv', expr_tv)
    tr_base = tr_base.withColumn('nb_rgus_mobile', expr_mobile)

    tr_base = tr_base.cache()
    base_ori = tr_base.count()

    from pykhaos.utils.date_functions import move_date_n_days
    closing_day_3m = move_date_n_days(closing_day, n=-90)
    tr_base_3m, tr_volume_ref_3m, tr_churn_ref_3m = get_customer_master_unlabeled(spark, closing_day_3m)

    from pyspark.sql.functions import max as _max
    max_rgus = tr_base_3m.select(_max(col('nb_rgus')).alias('max')).collect()

    expr_fbb = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'fbb', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_mobile = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'mobile', 1).otherwise(0) for x in range(max_rgus[0][0])])
    expr_tv = reduce(add, [F.when(F.col('rgus_list').getItem(x) == 'tv', 1).otherwise(0) for x in range(max_rgus[0][0])])

    tr_base_3m = tr_base_3m.withColumn('nb_rgus_fbb_3m', expr_fbb)
    tr_base_3m = tr_base_3m.withColumn('nb_rgus_tv_3m', expr_tv)
    tr_base_3m = tr_base_3m.withColumn('nb_rgus_mobile_3m', expr_mobile)

    base_3m = tr_base_3m.count()

    # Take only customers who have been in the base for over 3 months
    tr_base = tr_base_3m.select('nif_cliente', 'nb_rgus_fbb_3m', 'nb_rgus_tv_3m', 'nb_rgus_mobile_3m').join(tr_base, ['nif_cliente'], 'inner')

    base_join = tr_base.count()
    tr_base = tr_base.cache()

    print('[Info build_exploration_car] Number of customers in the base: ') + str(base_ori)
    print('[Info build_exploration_car] Number of customers in the (M-3) base: ') + str(base_3m)
    print('[Info build_exploration_car] Number of stable customers in the base: ') + str(base_join)

    print('[Info build_exploration_car] Number of discarded customers because of early churn: ') + str(base_ori - base_join)

    tr_base = tr_base.withColumn('inc_fbb_rgus', col('nb_rgus_fbb') - col('nb_rgus_fbb_3m')).withColumn('inc_tv_rgus', col('nb_rgus_tv') - col('nb_rgus_tv_3m')).withColumn('inc_mobile_rgus', col('nb_rgus_mobile') - col('nb_rgus_mobile_3m'))

    tr_base = tr_base.where((col('inc_fbb_rgus') >= 0) & (col('inc_tv_rgus') >= 0) & (col('inc_mobile_rgus') >= 0))

    base_rgu = tr_base.count()
    print('[Info build_exploration_car] Number of customers without any rgu decrements: ') + str(base_rgu)

    print('[Info build_exploration_car] Number of discarded customers because of rgu decrements: ') + str(base_join - base_rgu)

    metadata_ccc = get_ccc_metadata(spark)
    ccc_map_tmp = metadata_ccc.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    ccc_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in ccc_map_tmp if x[0] in tr_ccc_all.columns])

    tr_ccc_base = tr_base.join(tr_ccc_all, ['nif_cliente'], 'left').na.fill(ccc_map)

    tr_input_df = tr_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label', 'segment_nif', 'tgs_days_until_f_fin_bi', 'tgs_has_discount'])
    # tr_input_df = tr_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0) & (col('BILLING_POSTPAID_w4') > 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label'])

    print('[Info build_exploration_car] CUSTOMER MASTER info loaded')

    ################ ORDERS #####################

    from churn_nrt.src.data.orders_sla import OrdersSLA
    tr_orders = OrdersSLA(spark).get_module(closing_day).select("nif_cliente", 'nb_started_orders_last30',  'nb_running_last30_gt5', "has_forbidden_orders_last90", 'nb_started_orders_last14', 'nb_started_orders_last7')
    #
    orders_meta = get_orders_metadata(spark, tr_orders)
    orders_map_tmp = orders_meta.select("feature", "imp_value", "type").rdd.map( lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    orders_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in orders_map_tmp if x[0] in tr_orders.columns])

    print('[Info build_exploration_car] ORDERS info loaded')

    tr_input_df = tr_input_df.join(tr_orders, on=["nif_cliente"], how="left").na.fill(orders_map)
    tr_input_df = tr_input_df.cache()
    print("Filtering clients with forbidden orders last 90 days")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~(col("has_forbidden_orders_last90") > 0))
    after_cust = tr_input_df.count()
    print("Filtered NIFs with forbidden orders before={} after={} eliminados={}".format(before_cust, after_cust, before_cust - after_cust))

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

    tr_reimb_df = spark.read.parquet('/data/attributes/vf_es/trigger_analysis/reimbursements/' + partition_date).withColumnRenamed("NIF_CLIENTE",
                                                                                                      "nif_cliente")
    tr_reimb_df = tr_reimb_df.drop_duplicates(["nif_cliente"])

    print('[Info build_exploration_car] REIMBURSEMENTS info loaded')

    reimb_df = get_reimbursements_metadata(spark)

    reimb_map_tmp = reimb_df.select("feature", "imp_value", "type").rdd.map(
        lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    reimb_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in reimb_map_tmp if x[0] in tr_reimb_df.columns])

    tr_input_df = tr_input_df.join(tr_reimb_df, ['nif_cliente'], 'left').na.fill(reimb_map)

    ############ BILLING ####################
    # Adding billing attributes
    # tr_billing_df = spark.read.parquet('/user/csanc109/projects/triggers/trigger_billing_car/' + partition_date)
    from churn.analysis.triggers.billing.run_segment_billing import get_billing_module
    tr_billing_df = get_billing_module(spark, closing_day)
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
                                                       'segment_nif'])

    tr_billing_df = tr_billing_df.drop_duplicates(["nif_cliente"])
    tr_billing_df = tr_billing_df.cache()

    print('[Info build_exploration_car] BILLING info loaded')

    metadata_billing = get_full_billing_metadata(spark, tr_billing_df)

    billing_map_tmp = metadata_billing.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    billing_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in billing_map_tmp if x[0] in tr_billing_df.columns])

    tr_input_df = tr_input_df.join(tr_billing_df.drop("num_cliente").drop("tgs_has_discount"), ['nif_cliente'], 'left').na.fill(billing_map)

    #from churn.analysis.triggers.ml_triggers.utils_trigger import get_tickets_car

    df_tickets = get_tickets_car(spark, closing_day)

    metadata_tickets = get_tickets_metadata(spark)
    tickets_map_tmp = metadata_tickets.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    tickets_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in tickets_map_tmp if x[0] in df_tickets.columns])


    path_timing = '/data/udf/vf_es/churn/triggers/tickets_attributes/full_df/' + partition_date
    if check_hdfs_exists(path_timing) == False:
        print'Path ' + path_timing + ' does not exist'
        print'Creating additional tickets feats...'
        from churn.analysis.triggers.ml_triggers.utils_trigger import save_tickets_timing_car
        from pykhaos.utils.date_functions import move_date_n_days
        starting_day = move_date_n_days(closing_day, n=-60)
        save_tickets_timing_car(spark, starting_day, closing_day)
    df_tickets_additional = spark.read.load(path_timing)

    tr_input_df = tr_input_df.join(df_tickets.drop("num_cliente"), ['nif_cliente'], 'left').na.fill(tickets_map)
    tickets_map_add = get_tickets_additional_metadata_dict(spark)
    tr_input_df = tr_input_df.join(df_tickets_additional, ['nif_cliente'], 'left').na.fill(tickets_map_add)

    if select_cols:

        feats_tickets = metadata_tickets.select("feature").rdd.map(lambda x: str(x["feature"])).collect()
        feats_add = tickets_map_add.keys()

        select_cols = select_cols + feats_add + feats_tickets #[name_ for name_ in df_tickets.columns if 'num_tickets' in name_] +  df_tickets_additional.columns[1:]
        if labeled:
            tr_input_df = tr_input_df.select(*(select_cols + ["label"]))
        else:
            tr_input_df = tr_input_df.select(*(select_cols))

    tr_input_df = tr_input_df.cache()
    save_car(tr_input_df, closing_day, labeled)

    return tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref

def get_module_tickets(spark, starting_day, closing_day):
    ga_tickets_ = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(
        col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')

    print'Starting day: ' + starting_day
    print'Closing day: ' + closing_day
    print("############ Loading ticket sources ############")

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    ga_tickets_ = ga_tickets_.withColumn('CREATION_TIME',
                                         to_timestamp(ga_tickets_.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
    close = str(closing_day[0:4]) + '-' + str(closing_day[4:6]) + '-' + str(closing_day[6:8]) + ' 00:00:00'
    start = str(starting_day[0:4]) + '-' + str(starting_day[4:6]) + '-' + str(starting_day[6:8]) + ' 00:00:00'
    dates = (start, close)
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
    ga_tickets = ga_tickets_.where((ga_tickets_.CREATION_TIME < date_to) & (ga_tickets_.CREATION_TIME > date_from))
    ga_tickets_detalle = ga_tickets_detalle.drop_duplicates(subset=['OBJID', 'FECHA_1ER_CIERRE'])
    tickets_ = ga_tickets.withColumn('ticket_date',
                                     concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')) \
        .withColumn('DURACION', regexp_replace('DURACION', ',', '.')) \
        .join(ga_tickets_detalle, ['OBJID'], 'left_outer')
    clientes = clientes.withColumnRenamed('OBJID', 'CLIENTES_ID')

    tickets = tickets_.join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.CLIENTES_ID, 'inner') \
        .filter(col('NIF_CLIENTE').isNotNull())

    print'Number of tickets during period: ' + str(tickets.count())
    df = tickets

    codigos = ['Tramitacion', 'Reclamacion', 'Averia', 'Incidencia']

    from pyspark.sql.functions import countDistinct
    df_tickets = df.groupby('nif_cliente').agg(
        *[countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias('num_tickets_tipo_' + TIPO.lower())
          for TIPO in codigos])

    base_cols = [col_ for col_ in df_tickets.columns if col_.startswith('num_tickets_tipo_')]
    return df_tickets.select(['NIF_CLIENTE'] + base_cols).drop_duplicates(["nif_cliente"])


def save_car(df, closing_day, labeled=True):
    path_to_save = CAR_PATH if labeled else CAR_PATH_UNLABELED

    df = df.withColumn("day", lit(int(closing_day[6:])))
    df = df.withColumn("month", lit(int(closing_day[4:6])))
    df = df.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {}".format(path_to_save))
    (df.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved {} for closing_day {}".format(path_to_save, closing_day))


def get_exploration_car(spark, closing_day, exploration_cols):
    car_path = CAR_PATH + PARTITION_DATE.format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(car_path):
        print("Found already a car saved in path - '{}'".format(car_path))
        tr_input_df = spark.read.parquet(car_path)
        tr_input_df = tr_input_df.where(col("segment_nif").isNotNull())

        _, tr_volume_ref, tr_churn_ref = get_customer_master(spark, closing_day)
        tr_segment_churn_ref = tr_input_df.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']
        # tr_segment_volume = tr_input_df.count()
    else:
        start_time = time.time()
        tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref = build_exploration_car(spark, closing_day,
                                                                                                   exploration_cols)
        print("Build_exploration_car {} minutes".format((time.time() - start_time) / 60.0))

    print("Filtering clients with standalone_fbb+diff_rgus_d_d98<0")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~((col('diff_rgus_d_d98') < 0) & (col("segment_nif") == "Standalone_FBB")))
    after_cust = tr_input_df.count()
    print("Filtered before={} after={}".format(before_cust, after_cust))
    tr_segment_volume = tr_input_df.count()

    return tr_input_df, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref


def get_exploration_car_unlabeled(spark, closing_day, exploration_cols):
    car_path = CAR_PATH_UNLABELED + PARTITION_DATE.format(int(closing_day[:4]), int(closing_day[4:6]),
                                                          int(closing_day[6:]))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(car_path):
        print("Found already a car saved in path - '{}'".format(car_path))
        tr_input_df = spark.read.parquet(car_path)
        tr_input_df = tr_input_df.where(col("segment_nif").isNotNull())
    else:
        start_time = time.time()
        tr_input_df, _, _, _ = build_exploration_car(spark, closing_day, exploration_cols, False)
        print("Build_exploration_car {} minutes".format((time.time() - start_time) / 60.0))

    print("Filtering clients with standalone_fbb+diff_rgus_d_d98<0")
    before_cust = tr_input_df.count()
    tr_input_df = tr_input_df.where(~((col('diff_rgus_d_d98') < 0) & (col("segment_nif") == "Standalone_FBB")))
    after_cust = tr_input_df.count()
    print("Filtered before={} after={}".format(before_cust, after_cust))

    return tr_input_df


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


if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn.utils.general_functions import init_spark

    spark = init_spark("trigger_segm")
    sc = spark.sparkContext

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    tr_date_ = sys.argv[1]

    labeled = int(sys.argv[2])

    ##########################################################################################
    # The model is built on the segment of customers with calls related to billing to the call centre during the last month.
    # Thus, additional feats are added to this segment by using left_outer + na.fill
    ################################################################################x##########

    EXPLORATION_COLS = list({'BILLING_POSTPAID_w2',
                             'BILLING_POSTPAID_w4',
                             'BILLING_POSTPAID_w8',
                             'nif_cliente',
                             'tgs_days_until_f_fin_bi',
                             'num_calls_w8',
                             'billing_std',
                             'billing_mean',
                             'nb_rgus',
                             'nb_started_orders_last30','nb_started_orders_last7',
                             'nb_running_last30_gt5',
                             'blindaje',
                             'nb_superofertas',
                             'segment_nif',
                             'nb_rgus_d98',
                             'diff_rgus_d_d98',
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
                             'Reimbursement_days_2_solve','flag_incremento_rgus', 'tgs_has_discount',
                             'Reimbursement_adjustment_net','Reimbursement_num_n1','Reimbursement_num_n2','nb_started_orders_last14'
                             })
    #,

    if labeled:
        df_tr_all, tr_segment_churn_ref, tr_segment_volume, tr_churn_ref = get_exploration_car(spark, tr_date_, EXPLORATION_COLS)

        df = df_tr_all.withColumn('NUM_TICKETS', col('num_tickets_tipo_incidencia_w8') + col('num_tickets_tipo_reclamacion_w8') + col('num_tickets_tipo_averia_w8'))
        df = df.withColumn('flag_reimb', when(col('Reimbursement_adjustment_net') != 0, 1).otherwise(0))
        df = df.withColumn('flag_billshock', when(col('greatest_diff_bw_bills') > 35, 1).otherwise(0))
        df = df.withColumn('flag_fact', when(col('num_tickets_tipo_tramitacion_w8') > 0, 1).otherwise(0))
        df = df.withColumn('flag_tickets', when(col('NUM_TICKETS') > 0, 1).otherwise(0))
        df = df.withColumn('flag_orders', when(col('nb_running_last30_gt5') > 0, 1).otherwise(0))

        print'Size of the tr car: ' + str(df_tr_all.count())
        df_tr_all = df.where((col('flag_reimb') > 0) | (col('flag_billshock') > 0) | (col('flag_fact') > 0) | (col('flag_tickets') > 0) | (col('flag_orders') > 0))
        df_tr_all = df_tr_all.cache()

        print'Size of the tr car (only with problems): ' + str(df_tr_all.count())
        print'Churn rate for customers with reimbursements: ' + str(
            df.where(col('flag_reimb') > 0).select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate'])
        print'Churn rate for customers with billshock: ' + str(
            df.where(col('flag_billshock') > 0).select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate'])
        print'Churn rate for customers with orders: ' + str(
            df.where(col('flag_orders') > 0).select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate'])
        print'Churn rate for customers with tickets: ' + str(
            df.where(col('flag_tickets') > 0).select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate'])
        print'Churn rate for customers with wrong fact: ' + str(
            df.where(col('flag_fact') > 0).select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate'])
        print'############### Finished process ###############'
    else:
        get_exploration_car_unlabeled(spark, tr_date_, EXPLORATION_COLS)
        print'############### Finished process ###############'