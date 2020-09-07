#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit
from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles


def keep_active_services(df):
    return df.where(col("cust_cod_estado_general").isin("01", "09"))


def keep_active_and_debt_services(df):
    return df.where(col("cust_cod_estado_general").isin("01", "03", "09"))


def get_non_recent_customers_filter(spark, date_, n_cycles, level='msisdn', verbose=False):
    '''
    Return the customers that have been in our base for the last n_days
    :param spark:
    :param date_:
    :param n_days:
    :param level:
    :param verbose:
    :param only_active: return only active customers
    :return:
    '''

    if ('nif' in level):
        level = 'nif_cliente'

    valid_rgus = ['mobile'] if level == 'msisdn' else ['fbb', 'mobile', 'tv', 'bam_mobile', 'fixed', 'bam']

    # REMOVING RECENT CUSTOMERS (EARLY CHURN EFFECT IS REMOVED)

    # Only customers active for at least the last N cycles are retained

    # Current

    current_base_df = spark \
        .read \
        .parquet(
        "/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/year=" + str(int(date_[:4])) + "/month=" + str(
            int(date_[4:6])) + "/day=" + str(int(date_[6:8]))) \
        .filter(col('Serv_rgu').isin(valid_rgus)) \
        .select(level) \
        .distinct()

    current_base_df = keep_active_services(current_base_df)

    #size_current_base = current_base_df.count()

    # Previous

    prev_date_ = move_date_n_cycles(date_, -n_cycles)

    prev_base_df = spark \
        .read \
        .parquet("/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/year=" + str(
        int(prev_date_[:4])) + "/month=" + str(int(prev_date_[4:6])) + "/day=" + str(int(prev_date_[6:8]))) \
        .filter(col('Serv_rgu').isin(valid_rgus)) \
        .select(level) \
        .distinct()

    prev_base_df = keep_active_services(prev_base_df)

    active_base_df = current_base_df.join(prev_base_df, [level], 'inner').select(level).distinct()

    if (verbose):
        print '[Info get_active_filter] Services retained after the active filter for ' + str(date_) + ' and ' + str(
            n_cycles*7) + ' is ' + str(active_base_df.count()) + ' out of ' + str(current_base_df.count())

    return active_base_df


def get_disconnection_process_filter(spark, date_, n_cycles, verbose=False):
    ''''
    Return a pyspark df with column nif_cliente
    '''

    # REMOVING CUSTOMERS WHO HAVE INITIATED THE DISCONNECTION PROCESS (has decreased the numnber of mobile, fbb or tv services)

    current_base_df = spark \
        .read \
        .parquet(
        "/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/year=" + str(int(date_[:4])) + "/month=" + str(
            int(date_[4:6])) + "/day=" + str(int(date_[6:8]))) \
        .select('nif_cliente', 'Cust_Agg_fbb_services_nif', 'Cust_Agg_mobile_services_nif', 'Cust_Agg_tv_services_nif') \
        .distinct() \
        .withColumnRenamed('Cust_Agg_fbb_services_nif', 'current_nb_fbb_services_nif') \
        .withColumnRenamed('Cust_Agg_mobile_services_nif', 'current_nb_mobile_services_nif') \
        .withColumnRenamed('Cust_Agg_tv_services_nif', 'current_nb_tv_services_nif')

    # Previous

    prev_date_ = move_date_n_cycles(date_, -n_cycles)

    prev_base_df = spark \
        .read \
        .parquet("/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/year=" + str(
        int(prev_date_[:4])) + "/month=" + str(int(prev_date_[4:6])) + "/day=" + str(int(prev_date_[6:8]))) \
        .select('nif_cliente', 'Cust_Agg_fbb_services_nif', 'Cust_Agg_mobile_services_nif', 'Cust_Agg_tv_services_nif') \
        .distinct() \
        .withColumnRenamed('Cust_Agg_fbb_services_nif', 'prev_nb_fbb_services_nif') \
        .withColumnRenamed('Cust_Agg_mobile_services_nif', 'prev_nb_mobile_services_nif') \
        .withColumnRenamed('Cust_Agg_tv_services_nif', 'prev_nb_tv_services_nif')

    non_churning_df = current_base_df \
        .join(prev_base_df, ['nif_cliente'], 'left') \
        .na.fill({'prev_nb_fbb_services_nif': 0, 'prev_nb_mobile_services_nif': 0, 'prev_nb_tv_services_nif': 0}) \
        .withColumn('inc_nb_fbb_services_nif', col('current_nb_fbb_services_nif') - col('prev_nb_fbb_services_nif')) \
        .withColumn('inc_nb_mobile_services_nif',
                    col('current_nb_mobile_services_nif') - col('prev_nb_mobile_services_nif')) \
        .withColumn('inc_nb_tv_services_nif', col('current_nb_tv_services_nif') - col('prev_nb_tv_services_nif')) \
        .filter((col('inc_nb_fbb_services_nif') >= 0) & (col('inc_nb_mobile_services_nif') >= 0) & (
                col('inc_nb_tv_services_nif') >= 0)) \
        .select('nif_cliente').distinct()

    if (verbose):
        print '[Info get_disconnection_process_filter] Customers retained after the disconnection process filter is ' + str(
            non_churning_df.count()) + ' out of ' + str(current_base_df.count())

    return non_churning_df
