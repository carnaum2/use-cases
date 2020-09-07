#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np
from churn_nrt.src.projects.models.trigger_zhilabs.constants import HIGH_CORRELATED_VARS_FTTH

# Add here the verified modules to be considered in the navcomp model
METADATA_STANDARD_MODULES = ["reimbursements", "tickets", "orders", "customer", "ccc", "billing", "spinners", "additional", "zhilabs_ftth", "inc_ftth"]

METADATA_STANDARD_MODULES_DSL = ["reimbursements", "tickets", "orders", "customer", "ccc", "billing", "spinners", "additional", "zhilabs_dsl", "inc_dsl"]

METADATA_STANDARD_MODULES_HFC = ["reimbursements", "tickets", "orders", "customer", "ccc", "billing", "spinners", "additional", "zhilabs_hfc", "inc_hfc"]


CATEGORICAL_COLS = ["blindaje", "segment_nif", "cpe_model", "network_access_type"]
NON_INFO_COLS = ["nif_cliente", "num_cliente"]

def get_metadata(spark, sources=None, filter_correlated_feats=False):
    from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_metadata_all, METADATA_STANDARD_MODULES
    if not sources:
        sources = METADATA_STANDARD_MODULES
    print(sources)
    metadata = get_metadata_all(spark, sources, True)

    return metadata

def get_additional_trigger_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''


    # Select only these columns for CustomerBase and CustomerAdditional
    cat_feats = ['blindaje']#cpe_model

    null_imp_dict_cat = dict([(x, 'unknown') for x in cat_feats])

    feats_cat = null_imp_dict_cat.keys()

    na_vals_cat = null_imp_dict_cat.values()


    data_cat = {'feature': feats_cat, 'imp_value': na_vals_cat}

    import pandas as pd

    metadata_df_cat = spark \
        .createDataFrame(pd.DataFrame(data_cat)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('nif'))

    metadata_df = metadata_df_cat

    return metadata_df

def get_metadata_all(spark, sources=None, filter_correlated_feats=True):

    if not sources:
        sources = METADATA_STANDARD_MODULES

    metadata = None
    if "reimbursements" in sources:
        from churn_nrt.src.data.reimbursements import Reimbursements
        metadata_ = Reimbursements(spark).get_metadata()
        print("[Metadata] Adding reimbursements metadata")
        metadata = metadata_ if metadata is None else  metadata.union(metadata_)

    if "tickets" in sources:
        from churn_nrt.src.data.tickets import Tickets
        metadata_ = Tickets(spark).get_metadata()
        print("[Metadata] Adding tickets metadata")
        feats_tickets = ['num_tickets_tipo_tramitacion_w8', 'num_tickets_tipo_reclamacion_w8','num_tickets_tipo_averia_w8', \
                         'num_tickets_tipo_averia_w4', 'num_tickets_tipo_reclamacion_w4','num_tickets_tipo_averia_w4w2', \
                         'INC_num_tickets_tipo_averia_w4w4', 'num_tickets_tipo_averia_closed','num_tickets_tipo_reclamacion_closed', \
                         'num_tickets_closed', 'max_time_closed_tipo_averia', 'max_time_closed_tipo_reclamacion', \
                         'mean_time_closed_averia']
        metadata_ = metadata_.where(col("feature").isin(feats_tickets))
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "orders" in sources:
        from churn_nrt.src.data.orders_sla import OrdersSLA
        metadata_ = OrdersSLA(spark).get_metadata()
        feats_orders = ['nb_started_orders_last14', 'nb_started_orders_last120', 'nb_started_orders_last240', 'nb_completed_orders_last14',\
               'nb_completed_orders_last240', 'avg_days_bw_open_orders', 'first_order_last365', 'last_order_last365', 'mean_sla_factor_last240',\
               'nb_started_orders_last14_impact', 'nb_started_orders_last30_impact', 'nb_started_orders_last90_impact', 'nb_started_orders_last120_impact',\
               'nb_started_orders_last240_low_impact', 'aumento_last_order_last365', 'cambio_orders_last90', \
               'cambio_orders_last240', 'cambio_nb_completed_orders_last90', 'disminucion_orders_last120', \
               'disminucion_avg_days_completed_orders_last180', 'disminucion_nb_completed_orders_last30', \
               'disminucion_nb_completed_orders_last90', 'ord_admin_orders_last365']
        print("[Metadata] Adding orders metadata")
        metadata_ = metadata_.where(col("feature").isin(feats_orders))
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "customer" in sources:
        from churn_nrt.src.data.customer_base import CustomerBase
        metadata_ = CustomerBase(spark).get_metadata()
        print("[Metadata] Adding Customer metadata")
        non_inf_cust = ['rgus_list', 'cod_estado_general', 'srv_basic', 'TARIFF', 'rgu']
        metadata_ = metadata_.where(col("feature").isin(non_inf_cust)==False)
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "ccc" in sources:
        from churn_nrt.src.data.ccc import CCC
        metadata_ = CCC(spark).get_metadata()
        feats_ccc = ['num_calls_w2', 'DSL_FIBER_INCIDENCES_AND_SUPPORT_w4', 'BILLING_POSTPAID_w4', 'num_calls_w4', 'PRODUCT_AND_SERVICE_MANAGEMENT_w8',\
            'num_calls_w8', 'inc_BILLING_POSTPAID_w4vsw4', 'inc_num_calls_w4vsw4']
        print("[Metadata] Adding CCC metadata")
        metadata_ = metadata_.where(col("feature").isin(feats_ccc))
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "billing" in sources:
        from churn_nrt.src.data.billing import Billing
        metadata_ = Billing(spark).get_metadata()
        non_inf_billing = ['Bill_N1_yearmonth_billing', 'Bill_N2_yearmonth_billing', 'Bill_N3_yearmonth_billing', 'Bill_N4_yearmonth_billing', 'Bill_N5_yearmonth_billing']
        metadata_ = metadata_.where(col("feature").isin(non_inf_billing)==False)
        print("[Metadata] Adding Billing metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "spinners" in sources:
        from churn_nrt.src.data.spinners import Spinners
        metadata_ = Spinners(spark).get_metadata()
        print("[Metadata] Adding Spinners metadata")
        feats_spinners = ['nif_port_number', 'nif_min_days_since_port', 'nif_distinct_msisdn', 'nif_port_freq_per_day', 'nif_port_freq_per_msisdn', 'yoigo_ACAN', 'otros_APOR', 'total_acan', 'total_apor',
                          'total_orange', 'total_yoigo', 'num_distinct_operators']
        metadata_ = metadata_.where(col("feature").isin(feats_spinners))
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "additional" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_additional_trigger_metadata
        metadata_ = get_additional_trigger_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "zhilabs_ftth" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_ftth_metadata
        metadata_ = get_zhilabs_ftth_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "zhilabs_dsl" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_dsl_metadata
        metadata_ = get_zhilabs_dsl_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "inc_ftth" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_inc_ftth_metadata
        metadata_ = get_zhilabs_inc_ftth_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "zhilabs_hfc" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_hfc_metadata
        metadata_ = get_zhilabs_hfc_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "inc_hfc" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_inc_hfc_metadata
        metadata_ = get_zhilabs_inc_hfc_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "inc_dsl" in sources:
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_zhilabs_inc_dsl_metadata
        metadata_ = get_zhilabs_inc_dsl_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if filter_correlated_feats:
        from churn_nrt.src.projects.models.trigger_zhilabs.constants import HIGH_CORRELATED_VARS_FTTH, HIGH_CORRELATED_VARS_HFC
        before_rem = metadata.count()
        print("[metadata] Removing high correlated feats from metadata")
        if "zhilabs_hfc" in sources:
            correlated_feats = []#HIGH_CORRELATED_VARS_HFC
        elif "zhilabs_ftth" in sources:
            correlated_feats = HIGH_CORRELATED_VARS_FTTH
        metadata = metadata.where(~col("feature").isin(correlated_feats))
        after_rem = metadata.count()
        print("[metadata] Removed {} feats from metadata: before={} after={}".format(before_rem-after_rem, before_rem, after_rem))

    return metadata


def get_zhilabs_ftth_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    feats_hourly_ftth = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    feats_daily = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_degradation', 'issue_ftth_prx']


    feats_total = feats_hourly_ftth + feats_daily
    feats_ftth_ = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_mean_', 'zhilabs_ftth_std_'] for f in feats_total] + ["min_memory"]

    add_feats = ['zhilabs_ftth_max_num_restarts_day','zhilabs_ftth_mean_num_restarts_day','zhilabs_ftth_days_with_quick_restarts',
 'zhilabs_ftth_count_flag_ber_down_warning','zhilabs_ftth_count_flag_ber_up_warning',  'zhilabs_ftth_count_flag_ber_down_critical','zhilabs_ftth_count_flag_ber_up_critical']

    feats_ftth = feats_ftth_ + add_feats

    null_imp_dict = dict([(x, '0.0') for x in feats_ftth])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    cat_feats = ['network_access_type', 'network_type', 'network_sharing','cpe_model']
    #cat_feats = []
    null_imp_dict_cat = dict([(x, 'unknown') for x in cat_feats])

    feats_cat = null_imp_dict_cat.keys()

    na_vals_cat = null_imp_dict_cat.values()


    data_cat = {'feature': feats_cat, 'imp_value': na_vals_cat}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('zhilabs')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('client_id'))

    metadata_df_cat = spark \
        .createDataFrame(pd.DataFrame(data_cat)) \
        .withColumn('source', lit('zhilabs')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('client_id'))

    metadata_df = metadata_df_cat.union(metadata_df)

    #metadata_df = metadata_df.where(col("feature").isin(rem_feats) == False)

    return metadata_df

def get_zhilabs_inc_ftth_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    feats_hourly_ftth = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    feats_daily = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_degradation', 'issue_ftth_prx']

    feats_total = feats_hourly_ftth + feats_daily
    feats_ftth_ = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_mean_', 'zhilabs_ftth_std_'] for f in feats_total]

    feats_ftth = ['inc_' + f + suf for f in feats_ftth_ for suf in ['_w1w1', '_w2w2']]

    null_imp_dict = dict([(x, '0.0') for x in feats_ftth])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('zhilabs_inc')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('client_id'))

    #metadata_df = metadata_df.where(col("feature").isin(rem_feats) == False)#.where(col("feature").isin(sel_feats))

    return metadata_df

def get_zhilabs_hfc_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    hfc_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'issue_hfc_docsis_3_1_equivalent_modulation_critical_current',
                 'issue_hfc_fec_upstream_critical_current', 'issue_hfc_fec_upstream_warning_current',
                 'issue_hfc_flaps_critical_current', 'issue_hfc_flaps_warning_current',
                 'issue_hfc_prx_downstream_critical_current', 'issue_hfc_prx_downstream_warning_current',
                 'issue_hfc_ptx_upstream_critical_current', 'issue_hfc_ptx_upstream_warning_current',
                 'issue_hfc_snr_downstream_critical_current', 'issue_hfc_snr_downstream_warning_current',
                 'issue_hfc_snr_upstream_critical_current', 'issue_hfc_snr_upstream_warning_current',
                 'issue_hfc_status_critical_current', 'issue_hfc_status_warning_current',
                 'hfc_percent_words_uncorrected_downstream', 'hfc_percent_words_uncorrected_upstream',
                 'hfc_prx_dowstream_average', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average',
                 'hfc_number_of_flaps_current', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    feats_hfc = [pre + f for pre in ['zhilabs_hfc_max_', 'zhilabs_hfc_min_', 'zhilabs_hfc_mean_', 'zhilabs_hfc_std_'] for f in hfc_cols]
    null_imp_dict = dict([(x, '0.0') for x in feats_hfc])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    cat_feats = ['cpe_model', 'network_access_type']

    null_imp_dict_cat = dict([(x, 'unknown') for x in cat_feats])

    feats_cat = null_imp_dict_cat.keys()

    na_vals_cat = null_imp_dict_cat.values()
    

    data_cat = {'feature': feats_cat, 'imp_value': na_vals_cat}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('nif'))

    metadata_df_cat = spark \
        .createDataFrame(pd.DataFrame(data_cat)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('nif'))

    metadata_df = metadata_df.union(metadata_df_cat)

    return metadata_df

def get_zhilabs_inc_hfc_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    hfc_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'issue_hfc_docsis_3_1_equivalent_modulation_critical_current',
                 'issue_hfc_fec_upstream_critical_current', 'issue_hfc_fec_upstream_warning_current',
                 'issue_hfc_flaps_critical_current', 'issue_hfc_flaps_warning_current',
                 'issue_hfc_prx_downstream_critical_current', 'issue_hfc_prx_downstream_warning_current',
                 'issue_hfc_ptx_upstream_critical_current', 'issue_hfc_ptx_upstream_warning_current',
                 'issue_hfc_snr_downstream_critical_current', 'issue_hfc_snr_downstream_warning_current',
                 'issue_hfc_snr_upstream_critical_current', 'issue_hfc_snr_upstream_warning_current',
                 'issue_hfc_status_critical_current', 'issue_hfc_status_warning_current',
                 'hfc_percent_words_uncorrected_downstream', 'hfc_percent_words_uncorrected_upstream',
                 'hfc_prx_dowstream_average', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average',
                 'hfc_number_of_flaps_current', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    feats_hfc_ = [pre + f for pre in ['zhilabs_hfc_max_', 'zhilabs_hfc_min_', 'zhilabs_hfc_mean_', 'zhilabs_hfc_std_'] for f in hfc_cols]

    feats_hfc = ['inc_' + f + suf for f in feats_hfc_ for suf in ['_w1w1', '_w2w2']]

    null_imp_dict = dict([(x, '0.0') for x in feats_hfc])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('zhilabs_inc')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('client_id'))

    #metadata_df = metadata_df.where(col("feature").isin(rem_feats) == False)#.where(col("feature").isin(sel_feats))

    return metadata_df

def get_zhilabs_dsl_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    adsl_cols = ['cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries', 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_sent',
                 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received', 'wlan_2_4_stats_errors_sent',
                 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received', 'wlan_5_stats_errors_sent',
                 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'cpe_cpu_usage', 'cpe_memory_free',
                 'cpe_memory_total', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    feats_dsl = [pre + f for pre in ['zhilabs_dsl_max_', 'zhilabs_dsl_min_', 'zhilabs_dsl_mean_', 'zhilabs_dsl_std_'] for f in adsl_cols]
    null_imp_dict = dict([(x, '0.0') for x in feats_dsl])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    # Select only these columns for CustomerBase and CustomerAdditional
    cat_feats = ['cpe_model',  'network_access_type']

    null_imp_dict_cat = dict([(x, 'unknown') for x in cat_feats])

    feats_cat = null_imp_dict_cat.keys()

    na_vals_cat = null_imp_dict_cat.values()


    data_cat = {'feature': feats_cat, 'imp_value': na_vals_cat}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('nif'))

    metadata_df_cat = spark \
        .createDataFrame(pd.DataFrame(data_cat)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('nif'))

    metadata_df = metadata_df_cat.union(metadata_df)

    return metadata_df

def get_zhilabs_inc_dsl_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    adsl_cols = ['cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries', 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_sent',
                 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received', 'wlan_2_4_stats_errors_sent',
                 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received', 'wlan_5_stats_errors_sent',
                 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'cpe_cpu_usage', 'cpe_memory_free',
                 'cpe_memory_total', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    feats_dsl_ = [pre + f for pre in ['zhilabs_dsl_max_', 'zhilabs_dsl_min_', 'zhilabs_dsl_mean_', 'zhilabs_dsl_std_'] for f in adsl_cols]

    feats_dsl = ['inc_' + f + suf for f in feats_dsl_ for suf in ['_w1w1', '_w2w2']]

    null_imp_dict = dict([(x, '0.0') for x in feats_dsl])
    feats_num = null_imp_dict.keys()
    na_vals_num = null_imp_dict.values()
    data = {'feature': feats_num, 'imp_value': na_vals_num}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('zhilabs_inc')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('client_id'))

    #metadata_df = metadata_df.where(col("feature").isin(rem_feats) == False)#.where(col("feature").isin(sel_feats))

    return metadata_df
