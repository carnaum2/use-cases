#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np

# Add here the verified modules to be considered in the navcomp model
METADATA_STANDARD_MODULES =  ['reimbursements', 'tickets', 'orders', 'customer', "ccc", "billing", "additional"]


# List of columns to be used in the model
FEAT_COLS = list({'BILLING_POSTPAID_w2',
                         'BILLING_POSTPAID_w4',
                         'BILLING_POSTPAID_w8',
                         'nif_cliente',
                         'tgs_days_until_f_fin_bi',
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
                         'billing_nb_last_bills',
                         'billing_avg_days_bw_bills',
                         'billing_min_days_bw_bills',
                         'nb_invoices',
                         'greatest_diff_bw_bills',
                         'least_diff_bw_bills',
                         'billing_current_debt',
                         'billing_current_vf_debt',
                         'billing_current_client_debt',
                         'Reimbursement_days_2_solve', 'flag_incremento_rgus',
                         'Reimbursement_adjustment_net', 'Reimbursement_num_n1', 'Reimbursement_num_n2', 'Reimbursement_num',
                         'nb_started_orders_last14', 'tgs_has_discount', 'billing_max_days_bw_bills'
                         })#

CATEGORICAL_COLS = ['blindaje', 'segment_nif']
NON_INFO_COLS = ["nif_cliente"]

def get_metadata(spark, sources=None, filter_correlated_feats=False):
    from churn_nrt.src.projects.models.trigger_service.metadata import get_metadata_all, FEAT_COLS
    metadata = get_metadata_all(spark, sources)

    from churn_nrt.src.data.tickets import Tickets
    metadata_tickets = Tickets(spark).get_metadata()

    ticket_feats = metadata_tickets.select("feature").rdd.flatMap(lambda x: x).collect()
    ticket_feats = [str(f) for f in ticket_feats]

    model_feats = FEAT_COLS + ticket_feats

    metadata_filtered = metadata.where(col("feature").isin(model_feats))

    return metadata_filtered


def get_metadata_all(spark, sources=None, filter_correlated_feats=False):

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
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "orders" in sources:
        from churn_nrt.src.data.orders_sla import OrdersSLA
        metadata_ = OrdersSLA(spark).get_metadata()
        print("[Metadata] Adding orders metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "customer" in sources:
        from churn_nrt.src.data.customer_base import CustomerBase
        metadata_ = CustomerBase(spark).get_metadata()
        print("[Metadata] Adding Customer metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "ccc" in sources:
        from churn_nrt.src.data.ccc import CCC
        metadata_ = CCC(spark).get_metadata()
        print("[Metadata] Adding CCC metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "billing" in sources:
        from churn_nrt.src.data.billing import Billing
        metadata_ = Billing(spark).get_metadata()
        print("[Metadata] Adding Billing metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "additional" in sources:
        metadata_ = get_additional_trigger_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    return metadata

def get_additional_trigger_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''


    # Select only these columns for CustomerBase and CustomerAdditional
    add_feats = ['nb_superofertas', 'diff_rgus_d_d98']
    cat_feats = ['blindaje']#cpe_model

    null_imp_dict = dict([(x, 0.0) for x in add_feats])
    null_imp_dict_cat = dict([(x, 'unknown') for x in cat_feats])

    feats = null_imp_dict.keys()
    feats_cat = null_imp_dict_cat.keys()

    na_vals = null_imp_dict.values()
    na_vals_cat = null_imp_dict_cat.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}
    data_cat = {'feature': feats_cat, 'imp_value': na_vals_cat}

    import pandas as pd

    metadata_df_ = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('rgus')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    metadata_df_cat = spark \
        .createDataFrame(pd.DataFrame(data_cat)) \
        .withColumn('source', lit('bound')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('nif'))

    metadata_df = metadata_df_.union(metadata_df_cat)

    return metadata_df


