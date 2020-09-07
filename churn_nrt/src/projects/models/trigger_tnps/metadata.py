#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np

METADATA_STANDARD_MODULES = ["customer", "ccc", "billing", "spinners", "additional", "tnps"]

CATEGORICAL_COLS = ["blindaje", "segment_nif"]
NON_INFO_COLS = ["nif_cliente", "num_cliente", "msisdn"]

def get_metadata(spark, sources=None, filter_correlated_feats=True):
    from churn_nrt.src.projects.models.trigger_tnps.metadata import get_metadata_all, METADATA_STANDARD_MODULES
    if not sources:
        sources = METADATA_STANDARD_MODULES
    print(sources)
    metadata = get_metadata_all(spark, sources, filter_correlated_feats)

    return metadata


def get_metadata_all(spark, sources=None, filter_correlated_feats=False):

    if not sources:
        sources = METADATA_STANDARD_MODULES

    metadata = None

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
        print("[Metadata] Adding CCC metadata")
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
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "additional" in sources:
        from churn_nrt.src.projects.models.trigger_tnps.metadata import get_additional_trigger_metadata
        metadata_ = get_additional_trigger_metadata(spark)
        print("[Metadata] Adding Additional metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    #from churn_nrt.src.projects.models.trigger_tnps.constants import FEAT_COLS
    #metadata = metadata.where(col("feature").isin(FEAT_COLS))

    if "tnps" in sources:
        from churn_nrt.src.data.tnps_data import TNPS
        metadata_ = TNPS(spark).get_metadata()
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if filter_correlated_feats:
        from churn_nrt.src.projects.models.trigger_tnps.constants import HIGHLY_CORRELATED
        metadata = metadata.where(col("feature").isin(HIGHLY_CORRELATED)==False)


    return metadata

def get_additional_trigger_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''


    # Select only these columns for CustomerBase and CustomerAdditional
    cat_feats = ['blindaje']

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
