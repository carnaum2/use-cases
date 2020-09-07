#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np

# Add here the verified modules to be considered in the navcomp model
METADATA_STANDARD_MODULES =  ["billing"]

def get_metadata(spark, sources=None):

    if not sources:
        sources = METADATA_STANDARD_MODULES

    metadata = None

    '''

    if "ccc" in sources:
        from churn_nrt.src.data.ccc import CCC
        metadata_ = CCC(spark, level="msisdn").get_metadata()
        print("[Metadata] Adding CCC metadata")
        metadata = metadata_ if metadata is None else  metadata.union(metadata_)

    if "spinners" in sources:
        from churn_nrt.src.data.spinners import Spinners
        metadata_ = Spinners(spark).get_metadata()
        print("[Metadata] Adding Spinners metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "customer" in sources:
        metadata_ = get_customer_metadata(spark) # custom function
        print("[Metadata] Adding customer metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)
    
    '''

    if "billing" in sources:
        from churn_nrt.src.data.billing import Billing
        metadata_ = Billing(spark).get_metadata()
        print("[Metadata] Adding Billing metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    return metadata

def get_customer_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''
    from churn_nrt.src.data.customer_base import CustomerBase, CustomerAdditional

    #ccc_metadata1 = get_metadata(spark, sources=['customer'])
    customer_base_metadata = CustomerBase(spark).get_metadata()
    customer_additional_metadata = CustomerAdditional(spark).get_metadata()
    customer_metadata = customer_base_metadata.union(customer_additional_metadata)

    # Select only these columns for CustomerBase and CustomerAdditional
    customer_feats = [u'tgs_days_until_f_fin_bi', u'nb_rgus_nif', u'segment_nif', u'age', u'age_disc', u'nb_mobile_services_nif',
                      u'nb_fixed_services_nif', u'nb_fbb_services_nif', u'nb_tv_services_nif', u'nb_bam_services_nif',
                      u'nb_prepaid_services_nif', u'nb_bam_mobile_services_nif']

    customer_metadata = customer_metadata.where(col("feature").isin(customer_feats))

    # source name is renamed to "customer"
    customer_metadata = customer_metadata.withColumn("source", lit("customer"))

    return customer_metadata

