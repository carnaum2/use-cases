#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np
import re

from churn_nrt.src.data.customer_base import CustomerBase, CustomerAdditional

# Add here the verified modules to be considered in the myvf model
METADATA_STANDARD_MODULES =  ['navcomp', 'customer', 'ccc', 'spinners', "scores", "myvf"]

def get_metadata(spark, sources=None, filter_correlated_feats=False):

    if not sources:
        sources = METADATA_STANDARD_MODULES

    metadata = None

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

    if "navcomp" in sources:
        from churn_nrt.src.data.navcomp_data import NavCompData
        metadata_ = NavCompData(spark, 15).get_metadata()
        print("[Metadata] Adding NavCompData metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "scores" in sources:
        from churn_nrt.src.data.scores import Scores
        metadata_ = Scores(spark).get_metadata()
        print("[Metadata] Adding Scores metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if any([ss.startswith("myvf") for ss in sources]):
        print(sources)
        if "myvf" in sources:
            version = 1
        else:
            version = int(re.search("myvf_v([1-9])", [ss for ss in sources if ss.startswith("myvf")][0]).group(1))

        from churn_nrt.src.data.myvf_data import MyVFdata
        metadata_ = MyVFdata(spark, "app", version=version).get_metadata()
        print("Adding MyVFdata metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "geneva" in sources:
        from churn_nrt.src.data.geneva_data import GenevaData
        #FIXME hard-coded 90?
        metadata_ = GenevaData(spark, 90).get_metadata()
        print("[Metadata] Adding Geneva 90 metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "myvfweb" in sources:
        #FIXME pass platform?
        from churn_nrt.src.data.myvf_data import MyVFdata
        metadata_ = MyVFdata(spark, "web").get_metadata()
        print("Adding MyVFdata Web metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if filter_correlated_feats and any([ss.startswith("myvf") for ss in sources]):
        before_rem = metadata.count()
        print("[metadata] Removing high correlated feats from metadata")
        #if version == 3:
        #    metadata = metadata.where(~col("feature").isin(HIGH_CORRELATED_VARS_VERSION3))
        #else:
        #    metadata = metadata.where(~col("feature").isin(HIGH_CORRELATED_VARS_VERSION1_2))

        after_rem = metadata.count()
        print("[metadata] Removed {} feats from metadata: before={} after={}".format(before_rem-after_rem, before_rem, after_rem))

    return metadata

def get_customer_metadata(spark):
    '''
    Build customer metadata from CustomerBase and CustomerAdditional metadatas
    :param spark:
    :return:
    '''

    #ccc_metadata1 = get_metadata(spark, sources=['customer'])
    customer_base_metadata = CustomerBase(spark).get_metadata()
    customer_additional_metadata = CustomerAdditional(spark).get_metadata()

    customer_metadata = customer_base_metadata.union(customer_additional_metadata)

    customer_feats = [u'tgs_days_until_f_fin_bi','tgs_has_discount', u'nb_rgus_nif', u'segment_nif', u'age', u'age_disc', u'nb_mobile_services_nif',
                      u'nb_fixed_services_nif', u'nb_fbb_services_nif', u'nb_tv_services_nif', u'nb_bam_services_nif',
                      u'nb_prepaid_services_nif', u'nb_bam_mobile_services_nif']

    customer_metadata = customer_metadata.where(col("feature").isin(customer_feats))
    customer_metadata = customer_metadata.withColumn("source", lit("customer"))

    data = {'feature': ["blindaje"], 'imp_value': "unknown"}

    import pandas as pd

    df_blindaje_metadata = (spark.createDataFrame(pd.DataFrame(data)).withColumn('source', lit('customer'))
                                                                          .withColumn('type', lit('categorical'))
                                                                          .withColumn('level', lit('msisdn')))

    customer_metadata = customer_metadata.union(df_blindaje_metadata)

    return customer_metadata

