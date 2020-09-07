#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import lit, col, when
import re

from churn_nrt.src.data.customer_base import CustomerBase, CustomerAdditional
from churn_nrt.src.projects.models.virgin.constants import HIGH_CORRELATED_VARS

# Add here the verified modules to be considered in the myvf model
METADATA_STANDARD_MODULES =  ['navcomp_adv', 'callscomp_adv', 'customer'] # , 'ccc', 'spinners', "scores", "myvf"] 

def get_metadata(spark, sources=None, filter_correlated_feats=False):

    print("[metadata] get_metadata ")
    if not sources:
        print("[metadata] Using standard modules {}".format(",".join(METADATA_STANDARD_MODULES)))
        sources = METADATA_STANDARD_MODULES

    metadata = None
    version=1

    if "ccc" in sources:
        from churn_nrt.src.data.ccc import CCC
        metadata_ = CCC(spark, level="msisdn").get_metadata()
        print("[metadata] Adding CCC metadata")
        metadata = metadata_ if metadata is None else  metadata.union(metadata_)

    if "spinners" in sources:
        from churn_nrt.src.data.spinners import Spinners
        metadata_ = Spinners(spark).get_metadata()
        print("[metadata] Adding Spinners metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "customer" in sources:
        metadata_ = get_customer_metadata(spark) # custom function
        print("[metadata] Adding customer metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "navcomp_adv" in sources:
        from churn_nrt.src.data.navcomp_data import NavCompAdvData
        metadata_ = NavCompAdvData(spark).get_metadata()
        print("[metadata] Excluding categoricals from NavCompAdvData metadata")

        metadata_ = metadata_.where(~col("feature").isin("most_consulted_operator_dd30", "most_consulted_operator_last30",
                                                        "most_consulted_operator_last7", "most_consulted_operator_last60",
                                                        "most_consulted_operator_last15"))

        print("[metadata] Adding NavCompDataAdv metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "callscomp_adv" in sources:
        from churn_nrt.src.data.calls_comps_data import CallsCompAdvData
        metadata_ = CallsCompAdvData(spark).get_metadata()
        print("[metadata] Excluding categoricals from CallsCompDataAdv metadata")
        metadata_ = metadata_.where(~col("feature").isin("callscomp_most_consulted_comp_last15", "callscomp_most_consulted_comp_dd30",
                                                        "callscomp_most_consulted_group_last7", "callscomp_most_consulted_comp_last30",
                                                        "callscomp_most_consulted_group_dd30", "callscomp_most_consulted_comp_last7",
                                                        "callscomp_most_consulted_group_last30", "callscomp_most_consulted_group_last15",
                                                        "callscomp_most_consulted_comp_last60", "callscomp_most_consulted_group_last60"))

        print("[metadata] Adding CallsCompDataAdv metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    # if "callscomp" in sources:
    #     from churn_nrt.src.data.calls_comps_data import CallsCompData
    #     metadata_ = CallsCompData(spark, 7).get_metadata()
    #     print("[metadata] Adding CallsCompData metadata")
    #     metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "billing" in sources:
        from churn_nrt.src.data.billing import Billing
        metadata_ = Billing(spark).get_metadata()
        print("[Metadata] Adding Billing metadata")
        metadata_ = metadata_.where(~col("feature").rlike("yearmonth_billing"))
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if "scores" in sources:
        from churn_nrt.src.data.scores import Scores
        metadata_ = Scores(spark).get_metadata()
        print("[metadata] Adding Scores metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if any([ss.startswith("myvf") and not ss.startswith("myvfweb") for ss in sources]):
        print(sources)
        if "myvf" in sources:
            version = 1
        else:
            version = int(re.search("myvf_v([1-9])", [ss for ss in sources if ss.startswith("myvf")][0]).group(1))

        from churn_nrt.src.data.myvf_data import MyVFdata
        metadata_ = MyVFdata(spark, "app", version=version).get_metadata()
        print("Adding MyVFdata metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)

    if any([ss.startswith("myvfweb") for ss in sources]):
        print(sources)
        if "myvfweb" in sources:
            version = 1
        else:
            version = int(re.search("myvfweb_v([1-9])", [ss for ss in sources if ss.startswith("myvfweb")][0]).group(1))

        from churn_nrt.src.data.myvf_data import MyVFdata
        metadata_ = MyVFdata(spark, "web", version=version).get_metadata()
        print("Adding MyVFdata Web metadata")
        metadata = metadata_ if metadata is None else metadata.union(metadata_)


    if filter_correlated_feats:
        print("[metadata] Removing high correlated feats from metadata")
        before_rem = metadata.count()
        metadata = metadata.where(~col("feature").isin(HIGH_CORRELATED_VARS))
        after_rem = metadata.count()
        print("[metadata] Removed {} feats from metadata: before={} after={}".format(before_rem-after_rem, before_rem, after_rem))
    else:
        print("[metadata] High correlated feats wont be removed from metadata")



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

    categoricals = ["blindaje"]
    imp_val_categ = ["unknown"]

    data = {'feature': categoricals + ["flag_north_ccaa"], 'imp_value': imp_val_categ + [str(0)]}

    import pandas as pd

    df_extra_metadata = (spark.createDataFrame(pd.DataFrame(data)).withColumn('source', lit('customer')).withColumn('type', lit('numeric'))
                         .withColumn('type', when(col('feature').isin(categoricals), 'categorical').otherwise(col('type')))
                         .withColumn('level', lit('msisdn')))

    customer_metadata = customer_metadata.union(df_extra_metadata)

    return customer_metadata

