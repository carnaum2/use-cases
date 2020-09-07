#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import lit, col, when

def get_metadata(spark, sources=None, filter_correlated_feats=False):
    if not sources:
        sources = "ids"
    print(sources)
    metadata = get_metadata_all(spark, "20191231", sources, True)

    from churn_nrt.src.projects.models.price_sensitivity.price_elasticity.constants import CORRELATED_FEATS
    #metadata = metadata.where(~col("feature").isin(CORRELATED_FEATS))

    return metadata

def get_metadata_all(spark, date_, sources=None, filter_correlated_feats=True):
    from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features
    from churn_nrt.src.data_utils.ids_utils import get_ids_nif_feats, get_ids_msisdn_feats, get_ids_nc_feats, get_ids, get_ids_nc
    df_ids_train = get_ids_nc(spark, "20200114")
    nif_feats = get_ids_nif_feats(df_ids_train.dtypes)
    nc_feats = get_ids_nc_feats(df_ids_train.dtypes)
    df = get_ids(spark, "20200114")
    msisdn_feats_ = get_ids_msisdn_feats(df.dtypes, True)
    msisdn_feats = [f + '_agg_mean' for f in msisdn_feats_]

    feats = nif_feats + msisdn_feats + nc_feats

    numeric_feats = [f[0] for f in df_ids_train.select(feats).dtypes if
                     f[1] in ["double", "int", "float", "long", "bigint"]] + ['agg_sum_abs_inc']

    non_inf_features = get_noninf_features() + get_no_input_feats()
    print "[Info] Metadata has been read"
    gnv_roam_columns = [c for c in numeric_feats if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_feats if ('address_' in c)]
    numeric_columns = list(set(numeric_feats) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))
    #numeric_columns = [c for c in numeric_columns if ('bill' not in c.lower())]
    print"Selected feats for train: "
    print(numeric_columns)
    print"Number of selected feats: " + str(len(numeric_columns))
    #numeric_columns = [c for c in numeric_columns if 'Ord_' not in c]
    na_vals_num = ['0.0' for x in numeric_columns]
    data = {'feature': numeric_columns, 'imp_value': na_vals_num}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('ids')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('num_cliente'))

    return metadata_df


