#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import lit, col, when

def get_metadata(spark, sources=None, filter_correlated_feats=False):
    if not sources:
        sources = "ids"
    print(sources)
    metadata = get_metadata_all(spark, "20200114", sources, True)

    return metadata

def get_metadata_fbb(spark, sources=None, filter_correlated_feats=False):
    if not sources:
        sources = "ids"
    print(sources)
    metadata = get_metadata_all_fbb(spark, "20200114", sources, True)

    return metadata

def get_metadata_all(spark, date_, sources=None, filter_correlated_feats=True):

    from src.main.python.utils.general_functions import get_all_metadata
    from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features

    non_inf_features = get_noninf_features() + get_no_input_feats()
    final_map, categ_map, numeric_map, date_map, na_map = get_all_metadata("20200114")
    print "[Info] Metadata has been read"
    gnv_roam_columns = [c for c in numeric_map.keys() if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_map.keys() if ('address_' in c)]
    numeric_columns = list(set(numeric_map.keys()) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))
    #na_vals_num = ['0.0' for x in numeric_columns]
    data = {'feature': numeric_map.keys(), 'imp_value': numeric_map.values()}
    print"Selected feats for train: "
    print(numeric_columns)
    print"Number of selected feats: "+ str(len(numeric_columns))

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('ids')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('msisdn'))

    return metadata_df

def get_metadata_all_fbb(spark, date_, sources=None, filter_correlated_feats=True):

    from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features
    from churn_nrt.src.data_utils.ids_utils import get_ids_nif_feats, get_ids_msisdn_feats, get_ids_nc_feats, get_ids, get_ids_nc
    df_ids_train = get_ids_nc(spark, "20200114")
    nif_feats = get_ids_nif_feats(df_ids_train.dtypes)
    nc_feats = get_ids_nc_feats(df_ids_train.dtypes)
    df = get_ids(spark, "20200114")
    msisdn_feats_ = get_ids_msisdn_feats(df.dtypes, True)
    msisdn_feats = [f + '_agg_mean' for f in msisdn_feats_]

    feats = nif_feats + msisdn_feats + nc_feats

    numeric_feats = [f[0] for f in df_ids_train.select(feats).dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]

    non_inf_features = get_noninf_features() + get_no_input_feats()
    print "[Info] Metadata has been read"
    gnv_roam_columns = [c for c in numeric_feats if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_feats if ('address_' in c)]
    numeric_columns = list(set(numeric_feats) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))
    na_vals_num = ['0.0' for x in numeric_columns]
    data = {'feature': numeric_columns, 'imp_value': na_vals_num}
    print"Selected feats for train: "
    print(numeric_columns)
    print"Number of selected feats: "+ str(len(numeric_columns))

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('ids')) \
        .withColumn('type', lit('numerical')) \
        .withColumn('level', lit('num_cliente'))

    return metadata_df

