#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import lit, col, when
import re

from churn_nrt.src.projects.models.price_sensitivity.new_product.constants import VERSION_10_VARS
from churn_nrt.src.data_utils.ids_utils import get_ids_nc
from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features
from churn_nrt.src.data_utils.ids_utils import get_ids_nc_feats

# Add here the verified modules to be considered in the myvf model
METADATA_STANDARD_MODULES =  ['pricesens_newprod']

FEATS_WITH_NULLS = ['Order_N4_Id', 'device_n4_imei', 'address_FLAG_HUELLA_ARIEL', 'Order_N10_Id', 'address_FECHA_ZC', 'Order_N2_Id',
                    'address_FECHA_HUELLA_NEBA', 'TNPS_VDN_10201', 'Order_N9_Id', 'TNPS_min_VDN', 'device_n1_imei', 'TNPS_VDN_20056',
                    'address_CODIGO_OLT', 'address_FLAG_HUELLA_HFC', 'device_n2_imei', 'address_FECHA_HUELLA_FTTH_ULTIMA', 'TNPS_VDN_20070',
                    'TNPS_VDN_20071', 'TNPS_VDN_20074', 'TNPS_VDN_20076', 'TNPS_VDN_20079', 'TNPS_VDN_10340', 'TNPS_VDN_20035', 'TNPS_VDN_20030',
                    'TNPS_VDN_20039', 'TNPS_mean_VDN', 'TNPS_VDN_10361', 'TNPS_VDN_10360', 'Serv_OBJID', 'TNPS_VDN_10421', 'Order_N8_Id',
                    'address_VELOCIDAD_MAX_HFC', 'nif_cliente_tgs', 'TNPS_VDN_10324', 'address_FECHA_HUELLA_ARIEL', 'address_TIPO_HUELLA_FTTH',
                    'TNPS_max_VDN', 'address_FECHA_HUELLA_EUSKALTEL', 'TNPS_VDN_10388', 'TNPS_VDN_10381', 'TNPS_VDN_10380', 'TNPS_VDN_10383',
                    'TNPS_VDN_10382', 'TNPS_VDN_10385', 'TNPS_VDN_10384', 'TNPS_VDN_10386', 'address_FLAG_ZC', 'address_X_ID_GESCAL',
                    'TNPS_VDN_10346', 'address_FECHA_HUELLA_HFC_PRIMERA', 'address_FECHA_HUELLA_HFC_ULTIMA', 'Serv_TACADA', 'address_FLAG_NH_REAL',
                    'address_FLAG_HUELLA_MOVISTAR', 'TNPS_std_VDN', 'address_FLAG_HUELLA_NEBA', 'TNPS_VDN_20081', 'TNPS_VDN_20080', 'TNPS_VDN_20083',
                    'TNPS_VDN_20082', 'TNPS_VDN_20085', 'TNPS_VDN_20086', 'address_GRID', 'TNPS_VDN_20045', 'TNPS_VDN_20044', 'TNPS_VDN_20041',
                    'TNPS_VDN_20040', 'TNPS_VDN_20043', 'TNPS_VDN_20042', 'Order_N7_Id', 'OBJID_DECO_TV', 'TNPS_VDN_10368', 'TNPS_VDN_20060',
                    'address_FLAG_HUELLA_FTTH', 'address_FECHA_HUELLA_FTTH_PRIMERA', 'TNPS_VDN_10358', 'TNPS_VDN_10359', 'IMSI', 'TNPS_VDN_20055',
                    'Order_N6_Id', 'TNPS_VDN_10371', 'TNPS_VDN_10372', 'TNPS_VDN_10373', 'num_cliente_tgs', 'TNPS_VDN_20029', 'TNPS_VDN_10419',
                    'TNPS_VDN_10415', 'TNPS_VDN_10417', 'Order_N1_Id', 'device_n3_imei', 'TNPS_VDN_10339', 'TNPS_VDN_10331',
                    'address_FLAG_HUELLA_JAZZTEL', 'TNPS_VDN_10110', 'address_FECHA_HUELLA_JAZZTEL', 'TNPS_num_vdn', 'address_FLAG_NH_PREVISTA',
                    'Order_N5_Id', 'address_FECHA_HUELLA_MOVISTAR', 'Order_N3_Id', 'TNPS_VDN_20053', 'TNPS_list_VDN', 'address_FLAG_HUELLA_EUSKALTEL']



#REF_CLOSING_DAY = "20200114"

def get_version(sources):
    if "pricesens_newprod" in sources:
        version = 1
    else:
        version = int(re.search("pricesens_newprod([0-9]+)", [ss for ss in sources if ss.startswith("pricesens_newprod")][0]).group(1))
    return version



def get_metadata(spark, sources, filter_correlated_feats=False):

    print("[metadata] get_metadata")

    version = get_version(sources)

    cat_feats = []
    # To deal with different versions of ids....
    feats = list(set(get_feat_cols(spark, "20200121", version)) & set(get_feat_cols(spark, "20190831", version)) & set(get_feat_cols(spark, "20191221", version)))

    na_vals = [str(x) for x in len(feats) * [0]]
    data = {'feature': feats, 'imp_value': na_vals}
    #FIXME do something when more than one source is in sources
    import pandas as pd
    metadata_df = (spark.createDataFrame(pd.DataFrame(data))
                   .withColumn('source', lit('pricesens_newprod{}'.format(version)))
                   .withColumn('type', lit('numeric'))
                   .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))
                   .withColumn('level', lit('nc')))

    if filter_correlated_feats:
        before_rem = metadata_df.count()
        if version == 10:
            print("[metadata] Removing high correlated feats from metadata")
            metadata_df = metadata_df.where(col("feature").isin(VERSION_10_VARS))
            after_rem = metadata_df.count()
            print("[metadata] Removed {} feats from metadata: before={} after={}".format(before_rem-after_rem, before_rem, after_rem))

    return metadata_df

def get_feat_cols(spark, closing_day, version):

    df_date1 = get_ids_nc(spark, closing_day)
    non_inf_features = get_noninf_features() + get_no_input_feats()

    if version==5:
        nc_feats = get_ids_nc_feats(df_date1.dtypes, numeric=True)
        numeric_feats = [f[0] for f in df_date1.select(nc_feats).dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]
    else:
        numeric_feats = [f[0] for f in df_date1.dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]

    gnv_roam_columns = [c for c in numeric_feats if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_feats if ('address_' in c)]
    feats = list(set(numeric_feats) - set(non_inf_features) - set(gnv_roam_columns)- set(address_columns) - set(FEATS_WITH_NULLS))

    feats = list(set(feats))

    print("[metadata] get_feat_cols returned {} feats".format(len(feats)))

    return feats


