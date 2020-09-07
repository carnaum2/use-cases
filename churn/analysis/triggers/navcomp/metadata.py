#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import lit, col, when
import datetime as dt
import numpy as np

# Add here the verified modules to be considered in the navcomp model
METADATA_STANDARD_MODULES =  ['navcomp', 'customer', 'ccc', 'spinners', "scores"]

def get_metadata(spark, sources=None):

    if not sources:
        sources = METADATA_STANDARD_MODULES

    metadata = get_ccc_metadata(spark).union(get_spinners_metadata(spark))

    if "customer_basic" in sources:
        metadata = metadata.union(get_customer_metadata_basic(spark))
        print("Adding get_metadata += get_customer_metadata_basic")
    else:
        metadata = metadata.union(get_customer_metadata(spark))
        print("Adding get_metadata += get_customer_metadata")

    if "navcomp_basic" in sources:
        metadata = metadata.union(get_navcomp_metadata_basic(spark))
        print("Adding get_metadata += get_navcomp_metadata_basic")
    else:
        metadata = metadata.union(get_navcomp_metadata(spark))
        print("Adding get_metadata += get_navcomp_metadata")

    if "scores" in sources:
        metadata = metadata.union(get_scores_metadata(spark))
        print("Adding get_metadata += get_scores_metadata")

    if "volumen" in sources:
        metadata = metadata.union(get_volumen_metadata(spark))
        print("Adding get_metadata += get_volumen_metadata")

    if('all' not in sources):
        sources = [ss.replace("_basic", "") for ss in sources]
        print("sources", sources)
        metadata = metadata.filter(col('source').isin(sources))

    return metadata

def get_spinners_metadata(spark):

    na_map_count_feats = dict([("nif_port_number", 0.0),\
                               ("nif_min_days_since_port", -1.0),\
                               ("nif_max_days_since_port", 100000.0),\
                               ("nif_avg_days_since_port", 100000.0),\
                               ("nif_var_days_since_port", -1.0),\
                               ("nif_distinct_msisdn", 0.0),\
                               ("nif_port_freq_per_day", 0.0),\
                               ("nif_port_freq_per_msisdn", 0.0)])

    destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown",
                "otros"]

    estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

    import itertools

    na_map_destino_estado_feats = dict([(p[0] + "_" + p[1], 0.0) for p in list(itertools.product(destinos, estados))])

    total_feats = ["total_acan",\
     "total_apor",\
     "total_arec",\
     "total_movistar",\
     "total_simyo",\
     "total_orange",\
     "total_jazztel",\
     "total_yoigo",\
     "total_masmovil",\
     "total_pepephone",\
     "total_reuskal",\
     "total_unknown",\
     "total_otros",\
     "num_distinct_operators"]

    na_map = dict([(f, 0.0) for f in total_feats])

    na_map.update(na_map_count_feats)
    na_map.update(na_map_destino_estado_feats)

    feats = na_map.keys()

    na_vals = [str(x) for x in na_map.values()]

    cat_feats = []

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('spinners')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type'))) \
        .withColumn('level', lit('nif'))

    return metadata_df

def get_navcomp_metadata(spark):

    import itertools

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown"]

    count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["sum_count", "max_count"]))]

    days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]

    add_feats = ["sum_count_vdf",\
                 "sum_count_comps",\
                 "num_distinct_comps",\
                 "max_count_comps",\
                 "sum_distinct_days_with_navigation_vdf",\
                 "norm_sum_distinct_days_with_navigation_vdf",\
                 "sum_distinct_days_with_navigation_comps",\
                 "norm_sum_distinct_days_with_navigation_comps",\
                 "min_days_since_navigation_comps",\
                 "norm_min_days_since_navigation_comps",\
                 "max_days_since_navigation_comps",\
                 "norm_max_days_since_navigation_comps"]

    null_imp_dict = dict([(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if ("min_days" in x)] + [(x, 0) for x in add_feats])

    feats = null_imp_dict.keys()

    na_vals = [str(x) for x in null_imp_dict.values()]

    cat_feats = []
    # TODO add in production most_consulted operator
    # cat_feats = ["most_consulted_operator"]
    #cat_na_vals = ["None"]

    data = {'feature': feats, 'imp_value': na_vals}
    #data = {'feature': feats+cat_feats, 'imp_value': na_vals+cat_na_vals}

    import pandas as pd

    metadata_df = spark\
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('navcomp'))\
        .withColumn('type', lit('numeric')) \
        .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type'))) \
        .withColumn('level', lit('nif'))

    return metadata_df

def get_navcomp_metadata_basic(spark):

    import itertools

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown"]

    count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["sum_count", "max_count"]))]

    days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]


    null_imp_dict = dict([(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if ("min_days" in x)] )

    feats = null_imp_dict.keys()

    na_vals = [str(x) for x in null_imp_dict.values()]

    cat_feats = []
    # TODO add in production most_consulted operator
    # cat_feats = ["most_consulted_operator"]
    #cat_na_vals = ["None"]

    data = {'feature': feats, 'imp_value': na_vals}
    #data = {'feature': feats+cat_feats, 'imp_value': na_vals+cat_na_vals}

    import pandas as pd

    metadata_df = spark\
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('navcomp'))\
        .withColumn('type', lit('numeric')) \
        .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type'))) \
        .withColumn('level', lit('nif'))

    return metadata_df

def get_volumen_metadata(spark):

    feats=__build_volume_metadata()

    cat_feats = []

    na_vals = [str(x) for x in [0]*len(feats)]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('volumen'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))\
        .withColumn('level', lit('nif'))

    return metadata_df


def get_scores_metadata(spark):

    feats = ["latest_score"]

    cat_feats = []

    na_vals = [str(x) for x in [-1]]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('scores'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))\
        .withColumn('level', lit('nif'))

    return metadata_df


def get_customer_metadata_basic(spark):

    feats = ['tgs_days_until_f_fin_bi', 'nb_rgus', 'segment_nif']

    cat_feats = ['segment_nif']

    na_vals = [str(x) for x in [-1, 0, 'unknown']]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('customer'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))\
        .withColumn('level', lit('nif'))

    return metadata_df



def get_customer_metadata(spark):

    feats = ['tgs_days_until_f_fin_bi', 'nb_rgus', 'segment_nif', 'age', 'age_disc', "nb_rgus_mobile", "nb_rgus_fixed", "nb_rgus_fbb", "nb_rgus_tv", "nb_rgus_bam"]

    cat_feats = ['segment_nif', 'age_disc']

    na_vals = [str(x) for x in [-1, 0, 'unknown', '-1', 'other', 0, 0, 0, 0, 0]]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('customer'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))\
        .withColumn('level', lit('nif'))

    return metadata_df

def get_ccc_metadata(spark):

    from churn.analysis.triggers.ccc_utils.ccc_utils import get_bucket_info

    bucket = get_bucket_info(spark)

    bucket_list = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect() + ['num_calls']

    suffixes = ['w2', 'w4', 'w8']

    import itertools

    agg_feats = [p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, suffixes))]

    incs = ['w2vsw2', 'w4vsw4']

    inc_feats = ["inc_" + p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, incs))]

    feats = agg_feats + inc_feats

    cat_feats = []

    na_vals = [str(x) for x in len(feats)*[0]]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('ccc')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type'))) \
        .withColumn('level', lit('nif'))

    return metadata_df

def get_reimbursements_metadata(spark):

    reimb_map = {'Reimbursement_adjustment_net': 0.0,\
                 'Reimbursement_adjustment_debt': 0.0,\
                 'Reimbursement_num': 0.0,\
                 'Reimbursement_num_n8': 0.0,\
                 'Reimbursement_num_n6': 0.0,\
                 'Reimbursement_days_since': 10000.0,\
                 'Reimbursement_num_n4': 0.0,\
                 'Reimbursement_num_n5': 0.0,\
                 'Reimbursement_num_n2': 0.0,\
                 'Reimbursement_num_n3': 0.0,\
                 'Reimbursement_days_2_solve': -1.0,\
                 'Reimbursement_num_month_2': 0.0,\
                 'Reimbursement_num_n7': 0.0,\
                 'Reimbursement_num_n1': 0.0,\
                 'Reimbursement_num_month_1': 0.0}

    feats = reimb_map.keys()

    cat_feats = []

    na_vals = [str(x) for x in reimb_map.values()]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('reimbursements')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type'))) \
        .withColumn('level', lit('nif'))

    return metadata_df






def __build_volume_metadata():

    # from pykhaos.utils.date_functions import move_date_n_days, months_range
    # starting_day=move_date_n_days(closing_day, nb_days)
    # diff_days = (dt.datetime.strptime(closing_day, '%Y%m%d') - dt.datetime.strptime(starting_day, '%Y%m%d')).days
    # To be used when bgmerin1 can test it
    # periods = [str(dday) if dday!=2 else "prev" for dday in (np.arange(diff_days / 7) + 1)] + ['1w2']

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2"]

    cols = [col_.format(cc, week) for cc in competitors for week in ['1', '2'] for col_ in
    [
     'min_upload_{}_w{}',
     'max_upload_{}_w{}',
     'avg_upload_{}_w{}',

     'min_download_{}_w{}',
     'max_download_{}_w{}',
     'avg_download_{}_w{}',
    ]]


    cols += [col_.format(cc, week) for cc in competitors for week in ['1', '2', '1w2'] for col_ in
    ['total_upload_{}_w{}',
     'total_download_{}_w{}'
     ]]

    cols += [col_.format(cc, week) for cc in competitors for week in ['1', '2'] for col_ in [
      'Ratio_upload_{}_w{}',
      'Ratio_download_{}_w{}'
     ]]

    cols += [col_.format(week) for week in ['1', '2', '1w2'] for col_ in
     [
     'totalUploadCompetidores_w{}',
     'totalDownloadCompetidores_w{}',
     ]]

    cols += [col_.format(week) for week in ['1', '2'] for col_ in
     [
     'totalUploadOperadores_w{}',
     'totalDownloadOperadores_w{}'
     ]]

    return cols