#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit, max as sql_max, sum as sql_sum
from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles

CURRENT_IDS_PATH = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/'

def get_ids_nif_feats(columns, date_ = "20190930"):
    '''
    returns all the NIF level feats on the IDS
    :param columns: numeric columns of the IDS
    '''
    import itertools
    # Cust_Agg includes both num_cliente and nif features
    if int(date_) >= 20190930:
        nif_prefix = ["Tickets_", "Reimbursement", "Pbms_srv", "NUM_SOPORTE_TECNICO", "NUM_AVERIAS", "NUM_RECLAMACIONES", "Spinners_", "Ord_sla", "CCC_"]
    else:
        nif_prefix = ["Tickets_", "Reimbursement", "Pbms_srv", "NUM_SOPORTE_TECNICO", "NUM_AVERIAS", "NUM_RECLAMACIONES", "Spinners_", "Ord_sla"]

    lists_NIF = []
    for pref in nif_prefix:
        sel_nif_cols = [f[0] for f in columns if(f[0].startswith(pref) | (f[0].startswith('Cust_Agg') & f[0].endswith("nif")))]
        lists_NIF.append(sel_nif_cols)
    merged_NIF = list(itertools.chain(*lists_NIF))
    return list(set(merged_NIF))


def get_ids_nc_feats(columns, numeric = False):
    '''
    returns all the NC level feats on the IDS
    :param columns: numeric columns of the IDS
    '''
    import itertools
    # Cust_Agg includes both num_cliente and nif features
    # Order must not be used
    # Address must not be used
    # Penal_CUST: only L2 features must be considered (identified by the prefix Penal_L2_CUST)
    # PER_PREFS must not be used
    num_cliente_prefix = ["Cust_", "Order_Agg_", "Penal_L2_CUST", "Bill_"]

    lists_nc = []
    for pref in num_cliente_prefix:
        sel_nc_cols = [f[0] for f in columns if ((f[0].startswith(pref) & ('nif' not in f[0])) | (f[0].startswith('Cust_Agg') & f[0].endswith("nc")))]
        lists_nc.append(sel_nc_cols)
    merged_nc = list(itertools.chain(*lists_nc))

    if(numeric):
        # Only numerical columns will be agregated
        columns = [c for c in columns if (c[0] in merged_nc)]
        merged_nc = [c[0] for c in columns if(c[1] in ["double", "int", "float", "long", "bigint"])]

    return merged_nc

def get_ids_msisdn_feats(columns, numeric = False):
    '''
    returns all the msisdn level feats on the IDS
    :param columns: numeric columns of the IDS
    '''
    import itertools

    #msisdn_prefix = ["Serv_","Camp_", "GNV_Type", "GNV_Voice", "GNV_Data", "GNV_Roam_Voice", "GNV_Roam_Data", "device", "TNPS_", "tgs_", "netscout","Comp_"]
    msisdn_prefix = ["GNV_Voice", "GNV_Data", "tgs_", "netscout", "Comp_", "Penal_SRV"]
    lists_msisdn = []
    for pref in msisdn_prefix:
        #sel_msisdn_cols = [f[0] for f in columns if f[0].startswith(pref) and f[0] not in merged_nif]
        sel_msisdn_cols = [f[0] for f in columns if f[0].startswith(pref)]
        lists_msisdn.append(sel_msisdn_cols)
    merged_msisdn = list(itertools.chain(*lists_msisdn))

    if (numeric):
        # Only numerical columns will be agregated
        columns = [c for c in columns if (c[0] in merged_msisdn)]
        merged_msisdn = [c[0] for c in columns if (c[1] in ["double", "int", "float", "long", "bigint"])]

    return merged_msisdn

def get_ids_nif_agg_feats_nc(df):
    nc_feats = get_ids_nc_feats(df.dtypes, True)
    df_sel = df.select(['nif_cliente'] + nc_feats).distinct()
    from pyspark.sql.functions import max as sql_max, avg as sql_avg, min as sql_min
    df_agg = df_sel.groupBy('nif_cliente').agg(*[sql_avg(col(x)).alias(x + '_agg_mean') for x in nc_feats])
    #+[sql_max(col(x)).alias(x + '_agg_max') for x in nc_feats])
    return df_agg

def get_ids_nif_agg_feats_msisdn(df, level = 'nif_cliente'):
    #msisdn mobile
    msisdn_feats = get_ids_msisdn_feats(df.dtypes, True)
    df_sel = df.filter(col("serv_rgu")=="mobile").select([level] + msisdn_feats).distinct()
    from pyspark.sql.functions import max as sql_max, avg as sql_avg, min as sql_min
    df_agg = df_sel.groupBy(level).agg(*[sql_avg(col(x)).alias(x + '_agg_mean') for x in msisdn_feats])
    #+[sql_max(col(x)).alias(x + '_agg_max') for x in msisdn_feats])
    return df_agg

def get_ids(spark, date_, filter_ = True):
    path_ids = CURRENT_IDS_PATH

    from churn_nrt.src.utils.constants import PARTITION_DATE
    partition_date = PARTITION_DATE.format(date_[:4], str(int(date_[4:6])), str(int(date_[6:8])))

    path_ids = path_ids + partition_date

    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    from pyspark.sql.functions import count
    from pyspark.sql import Window
    print(path_ids)
    if check_hdfs_exists(path_ids):
        df_ids = spark.read.load(path_ids).filter((col('Serv_RGU') != 'prepaid') & (col("Cust_CLASE_CLI_COD_CLASE_CLIENTE") == "RS") & (col("Cust_COD_ESTADO_GENERAL").isin("01", "09")) & (col("Serv_SRV_BASIC").isin("MRSUI", "MPSUI") == False)) if(filter_) else spark.read.load(path_ids)
        df_ids = df_ids.withColumn('num_reps', count("*").over(Window.partitionBy('msisdn'))).filter(col('num_reps') == 1).drop('num_reps')

    else:
        import sys
        print('IDS is not generated for date {}'.format(date_))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)

    return df_ids

def get_ids_nif(spark, date_):

    from churn_nrt.src.data_utils.ids_utils import get_ids
    df_ids = get_ids(spark, date_)

    nif_feats = ['nif_cliente'] + get_ids_nif_feats(df_ids.dtypes)

    ids_nif_agg_feats_nc_df = get_ids_nif_agg_feats_nc(df_ids)

    ids_nif_agg_feats_msisdn_df = get_ids_nif_agg_feats_msisdn(df_ids)

    # Join is inner as the size of ids_nif_agg_feats_nc_df, ids_nif_agg_feats_msisdn_df and ids with NIF feats should be the same

    ids_nif_df = df_ids\
        .select(nif_feats)\
        .distinct()

    #print "[Info] Size of ids_nif_df: " + str(ids_nif_df.count()) + " - Num NIFs: " + str(ids_nif_df.select("nif_cliente").distinct().count())

    ids_nif_df = ids_nif_df\
        .join(ids_nif_agg_feats_nc_df, ['nif_cliente'], 'inner') \
        .join(ids_nif_agg_feats_msisdn_df, ['nif_cliente'], 'inner')

    return ids_nif_df

def get_ids_nc(spark, date_):

    df_ids = get_ids(spark, date_)

    nc_feats = ['num_cliente'] + get_ids_nc_feats(df_ids.dtypes, True) +  get_ids_nif_feats(df_ids.dtypes, date_)

    ids_nc_agg_feats_msisdn_df = get_ids_nif_agg_feats_msisdn(df_ids, 'num_cliente')

    # Join is inner as the size of ids_nif_agg_feats_nc_df, ids_nif_agg_feats_msisdn_df and ids with NIF feats should be the same

    ids_nc_df = df_ids\
        .select(nc_feats)\
        .distinct()

    #print "[Info] Size of ids_nif_df: " + str(ids_nif_df.count()) + " - Num NIFs: " + str(ids_nif_df.select("nif_cliente").distinct().count())

    ids_nc_df = ids_nc_df\
        .join(ids_nc_agg_feats_msisdn_df, ['num_cliente'], 'inner') \

    return ids_nc_df

def get_ids_segments(spark, date_, col_bi = 'tgs_days_until_f_fin_bi', level='msisdn', verbose= False):

    from churn_nrt.src.data_utils.ids_utils import get_filtered_ids
    df_target_nifs = get_filtered_ids(spark, date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_cc =True, n_cycles=12, level='nif_cliente', verbose=verbose)

    from churn_nrt.src.data_utils.ids_utils import get_ids
    df_ids = get_ids(spark, date_)

    df_ids_target = df_ids.join(df_target_nifs ,['nif_cliente'], 'inner')

    if level == 'num_cliente':
        from pyspark.sql.functions import max as sql_max
        new_bi = df_ids_target.groupBy('num_cliente').agg(sql_max(col_bi).alias('agg_days_until_fin_bi'))
        df_ids_target = df_ids_target.join(new_bi, ['num_cliente'], 'inner')
        col_bi = 'agg_days_until_fin_bi'

    df_ids_target = df_ids_target.withColumn('blindaje', lit('none'))\
    .withColumn('blindaje', when((col(col_bi) >= -60) & (col(col_bi) <= 60), 'soft').otherwise(col('blindaje')))\
    .withColumn('blindaje',  when((col(col_bi) > 60), 'hard').otherwise(col('blindaje')))

    df_ids_target = df_ids_target.select('nif_cliente', 'num_cliente', 'msisdn', 'blindaje', 'serv_rgu')

    return df_ids_target

def get_filtered_ids(spark, date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_cc =True, n_cycles=12, level='nif_cliente', verbose=False):

    from churn_nrt.src.utils.date_functions import move_date_n_cycles, get_diff_days
    date_prev = move_date_n_cycles(date_, -n_cycles)
    diff_days = get_diff_days(date_prev, date_, format_date="%Y%m%d")

    PATH_IDS = CURRENT_IDS_PATH

    from churn_nrt.src.utils.constants import PARTITION_DATE

    partition_date = PARTITION_DATE.format(date_[:4], str(int(date_[4:6])), str(int(date_[6:8])))

    path_ids = PATH_IDS + partition_date

    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists

    print(path_ids)

    if check_hdfs_exists(path_ids):
        df_ids_ori = spark.read.load(path_ids)
        df_ids = df_ids_ori.select('nif_cliente', 'num_cliente').distinct()
    else:
        import sys
        print('IDS is not generated for date {}'.format(date_))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)

    if verbose:
        print('Number of distinct nif_cliente {}'.format(df_ids.count()))

    if verbose:
        print('Size target customers df: {}'.format(df_ids.count()))

    if filter_recent:
        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter
        df_ids_rec = get_non_recent_customers_filter(spark, date_, diff_days, level='nif', verbose = verbose)
        df_ids = df_ids.join(df_ids_rec.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after early churn filter: {}'.format(df_ids.count()))

    if filter_disc:
        from churn_nrt.src.data_utils.base_filters import get_disconnection_process_filter
        df_ids_disc = get_disconnection_process_filter(spark, date_, diff_days, verbose=verbose, level="nif_cliente")
        df_ids = df_ids.join(df_ids_disc.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after disconnection filter: {}'.format(df_ids.count()))

    if filter_ord:
        # Order features are obtained at num_cliente level, so a NIF aggregation is obtained
        df_ord_agg = df_ids_ori\
            .select('nif_cliente', "Ord_sla_has_forbidden_orders_last90")\
            .groupBy('nif_cliente')\
            .agg(sql_max('Ord_sla_has_forbidden_orders_last90').alias('Ord_sla_has_forbidden_orders_last90'))
        df_ids = df_ids.join(df_ord_agg, ['nif_cliente'], 'inner').where(~(col("Ord_sla_has_forbidden_orders_last90") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after forbidden orders filter: {}'.format(df_ids.count()))

    if filter_cc:
        # CCC features are obtained at msisdn level, so a NIF aggregation is obtained
        df_ccc_agg = df_ids_ori\
            .select('nif_cliente', "CCC_CHURN_CANCELLATIONS_w8")\
            .groupBy('nif_cliente')\
            .agg(sql_sum('CCC_CHURN_CANCELLATIONS_w8').alias('CCC_CHURN_CANCELLATIONS_w8'))
        df_ids = df_ids.join(df_ccc_agg, ['nif_cliente'], 'inner').where(~(col("CCC_CHURN_CANCELLATIONS_w8") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after churn cancellations filter: {}'.format(df_ids.count()))

    df_ids_final = df_ids.select(level).drop_duplicates([level])
    df_ids_final = df_ids_final.cache()
    print('Number of {}s on the final IDS target: {}'.format(level,df_ids_final.select(level).distinct().count()))

    return df_ids_final

def get_noninf_features():
    """

        : return: set of features in the IDS that should not be used as input since they do not provide relevant information;
        instead, proper aggregates on these features are included as model inputs.

        """

    non_inf_feats = (["Serv_L2_days_since_Serv_fx_data",
                      "Cust_Agg_L2_total_num_services_nc",
                      "Cust_Agg_L2_total_num_services_nif",
                      "GNV_Data_hour_0_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_1_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_2_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_3_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_4_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_4_W_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_6_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_7_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_8_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_9_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_10_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_11_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_12_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_13_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_16_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_17_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_18_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_21_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_21_W_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_22_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_0_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_L2_total_data_volume_W_0",
                      "GNV_Data_L2_total_data_volume_WE_0",
                      "GNV_Data_L2_total_data_volume_W_1",
                      "GNV_Data_L2_total_data_volume_WE_1",
                      "GNV_Data_L2_total_data_volume_W_2",
                      "GNV_Data_L2_total_data_volume_WE_2",
                      "GNV_Data_L2_total_data_volume_W_3",
                      "GNV_Data_L2_total_data_volume_WE_3",
                      "GNV_Data_L2_total_data_volume_W_5",
                      "GNV_Data_L2_total_data_volume_W_6",
                      "GNV_Data_L2_total_data_volume_WE_6",
                      "GNV_Data_L2_total_data_volume_W_13",
                      "GNV_Data_L2_total_data_volume_WE_13",
                      "GNV_Data_L2_total_data_volume_WE_14",
                      "GNV_Data_L2_total_data_volume_W_15",
                      "GNV_Data_L2_total_data_volume_WE_15",
                      "GNV_Data_L2_total_data_volume_W_16",
                      "GNV_Data_L2_total_data_volume_WE_16",
                      "GNV_Data_L2_total_data_volume_W_17",
                      "GNV_Data_L2_total_data_volume_WE_17",
                      "GNV_Data_L2_total_data_volume_W_18",
                      "GNV_Data_L2_total_data_volume_WE_18",
                      "GNV_Data_L2_total_data_volume_W_19",
                      "GNV_Data_L2_total_data_volume_WE_19",
                      "GNV_Data_L2_total_data_volume_W_20",
                      "GNV_Data_L2_total_data_volume_WE_20",
                      "GNV_Data_L2_total_data_volume_W_21",
                      "GNV_Data_L2_total_data_volume_WE_21",
                      "GNV_Data_L2_total_data_volume",
                      "GNV_Data_L2_total_connections",
                      "GNV_Data_L2_data_per_connection_W",
                      "GNV_Data_L2_data_per_connection",
                      "GNV_Data_L2_max_data_volume_W",
                      "Camp_NIFs_Delight_TEL_Target_0",
                      "Camp_NIFs_Delight_TEL_Universal_0",
                      "Camp_NIFs_Ignite_EMA_Target_0",
                      "Camp_NIFs_Ignite_SMS_Control_0",
                      "Camp_NIFs_Ignite_SMS_Target_0",
                      "Camp_NIFs_Ignite_TEL_Universal_0",
                      "Camp_NIFs_Legal_Informativa_SLS_Target_0",
                      "Camp_NIFs_Retention_HH_SAT_Target_0",
                      "Camp_NIFs_Retention_HH_TEL_Target_0",
                      "Camp_NIFs_Retention_Voice_EMA_Control_0",
                      "Camp_NIFs_Retention_Voice_EMA_Control_1",
                      "Camp_NIFs_Retention_Voice_EMA_Target_0",
                      "Camp_NIFs_Retention_Voice_EMA_Target_1",
                      "Camp_NIFs_Retention_Voice_SAT_Control_0",
                      "Camp_NIFs_Retention_Voice_SAT_Control_1",
                      "Camp_NIFs_Retention_Voice_SAT_Target_0",
                      "Camp_NIFs_Retention_Voice_SAT_Target_1",
                      "Camp_NIFs_Retention_Voice_SAT_Universal_0",
                      "Camp_NIFs_Retention_Voice_SAT_Universal_1",
                      "Camp_NIFs_Retention_Voice_SMS_Control_0",
                      "Camp_NIFs_Retention_Voice_SMS_Control_1",
                      "Camp_NIFs_Retention_Voice_SMS_Target_0",
                      "Camp_NIFs_Retention_Voice_SMS_Target_1",
                      "Camp_NIFs_Terceros_TEL_Universal_0",
                      "Camp_NIFs_Terceros_TER_Control_0",
                      "Camp_NIFs_Terceros_TER_Control_1",
                      "Camp_NIFs_Terceros_TER_Target_0",
                      "Camp_NIFs_Terceros_TER_Target_1",
                      "Camp_NIFs_Terceros_TER_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_EMA_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_EMA_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_EMA_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_EMA_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_NOT_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_1",
                      "Camp_NIFs_Up_Cross_Sell_MLT_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_MMS_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_MMS_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_MMS_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_MMS_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_SMS_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_SMS_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_SMS_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_SMS_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Control_0",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Control_1",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Target_1",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Universal_0",
                      "Camp_NIFs_Up_Cross_Sell_TEL_Universal_1",
                      "Camp_NIFs_Up_Cross_Sell_TER_Target_0",
                      "Camp_NIFs_Up_Cross_Sell_TER_Universal_0",
                      "Camp_NIFs_Welcome_EMA_Target_0",
                      "Camp_NIFs_Welcome_TEL_Target_0",
                      "Camp_NIFs_Welcome_TEL_Universal_0",
                      "Camp_SRV_Delight_NOT_Universal_0",
                      "Camp_SRV_Delight_SMS_Control_0",
                      "Camp_SRV_Delight_SMS_Universal_0",
                      "Camp_SRV_Ignite_MMS_Target_0",
                      "Camp_SRV_Ignite_NOT_Target_0",
                      "Camp_SRV_Ignite_SMS_Target_0",
                      "Camp_SRV_Ignite_SMS_Universal_0",
                      "Camp_SRV_Legal_Informativa_EMA_Target_0",
                      "Camp_SRV_Legal_Informativa_MLT_Universal_0",
                      "Camp_SRV_Legal_Informativa_MMS_Target_0",
                      "Camp_SRV_Legal_Informativa_NOT_Target_0",
                      "Camp_SRV_Legal_Informativa_SMS_Target_0",
                      "Camp_SRV_Retention_Voice_EMA_Control_0",
                      "Camp_SRV_Retention_Voice_EMA_Control_1",
                      "Camp_SRV_Retention_Voice_EMA_Target_0",
                      "Camp_SRV_Retention_Voice_EMA_Target_1",
                      "Camp_SRV_Retention_Voice_MLT_Universal_0",
                      "Camp_SRV_Retention_Voice_NOT_Control_0",
                      "Camp_SRV_Retention_Voice_NOT_Target_0",
                      "Camp_SRV_Retention_Voice_NOT_Target_1",
                      "Camp_SRV_Retention_Voice_NOT_Universal_0",
                      "Camp_SRV_Retention_Voice_SAT_Control_0",
                      "Camp_SRV_Retention_Voice_SAT_Control_1",
                      "Camp_SRV_Retention_Voice_SAT_Target_0",
                      "Camp_SRV_Retention_Voice_SAT_Target_1",
                      "Camp_SRV_Retention_Voice_SAT_Universal_0",
                      "Camp_SRV_Retention_Voice_SAT_Universal_1",
                      "Camp_SRV_Retention_Voice_SLS_Control_0",
                      "Camp_SRV_Retention_Voice_SLS_Control_1",
                      "Camp_SRV_Retention_Voice_SLS_Target_0",
                      "Camp_SRV_Retention_Voice_SLS_Target_1",
                      "Camp_SRV_Retention_Voice_SLS_Universal_0",
                      "Camp_SRV_Retention_Voice_SLS_Universal_1",
                      "Camp_SRV_Retention_Voice_SMS_Control_0",
                      "Camp_SRV_Retention_Voice_SMS_Control_1",
                      "Camp_SRV_Retention_Voice_SMS_Target_1",
                      "Camp_SRV_Retention_Voice_SMS_Universal_0",
                      "Camp_SRV_Retention_Voice_SMS_Universal_1",
                      "Camp_SRV_Retention_Voice_TEL_Control_1",
                      "Camp_SRV_Retention_Voice_TEL_Target_1",
                      "Camp_SRV_Retention_Voice_TEL_Universal_0",
                      "Camp_SRV_Retention_Voice_TEL_Universal_1",
                      "Camp_SRV_Up_Cross_Sell_EMA_Control_0",
                      "Camp_SRV_Up_Cross_Sell_EMA_Control_1",
                      "Camp_SRV_Up_Cross_Sell_EMA_Target_0",
                      "Camp_SRV_Up_Cross_Sell_EMA_Target_1",
                      "Camp_SRV_Up_Cross_Sell_EMA_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0",
                      "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_1",
                      "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0",
                      "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_1",
                      "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_1",
                      "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0",
                      "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_1",
                      "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0",
                      "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_1",
                      "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0",
                      "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_1",
                      "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0",
                      "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_1",
                      "Camp_SRV_Up_Cross_Sell_HH_SMS_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_MLT_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_MMS_Control_0",
                      "Camp_SRV_Up_Cross_Sell_MMS_Control_1",
                      "Camp_SRV_Up_Cross_Sell_MMS_Target_0",
                      "Camp_SRV_Up_Cross_Sell_MMS_Target_1",
                      "Camp_SRV_Up_Cross_Sell_NOT_Control_0",
                      "Camp_SRV_Up_Cross_Sell_NOT_Target_0",
                      "Camp_SRV_Up_Cross_Sell_NOT_Target_1",
                      "Camp_SRV_Up_Cross_Sell_NOT_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
                      "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
                      "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
                      "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
                      "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
                      "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
                      "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
                      "Camp_SRV_Up_Cross_Sell_SMS_Target_1",
                      "Camp_SRV_Up_Cross_Sell_SMS_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_SMS_Universal_1",
                      "Camp_SRV_Up_Cross_Sell_TEL_Control_0",
                      "Camp_SRV_Up_Cross_Sell_TEL_Control_1",
                      "Camp_SRV_Up_Cross_Sell_TEL_Target_0",
                      "Camp_SRV_Up_Cross_Sell_TEL_Target_1",
                      "Camp_SRV_Up_Cross_Sell_TEL_Universal_0",
                      "Camp_SRV_Up_Cross_Sell_TEL_Universal_1",
                      "Camp_SRV_Welcome_EMA_Target_0",
                      "Camp_SRV_Welcome_MMS_Target_0",
                      "Camp_SRV_Welcome_SMS_Target_0",
                      "Camp_L2_srv_total_redem_Target",
                      "Camp_L2_srv_pcg_redem_Target",
                      "Camp_L2_srv_total_redem_Control",
                      "Camp_L2_srv_total_redem_Universal",
                      "Camp_L2_srv_total_redem_EMA",
                      "Camp_L2_srv_total_redem_TEL",
                      "Camp_L2_srv_total_camps_SAT",
                      "Camp_L2_srv_total_redem_SAT",
                      "Camp_L2_srv_total_redem_SMS",
                      "Camp_L2_srv_pcg_redem_SMS",
                      "Camp_L2_srv_total_camps_MMS",
                      "Camp_L2_srv_total_redem_MMS",
                      "Camp_L2_srv_total_redem_Retention_Voice",
                      "Camp_L2_srv_total_redem_Up_Cross_Sell",
                      "Camp_L2_nif_total_redem_Target",
                      "Camp_L2_nif_pcg_redem_Target",
                      "Camp_L2_nif_total_redem_Control",
                      "Camp_L2_nif_total_redem_Universal",
                      "Camp_L2_nif_total_redem_EMA",
                      "Camp_L2_nif_total_redem_TEL",
                      "Camp_L2_nif_total_camps_SAT",
                      "Camp_L2_nif_total_redem_SAT",
                      "Camp_L2_nif_total_redem_SMS",
                      "Camp_L2_nif_pcg_redem_SMS",
                      "Camp_L2_nif_total_camps_MMS",
                      "Camp_L2_nif_total_redem_MMS",
                      "Camp_L2_nif_total_redem_Retention_Voice",
                      "Camp_L2_nif_total_redem_Up_Cross_Sell",
                      "Serv_PRICE_TARIFF",
                      "GNV_Data_hour_0_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_RegularData_Num_Of_Connections",
                      "GNV_Voice_hour_23_WE_MOU",  ### solo un mou??
                      "GNV_Data_hour_7_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_RegularData_Data_Volume_MB",
                      "GNV_Data_hour_16_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_18_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_19_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_20_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_21_W_RegularData_Num_Of_Connections",
                      "GNV_Roam_Data_L2_total_connections_W",
                      "GNV_Roam_Data_L2_total_data_volume_WE",
                      "GNV_Roam_Data_L2_total_data_volume_W",
                      "Camp_NIFs_Delight_SMS_Control_0",
                      "Camp_NIFs_Delight_SMS_Target_0",
                      "Camp_NIFs_Legal_Informativa_EMA_Target_0",
                      "Camp_SRV_Delight_EMA_Target_0",
                      "Camp_SRV_Delight_MLT_Universal_0",
                      "Camp_SRV_Delight_NOT_Target_0",
                      "Camp_SRV_Delight_TEL_Universal_0",
                      "Camp_SRV_Retention_Voice_TEL_Control_0",
                      "Camp_L2_srv_total_camps_TEL",
                      "Camp_L2_srv_total_camps_Target",
                      "Camp_L2_srv_total_camps_Up_Cross_Sell",
                      "Camp_L2_nif_total_camps_TEL",
                      "Camp_L2_nif_total_camps_Target",
                      "Camp_L2_nif_total_camps_Up_Cross_Sell",
                      "Cust_Agg_flag_prepaid_nc",
                      "tgs_ind_riesgo_mm",
                      "tgs_ind_riesgo_mv",
                      "tgs_meses_fin_dto_ok",
                      "CCC_L2_bucket_1st_interaction",
                      "tgs_tg_marta",
                      "CCC_L2_bucket_latest_interaction",
                      "tgs_ind_riesgo_o2",
                      "tgs_ind_riesgo_max",
                      "tgs_sol_24m",
                      "CCC_L2_latest_interaction",
                      "tgs_blinda_bi_pos_n12",
                      "CCC_L2_first_interaction"
                      ])
    return non_inf_feats

def get_no_input_feats():

    """
    :return: set of attributes that have been identified as invalid
    """



    return ["Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_NOMBRE", "Cust_PRIM_APELLIDO", "Cust_SEG_APELLIDO", "Cust_DIR_LINEA1", "Cust_DIR_LINEA2",
            "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2", "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4",
            "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT", "Cust_APELLIDO1_CLI_FACT",
            "Cust_APELLIDO2_CLI_FACT", "Cust_DIR_LINEA1", "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2",
            "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4", "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT",
            "Cust_APELLIDO1_CLI_FACT", "Cust_APELLIDO2_CLI_FACT", "Cust_CTA_CORREO_CONTACTO", "Cust_CTA_CORREO",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE", "CCC_L2_bucket_list",
            "CCC_L2_bucket_set", "Serv_NUM_SERIE_DECO_TV", "Order_N1_Description", "Order_N2_Description",
            "Order_N5_Description", "Order_N7_Description", "Order_N8_Description", "Order_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "device_n5_imei", "Order_N1_Id", "Order_N2_Id", "Order_N3_Id", "Order_N4_Id", "Order_N5_Id", "Order_N6_Id",
            "Order_N7_Id", "Order_N8_Id", "Order_N9_Id", "Order_N10_Id"]