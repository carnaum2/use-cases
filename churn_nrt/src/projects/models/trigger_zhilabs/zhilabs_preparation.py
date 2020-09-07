#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull, substring, size, length, desc)
from pyspark.sql.functions import *

'''
def prepare_ftth_zhilabs_daily(df_ftth):
    df_ftth = df_ftth.withColumn('mod_ftth_ber_down_average',when(col('mod_ftth_ber_down_average') < 0, None).otherwise(
    when(col('mod_ftth_ber_down_average') > 1, None).otherwise(col('mod_ftth_ber_down_average')))) \
    .withColumn('mod_ftth_ber_up_average', when(col('mod_ftth_ber_up_average') < 0, None).otherwise(
    when(col('mod_ftth_ber_up_average') > 1, None).otherwise(col('mod_ftth_ber_up_average')))) \
    .withColumn('ftth_olt_prx_average', when(col('ftth_olt_prx_average') < -35, None).otherwise(
    when(col('ftth_olt_prx_average') > 0, None).otherwise(col('ftth_olt_prx_average')))) \
    .withColumn('ftth_ont_prx_average', when(col('ftth_ont_prx_average') < -35, None).otherwise(
    when(col('ftth_ont_prx_average') > 0, None).otherwise(col('ftth_ont_prx_average'))))

    return df_ftth

'''

def prepare_ftth_zhilabs_daily(df_ftth):
    df_ftth = df_ftth.withColumn('ftth_olt_prx_average', when(col('ftth_olt_prx_average') < -35, None).otherwise(
    when(col('ftth_olt_prx_average') > 0, None).otherwise(col('ftth_olt_prx_average')))) \
    .withColumn('ftth_ont_prx_average', when(col('ftth_ont_prx_average') < -35, None).otherwise(
    when(col('ftth_ont_prx_average') > 0, None).otherwise(col('ftth_ont_prx_average'))))
    return df_ftth


def prepare_adsl_zhilabs(df_adsl):
    #TODO
    return df_adsl

def prepare_hfc_zhilabs(df_hfc):
    df_hfc = df_hfc.withColumn('hfc_prx_dowstream_average', when(col('hfc_prx_dowstream_average') < -45, None).otherwise(
        when(col('hfc_prx_dowstream_average') > 45, None).otherwise(col('hfc_prx_dowstream_average')))) \
        .withColumn('hfc_snr_downstream_average', when(col('hfc_snr_downstream_average') < 10, None).otherwise(
        when(col('hfc_snr_downstream_average') > 50, None).otherwise(col('hfc_snr_downstream_average')))) \
        .withColumn('hfc_snr_upstream_average', when(col('hfc_snr_upstream_average') < 0, None).otherwise(
        when(col('hfc_snr_upstream_average') > 50, None).otherwise(col('hfc_snr_upstream_average')))) \
        .withColumn('hfc_snr_upstream_average', when(col('hfc_snr_upstream_average') < 0, None).otherwise(
        when(col('hfc_snr_upstream_average') > 50, None).otherwise(col('hfc_snr_upstream_average'))))

    return df_hfc


def prepare_adsl_zhilabs(df_adsl):
    # TODO
    return df_adsl


def ftth_zhilabs_daily_metadata(spark, sample=None, feats_daily_=[]):
    from pyspark.sql.functions import avg as sql_avg

    feats_m = ['ftth_olt_prx_average', 'ftth_ont_prx_average']

    mean_feats = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_std_', 'zhilabs_ftth_mean_'] for f in feats_m]

    imp = []
    for f in mean_feats:
        val = sample \
            .filter((col(f).isNotNull())) \
            .select(f).distinct().select(sql_avg(f).alias(f)).rdd.map(lambda r: [r[f]]).first()[0]

        imp.append((f, val))

    feats_daily = [pre + f for pre in ['max_', 'min_', 'std_', 'mean_'] for f in feats_daily_]

    ber_daily_feats_ = ["mod_ftth_ber_down_average", "mod_ftth_ber_up_average"]

    ber_daily_feats = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_std_', 'zhilabs_ftth_mean_'] for f in ber_daily_feats_]

    daily_na_fill = {i: 0 for i in feats_daily}

    ber_daily_feats_dict = {i[0]: 0.001 for i in ber_daily_feats}

    mean_ftth_feats = {i[0]: i[1] for i in imp}

    daily_na_fill.update(mean_ftth_feats)

    daily_na_fill.update(ber_daily_feats_dict)

    feats = daily_na_fill.keys()

    na_vals = daily_na_fill.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('daily_ftth_zhilabs')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('num_client')).fillna(0.0)

    return metadata_df


def zhilabs_ftth_hourly_metadata(spark, sample=None, feats_hourly_=[]):
    from pyspark.sql.functions import avg as sql_avg

    feats_m = ["wlan_2_4_stats_packets_received",\
    "wlan_2_4_stats_packets_sent","wlan_5_stats_packets_received","wlan_5_stats_packets_sent","cpe_cpu_usage","cpe_memory_free","cpe_memory_total"]

    mean_feats = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_std_', 'zhilabs_ftth_mean_'] for f in feats_m]

    imp = []
    for f in mean_feats:
        val = sample \
            .filter((col(f).isNotNull())) \
            .select(f).distinct().select(sql_avg(f).alias(f)).rdd.map(lambda r: [r[f]]).first()[0]
        val_ = val if val != None and val != 'nan' else 0
        imp.append((f, val_))

    feats_hourly = [pre + f for pre in ['zhilabs_ftth_max_', 'zhilabs_ftth_min_', 'zhilabs_ftth_std_', 'zhilabs_ftth_mean_'] for f in feats_hourly_]

    hourly_na_fill = {i: 0 for i in feats_hourly}

    mean_ftth_feats = {i[0]: i[1] for i in imp}

    hourly_na_fill.update(mean_ftth_feats)

    feats = hourly_na_fill.keys()

    na_vals = hourly_na_fill.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('hourly_ftth_zhilabs')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('num_client'))

    return metadata_df


def hfc_zhilabs_metadata(spark, sample=None, feats_hfc_=[]):
    from pyspark.sql.functions import avg as sql_avg

    feats_m = ['lan_ethernet_stats_mbytes_received', 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_packets_received','wlan_2_4_stats_packets_sent',\
               'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average', 'cpe_cpu_usage',\
               'cpe_memory_free', 'cpe_memory_total']

    mean_feats = [pre + f for pre in ['zhilabs_hfc_max_', 'zhilabs_hfc_min_', 'zhilabs_hfc_std_', 'zhilabs_hfc_mean_'] for f in feats_m]

    imp = []
    for f in mean_feats:
        val = sample \
            .filter((col(f).isNotNull())) \
            .select(f).distinct().select(sql_avg(f).alias(f)).rdd.map(lambda r: [r[f]]).first()[0]

        imp.append((f, val))

    feats_hfc = [pre + f for pre in ['zhilabs_hfc_max_', 'zhilabs_hfc_min_', 'zhilabs_hfc_std_', 'zhilabs_hfc_mean_'] for f in feats_hfc_]

    hfc_na_fill = {i: 0 for i in feats_hfc}

    mean_hfc_feats = {i[0]: i[1] for i in imp}

    hfc_na_fill.update(mean_hfc_feats)

    feats = hfc_na_fill.keys()

    na_vals = hfc_na_fill.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('hfc_zhilabs')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('num_client')).fillna(0.0)

    return metadata_df