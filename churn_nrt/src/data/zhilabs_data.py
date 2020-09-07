#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce, concat,\
from_unixtime, lpad, unix_timestamp, max as sql_max

from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def get_last_ODIN_date(spark, date_):
    return str(spark
               .read
               .parquet('/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/')
               .withColumn("date", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
               .filter(from_unixtime(unix_timestamp(col('date'), 'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(date_), 'yyyyMMdd')))
               .select(sql_max("date").alias('last_date')).first()['last_date'])

class ZhilabsFTTHData(DataTemplate):

    CRITICAL = "critical"

    def __init__(self, spark, churn_window=30, critical="critical"):
        self.CHURN_WINDOW = churn_window
        self.CRITICAL = critical
        DataTemplate.__init__(self, spark, "ftth_data")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, preparation_ = True, force_gen= False):
        from churn_nrt.src.utils.date_functions import move_date_n_days
        print(preparation_)
        starting_day = move_date_n_days(closing_day, n=-n_days)
        print(closing_day)
        print(starting_day)
        from churn_nrt.src.data.zhilabs_data import get_zhilabs_ftth_att

        if self.CRITICAL == "critical":
            critical = True
        else:
            critical = False
        df_ftth = get_zhilabs_ftth_att(self.SPARK, starting_day, closing_day, critical=critical,preparation=preparation_)

        return df_ftth

class ZhilabsFTTHIncrementalData(DataTemplate):

    CRITICAL = "critical"

    def __init__(self, spark, churn_window=30, critical="critical"):
        self.CHURN_WINDOW = churn_window
        self.CRITICAL = critical
        DataTemplate.__init__(self, spark, "ftth_inc_data")

    def build_module(self, closing_day, save_others, select_cols=None, preparation_=True, force_gen=False):

        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_w = move_date_n_days(closing_day, n=-7)
        closing_day_2w = move_date_n_days(closing_day, n=-15)
        closing_day_4w = move_date_n_days(closing_day, n=-30)

        if self.CRITICAL == "critical":
            critical = True
        else:
            critical = False

        from churn_nrt.src.data.zhilabs_data import get_zhilabs_ftth_att
        df_ftth_w = get_zhilabs_ftth_att(self.SPARK, closing_day_w, closing_day,critical=critical, preparation=preparation_)
        df_ftth_w2w1 = get_zhilabs_ftth_att(self.SPARK, closing_day_2w, closing_day_w, critical=critical,preparation=preparation_)
        df_ftth_w4w2 = get_zhilabs_ftth_att(self.SPARK, closing_day_4w, closing_day_2w, critical=critical,preparation=preparation_)
        df_ftth_w2 = get_zhilabs_ftth_att(self.SPARK, closing_day_2w, closing_day, critical=critical,preparation=preparation_)

        base_cols = [col_ for col_ in df_ftth_w.columns if col_.startswith('zhilabs_ftth')]
        rem_cols = ['service_id', 'cpe_model', 'network_access_type', 'crmid']
        not_inc_cols = ['service_id', 'serviceid', 'cpe_model', 'network_access_type', 'crmid']

        from churn_nrt.src.utils.pyspark_utils import rename_columns_sufix

        df_ftth_w = rename_columns_sufix(df_ftth_w, "w", sep="_", nocols=not_inc_cols).drop('service_id', 'cpe_model','network_access_type','crmid')
        df_ftth_w2 = rename_columns_sufix(df_ftth_w2, "w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_ftth_w2w1 = rename_columns_sufix(df_ftth_w2w1, "w2w1", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_ftth_w4w2 = rename_columns_sufix(df_ftth_w4w2, "w4w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')

        df_ftth_w = df_ftth_w.repartition(400).cache()
        df_ftth_w2 = df_ftth_w2.repartition(400).cache()
        df_ftth_w4w2 = df_ftth_w4w2.repartition(400).cache()
        df_ftth_w2w1 = df_ftth_w2w1.repartition(400).cache()

        print'Size of df w: ' + str(df_ftth_w.count())
        print'Size of df w2: ' + str(df_ftth_w2.count())
        print'Size of df w4w2: ' + str(df_ftth_w4w2.count())
        print'Size of df w2w1: ' + str(df_ftth_w2w1.count())

        df_ftth_add = df_ftth_w.join(df_ftth_w2w1, ['serviceid'], 'left').join(df_ftth_w2, ['serviceid'], 'left') \
            .join(df_ftth_w4w2, ['serviceid'], 'left').repartition(400).cache()

        print'Size of df before computing inc feats: ' + str(df_ftth_add.count())

        for col_ in base_cols:
            df_ftth_add = df_ftth_add\
                .withColumn('inc_' + col_ + '_w1w1', col(col_ + '_w') - col(col_ + '_w2w1'))\
                .withColumn('inc_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))

        sel_cols = [f for f in df_ftth_add.columns if f.startswith('inc_')]
        df_ftth_add_sel = df_ftth_add.select(sel_cols + ['serviceid']).fillna(0.0)

        df_ftth_add_sel = df_ftth_add_sel.cache()
        print'Size of the incremental FTTH df: ' + str(df_ftth_add_sel.count())

        return df_ftth_add_sel
def get_ftth_population(spark, closing_day, n_days=30, critical=True, active=True, extra_filters=False, pre_pro=True, force_gen=False):
    from churn_nrt.src.data.customer_base import CustomerBase
    from churn_nrt.src.data.zhilabs_data import ZhilabsFTTHData
    ftth_feats = ZhilabsFTTHData(spark, critical).get_module(closing_day,  n_days=n_days, preparation_ = pre_pro, force_gen= force_gen)

    ftth_feats = ftth_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(ftth_feats.count())
    df_services = CustomerBase(spark).get_module(closing_day)
    if extra_filters:
        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, \
            get_disconnection_process_filter, \
            get_churn_call_filter, get_forbidden_orders_filter
        df_non_recent = get_non_recent_customers_filter(spark, closing_day, 90, level='nif', verbose=True, only_active=active)
        df_disc = get_disconnection_process_filter(spark, closing_day, 90, verbose=False)
        df_churn_calls = get_churn_call_filter(spark, closing_day, 56, level='nif', verbose=False)
        df_forb = get_forbidden_orders_filter(spark, closing_day, level='nif', verbose=False, only_active=active)
        valid_nifs = df_non_recent.join(df_disc, ['nif_cliente'], 'inner').join(df_churn_calls, ['nif_cliente'],
                                                                                'inner') \
            .join(df_forb, ['nif_cliente'], 'inner')
        df_services = df_services.join(valid_nifs, ['nif_cliente'], 'inner')
    if active:
        from churn_nrt.src.data_utils.base_filters import keep_active_services
        df_services = keep_active_services(df_services)
        df_services = df_services.cache()
        print'Number of active services: ' + str(df_services.count())
    else:
        df_services = df_services.cache()
        print'Number of services: ' + str(df_services.count())

    ftth_population = ftth_feats.join(df_services.select('nif_cliente', 'num_cliente'), ftth_feats.crmid == df_services.NUM_CLIENTE,'inner').drop_duplicates(subset=['num_cliente'])

    from pyspark.sql.functions import countDistinct
    cpes = ftth_population.groupBy('cpe_model').agg(countDistinct('num_cliente').alias('clientes')).where(col('clientes') > 2000).select('cpe_model')
    lista = [str(row['cpe_model']) for row in cpes.select('cpe_model').distinct().collect()]
    ftth_population = ftth_population.withColumn('cpe_model',when(col('cpe_model').isin(lista), col('cpe_model')).otherwise('Others'))

    return ftth_population


def get_zhilabs_ftth_att(spark, starting_day, closing_day, critical=False, preparation=False):
    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn_nrt.src.utils.pyspark_utils import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_ftth = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')
    from churn_nrt.src.data.zhilabs_data import get_last_ODIN_date
    closing_day_odin = get_last_ODIN_date(spark, closing_day)

    print("Last available ODIN date: %s" % closing_day_odin)
    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(
        closing_day_odin[:4],
        int(closing_day_odin[
            4:6]),
        int(closing_day_odin[
            6:8]))
    odin = spark.read.load(odin_path)

    df_ftth = df_ftth.join(odin.select('service_id', 'crmid', 'network_type', 'network_ont_model'),
                           df_ftth.serviceid == odin.service_id,
                           'inner').where(
        (col('network_type') == 'FTTH') | (col('network_type') == 'NEBA-L') | (col('network_type') == 'NEBA-FTTH'))

    feats_ftth = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    df_ftth = df_ftth.withColumn("wlan_2_4_errors_received_rate",
                                 100.0 * col('wlan_2_4_stats_errors_received') / (
                                             col('wlan_2_4_stats_errors_received') + col(
                                         'wlan_2_4_stats_packets_received'))) \
        .withColumn("wlan_2_4_errors_sent_rate", 100.0 * col('wlan_2_4_stats_errors_sent') / (
            col('wlan_2_4_stats_errors_sent') + col('wlan_2_4_stats_packets_sent'))) \
        .withColumn("wlan_5_errors_received_rate",
                    100.0 * col('wlan_5_stats_errors_received') / (
                            col('wlan_5_stats_errors_received') + col('wlan_5_stats_packets_received'))) \
        .withColumn("wlan_5_errors_sent_rate", 100.0 * col('wlan_5_stats_errors_sent') / (
            col('wlan_5_stats_errors_sent') + col('wlan_5_stats_packets_sent')))

    ################## Calculo de % de nulos ##################
    from pyspark.sql.functions import sum as sql_sum, avg as sql_avg, max as sql_max, min as sql_min, stddev
    from pyspark.sql.functions import countDistinct
    nulls_att = df_ftth.groupBy('serviceid').agg(
        *([sql_sum(when(col(f).isNull(), 1.0).otherwise(0.0)).alias("nulls_" + f) for f in feats_ftth] + [
            countDistinct('Timestamp').alias("num_meas_id")]))

    nulls_att_ = nulls_att

    nulls_att_ = nulls_att_.cache()
    # print'Number of serviceid to discard atts: ' + str(nulls_att_.count())

    for col_ in nulls_att.columns[1:]:
        nulls_att_ = nulls_att_.withColumn('ratio_' + col_, 100.0 * col(col_) / col('num_meas_id'))

    df_ftth_nulls = df_ftth.join(nulls_att_, ['serviceid'], 'left')

    ################## Calculo atributos para clientes válidos ##################
    th = 30
    df_ftth_feats = df_ftth_nulls.groupby('serviceid').agg(
        *([sql_max(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_max_" + kpi) for kpi in
           feats_ftth] + \
          [sql_min(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_min_" + kpi) for kpi in
           feats_ftth] + \
          [sql_avg(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_mean_" + kpi) for kpi in
           feats_ftth] + \
          [stddev(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_std_" + kpi) for kpi in
           feats_ftth]))
    from pyspark.sql.functions import count as sql_count
    df_ftth_feats_ = df_ftth_nulls.groupby('serviceid', 'day').agg(
        sql_sum(when(col('ratio_nulls_cpe_quick_restarts') < th, col('cpe_quick_restarts'))).alias('num_restarts_day'))
    df_ftth_feats_ = df_ftth_feats_.groupby('serviceid').agg(
        sql_max(col('num_restarts_day')).alias('zhilabs_ftth_max_num_restarts_day'),
        sql_avg(col('num_restarts_day')).alias('zhilabs_ftth_mean_num_restarts_day'),
        sql_count(when(col('num_restarts_day') > 0, col('num_restarts_day'))).alias(
            'zhilabs_ftth_days_with_quick_restarts'))

    df_ftth_feats = df_ftth_feats.join(df_ftth_feats_, ['serviceid'], "inner").fillna(
        {"zhilabs_ftth_max_num_restarts_day": 0.0}).fillna({"zhilabs_ftth_mean_num_restarts_day": 0.0})

    from churn_nrt.src.projects.models.trigger_zhilabs.zhilabs_preparation import zhilabs_ftth_hourly_metadata
    df_ftth_feats = df_ftth_feats.cache()
    print'Size of hourly ftth df: ' + str(df_ftth_feats.count())
    ftth_hourly_meta = zhilabs_ftth_hourly_metadata(spark, sample=df_ftth_feats, feats_hourly_=feats_ftth)

    hourly_map_tmp = ftth_hourly_meta.select("feature", "imp_value", "type").rdd.map(
        lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    hourly_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in hourly_map_tmp if
                       x[0] in df_ftth_feats.columns])
    df_ftth_feats = df_ftth_feats

    # DAILY

    zhilabs_path_daily = '/data/raw/vf_es/fixnetprobes/ZHILABS_DAILY/1.0/parquet/'
    paths_daily_ = get_partitions_path_range(zhilabs_path_daily, starting_day, closing_day)
    paths_daily = []
    for p in paths_daily_:
        if check_hdfs_exists(p) == True:
            paths_daily.append(p)

    df_ftth_daily = spark.read.option("basePath", zhilabs_path_daily).load(paths_daily)
    from pyspark.sql.functions import abs
    df_ftth_daily = df_ftth_daily.withColumn("mod_ftth_ber_down_average", abs(col('ftth_ber_down_average'))) \
        .withColumn("mod_ftth_ber_up_average", abs(col('ftth_ber_up_average')))

    feats_daily = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_degradation', 'issue_ftth_prx']

    if preparation:
        from churn_nrt.src.projects.models.trigger_zhilabs.zhilabs_preparation import prepare_ftth_zhilabs_daily, \
            ftth_zhilabs_daily_metadata
        df_ftth_daily = prepare_ftth_zhilabs_daily(df_ftth_daily)

    ################## Calculo de % de nulos ##################
    from pyspark.sql.functions import sum as sql_sum, avg as sql_avg, max as sql_max, min as sql_min, stddev, \
        countDistinct

    nulls_att_daily = df_ftth_daily.groupBy('serviceid').agg(
        *([sql_sum(when(col(f).isNull(), 1.0).otherwise(0.0)).alias("nulls_" + f) for f in feats_daily] + [
            countDistinct('Timestamp').alias("num_meas_id")]))

    nulls_att_daily_ = nulls_att_daily

    nulls_att_daily_ = nulls_att_daily_.cache()
    # print'Number of serviceid to discard atts (daily): ' + str(nulls_att_daily_.count())

    for col_ in nulls_att_daily.columns[1:]:
        nulls_att_daily_ = nulls_att_daily_.withColumn('ratio_' + col_, 100.0 * col(col_) / col('num_meas_id'))

    df_ftth_daily_nulls = df_ftth_daily.join(nulls_att_daily_.drop('num_meas_id'), ['serviceid'], 'left')

    ################## Calculo atributos para clientes válidos ##################
    th = 30

    df_ftth_daily_nulls_ = df_ftth_daily_nulls.withColumn("flag_ber_down_warning",when(col("mod_ftth_ber_down_average") > 100,
    1.0).otherwise(0.0)).withColumn("flag_ber_up_warning", when(col("mod_ftth_ber_up_average") > 100, 1.0).otherwise(0.0))\
    .withColumn("flag_ber_down_critical",when(col("mod_ftth_ber_down_average") > 1000,1.0).otherwise(0.0)) \
    .withColumn("flag_ber_up_critical", when(col("mod_ftth_ber_up_average") > 1000, 1.0).otherwise(0.0))


    df_ftth_feats_daily_ = df_ftth_daily_nulls_.groupby('serviceid').agg(
        *([sql_sum(col(kpi)).alias(
            "zhilabs_ftth_count_" + kpi) for kpi in ["flag_ber_down_warning", "flag_ber_up_warning", "flag_ber_down_critical", "flag_ber_up_critical"]]))

    df_ftth_feats_daily = df_ftth_daily_nulls.groupby('serviceid').agg(
        *([sql_max(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_max_" + kpi) for kpi in
           feats_daily] + \
          [sql_min(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_min_" + kpi) for kpi in
           feats_daily] + \
          [sql_avg(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_mean_" + kpi) for kpi in
           feats_daily] + \
          [stddev(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_ftth_std_" + kpi) for kpi in
           feats_daily]))

    df_ftth_feats_daily = df_ftth_feats_daily.join(df_ftth_feats_daily_, ['serviceid'], "inner")

    df_ftth_feats_daily = df_ftth_feats_daily.cache()
    print'Size of daily ftth df: ' + str(df_ftth_feats_daily.count())

    ftth_daily_meta = ftth_zhilabs_daily_metadata(spark, sample=df_ftth_feats_daily, feats_daily_=feats_daily)
    daily_map_tmp = ftth_daily_meta.select("feature", "imp_value", "type").rdd.map(
        lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
    daily_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in daily_map_tmp if
                      x[0] in df_ftth_feats_daily.columns])
    df_ftth_feats_daily = df_ftth_feats_daily

    df_ftth_feats_all = df_ftth_feats.join(df_ftth_feats_daily, ['serviceid'], 'left_outer').fillna(daily_map).fillna(
        hourly_map).fillna(0.0)
    # df_ftth_feats_all = df_ftth_feats.fillna(hourly_map)

    df_ftth_feats_all = df_ftth_feats_all.join(
        odin.select('service_id', 'crmid', 'cpe_model', 'network_access_type', 'network_ont_model', 'network_type',
                    'network_sharing'), \
        df_ftth_feats_all.serviceid == odin.service_id, 'inner').fillna(
        {'cpe_model': 'unknown', 'network_access_type': 'unknown', 'network_ont_model': 'unknown',
         'network_sharing': 'unknown'})

    from pyspark.sql.functions import countDistinct
    onts = df_ftth_feats_all.groupBy('network_ont_model').agg(countDistinct('crmid').alias('clientes')).where(
        col('clientes') > 2000).select('network_ont_model')
    lista = [str(row['network_ont_model']) for row in onts.select('network_ont_model').distinct().collect()]
    df_ftth_feats_all = df_ftth_feats_all.withColumn('network_ont_model', when(col('network_ont_model').isin(lista),
                                                                               col('network_ont_model')).otherwise(
        'Others'))

    return df_ftth_feats_all

class ZhilabsHFCData(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "hfc_data")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, preparation_ = True, force_gen= False):
        from churn_nrt.src.utils.date_functions import move_date_n_days
        starting_day = move_date_n_days(closing_day, n=-n_days)
        from churn_nrt.src.data.zhilabs_data import get_zhilabs_hfc_att
        df_hfc = get_zhilabs_hfc_att(self.SPARK, starting_day, closing_day, preparation=preparation_)


        return df_hfc
class ZhilabsHFCIncrementalData(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "hfc_inc_data")

    def build_module(self, closing_day, save_others, select_cols=None, preparation_=True, force_gen=False):

        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_w = move_date_n_days(closing_day, n=-7)
        closing_day_2w = move_date_n_days(closing_day, n=-15)
        closing_day_4w = move_date_n_days(closing_day, n=-30)

        from churn_nrt.src.data.zhilabs_data import get_zhilabs_hfc_att
        df_hfc_w = get_zhilabs_hfc_att(self.SPARK, closing_day_w, closing_day, preparation=preparation_)
        df_hfc_w2w1 = get_zhilabs_hfc_att(self.SPARK, closing_day_2w, closing_day_w, preparation=preparation_)
        df_hfc_w4w2 = get_zhilabs_hfc_att(self.SPARK, closing_day_4w, closing_day_2w, preparation=preparation_)
        df_hfc_w2 = get_zhilabs_hfc_att(self.SPARK, closing_day_2w, closing_day, preparation=preparation_)

        base_cols = [col_ for col_ in df_hfc_w.columns if col_.startswith('zhilabs_hfc')]
        not_inc_cols = ['service_id', 'serviceid', 'cpe_model', 'network_access_type', 'crmid']

        from churn_nrt.src.utils.pyspark_utils import rename_columns_sufix

        df_hfc_w = rename_columns_sufix(df_hfc_w, "w", sep="_", nocols=not_inc_cols).drop('service_id', 'cpe_model','network_access_type','crmid')
        df_hfc_w2 = rename_columns_sufix(df_hfc_w2, "w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_hfc_w2w1 = rename_columns_sufix(df_hfc_w2w1, "w2w1", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_hfc_w4w2 = rename_columns_sufix(df_hfc_w4w2, "w4w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        '''
        df_hfc_w = df_hfc_w.repartition(400).cache()
        df_hfc_w2 = df_hfc_w2.repartition(400).cache()
        df_hfc_w4w2 = df_hfc_w4w2.repartition(400).cache()
        df_hfc_w2w1 = df_hfc_w2w1.repartition(400).cache()

        print'Size of df w: ' + str(df_hfc_w.count())
        print'Size of df w2: ' + str(df_hfc_w2.count())
        print'Size of df w4w2: ' + str(df_hfc_w4w2.count())
        print'Size of df w2w1: ' + str(df_hfc_w2w1.count())
        '''
        df_hfc_add = df_hfc_w.join(df_hfc_w2w1, ['serviceid'], 'left').join(df_hfc_w2, ['serviceid'], 'left') \
            .join(df_hfc_w4w2, ['serviceid'], 'left').repartition(400).cache()

        print'Size of df before computing inc feats: ' + str(df_hfc_add.count())

        for col_ in base_cols:
            df_hfc_add = df_hfc_add\
                .withColumn('inc_' + col_ + '_w1w1', col(col_ + '_w') - col(col_ + '_w2w1'))\
                .withColumn('inc_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))

        sel_cols = [f for f in df_hfc_add.columns if f.startswith('inc_')]
        df_hfc_add_sel = df_hfc_add.select(sel_cols + ['serviceid']).fillna(0.0)

        df_hfc_add_sel = df_hfc_add_sel.cache()
        print'Size of the incremental FTTH df: ' + str(df_hfc_add_sel.count())

        return df_hfc_add_sel
def get_hfc_population(spark, closing_day, n_days=30, active=True, extra_filters=False, pre_pro=True, force_gen=False):
    from churn_nrt.src.data.customer_base import CustomerBase
    from churn_nrt.src.data.zhilabs_data import ZhilabsHFCData
    hfc_feats = ZhilabsHFCData(spark).get_module(closing_day, n_days=n_days, preparation_ = pre_pro, force_gen= force_gen)
    hfc_feats = hfc_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(hfc_feats.count())
    df_services = CustomerBase(spark).get_module(closing_day)
    if extra_filters:
        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, \
            get_disconnection_process_filter, \
            get_churn_call_filter, get_forbidden_orders_filter
        df_non_recent = get_non_recent_customers_filter(spark, closing_day, 90, level='nif', verbose=True,
                                                        only_active=active)
        df_disc = get_disconnection_process_filter(spark, closing_day, 90, verbose=False)
        df_churn_calls = get_churn_call_filter(spark, closing_day, 56, level='nif', verbose=False)
        df_forb = get_forbidden_orders_filter(spark, closing_day, level='nif', verbose=False, only_active=active)
        valid_nifs = df_non_recent.join(df_disc, ['nif_cliente'], 'inner').join(df_churn_calls, ['nif_cliente'],'inner') \
            .join(df_forb, ['nif_cliente'], 'inner')
        df_services = df_services.join(valid_nifs, ['nif_cliente'], 'inner')
    if active:
        from churn_nrt.src.data_utils.base_filters import keep_active_services
        df_services = keep_active_services(df_services)
        df_services = df_services.cache()
        print'Number of active services: ' + str(df_services.count())
    else:
        df_services = df_services.cache()
        print'Number of services: ' + str(df_services.count())

    hfc_population = hfc_feats.join(df_services.select('nif_cliente', 'num_cliente'), hfc_feats.crmid == df_services.NUM_CLIENTE, 'inner').drop_duplicates( subset=['num_cliente'])
    from pyspark.sql.functions import countDistinct
    cpes = hfc_population.groupBy('cpe_model').agg(countDistinct('num_cliente').alias('clientes')).where(col('clientes') > 2000).select('cpe_model')
    lista = [str(row['cpe_model']) for row in cpes.select('cpe_model').distinct().collect()]
    hfc_population = hfc_population.withColumn('cpe_model', when(col('cpe_model').isin(lista), col('cpe_model')).otherwise('Others'))

    return hfc_population
def get_zhilabs_hfc_att(spark, starting_day, closing_day, preparation=False):
    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn_nrt.src.utils.pyspark_utils import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_hfc = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')

    closing_day_odin = get_last_ODIN_date(spark, closing_day)

    print("Last available ODIN date: %s"%closing_day_odin)

    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(closing_day_odin[:4],
                                                                                                       int(closing_day_odin[
                                                                                                           4:6]), int(
            closing_day[6:8]))

    odin = spark.read.load(odin_path)

    df_hfc = df_hfc.join(odin.select('service_id', 'crmid', 'network_type'), df_hfc.serviceid == odin.service_id,
                         'inner').where((col('network_type') == 'HFC'))

    df_zhilabs_thot = df_hfc.withColumn("wlan_2_4_errors_received_rate",
                                        100.0*col('wlan_2_4_stats_errors_received') / (
                                                    col('wlan_2_4_stats_errors_received') + col(
                                                'wlan_2_4_stats_packets_received'))) \
        .withColumn("wlan_2_4_errors_sent_rate", 100.0*col('wlan_2_4_stats_errors_sent') / (
                col('wlan_2_4_stats_errors_sent') + col('wlan_2_4_stats_packets_sent'))) \
        .withColumn("wlan_5_errors_received_rate",
                    100.0*col('wlan_5_stats_errors_received') / (
                                col('wlan_5_stats_errors_received') + col('wlan_5_stats_packets_received'))) \
        .withColumn("wlan_5_errors_sent_rate", 100.0*col('wlan_5_stats_errors_sent') / (
                col('wlan_5_stats_errors_sent') + col('wlan_5_stats_packets_sent')))

    feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'issue_hfc_docsis_3_1_equivalent_modulation_critical_current',
                 'issue_hfc_fec_upstream_critical_current', 'issue_hfc_fec_upstream_warning_current',
                 'issue_hfc_flaps_critical_current', 'issue_hfc_flaps_warning_current',
                 'issue_hfc_prx_downstream_critical_current', 'issue_hfc_prx_downstream_warning_current',
                 'issue_hfc_ptx_upstream_critical_current', 'issue_hfc_ptx_upstream_warning_current',
                 'issue_hfc_snr_downstream_critical_current', 'issue_hfc_snr_downstream_warning_current',
                 'issue_hfc_snr_upstream_critical_current', 'issue_hfc_snr_upstream_warning_current',
                 'issue_hfc_status_critical_current', 'issue_hfc_status_warning_current',
                 'hfc_percent_words_uncorrected_downstream', 'hfc_percent_words_uncorrected_upstream',
                 'hfc_prx_dowstream_average', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average',
                 'hfc_number_of_flaps_current', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

    ################## Imputar valores anomalos a nulos ##################
    if preparation:
        from churn_nrt.src.projects.models.trigger_zhilabs.zhilabs_preparation import prepare_hfc_zhilabs, hfc_zhilabs_metadata
        df_zhilabs_thot = prepare_hfc_zhilabs(df_zhilabs_thot)

    ################## Calculo de % de nulos ##################
    from pyspark.sql.functions import sum as sql_sum, avg as sql_avg, max as sql_max, min as sql_min, stddev, countDistinct

    nulls_att = df_zhilabs_thot.groupBy('serviceid').agg(*([sql_sum(when(col(f).isNull(), 1.0).otherwise(0.0)).alias("nulls_" + f) for f in feat_cols] + \
    [countDistinct('Timestamp').alias("num_meas_id")]))

    nulls_att_ = nulls_att

    nulls_att_ = nulls_att_.cache()
    #print'Number of serviceid to discard atts: ' + str(nulls_att_.count())

    for col_ in nulls_att.columns[1:]:
        nulls_att_ = nulls_att_.withColumn('ratio_' + col_, 100.0 * col(col_) / col('num_meas_id'))

    df_zhilabs_thot_nulls = df_zhilabs_thot.join(nulls_att_, ['serviceid'], 'left')

    ################## Calculo atributos para clientes válidos ##################
    th = 30
    df_zhilabs_thot_feats = df_zhilabs_thot_nulls.groupby('serviceid').agg(
        *([sql_max(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_hfc_max_" + kpi) for kpi in feat_cols] + \
          [sql_min(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_hfc_min_" + kpi) for kpi in feat_cols] + \
          [sql_avg(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_hfc_mean_" + kpi) for kpi in feat_cols] + \
          [stddev(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_hfc_std_" + kpi) for kpi in feat_cols]))

    df_zhilabs_thot_feats = df_zhilabs_thot_feats.cache()
    print'Size of hfc df: ' + str(df_zhilabs_thot_feats.count())

    if preparation:
        hfc_meta = hfc_zhilabs_metadata(spark, sample=df_zhilabs_thot_feats, feats_hfc_=feat_cols)
        hfc_map_tmp = hfc_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
        hfc_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in hfc_map_tmp if x[0] in df_zhilabs_thot_feats.columns])
        print'Filling nulls'
        df_zhilabs_thot_feats = df_zhilabs_thot_feats.fillna(hfc_map)


    lista_hfc = [str(row['hfc_status']) for row in df_hfc.select('hfc_status').distinct().collect()]

    stats_cods = df_hfc.groupby('serviceid').agg(*([countDistinct(when(col("hfc_status") == cod, col('Timestamp'))) \
                                                        .alias('num_meas_' + cod.replace('(', '_').replace(')', '_').lower()) for cod in
                                                    lista_hfc])).fillna(0.0)

    df_zhilabs_thot_feats = df_zhilabs_thot_feats.join(stats_cods, ['serviceid'], 'left')

    df_zhilabs_thot_feats = df_zhilabs_thot_feats.join(
        odin.select('service_id', 'crmid', 'cpe_model', 'network_access_type', 'network_type'), \
        df_zhilabs_thot_feats.serviceid == odin.service_id, 'inner').fillna({'cpe_model': 'unknown', 'network_access_type': 'unknown', 'network_type':'unknown'})

    return df_zhilabs_thot_feats

class ZhilabsDSLData(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "dsl_data")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, preparation_ = True, force_gen= False):

        from churn_nrt.src.utils.date_functions import move_date_n_days
        starting_day = move_date_n_days(closing_day, n=-n_days)
        from churn_nrt.src.data.zhilabs_data import get_zhilabs_adsl_att
        df_adsl = get_zhilabs_adsl_att(self.SPARK, starting_day, closing_day, preparation=preparation_)


        return df_adsl
class ZhilabsDSLIncrementalData(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "hfc_inc_data")

    def build_module(self, closing_day, save_others, select_cols=None, preparation_=True, force_gen=False):

        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_w = move_date_n_days(closing_day, n=-7)
        closing_day_2w = move_date_n_days(closing_day, n=-15)
        closing_day_4w = move_date_n_days(closing_day, n=-30)

        from churn_nrt.src.data.zhilabs_data import get_zhilabs_adsl_att
        df_dsl_w = get_zhilabs_adsl_att(self.SPARK, closing_day_w, closing_day, preparation=preparation_)
        df_dsl_w2w1 = get_zhilabs_adsl_att(self.SPARK, closing_day_2w, closing_day_w, preparation=preparation_)
        df_dsl_w4w2 = get_zhilabs_adsl_att(self.SPARK, closing_day_4w, closing_day_2w, preparation=preparation_)
        df_dsl_w2 = get_zhilabs_adsl_att(self.SPARK, closing_day_2w, closing_day, preparation=preparation_)

        base_cols = [col_ for col_ in df_dsl_w.columns if col_.startswith('zhilabs_dsl')]
        rem_cols = ['service_id', 'cpe_model', 'network_access_type', 'crmid']
        not_inc_cols = ['service_id', 'serviceid', 'cpe_model', 'network_access_type', 'crmid']

        from churn_nrt.src.utils.pyspark_utils import rename_columns_sufix

        df_dsl_w = rename_columns_sufix(df_dsl_w, "w", sep="_", nocols=not_inc_cols).drop('service_id', 'cpe_model','network_access_type','crmid')
        df_dsl_w2 = rename_columns_sufix(df_dsl_w2, "w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_dsl_w2w1 = rename_columns_sufix(df_dsl_w2w1, "w2w1", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')
        df_dsl_w4w2 = rename_columns_sufix(df_dsl_w4w2, "w4w2", sep="_", nocols=not_inc_cols).drop('service_id','cpe_model','network_access_type','crmid')

        df_dsl_w = df_dsl_w.repartition(400).cache()
        df_dsl_w2 = df_dsl_w2.repartition(400).cache()
        df_dsl_w4w2 = df_dsl_w4w2.repartition(400).cache()
        df_dsl_w2w1 = df_dsl_w2w1.repartition(400).cache()

        print'Size of df w: ' + str(df_dsl_w.count())
        print'Size of df w2: ' + str(df_dsl_w2.count())
        print'Size of df w4w2: ' + str(df_dsl_w4w2.count())
        print'Size of df w2w1: ' + str(df_dsl_w2w1.count())

        df_dsl_add = df_dsl_w.join(df_dsl_w2w1, ['serviceid'], 'left').join(df_dsl_w2, ['serviceid'], 'left') \
            .join(df_dsl_w4w2, ['serviceid'], 'left').repartition(400).cache()

        print'Size of df before computing inc feats: ' + str(df_dsl_add.count())

        for col_ in base_cols:
            df_dsl_add = df_dsl_add\
                .withColumn('inc_' + col_ + '_w1w1', col(col_ + '_w') - col(col_ + '_w2w1'))\
                .withColumn('inc_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))

        sel_cols = [f for f in df_dsl_add.columns if f.startswith('inc_')]
        df_dsl_add_sel = df_dsl_add.select(sel_cols + ['serviceid']).fillna(0.0)

        df_dsl_add_sel = df_dsl_add_sel.cache()
        print'Size of the incremental FTTH df: ' + str(df_dsl_add_sel.count())

        return df_dsl_add_sel
def get_adsl_population(spark, closing_day, n_days=30, active=True, extra_filters=False, pre_pro=True, force_gen=False):
    from churn_nrt.src.data.customer_base import CustomerBase
    from churn_nrt.src.data.zhilabs_data import ZhilabsDSLData
    adsl_feats = ZhilabsDSLData(spark).get_module(closing_day, n_days=n_days, preparation_ = pre_pro, force_gen= force_gen)
    adsl_feats = adsl_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(adsl_feats.count())
    df_services = CustomerBase(spark).get_module(closing_day)
    if extra_filters:
        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, \
            get_disconnection_process_filter, \
            get_churn_call_filter, get_forbidden_orders_filter
        df_non_recent = get_non_recent_customers_filter(spark, closing_day, 90, level='nif', verbose=True,
                                                        only_active=active)
        df_disc = get_disconnection_process_filter(spark, closing_day, 90, verbose=False)
        df_churn_calls = get_churn_call_filter(spark, closing_day, 56, level='nif', verbose=False)
        df_forb = get_forbidden_orders_filter(spark, closing_day, level='nif', verbose=False, only_active=active)
        valid_nifs = df_non_recent.join(df_disc, ['nif_cliente'], 'inner').join(df_churn_calls, ['nif_cliente'],
                                                                                'inner') \
            .join(df_forb, ['nif_cliente'], 'inner')
        df_services = df_services.join(valid_nifs, ['nif_cliente'], 'inner')
    if active:
        from churn_nrt.src.data_utils.base_filters import keep_active_services
        df_services = keep_active_services(df_services)
        df_services = df_services.cache()
        print'Number of active services: ' + str(df_services.count())
    else:
        df_services = df_services.cache()
        print'Number of services: ' + str(df_services.count())

    adsl_population = adsl_feats.join(df_services.select('nif_cliente', 'num_cliente'), adsl_feats.crmid == df_services.NUM_CLIENTE,'inner').drop_duplicates(subset=['num_cliente'])

    from pyspark.sql.functions import countDistinct
    cpes = adsl_population.groupBy('cpe_model').agg(countDistinct('num_cliente').alias('clientes')).where(col('clientes') > 2000).select('cpe_model')
    lista = [str(row['cpe_model']) for row in cpes.select('cpe_model').distinct().collect()]
    adsl_population = adsl_population.withColumn('cpe_model',when(col('cpe_model').isin(lista), col('cpe_model')).otherwise('Others'))

    return adsl_population
def get_zhilabs_adsl_att(spark, starting_day, closing_day, preparation=False):

    from pyspark.sql.types import DoubleType, StringType, IntegerType
    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn_nrt.src.utils.pyspark_utils import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_adsl = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')

    closing_day_odin = get_last_ODIN_date(spark, closing_day)

    print("Last available ODIN date: %s"%closing_day_odin)

    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(closing_day_odin[:4],
                                                                                                       int(closing_day_odin[
                                                                                                           4:6]), int(
            closing_day[6:8]))
    odin = spark.read.load(odin_path)

    df_adsl = df_adsl.join(odin.select('service_id', 'crmid', 'network_type'), df_adsl.serviceid == odin.service_id,
                           'inner').where(
        (col('network_type') == 'AD') | (col('network_type') == 'AI') | (col('network_type') == 'NEBA-Cobre'))

    adsl_cols = ['cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries', 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_received', 'lan_ethernet_stats_mbytes_sent',
                 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received', 'wlan_2_4_stats_errors_sent',
                 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received', 'wlan_5_stats_errors_sent',
                 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'cpe_cpu_usage', 'cpe_memory_free',
                 'cpe_memory_total', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual', 'Timestamp']

    df = df_adsl.select(['serviceid', 'network_type', 'crmid'] + adsl_cols)

    df_zhilabs_adsl = df.withColumn("wlan_2_4_errors_received_rate",
                                    100.0*col('wlan_2_4_stats_errors_received') /(col('wlan_2_4_stats_errors_received') + col('wlan_2_4_stats_packets_received'))) \
        .withColumn("wlan_2_4_errors_sent_rate", 100.0*col('wlan_2_4_stats_errors_sent') /(col('wlan_2_4_stats_errors_sent') + col('wlan_2_4_stats_packets_sent'))) \
        .withColumn("wlan_5_errors_received_rate",
                    100.0*col('wlan_5_stats_errors_received') /( col('wlan_5_stats_errors_received') + col('wlan_5_stats_packets_received'))) \
        .withColumn("wlan_5_errors_sent_rate", 100.0*col('wlan_5_stats_errors_sent') /(col('wlan_5_stats_errors_sent') + col('wlan_5_stats_packets_sent'))) \
        .withColumn("adsl_max_vel__down_alcanzable", col('adsl_max_vel__down_alcanzable').cast(IntegerType()))

    if preparation:
        from churn_nrt.src.projects.models.trigger_zhilabs.zhilabs_preparation import prepare_adsl_zhilabs
        df_zhilabs_adsl = prepare_adsl_zhilabs(df_zhilabs_adsl)

    feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'adsl_cortes_adsl_1_hora',
                 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    ################## Calculo de % de nulos ##################
    from pyspark.sql.functions import sum as sql_sum, avg as sql_avg, max as sql_max, min as sql_min, stddev, countDistinct

    nulls_att = df_zhilabs_adsl.groupBy('serviceid').agg(*([sql_sum(when(col(f).isNull(), 1.0).otherwise(0.0)).alias("nulls_" + f) for f in feat_cols] + \
    [countDistinct('Timestamp').alias("num_meas_id")]))

    nulls_att_ = nulls_att

    nulls_att_ = nulls_att_.cache()
    #print'Number of serviceid to discard atts: ' + str(nulls_att_.count())

    for col_ in nulls_att.columns[1:]:
        nulls_att_ = nulls_att_.withColumn('ratio_' + col_, 100.0 * col(col_) / col('num_meas_id'))

    df_zhilabs_adsl_nulls = df_zhilabs_adsl.join(nulls_att_, ['serviceid'], 'left')

    ################## Calculo atributos para clientes válidos ##################

    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev
    th = 30
    df_zhilabs_adsl_nulls_feats = df_zhilabs_adsl_nulls.groupby('serviceid').agg(
        *([sql_max(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_dsl_max_" + kpi) for kpi in feat_cols] + \
          [sql_min(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_dsl_min_" + kpi) for kpi in feat_cols] + \
          [sql_avg(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_dsl_mean_" + kpi) for kpi in feat_cols] + \
          [stddev(when(col('ratio_nulls_' + kpi) < th, col(kpi))).alias("zhilabs_dsl_std_" + kpi) for kpi in feat_cols]))

    df_zhilabs_adsl_nulls_feats = df_zhilabs_adsl_nulls_feats.cache()
    print'Size of adsl df: ' + str(df_zhilabs_adsl_nulls_feats.count())

    #df_adsl_feats = df_zhilabs_adsl_feats.join(odin.select('service_id', 'crmid', 'network_type'), df_zhilabs_adsl_feats.serviceid == odin.service_id, 'inner').where((col('network_type') == 'AD')|(col('network_type') == 'AI')|(col('network_type') == 'NEBA-Cobre') )
    df_zhilabs_adsl_feats = df_zhilabs_adsl_nulls_feats.join(odin.select('service_id', 'crmid','network_access_type', 'cpe_model'), df_zhilabs_adsl_nulls_feats.serviceid == odin.service_id, 'inner')

    return df_zhilabs_adsl_feats


