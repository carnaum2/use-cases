import sys
from datetime import datetime as dt
import imp
import os
from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull,substring, size, length, desc)
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql.functions import avg as sql_avg

def get_partitions_path_range(path, start, end):
    """
    Returns a list of complete paths with data to read of the source between two dates
    :param path: string path
    :param start: string date start
    :param end: string date end
    :return: list of paths
    """
    from datetime import timedelta, datetime
    star_date = datetime.strptime(start, '%Y%m%d')
    delta = datetime.strptime(end, '%Y%m%d') - datetime.strptime(start, '%Y%m%d')
    days_list = [star_date + timedelta(days=i) for i in range(delta.days + 1)]
    return [path + "year={}/month={}/day={}".format(d.year, d.month, d.day) for d in days_list]

def check_hdfs_exists(path_to_file):
    try:
        cmd = 'hdfs dfs -ls {}'.format(path_to_file).split()
        files = subprocess.check_output(cmd).strip().split('\n')
        return True if files and len(files[0])>0 else False
    except:
        return False


def get_zhilabs_adsl_att(spark, starting_day, closing_day):
    from pyspark.sql.types import DoubleType, StringType, IntegerType
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_adsl = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')

    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(closing_day[:4],
                                                                                                       int(closing_day[4:6]), int(closing_day[6:8]))
    odin = spark.read.load(odin_path)

    df_adsl = df_adsl.join(odin.select('service_id', 'crmid', 'network_type'), df_adsl.serviceid == odin.service_id,
                           'inner').where((col('network_type') == 'AD') | (col('network_type') == 'AI') | (col('network_type') == 'NEBA-Cobre'))

    adsl_cols = ['cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries', 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_received', 'lan_ethernet_stats_mbytes_sent',
                 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received', 'wlan_2_4_stats_errors_sent',
                 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received', 'wlan_5_stats_errors_sent',
                 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'cpe_cpu_usage', 'cpe_memory_free',
                 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    df = df_adsl.select(['serviceid', 'network_type', 'crmid'] + adsl_cols)

    df_zhilabs_adsl = df.withColumn("wlan_2_4_errors_received_rate",
                                    col('wlan_2_4_stats_errors_received') /(col('wlan_2_4_stats_errors_received')+ col('wlan_2_4_stats_packets_received'))) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') /(col('wlan_2_4_stats_errors_sent')+col('wlan_2_4_stats_packets_sent')) )\
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') / col('wlan_5_stats_packets_received')) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_packets_sent')) \
        .withColumn("adsl_max_vel__down_alcanzable", col('adsl_max_vel__down_alcanzable').cast(IntegerType()))

    feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev
    df_zhilabs_adsl_feats = df_zhilabs_adsl.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feat_cols] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feat_cols] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                     feat_cols] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feat_cols]))

    # df_adsl_feats = df_zhilabs_adsl_feats.join(odin.select('service_id', 'crmid', 'network_type'), df_zhilabs_adsl_feats.serviceid == odin.service_id, 'inner').where((col('network_type') == 'AD')|(col('network_type') == 'AI')|(col('network_type') == 'NEBA-Cobre') )
    df_zhilabs_adsl_feats = df_zhilabs_adsl_feats.join(odin.select('service_id', 'crmid'),
                                                       df_zhilabs_adsl_feats.serviceid == odin.service_id, 'inner')

    return df_zhilabs_adsl_feats


def get_zhilabs_hfc_att(spark, starting_day, closing_day):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_hfc = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')

    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(closing_day[:4],
                                                                                                       int(closing_day[
                                                                                                           4:6]),
                                                                                                       int(closing_day[
                                                                                                           6:8]))
    odin = spark.read.load(odin_path)

    df_hfc = df_hfc.join(odin.select('service_id', 'crmid', 'network_type'), df_hfc.serviceid == odin.service_id,
                         'inner').where((col('network_type') == 'HFC'))

    df_zhilabs_thot = df_hfc.withColumn("wlan_2_4_errors_received_rate",
                                        col('wlan_2_4_stats_errors_received') / (col('wlan_2_4_stats_errors_received') + col('wlan_2_4_stats_packets_received'))) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') /(col('wlan_2_4_stats_errors_sent') + col('wlan_2_4_stats_packets_sent'))) \
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') /(col('wlan_5_stats_errors_received') + col('wlan_5_stats_packets_received'))) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_errors_sent') + col('wlan_5_stats_packets_sent'))

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
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable']
    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev
    df_zhilabs_thot_feats = df_zhilabs_thot.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feat_cols] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feat_cols] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                     feat_cols] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feat_cols]))

    df_zhilabs_thot_feats = df_zhilabs_thot_feats.join(odin.select('service_id', 'crmid'),
                                                       df_zhilabs_thot_feats.serviceid == odin.service_id, 'inner')
    return df_zhilabs_thot_feats


def get_zhilabs_ftth_att(spark, starting_day, closing_day):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_ftth = spark.read.option("basePath", zhilabs_path).load(paths).withColumnRenamed('Serviceid', 'serviceid')

    odin_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_ODIN/1.0/parquet/year={}/month={}/day={}'.format(closing_day[:4],
                                                                                                       int(closing_day[
                                                                                                           4:6]),
                                                                                                       int(closing_day[
                                                                                                           6:8]))
    odin = spark.read.load(odin_path)

    df_ftth = df_ftth.join(odin.select('service_id', 'crmid', 'network_type'), df_ftth.serviceid == odin.service_id,
                           'inner').where(
        (col('network_type') == 'FTTH') | (col('network_type') == 'NEBA-L') | (col('network_type') == 'NEBA-FTTH'))

    feats_ftth = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                  'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                  'wlan_5_configuration_autochannel_enable']
    id_ = ['serviceid']

    df_ftth = df_ftth.withColumn("wlan_2_4_errors_received_rate",
                                 col('wlan_2_4_stats_errors_received') / col('wlan_2_4_stats_packets_received')) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') / col('wlan_2_4_stats_packets_sent')) \
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') / col('wlan_5_stats_packets_received')) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_packets_sent'))
    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev, abs
    df_ftth_feats = df_ftth.groupby('serviceid').agg(*([sql_max(kpi).alias("max_" + kpi) for kpi in feats_ftth] + \
                                                       [sql_min(kpi).alias("min_" + kpi) for kpi in feats_ftth] + [
                                                           sql_avg(kpi).alias("mean_" + kpi) for kpi in feats_ftth] \
                                                       + [stddev(kpi).alias("std_" + kpi) for kpi in feats_ftth]))

    # DAILY
    zhilabs_path_daily = '/data/raw/vf_es/fixnetprobes/ZHILABS_DAILY/1.0/parquet/'
    paths_daily_ = get_partitions_path_range(zhilabs_path_daily, starting_day, closing_day)
    paths_daily = []
    for p in paths_daily_:
        if check_hdfs_exists(p) == True:
            paths_daily.append(p)

    df_ftth_daily = spark.read.option("basePath", zhilabs_path_daily).load(paths_daily)

    df_ftth_daily = df_ftth_daily.withColumn("mod_ftth_ber_down_average", abs(col('ftth_ber_down_average'))) \
        .withColumn("mod_ftth_ber_up_average", abs(col('ftth_ber_up_average')))

    feats_daily = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_ber_critical', 'issue_ftth_ber_warning',
                   'issue_ftth_degradation', 'issue_ftth_prx']

    df_ftth_feats_daily = df_ftth_daily.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feats_daily] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feats_daily] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                       feats_daily] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feats_daily]))

    df_ftth_feats_all = df_ftth_feats.join(df_ftth_feats_daily, ['serviceid'], 'left_outer')

    df_ftth_feats_all = df_ftth_feats_all.join(odin.select('service_id', 'crmid'),
                                               df_ftth_feats_all.serviceid == odin.service_id, 'inner')

    return df_ftth_feats_all


def get_hfc_population(spark, starting_day, closing_day,n_days= 30, labeled=True):
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range  # , get_zhilabs_hfc_feats
    from churn.analysis.triggers.orders.customer_master import get_segment_msisdn_anyday
    from pyspark.sql import Window
    window_nc = Window.partitionBy("NUM_CLIENTE")
    hfc_feats = get_zhilabs_hfc_att(spark, starting_day, closing_day)
    hfc_feats = hfc_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(hfc_feats.count())
    df_services = get_segment_msisdn_anyday(spark, anyday=closing_day)
    df_services = df_services.cache()
    print'Number of active services: ' + str(df_services.count())
    if labeled:
        from churn.analysis.triggers.orders.customer_master import getFbbDxsForCycleList_anyday
        from churn.models.fbb_churn_amdocs.utils_fbb_churn import getFixPortRequestsForCycleList
        from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests
        from pykhaos.utils.date_functions import move_date_n_days

        start_port = closing_day
        end_port = move_date_n_days(closing_day, n=n_days)

        df_sopo_fix = (getFixPortRequestsForCycleList(spark, start_port, end_port) \
                       .withColumn("date_srv", from_unixtime(unix_timestamp(col("FECHA_INSERCION_SGP")), "yyyyMMdd")))

        df_baja_fix = getFbbDxsForCycleList_anyday(spark, start_port, end_port)

        df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

        df_services_labeled = df_services.join(df_sopo_fix, ['msisdn'], "left").na.fill({'label_srv': 0.0}) \
            .join(df_baja_fix, ['msisdn'], "left").na.fill({'label_dx': 0.0}) \
            .join(df_sol_port, ['msisdn'], "left") \
            .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                    1.0).otherwise(0.0))
        df_services_labeled = df_services_labeled.withColumn('label', max('tmp').over(window_nc)).drop("tmp")

        df_services_labeled_join = df_services_labeled.select('NIF_CLIENTE', 'NUM_CLIENTE', 'label').distinct()

        hfc_population = hfc_feats.join(df_services_labeled_join,
                                        hfc_feats.crmid == df_services_labeled_join.NUM_CLIENTE, 'inner')
    else:
        hfc_population = hfc_feats.join(df_services, hfc_feats.crmid == df_services.NUM_CLIENTE, 'inner')

    return hfc_population


def get_ftth_population(spark, starting_day, closing_day,n_days= 30, labeled=True):
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range  # , get_zhilabs_ftth_feats
    from churn.analysis.triggers.orders.customer_master import get_segment_msisdn_anyday
    from pyspark.sql import Window
    window_nc = Window.partitionBy("NUM_CLIENTE")
    ftth_feats = get_zhilabs_ftth_att(spark, starting_day, closing_day)
    ftth_feats = ftth_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(ftth_feats.count())
    df_services = get_segment_msisdn_anyday(spark, anyday=closing_day)
    df_services = df_services.cache()
    print'Number of active services: ' + str(df_services.count())
    if labeled:
        from churn.analysis.triggers.orders.customer_master import getFbbDxsForCycleList_anyday
        from churn.models.fbb_churn_amdocs.utils_fbb_churn import getFixPortRequestsForCycleList
        from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests
        from pykhaos.utils.date_functions import move_date_n_days

        start_port = closing_day
        end_port = move_date_n_days(closing_day, n=n_days)

        df_sopo_fix = (getFixPortRequestsForCycleList(spark, start_port, end_port) \
                       .withColumn("date_srv", from_unixtime(unix_timestamp(col("FECHA_INSERCION_SGP")), "yyyyMMdd")))

        df_baja_fix = getFbbDxsForCycleList_anyday(spark, start_port, end_port)

        df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

        df_services_labeled = df_services.join(df_sopo_fix, ['msisdn'], "left").na.fill({'label_srv': 0.0}) \
            .join(df_baja_fix, ['msisdn'], "left").na.fill({'label_dx': 0.0}) \
            .join(df_sol_port, ['msisdn'], "left") \
            .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                    1.0).otherwise(0.0))
        df_services_labeled = df_services_labeled.withColumn('label', max('tmp').over(window_nc)).drop("tmp")
        df_services_labeled_join = df_services_labeled.select('NIF_CLIENTE', 'NUM_CLIENTE', 'label').distinct()

        ftth_population = ftth_feats.join(df_services_labeled_join,
                                          ftth_feats.crmid == df_services_labeled_join.NUM_CLIENTE, 'inner')
    else:
        ftth_population = ftth_feats.join(df_services, ftth_feats.crmid == df_services.NUM_CLIENTE, 'inner')

    return ftth_population


def get_adsl_population(spark, starting_day, closing_day,n_days= 30, labeled=True):
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range  # , get_zhilabs_adsl_feats
    from churn.analysis.triggers.orders.customer_master import get_segment_msisdn_anyday
    from pyspark.sql import Window
    window_nc = Window.partitionBy("NUM_CLIENTE")
    adsl_feats = get_zhilabs_adsl_att(spark, starting_day, closing_day)
    adsl_feats = adsl_feats.cache()
    print'Number of num_clientes (zhilabs): ' + str(adsl_feats.count())
    df_services = get_segment_msisdn_anyday(spark, anyday=closing_day)
    df_services = df_services.cache()
    print'Number of active services: ' + str(df_services.count())
    if labeled:
        from churn.analysis.triggers.orders.customer_master import getFbbDxsForCycleList_anyday
        from churn.models.fbb_churn_amdocs.utils_fbb_churn import getFixPortRequestsForCycleList
        from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests
        from pykhaos.utils.date_functions import move_date_n_days

        start_port = closing_day
        end_port = move_date_n_days(closing_day, n=n_days)

        df_sopo_fix = (getFixPortRequestsForCycleList(spark, start_port, end_port) \
                       .withColumn("date_srv", from_unixtime(unix_timestamp(col("FECHA_INSERCION_SGP")), "yyyyMMdd")))

        df_baja_fix = getFbbDxsForCycleList_anyday(spark, start_port, end_port)

        df_sol_port = get_mobile_portout_requests(spark, start_port, end_port)

        df_services_labeled = df_services.join(df_sopo_fix, ['msisdn'], "left").na.fill({'label_srv': 0.0}) \
            .join(df_baja_fix, ['msisdn'], "left").na.fill({'label_dx': 0.0}) \
            .join(df_sol_port, ['msisdn'], "left") \
            .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                    1.0).otherwise(0.0))
        df_services_labeled = df_services_labeled.withColumn('label', max('tmp').over(window_nc)).drop("tmp")
        df_services_labeled_join = df_services_labeled.select('NIF_CLIENTE', 'NUM_CLIENTE', 'label').distinct()

        adsl_population = adsl_feats.join(df_services_labeled_join,
                                          adsl_feats.crmid == df_services_labeled_join.NUM_CLIENTE, 'inner')
    else:
        adsl_population = adsl_feats.join(df_services, adsl_feats.crmid == df_services.NUM_CLIENTE, 'inner')

    return adsl_population