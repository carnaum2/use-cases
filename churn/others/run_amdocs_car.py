#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import time
from pyspark.sql.functions import  (sum as sql_sum, countDistinct, trim
                                    ,max , split,desc, col, current_date
                                    , datediff, lit, translate, udf
                                    , when, concat_ws, concat, decode, length
                                    , substring, to_date, regexp_replace, lpad
                                    , hour, date_format, count as sql_count
                                   , expr, coalesce, udf, lower as sql_lower)

import pyspark.sql.functions as func
from pyspark.sql import Row#
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType
import datetime, time
import pyspark.mllib.stat
import datetime, time

#from engine.amdocs_ids_config import *


def set_paths_and_logger():
    '''
    :return:
    '''
    import sys, os, re, datetime as dt

    print(__file__)
    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print(pathname)
    pathname = pathname.replace("/./","/")
    print(pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        root_dir = "/var/SP/data/bdpmdses/deliveries_churn/"
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "generate_extra_feats_new" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="", msg_format="%(asctime)s [%(levelname)-5.5s] CHURN|%(message)s")
    logger.info("Logging to file {}".format(logging_file))

    return logger


def saveDF(path, df, partitions):
    print('[' + time.ctime() + '] ' + 'Saving started at: ' + path)
    df.coalesce(partitions).write.mode('overwrite').format('parquet').save(path)
    print('[' + time.ctime() + '] ' + 'Saving finished!')


def amdocs_car_main(ClosingDay, StartingDay, spark):

  # year_car = int(str(ClosingDay)[0:4])
  # month_car = int(str(ClosingDay)[4:6])
  # day_car = int(str(ClosingDay)[6:8])

  ts = time.time()
  st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

  print('AMDOCS Informational Dataset building started...'+
        '\n   Starting Time: '+st+
        '\n   Date range studied: From '+StartingDay+' to '+ClosingDay)

  ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
  StartingDay_date = datetime.date(int(StartingDay[:4]), int(StartingDay[4:6]), int(StartingDay[6:8]))
  hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))
  #path_voiceusage = '/data/udf/vf_es/amdocs_ids/usage_geneva_voice/'
  #path_datausage = '/data/udf/vf_es/amdocs_ids/usage_geneva_data/'

  hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'

  path_customer = hdfs_write_path_common + 'customer/'
  path_service = hdfs_write_path_common + 'service/'
  #path_customeragg = hdfs_write_path_common + 'customer_agg/'
  path_voiceusage = hdfs_write_path_common + 'usage_geneva_voice/'
  path_datausage = hdfs_write_path_common + 'usage_geneva_data/'
  path_billing = hdfs_write_path_common + 'billing/'
  path_campaignscustomer = hdfs_write_path_common + 'campaigns_customer/'
  path_campaignsservice = hdfs_write_path_common + 'campaigns_service/'
  path_roamvoice = hdfs_write_path_common + 'usage_geneva_roam_voice/'
  path_roamdata = hdfs_write_path_common + 'usage_geneva_roam_data/'
  path_orders_hist = hdfs_write_path_common + 'orders/'
  path_orders_agg = hdfs_write_path_common + 'orders_agg/'
  path_penal_cust = hdfs_write_path_common + 'penalties_customer/'
  path_penal_srv = hdfs_write_path_common + 'penalties_service/'
  path_devices = hdfs_write_path_common + 'device_catalogue/'
  path_ccc = hdfs_write_path_common + 'call_centre_calls/'
  path_tnps = hdfs_write_path_common + 'tnps/'
  path_customer_agg = hdfs_write_path_common + 'customer_agg/'
  path_perms_and_prefs = hdfs_write_path_common + 'perms_and_prefs/'
  path_netscout_apps = hdfs_write_path_common + 'netscout_apps/'


  # Customer Information
  customerDF = get_customer_df(spark, ClosingDay)
  path_customer_save = path_customer + hdfs_partition_path
  saveDF(path_customer_save, customerDF, 10)

  ## Services Information
  serviceDF = get_services_df(spark, ClosingDay)
  path_service_save = path_service + hdfs_partition_path
  saveDF(path_service_save, serviceDF, 5)

  # Customer Aggregated Information (Services aggregations at customer level)
  customerAggDF = get_customer_agg_df(spark, ClosingDay)
  path_customer_agg_save = path_customer_agg + hdfs_partition_path
  saveDF(path_customer_agg_save, customerAggDF, 3)

  # Voice Usage based on Geneva information
  voiceUsageDF = get_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_voiceusage_save = path_voiceusage + hdfs_partition_path
  saveDF(path_voiceusage_save, voiceUsageDF, 4)

  # Data Usage based on Geneva information
  dataUsageDF = get_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_datausage_save = path_datausage + hdfs_partition_path
  saveDF(path_datausage_save, dataUsageDF, 10)

  # Billing Information - Customer Level - Geneva
  billingDF = get_billing_customer_df(spark, ClosingDay)
  path_billing_save = path_billing + hdfs_partition_path
  saveDF(path_billing_save, billingDF, 2)

  # Customer Level Campaigns
  customerCampaignsDF = get_campaigns_customer_df(spark, ClosingDay, StartingDay)
  path_campaignscustomer_save = path_campaignscustomer + hdfs_partition_path
  saveDF(path_campaignscustomer_save, customerCampaignsDF, 1)

  # Service Level Campaigns
  serviceCampaignsDF = get_campaigns_service_df(spark, ClosingDay, StartingDay)
  path_campaignsservice_save = path_campaignsservice + hdfs_partition_path
  saveDF(path_campaignsservice_save, serviceCampaignsDF, 1)

  # Roaming Voice Usage
  RoamVoiceUsageDF = get_roaming_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamvoice_save = path_roamvoice + hdfs_partition_path
  saveDF(path_roamvoice_save, RoamVoiceUsageDF, 1)

  # Roaming Data Usage
  RoamDataUsageDF = get_roaming_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamdata_save = path_roamdata + hdfs_partition_path
  saveDF(path_roamdata_save, RoamDataUsageDF, 1)

  # Customer Level last 10 orders
  customer_orders_hist = get_orders_customer_level(spark, ClosingDay_date)
  path_orders_hist_save = path_orders_hist + hdfs_partition_path
  saveDF(path_orders_hist_save, customer_orders_hist, 3)

  # Customer Level orders aggregated
  customer_orders_agg = get_orders_customer_level_agg(spark, ClosingDay_date, StartingDay_date)
  path_orders_agg_save = path_orders_agg + hdfs_partition_path
  saveDF(path_orders_agg_save, customer_orders_agg, 1)

  ## Penalties Customer Level
  penalties_cust_level_df = get_penalties_customer_level(spark, ClosingDay)
  path_penal_cust_save = path_penal_cust + hdfs_partition_path
  saveDF(path_penal_cust_save, penalties_cust_level_df, 1)

  # Penalties Service Level
  penalties_serv_level_df = get_penalties_service_level(spark, ClosingDay)
  penalties_serv_level_df = penalties_serv_level_df.cache()
  path_penal_srv_save = path_penal_srv + hdfs_partition_path
  saveDF(path_penal_srv_save, penalties_serv_level_df, 1)

  # Devices per msisdn
  devices_srv_df = get_device_catalogue_df(spark, ClosingDay_date, StartingDay_date)
  path_devices_save = path_devices + hdfs_partition_path
  saveDF(path_devices_save, devices_srv_df, 10)

  # Permissions & Preferences
  perms_and_prefs_df = get_permsandprefs_cust_df(spark, ClosingDay)
  path_perms_and_prefs_save = path_perms_and_prefs + hdfs_partition_path
  saveDF(path_perms_and_prefs_save, perms_and_prefs_df, 10)

  # Call Centre Calls
  df_ccc = CallCentreCalls(spark).get_ccc_service_df(ClosingDay, StartingDay)
  df_ccc = (df_ccc.drop(*['partitioned_month', 'month', 'year', 'day']))
  path_ccc_save = path_ccc + hdfs_partition_path
  saveDF(path_ccc_save, df_ccc, 1)

  # TNPS Information
  df_tnps = Tnps(spark).get_tnps_service_df(ClosingDay, StartingDay)
  df_tnps = df_tnps['msisdn',
                    'tnps_min_VDN', 'tnps_mean_VDN', 'tnps_std_VDN', 'tnps_max_VDN',
                    'tnps_TNPS01',
                    'tnps_TNPS4',
                    'tnps_TNPS', 'tnps_TNPS3PRONEU', 'tnps_TNPS3NEUSDET',
                    'tnps_TNPS2HDET', 'tnps_TNPS2SDET', 'tnps_TNPS2DET', 'tnps_TNPS2NEU', 'tnps_TNPS2PRO', 'tnps_TNPS2INOUT']
  path_tnps_save = path_tnps + hdfs_partition_path
  saveDF(path_tnps_save, df_tnps, 1)
  ##  Netscout Apps
  ## netscout_apps_df = get_netscout_app_usage_basic_df(spark, ClosingDay_date, StartingDay_date)
  ## path_netscout_apps_save = path_netscout_apps + hdfs_partition_path
  ## saveDF(path_netscout_apps_save, netscout_apps_df, 200)

  return 1


def amdocs_car_main_custom(ClosingDay, StartingDay, spark, modules_dict=None, time_logger=None):

  from engine.customer import *
  from engine.services import *
  from engine.customer_aggregations import *
  from engine.geneva_traffic import *
  from engine.billing import *
  from engine.campaigns import *
  from engine.orders import *
  from engine.customer_penalties import *
  from engine.device_catalogue import *
  from engine.permsandprefs import *
  from engine.call_centre_calls import CallCentreCalls
  from engine.tnps import Tnps
  from engine.netscout import *


  print("CSANC109, run_amdocs_car:")
  import pprint
  pprint.pprint(modules_dict)

  # year_car = int(str(ClosingDay)[0:4])
  # month_car = int(str(ClosingDay)[4:6])
  # day_car = int(str(ClosingDay)[6:8])

  ts = time.time()
  st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

  print('AMDOCS Informational Dataset building started...'+
        '\n   Starting Time: '+st+
        '\n   Date range studied: From '+StartingDay+' to '+ClosingDay)

  ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
  StartingDay_date = datetime.date(int(StartingDay[:4]), int(StartingDay[4:6]), int(StartingDay[6:8]))
  hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))
  #path_voiceusage = '/data/udf/vf_es/amdocs_ids/usage_geneva_voice/'
  #path_datausage = '/data/udf/vf_es/amdocs_ids/usage_geneva_data/'

  hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'

  path_customer = hdfs_write_path_common + 'customer/'
  path_service = hdfs_write_path_common + 'service/'
  #path_customeragg = hdfs_write_path_common + 'customer_agg/'
  path_voiceusage = hdfs_write_path_common + 'usage_geneva_voice/'
  path_datausage = hdfs_write_path_common + 'usage_geneva_data/'
  path_billing = hdfs_write_path_common + 'billing/'
  path_campaignscustomer = hdfs_write_path_common + 'campaigns_customer/'
  path_campaignsservice = hdfs_write_path_common + 'campaigns_service/'
  path_roamvoice = hdfs_write_path_common + 'usage_geneva_roam_voice/'
  path_roamdata = hdfs_write_path_common + 'usage_geneva_roam_data/'
  path_orders_hist = hdfs_write_path_common + 'orders/'
  path_orders_agg = hdfs_write_path_common + 'orders_agg/'
  path_penal_cust = hdfs_write_path_common + 'penalties_customer/'
  path_penal_srv = hdfs_write_path_common + 'penalties_service/'
  path_devices = hdfs_write_path_common + 'device_catalogue/'
  path_ccc = hdfs_write_path_common + 'call_centre_calls/'
  path_tnps = hdfs_write_path_common + 'tnps/'
  path_customer_agg = hdfs_write_path_common + 'customer_agg/'
  path_perms_and_prefs = hdfs_write_path_common + 'perms_and_prefs/'
  path_netscout_apps = hdfs_write_path_common + 'netscout_apps/'

  if modules_dict == None or modules_dict["customer"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "customer", ClosingDay, start_time, -1)

      # Customer Information
      customerDF = get_customer_df(spark, ClosingDay)
      path_customer_save = path_customer + hdfs_partition_path
      saveDF(path_customer_save, customerDF, 10)

      if time_logger: time_logger.register_time(spark, "customer", ClosingDay, start_time, time.time())

  if modules_dict == None or modules_dict["service"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "service", ClosingDay, start_time, -1)

      ## Services Information
      serviceDF = get_services_df(spark, ClosingDay)
      path_service_save = path_service + hdfs_partition_path
      saveDF(path_service_save, serviceDF, 5)

      if time_logger: time_logger.register_time(spark, "service", ClosingDay, start_time, time.time())

  if modules_dict == None or modules_dict["customer_agg"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "customer_agg", ClosingDay, start_time, -1)

      # Customer Aggregated Information (Services aggregations at customer level)
      customerAggDF = get_customer_agg_df(spark, ClosingDay)
      path_customer_agg_save = path_customer_agg + hdfs_partition_path
      saveDF(path_customer_agg_save, customerAggDF, 3)

      if time_logger: time_logger.register_time(spark, "customer_agg", ClosingDay, start_time, time.time())

  if modules_dict == None or modules_dict["usage_geneva_voice"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "usage_geneva_voice", ClosingDay, start_time, -1)

      # Voice Usage based on Geneva information
      voiceUsageDF = get_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
      path_voiceusage_save = path_voiceusage + hdfs_partition_path
      saveDF(path_voiceusage_save, voiceUsageDF, 4)

      if time_logger: time_logger.register_time(spark, "usage_geneva_voice", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["usage_geneva_data"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "usage_geneva_data", ClosingDay, start_time, -1)


      # Data Usage based on Geneva information
      dataUsageDF = get_data_usage_geneva_df(spark, ClosingDay, StartingDay)
      path_datausage_save = path_datausage + hdfs_partition_path
      saveDF(path_datausage_save, dataUsageDF, 10)

      if time_logger: time_logger.register_time(spark, "usage_geneva_data", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["billing"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "billing", ClosingDay, start_time, -1)


      # Billing Information - Customer Level - Geneva
      billingDF = get_billing_customer_df(spark, ClosingDay)
      path_billing_save = path_billing + hdfs_partition_path
      saveDF(path_billing_save, billingDF, 2)

      if time_logger: time_logger.register_time(spark, "billing", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["campaigns_customer"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "campaigns_customer", ClosingDay, start_time, -1)


      # Customer Level Campaigns
      customerCampaignsDF = get_campaigns_customer_df(spark, ClosingDay, StartingDay)
      path_campaignscustomer_save = path_campaignscustomer + hdfs_partition_path
      saveDF(path_campaignscustomer_save, customerCampaignsDF, 1)

      if time_logger: time_logger.register_time(spark, "campaigns_customer", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["campaigns_service"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "campaigns_service", ClosingDay, start_time, -1)

      # Service Level Campaigns
      serviceCampaignsDF = get_campaigns_service_df(spark, ClosingDay, StartingDay)
      path_campaignsservice_save = path_campaignsservice + hdfs_partition_path
      saveDF(path_campaignsservice_save, serviceCampaignsDF, 1)

      if time_logger: time_logger.register_time(spark, "campaigns_service", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["usage_geneva_roam_voice"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "usage_geneva_roam_voice", ClosingDay, start_time, -1)

      # Roaming Voice Usage
      RoamVoiceUsageDF = get_roaming_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
      path_roamvoice_save = path_roamvoice + hdfs_partition_path
      saveDF(path_roamvoice_save, RoamVoiceUsageDF, 1)

      if time_logger: time_logger.register_time(spark, "usage_geneva_roam_voice", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["usage_geneva_roam_data"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "usage_geneva_roam_data", ClosingDay, start_time, -1)

      # Roaming Data Usage
      RoamDataUsageDF = get_roaming_data_usage_geneva_df(spark, ClosingDay, StartingDay)
      path_roamdata_save = path_roamdata + hdfs_partition_path
      saveDF(path_roamdata_save, RoamDataUsageDF, 1)

      if time_logger: time_logger.register_time(spark, "usage_geneva_roam_data", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["orders"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "orders", ClosingDay, start_time, -1)


      # Customer Level last 10 orders
      customer_orders_hist = get_orders_customer_level(spark, ClosingDay_date)
      path_orders_hist_save = path_orders_hist + hdfs_partition_path
      saveDF(path_orders_hist_save, customer_orders_hist, 3)

      if time_logger: time_logger.register_time(spark, "orders", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["orders_agg"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "orders_agg", ClosingDay, start_time, -1)

      # Customer Level orders aggregated
      customer_orders_agg = get_orders_customer_level_agg(spark, ClosingDay_date, StartingDay_date)
      path_orders_agg_save = path_orders_agg + hdfs_partition_path
      saveDF(path_orders_agg_save, customer_orders_agg, 1)

      if time_logger: time_logger.register_time(spark, "orders_agg", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["penalties_customer"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "penalties_customer", ClosingDay, start_time, -1)

      ## Penalties Customer Level
      penalties_cust_level_df = get_penalties_customer_level(spark, ClosingDay)
      path_penal_cust_save = path_penal_cust + hdfs_partition_path
      saveDF(path_penal_cust_save, penalties_cust_level_df, 1)

      if time_logger: time_logger.register_time(spark, "penalties_customer", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["penalties_service"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "penalties_service", ClosingDay, start_time, -1)

      # Penalties Service Level
      penalties_serv_level_df = get_penalties_service_level(spark, ClosingDay)
      penalties_serv_level_df = penalties_serv_level_df.cache()
      path_penal_srv_save = path_penal_srv + hdfs_partition_path
      saveDF(path_penal_srv_save, penalties_serv_level_df, 1)

      if time_logger: time_logger.register_time(spark, "penalties_service", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["device_catalogue"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "device_catalogue", ClosingDay, start_time, -1)

      # Devices per msisdn
      devices_srv_df = get_device_catalogue_df(spark, ClosingDay_date, StartingDay_date)
      path_devices_save = path_devices + hdfs_partition_path
      saveDF(path_devices_save, devices_srv_df, 10)

      if time_logger: time_logger.register_time(spark, "device_catalogue", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["perms_and_prefs"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "perms_and_prefs", ClosingDay, start_time, -1)

      # Permissions & Preferences
      perms_and_prefs_df = get_permsandprefs_cust_df(spark, ClosingDay)
      path_perms_and_prefs_save = path_perms_and_prefs + hdfs_partition_path
      saveDF(path_perms_and_prefs_save, perms_and_prefs_df, 10)

      if time_logger: time_logger.register_time(spark, "perms_and_prefs", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["call_centre_calls"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "call_centre_calls", ClosingDay, start_time, -1)

      # Call Centre Calls
      df_ccc = CallCentreCalls(spark).get_ccc_service_df(ClosingDay, StartingDay)
      df_ccc = (df_ccc.drop(*['partitioned_month', 'month', 'year', 'day']))
      path_ccc_save = path_ccc + hdfs_partition_path
      saveDF(path_ccc_save, df_ccc, 1)

      if time_logger: time_logger.register_time(spark, "call_centre_calls", ClosingDay, start_time, time.time())


  if modules_dict == None or modules_dict["tnps"]:

      start_time = time.time()
      if time_logger: time_logger.register_time(spark, "tnps", ClosingDay, start_time, -1)

      # TNPS Information
      df_tnps = Tnps(spark).get_tnps_service_df(ClosingDay, StartingDay)
      df_tnps = df_tnps['msisdn',
                        'tnps_min_VDN', 'tnps_mean_VDN', 'tnps_std_VDN', 'tnps_max_VDN',
                        'tnps_TNPS01',
                        'tnps_TNPS4',
                        'tnps_TNPS', 'tnps_TNPS3PRONEU', 'tnps_TNPS3NEUSDET',
                        'tnps_TNPS2HDET', 'tnps_TNPS2SDET', 'tnps_TNPS2DET', 'tnps_TNPS2NEU', 'tnps_TNPS2PRO', 'tnps_TNPS2INOUT']
      path_tnps_save = path_tnps + hdfs_partition_path
      saveDF(path_tnps_save, df_tnps, 1)

      if time_logger: time_logger.register_time(spark, "tnps", ClosingDay, start_time, time.time())

  ##  Netscout Apps
  ## netscout_apps_df = get_netscout_app_usage_basic_df(spark, ClosingDay_date, StartingDay_date)
  ## path_netscout_apps_save = path_netscout_apps + hdfs_partition_path
  ## saveDF(path_netscout_apps_save, netscout_apps_df, 200)

  return 1

def amdocs_car_main_geneva2(ClosingDay, StartingDay,spark):

  year_car = int(str(ClosingDay)[0:4])
  month_car = int(str(ClosingDay)[4:6])
  day_car = int(str(ClosingDay)[6:8])

  ts = time.time()
  st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

  print('AMDOCS Informational Dataset building started...'+
        '\n   Starting Time: '+st+
        '\n   Date range studied: From '+StartingDay+' to '+ClosingDay)

  ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
  StartingDay_date = datetime.date(int(StartingDay[:4]), int(StartingDay[4:6]), int(StartingDay[6:8]))
  hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))


  hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'

  path_voiceusage = hdfs_write_path_common + 'usage_geneva_voice/'
  path_datausage = hdfs_write_path_common + 'usage_geneva_data/'
  path_roamvoice = hdfs_write_path_common + 'usage_geneva_roam_voice/'
  path_roamdata = hdfs_write_path_common + 'usage_geneva_roam_data/'



  # Voice Usage based on Geneva information
  voiceUsageDF = get_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_voiceusage_save = path_voiceusage + hdfs_partition_path
  saveDF(path_voiceusage_save, voiceUsageDF, 4)

  # Data Usage based on Geneva information
  dataUsageDF = get_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_datausage_save = path_datausage + hdfs_partition_path
  saveDF(path_datausage_save, dataUsageDF, 10)

  # Roaming Voice Usage
  RoamVoiceUsageDF = get_roaming_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamvoice_save = path_roamvoice + hdfs_partition_path
  saveDF(path_roamvoice_save, RoamVoiceUsageDF, 1)

  # Roaming Data Usage
  RoamDataUsageDF = get_roaming_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamdata_save = path_roamdata + hdfs_partition_path
  saveDF(path_roamdata_save, RoamDataUsageDF, 1)


  return 1

def amdocs_car_main_prepare_ccc_features(ClosingDay, StartingDay,spark):

  year_car = int(str(ClosingDay)[0:4])
  month_car = int(str(ClosingDay)[4:6])
  day_car = int(str(ClosingDay)[6:8])

  ts = time.time()
  st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

  print('AMDOCS Informational Dataset building started...'+
        '\n   Starting Time: '+st+
        '\n   Date range studied: From '+StartingDay+' to '+ClosingDay)

  ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
  StartingDay_date = datetime.date(int(StartingDay[:4]), int(StartingDay[4:6]), int(StartingDay[6:8]))
  hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))


  hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'

  path_voiceusage = hdfs_write_path_common + 'usage_geneva_voice/'
  path_datausage = hdfs_write_path_common + 'usage_geneva_data/'
  path_roamvoice = hdfs_write_path_common + 'usage_geneva_roam_voice/'
  path_roamdata = hdfs_write_path_common + 'usage_geneva_roam_data/'



  # Voice Usage based on Geneva information
  voiceUsageDF = get_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_voiceusage_save = path_voiceusage + hdfs_partition_path
  saveDF(path_voiceusage_save, voiceUsageDF, 4)

  # Data Usage based on Geneva information
  dataUsageDF = get_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_datausage_save = path_datausage + hdfs_partition_path
  saveDF(path_datausage_save, dataUsageDF, 10)

  # Roaming Voice Usage
  RoamVoiceUsageDF = get_roaming_voice_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamvoice_save = path_roamvoice + hdfs_partition_path
  saveDF(path_roamvoice_save, RoamVoiceUsageDF, 1)

  # Roaming Data Usage
  RoamDataUsageDF = get_roaming_data_usage_geneva_df(spark, ClosingDay, StartingDay)
  path_roamdata_save = path_roamdata + hdfs_partition_path
  saveDF(path_roamdata_save, RoamDataUsageDF, 1)


  return 1


if __name__ == "__main__":
    print '[' + time.ctime() + ']', 'Process started'

    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')

    set_paths_and_logger()

    from common.src.main.python.utils.hdfs_generic import *

    global sqlContext
    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession.builder
             .appName("VF_ES vf12")
             .master("yarn")
             .config("spark.submit.deployMode", "client")
             .config("spark.ui.showConsoleProgress", "true")
             .enableHiveSupport()
             .getOrCreate()
             )

    import argparse

    parser = argparse.ArgumentParser(
        description='Generate car',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=True,
                        help='Closing day YYYYMMDD)')
    parser.add_argument('-s', '--starting_day', metavar='<STARTING_DAY>', type=str, required=True,
                        help='starting day YYYYMMDD')
    args = parser.parse_args()

    print(args)


    closing_day = args.closing_day
    starting_day = args.starting_day

    from engine.customer import *
    from engine.services import *
    from engine.customer_aggregations import *
    from engine.geneva_traffic import *
    from engine.billing import *
    from engine.campaigns import *
    from engine.orders import *
    from engine.customer_penalties import *
    from engine.device_catalogue import *
    from engine.permsandprefs import *
    from engine.call_centre_calls import CallCentreCalls
    from engine.tnps import Tnps
    from engine.netscout import *
    from engine.general_functions import *

    closingdays = [ (closing_day, starting_day) ]

    for i in closingdays:
        ClosingDay = i[0]
        StartingDay = i[1]
        print('Ciclo bueno: ' + StartingDay + ' a ' + ClosingDay)
        ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
        StartingDay_date = datetime.date(int(StartingDay[:4]), int(StartingDay[4:6]), int(StartingDay[6:8]))
        result = amdocs_car_main(ClosingDay, StartingDay, spark)


