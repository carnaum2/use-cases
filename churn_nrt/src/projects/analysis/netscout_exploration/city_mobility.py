#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import (udf, col, decode, when, lit, lower, concat,
                                   translate, count, sum as sql_sum, max as sql_max, min as sql_min, avg as sql_avg,
                                   greatest,
                                   least,
                                   isnull,
                                   isnan,
                                   struct,
                                   substring,
                                   size,
                                   length,
                                   year,
                                   month,
                                   dayofmonth,
                                   unix_timestamp,
                                   date_format,
                                   from_unixtime,
                                   datediff,
                                   to_date,
                                   desc,
                                   asc,
                                   countDistinct,
                                   row_number,
                                   regexp_replace,
                                   first,
                                   to_timestamp,
                                   lpad,
                                   rpad,
                                   coalesce,
                                   udf,
                                   date_add,
                                   explode,
                                   collect_set,
                                   length,
                                   expr,
                                   split,
                                   hour,
                                   minute)
import datetime
import pandas as pd

path_netscout = "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/"

list_netscout_working_apps = ['AccuWeather', 'Adult', 'AdultWeb', 'Alcohol', 'Alibaba', 'Alipay', 'Amazon', 'Apple',
                              'Arts',
                              'Astrology', 'Audio', 'Auto', 'Badoo', 'Baidu', 'BBC', 'Booking', 'Books', 'Business',
                              'Chats',
                              'Classified', 'Dating', 'Dropbox', 'Ebay', 'Education', 'Facebook', 'Facebook Video',
                              'FacebookMessages', 'Family', 'Fashion', 'Finance', 'Food', 'Foursquare', 'Gambling',
                              'GitHub',
                              'Gmail', 'GoogleDrive', 'GoogleEarth', 'GoogleMaps', 'GooglePlay', 'Groupon', 'Hacking',
                              'Home',
                              'Instagram', 'iTunes', 'Jobs', 'Kids', 'Legal', 'LINE', 'Linkedin', 'Medical', 'Music',
                              'Netflix',
                              'NetFlixVideo', 'news', 'News', 'Paypal', 'Pets', 'Pinterest', 'Politics', 'Pregnancy',
                              'QQ',
                              'Reddit', 'Samsung', 'Science', 'Shopping', 'Skype', 'SKYPE', 'Snapchat', 'SocialNetwork',
                              'Sports',
                              'Spotify', 'Steam', 'TaoBao', 'Technology', 'Travel', 'Tumblr', 'Twitch', 'Twitter',
                              'Video',
                              'VideoStreaming', 'Vimeo', 'Violence', 'WEB_JAZZTEL', 'WEB_JAZZTEL_HTTP',
                              'WEB_JAZZTEL_HTTPS',
                              'WEB_LOWI', 'WEB_LOWI_HTTP', 'WEB_LOWI_HTTPS', 'WEB_MARCA.COM_HTTP',
                              'WEB_MARCA.com_HTTPS',
                              'WEB_MASMOVIL', 'WEB_MASMOVIL_HTTP', 'WEB_MASMOVIL_HTTPS', 'WEB_MOVISTAR',
                              'WEB_MOVISTAR_HTTP',
                              'WEB_MOVISTAR_HTTPS', 'WEB_O2', 'WEB_O2_HTTP', 'WEB_O2_HTTPS', 'WEB_ORANGE',
                              'WEB_ORANGE_HTTP',
                              'WEB_ORANGE_HTTPS', 'WEB_PEPEPHONE', 'WEB_PEPEPHONE_HTTP', 'WEB_PEPEPHONE_HTTPS',
                              'WEB_VODAFONE',
                              'WEB_VODAFONE_HTTP', 'WEB_VODAFONE_HTTPS', 'WEB_YOIGO', 'WEB_YOIGO_HTTP',
                              'WEB_YOIGO_HTTPS',
                              'WEB_YOMVI_HTTP', 'WEB_YOMVI_HTTPS', 'WebGames', 'WebMobile', 'WeChat', 'Whatsapp',
                              'WhatsApp Media Message', 'WhatsApp Voice Calling', 'Wikipedia', 'Yandex', 'Youtube',
                              'Zynga']

ns_app_values = ['NS_APPS_ACCUWEATHER', 'NS_APPS_ADULT', 'NS_APPS_ADULTWEB', 'NS_APPS_ALCOHOL', 'NS_APPS_ALIBABA'
    , 'NS_APPS_ALIPAY', 'NS_APPS_AMAZON', 'NS_APPS_APPLE', 'NS_APPS_ARTS', 'NS_APPS_ASTROLOGY'
    , 'NS_APPS_AUDIO', 'NS_APPS_AUTO', 'NS_APPS_BADOO', 'NS_APPS_BAIDU', 'NS_APPS_BBC'
    , 'NS_APPS_BOOKING', 'NS_APPS_BOOKS', 'NS_APPS_BUSINESS', 'NS_APPS_CHATS', 'NS_APPS_CLASSIFIED'
    , 'NS_APPS_DATING', 'NS_APPS_DROPBOX', 'NS_APPS_EBAY', 'NS_APPS_EDUCATION', 'NS_APPS_FACEBOOK'
    , 'NS_APPS_FACEBOOKMESSAGES', 'NS_APPS_FACEBOOK_VIDEO', 'NS_APPS_FAMILY', 'NS_APPS_FASHION'
    , 'NS_APPS_FINANCE', 'NS_APPS_FOOD', 'NS_APPS_FOURSQUARE', 'NS_APPS_GAMBLING', 'NS_APPS_GITHUB'
    , 'NS_APPS_GMAIL', 'NS_APPS_GOOGLEDRIVE', 'NS_APPS_GOOGLEEARTH', 'NS_APPS_GOOGLEMAPS'
    , 'NS_APPS_GOOGLEPLAY', 'NS_APPS_GROUPON', 'NS_APPS_HACKING', 'NS_APPS_HOME', 'NS_APPS_INSTAGRAM'
    , 'NS_APPS_ITUNES', 'NS_APPS_JOBS', 'NS_APPS_KIDS', 'NS_APPS_LEGAL', 'NS_APPS_LINE', 'NS_APPS_LINKEDIN'
    , 'NS_APPS_MEDICAL', 'NS_APPS_MUSIC', 'NS_APPS_NETFLIX', 'NS_APPS_NETFLIXVIDEO', 'NS_APPS_NEWS'
    , 'NS_APPS_PAYPAL', 'NS_APPS_PETS', 'NS_APPS_PINTEREST', 'NS_APPS_POLITICS', 'NS_APPS_PREGNANCY'
    , 'NS_APPS_QQ', 'NS_APPS_REDDIT', 'NS_APPS_SAMSUNG', 'NS_APPS_SCIENCE', 'NS_APPS_SHOPPING'
    , 'NS_APPS_SKYPE', 'NS_APPS_SNAPCHAT', 'NS_APPS_SOCIALNETWORK', 'NS_APPS_SPORTS', 'NS_APPS_SPOTIFY'
    , 'NS_APPS_STEAM', 'NS_APPS_TAOBAO', 'NS_APPS_TECHNOLOGY', 'NS_APPS_TRAVEL', 'NS_APPS_TUMBLR'
    , 'NS_APPS_TWITCH', 'NS_APPS_TWITTER', 'NS_APPS_VIDEO', 'NS_APPS_VIDEOSTREAMING', 'NS_APPS_VIMEO'
    , 'NS_APPS_VIOLENCE', 'NS_APPS_WEBGAMES', 'NS_APPS_WEBMOBILE', 'NS_APPS_WEB_JAZZTEL'
    , 'NS_APPS_WEB_JAZZTEL_HTTP', 'NS_APPS_WEB_JAZZTEL_HTTPS', 'NS_APPS_WEB_LOWI'
    , 'NS_APPS_WEB_LOWI_HTTP', 'NS_APPS_WEB_LOWI_HTTPS', 'NS_APPS_WEB_MARCACOM_HTTP'
    , 'NS_APPS_WEB_MARCACOM_HTTPS', 'NS_APPS_WEB_MASMOVIL', 'NS_APPS_WEB_MASMOVIL_HTTP'
    , 'NS_APPS_WEB_MASMOVIL_HTTPS', 'NS_APPS_WEB_MOVISTAR', 'NS_APPS_WEB_MOVISTAR_HTTP'
    , 'NS_APPS_WEB_MOVISTAR_HTTPS', 'NS_APPS_WEB_O2', 'NS_APPS_WEB_O2_HTTP', 'NS_APPS_WEB_O2_HTTPS'
    , 'NS_APPS_WEB_ORANGE', 'NS_APPS_WEB_ORANGE_HTTP', 'NS_APPS_WEB_ORANGE_HTTPS', 'NS_APPS_WEB_PEPEPHONE'
    , 'NS_APPS_WEB_PEPEPHONE_HTTP', 'NS_APPS_WEB_PEPEPHONE_HTTPS', 'NS_APPS_WEB_VODAFONE'
    , 'NS_APPS_WEB_VODAFONE_HTTP', 'NS_APPS_WEB_VODAFONE_HTTPS', 'NS_APPS_WEB_YOIGO'
    , 'NS_APPS_WEB_YOIGO_HTTP', 'NS_APPS_WEB_YOIGO_HTTPS', 'NS_APPS_WEB_YOMVI_HTTP'
    , 'NS_APPS_WEB_YOMVI_HTTPS', 'NS_APPS_WECHAT', 'NS_APPS_WHATSAPP', 'NS_APPS_WHATSAPP_MEDIA_MESSAGE'
    , 'NS_APPS_WHATSAPP_VOICE_CALLING', 'NS_APPS_WIKIPEDIA', 'NS_APPS_YANDEX', 'NS_APPS_YOUTUBE'
    , 'NS_APPS_ZYNGA']

ns_cell_values=['ns_mostfrequent_cell_1_WE_10_14', 'ns_mostfrequent_cell_1_WE_14_18', 'ns_mostfrequent_cell_1_WE_18_22', 'ns_mostfrequent_cell_1_WE_22_6', 'ns_mostfrequent_cell_1_WE_6_10', 'ns_mostfrequent_cell_1_W_10_14', 'ns_mostfrequent_cell_1_W_14_18', 'ns_mostfrequent_cell_1_W_18_22', 'ns_mostfrequent_cell_1_W_22_6', 'ns_mostfrequent_cell_1_W_6_10', 'ns_mostfrequent_cell_2_WE_10_14', 'ns_mostfrequent_cell_2_WE_14_18', 'ns_mostfrequent_cell_2_WE_18_22', 'ns_mostfrequent_cell_2_WE_22_6', 'ns_mostfrequent_cell_2_WE_6_10', 'ns_mostfrequent_cell_2_W_10_14', 'ns_mostfrequent_cell_2_W_14_18', 'ns_mostfrequent_cell_2_W_18_22', 'ns_mostfrequent_cell_2_W_22_6', 'ns_mostfrequent_cell_2_W_6_10', 'ns_mostfrequent_cell_3_WE_10_14', 'ns_mostfrequent_cell_3_WE_14_18', 'ns_mostfrequent_cell_3_WE_18_22', 'ns_mostfrequent_cell_3_WE_22_6', 'ns_mostfrequent_cell_3_WE_6_10', 'ns_mostfrequent_cell_3_W_10_14', 'ns_mostfrequent_cell_3_W_14_18', 'ns_mostfrequent_cell_3_W_18_22', 'ns_mostfrequent_cell_3_W_22_6', 'ns_mostfrequent_cell_3_W_6_10']

ns_qlt_values1 = ['ns_quality_2G_WE_10_14', 'ns_quality_2G_WE_14_18', 'ns_quality_2G_WE_18_22', 'ns_quality_2G_WE_22_6', 'ns_quality_2G_WE_6_10', 'ns_quality_2G_W_10_14', 'ns_quality_2G_W_14_18', 'ns_quality_2G_W_18_22', 'ns_quality_2G_W_22_6', 'ns_quality_2G_W_6_10', 'ns_quality_3G_WE_10_14', 'ns_quality_3G_WE_14_18', 'ns_quality_3G_WE_18_22', 'ns_quality_3G_WE_22_6', 'ns_quality_3G_WE_6_10', 'ns_quality_3G_W_10_14', 'ns_quality_3G_W_14_18', 'ns_quality_3G_W_18_22', 'ns_quality_3G_W_22_6', 'ns_quality_3G_W_6_10', 'ns_quality_4G_WE_10_14', 'ns_quality_4G_WE_14_18', 'ns_quality_4G_WE_18_22', 'ns_quality_4G_WE_22_6', 'ns_quality_4G_WE_6_10', 'ns_quality_4G_W_10_14', 'ns_quality_4G_W_14_18', 'ns_quality_4G_W_18_22', 'ns_quality_4G_W_22_6', 'ns_quality_4G_W_6_10', 'ns_quality_OTHER_WE_10_14', 'ns_quality_OTHER_WE_14_18', 'ns_quality_OTHER_WE_18_22', 'ns_quality_OTHER_WE_22_6', 'ns_quality_OTHER_WE_6_10', 'ns_quality_OTHER_W_10_14', 'ns_quality_OTHER_W_14_18', 'ns_quality_OTHER_W_18_22', 'ns_quality_OTHER_W_22_6', 'ns_quality_OTHER_W_6_10']

ns_qlt_values2 = ['ns_quality_2G','ns_quality_3G','ns_quality_4G','ns_quality_OTHER']


def get_cell_agg_df(spark, date_, city_):
    """
    Function to obtain a uniform dataframe from netscout subscriber user plane
    Dates and applications are filtered to only collect the relevant information
    :return: dataframe netscout curated
    """
    #closing_day_date = datetime.datetime.strptime(closing_day, "%Y%m%d")
    #starting_day_date = datetime.datetime.strptime(starting_day, "%Y%m%d")
    year_ = date_[0:4]
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))

    from churn_nrt.src.data.customer_base import CustomerBase

    base_df = CustomerBase(spark) \
        .get_module(date_, save=False, save_others=False, force_gen=True) \
        .filter(col('rgu') == 'mobile') \
        .select("msisdn") \
        .distinct() \
        .repartition(400)

    cell_df = spark.read.table("raw_es.cellinventory_cellinventory")\
        .filter((col("year") == int(year_)) & (col("month") == int(month_)) & (col("day") == int(day_)))\
        .withColumnRenamed('mcc', 'cell_mcc')\
        .withColumn('cell_mnc', regexp_replace('mnc', r'^[0]*', ''))\
        .withColumn('cell_area', regexp_replace('lac', r'^[0]*', ''))\
        .withColumn('cell_id', regexp_replace('CID', r'^[0]*', ''))\
        .select('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id', 'city', 'Type_of_cell')\
        .distinct()

    data_netscout_ori = (spark
                         .read
                         .parquet(path_netscout + "year=" + year_ + "/month=" + month_ + "/day=" + day_)
                         .where(col('application_name').isin(list_netscout_working_apps))
                         .where(~col('subscriber_msisdn').isNull())
                         .withColumn('msisdn', when(substring(col('subscriber_msisdn'), 1, 2) == '34', substring(col('subscriber_msisdn'), 3, 9)).otherwise(col('subscriber_msisdn')))
                         .select('msisdn', 'cell_mcc', 'cell_mnc', 'cell_area', 'cell_id')
                         .join(base_df, ['msisdn'], 'inner')
                         .join(cell_df, ['cell_mcc', 'cell_mnc', 'cell_area', 'cell_id'], 'inner')
                         .filter(col('city')==city_)
                         .groupBy('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id')
                         .agg(countDistinct('msisdn').alias('num_msisdn'))
                         .withColumn('num_msisdn', col('num_msisdn').cast('double'))
                         .withColumn('year', lit(year_))
                         .withColumn('month', lit(month_))
                         .withColumn('day', lit(day_))
                         .withColumn('time', concat(lit(year_), lpad(lit(month_), 2, '0'), lpad(lit(day_), 2, '0')))
                         .withColumn('city', lit(city_))
                         )

    return data_netscout_ori


def set_paths():

    import os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

if __name__ == "__main__":

    set_paths()

    ############### 0. Spark ##################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("movility_analysis")

    #path_netscout = "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/"

    from churn_nrt.src.utils.date_functions import move_date_n_days

    date_ = "20200317"
    dates_ = [move_date_n_days(date_, d) for d in list(range(0, 6))]

    dates_ = ["20200305", "20200319"]

    mobility_curve_list = [get_cell_agg_df(spark, d_, 'MADRID') for d_ in dates_]

    from functools import reduce

    mobility_curve_df = reduce(lambda x, y: x.union(y), mobility_curve_list)

    mobility_curve_df.cache()

    mobility_curve_df.show(200, False)

    print "[Info] Cell-level info showed above"

    mobility_curve_df \
        .groupBy('time').agg(sql_avg('num_msisdn').alias('avg_num_msisdn'), countDistinct('cell_mcc', 'cell_mnc', 'cell_area', 'cell_id').alias('num_cells')) \
        .show()

    print "[Info] Time-agg info showed above"

    mobility_curve_df\
        .write \
        .partitionBy('year', 'month', 'day', 'city') \
        .mode("overwrite") \
        .format("parquet").save('/user/jmarcoso/mobility/')

