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


def get_cell_agg_df(spark, date_):
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

    data_netscout_ori = (spark
                         .read
                         .parquet(path_netscout + "year=" + year_ + "/month=" + month_ + "/day=" + day_)
                         .where(col('application_name').isin(list_netscout_working_apps))
                         .where(~col('subscriber_msisdn').isNull())
                         .withColumn('msisdn', when(substring(col('subscriber_msisdn'), 1, 2) == '34', substring(col('subscriber_msisdn'), 3, 9)).otherwise(col('subscriber_msisdn')))
                         .select('msisdn', 'cell_id')
                         .join(base_df, ['msisdn'])
                         .groupBy(['msisdn'])
                         .agg(countDistinct(col('cell_id')).alias('num_cells'))
                         .withColumn("year", lit(year_))
                         .withColumn("month", lit(month_))
                         .withColumn("day", lit(day_))
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

    dates_ = ["20200228", "20200302"]

    movility_curve = [(d_, get_cell_agg_df(spark, d_).withColumn('num_cells', col('num_cells').cast('double')).select(sql_avg('num_cells').alias('avg_num_cells')).first()['avg_num_cells']) for d_ in dates_]

    for e in movility_curve:
        print "[Info] " + str(e[0]) + "," + str(e[1])

    '''
    start_date = "20200305"

    end_date = "20200319"

    start_cell_agg = get_cell_agg_df(spark, start_date).withColumnRenamed('num_cells', 'start_num_cells')

    start_cell_agg.describe('start_num_cells').show()

    print "[Info] Stats on start_num_cells showed above - start_date = " + start_date

    end_cell_agg = get_cell_agg_df(spark, end_date).withColumnRenamed('num_cells', 'end_num_cells')

    end_cell_agg.describe('end_num_cells').show()

    print "[Info] Stats on end_num_cells showed above - end_date = " + end_date

    start_cell_agg.join(end_cell_agg, ['msisdn'], 'inner')\
        .withColumn('num_cell_decrease', col('end_num_cells') - col('start_num_cells'))\
        .describe('num_cell_decrease')\
        .show()

    print "[Info] Stats on num_cell_decrease showed above"
    
    '''

    '''
    
    closing_day_date = datetime.datetime.strptime("20200310", "%Y%m%d")
    starting_day_date = datetime.datetime.strptime("20200310", "%Y%m%d")

    df = spark.read.parquet(path_netscout) \
        .where(col('cal_timestamp_time').between(starting_day_date, closing_day_date)) \
        .where(~col('subscriber_msisdn').isNull()) \
        .withColumn('msisdn', when(substring(col('subscriber_msisdn'), 1, 2) == '34',
                                   substring(col('subscriber_msisdn'), 3, 9)).otherwise(col('subscriber_msisdn')))

    df.printSchema()

    df.show()

    # Input arguments

    date_ = sys.argv[1]

    app_name_ = sys.argv[2]

    n_samples = int(sys.argv[3])

    window_length = int(sys.argv[4])

    expname = "Date" + date_ + "_App" + app_name_ + "_NSamples" + str(n_samples) + "_WindowLenght" + str(window_length)

    # up to 4 months ago

    # min date in the source ("/data/udf/vf_es/netscout/dailyMSISDNApplicationName/") is 20190401

    from churn_nrt.src.utils.date_functions import move_date_n_days

    starting_date = move_date_n_days(date_, -window_length*n_samples)

    dates_ = [move_date_n_days(date_, -window_length*i) for i in range(1, n_samples+1)] + [date_]

    dates_.sort()

    print "[Info] Starting date: " + str(starting_date) + " - Ending date: " + str(date_)

    for d in dates_:
        print "[Info] Date to be sampled: " + str(d)

    # Getting the populations (churners & nochurners) to be tracked

    from churn_nrt.src.data.sopos_dxs import MobPort

    df_sols_port = MobPort(spark)\
        .get_module(date_, save=False, churn_window=30)\
        .select("msisdn")\
        .distinct().withColumn("tmp", lit(1))

    # Active base on date_ that was active on starting_date

    from churn_nrt.src.data.customer_base import CustomerBase

    df_base_msisdn_end = CustomerBase(spark) \
        .get_module(date_, save=False) \
        .filter(col("rgu") == "mobile") \
        .select("msisdn") \
        .distinct()

    df_base_msisdn_ini = CustomerBase(spark) \
        .get_module(starting_date, save=False) \
        .filter(col("rgu") == "mobile") \
        .select("msisdn") \
        .distinct()

    df_base_msisdn = df_base_msisdn_end.join(df_base_msisdn_ini, ['msisdn'], 'inner')

    # Valid customers

    # df_population = df_sols_port.join(df_base_msisdn, ["msisdn"], "inner")

    df_population = df_base_msisdn.join(df_sols_port, ["msisdn"], "left")

    df_churn_population = df_population.filter(col("tmp")==1)

    df_nochurn_population = df_population.filter(isnull(col("tmp")))

    # Generate n_samples dates from date_

    churn_track = [get_app_consumption_last_n_days(spark, df_churn_population, d, app = app_name_, n = window_length) for d in dates_]

    nochurn_track = [get_app_consumption_last_n_days(spark, df_nochurn_population, d, app=app_name_, n=window_length) for d in dates_]

    import pandas as pd

    df_churn = pd.DataFrame(churn_track, columns=['date', 'churn_' + app_name_.lower(), 'churn_num_users', 'churn_num_days'])

    df_nochurn = pd.DataFrame(nochurn_track, columns=['date', 'nochurn_' + app_name_.lower(), 'nochurn_num_users', 'nochurn_num_days'])

    df_track = pd.merge(df_churn, df_nochurn, on='date', how='inner')

    df_track.to_csv('/var/SP/data/home/jmarcoso/ConsumpTrack_' + expname + '.txt', sep='|', header=True, index=False)

    ########################
    
    '''

'''

+----------------------+---------+-----------+
|application_name      |num_users|total_count|
+----------------------+---------+-----------+
|HTTP                  |1620366.0|633714399  |
|Google                |1620120.0|3386187112 |
|HTTPS                 |1618250.0|1756003292 |
|GooglePlay            |1616344.0|1697346819 |
|GoogleAPIs            |1616011.0|2147495934 |
|Facebook              |1615012.0|1948954518 |
|Generic               |1614614.0|1070361391 |
|Youtube               |1613801.0|782212036  |
|NTP                   |1612592.0|420644172  |
|ICMP                  |1612293.0|591100141  |
|GoogleAds             |1609228.0|547025764  |
|ANDRDMKT              |1607341.0|2696319051 |
|GoogleAnalytics       |1607305.0|451784723  |
|IP_OTHER              |1606207.0|2743438116 |
|XMPP                  |1601457.0|1735625320 |
|Whatsapp              |1601251.0|1379943409 |
|AmazonWS              |1598021.0|782114022  |
|cdn.ampproject.org    |1587993.0|239423382  |
|FacebookMessages      |1584052.0|1699038224 |
|Amazon                |1578341.0|327730312  |
|GoogleDrive           |1577965.0|156997206  |
|Akamai                |1575035.0|314786328  |
|Amazon-us-east-1      |1565541.0|297397724  |
|DNS                   |1529516.0|155204981  |
|GoogleXlate           |1529216.0|28226848   |
|Technology            |1516822.0|113941320  |
|Business              |1515093.0|92813248   |
|Amazon-eu-west-1      |1510668.0|87946439   |
|GoogleMaps            |1509403.0|22109763   |
|ScoreCard             |1488383.0|132090921  |
|Samsung               |1487229.0|105977782  |
|Yahoo                 |1476878.0|115860187  |
|SoundCloud            |1464677.0|54831772   |
|AppNexus              |1454357.0|81346194   |
|Mozilla               |1451591.0|47663314   |
|AdobeAnalytics        |1451236.0|84914212   |
|Outbrain              |1436455.0|89944599   |
|Ads                   |1436094.0|72466658   |
|FileHosting           |1432507.0|59313781   |
|Twitter               |1431806.0|146653175  |
|Adobe                 |1429669.0|39906266   |
|MSOnline              |1427816.0|235540756  |
|Bing                  |1417795.0|43191215   |
|Nielsen               |1415461.0|36489201   |
|SmartAds              |1402849.0|56230664   |
|Marketing             |1397620.0|50957692   |
|Viber                 |1390845.0|29490143   |
|Instagram             |1390569.0|378657333  |
|news                  |1389715.0|40579111   |
|General               |1382982.0|60025726   |
|Snapchat              |1376858.0|44906961   |
|Shopping              |1371967.0|115111414  |
|Finance               |1364847.0|33742091   |
|Cedexis               |1360505.0|44488219   |
|Travel                |1359077.0|32555670   |
|Gmail                 |1359063.0|117038529  |
|Optimax               |1356557.0|42438237   |
|Booking               |1352370.0|38540125   |
|Amazon-eu-central-1   |1351725.0|33760409   |
|WEB_ELPAIS_HTTP       |1347653.0|34370646   |
|AOLAds                |1333381.0|32774639   |
|SSL_UDP               |1326676.0|52334824   |
|ChartBeat             |1308842.0|58944303   |
|WEB_RTVE_HTTP         |1305854.0|23104321   |
|Quantcast             |1295561.0|21342605   |
|p0-ipstatp-APP        |1270762.0|22328689   |
|AdultWeb              |1264650.0|40127949   |
|Mediamind             |1261108.0|27462223   |
|xtrapath2-i-APP       |1254765.0|16538933   |
|xtrapath3-i-APP       |1244738.0|13877404   |
|Foursquare            |1222354.0|18514017   |
|Amazon_Video_HTTP     |1202982.0|17330575   |
|WEB_VODAFONE_HTTP     |1201108.0|12913615   |
|Blogger               |1200722.0|6973608    |
|Twitch                |1189772.0|13041786   |
|Sports                |1189262.0|23135823   |
|Spotify               |1189085.0|122196335  |
|WEB_ABC_HTTP          |1182610.0|20821763   |
|WEB_BLES_HTTPS        |1180402.0|15307919   |
|Facebook Video        |1178365.0|152369292  |
|Search_HTTP           |1174588.0|18424888   |
|UniversoOnline        |1171197.0|10314558   |
|IMDB                  |1151592.0|23140179   |
|gllto-glpal-APP       |1147570.0|70981208   |
|xtrapath1-i-APP       |1140703.0|11544381   |
|ASP-Omniture          |1138367.0|12204434   |
|Auto                  |1135702.0|15655311   |
|AtlasAds              |1133629.0|11921584   |
|WindowsUpdate         |1132908.0|15788311   |
|Forbes                |1132559.0|11052020   |
|dm-maliva16-APP       |1129454.0|28462437   |
|Exchange              |1124184.0|235632185  |
|BBC                   |1118888.0|8920381    |
|WEB_MARCA_HTTP        |1112671.0|15757630   |
|Pinterest             |1112375.0|22340619   |
|TikTok                |1109263.0|13055140   |
|WEB_ELMUNDO_HTTP      |1108154.0|12629992   |
|WhatsApp Voice Calling|1107108.0|26776106   |
|Apple                 |1103149.0|549540391  |
|BitDefender           |1101846.0|15488963   |
+----------------------+---------+-----------+

'''