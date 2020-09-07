#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import (desc,
                                   asc,
                                   sum as sql_sum,
                                   avg as sql_avg,
                                   isnull,
                                   when,
                                   col,
                                   isnan,
                                   count,
                                   row_number,
                                   lit,
                                   coalesce,
                                   countDistinct,
                                   from_unixtime,
                                   unix_timestamp,
                                   concat,
                                   lpad,
                                   date_format)

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


def get_ref_dates_df(spark, mindate_, maxdate_):

    from churn_nrt.src.utils.date_functions import move_date_n_days, get_diff_days

    max_ = get_diff_days(mindate_, maxdate_)

    dates_ = [(move_date_n_days(mindate_, i), i) for i in range(0, max_ + 1)]

    print dates_

    from pyspark.sql.types import StringType, StructType, StructField, IntegerType

    schema_ = StructType([StructField("date", StringType()), StructField("inc", IntegerType())])

    df_dates = spark.createDataFrame(dates_, schema_).select("date")

    return df_dates

def get_app_consumption_last_n_days(spark, df_msisdn, date_, app = 'all', n = 15):

    app_name_ = ['Instagram', 'Whatsapp', 'Facebook', 'Twitter', 'Youtube'] if app == 'all' else [app]

    from churn_nrt.src.utils.date_functions import move_date_n_days

    # [starting_date, date_) = n days

    init_date_ = move_date_n_days(date_, -n)

    # Reading Netscout data

    df_all_app = spark \
        .read \
        .parquet("/data/udf/vf_es/netscout/dailyMSISDNApplicationName/") \
        .withColumn("event_date", from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')), "yyyyMMdd"))) \
        .filter((col("event_date") >= from_unixtime(unix_timestamp(lit(init_date_), "yyyyMMdd"))) & (col("event_date") < from_unixtime(unix_timestamp(lit(date_), "yyyyMMdd")))) \
        .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9)).filter(col("application_name").isin(app_name_)) \
        .withColumn("data", col("SUM_userplane_upload_bytes_count") + col("SUM_userplane_download_bytes_count"))

    df_all_app.repartition(400)

    df_all_app.cache()

    print "[Info] date_ = " + date_ + " Before filtering - Size of df_all_app: " + str(df_all_app.count())

    # Only those msisdn to be tracked are retained

    df_app = df_all_app \
        .join(df_msisdn, ['msisdn'], 'inner')

    df_app.repartition(400)

    df_app.cache()

    print "[Info] date_ = " + date_ + " After filtering - Size of df_app: " + str(df_app.count())

    num_days = df_app.select("year", "month", "day").distinct().count()

    num_users = df_app.select("msisdn").distinct().count()

    daily_vol_per_user = df_app\
        .select((sql_sum(col("data")).cast("double")/(lit(num_days).cast("double")*lit(num_users).cast("double"))).alias("daily_vol_per_user"))\
        .first()["daily_vol_per_user"]

    print "[Info] app_name_: " + app + " - date_: " + str(date_) + " - consumption: " + str(daily_vol_per_user) + " - num users: " + str(num_users) + " - num days: " + str(num_days)

    return (date_, daily_vol_per_user, num_users, num_days)


if __name__ == "__main__":

    set_paths()

    sc, sparkSession, _ = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

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