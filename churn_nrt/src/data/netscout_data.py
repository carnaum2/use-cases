from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.utils.date_functions import days_range, move_date_n_days

from pyspark.sql.functions import (udf,
                                   col,
                                   when,
                                   lit,
                                   lower,
                                   count,
                                   expr,
                                   sum as sql_sum,
                                   lag,
                                   unix_timestamp,
                                   from_unixtime,
                                   datediff,
                                   desc,
                                   countDistinct,
                                   asc,
                                   row_number,
                                   upper,
                                   coalesce,
                                   concat,
                                   lpad,
                                   trim,
                                   split,
                                    length,
                                   regexp_replace,
                                   substring
                                   )
import pandas as pd

# Netscout path

TABLE_NETSCOUT = "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/"


def get_grouped_apps(option):

    if option == 'PEGA':
        return {'Facebook': ["Facebook", "Facebook Video", "FacebookMessages"],
                "Internet": ['Google', 'Yahoo', 'Mozilla', "Bing"],
                "Netflix": ["Netflix", "NetflixVideo"],
                "Amazon": ["Amazon"],
                "GoogleMaps": ["GoogleMaps"],
                "GooglePlay": ["GooglePlay"],
                "YouTube": ["YouTube"],
                "Whatsapp": ["Whatsapp"],
                "HBO": ["HBO"],
                "Amazon_Video_HTTP": ["Amazon_Video_HTTP"],
                "Music": ["Music"],
                "Spotify": ["Spotify"],
                "iTunes": ["iTunes"],
                "TikTok": ["TikTok"],
                "Booking": ["Booking"],
                "SoundCloud": ["SoundCloud"],
                "Snapchat": ["Snapchat"],
                "Twitter": ["Twitter"],
                "Instagram": ["Instagram"],
                "Wallapop": ["Wallapop"],
                "Skype": ["Skype"],
                "Telegram": ["Telegram"],
                "Tripadvisor": ["Tripadvisor"],
                "Airbnb": ["Airbnb"]
                }

    if option == 'DIGITAL':

        return {'social_networks': ["Twitter", "Facebook", "Facebook Video", "FacebookMessages", "TikTok", "Twitter",
                                    "Instagram", "Pinterest", "Periscope", "SocialNetwork", "Flickr", "Tinder",
                                    "Linkedin"],
                "google_apps": ['GoogleAnalytics', "GoogleDrive", "GoogleMaps", "Gmail", "GoogleXlate", "GoogleAE",
                                "GoogleEarth", "GooglePlus", "GoogleVideo"],
                "messaging": ['Whatsapp', 'Snapchat', "Viber", "WhatsApp Voice Calling", "Skype", "Telegram", "Chats",
                              "Line", "WeChat"],
                "internet": ['Google', 'Yahoo', 'Mozilla', "Bing"],
                "travel": ['Booking', "Travel", "Vueling", "Tripadvisor", "Airbnb", "Renfe"],
                "music": ['Youtube', "Audio", "Spotify", "iTunes", "Music"],
                "e_commerce": ['Amazon', "Ebay", "Deliveroo"],
                "technology": ["Samsung", "Apple", "Technology", "Android", "BlackBerry"],
                "bank": ["BBVA_URL", "Paypal", "BancoSantander"],
                "news": ["WEB_ELPAIS_HTTP", "news", "WEB_ABC_HTTP", "WEB_ELMUNDO_HTTP", "WEB_ELPAIS_HTTPS",
                         "WEB_ABC_HTTPS", "NYTimes", "BBC"],
                "streaming": ["HBO", "Amazon_Video_HTTP", "WEB_RTVE_HTTP", "Netflix", "Mediaset", "VideoStreaming",
                              "es-antena3-APP", "NetFlixVideo"],
                "sports": ["Sports", "WEB_MARCA_HTTP", "WEB_MARCA_HTTPS"]}


def get_app_list(option):
    '''

    :return: list with all the apps considered in the grouping dict (used for filtering)
    '''

    from functools import reduce
    return reduce(lambda a, b: a + b, get_grouped_apps(option).values())


def get_one_day_netscout(spark, closing_day, option):
    # Get netscout dataframe aggregated for one day
    # Note that the resulting DF is not filtered by using the base. As a result, you may find
    # msisdn that are not in the base. The reason is that a significant overload would be added
    # to the process for every day, what may result in significant computing time when several days are
    # analyzed

    year_ = closing_day[0:4]
    month_ = str(int(closing_day[4:6]))
    day_ = str(int(closing_day[6:8]))

    # Read netscout data (only applications contained in the list of apps)

    df_netscout = (spark.read.parquet(TABLE_NETSCOUT + "year=" + year_ + "/month=" + month_ + "/day=" + day_) \
                   .where(col('application_name').isin(get_app_list(option))) \
                   .where(~col('subscriber_msisdn').isNull()) \
                   .withColumn('msisdn', when(substring(col('subscriber_msisdn'), 1, 2) == '34',
                                              substring(col('subscriber_msisdn'), 3, 9)).otherwise(
        col('subscriber_msisdn'))))

    # Add column of total exchange data volume by app

    df_netscout = df_netscout \
        .withColumn('data_exchange_volume', col('userplane_upload_bytes_count') + col('userplane_download_bytes_count')) \
        .where(col('data_exchange_volume') > 524288) \
        .select('msisdn', 'application_name', 'data_exchange_volume')

    # Get grouped apps

    dict_apps = get_grouped_apps(option)

    if option == 'PEGA':
        df_netscout = df_netscout \
            .withColumn('app_group', when(col('application_name').isin(dict_apps['Facebook']), 'Facebook') \
                        .otherwise(when(col('application_name').isin(dict_apps['Internet']), 'Internet') \
                                   .otherwise(when(col('application_name').isin(dict_apps['Netflix']), 'Netflix') \
                                              .otherwise(col('application_name')))))

    if option == 'DIGITAL':
        df_netscout = df_netscout \
            .withColumn('app_group', when(col('application_name').isin(dict_apps['social_networks']), 'social_networks') \
                        .otherwise(when(
            col('application_name').isin(dict_apps['google_apps']),
            'google_apps')
            .otherwise(when(
            col('application_name').isin(dict_apps['streaming']),
            'streaming').otherwise(
            when(col('application_name').isin(dict_apps['news']),
                 'news').otherwise(when(col('application_name').isin(
                dict_apps['technology']), 'technology').otherwise(when(
                col('application_name').isin(dict_apps['bank']),
                'bank').otherwise(when(
                col('application_name').isin(dict_apps['travel']),
                'travel').otherwise(when(
                col('application_name').isin(dict_apps['sports']),
                'sports').otherwise(
                when(col('application_name').isin(
                    dict_apps['e_commerce']), 'e_commerce').otherwise(when(col('application_name').isin(
                    dict_apps['messaging']), 'messaging').otherwise(when(
                    col('application_name').isin(
                        dict_apps['music']), 'music').otherwise(when(
                    col('application_name').isin(dict_apps['internet']),
                    'internet')))))))))))))

    # Data exchange volume by app group: one column by app

    groups = dict_apps.keys()

    df_netscout_final = df_netscout \
        .groupby('msisdn') \
        .pivot("app_group", groups) \
        .sum("data_exchange_volume")

    df_netscout_final = df_netscout_final.na.fill(0)

    return df_netscout_final


def get_agg_netscout(spark, start_dt, end_dt, option):
    # Get days between two dates provided

    days = days_range(start_dt, end_dt)

    # Get dataframe aggregated for each of the dates

    daily_netscout_list = [get_one_day_netscout(spark, day, option) for day in days]

    aggs_list = [sql_sum(col_).alias(col_) for col_ in daily_netscout_list[0].columns if col_ != "msisdn"]

    # Append dataframes

    from functools import reduce

    df_final_grouped = reduce(lambda a, b: a.union(b).groupBy("msisdn").agg(*aggs_list), daily_netscout_list)

    return df_final_grouped


class NetscoutData(DataTemplate):
    INCREMENTAL_PERIOD = None  # days

    def __init__(self, spark, incremental_period):
        self.INCREMENTAL_PERIOD = incremental_period
        DataTemplate.__init__(self, spark, "netscout_digital/{}".format(self.INCREMENTAL_PERIOD))

    def build_module(self, closing_day, save_others, option, **kwargs):
        starting_day = move_date_n_days(closing_day, n=-self.INCREMENTAL_PERIOD)

        print("[NetscoutData] build_module | starting_day={} closing_day={}".format(starting_day, closing_day))

        df_netscout_agg = get_agg_netscout(self.SPARK, starting_day, closing_day, option)

        return df_netscout_agg

    def get_metadata(self):
        dict_apps = get_grouped_apps(option)

        groups = dict_apps.keys()

        na_map = dict([(g, 0.0) for g in groups])

        feats = na_map.keys()

        na_vals = [str(x) for x in na_map.values()]

        cat_feats = []

        data = {'feature': feats, 'imp_value': na_vals}

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
                       .withColumn('source', lit('netscout_digital'))
                       .withColumn('type', lit('numeric'))
                       .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type')))
                       .withColumn('level', lit('msisdn')))

        return metadata_df


