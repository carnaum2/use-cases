def set_paths():
    import os, re, sys

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/PEGA(.*)", pathname).group(1)
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

    from churn_nrt.src.utils.spark_session import get_spark_session
    from churn_nrt.src.utils.date_functions import get_next_dow

    from pyspark.sql.functions import (lit, col, lpad, max as sql_max,
                                       col, lit, concat, from_unixtime,
                                       unix_timestamp, regexp_replace, split)
    import datetime
    import sys


    from churn_nrt.src.utils.spark_session import get_spark_session

    from churn_nrt.src.data.netscout_data import NetscoutData

    sc, spark, sql_context = get_spark_session(app_name="netscout_variables_PEGA",
                    min_n_executors = 20, max_n_executors = 35, n_cores = 12, executor_memory = "10g", driver_memory = "10g")


    path_netscout = "/data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/"


    '''

    def get_last_date(spark):

        last_date = (spark.read.load(path_netscout + "year=2020/").withColumn("year", lit(2020))
            .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
            .select(sql_max(col('mydate')).alias('last_date'))
            .rdd.first()['last_date'])

        return last_date

    closing_day = get_last_date(spark)
    
    '''

    closing_day = sys.argv[1]  # Especificamos aqui el ultimo dia del que hay datos (previamente se busca en hdfs): solucion mejor la funcion anterior pero tarda mucho

    print("[Info] Data until closing_day={}".format(closing_day))

    print('[Info]: Loading netscout data for 1 week...')

    netscout_pega = NetscoutData(spark, incremental_period=7).get_module(closing_day, save_others=True,
                                                                         force_gen=True, option='PEGA')


    print('[Info]: Put variables in extra_info format...')

    final_df = netscout_pega.withColumn('extra_info', concat(lit('Facebook='), col("Facebook"),
                                                                lit(";Internet="), col('Internet'),
                                                                lit(";Amazon="), col('Amazon'),
                                                                lit(";GoogleMaps="), col('GoogleMaps'),
                                                                lit(";GooglePlay="), col('GooglePlay'),
                                                                lit(";YouTube="), col('YouTube'),
                                                                lit(";Whatsapp="), col('Whatsapp'),
                                                                lit(";HBO="), col('HBO'),
                                                                lit(";Netflix="), col('Netflix'),
                                                                lit(";Amazon_Video_HTTP="), col('Amazon_Video_HTTP'),
                                                                lit(";Music="), col('Music'),
                                                                lit(";Spotify="), col('Spotify'),
                                                                lit(";iTunes="), col('iTunes'),
                                                                lit(";TikTok="), col('TikTok'),
                                                                lit(";Booking="), col('Booking'),
                                                                lit(";SoundCloud="), col('SoundCloud'),
                                                                lit(";Snapchat="), col('Snapchat'),
                                                                lit(";Twitter="), col('Twitter'),
                                                                lit(";Instagram="), col('Instagram'),
                                                                lit(";Wallapop="), col('Wallapop'),
                                                                lit(";Skype="), col('Skype'),
                                                                lit(";Telegram="), col('Telegram'),
                                                                lit(";Tripadvisor="), col('Tripadvisor'),
                                                                lit(";Airbnb="), col('Airbnb')))

    print('[Info]: Put variables in model_output format')

    model_output_cols = ["model_name",
                         "executed_at",
                         "model_executed_at",
                         "predict_closing_date",
                         "msisdn",
                         "client_id",
                         "nif",
                         "model_output",
                         "scoring",
                         "prediction",
                         "extra_info",
                         "year",
                         "month",
                         "day",
                         "time"]

    executed_at = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    partition_date = str(get_next_dow(1)) #se envia los lunes
    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[5:7])
    partition_day = int(partition_date[8:10])

    df_model_scores = (final_df
                       .withColumn("model_name", lit("var_apps").cast("string"))
                       .withColumn("executed_at",
                                   from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast(
                                       "string"))
                       .withColumn("model_executed_at", col("executed_at").cast("string"))
                       .withColumn("client_id", lit(""))
                       .withColumn("msisdn", col("msisdn").cast("string"))
                       .withColumn("nif", lit(""))
                       .withColumn("scoring", lit(""))
                       .withColumn("model_output", lit(""))
                       .withColumn("prediction", lit(""))
                       .withColumn("extra_info", col('extra_info').cast("string"))
                       .withColumn("predict_closing_date", lit(closing_day))
                       .withColumn("year", lit(partition_year).cast("integer"))
                       .withColumn("month", lit(partition_month).cast("integer"))
                       .withColumn("day", lit(partition_day).cast("integer"))
                       .withColumn("time",
                                   regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .select(*model_output_cols))

    print('[Info]: Saving data')

    df_model_scores \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save('/data/attributes/vf_es/model_outputs/model_scores/')

    print('[Info]: Dataframe saved in model output format')