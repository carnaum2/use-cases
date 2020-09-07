# coding=utf-8

from common.src.main.python.utils.hdfs_generic import *
import sys
import time
from pyspark.sql.functions import (udf,
                                    col,
                                    decode,
                                    when,
                                    lit,
                                    lower,
                                    concat,
                                    translate,
                                    count,
                                    sum as sql_sum,
                                    max as sql_max,
                                    min as sql_min,
                                    avg as sql_avg,
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
                                    skewness,
                                    kurtosis,
                                    concat_ws,
                                   array,
                                   lpad,
                                   split,
                                   regexp_replace)


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

def get_incorrect_fbb_dxs(spark, closing_day, churn_window):

    from churn_nrt.src.data.customer_base import CustomerBase
    current_base = CustomerBase(spark) \
        .get_module(closing_day, save=False, save_others=False, force_gen=True) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn", "cod_estado_general") \
        .distinct() \
        .repartition(400)

    day_target = move_date_n_days(closing_day, n=churn_window)

    from churn_nrt.src.data_utils.base_filters import keep_active_services

    current_base = keep_active_services(current_base).select("msisdn").distinct()

    target_base = CustomerBase(spark) \
        .get_module(day_target, save=False, save_others=False, force_gen=True) \
        .filter(col('rgu') == 'fbb') \
        .select("msisdn", "cod_estado_general") \
        .distinct()

    target_base = keep_active_services(target_base).select("msisdn").distinct()

    # It is not clear when the disconnection occurs. Thus, the nid point between both dates is assigned

    portout_date = move_date_n_days(closing_day, int(churn_window/2))

    churn_base = current_base \
        .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left") \
        .filter(col("tmp").isNull()) \
        .select("msisdn") \
        .withColumn("label_dx", lit(1.0)) \
        .distinct() \
        .withColumn('portout_date_dx', from_unixtime(unix_timestamp(lit(portout_date), 'yyyyMMdd')))

    print("[Info get_fbb_dxs] - DXs for FBB services during the period: " + closing_day + " - " + day_target + ": " + str(churn_base.count()))

    return churn_base


def get_campaign_results(spark, model_date, camp_date_init, camp_date_end, campaign):

    # Current datetime
    import datetime as dt
    current_date = dt.datetime.today().strftime('%Y%m%d %H:%M:%S')

    # Parsing the dates
    model_year = model_date[0:4]
    model_month = model_date[4:6]
    model_day = model_date[6:8]

    camp_date = camp_date_init

    #### Lectura de Datos ####
    ###########################

    ## Delivery ##

    start_time_delivery = time.time()

    df_delivery = spark \
        .read \
        .parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml') \
        .filter((col('month') == int(model_month)) & (col('day') == int(model_day)) & (col('year') == int(model_year))) \
        .withColumn('risk', split(col('extra_info'), ';').getItem(1).substr(17, 1))\
        .filter(col('risk')==1)\
        .withColumnRenamed('nif', 'nif_cliente')\
        .repartition(400)

    df_delivery.cache()

    car_date = str(df_delivery.select('predict_closing_date').first()["predict_closing_date"])

    from churn_nrt.src.utils.date_functions import move_date_n_days, get_diff_days

    days_from_car_to_port = get_diff_days(car_date, end_port_date)

    print("[Info] Delivery load (minutes): " + str((time.time() - start_time_delivery) / 60.0))

    print '[Info] Fecha CAR: ' + car_date
    print '[Info] Fecha Campaign: ' + camp_date
    print '[Info] Fecha Model: ' + model_date
    print '[Info] Fecha Fin Portas: ' + end_port_date

    ## Cartera para el día en que se ha tomado la foto del CAR ##

    start_time_cartera_car = time.time()

    from churn_nrt.src.data.customer_base import CustomerBase
    from churn_nrt.src.data_utils.base_filters import keep_active_services

    df_cartera_car = keep_active_services(
        CustomerBase(spark).get_module(car_date, save=False, save_others=False, force_gen=True)) \
        .select("nif_cliente", "msisdn") \
        .distinct()\
        .repartition(400)

    df_cartera_car.cache()

    print("[Info] Cartera CAR - Size: " + str(df_cartera_car.count()) + " - Load (minutes): " + str((time.time() - start_time_cartera_car) / 60.0))

    ## Cartera para el día en que se lanzó la campaña ##

    start_time_cartera_camp = time.time()

    df_cartera_camp = keep_active_services(
        CustomerBase(spark).get_module(camp_date, save=False, save_others=False, force_gen=True)) \
        .select("nif_cliente", "msisdn") \
        .distinct()\
        .repartition(400)

    df_cartera_camp.cache()

    print("[Info] Cartera - Size: " + str(df_cartera_camp.count()) + " - Camp load (minutes): " + str((time.time() - start_time_cartera_camp) / 60.0))

    ## Campaña ##

    start_time_camp = time.time()

    df_campaign = (spark.read.table('raw_es.campaign_nifcontacthist')
                   .withColumn("camp_date", regexp_replace(substring(col('UpdateDateTime'), 0, 10), '-', ''))
                   .filter((from_unixtime(unix_timestamp(col('camp_date'), 'yyyyMMdd')) >= from_unixtime(unix_timestamp(lit(camp_date_init), 'yyyyMMdd'))) &
                           (from_unixtime(unix_timestamp(col('camp_date'), 'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(camp_date_end), 'yyyyMMdd'))) &
                           (col('CampaignCode').isin('AUTOMSEM_PXXXT_TRIGG_SERVICIO')))
                   .withColumn('Grupo',
                               when(col('cellcode').startswith('CU'), 'Universal')
                               .when(col('cellcode').startswith('CC'), 'Control')
                               .otherwise('Target'))
                   .select('CIF_NIF', 'CampaignCode', 'Grupo', 'camp_date', 'year', 'month', 'day')
                   .withColumnRenamed('CIF_NIF', 'nif_cliente'))

    print("[Info] Camp - Size: " + str(df_campaign.count()) + " - Load (minutes): " + str((time.time() - start_time_camp) / 60.0))

    ############# Getting the labels ###############

    # 1. Complete churn: mob port, fix port or mob port

    start_time_correct_churn = time.time()

    from churn_nrt.src.data.sopos_dxs import Target

    df_churn_camp = Target(spark, 30) \
        .get_module(camp_date, save=False, save_others=False, force_gen=True) \
        .select("nif_cliente", "label") \
        .distinct().withColumnRenamed("label", "churn")

    df_churn_camp = df_churn_camp.cache()

    df_churn_car = Target(spark, days_from_car_to_port) \
        .get_module(car_date, save=False, save_others=False, force_gen=True) \
        .select("nif_cliente", "label") \
        .distinct().withColumnRenamed("label", "churn")

    df_churn_car = df_churn_car.cache()

    print("[Info] Correct churn - Size (camp): " + str(df_churn_camp.count()) + " - Size (CAR): " + str(df_churn_car.count()) + " - Load (minutes): " + str((time.time() - start_time_correct_churn) / 60.0))

    # 2. Portability: mob port or fix port

    start_time_port = time.time()

    from churn_nrt.src.data.sopos_dxs import MobPort, FixPort

    df_mob_port_camp = MobPort(spark, 30) \
        .get_module(camp_date, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_mob") \
        .distinct() \
        .withColumnRenamed("label_mob", "port")

    df_mob_port_camp.cache()

    print "[Info] df_mob_port_camp = " + str(df_mob_port_camp.count())

    df_fix_port_camp = FixPort(spark, 30) \
        .get_module(camp_date, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_srv") \
        .distinct() \
        .withColumnRenamed("label_srv", "port")

    df_fix_port_camp.cache()

    print "[Info] df_fix_port_camp = " + str(df_fix_port_camp.count())

    df_port_camp = df_cartera_camp \
        .join(df_mob_port_camp.union(df_fix_port_camp).distinct(), ['msisdn'], 'inner') \
        .select("nif_cliente", "port") \
        .distinct()

    df_port_camp.cache()

    print "[Info] df_port_camp = " + str(df_port_camp.count())

    df_mob_port_car = MobPort(spark, days_from_car_to_port) \
        .get_module(car_date, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_mob") \
        .distinct() \
        .withColumnRenamed("label_mob", "port")

    df_mob_port_car.cache()

    print "[Info] df_mob_port_car = " + str(df_mob_port_car.count())

    df_fix_port_car = FixPort(spark, days_from_car_to_port) \
        .get_module(car_date, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_srv") \
        .distinct() \
        .withColumnRenamed("label_srv", "port")

    df_fix_port_car.cache()

    print "[Info] df_fix_port_car = " + str(df_fix_port_car.count())

    df_port_car = df_cartera_car \
        .join(df_mob_port_car.union(df_fix_port_car).distinct(), ['msisdn'], 'inner') \
        .select("nif_cliente", "port") \
        .distinct()

    df_port_car.cache()

    print "[Info] df_port_car = " + str(df_port_car.count())

    print("[Info] Port load (minutes): " + str((time.time() - start_time_port) / 60.0))

    ################# LABELING ###############

    # 1. Base

    start_time_lab_base = time.time()

    df_cartera_car_lab = df_cartera_car \
        .select("nif_cliente") \
        .distinct() \
        .join(df_churn_car, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
        .join(df_port_car, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_cartera_car_lab.cache()

    print "[Info] df_cartera_car_lab = " + str(df_cartera_car_lab.count())

    df_cartera_camp_lab = df_cartera_camp \
        .select("nif_cliente") \
        .distinct() \
        .join(df_churn_camp, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
        .join(df_port_camp, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_cartera_camp_lab.cache()

    print "[Info] df_cartera_camp_lab = " + str(df_cartera_camp_lab.count())

    print("[Info] Base labeling (minutes): " + str((time.time() - start_time_lab_base) / 60.0))

    # 2. Delivery

    start_time_lab_delivery = time.time()

    df_delivery_car_lab = df_delivery \
          .join(df_churn_car, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
          .join(df_port_car, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_delivery_car_lab.cache()

    print "[Info] df_delivery_car_lab = " + str(df_delivery_car_lab.count())

    df_delivery_camp_lab = df_delivery \
          .join(df_churn_camp, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
         .join(df_port_camp, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_delivery_camp_lab.cache()

    print "[Info] df_delivery_camp_lab = " + str(df_delivery_camp_lab.count())

    print("[Info] Delivery labeling (minutes): " + str((time.time() - start_time_lab_delivery) / 60.0))

    # 3. Campaign

    start_time_lab_camp = time.time()

    df_campaign_car_lab = df_campaign \
        .join(df_churn_car, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
        .join(df_port_car, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_campaign_car_lab.cache()

    print "[Info] df_campaign_car_lab = " + str(df_campaign_car_lab.count())

    df_campaign_camp_lab = df_campaign \
        .join(df_churn_camp, ["nif_cliente"], "left").na.fill({"churn": 0.0}) \
        .join(df_port_camp, ["nif_cliente"], "left").na.fill({"port": 0.0})

    df_campaign_camp_lab.cache()

    print "[Info] df_campaign_camp_lab = " + str(df_campaign_camp_lab.count())

    print("[Info] Camp labeling (minutes): " + str((time.time() - start_time_lab_camp) / 60.0))

    ################ CHURN EVAL ################

    aggs = [count("*").cast("double").alias("num_customers"),
            sql_sum(col('churn')).cast("double").alias('num_churners'),
            sql_sum(col('port')).cast("double").alias('num_churners_port')]

    cols = ['date', 'num_customers', 'num_churners', 'num_churners_port','churn_rate', 'churn_port_rate']

    # 1. Base

    # 1.1. Dict with the cols for the car_date

    start_time_eval_base_car = time.time()

    df_cartera_car_lab_agg = df_cartera_car_lab \
        .withColumn('population', lit('cartera')) \
        .withColumn('date', lit(car_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_cartera_car_lab_agg = df_cartera_car_lab_agg.withColumnRenamed(c, c + '_car')

    dict_cartera_car_lab_agg = df_cartera_car_lab_agg.first().asDict()

    print("[Info] Eval base CAR (minutes): " + str((time.time() - start_time_eval_base_car) / 60.0))

    # 1.2. Dict with the cols for the camp_date

    start_time_eval_base_camp = time.time()

    df_cartera_camp_lab_agg = df_cartera_camp_lab \
        .withColumn('population', lit('cartera')) \
        .withColumn('date', lit(camp_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_cartera_camp_lab_agg = df_cartera_camp_lab_agg.withColumnRenamed(c, c + '_camp')

    dict_cartera_camp_lab_agg = df_cartera_camp_lab_agg.first().asDict()

    dict_cartera_car_lab_agg.update(dict_cartera_camp_lab_agg)

    dict_cartera_agg = dict_cartera_car_lab_agg

    for (k, v) in dict_cartera_agg.items():
        print "[Info] Cartera - " + k + ": " + str(v)

    print("[Info] Eval base Camp (minutes): " + str((time.time() - start_time_eval_base_camp) / 60.0))

    # 2. Delivery

    # 2.1. Dict with the cols for the car_date

    start_time_eval_delivery_car = time.time()

    df_delivery_car_lab_agg = df_delivery_car_lab \
        .withColumn('population', lit('delivery')) \
        .withColumn('date', lit(car_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_delivery_car_lab_agg = df_delivery_car_lab_agg.withColumnRenamed(c, c + '_car')

    dict_delivery_car_lab_agg = df_delivery_car_lab_agg.first().asDict()

    print("[Info] Eval delivery CAR (minutes): " + str((time.time() - start_time_eval_delivery_car) / 60.0))

    # 2.2. Dict with the cols for the camp_date

    start_time_eval_delivery_camp = time.time()

    df_delivery_camp_lab_agg = df_delivery_camp_lab \
        .withColumn('population', lit('delivery')) \
        .withColumn('date', lit(camp_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_delivery_camp_lab_agg = df_delivery_camp_lab_agg.withColumnRenamed(c, c + '_camp')

    dict_delivery_camp_lab_agg = df_delivery_camp_lab_agg.first().asDict()

    dict_delivery_car_lab_agg.update(dict_delivery_camp_lab_agg)

    dict_delivery_agg = dict_delivery_car_lab_agg

    for (k, v) in dict_delivery_agg.items():
        print "[Info] Delivery - " + k + ": " + str(v)

    print("[Info] Eval delivery Camp (minutes): " + str((time.time() - start_time_eval_delivery_camp) / 60.0))

    # 3. Campaign

    # 3.1. Target group

    # 3.1.1. Dict with the cols for the car_date

    start_time_eval_target_car = time.time()

    df_target_car_lab_agg = df_campaign_car_lab.filter(col('Grupo') == 'Target') \
        .withColumn('population', lit('target')) \
        .withColumn('date', lit(car_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_target_car_lab_agg = df_target_car_lab_agg.withColumnRenamed(c, c + '_car')

    dict_target_car_lab_agg = df_target_car_lab_agg.first().asDict()

    print("[Info] Eval Target CAR (minutes): " + str((time.time() - start_time_eval_target_car) / 60.0))

    # 3.1.2. Dict with the cols for the camp_date

    start_time_eval_target_camp = time.time()

    df_target_camp_lab_agg = df_campaign_camp_lab.filter(col('Grupo') == 'Target') \
        .withColumn('population', lit('target')) \
        .withColumn('date', lit(camp_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_target_camp_lab_agg = df_target_camp_lab_agg.withColumnRenamed(c, c + '_camp')

    dict_target_camp_lab_agg = df_target_camp_lab_agg.first().asDict()

    dict_target_car_lab_agg.update(dict_target_camp_lab_agg)

    dict_target_agg = dict_target_car_lab_agg

    for (k, v) in dict_target_agg.items():
        print "[Info] Target - " + k + ": " + str(v)

    print("[Info] Eval Target Camp (minutes): " + str((time.time() - start_time_eval_target_camp) / 60.0))

    # 3.2. Control group

    # 3.2.1. Dict with the cols for the car_date

    start_time_eval_control_car = time.time()

    df_control_car_lab_agg = df_campaign_car_lab.filter(col('Grupo') == 'Control') \
        .withColumn('population', lit('target')) \
        .withColumn('date', lit(car_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_control_car_lab_agg = df_control_car_lab_agg.withColumnRenamed(c, c + '_car')

    dict_control_car_lab_agg = df_control_car_lab_agg.first().asDict()

    print("[Info] Eval Control CAR (minutes): " + str((time.time() - start_time_eval_control_car) / 60.0))

    # 3.2.2. Dict with the cols for the camp_date

    start_time_eval_control_camp = time.time()

    df_control_camp_lab_agg = df_campaign_camp_lab.filter(col('Grupo') == 'Control') \
        .withColumn('population', lit('control')) \
        .withColumn('date', lit(camp_date)) \
        .groupBy('population', 'date') \
        .agg(*aggs) \
        .withColumn('churn_rate', col("num_churners") / col("num_customers")) \
        .withColumn('churn_port_rate', col("num_churners_port") / col("num_customers"))

    for c in cols:
        df_control_camp_lab_agg = df_control_camp_lab_agg.withColumnRenamed(c, c + '_camp')

    dict_control_camp_lab_agg = df_control_camp_lab_agg.first().asDict()

    dict_control_car_lab_agg.update(dict_control_camp_lab_agg)

    dict_control_agg = dict_control_car_lab_agg

    for (k, v) in dict_control_agg.items():
        print "[Info] Control - " + k + ": " + str(v)

    print("[Info] Eval Control Camp (minutes): " + str((time.time() - start_time_eval_control_camp) / 60.0))

    # Combining all the dicts to get one df

    dict_list = [dict_cartera_agg, dict_delivery_agg, dict_target_agg, dict_control_agg]
    dict_tracing_result = {}
    for k in dict_cartera_agg.iterkeys():
        dict_tracing_result[k] = [m[k] for m in dict_list]

    import pandas as pd

    pd_tracking_result = pd.DataFrame(dict_tracing_result)

    df_tracking_result = spark.createDataFrame(pd_tracking_result).withColumn('executed_at', lit(current_date))

    df_tracking_result.show()

    ############################################
    # Profiling
    ############################################

    # Range of indices/positions in the delivery for customers in the campaign

    start_time_profiling = time.time()

    pd_delivdf = df_delivery.select('nif_cliente', 'scoring').toPandas()

    pd_delivdf = pd_delivdf.sort_values(by='scoring', ascending=False)

    pd_delivdf['position'] = range(1, len(pd_delivdf) + 1)

    ord_delivdf = spark.createDataFrame(pd_delivdf)

    ord_campaigndf = df_campaign \
        .select('nif_cliente', 'Grupo') \
        .distinct() \
        .join(ord_delivdf, ['nif_cliente'], 'inner') \
        .groupBy('Grupo') \
        .agg(sql_min('position').alias('min_position'), sql_max('position').alias('max_position'))\
        .withColumnRenamed('Grupo', 'population')\
        .withColumn('population', lower(col('population')))\
        .filter(col('population').isin('control', 'target'))

    ord_campaigndf.show()

    print('[Info Campaign Tracking] Min and max positions for each group in the campaign for ' + str(model_date) + ' showed above')

    print("[Info] Profiling (minutes): " + str((time.time() - start_time_profiling) / 60.0))

    # Adding min/max positions to result DF

    df_tracking_result = df_tracking_result\
        .join(ord_campaigndf, ['population'], 'left')\
        .na.fill({'min_position': -1, 'max_position': -1})

    return (current_date, car_date, df_tracking_result, df_delivery_car_lab, df_delivery_camp_lab)

def rename_columns(df, prefix, sep = "_", nocols = []):

    new_cols = ["".join([prefix, sep, col_]) if ((col_ in nocols) == False) else col_ for col_ in df.columns]
    rendf = df.toDF(*new_cols)

    return rendf

if __name__ == "__main__":

    set_paths()

    ############### 0. Spark ################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("campaign_tracking_trigger_services")

    ############### 0. Input arguments #################

    model_date = sys.argv[1]
    store_ = sys.argv[2]

    if (store_.lower() == 's'):
        print "[Info] Running the tracker for model_date = " + model_date + ' - Results will be stored'
    else:
        print "[Info] Running the tracker for model_date = " + model_date + ' - Results will not be stored'

    camp_dates = (spark
                  .read
                  .table('raw_es.campaign_nifcontacthist')
                  .withColumn("camp_date", regexp_replace(substring(col('UpdateDateTime'), 0, 10), '-', ''))
                  .filter((from_unixtime(unix_timestamp(col("camp_date"), 'yyyyMMdd')) >= from_unixtime(unix_timestamp(lit(model_date), 'yyyyMMdd'))) & (col('CampaignCode') == 'AUTOMSEM_PXXXT_TRIGG_SERVICIO'))
                  .select("camp_date").distinct().orderBy(asc("camp_date"))
                  .rdd.map(lambda r: r["camp_date"]).collect())

    camp_dates.sort()
    print('Camp Days',camp_dates)
    camp_date = str(camp_dates[0])

    from churn_nrt.src.utils.date_functions import get_diff_days

    valid_camp_dates = [c for c in camp_dates if get_diff_days(model_date, str(c)) <= 5]

    camp_date_init = min(valid_camp_dates)
    camp_date_end = max(valid_camp_dates)

    model_year = str(int(model_date[0:4]))
    model_month = str(int(model_date[4:6]))
    model_day = str(int(model_date[6:8]))

    camp_year = str(int(camp_date[0:4]))
    camp_month = str(int(camp_date[4:6]))
    camp_day = str(int(camp_date[6:8]))

    path = '/data/udf/vf_es/churn/triggers/campaign_tracker/trigger_services'

    # Computing end_port_date

    from churn_nrt.src.utils.date_functions import move_date_n_days

    end_port_date = move_date_n_days(camp_date, n=30)

    ########## Campaign results ############

    start_time_camp_results = time.time()

    (current_date, car_date, df_tracking_result, df_delivery_car_lab, df_delivery_camp_lab) = get_campaign_results(spark, model_date, camp_date_init, camp_date_end, 'AUTOMSEM_PXXXT_TRIGG_SERVICIO')

    ################# 1. Campaign tracking is stored (partition by year, month, day corresponding with the camp date) ################

    if (store_.lower() == 's'):

        df_tracking_result\
            .withColumn("year", lit(camp_year))\
            .withColumn("month", lit(camp_month))\
            .withColumn("day", lit(camp_day))\
            .write\
            .partitionBy('year', 'month', 'day')\
            .mode("append") \
            .format("parquet").save(path + '/tracking')

    df_tracking_result.show(500, False)

    print("[Info] Campaign results obtained and saved (minutes): " + str((time.time() - start_time_camp_results) / 60.0))

    ############### 2. Lift ################

    start_time_lift_results = time.time()

    from churn_nrt.src.projects_utils.models.modeler import get_cumulative_churn_rate_fix_step

    lift_delivery_car_churn = get_cumulative_churn_rate_fix_step(spark, df_delivery_car_lab, step_=2500,
                                                                 ord_col="scoring", label_col="churn")
    lift_delivery_car_churn = rename_columns(lift_delivery_car_churn, "churn_car", sep="_", nocols=['bucket']) \
        .select('bucket', 'churn_car_num_churners', 'churn_car_churn_rate', 'churn_car_cum_num_churners','churn_car_cum_churn_rate')

    lift_delivery_car_port = get_cumulative_churn_rate_fix_step(spark, df_delivery_car_lab, step_=2500,
                                                                ord_col="scoring", label_col="port")
    lift_delivery_car_port = rename_columns(lift_delivery_car_port, "port_car", sep="_", nocols=['bucket']) \
        .select('bucket', 'port_car_num_churners', 'port_car_churn_rate', 'port_car_cum_num_churners',
                'port_car_cum_churn_rate')

    lift_delivery_car = lift_delivery_car_churn.join(lift_delivery_car_port, ['bucket'], 'inner')

    lift_delivery_car.show(500, False)

    print "[Info] lift_delivery_car showed above"

    lift_delivery_camp_churn = get_cumulative_churn_rate_fix_step(spark, df_delivery_camp_lab, step_=2500,
                                                                  ord_col="scoring", label_col="churn")
    lift_delivery_camp_churn = rename_columns(lift_delivery_camp_churn, "churn_camp", sep="_", nocols=['bucket']) \
        .select('bucket', 'churn_camp_num_churners', 'churn_camp_churn_rate', 'churn_camp_cum_num_churners','churn_camp_cum_churn_rate')

    lift_delivery_camp_port = get_cumulative_churn_rate_fix_step(spark, df_delivery_camp_lab, step_=2500,
                                                                 ord_col="scoring", label_col="port")
    lift_delivery_camp_port = rename_columns(lift_delivery_camp_port, "port_camp", sep="_", nocols=['bucket']) \
        .select('bucket', 'port_camp_num_churners', 'port_camp_churn_rate', 'port_camp_cum_num_churners',
                'port_camp_cum_churn_rate')

    lift_delivery_camp = lift_delivery_camp_churn.join(lift_delivery_camp_port, ['bucket'], 'inner')

    lift_delivery_camp.show(500, False)

    print "[Info] lift_delivery_camp showed above"

    if(store_.lower()=='s'):

        lift_delivery = lift_delivery_car\
            .join(lift_delivery_camp, ['bucket'], "inner")\
            .withColumn("date_camp", lit(camp_date))\
            .withColumn("date_car", lit(car_date))\
            .withColumn("year", lit(camp_year)) \
            .withColumn("month", lit(camp_month)) \
            .withColumn("day", lit(camp_day)) \
            .withColumn('executed_at', lit(current_date))\
            .write \
            .partitionBy('year', 'month', 'day') \
            .mode("append") \
            .format("parquet").save(path + '/lift')

    print("[Info] Lift results obtained and saved (minutes): " + str((time.time() - start_time_lift_results) / 60.0))