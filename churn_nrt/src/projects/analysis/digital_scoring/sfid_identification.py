#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import desc, asc, sum as sql_sum, avg as sql_avg, max as sql_max, isnull, when, col, isnan, count, row_number, lit, coalesce, concat, lpad, unix_timestamp, from_unixtime, greatest

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
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

def get_base_sfid(spark, start_date, end_date, verbose = True):

    '''
    spark:
    start_date: starting date of the time window considered for the observation of purchasing orders
    end_date: ending date of the time window considered for the observation of purchasing orders

    The function computes a structure with the following fields:
    - num_cliente: customer id (several msisdn may be associated with the same num_cliente)
    - num_buy_orders: for a given num_cliente, number of purchasing orders observed between start_date and end_date
    - num_digital_buy_orders: for a given num_cliente, number of purchasing orders observed between start_date and end_date by means of a digital channel
    - pcg_digital_orders: percentage of digital purchasing orders in the interval between start_date and end_date
    '''


    # 1. Class order: this info is required to retain purchasing orders
    class_order_df = spark.read.table("raw_es.cvm_classesorder") \
        .select("OBJID", "X_CLASIFICACION", "year", "month", "day")

    from pyspark.sql import Window

    w_orderclass = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

    class_order_df = class_order_df \
        .filter(from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                             'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(end_date), 'yyyyMMdd'))) \
        .withColumn("rowNum", row_number().over(w_orderclass)) \
        .filter(col('rowNum') == 1)

    class_order_df.repartition(400)

    class_order_df.cache()

    # 2. CRM Order: all the orders

    window = Window.partitionBy("NUM_CLIENTE", "INSTANCIA_SRV").orderBy(asc("FECHA_WO_COMPLETA"), asc("HORA_CIERRE"))

    orders_crm_df = (spark
                     .read
                     .table("raw_es.customerprofilecar_ordercrmow")
                     .filter(
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) >= from_unixtime(unix_timestamp(lit(start_date), 'yyyyMMdd')))
        &
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(end_date), 'yyyyMMdd')))
    )
                     .select("NUM_CLIENTE", "INSTANCIA_SRV", "NUM_VEND", "WDQ01", "WDQ02", "COD_SERVICIO",
                             "FECHA_WO_COMPLETA", "HORA_CIERRE", "CLASE_ORDEN", "ESTADO")
                     .withColumnRenamed("CLASE_ORDEN", "OBJID")
                     .filter(col("ESTADO") == "CP").withColumn("row_nb", row_number().over(window))
                     .filter((col("row_nb") == 1) & (col("WDQ01") == 0) & (col("WDQ02") == 1))
                     )

    # 3. Base: customer IDs found in the base on end_date

    from churn_nrt.src.data.customer_base import CustomerBase

    base_df = CustomerBase(spark) \
        .get_module(end_date, save=False, save_others=False, force_gen=True) \
        .select("NUM_CLIENTE") \
        .distinct()

    base_df.repartition(400)

    base_df.cache()

    if verbose:
        print "Number of customers in the base: " + str(base_df.count())

    # 4. Filtering the orders by using the base: only those orders from customers in the base on end_date are reatined

    filt_orders_crm_df = orders_crm_df.join(base_df, ['NUM_CLIENTE'], 'inner')

    filt_orders_crm_df.repartition(400)

    filt_orders_crm_df.cache()

    # 5. Join orders and class: Field X_CLASIFICACION is added to the orders (X_CLASIFICACION specifies the order type)

    order_crm_sfid = filt_orders_crm_df \
        .join(class_order_df, ['OBJID'], 'left') \
        .filter(col("X_CLASIFICACION").isin('Instalacion', 'Reconexion', 'Aumento')) \
        .distinct()

    order_crm_sfid.repartition(400)

    order_crm_sfid.cache()

    if verbose:
        print "[Info] Volume: " + str(order_crm_sfid.count()) + " - Num distinct OBJID: " + str(order_crm_sfid.select("OBJID").distinct().count()) + " - Num distinct NUM_CLIENTE: " + str(order_crm_sfid.select("NUM_CLIENTE").distinct().count())

    # 6. Labeling NUM_VEND as digital or not

    digital_sfid_df = spark.read.csv("/user/jmarcoso/data/lista_sfid_digital.csv", header=True, sep=";") \
        .filter(col("VERTICAL") == "DIGITAL").select("SFID").distinct().withColumn("flag_sfid_digital", lit(1.0))

    order_crm_sfid_df = order_crm_sfid \
        .withColumnRenamed("NUM_VEND", "SFID") \
        .join(digital_sfid_df, ["SFID"], "left").na.fill({"flag_sfid_digital": 0.0})

    order_crm_sfid_df.cache()

    # 7. Aggregating by num_cliente

    nc_order_crm_sfid_df = order_crm_sfid_df \
        .groupBy("NUM_CLIENTE") \
        .agg(count("*").alias("num_buy_orders"), sql_sum("flag_sfid_digital").alias("num_digital_buy_orders"))

    # 8. For each customer in the base, computing the number of purchasing orders and the number of purchasing orders by using a digital channel

    base_df = base_df \
        .join(nc_order_crm_sfid_df, ['num_cliente'], 'left') \
        .na.fill({'num_buy_orders': 0.0, 'num_digital_buy_orders': 0.0}) \
        .withColumn('pcg_digital_orders', when(col('num_buy_orders') > 0,
                                               col('num_digital_buy_orders').cast('double') / col(
                                                   'num_buy_orders').cast('double')).otherwise(0.0)) \

    return base_df


def get_kpis(spark, date_):
    year_ = date_[0:4]
    month_ = date_[4:6]
    day_ = date_[6:8]

    ids_completo = (spark.read.load(
        '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year_ + '/month=' + month_ + '/day=' + day_)).filter(
        col("serv_rgu") == "mobile")

    # Calculo variable CCC_total (suma de las 16 CCC_bucket)
    ids_completo_2 = ids_completo.withColumn('foc', col('CCC_num_calls_w4').cast("double") / lit(28.0))

    # Calculo variable de minutos de llamadas totales (week+weekend): tengo en cuenta los nulos (-1)
    ids_completo_2 = ids_completo_2.withColumn('llamadas_total', when(
        (col('GNV_Voice_L2_total_mou_we') == -1) & (col('GNV_Voice_L2_total_mou_w') == -1), -1).otherwise(
        when((col('GNV_Voice_L2_total_mou_w') == -1) & (col('GNV_Voice_L2_total_mou_we') != -1),
             col('GNV_Voice_L2_total_mou_we')).otherwise(
            when((col('GNV_Voice_L2_total_mou_we') == -1) & (col('GNV_Voice_L2_total_mou_w') != -1),
                 col('GNV_Voice_L2_total_mou_w')).otherwise(
                col('GNV_Voice_L2_total_mou_we') + col('GNV_Voice_L2_total_mou_w')))))

    ids_completo_2 = ids_completo_2 \
        .withColumn('tenure',
                    greatest('Cust_Agg_L2_fbb_fx_first_days_since_nc', 'Cust_Agg_L2_fixed_fx_first_days_since_nc',
                             'Cust_Agg_L2_mobile_fx_first_days_since_nc', 'Cust_Agg_L2_tv_fx_first_days_since_nc'))
    # Creo IDS basic seleccionando variables que nos interesan
    '''
    ids_basic = ids_completo_2.select('msisdn',
                                      'NUM_CLIENTE',
                                      'NIF_CLIENTE',
                                      'Serv_RGU',
                                      'Cust_COD_ESTADO_GENERAL',
                                      'Cust_Agg_L2_fbb_fx_first_days_since_nc',
                                      'Cust_Agg_L2_fixed_fx_first_days_since_nc',
                                      'Cust_Agg_L2_mobile_fx_first_days_since_nc',
                                      'Cust_Agg_L2_tv_fx_first_days_since_nc',
                                      'Bill_N1_Amount_To_Pay',
                                      'GNV_Data_L2_total_data_volume',
                                      'GNV_Data_L2_total_connections',
                                      'llamadas_total',
                                      'Cust_Agg_fbb_services_nc',
                                      'Cust_Agg_fixed_services_nc',
                                      'Cust_Agg_mobile_services_nc',
                                      'Cust_Agg_tv_services_nc',
                                      'Cust_Agg_L2_total_num_services_nc',
                                      'tgs_days_until_f_fin_bi',
                                      'Serv_L2_mobile_tariff_proc',
                                      'Serv_L2_desc_tariff_proc',
                                      'Cust_SUPEROFERTA',
                                      'Serv_PRICE_TARIFF',
                                      'Serv_voice_tariff',
                                      'Serv_L2_real_price',
                                      'Penal_L2_CUST_PENDING_end_date_total_max_days_until',
                                      'Comp_sum_count_comps',
                                      'Comp_num_distinct_comps',
                                      'CCC_total',
                                      'Spinners_total_acan',
                                      'Spinners_num_distinct_operators',
                                      'Spinners_nif_port_freq_per_msisdn')
    '''

    # adding churn30 label

    from churn_nrt.src.data.sopos_dxs import MobPort

    df_mob_port = MobPort(spark, 30) \
        .get_module(date_, save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_mob") \
        .distinct() \
        .withColumnRenamed("label_mob", "churn30")

    df_mob_port.cache()

    # base_df = base_df.join(df_mob_port, ['msisdn'], 'left').na.fill({'churn30': 0.0})

    kpis_df = ids_completo_2.select('msisdn',
                                    'NUM_CLIENTE',
                                    'NIF_CLIENTE',
                                    'tenure',
                                    'foc',
                                    'Bill_N1_Amount_To_Pay',
                                    'GNV_Data_L2_total_data_volume',
                                    'llamadas_total',
                                    'Cust_Agg_L2_total_num_services_nc',
                                    'tgs_days_until_f_fin_bi',
                                    'Serv_L2_real_price',
                                    'CCC_num_calls_w4')\
        .join(df_mob_port, ['msisdn'], 'left')\
        .na.fill({'churn30': 0.0})\
        .withColumnRenamed('Bill_N1_Amount_To_Pay', 'arpu')

    return kpis_df


if __name__ == "__main__":

    set_paths()

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    from churn_nrt.src.utils.spark_session import get_spark_session
    sc, spark, sql_context = get_spark_session("sfid_identification")

    # 0. Getting the base of msisdn services (that will be used as a reference)

    from churn_nrt.src.data.customer_base import CustomerBase

    mob_base_df = CustomerBase(spark) \
        .get_module(end_date, save=False, save_others=False, force_gen=True) \
        .filter(col("rgu")=="mobile")\
        .select("msisdn", "num_cliente", "nif_cliente") \
        .distinct()

    mob_base_df.repartition(400)

    mob_base_df.cache()

    # 1. Getting the base (num_cliente) with columns related to recent purchasing orders (sfid) and adding them to the reference base

    sfid_df = get_base_sfid(spark, start_date, end_date, verbose=True)

    mob_base_df = mob_base_df.join(sfid_df, ['num_cliente'], 'inner')

    # 2. Adding Adobe-based columns_ number of recent visits

    app_df = spark \
        .read \
        .table("raw_es.customerprofilecar_adobe_sections") \
        .filter(
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) >= from_unixtime(unix_timestamp(lit(start_date), 'yyyyMMdd')))
        &
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(end_date), 'yyyyMMdd')))
    ).groupBy('msisdn').agg(count("*").alias('nb_app_access'))\
        .withColumn("msisdn", col("msisdn").substr(3, 9)) \
        .distinct()

    mob_base_df = mob_base_df.join(app_df, ['msisdn'], 'left').na.fill({'nb_app_access': 0.0})

    mob_base_df.cache()

    print "[Info] Size of mob_base_df: " + str(mob_base_df.count())

    # 9. Adding KPIs

    kpis_df = get_kpis(spark, end_date)

    mob_base_kpi_df = mob_base_df.join(kpis_df, ['msisdn', 'num_cliente', 'nif_cliente'], 'inner')

    mob_base_kpi_df.cache()

    print "[Info] Size of mob_base_kpi_df: " + str(mob_base_kpi_df.count())

    # 3. Applying the conditions to define the poles: digital vs traditional

    mob_base_kpi_df = mob_base_kpi_df\
        .withColumn("hero", lit(-1)) \
        .withColumn("hero", when((col('num_digital_buy_orders') > 0) | (col('nb_app_access') > 0), 1).otherwise(col("hero"))) \
        .withColumn("hero", when(((col('num_buy_orders') > 0) & (col('num_digital_buy_orders') == 0)) | ((col('nb_app_access') == 0) & (col('CCC_num_calls_w4') > 0)), 0).otherwise(col("hero")))

    mob_base_kpi_df.cache()

    print "[Info] Size of mob_base_kpi_df: " + str(mob_base_kpi_df.count())

    # 4. Counting the number of services in each segment

    mob_base_kpi_df\
        .groupBy("hero")\
        .agg(count("*").alias("num_services"))\
        .show()

    print "[Info] Number of services in each segment"

    mob_base_kpi_df.filter(col('tenure') > 0).groupBy("hero").agg(sql_avg('tenure')).show()

    print "[Info] Average tenure above"

    mob_base_kpi_df.groupBy("hero").agg(sql_avg('foc')).show()

    print "[Info] Average foc above"

    mob_base_kpi_df.filter(col('arpu') > 0).groupBy("hero").agg(sql_avg('arpu')).show()

    print "[Info] Average arpu above"

    mob_base_kpi_df.groupBy("hero").agg(sql_avg('churn30')).show()

    print "[Info] Churn rate (30 days) above"




    '''

    from churn_nrt.src.projects.analysis.digital_scoring.metadata import METADATA_STANDARD_MODULES, get_metadata

    from churn_nrt.src.data_utils.Metadata import Metadata

    metadata_obj = Metadata(spark, get_metadata, ["num_cliente"], ["billing"])

    from churn_nrt.src.projects.analysis.digital_scoring.model_classes import SfidAnalysisData

    sfidanalysis_df = SfidAnalysisData(spark, metadata_obj).get_module(end_date, force_gen=True)

    mob_base_df = mob_base_df.join(sfidanalysis_df, ['num_cliente'], 'inner')

    kpi_feats = ['Bill_N1_Amount_To_Pay', 'billing_mean', 'num_calls_w4', 'num_calls_w8', 'churn30']

    billing_aggs = [sql_avg(col_).alias('avg_' + col_) for col_ in kpi_feats]

    mob_base_df.groupBy('hero').agg(*billing_aggs).show()
    
    '''
