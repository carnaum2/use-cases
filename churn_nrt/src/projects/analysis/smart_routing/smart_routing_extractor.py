# coding=utf-8

from common.src.main.python.utils.hdfs_generic import *
import os
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

import numpy as np


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

    ############### 0. Spark ################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("smart_routing")

    ############### 0. Input arguments #################

    import argparse

    parser = argparse.ArgumentParser(
        description="Run smart_routing --ids_date YYYYMMDD --store y/n",
        epilog='Please report bugs and issues to Victor <jose.marcos@vodafone.com>')

    parser.add_argument('--ids_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Picture date of the IDS from which to extract the attributes')

    parser.add_argument('--store', metavar='y/n', type=str, required=False,
                        help='Store or not the extracted DF in the model scores path')

    parser.add_argument('--insert_day', metavar='<insert_day>', type=int, required=False, default=5,
                        help='day to insert in model outputs')

    args = parser.parse_args()
    print(args)

    date_ = args.ids_date
    store_ = args.store
    insert_day = args.insert_day

    #date_ = sys.argv[1]
    #store_ = sys.argv[2]

    year_ = str(int(date_[0:4]))
    month_ = str(int(date_[4:6]))
    day_ = str(int(date_[6:8]))


    def getInitialCrmColumns():
        return {"codigo_postal": "Cust_L2_codigo_postal_city",
                "metodo_pago": "Cust_METODO_PAGO",
                "cta_correo": "Cust_cta_correo_flag",
                "factura_electronica": "Cust_FACTURA_ELECTRONICA",
                "superoferta": "Cust_SUPEROFERTA",
                "fecha_migracion": "Cust_FECHA_MIGRACION",
                "tipo_documento": "Cust_TIPO_DOCUMENTO",
                "nacionalidad": "Cust_NACIONALIDAD",
                "x_antiguedad_cuenta": "Cust_x_antiguedad_cuenta",
                "x_datos_navegacion": "Cust_x_datos_navegacion",
                "x_datos_trafico": "Cust_x_datos_trafico",
                "x_cesion_datos": "Cust_x_cesion_datos",
                "x_user_facebook": "Cust_x_user_facebook",
                "x_user_twitter": "Cust_x_user_twitter",
                "marriage2hgbst_elm": "Cust_civil_status",
                "gender2hgbst_elm": "Cust_gender",
                "flg_robinson": "Cust_FLG_ROBINSON",
                "x_formato_factura": "Cust_X_FORMATO_FACTURA",
                "x_idioma_factura": "Cust_X_IDIOMA_FACTURA",
                "bam_services": "Cust_Agg_bam_services_nc",
                "bam_fx_first": "Cust_Agg_bam_fx_first_nc",
                "bam-movil_services": "Cust_Agg_bam_mobile_services_nc",
                "bam-movil_fx_first": "Cust_Agg_bam_mobile_fx_first_nc",
                "fbb_services": "Cust_Agg_fbb_services_nc",
                "fbb_fx_first": "Cust_Agg_fbb_fx_first_nc",
                "fixed_services": "Cust_Agg_fixed_services_nc",
                "fixed_fx_first": "Cust_Agg_fixed_fx_first_nc",
                "movil_services": "Cust_Agg_mobile_services_nc",
                "movil_fx_first": "Cust_Agg_bam_mobile_fx_first_nc",
                "prepaid_services": "Cust_Agg_prepaid_services_nc",
                "prepaid_fx_first": "Cust_Agg_prepaid_fx_first_nc",
                "tv_services": "Cust_Agg_tv_services_nc",
                "tv_fx_first": "Cust_Agg_tv_fx_first_nc",
                "fx_srv_basic": "Serv_FX_SRV_BASIC",
                "tipo_sim": "Serv_tipo_sim",
                "desc_tariff": "Serv_DESC_TARIFF",
                "tariff": "Serv_tariff",
                "fx_tariff": "Serv_FX_TARIFF",
                "price_tariff": "Serv_PRICE_TARIFF",
                "voice_tariff": "Serv_voice_tariff",
                "fx_voice_tariff": "Serv_FX_VOICE_TARIFF",
                "data": "Serv_data",
                "fx_data": "Serv_FX_DATA",
                "dto_lev1": "Serv_dto_lev1",
                "fx_dto_lev1": "Serv_FX_DTO_LEV1",
                "price_dto_lev1": "Serv_price_dto_lev1",
                "dto_lev2": "Serv_dto_lev2",
                "fx_dto_lev2": "Serv_FX_DTO_LEV2",
                "price_dto_lev2": "Serv_price_dto_lev2",
                "data_additional": "Serv_data_additional",
                "fx_data_additional": "Serv_FX_DATA_ADDITIONAL",
                "roam_zona_2": "Serv_roam_zona_2",
                "fx_roam_zona_2": "Serv_FX_ROAM_ZONA_2",
                "sim_vf": "Serv_sim_vf"}


    def getNewCrmColumns():
        return {"dias_desde_fecha_migracion": "Cust_L2_days_since_fecha_migracion",
                "cliente_migrado": "Cust_L2_migrated_client",
                "dias_desde_bam_fx_first": "Cust_Agg_L2_bam_fx_first_days_since_nc",
                "dias_desde_bam-movil_fx_first": "Cust_Agg_L2_bam_mobile_fx_first_days_since_nc",
                "dias_desde_fbb_fx_first": "Cust_Agg_L2_fbb_fx_first_days_since_nc",
                "dias_desde_fixed_fx_first": "Cust_Agg_L2_fixed_fx_first_days_since_nc",
                "dias_desde_movil_fx_first": "Cust_Agg_L2_mobile_fx_first_days_since_nc",
                "dias_desde_prepaid_fx_first": "Cust_Agg_L2_prepaid_fx_first_days_since_nc",
                "dias_desde_tv_fx_first": "Cust_Agg_L2_tv_fx_first_days_since_nc",
                # "dias_desde_fx_srv_basic": "----",
                "segunda_linea": "Serv_L2_segunda_linea",
                # "dias_desde_fx_tariff": "----",
                "dias_desde_fx_voice_tariff": "Serv_L2_days_since_Serv_fx_voice_tariff",
                "dias_desde_fx_data": "Serv_L2_days_since_Serv_fx_data",
                "dias_desde_fx_dto_lev1": "Serv_L2_days_since_Serv_fx_dto_lev1",
                "dias_desde_fx_dto_lev2": "Serv_L2_days_since_Serv_fx_dto_lev2",
                "dias_desde_fx_data_additional": "Serv_L2_days_since_Serv_fx_data_additional",
                "dias_desde_fx_roam_zona_2": "Serv_L2_days_since_Serv_fx_roam_zona_2",
                "total_num_services": "Cust_Agg_L2_total_num_services_nc",
                "real_price": "Serv_L2_real_price",
                # "num_movil": "Cust_Agg_mobile_services_nc",
                # "num_bam": "Cust_Agg_bam_services_nc",
                # "num_fixed": "Cust_Agg_fixed_services_nc",
                # "num_prepaid": "Cust_Agg_prepaid_services_nc",
                # "num_tv": "Cust_Agg_tv_services_nc",
                # "num_fbb": "Cust_Agg_fbb_services_nc",
                "num_futbol": "Cust_Agg_football_services",
                "total_futbol_price": "Cust_Agg_total_football_price_nc",
                "total_penal_cust_pending_n1_penal_amount": "Penal_CUST_L2_PENDING_N1_penal_amount_total",
                "total_penal_cust_pending_n2_penal_amount": "Penal_CUST_L2_PENDING_N2_penal_amount_total",
                "total_penal_cust_pending_n3_penal_amount": "Penal_CUST_L2_PENDING_N3_penal_amount_total",
                "total_penal_cust_pending_n4_penal_amount": "Penal_CUST_L2_PENDING_N4_penal_amount_total",
                "total_penal_cust_pending_n5_penal_amount": "Penal_CUST_L2_PENDING_N5_penal_amount_total",
                "total_penal_srv_pending_n1_penal_amount": "Penal_SRV_L2_PENDING_N1_penal_amount_total",
                "total_penal_srv_pending_n2_penal_amount": "Penal_SRV_L2_PENDING_N2_penal_amount_total",
                "total_penal_srv_pending_n3_penal_amount": "Penal_SRV_L2_PENDING_N3_penal_amount_total",
                "total_penal_srv_pending_n4_penal_amount": "Penal_SRV_L2_PENDING_N4_penal_amount_total",
                "total_penal_srv_pending_n5_penal_amount": "Penal_SRV_L2_PENDING_N5_penal_amount_total",
                "total_max_dias_hasta_penal_cust_pending_end_date": "Penal_CUST_L2_PENDING_end_date_total_max_days_until",
                "total_max_dias_hasta_penal_srv_pending_end_date": "Penal_SRV_L2_PENDING_end_date_total_max_days_until"}


    def getRemColumns():
        return {"fecha_migracion": "Cust_FECHA_MIGRACION",
                "bam_fx_first": "Cust_Agg_bam_fx_first_nc",
                "bam-movil_fx_first": "Cust_Agg_bam_mobile_fx_first_nc",
                "fbb_fx_first": "Cust_Agg_fbb_fx_first_nc",
                "fixed_fx_first": "Cust_Agg_fixed_fx_first_nc",
                "movil_fx_first": "Cust_Agg_mobile_fx_first_nc",
                "prepaid_fx_first": "Cust_Agg_prepaid_fx_first_nc",
                "tv_fx_first": "Cust_Agg_tv_fx_first_nc",
                "fx_srv_basic": "Serv_FX_SRV_BASIC",
                "fx_tariff": "Serv_FX_TARIFF",
                "fx_voice_tariff": "Serv_FX_VOICE_TARIFF",
                "fx_data": "Serv_FX_DATA",
                "fx_dto_lev1": "Serv_FX_DTO_LEV1",
                "fx_dto_lev2": "Serv_FX_DTO_LEV2",
                "fx_data_additional": "Serv_FX_DATA_ADDITIONAL",
                "fx_roam_zona_2": "Serv_FX_ROAM_ZONA_2"}


    def getProcessedCrmColumns():
        all_dict = dict(getInitialCrmColumns().items() + getNewCrmColumns().items())
        filt_dict = dict([(k, v) for (k, v) in all_dict.items() if (v not in getRemColumns().values())])
        return filt_dict


    CURRENT_IDS_PATH = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/'

    ids_df = spark \
        .read \
        .parquet(CURRENT_IDS_PATH + 'year=' + year_ + '/month=' + month_ + '/day=' + day_) \
        .filter(col('serv_rgu').isin('mobile', 'fixed')) \
        .select(getProcessedCrmColumns().values() + ['Serv_FX_SRV_BASIC', 'Serv_FX_TARIFF', 'msisdn']) \
        .withColumn('dias_desde_fx_srv_basic', datediff(from_unixtime(unix_timestamp(lit(date_), "yyyyMMdd")),
                                                        from_unixtime(
                                                            unix_timestamp(col('Serv_FX_SRV_BASIC'), "yyyyMMdd")))) \
        .withColumn('dias_desde_fx_tariff', datediff(from_unixtime(unix_timestamp(lit(date_), "yyyyMMdd")),
                                                     from_unixtime(unix_timestamp(col('Serv_FX_TARIFF'), "yyyyMMdd")))) \
        .drop('Serv_FX_SRV_BASIC', 'Serv_FX_TARIFF')

    ids_df.printSchema()

    # renaming columns

    for (old_, new_) in getProcessedCrmColumns().items():
        ids_df = ids_df.withColumnRenamed(new_, old_)

    ids_df = ids_df \
        .withColumn('num_movil', col('movil_services')) \
        .withColumn('num_bam', col('bam_services')) \
        .withColumn('num_fixed', col('fixed_services')) \
        .withColumn('num_prepaid', col('prepaid_services')) \
        .withColumn('num_tv', col('tv_services')) \
        .withColumn('num_fbb', col('fbb_services')) \
        .withColumn('picture_date', lit(date_))

    sorted_cols=['codigo_postal',
                 'metodo_pago',
                 'cta_correo',
                 'factura_electronica',
                 'superoferta',
                 'tipo_documento',
                 'nacionalidad',
                 'x_antiguedad_cuenta',
                 'x_datos_navegacion',
                 'x_datos_trafico',
                 'x_cesion_datos',
                 'x_user_facebook',
                 'x_user_twitter',
                 'marriage2hgbst_elm',
                 'gender2hgbst_elm',
                 'flg_robinson',
                 'x_formato_factura',
                 'x_idioma_factura',
                 'bam_services',
                 'bam-movil_services',
                 'fbb_services',
                 'fixed_services',
                 'movil_services',
                 'prepaid_services',
                 'tv_services',
                 'tipo_sim',
                 'desc_tariff',
                 'tariff',
                 'price_tariff',
                 'voice_tariff',
                 'data',
                 'dto_lev1',
                 'price_dto_lev1',
                 'dto_lev2',
                 'price_dto_lev2',
                 'data_additional',
                 'roam_zona_2',
                 'sim_vf',
                 'dias_desde_fecha_migracion',
                 'cliente_migrado',
                 'dias_desde_bam_fx_first',
                 'dias_desde_bam-movil_fx_first',
                 'dias_desde_fbb_fx_first',
                 'dias_desde_fixed_fx_first',
                 'dias_desde_movil_fx_first',
                 'dias_desde_prepaid_fx_first',
                 'dias_desde_tv_fx_first',
                 'dias_desde_fx_srv_basic',
                 'segunda_linea',
                 'dias_desde_fx_tariff',
                 'dias_desde_fx_voice_tariff',
                 'dias_desde_fx_data',
                 'dias_desde_fx_dto_lev1',
                 'dias_desde_fx_dto_lev2',
                 'dias_desde_fx_data_additional',
                 'dias_desde_fx_roam_zona_2',
                 'total_num_services',
                 'real_price',
                 'num_movil',
                 'num_bam',
                 'num_fixed',
                 'num_prepaid',
                 'num_tv',
                 'num_fbb',
                 'num_futbol',
                 'total_futbol_price',
                 'total_penal_cust_pending_n1_penal_amount',
                 'total_penal_cust_pending_n2_penal_amount',
                 'total_penal_cust_pending_n3_penal_amount',
                 'total_penal_cust_pending_n4_penal_amount',
                 'total_penal_cust_pending_n5_penal_amount',
                 'total_penal_srv_pending_n1_penal_amount',
                 'total_penal_srv_pending_n2_penal_amount',
                 'total_penal_srv_pending_n3_penal_amount',
                 'total_penal_srv_pending_n4_penal_amount',
                 'total_penal_srv_pending_n5_penal_amount',
                 'total_max_dias_hasta_penal_cust_pending_end_date',
                 'total_max_dias_hasta_penal_srv_pending_end_date',
                 'picture_date']

    ids_df = ids_df.select(['msisdn'] + sorted_cols)

    for c in ids_df.columns:
        ids_df = ids_df.withColumn(c, col(c).cast('string'))

    print '[Info] Size of the DF: ' + str(ids_df.count()) + ' - Number of distinct msisdn: ' + str(ids_df.select('msisdn').distinct().count()) +  ' - Number of columns: ' + str(len(ids_df.columns))

    if(store_=='y'):

        print '[Info] Data to be stored'

        '''
        ids_df\
            .coalesce(1)\
            .write\
            .csv("/user/jmarcoso/smart_routing_" + date_, header = True)

        print '[Info] Data stored in ' + "/user/jmarcoso/smart_routing_" + date_
        '''

        import datetime as dt

        executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

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

        partition_date = ""

        if ((insert_day >= 1) & (insert_day <= 7)):
            from churn_nrt.src.utils.date_functions import get_next_nth_day
            partition_date = get_next_nth_day(date_, nth=5)
        else:
            partition_date = str(insert_day)

        partition_year = str(int(partition_date[0:4]))
        partition_month = str(int(partition_date[4:6]))
        partition_day = str(int(partition_date[6:8]))

        df_model_scores = (ids_df
                           .withColumn("model_name", lit("smart_routing").cast("string"))
                           .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string"))
                           .withColumn("model_executed_at", col("executed_at").cast("string"))
                           .withColumn("client_id", lit(""))
                           .withColumn("msisdn", col("msisdn").cast("string"))
                           .withColumn("nif", lit(""))
                           .withColumn("scoring", lit(0.0).cast("float"))
                           .withColumn("model_output", lit("0"))
                           .withColumn("prediction", lit("").cast("string"))
                           .withColumn("extra_info", concat_ws("|", *sorted_cols))
                           .withColumn("predict_closing_date", col('picture_date'))
                           .withColumn("year", lit(partition_year).cast("integer"))
                           .withColumn("month", lit(partition_month).cast("integer"))
                           .withColumn("day", lit(partition_day).cast("integer"))
                           .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                           .select(*model_output_cols))

        score_path = "/data/attributes/vf_es/model_outputs/model_scores/"
        #score_path = '/user/jmarcoso/jmarcoso_model_scores/'

        df_model_scores \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save(score_path)

        print '[Info] Data stored in ' + score_path + " with partition " + partition_date


