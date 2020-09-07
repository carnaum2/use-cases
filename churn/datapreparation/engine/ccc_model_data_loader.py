

from pyspark.sql.functions import when, col

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

import churn.datapreparation.general.data_loader as general_data_loader
from churn.datapreparation.general.ccc_data_loader import get_ccc_profile, NO_PROB

import sys
import os
from pykhaos.utils.date_functions import move_date_n_days
from churn.datapreparation.engine.ccc_model_auxiliar import prepare_car


def set_label_to_msisdn_df(spark, config_obj, df_car, logger=None, remove_unlabeled=True):
    '''

    :param spark:
    :param config_obj:
    :param df_car: unlabeled car to set labels to
    :param logger: object to log in file
    :param remove_unlabeled: remove rows with label unset
    :return:
    '''

    agg_by = config_obj.get_agg_by()

    if logger: logger.debug("Number of {} df_car: df_car={} ".format(agg_by, df_car.count()))

    # - - - - - - - - - - - - - - - - - - - - - -
    # GET CATEGORIES BASED ON CCC
    # - - - - - - - - - - - - - - - - - - - - - -
    from churn.datapreparation.general.ccc_data_loader import get_calls_for_portouts, get_calls_for_portouts_filename
    calls_for_portouts_filename = get_calls_for_portouts_filename(config_obj)
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if not check_hdfs_exists(calls_for_portouts_filename):
        if logger: logger.debug("'{}' does not exist!".format(calls_for_portouts_filename))
        df_data = get_calls_for_portouts(spark, config_obj, ref_date=None, portout_select_cols=["msisdn_a", "portout_date"], logger=logger)

        if logger: logger.debug(df_data.dtypes)

        for col_ in ['bucket_list', 'bucket_set', 'cat1_list', 'cat2_list']:
            if col_ in df_data.columns: df_data = df_data.drop(col_)

        if logger: logger.debug("Number of {} df_data={} ".format(agg_by, df_data.count()))

        if logger: logger.info("Prepared to write to {}".format(calls_for_portouts_filename))

        df_data.write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save(calls_for_portouts_filename)

    else:
        print("'{}' exists! Loading!".format(calls_for_portouts_filename))
        df_data = spark.read.option("delimiter", "|").option("header", True).csv(calls_for_portouts_filename)

    if logger: logger.debug("Number of NIFs df_data: df_data={} ".format(df_data.count()))

    for col_ in ['cat1_list', "cat2_list", "first_interaction", "latest_interaction", "fx_latest_interaction", "fx_first_interaction"]:
        if col_ in df_data.columns: df_data = df_data.drop(col_)

    # - - - - - - - - - - - - - - - - - - - - - -
    # CHOOSE LABEL BASED ON MODEL TARGET
    # - - - - - - - - - - - - - - - - - - - - - -
    if config_obj.get_model_target() == "comercial":
        df_data = df_data.withColumn("label", when(col("CAT1_MODE")=="COMERCIAL", 1).when(col("CAT1_MODE")=="NO_COMERCIAL", 0).otherwise(-1)).select(["msisdn", "label"])
        if remove_unlabeled:
            if logger: logger.info("Removing unlabeled rows")
            df_data = df_data.where(col("label").isin([0,1])) # filter unlabeled rows
    elif config_obj.get_model_target() == "encuestas":
        if config_obj.get_agg_by() != "nif":
            if logger: logger.error("ERROR target encuestas is only supported by nif aggregation")
            sys.exit()
        from churn.datapreparation.engine.bajas_data_loader import load_reasons_encuestas
        df_encuestas = load_reasons_encuestas(spark, config_obj=None).withColumnRenamed("NIF_reasons", "NIF")
        df_encuestas = df_encuestas.withColumnRenamed("reason", "label")

        df_data = df_encuestas.join(df_data, on=["NIF"], how="left")
        if logger: logger.debug("Numero de encuestas {}".format(df_data.count()))

    else:
        if logger: logger.error("Model target {} is not implemented".format(config_obj.get_model_target()))
        sys.exit()

    # - - - - - - - - - - - - - - - - - - - - - -
    # JOINED UNLABELED CAR WITH LABELS
    # - - - - - - - - - - - - - - - - - - - - - -
    # Join between CAR and reasons using "NIF"
    df_tar = (df_data.join(df_car, on=[agg_by], how="left")) # only want NIFs with label

    # - - - - - - - - - - - - - - - - - - - - - -
    # PREPARA OUTPUT CAR (remove columns, nulls ,...)
    # - - - - - - - - - - - - - - - - - - - - - -
    #df_tar = __prepare_car(df_tar, config_obj, logger)

    return df_tar

def __prepare_car(df_car, config_obj, logger=None):
    '''
    This function is intented to be use with both options "unlabeled" and "labeled" cars.
    The goal is to clean the table of unwanted columns, to impute nulls, ....
    :param df_car:
    :return:
    '''
    agg_by = config_obj.get_agg_by()

    # - - - - - - - - - - - - - - - - - - - - - -
    # REMOVE SOME COLUMNS FROM CAR
    # - - - - - - - - - - - - - - - - - - - - - -
    fx_cols = [col_ for col_ in df_car.columns if
               ("fx_" in col_.lower() or "fecha" in col_.lower() or "date" in col_.lower()) and not ("days_since" in col_ or "days_until" in col_)]
    imputed_avg = [col_ for col_ in df_car.columns if col_.endswith("imputed_avg")]
    pcg_redem = [col_ for col_ in df_car.columns if col_.startswith("pcg_redem_")]
    orders = [col_ for col_ in df_car.columns if col_.lower().startswith("order") and (col_.lower().endswith("id") or
                                                                                       col_.lower().endswith("date") or
                                                                                       col_.lower().endswith("description"))]
    # remove penalties of descriptions
    novalen_penal_cols = [col_ for col_ in df_car.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("desc_promo") or
                                                                                                    col_.lower().endswith("desc_penal"))]
    useless =  ['bucket_list', 'bucket_set', 'cat1_list', 'cat2_list', "DIR_LINEA1", "DIR_LINEA2", "DIR_LINEA3", "DIR_LINEA4", "DIR_FACTURA1",
                "DIR_FACTURA2", "DIR_FACTURA3", "DIR_FACTURA4", "num_cliente_car", "num_cliente", "NOMBRE", "PRIM_APELLIDO", "SEG_APELLIDO", "NOM_COMPLETO",
                'bucket_list', 'bucket_set', 'TRAT_FACT', 'NOMBRE_CLI_FACT', 'APELLIDO1_CLI_FACT',
                'APELLIDO2_CLI_FACT', 'NIF_CLIENTE', 'CTA_CORREO_CONTACTO', 'CTA_CORREO', 'codigo_postal_city']

    for col_ in pcg_redem + fx_cols + imputed_avg + useless + novalen_penal_cols + orders:
        if col_ in df_car.columns: df_car = df_car.drop(col_)

    if agg_by == "nif":
        car_cols = [col_ for col_ in df_car.columns if col_.endswith("_nif")] + ["NIF"]
        df_car = df_car.withColumnRenamed("NIF_cliente", "NIF").select(car_cols)
        df_car = df_car.dropDuplicates(["NIF"])
    else:
        df_car = df_car.dropDuplicates(["msisdn"])

    # - - - - - - - - - - - - - - - - - - - - - -
    # CLEAN CAR (remove columns, nulls ,...)
    # - - - - - - - - - - - - - - - - - - - - - -
    df_car = df_car.where(col(agg_by).isNotNull())
    df_car = df_car.dropDuplicates([agg_by])

    minus_one_imp = ["PRICE_FOOTBALL_TV",  "ZAPPER_TV", "MOTOR_TV",
                 "days_since_first_interaction", "days_since_latest_interaction",
                 "codigo_postal_city", "days_since_bam_mobile_fx_first_nif", "total_football_price_nif", "age",
                 'days_since_bam_fx_first_nc', 'days_since_bam_mobile_fx_first_nc', 'days_since_fbb_fx_first_nc',
                 'days_since_fixed_fx_first_nc', 'days_since_mobile_fx_first_nc', 'days_since_prepaid_fx_first_nc',
                 'days_since_tv_fx_first_nc', 'days_since_bam_fx_first_nif', 'days_since_bam_mobile_fx_first_nif',
                 'days_since_fbb_fx_first_nif', 'PRICE_SRV_BASIC', 'PRICE_TARIFF', 'PRICE_VOICE_TARIFF', 'PRICE_DTO_LEV1',
                 'PRICE_DTO_LEV2', 'DTO_LEV3', 'PRICE_DTO_LEV3', 'PRICE_DATA_ADDITIONAL', 'PRICE_NETFLIX_NAPSTER',
                 'PRICE_ROAMING_BASIC', 'ROAM_USA_EUR', 'PRICE_ROAM_USA_EUR', 'PRICE_ROAM_ZONA_2', 'CONSUM_MIN',
                 'PRICE_CONSUM_MIN', 'SIM_VF', 'HOMEZONE', 'PRICE_HOMEZONE', 'MOBILE_HOMEZONE', 'FBB_UPGRADE', 'PRICE_FBB_UPGRADE',
                 'DECO_TV', 'PRICE_DECO_TV', 'NUM_SERIE_DECO_TV', 'OBJID_DECO_TV', 'TV_CUOTA_ALTA', 'PRICE_TV_CUOTA_ALTA',
                 'TV_TARIFF', 'PRICE_TV_TARIFF', 'TV_CUOT_CHARGES', 'PRICE_TV_CUOT_CHARGES', 'TV_PROMO', 'PRICE_TV_PROMO',
                 'TV_PROMO_USER', 'PRICE_TV_PROMO_USER', 'TV_ABONOS', 'PRICE_TV_ABONOS', 'TV_LOYALTY', 'PRICE_TV_LOYALTY',
                 'TV_SVA', 'PRICE_TV_SVA', 'FOOTBALL_TV', 'PRICE_FOOTBALL_TV', 'MOTOR_TV', 'PRICE_MOTOR_TV', 'PVR_TV', 'PRICE_PVR_TV',
                 'ZAPPER_TV', 'PRICE_ZAPPER_TV', 'TRYBUY_TV', 'PRICE_TRYBUY_TV', 'TRYBUY_AUTOM_TV', 'PRICE_TRYBUY_AUTOM_TV',
                 'flag_msisdn_err', 'TV_TOTAL_CHARGES', 'MOBILE_BAM_TOTAL_CHARGES']

    gnv_hour_mou = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('MOU')]
    gnv_hour_num_of_calls = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Num_Of_Calls')]
    gnv_hour_num_of_connections = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Num_Of_Connections')]
    gnv_hour_data_volume = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Data_Volume_MB')]
    ccc_cols = [col_ for col_ in df_car.columns if col_.startswith('Raw_Pagar_menos') or col_.startswith('Bucket_')]
    penal_cols = [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('penal_amount')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('days_to')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('cod_promo')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('desc_promo')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('cod_penal')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('desc_penal')]

    hour_max = [col_ for col_ in df_car.columns if col_.startswith("GNV_hour")]


    zero_imp = ["num_interactions", "num_NA_buckets", "num_ivr_interactions", "nb_diff_buckets", "COMERCIAL",
                "NO_COMERCIAL", "PRECIO",
                "TERMINAL", "CONTENIDOS", "SERVICIO/ATENCION", "TECNICO", "BILLING", "FRAUD", "NO_PROB",
                "TOTAL_COMERCIAL", "TOTAL_NO_COMERCIAL", "PRICE_DATA", "total_data_volume_WE", "total_data_volume_W",
                "data_per_connection", "total_num_services_nif", "tv_services_nif", "prepaid_services_nif", "bam_mobile_services_nif",
                "fixed_services_nif", "bam_services_nif", "flag_prepaid_nif", "num_football_nif", "fbb_services_nif",
                "mobile_services_nif", 'FACTURA_ELECTRONICA', "bam_services_nc", 'fbb_services_nc', 'mobile_services_nc',
                'fixed_services_nc', 'tv_services_nc', 'prepaid_services_nc', 'bam_mobile_services_nc', 'flag_prepaid_nc',
                u'total_num_services_nif', u'total_football_price_nif',
                u'mobile_fx_first_nif', u'tv_services_nif', u'tv_fx_first_nif', u'prepaid_services_nif',
                u'prepaid_fx_first_nif',
                u'bam_mobile_services_nif', u'bam_mobile_fx_first_nif', u'fixed_services_nif', u'fixed_fx_first_nif',
                u'bam_services_nif', u'bam_fx_first_nif', u'flag_prepaid_nif', u'num_football_nif',
                u'fbb_services_nif', u'fbb_fx_first_nif', u'mobile_services_nif', 'num_football_nc'
                'total_num_services_nc', 'total_num_services_nif'] + gnv_hour_mou + gnv_hour_num_of_calls +\
               gnv_hour_num_of_connections + gnv_hour_data_volume + ccc_cols + penal_cols + hour_max

    unknown_imp = ["bucket_1st_interaction", "bucket_latest_interaction", "ROAM_USA_EUR"]

    df_car = df_car.fillna(-1.0, subset=minus_one_imp)
    df_car = df_car.fillna(0.0, subset=zero_imp)
    df_car = df_car.fillna("UNKNOWN", subset=unknown_imp)

    return df_car




def __prepare_car_new(df_car, config_obj, closing_day, logger=None):
    '''
    This function is intented to be use with both options "unlabeled" and "labeled" cars.
    The goal is to clean the table of unwanted columns, to impute nulls, ....
    :param df_car:
    :return:
    '''
    agg_by = config_obj.get_agg_by()

    ##df_car = prepare_car(df_car, closing_day)

    # # - - - - - - - - - - - - - - - - - - - - - -
    # # REMOVE SOME COLUMNS FROM CAR
    # # - - - - - - - - - - - - - - - - - - - - - -
    fx_cols = [col_ for col_ in df_car.columns if
               ("fx_" in col_.lower() or "fecha" in col_.lower() or "date" in col_.lower()) and not ("days_since" in col_ or "days_until" in col_)]
    imputed_avg = [col_ for col_ in df_car.columns if col_.endswith("imputed_avg")]
    pcg_redem = [col_ for col_ in df_car.columns if col_.startswith("pcg_redem_")]
    orders = [col_ for col_ in df_car.columns if col_.lower().startswith("order") and (col_.lower().endswith("id") or
                                                                                       col_.lower().endswith("date") or
                                                                                       col_.lower().endswith("description"))]
    # remove penalties of descriptions
    novalen_penal_cols = [col_ for col_ in df_car.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("desc_promo") or
                                                                                                    col_.lower().endswith("desc_penal"))]
    useless =  ['bucket_list', 'bucket_set', 'cat1_list', 'cat2_list', "DIR_LINEA1", "DIR_LINEA2", "DIR_LINEA3", "DIR_LINEA4", "DIR_FACTURA1",
                "DIR_FACTURA2", "DIR_FACTURA3", "DIR_FACTURA4", "num_cliente_car", "num_cliente", "NOMBRE", "PRIM_APELLIDO", "SEG_APELLIDO", "NOM_COMPLETO",
                'bucket_list', 'bucket_set', 'TRAT_FACT', 'NOMBRE_CLI_FACT', 'APELLIDO1_CLI_FACT',
                'APELLIDO2_CLI_FACT', 'NIF_CLIENTE', 'CTA_CORREO_CONTACTO', 'CTA_CORREO', 'codigo_postal_city']

    for col_ in pcg_redem + fx_cols + imputed_avg + useless + novalen_penal_cols + orders:
        if col_ in df_car.columns: df_car = df_car.drop(col_)

    if agg_by == "nif":
        car_cols = [col_ for col_ in df_car.columns if col_.endswith("_nif")] + ["NIF"]
        df_car = df_car.withColumnRenamed("NIF_cliente", "NIF").select(car_cols)
        df_car = df_car.dropDuplicates(["NIF"])
    else:
        df_car = df_car.dropDuplicates(["msisdn"])

    # - - - - - - - - - - - - - - - - - - - - - -
    # CLEAN CAR (remove columns, nulls ,...)
    # - - - - - - - - - - - - - - - - - - - - - -
    df_car = df_car.where(col(agg_by).isNotNull())
    df_car = df_car.dropDuplicates([agg_by])

    minus_one_imp = ["PRICE_FOOTBALL_TV",  "ZAPPER_TV", "MOTOR_TV",
                 "days_since_first_interaction", "days_since_latest_interaction",
                 "codigo_postal_city", "days_since_bam_mobile_fx_first_nif", "total_football_price_nif", "age",
                 'days_since_bam_fx_first_nc', 'days_since_bam_mobile_fx_first_nc', 'days_since_fbb_fx_first_nc',
                 'days_since_fixed_fx_first_nc', 'days_since_mobile_fx_first_nc', 'days_since_prepaid_fx_first_nc',
                 'days_since_tv_fx_first_nc', 'days_since_bam_fx_first_nif', 'days_since_bam_mobile_fx_first_nif',
                 'days_since_fbb_fx_first_nif', 'PRICE_SRV_BASIC', 'PRICE_TARIFF', 'PRICE_VOICE_TARIFF', 'PRICE_DTO_LEV1',
                 'PRICE_DTO_LEV2', 'DTO_LEV3', 'PRICE_DTO_LEV3', 'PRICE_DATA_ADDITIONAL', 'PRICE_NETFLIX_NAPSTER',
                 'PRICE_ROAMING_BASIC', 'ROAM_USA_EUR', 'PRICE_ROAM_USA_EUR', 'PRICE_ROAM_ZONA_2', 'CONSUM_MIN',
                 'PRICE_CONSUM_MIN', 'SIM_VF', 'HOMEZONE', 'PRICE_HOMEZONE', 'MOBILE_HOMEZONE', 'FBB_UPGRADE', 'PRICE_FBB_UPGRADE',
                 'DECO_TV', 'PRICE_DECO_TV', 'NUM_SERIE_DECO_TV', 'OBJID_DECO_TV', 'TV_CUOTA_ALTA', 'PRICE_TV_CUOTA_ALTA',
                 'TV_TARIFF', 'PRICE_TV_TARIFF', 'TV_CUOT_CHARGES', 'PRICE_TV_CUOT_CHARGES', 'TV_PROMO', 'PRICE_TV_PROMO',
                 'TV_PROMO_USER', 'PRICE_TV_PROMO_USER', 'TV_ABONOS', 'PRICE_TV_ABONOS', 'TV_LOYALTY', 'PRICE_TV_LOYALTY',
                 'TV_SVA', 'PRICE_TV_SVA', 'FOOTBALL_TV', 'PRICE_FOOTBALL_TV', 'MOTOR_TV', 'PRICE_MOTOR_TV', 'PVR_TV', 'PRICE_PVR_TV',
                 'ZAPPER_TV', 'PRICE_ZAPPER_TV', 'TRYBUY_TV', 'PRICE_TRYBUY_TV', 'TRYBUY_AUTOM_TV', 'PRICE_TRYBUY_AUTOM_TV',
                 'flag_msisdn_err', 'TV_TOTAL_CHARGES', 'MOBILE_BAM_TOTAL_CHARGES']

    gnv_hour_mou = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('MOU')]
    gnv_hour_num_of_calls = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Num_Of_Calls')]
    gnv_hour_num_of_connections = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Num_Of_Connections')]
    gnv_hour_data_volume = [col_ for col_ in df_car.columns if col_.startswith('GNV_hour') and col_.endswith('Data_Volume_MB')]
    ccc_cols = [col_ for col_ in df_car.columns if col_.startswith('Raw_Pagar_menos') or col_.startswith('Bucket_')]
    penal_cols = [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('penal_amount')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('days_to')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('cod_promo')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('desc_promo')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('cod_penal')] +\
                 [col_ for col_ in df_car.columns if col_.startswith('Penal') and col_.endswith('desc_penal')]

    hour_max = [col_ for col_ in df_car.columns if col_.startswith("GNV_hour")]


    zero_imp = ["num_interactions", "num_NA_buckets", "num_ivr_interactions", "nb_diff_buckets", "COMERCIAL",
                "NO_COMERCIAL", "PRECIO",
                "TERMINAL", "CONTENIDOS", "SERVICIO/ATENCION", "TECNICO", "BILLING", "FRAUD", "NO_PROB",
                "TOTAL_COMERCIAL", "TOTAL_NO_COMERCIAL", "PRICE_DATA", "total_data_volume_WE", "total_data_volume_W",
                "data_per_connection", "total_num_services_nif", "tv_services_nif", "prepaid_services_nif", "bam_mobile_services_nif",
                "fixed_services_nif", "bam_services_nif", "flag_prepaid_nif", "num_football_nif", "fbb_services_nif",
                "mobile_services_nif", 'FACTURA_ELECTRONICA', "bam_services_nc", 'fbb_services_nc', 'mobile_services_nc',
                'fixed_services_nc', 'tv_services_nc', 'prepaid_services_nc', 'bam_mobile_services_nc', 'flag_prepaid_nc',
                u'total_num_services_nif', u'total_football_price_nif',
                u'mobile_fx_first_nif', u'tv_services_nif', u'tv_fx_first_nif', u'prepaid_services_nif',
                u'prepaid_fx_first_nif',
                u'bam_mobile_services_nif', u'bam_mobile_fx_first_nif', u'fixed_services_nif', u'fixed_fx_first_nif',
                u'bam_services_nif', u'bam_fx_first_nif', u'flag_prepaid_nif', u'num_football_nif',
                u'fbb_services_nif', u'fbb_fx_first_nif', u'mobile_services_nif', 'num_football_nc'
                'total_num_services_nc', 'total_num_services_nif'] + gnv_hour_mou + gnv_hour_num_of_calls +\
               gnv_hour_num_of_connections + gnv_hour_data_volume + ccc_cols + penal_cols + hour_max

    unknown_imp = ["bucket_1st_interaction", "bucket_latest_interaction", "ROAM_USA_EUR"]

    df_car = df_car.fillna(-1.0, subset=minus_one_imp)
    df_car = df_car.fillna(0.0, subset=zero_imp)
    df_car = df_car.fillna("UNKNOWN", subset=unknown_imp)

    return df_car


# def get_orders_df(spark, origin, refdate):
#    # Feats derived from orders up to refdate
#    # If the model target is computed in a time window starting at refdate, feats derived from orders may include proxy effects
#    # Example: churn model trained with pairs of the type (status of the customer at time t, port-out request during [t, t + window]).
#    # The customer might have ordered the disconnection of the targeted service (or a different one) before t.
#    # As a result, feats derived from orders will take into accout a gap of one month, i.e., they will correspond to the historic behaviour
#    # of the customer up to refdate - 1 month
#
#    ref_yearmonth = substractMonth(refdate[0:6], 1)
#    newrefdate = ref_yearmonth + getLastDayOfMonth(ref_yearmonth[4:6])
#
#    ref_year = newrefdate[0:4]
#
#    ref_month = newrefdate[4:6]
#
#    ref_day = newrefdate[6:8]
#
#    #window = Window.partitionBy("num_cliente")
#
#    repartdf = spark\
#    .read\
#    .parquet("/data/udf/vf_es/amdocs_ids/orders/year=" + str(ref_year) + "/month=" + str(int(ref_month)) + "/day=" + str(int(ref_day)))\
#    .repartition(400)
#
#    ordersdf = repartdf\
#    .withColumn("refdate", from_unixtime(unix_timestamp(lit(newrefdate), 'yyyyMMdd')))\
#    .withColumn("days_since_last_order", datediff("refdate", from_unixtime(unix_timestamp('order_n1_startdate', 'yyyyMMdd')))   )\
#    .withColumn("order_n1_class", when(isnull(col("order_n1_class")), "unknown").otherwise(col("order_n1_class")))\
#    .withColumn("days_since_first_order", greatest(datediff("refdate", from_unixtime(unix_timestamp('order_n1_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n2_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n3_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n4_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n5_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n6_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n7_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n8_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n9_startdate', 'yyyyMMdd'))), datediff("refdate", from_unixtime(unix_timestamp('order_n10_startdate', 'yyyyMMdd')) )  )   )\
#    .select(["num_cliente", "days_since_last_order", "days_since_first_order"]).repartition(1000)
#
#    print "[Info get_orders_df] " + time.ctime() + " Orders feats up to " + newrefdate + " computed - Number of rows is " + str(ordersdf.count()) + " for a total of " + str(ordersdf.select("num_cliente").distinct().count()) + " distinct num_cliente"
#
#    return ordersdf


def get_unlabeled_car_closing_day(spark, config_obj, c_day, logger=None):
    '''
    Return an unlabeled car for MOBILE services
    :param spark:
    :param config_obj:
    :param c_day:
    :param logger:
    :return:
    '''

    print("ccc_model_data_loader - get_unlabeled_car_closing_day")
    df_car = general_data_loader.get_unlabeled_car_closing_day(spark, config_obj, closing_day=c_day)

    df_car = (df_car.where(col("clase_cli_cod_clase_cliente") == "RS")  # customer
               .where(col("cod_estado_general").isin(["01", "09"]))  # customer
               .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
               .where(col("rgu").isNotNull())).where(col("rgu") == "mobile")  # keep only mobile services


    # # Get customer and active services
    # df_car = (df_car.where(col("clase_cli_cod_clase_cliente") == "RS")  # customer
    #                .where(col("cod_estado_general").isin(["01", "09"]))  # customer
    #                .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
    #                .where(col("rgu")=="mobile")) #FIXME


    n_ccc = config_obj.get_ccc_range_duration()

    ccc_start_date = move_date_n_days(c_day, n=n_ccc, str_fmt="%Y%m%d")
    ccc_end_date = c_day

    # I am doing this intentionaly. In this model, we compute the labels for all clients.
    # For those that remain unset, we make predictions
    # For those that are set to (COMERCIAL, NO_COMERCIAL) I prefer not to to predictions
    agg_by = config_obj.get_agg_by()

    df_agg_all_ccc = get_ccc_profile(spark, ccc_start_date, ccc_end_date, agg_by, df_other=df_car, other_select_cols=[agg_by])

    df_tar = df_car.join(df_agg_all_ccc, on=["msisdn"], how="left")

    df_tar = (df_tar.withColumn("CAT1_MODE", when(col("CAT1_MODE").isNull(), NO_PROB).otherwise(col("CAT1_MODE")))
                    .withColumn("CAT2_MODE", when(col("CAT2_MODE").isNull(), NO_PROB).otherwise(col("CAT2_MODE"))))

    # yes, this is an unlabeled car. But, since we can determine the label according to CAT1_MODE we set the label column.
    if config_obj.get_model_target() == "comercial":
        df_tar = df_tar.withColumn("label", when(col("CAT1_MODE")=="COMERCIAL", 1).when(col("CAT1_MODE")=="NO_COMERCIAL", 0).otherwise(-1))
    else:
        if logger: logger.error("Model target {} is not implemented".format(config_obj.get_model_target()))
        print("Model target {} is not implemented".format(config_obj.get_model_target()))
        sys.exit()

    df_tar = __prepare_car(df_tar, config_obj, c_day)

    print("[DEBUG] Ended get_unlabeled_car ccc_model_data_loader")
    return df_tar

def get_unlabeled_car(spark, config_obj, logger=None):

    df_list = []

    for c_day in config_obj.get_closing_day():
        if logger: logger.info(" - - - - - - - - - - - - - - - - ")
        if logger: logger.info(c_day)
        if logger: logger.info(" - - - - - - - - - - - - - - - - ")

        df_label_car = get_unlabeled_car_closing_day(spark, config_obj, c_day, logger=logger)

        df_list.append(df_label_car)

    from pykhaos.utils.pyspark_utils import union_all
    return union_all(df_list)


def get_labeled_car(spark, config_obj, logger=None):

    df_list = []

    for c_day in config_obj.get_closing_day():
        if logger: logger.info(" - - - - - - - - - - - - - - - - ")
        if logger: logger.info("get_labeled_car {}".format(c_day))
        if logger: logger.info(" - - - - - - - - - - - - - - - - ")

        df_label_car = get_labeled_car_closing_day(spark, config_obj, c_day, logger=logger)
        import collections
        mylist = df_label_car.columns
        counter = collections.Counter(mylist)
        print(sorted(counter.items()))
        logger.info("after get_labeled_car_closing_day")
        df_list.append(df_label_car)

    from pykhaos.utils.pyspark_utils import union_all
    return union_all(df_list)

def get_labeled_car_closing_day(spark, config_obj, c_day, logger=None):

    # - - - - - - - - - - - - - - - - - - - - - -
    # UNLABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - -
    df_car = get_unlabeled_car_closing_day(spark, config_obj, c_day)

    # - - - - - - - - - - - - - - - - - - - - - -
    # Labeling
    # with the specified target (modeltarget) and the specified level (level).Services (service_set) in the
    # specified segment (target_num_cliente) are labeled

    df_label_car = set_label_to_msisdn_df(spark, config_obj, df_car, logger)

    import collections
    mylist = df_label_car.columns
    counter = collections.Counter(mylist)
    print(sorted(counter.items()))
    print("AFTER set_label_to_msisdn_df")

    #print("CSANC109 [Info Amdocs Car Preparation] Size of labelfeatcar: {}".format(df_label_car.count()))

    return df_label_car

def get_labeled_or_unlabeled_car(spark, config_obj, logger=None):

    labeled = config_obj.get_labeled()
    if labeled:
        if logger: logger.debug("Asked labeled car")
        df_labeled_car = get_labeled_car(spark, config_obj, logger=logger)

        return df_labeled_car
    elif labeled == False:
        if logger: logger.debug("Asked unlabeled car")
        df_unlabeled_car = get_unlabeled_car(spark, config_obj, logger=logger)
        return df_unlabeled_car
    else:
        logger.error("Not valid value for labeled in yaml file")
        return None



def build_storage_dir_name(start_port, end_port, n_ccc, closing_day, model_target, labeled, agg_by, segment_filter=None):
    from churn.utils.constants import HDFS_DIR_DATA

    if labeled:
        name_ = 'df_{}_{}_c{}_n{}_{}_{}'.format(start_port, end_port, closing_day, abs(n_ccc), model_target, agg_by)
        filename_df = os.path.join(HDFS_DIR_DATA, "ccc_model", model_target if labeled else model_target + "_unlabeled", name_)
    else:
        name_ = 'df_c{}_n{}_{}_{}_{}'.format(closing_day, abs(n_ccc), model_target, agg_by, segment_filter)
        filename_df = os.path.join(HDFS_DIR_DATA, "ccc_model", model_target + "_unlabeled", name_)
    return filename_df


def build_storage_dir_name_obj(config_obj, add_hdfs_preffix=False):
    start_port = config_obj.get_start_port()
    end_port = config_obj.get_end_port()
    n_ccc = config_obj.get_ccc_range_duration()
    closing_day = config_obj.get_closing_day()[0] if len(config_obj.get_closing_day())==0 else ",".join(config_obj.get_closing_day())

    model_target = config_obj.get_model_target()
    labeled = config_obj.get_labeled()
    agg_by = config_obj.get_agg_by()
    segment_filter = config_obj.get_segment_filter()

    filename_df = build_storage_dir_name(start_port, end_port, n_ccc, closing_day, model_target, labeled, agg_by, segment_filter)

    if add_hdfs_preffix:
        filename_df = "hdfs://" + filename_df

    print("Built filename: {}".format(filename_df))
    return filename_df


def save_results(df, config_obj, csv=True, logger=None):
    '''
    This function save the df and the yaml's (internal and user)
    :param df:
    :param path_filename:
    :return:
    '''
    storage_dir = build_storage_dir_name_obj(config_obj, add_hdfs_preffix=False)
    from churn.utils.general_functions import save_df
    if df:
        save_df(df, storage_dir, csv)
        if logger: logger.info("Saved df in '{}' format {}".format(storage_dir, "csv" if csv else "parquet"))
    from churn.utils.constants import YAML_FILES_DIR
    from pykhaos.utils.hdfs_functions import create_directory
    # create directory in hdfs to store config files
    yaml_dir = os.path.join(storage_dir, YAML_FILES_DIR)
    create_directory(yaml_dir)
    from pykhaos.utils.hdfs_functions import move_local_file_to_hdfs
    user_config_filename = config_obj.get_user_config_filename()
    internal_config_filename = config_obj.get_internal_config_filename()
    move_local_file_to_hdfs(yaml_dir, user_config_filename)
    move_local_file_to_hdfs(yaml_dir, internal_config_filename)
    return storage_dir



