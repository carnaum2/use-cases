#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit, length, concat_ws, regexp_replace,unix_timestamp,desc, concat, year,row_number, month, dayofmonth, split, regexp_extract, coalesce, count, lpad
from pyspark.sql.functions import sum as sql_sum, max as sql_max, countDistinct, min as sql_min, mean as sql_avg
from pyspark.sql import Row, DataFrame, Column, Window
from churn_nrt.src.data_utils.DataTemplate import DataTemplate, TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate


def get_target_nifs_ids(spark, date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_ccc=True, n_cycles=12, horizon=4, gap_window=4, verbose=False):

    from churn_nrt.src.utils.date_functions import move_date_n_cycles
    date_prev = move_date_n_cycles(date_, -n_cycles)

    horizon_sup = (horizon+gap_window) * 7
    horizon_inf = gap_window*7

    PATH_IDS = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/'

    from churn_nrt.src.utils.constants import PARTITION_DATE

    partition_date = PARTITION_DATE.format(date_[:4], str(int(date_[4:6])), str(int(date_[6:8])))
    partition_date_prev = PARTITION_DATE.format(date_prev[:4], str(int(date_prev[4:6])), str(int(date_prev[6:8])))

    path_ids = PATH_IDS + partition_date
    path_ids_prev = PATH_IDS + partition_date_prev

    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    print(path_ids)
    if check_hdfs_exists(path_ids):
        df_ids_ori = spark.read.load(path_ids)
        df_ids = df_ids_ori.select('nif_cliente', 'tgs_days_until_fecha_fin_dto').distinct()
    else:
        import sys
        print('IDS is not generated for date {}'.format(date_))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)
    print(path_ids_prev)
    if check_hdfs_exists(path_ids_prev):
        pass
    else:
        import sys
        print('IDS is not generated for date {}'.format(date_prev))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)
    if verbose:
        print('Number of distinct nif_cliente-tgs_dto: {}'.format(df_ids.count()))
        print('Number of distinct nif_cliente {}'.format(df_ids.select('nif_cliente').distinct().count()))

    df_ids = df_ids.where((col('tgs_days_until_fecha_fin_dto') < horizon_sup) & (col('tgs_days_until_fecha_fin_dto') > horizon_inf))
    if verbose:
        print('Size target customers df: {}'.format(df_ids.count()))

    if filter_recent:
        from churn_nrt.src.data_utils.ids_filters import get_non_recent_customers_filter
        df_ids_rec = get_non_recent_customers_filter(spark, date_, n_cycles, level='nif', verbose=verbose)
        df_ids = df_ids.join(df_ids_rec.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after early churn filter: {}'.format(df_ids.count()))

    if filter_disc:
        from churn_nrt.src.data_utils.ids_filters import get_disconnection_process_filter
        df_ids_disc = get_disconnection_process_filter(spark, date_, n_cycles, verbose)
        df_ids = df_ids.join(df_ids_disc.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after disconnection filter: {}'.format(df_ids.count()))


    if filter_ord:
        df_ids = df_ids.join(df_ids_ori.select('nif_cliente', "Ord_sla_has_forbidden_orders_last90"), ['nif_cliente'], 'inner').where(~(col("Ord_sla_has_forbidden_orders_last90") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after forbidden orders filter: {}'.format(df_ids.count()))

    if filter_ccc:
        df_ids = df_ids.join(df_ids_ori.select('nif_cliente', "CCC_num_calls_w4"), ['nif_cliente'], 'inner').where(~(col("CCC_num_calls_w4") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after forbidden orders filter: {}'.format(df_ids.count()))

    df_ids_final = df_ids.select('nif_cliente').drop_duplicates(['nif_cliente'])
    df_ids_final = df_ids_final.cache()
    print('Number of NIFs on the final IDS target: {}'.format(df_ids_final.select('nif_cliente').distinct().count()))

    return df_ids_final

def get_ccc_label(spark, date_, horizon=8, gap_window = 2, gap_label = 4, verbose=False):
    from pyspark.sql.functions import upper, isnull, col, unix_timestamp, from_unixtime, countDistinct
    bucket = spark.read.format("csv").option("header", "true").option("delimiter", ";") \
        .load("/data/udf/vf_es/ref_tables/amdocs_ids/Agrup_Buckets_unific.txt").withColumn('INT_Tipo',upper(col('INT_Tipo'))) \
        .withColumn('INT_Subtipo', upper(col('INT_Subtipo'))) \
        .withColumn('INT_Razon', upper(col('INT_Razon'))) \
        .withColumn('INT_Resultado', upper(col('INT_Resultado'))) \
        .withColumn("bucket", upper(col("bucket"))) \
        .withColumn("sub_bucket", upper(col("sub_bucket"))) \
        .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")))

    select_trip = bucket.where( \
        (col('INT_Razon').contains('PERMANENCIA')) | \
        (col('INT_Razon').contains('PLAN PRECIOS')) | \
        (col('INT_Razon').contains('DESCUENTOS')) | \
        (col('INT_Razon').contains('DTO')) | \
        (col('INT_Razon').contains('PROMO')) | \
        (col('INT_Razon').contains('TOTAL A PAGAR')) | \
        (col('INT_Razon').contains('IMPORTE')) | \
        (col('INT_Subtipo').contains('DEUDAS Y PAGOS')) | \
        (col('INT_Subtipo').contains('DUDAS FACTURACION')) | \
        (col('INT_Subtipo').contains('ERROR FACTURA')) | (col('INT_Subtipo').contains('ERROR IMPORTE')) | \
        (col('INT_Subtipo').contains('EXPLICAR FACTURA')) | (col('INT_Subtipo').contains('TARIFICA')) | \
        ((col('Bucket').contains('CHURN')) | (col('Sub_Bucket').contains('CHURN'))))
    if verbose:
        select_trip = select_trip.cache()
        print("Number of selected triplets: {}".format(select_trip.count()))

    from churn_nrt.src.utils.date_functions import move_date_n_cycles

    date_inf = date_
    date_sup = move_date_n_cycles(date_, horizon + gap_window + gap_label)

    interactions_ono = spark.table('raw_es.callcentrecalls_interactionono') \
        .withColumn('formatted_date', from_unixtime(unix_timestamp(col("FX_CREATE_DATE"), "yyyy-MM-dd"))) \
        .filter((col('formatted_date') >= from_unixtime(unix_timestamp(lit(date_inf), "yyyyMMdd"))) & (
                col('formatted_date') <= from_unixtime(unix_timestamp(lit(date_sup), "yyyyMMdd"))))

    interactions_ono = interactions_ono.filter(
        "DS_DIRECTION IN ('De entrada', 'Entrante')")  # DS_DIRECTION IN ('De entrada', 'Entrante')
    interactions_ono = interactions_ono.filter(col("CO_TYPE").rlike('(?i)^Llamada Telefono$|^Telefonica$|$\.\.\.^'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^BO'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^B\.O'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Emis'))
    interactions_ono = interactions_ono.filter(
        ~col("DS_REASON_1").rlike('(?i)^Gestion B\.O\.$|(?i)^Gestion casos$|(?i)^Gestion Casos Resueltos$|' \
                                  '(?i)^Gestion Casos Resueltos$|(?i)^Gestion documental$|(?i)^Gestion documental fax$|' \
                                  '(?i)^2ª Codificación$|(?i)^BAJ_BO Televenta$|(?i)^BAJ_B\.O\. Top 3000$|(?i)^BBOO$' \
                                  '(?i)^BO Fraude$|(?i)^B.O Gestion$|(?i)^B.O Portabilidad$|(?i)^BO Scoring$|' \
                                  '(?i)^Bo Scoring Permanencia$|(?i)^Consulta ficha$|(?i)^Callme back$|' \
                                  '(?i)^Consultar ficha$|(?i)^Backoffice Reclamaciones$|(?i)^BACKOFFICE$|(?i)^BackOffice Retención$|(?i)^NBA$|'
                                  '(?i)^Ofrecimiento comercial$|(?i)^No Ofrecimiento$|(?i)^Porta Salientes Emp Info$|(?i)^Porta Salientes Emp Movil$|' \
                                  '(?i)^Porta Salientes Emp Fijo$|(?i)^Callmeback$|(?i)^Caso Improcedente$|(?i)^Caso Resuelto$|(?i)^Caso Mal  Enviado BO 123$|(?i)^Gestion BO$'))

    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^CIERRE RAPID'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^SMS FollowUP Always Solv'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Ilocalizable'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Detractor'))

    interactions_ono = interactions_ono \
        .withColumnRenamed('DS_REASON_1', 'INT_Tipo') \
        .withColumnRenamed('DS_REASON_2', 'INT_Subtipo') \
        .withColumnRenamed('DS_REASON_3', 'INT_Razon') \
        .withColumnRenamed('DS_RESULT', 'INT_Resultado') \
        .withColumnRenamed('DS_DIRECTION', 'DIRECTION') \
        .withColumnRenamed('CO_TYPE', 'TYPE_TD') \
        .withColumn('INT_Tipo', upper(col('INT_Tipo'))) \
        .withColumn('INT_Subtipo', upper(col('INT_Subtipo'))) \
        .withColumn('INT_Razon', upper(col('INT_Razon'))) \
        .withColumn('INT_Resultado', upper(col('INT_Resultado')))

    interactions_ono_msisdn = interactions_ono.withColumnRenamed('DS_X_PHONE_CONSULTATION', 'msisdn')
    interactions_ono_target = interactions_ono_msisdn.join(select_trip.select('INT_Tipo', 'INT_Subtipo', 'INT_Razon').distinct(), ['INT_Tipo', 'INT_Subtipo', 'INT_Razon'],'inner')

    if verbose:
        interactions_ono_target = interactions_ono_target.cache()
        print("Number of msisdn with price CCC: {}".format(interactions_ono_target.count()))

    from churn_nrt.src.data.customers_data import Customer
    from churn_nrt.src.data.services_data import Service

    df_serv = Service(spark).get_module(date_, save=False, save_others=False, force_gen=True)
    df_cust = Customer(spark).get_module(date_, save=False, save_others=False, force_gen=True)

    df_services = df_serv.join(df_cust.select('NUM_CLIENTE', 'NIF_CLIENTE'), ['NUM_CLIENTE'], 'inner') \
        .withColumnRenamed('NUM_CLIENTE', 'num_cliente') \
        .withColumnRenamed('NIF_CLIENTE', 'nif_cliente').select(["msisdn", "num_cliente", "nif_cliente"])

    interactions_ono_target_nif = interactions_ono_target.join(df_services.select("msisdn","num_cliente", "nif_cliente"), ['msisdn'], 'inner')

    label_calls_agg = interactions_ono_target_nif.groupBy('num_cliente').agg(countDistinct('CL_OBJID').alias('label_calls'))

    if verbose:
        label_calls_agg = label_calls_agg.cache()
        print("Number of NIFs with price CCC: {}".format(label_calls_agg.count()))

    return label_calls_agg

def get_price_target(spark, date_, horizon=4, gap_window = 4, gap_label = 4, verbose=False):

    ########## Labels at NIF level ##########
    from churn_nrt.src.data.sopos_dxs import Target
    target_nifs_train = Target(spark, churn_window=(horizon+gap_window+gap_label)*7, level='num_cliente').get_module(date_, save=False, save_others=False, force_gen=False)

    ############### CCC ###############
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ccc_label = get_ccc_label(spark, date_, horizon, gap_window , gap_label, verbose).withColumn('label_ccc', when(col('label_calls')>0,1.0).otherwise(0.0))

    label_join = target_nifs_train.join(ccc_label,['num_cliente'], 'left').fillna({'label_ccc':0.0})
    label_join = label_join.withColumn('label_price', when(((col('label') + col('label_ccc'))>0), 1.0).otherwise(0.0))
    return label_join


def get_customers_with_increment(spark, date1_, date2_):
    filt_billing_before_df = get_filt_billing_info(spark, date1_)

    filt_billing_after_df = get_filt_billing_info(spark, date2_)

    # Selecting the feats used for join and computation of the price increment

    feats = ['num_cliente',
             'nif_cliente',
             'total_a_pagar',
             'Importe_deuda',
             'nb_fbb_services_nif',
             'nb_mobile_services_nif',
             'nb_tv_services_nif',
             'nb_prepaid_services_nif',
             'nb_bam_mobile_services_nif',
             'nb_fixed_services_nif',
             'nb_bam_services_nif',
             'nb_rgus_nif']

    sel_billing_before_df = filt_billing_before_df \
        .select(feats) \
        .withColumnRenamed('total_a_pagar', 'total_a_pagar_before') \
        .withColumnRenamed('Importe_deuda', 'Importe_deuda_before')

    sel_billing_after_df = filt_billing_after_df \
        .select(feats) \
        .withColumnRenamed('total_a_pagar', 'total_a_pagar_after') \
        .withColumnRenamed('Importe_deuda', 'Importe_deuda_after')

    # Joining both pictures: before and after the increment (customers with the same number of services)

    billing_comparison_df = sel_billing_after_df.join(sel_billing_before_df, ['NUM_CLIENTE',
                                                                              'nif_cliente',
                                                                              'nb_fbb_services_nif',
                                                                              'nb_mobile_services_nif',
                                                                              'nb_tv_services_nif',
                                                                              'nb_prepaid_services_nif',
                                                                              'nb_bam_mobile_services_nif',
                                                                              'nb_fixed_services_nif',
                                                                              'nb_bam_services_nif',
                                                                              'nb_rgus_nif'], 'inner')

    # Including inc_billing (absolute increment) and rel_inc_billing (relative increment) in the dataframe:
    # - Only customers without debt are retained)
    # - Only increments in [-100, 100] are considered
    # - Only customers with "total_a_pagar" > 0 before the increment are considered

    inc_billing_df = billing_comparison_df \
        .withColumn('inc_billing', col('total_a_pagar_after') - col('total_a_pagar_before')) \
        .withColumn('rel_inc_billing', col('inc_billing') / col('total_a_pagar_before')) \
        .filter(
        (col('total_a_pagar_before') > 0) & (col('Importe_deuda_after') == 0) & (col('Importe_deuda_before') == 0) & (
                    col('inc_billing') >= -100.0) & (col('inc_billing') <= 100.0)) \
        # .withColumn('flag_rel_inc_billing',
    #            when(col('rel_inc_billing') < 0, -1)
    #                      .otherwise(when(col('rel_inc_billing') == 0, 0)
    #                                .otherwise(when((col('rel_inc_billing') > 0) & (col('rel_inc_billing') <= 1), 1)
    #                                           .otherwise(when(col('rel_inc_billing') > 1, 2)))))

    # retaining customers with an increment in (0%, 10%]

    target_customers = inc_billing_df \
        .filter((col('rel_inc_billing') > 0) & (col('rel_inc_billing') <= 0.8)) \
        .select('num_cliente', 'nif_cliente', 'inc_billing', 'rel_inc_billing')

    target_customers = target_customers.cache()
    print"Number of num_cliente with billing inc: " + str(target_customers.count())

    from churn_nrt.src.utils.date_functions import get_next_cycle, get_previous_cycle
    next_cycle = "20191130"#get_previous_cycle(date1_)

    path_to_load = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/year={}/month={}/day={}' \
        .format(int(next_cycle[:4]), int(next_cycle[4:6]), int(next_cycle[6:8]))

    df_ids = spark.read.load(path_to_load)

    target_customers = target_customers \
        .join(df_ids.filter(col('Serv_RGU').isin('mobile', 'fbb')).select('num_cliente','msisdn', 'Cust_SUPEROFERTA','Serv_tariff'),
              ['num_cliente'], 'inner')

    target_customers = target_customers.where(col('Cust_SUPEROFERTA') == 'ON15').select('num_cliente', 'nif_cliente',
                                                                                        'inc_billing',
                                                                                        'rel_inc_billing', 'Serv_tariff', 'msisdn')

    target_customers = target_customers.cache()
    print"Number of msisdns with ON15 and billing inc: " + str(target_customers.count())

    import pandas as pd
    from churn_nrt.src.projects.models.price_sensitivity.price_elasticity.constants import TARIFF_INC_FILT_DICT
    tariff_inc_dict = TARIFF_INC_FILT_DICT
    keys_1 = tariff_inc_dict.keys()
    vals_abs = [f[0] for f in tariff_inc_dict.values()]
    vals_rel = [f[1] for f in tariff_inc_dict.values()]
    data = {'tariff': keys_1, 'rel_inc': vals_rel, 'abs_inc': vals_abs}
    tariffs_inc = spark.createDataFrame(pd.DataFrame(data))

    df_tariffs = target_customers.join(tariffs_inc, target_customers.Serv_tariff == tariffs_inc.tariff, "inner")

    df_tariffs = df_tariffs.cache()
    print"Number of msisdns with ON15, billing inc and selected tariffs: " + str(df_tariffs.count())

    df_tariffs_nc = df_tariffs.groupBy('num_cliente').agg(*([sql_sum(c).alias('agg_sum_' + c.lower())
          for c in ['rel_inc','abs_inc']]+[sql_avg(c).alias('agg_mean_' + c.lower()) for c in ['rel_inc','abs_inc']] + \
          [sql_max(c).alias('agg_max_' + c.lower()) for c in ['rel_inc', 'abs_inc']]))

    df_tariffs_nc = df_tariffs_nc.cache()
    print"Number of num_clientes with ON15, billing inc and selected tariffs: " + str(df_tariffs_nc.count())

    return df_tariffs_nc

def get_filt_billing_info(spark, date_):
    '''
    Stable (non-recent) customers including billing columns
    '''
    from churn_nrt.src.data.customer_base import CustomerBase
    base_before_df = CustomerBase(spark).get_module(date_, save=False, save_others=False, force_gen=True)
    base_before_df.cache()
    print base_before_df.count()

    from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter

    non_recent_df = get_non_recent_customers_filter(spark, date_, 90, level='nif', verbose=False, only_active=True) \
        .select('nif_cliente') \
        .distinct()

    nif_feats = ["nb_fbb_services_nif",
                 "nb_mobile_services_nif",
                 "nb_tv_services_nif",
                 "nb_prepaid_services_nif",
                 "nb_bam_mobile_services_nif",
                 "nb_fixed_services_nif",
                 "nb_bam_services_nif",
                 "nb_rgus_nif"]

    base_before_df = base_before_df \
        .select(["nif_cliente", "num_cliente"] + nif_feats).distinct() \
        .withColumn("nb_numclis", count("*").over(Window.partitionBy("nif_cliente"))) \
        .filter(col("nb_numclis") == 1).select(["nif_cliente", "num_cliente"] + nif_feats).distinct()

    filt_base_df = base_before_df.join(non_recent_df, ['nif_cliente'], 'inner')

    filt_base_df.cache()

    print "size: " + str(filt_base_df.count())
    print "num_cliente: " + str(filt_base_df.select("num_cliente").distinct().count())
    print "nif_cliente: " + str(filt_base_df.select("nif_cliente").distinct().count())

    billing_before_df = get_billing_customer_df(spark, date_) \
        .join(filt_base_df, ['num_cliente'], 'inner')

    billing_before_df.cache()

    print "size: " + str(billing_before_df.count())
    print "num_cliente: " + str(billing_before_df.select("num_cliente").distinct().count())

    return billing_before_df
def get_billing_customer_df(spark, closing_day):
    """
    Info related to the last bill
    """
    path_billing = '/data/raw/vf_es/billingtopsups/POSTBILLSUMOW/1.1/parquet/'
    path_fictional = '/data/raw/vf_es/customerprofilecar/CUSTFICTICALOW/1.0/parquet/'

    data_billing_numcliente_ori = spark \
        .read \
        .parquet(path_billing) \
        .where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    # Para cada numero de factura nos quedamos con la última versión de cada factura (una misma factura puede sufrir modificaciones y de ahí las diferentes versiones)
    w_bill_1 = Window().partitionBy("numero_factura").orderBy(desc("year"), desc("month"), desc("day"))
    data_billing_numcliente_tmp1 = (data_billing_numcliente_ori
                                    .withColumn("rowNum", row_number().over(w_bill_1))
                                    .where(col('rowNum') == 1)
                                    .withColumn('fecha_facturacion', (
        unix_timestamp(col('fecha_facturacion'), 'yyyyMMdd').cast("timestamp")))
                                    .withColumn('month_billing', month('fecha_facturacion'))
                                    .withColumn('year_billing', year('fecha_facturacion'))
                                    .withColumn('yearmonth_billing',
                                                concat(col('year_billing'), lpad(col('month_billing'), 2, '0')))
                                    .withColumn('net_charges', col('InvoiceCharges') - col('totalimpuesto'))
                                    )

    # Un cliente, aparte de su id de cliente, puede tener un id de cliente ficticio
    data_cust_fict = spark \
        .read \
        .parquet(path_fictional) \
        .where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

    # Nos quedamos con el ultimo id de cliente ficticio
    w_cust_fict_1 = Window().partitionBy("NUM_CLIENTE_FICTICIO").orderBy(desc("year"), desc("month"), desc("day"))
    data_cust_fict_tmp1 = (data_cust_fict
                           .withColumn("rowNum", row_number().over(w_cust_fict_1))
                           .where(col('rowNum') == 1)
                           )

    data_billing_fict_joined = (data_billing_numcliente_tmp1
                                # A cada factura de cliente, le juntamos id de cliente ficticio
                                .join(data_cust_fict_tmp1, (col('NUM_CLIENTE_FICTICIO') == col('customeraccount')),
                                      'leftouter')
                                # A cada factura de cliente, si viene con id de cliente ficticio, le ponemos su id de cliente real
                                .withColumn('NUM_CLIENTE_CALC', coalesce('NUM_CLIENTE', 'customeraccount'))
                                .groupBy('NUM_CLIENTE_CALC', 'yearmonth_billing')
                                # A cada cliente agregamos las facturas de un mismo ciclo
                                .agg(sql_sum('InvoiceCharges').alias('InvoiceCharges'),
                                     sql_sum('total_a_pagar').alias('total_a_pagar'),
                                     sql_sum('totalimpuesto').alias('totalimpuesto'),
                                     sql_sum('Importe_deuda').alias('Importe_deuda'),
                                     sql_sum('net_charges').alias('net_charges'),
                                     sql_max('fecha_facturacion').alias('fecha_facturacion_min'),
                                     sql_min('fecha_facturacion').alias('fecha_facturacion_max'),
                                     countDistinct('NUM_CLIENTE_FICTICIO').alias('num_ids_fict'),
                                     # Cuantos ids ficticios aparecian
                                     countDistinct('numero_factura').alias('num_facturas')  # Cuantas facturas tenia
                                     )
                                )

    # La última factura
    data_billing_fict_joined = data_billing_fict_joined \
        .withColumn("num_factura",
                    row_number().over(Window().partitionBy("NUM_CLIENTE_CALC").orderBy(desc("yearmonth_billing")))) \
        .filter(col("num_factura") == 1).withColumnRenamed('NUM_CLIENTE_CALC', 'NUM_CLIENTE')

    return data_billing_fict_joined


class PriceSensitivityCar(DataTemplate):  # unlabeled

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "price_sensitivity_car_filtered_tariffs")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False, labeled=True):

        if labeled:
            df_target_ncs = get_customers_with_increment(self.SPARK, "20191125", "20200125")
            #df_target_nifs = get_target_nifs_ids(self.SPARK, closing_day, filter_recent=True, filter_disc=True, filter_ord=True,filter_ccc=True, n_cycles=12, horizon=4, gap_window=4,verbose=False)
        else:
            from churn_nrt.src.data_utils.ids_utils import get_filtered_ids
            df_target_ncs = get_filtered_ids(self.SPARK, closing_day, filter_recent=True, filter_disc=True, filter_ord=True, filter_cc=True, n_cycles=12, level='num_cliente', verbose=False)
        from churn_nrt.src.data_utils.ids_utils import get_filtered_ids, get_ids_nc
        df_filtered_ncs = get_filtered_ids(self.SPARK, closing_day, filter_recent=True, filter_disc=True, filter_ord=True,filter_cc=True, n_cycles=12, level='num_cliente', verbose=False)
        df_ids = get_ids_nc(self.SPARK, closing_day).where(col("CCC_num_calls_w8")==0)

        df = df_target_ncs.join(df_ids, ['num_cliente'], 'inner')
        print"Size of df before filters: " + str(df.count())
        df = df.join(df_filtered_ncs, ['num_cliente'], 'inner')
        print"Size of df after filters: " + str(df.count())

        return df



class PriceSensitivity(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, segment, quantile, force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)
        self.MODE = mode_
        self.SEGMENT = segment
        self.PRICE_RANGE = quantile
    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None,  *args, **kwargs):
        from pyspark.sql.functions import count
        if closing_day == "20191131":
            df = PriceSensitivityCar(self.SPARK).get_module("20191130", save=True, save_others=True,labeled=labeled).fillna(0.0)
            df = df.withColumn('blindaje', lit('none')).withColumn('blindaje', when((col("tgs_days_until_f_fin_bi_agg_mean") >= -60) & (col("tgs_days_until_f_fin_bi_agg_mean") <= 60),'soft').otherwise(col('blindaje'))) \
            .withColumn('blindaje',when((col("tgs_days_until_f_fin_bi_agg_mean") > 60), 'hard').otherwise(col('blindaje')))

            if self.PRICE_RANGE == "high":
                df = df.where(col("Bill_N1_Amount_To_Pay") > 63.96)
            elif self.PRICE_RANGE == "medium":
                df = df.where(col("Bill_N1_Amount_To_Pay") >= 39.3).where(col("Bill_N1_Amount_To_Pay") <= 63.96)
            elif self.PRICE_RANGE == "low":
                df = df.where(col("Bill_N1_Amount_To_Pay") < 39.3)
            else:
                import sys
                print("Error: price quantile must be high, medium or low")
                sys.exit(-1)

            df_ = df
            [tr_input_df, df_val] = df.randomSplit([0.7, 0.3], 1234)
            df_ = df_.where(col('blindaje') == self.SEGMENT)
            df_ = df_.cache()
            print('Final prepared df size: {}'.format(df_val.count()))
            target_nc = get_price_target(self.SPARK, "20191130", horizon=4, gap_window=1, gap_label=4,verbose=False).withColumnRenamed('label', 'label_churn')
            df = df_.join(target_nc, on=["num_cliente"], how="inner").withColumnRenamed('label_price', 'label').fillna({'label': 0.0})
            df.groupBy('label').agg(count('*')).show()
            return df
        ##### Evaluation mode is not tested yet #####
        if self.MODE == "evaluation":
            print"Closing_day: " + closing_day
            df_ = PriceSensitivityCar(self.SPARK).get_module("20191130", save=True, save_others=False, labeled= labeled).fillna(0.0)
            if self.PRICE_RANGE == "high":
                df_ = df_.where(col("Bill_N1_Amount_To_Pay")>63.96)
            elif self.PRICE_RANGE == "medium":
                df_ = df_.where(col("Bill_N1_Amount_To_Pay") >= 39.3).where(col("Bill_N1_Amount_To_Pay")<=63.96)
            elif self.PRICE_RANGE == "low":
                df_ = df_.where(col("Bill_N1_Amount_To_Pay") < 39.3)
            else:
                import sys
                print("Error: price quantile must be high, medium or low")
                sys.exit(-1)

            df_ = df_.withColumn('blindaje', lit('none'))\
    .withColumn('blindaje', when((col("tgs_days_until_f_fin_bi_agg_mean") >= -60) & (col("tgs_days_until_f_fin_bi_agg_mean") <= 60), 'soft').otherwise(col('blindaje')))\
    .withColumn('blindaje',  when((col("tgs_days_until_f_fin_bi_agg_mean") > 60), 'hard').otherwise(col('blindaje')))
            df_ = df_.where(col('blindaje')==self.SEGMENT)
            target_nif = get_price_target(self.SPARK, "20191130", horizon=4, gap_window = 1, gap_label= 4,  verbose=False).withColumnRenamed('label','label_churn')
            df = df_.join(target_nif, on=["num_cliente"], how="inner").withColumnRenamed('label_price','label').fillna({'label': 0.0})
            df.groupBy('label_churn', 'label_ccc', 'label').agg(count('*')).show()
            [df_train, df_test] = df.randomSplit([0.7, 0.3], 1234)
            print('Final prepared df size: {}'.format(df.count()))
            if set_name == "train":
                df = df
                #df.groupBy('label_churn', 'label_ccc', 'label').agg(count('*')).show()
                df.groupBy('label').agg(count('*')).show()
            else:
                df = df
                #df.groupBy('label_churn', 'label_ccc', 'label').agg(count('*')).show()
                df.groupBy('label').agg(count('*')).show()
        elif self.MODE == "production":
            if set_name == "train":
                df = PriceSensitivityCar(self.SPARK).get_module("20191130", save=True, save_others=True,labeled=labeled).fillna(0.0)
                df = df.withColumn('blindaje', lit('none')) \
                    .withColumn('blindaje', when((col("tgs_days_until_f_fin_bi_agg_mean") >= -60) & (col("tgs_days_until_f_fin_bi_agg_mean") <= 60),'soft').otherwise(col('blindaje'))) \
                    .withColumn('blindaje',when((col("tgs_days_until_f_fin_bi_agg_mean") > 60), 'hard').otherwise(col('blindaje')))
                if self.PRICE_RANGE == "high":
                    df = df.where(col("Bill_N1_Amount_To_Pay")>63.96)
                elif self.PRICE_RANGE == "medium":
                    df = df.where(col("Bill_N1_Amount_To_Pay") >= 39.3).where(col("Bill_N1_Amount_To_Pay")<=63.96)
                elif self.PRICE_RANGE == "low":
                    df = df.where(col("Bill_N1_Amount_To_Pay") < 39.3)
                else:
                    import sys
                    print("Error: price quantile must be high, medium or low")
                    sys.exit(-1)

                df = df.where(col('blindaje') == self.SEGMENT).drop_duplicates(['num_cliente'])
                df = df.cache()
                print('Final prepared df size: {}'.format(df.count()))
                target_nc = get_price_target(self.SPARK, "20191130", horizon=4, gap_window=1, gap_label=4,verbose=False).withColumnRenamed('label','label_churn')
                df = df.join(target_nc, on=["num_cliente"], how="inner").withColumnRenamed('label_price','label').fillna({'label': 0.0})
                df.groupBy('label').agg(count('*')).show()

            else:
                from churn_nrt.src.data_utils.ids_utils import get_filtered_ids
                df = get_filtered_ids(self.SPARK, closing_day, filter_recent=True, filter_disc=True, filter_ord=True, filter_cc=True, n_cycles=12, level='num_cliente',verbose=False)
                from churn_nrt.src.data_utils.ids_utils import get_ids_nc
                df_ids = get_ids_nc(self.SPARK, closing_day)
                df = df.join(df_ids, ['num_cliente'], 'inner')
                df = df.withColumn('blindaje', lit('none')).withColumn('blindaje', when((col("tgs_days_until_f_fin_bi_agg_mean") >= -60) & (col("tgs_days_until_f_fin_bi_agg_mean") <= 60),
                'soft').otherwise(col('blindaje'))).withColumn('blindaje', when((col("tgs_days_until_f_fin_bi_agg_mean") > 60), 'hard').otherwise(col('blindaje')))
                df = df.where(col('blindaje') == self.SEGMENT).drop_duplicates(['num_cliente'])
                if self.PRICE_RANGE == "high":
                    df = df.where(col("Bill_N1_Amount_To_Pay")>63.96)
                elif self.PRICE_RANGE == "medium":
                    df = df.where(col("Bill_N1_Amount_To_Pay") >= 39.3).where(col("Bill_N1_Amount_To_Pay")<=63.96)
                elif self.PRICE_RANGE == "low":
                    df = df.where(col("Bill_N1_Amount_To_Pay") < 39.3)
        else:
            print("Error: mode must be evaluation or production")
            import sys
            sys.exit(-1)
        return df
