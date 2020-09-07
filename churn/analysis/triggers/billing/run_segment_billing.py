from pyspark.mllib.evaluation import BinaryClassificationMetrics
import pandas as pd

from pyspark.sql.functions import when, udf, size, array, coalesce, length, max as sql_max, count as sql_count, sum as sql_sum, col, lit, desc, greatest, least, \
    mean as sql_mean, concat_ws, regexp_replace, split, datediff,\
    concat, lit, lpad, row_number, unix_timestamp, month, year, coalesce, from_unixtime, sum as sql_sum, min as sql_min, countDistinct, collect_list


from pyspark.sql.window import Window


from pyspark.sql.types import FloatType, ArrayType, IntegerType
import numpy as np
import datetime as dt
import time

MODEL_TRIGGERS_ORDERS = "triggers_orders"

path_raw_base = '/data/raw/vf_es/'
PATH_RAW_BILLING = path_raw_base + 'billingtopsups/POSTBILLSUMOW/1.1/parquet/'
PATH_RAW_FICTIONAL_CUSTOMER = path_raw_base + 'customerprofilecar/CUSTFICTICALOW/1.0/parquet/'


# Copied to use-cases/churn_nrt/src/data/billing.py
def get_triggers_billing_saving_path():
    return  "/data/udf/vf_es/churn/triggers/billing/" # unlabeled

# Copied to use-cases/churn_nrt/src/data/billing.py
def save_trigger_billing_car(df, closing_day):

    path_to_save = get_triggers_billing_saving_path()

    df = df.withColumn("day", lit(int(closing_day[6:])))
    df = df.withColumn("month", lit(int(closing_day[4:6])))
    df = df.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {} for closing_day={}".format(path_to_save, closing_day))
    (df.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved - {} for closing_day={}".format(path_to_save, closing_day))

def __get_billing_df2(spark, closing_day):
    """
    The functions extract billing information from raw data
    :return: a dataframe containing last 5 bills information for each customer
    """
    print '[Info __get_billing_df2] Calculating Basic Billing (customer level) Information ...'
    data_billing_numcliente_ori = (spark.read.parquet(PATH_RAW_BILLING)
                                   .where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)))

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

    data_cust_fict = (spark.read.parquet(PATH_RAW_FICTIONAL_CUSTOMER)
                      .where(
        (concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))
                      )

    w_cust_fict_1 = Window().partitionBy("NUM_CLIENTE_FICTICIO").orderBy(desc("year"), desc("month"), desc("day"))
    data_cust_fict_tmp1 = (data_cust_fict
                           .withColumn("rowNum", row_number().over(w_cust_fict_1))
                           .where(col('rowNum') == 1)
                           )

    data_billing_fict_joined = (data_billing_numcliente_tmp1
                                .join(data_cust_fict_tmp1, (col('NUM_CLIENTE_FICTICIO') == col('customeraccount')),
                                      'leftouter')
                                .withColumn('NUM_CLIENTE_CALC', coalesce('NUM_CLIENTE', 'customeraccount'))
                                .groupBy('NUM_CLIENTE_CALC', 'yearmonth_billing')
                                .agg(sql_sum('InvoiceCharges').alias('InvoiceCharges')
                                     , sql_sum('total_a_pagar').alias('total_a_pagar')
                                     , sql_sum('totalimpuesto').alias('totalimpuesto')
                                     , sql_sum('Importe_deuda').alias('Importe_deuda')
                                     , sql_sum('net_charges').alias('net_charges')
                                     , sql_max('fecha_facturacion').alias('fecha_facturacion')
                                     , sql_min('fecha_facturacion').alias('fecha_facturacion')
                                     , countDistinct('NUM_CLIENTE_FICTICIO').alias('num_ids_fict')
                                     , countDistinct('numero_factura').alias('num_facturas')
                                     )
                                )

    w_bill_2 = Window().partitionBy("NUM_CLIENTE_CALC").orderBy(desc("yearmonth_billing"))
    data_billing_numcliente_tmp2 = (data_billing_fict_joined
                                    .withColumn("num_factura", row_number().over(w_bill_2))
                                    .withColumn('to_pivot', concat(lit('Bill_N'), col('num_factura')))
                                    .where(col('num_factura') <= 5)
                                    )

    values_bill_pivot = ['Bill_N1', 'Bill_N2', 'Bill_N3', 'Bill_N4', 'Bill_N5', ]
    data_billing_numcliente_tmp3 = (data_billing_numcliente_tmp2
                                    .groupBy('NUM_CLIENTE_CALC')
                                    .pivot('to_pivot', values_bill_pivot)
                                    .agg(
        # MUST: InvoiceCharges+Debt_Amount=Amount_To_Pay
        sql_max("InvoiceCharges").alias("InvoiceCharges"),
        sql_max("total_a_pagar").alias("Amount_To_Pay"),
        sql_max('totalimpuesto').alias("Tax_Amount"),
        sql_max('Importe_deuda').alias("Debt_Amount"),
        sql_max('net_charges').alias("net_charges"),
        sql_max('num_ids_fict').alias('num_ids_fict'),
        sql_max('num_facturas').alias('num_facturas'),
        sql_max('yearmonth_billing').alias('yearmonth_billing')
    )
                                    .na.fill(0)
                                    .withColumnRenamed('NUM_CLIENTE_CALC', 'NUM_CLIENTE'))

    # Prefix column names
    df_oldColumns = data_billing_numcliente_tmp3.columns
    df_newColumns = [
        'Bill_' + c if c not in ['NUM_CLIENTE', 'partitioned_month', 'year', 'month', 'day'] and not c.startswith(
            'Bill_') else c for c in data_billing_numcliente_tmp3.columns]
    # data_billing_numcliente_TMP3 = reduce(lambda df, idx: df.withColumnRenamed(df_oldColumns[idx], df_newColumns[idx]),
    #                                      xrange(len(df_oldColumns)), data_billing_numcliente_tmp3)

    data_billing_numcliente_tmp3 = data_billing_numcliente_tmp3.toDF(*df_newColumns)
    print '[Info __get_billing_df2] Calculating Basic Billing (customer level) Information Finished ...'

    return data_billing_numcliente_tmp3

# Copied to use-cases/churn_nrt/src/data/billing.py
def __get_billing_df(spark, closing_day):


    data_billing_numcliente_ori = (spark.read.parquet(PATH_RAW_BILLING)
                         .where((concat(col('year'),lpad(col('month'), 2, '0'),lpad(col('day'),2, '0'))<=closing_day))
                        )

    w_bill_1 = Window().partitionBy("numero_factura").orderBy(desc("year"), desc("month"), desc("day"))
    data_billing_numcliente_tmp1 = (data_billing_numcliente_ori
                                    .withColumn("rowNum", row_number().over(w_bill_1))
                                    .where(col('rowNum') == 1)
                                    .withColumn('fecha_facturacion', (unix_timestamp(col('fecha_facturacion'), 'yyyyMMdd').cast("timestamp")))
                                    .withColumn('month_billing', month('fecha_facturacion'))
                                    .withColumn('year_billing', year('fecha_facturacion'))
                                    .withColumn('yearmonth_billing', concat(col('year_billing'), lpad(col('month_billing'), 2, '0')))
                                    .withColumn('net_charges', col('InvoiceCharges') - col('totalimpuesto'))
                                    )

    data_cust_fict = (spark.read.parquet(PATH_RAW_FICTIONAL_CUSTOMER)
                      .where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))
                      )

    w_cust_fict_1 = Window().partitionBy("NUM_CLIENTE_FICTICIO").orderBy(desc("year"), desc("month"), desc("day"))
    data_cust_fict_tmp1 = (data_cust_fict
                           .withColumn("rowNum", row_number().over(w_cust_fict_1))
                           .where(col('rowNum') == 1)
                           )

    data_billing_fict_joined = (data_billing_numcliente_tmp1
                                .join(data_cust_fict_tmp1, (col('NUM_CLIENTE_FICTICIO') == col('customeraccount')), 'leftouter')
                                .withColumn('NUM_CLIENTE_CALC', coalesce('NUM_CLIENTE', 'customeraccount'))
                                .withColumn("days_since_bill", datediff(from_unixtime(unix_timestamp(lit(closing_day),"yyyyMMdd")), col("fecha_facturacion")).astype("double"))
                                .groupBy('NUM_CLIENTE_CALC', 'yearmonth_billing')
                                .agg(  sql_sum('InvoiceCharges').alias('InvoiceCharges'),
                                      sql_sum('total_a_pagar').alias('total_a_pagar'),
                                      sql_sum('totalimpuesto').alias('totalimpuesto'),
                                      sql_sum('Importe_deuda').alias('Importe_deuda'),
                                      sql_sum('net_charges').alias('net_charges'),
                                      #sql_max('days_since_bill').alias('days_since_1st_bill'),
                                      sql_min('days_since_bill').alias('days_since_last_bill'),
                                      countDistinct('NUM_CLIENTE_FICTICIO').alias('num_ids_fict'),
                                      countDistinct('numero_factura').alias('num_facturas')
                                     )
                                )

    w_bill_2 = Window().partitionBy("NUM_CLIENTE_CALC").orderBy(desc("yearmonth_billing"))
    data_billing_numcliente_tmp2 = (data_billing_fict_joined
                                    .withColumn("num_factura", row_number().over(w_bill_2))
                                    .withColumn('to_pivot', concat(lit('Bill_N'), col('num_factura')))
                                    .where(col('num_factura') <= 5)
                                    )

    values_bill_pivot = ['Bill_N1', 'Bill_N2', 'Bill_N3', 'Bill_N4', 'Bill_N5']
    df_billing = (data_billing_numcliente_tmp2
                                    .groupBy('NUM_CLIENTE_CALC')
                                    .pivot('to_pivot', values_bill_pivot)
                                    .agg(
                                        # MUST: InvoiceCharges+Debt_Amount=Amount_To_Pay
                                        sql_max("InvoiceCharges").alias("InvoiceCharges"),
                                        sql_max("total_a_pagar").alias("Amount_To_Pay"),
                                        sql_max('totalimpuesto').alias("Tax_Amount"),
                                        sql_max('Importe_deuda').alias("Debt_Amount"),
                                        sql_max('net_charges').alias("net_charges"),
                                        sql_max('num_ids_fict').alias('num_ids_fict'),
                                        sql_max('num_facturas').alias('num_facturas'),
                                        sql_max('days_since_last_bill').alias('days_since'),
                                        sql_max('yearmonth_billing').alias('yearmonth_billing')
                                        )
                                    #.na.fill(None)
                                    .withColumnRenamed('NUM_CLIENTE_CALC', 'NUM_CLIENTE'))

    return df_billing


def __prepare_billing(df):

    remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] == milista[ii] and milista[ii]!=None]), ArrayType(FloatType()))
    import numpy as np
    avg_bw_elems_udf = udf(lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())
    diff_2_vs_1_bw_elems_udf = udf(lambda milista: [float(milista[ii + 1] - milista[ii]) for ii in range(len(milista) - 1)], ArrayType(FloatType()))

    diff_bw_elems_udf = udf(lambda milista: [float(milista[ii] - milista[ii + 1]) for ii in range(len(milista) - 1)], ArrayType(FloatType()))

    stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
    #mean_udf = udf(lambda milista: float(np.mean(milista)), FloatType())
    max_udf = udf(lambda milista: float(np.max(milista)) if milista else None, FloatType())
    min_udf = udf(lambda milista: float(np.min(milista)) if milista else None, FloatType())


    mean_range_udf = udf(lambda lista,idx: float(np.mean(lista[:idx])), FloatType())
    negative_udf = udf(lambda lista: 1 if any([ii<0 for ii in lista]) else 0, IntegerType())

    # debt is negative when Vodafone has the debt
    df = (df.withColumn("aux_completed", array([_col for _col in df.columns if _col.endswith("days_since")]))
            .withColumn("aux_completed", remove_undefined_udf(col("aux_completed")))
            .withColumn("billing_nb_last_bills", when(col("aux_completed").isNotNull(), size(col("aux_completed"))).otherwise(0))
            .withColumn("billing_avg_days_bw_bills", when(col("billing_nb_last_bills") > 1, avg_bw_elems_udf(col("aux_completed"))).otherwise(-1.0))

            .withColumn("diff_bw_days_bills", when((col("aux_completed").isNotNull()) & (col("billing_nb_last_bills") > 1), diff_2_vs_1_bw_elems_udf(col("aux_completed"))).otherwise(None))
            .withColumn("billing_max_days_bw_bills", when(((col("aux_completed").isNotNull()) & (col("billing_nb_last_bills") > 1)), max_udf(col("diff_bw_days_bills"))).otherwise(None))
            .withColumn("billing_min_days_bw_bills",    when(((col("aux_completed").isNotNull()) & (col("billing_nb_last_bills") > 1)), min_udf(col("diff_bw_days_bills"))).otherwise(None))

            .withColumn("aux_invoice", array([_col for _col in df.columns if _col.endswith("_InvoiceCharges") and _col.startswith("Bill_N")]))
            .withColumn("aux_invoice", remove_undefined_udf(col("aux_invoice")))
            .withColumn("nb_invoices", when(col("aux_invoice").isNotNull(), size(col("aux_invoice"))).otherwise(0))
            .withColumn("diff_bw_bills", when(  (col("aux_invoice").isNotNull()) & (col("nb_invoices") > 1), diff_bw_elems_udf(col("aux_invoice"))).otherwise(None))
            .withColumn("greatest_diff_bw_bills", when(col("nb_invoices")>1, max_udf(col("diff_bw_bills"))).otherwise(None))
            .withColumn("least_diff_bw_bills",    when(col("nb_invoices")>1, min_udf(col("diff_bw_bills"))).otherwise(None))
            .withColumn("billing_std", when(col("aux_invoice").isNotNull(), stddev_udf("aux_invoice")).otherwise(-1.0))
            .withColumn("billing_mean", when((col("aux_invoice").isNotNull()) & (col("billing_nb_last_bills") > 0),
                                         mean_range_udf("aux_invoice","billing_nb_last_bills")).otherwise(-1.0))
            .withColumn("billing_current_debt", col('Bill_N1_InvoiceCharges') + col('Bill_N1_Debt_Amount') - col('Bill_N1_Amount_To_Pay'))

            .withColumn("aux_debt", array([_col for _col in df.columns if _col.endswith("_Debt_Amount") and _col.startswith("Bill_N")]))
            .withColumn("billing_any_vf_debt", negative_udf("aux_debt"))
            .withColumn("billing_current_vf_debt", when(col("billing_current_debt")<0,1).otherwise(0))
            .withColumn("billing_current_client_debt", when(col("billing_current_debt") > 0, 1).otherwise(0))
            .withColumn("billing_settle_debt", when( (col('Bill_N1_Debt_Amount') > col('billing_current_debt')) & (col("billing_current_vf_debt")==0),1) # deb is decreasing
                                            .otherwise(0))

          )


    return df

# Copied to use-cases/churn_nrt/src/data/billing.py
def __get_tariffs_dfs(spark, closing_day):
    #from churn.datapreparation.general.data_loader import get_active_services
    from churn.analysis.triggers.base_utils.base_utils import get_active_services

    customer_cols = ["num_cliente", "superoferta", "nif_cliente"]
    service_cols = ["msisdn", "num_cliente", "tariff"]

    df_services = get_active_services(spark, closing_day, False, customer_cols, service_cols).drop("num_cliente_customer").withColumnRenamed("num_cliente_service", "num_cliente")
    df_services = df_services.withColumn("is_superoferta", when(col("SUPEROFERTA") == "ON19", 1).otherwise(0))

    df_tarifas_nc = (df_services.groupBy("num_cliente").agg(collect_list('tariff').alias("tarifas_list")))
    df_on19_nif = (df_services.groupBy("nif_cliente").agg(sql_sum("is_superoferta").alias("nb_superofertas")))

    df_customers = df_services.select("num_cliente", "nif_cliente").drop_duplicates()
    df_customers = df_customers.join(df_tarifas_nc, on=["num_cliente"], how="left")
    df_customers = df_customers.join(df_on19_nif, on=["nif_cliente"], how="left")

    return df_customers


def __get_customer_master_aux(spark, closing_day, days):
    from pykhaos.utils.date_functions import move_date_n_days
    from churn.analysis.triggers.orders.customer_master import get_customer_master

    closing_day_d = move_date_n_days(closing_day, n=days)
    df_cust_d = get_customer_master(spark, closing_day_d, unlabeled=True)
    from churn_nrt.src.data.customer_base import CustomerBase
    base = CustomerBase(spark).get_module(closing_day_d, force_gen=False)
    df_cust_d = df_cust_d.join(base.select('nif_cliente', 'num_cliente').distinct(),['nif_cliente'], 'inner').select("num_cliente", "nb_rgus", "nb_rgus_cycles_2", 'tgs_has_discount')
    df_cust_d = df_cust_d.drop_duplicates()
    df_cust_d = df_cust_d.withColumnRenamed("nb_rgus", "nb_rgus_d{}".format(abs(days)))
    df_cust_d = df_cust_d.withColumnRenamed("tgs_has_discount", "tgs_has_discount_d{}".format(abs(days)))
    df_cust_d = df_cust_d.withColumnRenamed("nb_rgus_cycles_2", "nb_rgus_d{}".format(abs(days) + 14))
    return df_cust_d

def filter_car(df_tar):
    print("Filtering churn_cancellations_w8")
    df_tar = df_tar.filter(col('CHURN_CANCELLATIONS_w8') == 0)
    print("Filtering superoferta ON19")
    df_tar = df_tar.where(col("nb_superofertas") == 0)

    return df_tar

def get_billing_module(spark, closing_day):
    # Return an unlabeled car


    path_to_car = get_triggers_billing_saving_path() + "year={}/month={}/day={}".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))
    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    if check_hdfs_exists(path_to_car):
        print("Found already trigger_orders_car - '{}'".format(path_to_car))
        df_tar_all = spark.read.parquet(path_to_car)
    else:
        df_tar_all = __build_car(spark, closing_day)

    return df_tar_all


# Copied to use-cases/churn_nrt/src/data/billing.py
def __build_car(spark, closing_day):
    '''
    Call "get_billing_module" instead of this function.
    :param spark:
    :param closing_day:
    :param force_gen:
    :return:
    '''

    from churn.analysis.triggers.orders.customer_master import get_customer_master
    from churn.analysis.triggers.orders.run_segment_orders import get_ccc_attrs_w8
    from churn.analysis.triggers.orders.customer_master import get_segment_msisdn_anyday
    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    # BASE DE CLIENTES
    df_customers = __get_tariffs_dfs(spark, closing_day)
    print("df_customers={} after get_tariffs_dfs {}".format(df_customers.count(), closing_day))
    #LATER df_customers.select(col("nb_superofertas")).groupby("nb_superofertas").agg(sql_count("*")).show()

    #df_customers = df_customers.where(col("nb_superofertas") == 0)  # keep num_cliente without ON19
    #print("df_customers={} filtering nb_superofertas = 0".format(df_customers.count()))
    # df_customers.columns = ['nif_cliente', 'num_cliente', 'tarifas_list', 'nb_superofertas']


    # Obtener llamadas en los ultimos dos meses
    df_base_msisdn = get_segment_msisdn_anyday(spark, closing_day)
    df_ccc_w8 = get_ccc_attrs_w8(spark, closing_day, df_base_msisdn)
    #df_ccc_w8 = df_ccc_w8.select("nif_cliente", "num_calls_w8", "CHURN_CANCELLATIONS_w8")

    # Filtrar nifs que tienen llamadas por churn
    df_customers = df_customers.join(df_ccc_w8, on=["nif_cliente"], how="left")
    #LATER df_customers = df_customers.filter(col('CHURN_CANCELLATIONS_w8') == 0)


    df_cust_c = get_customer_master(spark, closing_day, unlabeled=True).select("nif_cliente", "nb_rgus", "nb_rgus_cycles_2",
                                                                                'tgs_days_until_fecha_fin_dto', 'tgs_has_discount',
                                                                                'segment_nif')
    df_cust_c = df_cust_c.drop_duplicates()

    df_cust_c = df_cust_c.where(col("segment_nif") != "Pure_prepaid")

    # la cartera de referencia es sin prepago y sin las llamadas por churn cancellation
    df_cust_c = df_cust_c.where(col("segment_nif") != "Pure_prepaid")
    df_cust_c = df_cust_c.withColumnRenamed("nb_rgus", "nb_rgus_d")
    df_cust_c = df_cust_c.withColumnRenamed("nb_rgus_cycles_2", "nb_rgus_d14")

    #from churn.datapreparation.general.data_loader import get_active_services
    from churn.analysis.triggers.base_utils.base_utils import get_active_services
    df_active = (get_active_services(spark, closing_day, new=False, customer_cols=["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "nif_cliente"])
        .select("num_cliente", "nif_cliente").drop_duplicates()
                 #.withColumnRenamed("num_cliente_customer", "num_cliente")
                 )

    print("before join active df_cust_c={}".format(df_cust_c.count()))
    df_cust_c = df_cust_c.join(df_active, on=["nif_cliente"], how="inner")
    print("after join active df_cust_c={}".format(df_cust_c.count()))


    print("COLUMNS-1 df_cust_c {}".format(",".join([col_ for col_ in df_cust_c.columns if col_ == "nif_cliente"])))


    print("df_customer {}".format(",".join(df_customers.columns)))
    print("df_cust_c {}".format(",".join(df_cust_c.columns)))

    df_cust = df_customers.join(df_cust_c.drop("nif_cliente"), on=["num_cliente"], how="inner")
    print("COLUMNS-2 df_cust {}".format(",".join([col_ for col_ in df_cust.columns if col_ == "nif_cliente"])))

    df_cust.select(col("segment_nif")).groupby("segment_nif").agg(sql_count("*")).show()

    df_cust_d28 = __get_customer_master_aux(spark, closing_day, days=-28)
    df_cust = df_cust.join(df_cust_d28, on=["num_cliente"], how="left")

    df_cust_d56 = __get_customer_master_aux(spark, closing_day, days=-56)
    df_cust = df_cust.join(df_cust_d56, on=["num_cliente"], how="left")

    df_cust_d84 = __get_customer_master_aux(spark, closing_day, days=-84)
    df_cust = df_cust.join(df_cust_d84, on=["num_cliente"], how="left")
    print("COLUMNS-3 df_cust {}".format(",".join([col_ for col_ in df_cust.columns if col_ == "nif_cliente"])))

    df_cust = df_cust.withColumn("diff_rgus_d_d14", col("nb_rgus_d") - col("nb_rgus_d14"))
    df_cust = df_cust.withColumn("diff_rgus_d_d28", col("nb_rgus_d") - col("nb_rgus_d28"))
    df_cust = df_cust.withColumn("diff_rgus_d_d42", col("nb_rgus_d") - col("nb_rgus_d42"))
    df_cust = df_cust.withColumn("diff_rgus_d_d56", col("nb_rgus_d") - col("nb_rgus_d56"))
    df_cust = df_cust.withColumn("diff_rgus_d_d70", col("nb_rgus_d") - col("nb_rgus_d70"))
    df_cust = df_cust.withColumn("diff_rgus_d_d84", col("nb_rgus_d") - col("nb_rgus_d84"))
    df_cust = df_cust.withColumn("diff_rgus_d_d98", col("nb_rgus_d") - col("nb_rgus_d98"))

    df_cust = df_cust.withColumn("flag_decremento_rgus", when(((col("diff_rgus_d_d14") < 0) | (col("diff_rgus_d_d28") < 0) | (col("diff_rgus_d_d42") < 0) | (col("diff_rgus_d_d56") < 0) | (col("diff_rgus_d_d70") < 0) | (col("diff_rgus_d_d84") < 0) | (col("diff_rgus_d_d98") < 0)), 1).otherwise(0))
    df_cust = df_cust.withColumn("flag_incremento_rgus", when(((col("diff_rgus_d_d14") > 0) | (col("diff_rgus_d_d28") > 0) | (col("diff_rgus_d_d42") > 0) | (col("diff_rgus_d_d56") > 0) | (col("diff_rgus_d_d70") > 0) | (col("diff_rgus_d_d84") > 0) | (col("diff_rgus_d_d98") > 0)), 1).otherwise(0))
    df_cust = df_cust.withColumn("flag_no_inc_rgus",     when(((col("diff_rgus_d_d14") <= 0) | (col("diff_rgus_d_d28") <= 0) | (col("diff_rgus_d_d42") <= 0) | (col("diff_rgus_d_d56") <= 0) | (col("diff_rgus_d_d70") <= 0) | (col("diff_rgus_d_d84") <= 0) | (col("diff_rgus_d_d98") <= 0)), 1).otherwise(0))
    df_cust = df_cust.withColumn("flag_keep_rgus",       when(((col("diff_rgus_d_d14") == 0) | (col("diff_rgus_d_d28") == 0) | (col("diff_rgus_d_d42") == 0) | (col("diff_rgus_d_d56") == 0) | (col("diff_rgus_d_d70") == 0) | (col("diff_rgus_d_d84") == 0) | (col("diff_rgus_d_d98") == 0)), 1).otherwise(0))

    df_billing = __get_billing_df(spark, closing_day)
    df_billing_p = __prepare_billing(df_billing)
    #print("before", df_cust.count(), df_billing_p.count())
    df_tar_all = df_cust.join(df_billing_p, on=["num_cliente"], how="left")

    from pykhaos.utils.date_functions import move_date_n_cycles

    closing_day_c14 = move_date_n_cycles(closing_day, n=-14)
    closing_day_c6 = move_date_n_cycles(closing_day, n=-6)

    df_tarifas_c14 = __get_tariffs_dfs(spark, closing_day_c14).drop("nb_superofertas").withColumnRenamed("tarifas_list", "tarifas_list_c14").drop("nif_cliente")
    df_tarifas_c6 = __get_tariffs_dfs(spark, closing_day_c6).drop("nb_superofertas").withColumnRenamed("tarifas_list", "tarifas_list_c6").drop("nif_cliente")

    df_tar_all = df_tar_all.join(df_tarifas_c14, on=["num_cliente"], how="left")
    df_tar_all = df_tar_all.join(df_tarifas_c6, on=["num_cliente"], how="left")

    import collections
    compare_lists = udf(lambda x, y: 1 if collections.Counter(x) == collections.Counter(y) else 0)
    df_tar_all = df_tar_all.withColumn("num_tarifas_c6", size(col("tarifas_list_c6"))) # -1 when tariffs list is []
    df_tar_all = df_tar_all.withColumn("num_tarifas_c14", size(col("tarifas_list_c14")))
    df_tar_all = df_tar_all.withColumn("equal_c6_c14", compare_lists(col("tarifas_list_c14"), col("tarifas_list_c6"))) # no matter the order [T2C1G, TSP5M, TS215, TS215] == [TS215, TS215, T2C1G, TSP5M] --> 1

    print("COLUMNS {}".format(",".join(df_tar_all.columns)))
    df_tar_all = df_tar_all.cache()
    print("after", df_tar_all.count())
    print("Starting to save  {}".format(closing_day))
    import time
    starting_save = time.time()
    save_trigger_billing_car(df_tar_all, closing_day)
    print("Elapsed time {} minutes".format((time.time() - starting_save)/60.0))

    df_tar = df_tar_all


    volume = df_tar.count()

    return df_tar




#spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 churn/analysis/triggers/billing/run_segment_billing.py -c 20190414 2>&1 | tee /var/SP/data/home/csanc109/logging/triggers_billing_`date '+%Y%m%d_%H%M%S'`.log
if __name__ == "__main__":
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

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    from pykhaos.modeling.model_performance import get_lift

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD to compute lifts by type')
    parser.add_argument('-p', '--predict_closing_day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Predict closing day to generate trigger')
    parser.add_argument('-s', '--save', action='store_true', help='save to model outputs')
    parser.add_argument('-f', '--force', action='store_true', help='force generation')


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    predict_closing_day = args.predict_closing_day.split(" ")[0] if args.predict_closing_day else None
    save_trigger = args.save


    import sys
    if "," in closing_day:
        print("Incorrect format for closing_day {}".format(closing_day))
        sys.exit()


    if predict_closing_day and "," in predict_closing_day:
        print("Incorrect format for predict_closing_day {}".format(predict_closing_day))
        sys.exit()

    if save_trigger and not predict_closing_day:
        print("Unable to save trigger without a predict_closing_day")



    print(closing_day)
    print(predict_closing_day)
    print(save_trigger)



    # +--------------+
    # | segment_nif |
    # +--------------+
    # | unknown |
    # | Pure_prepaid |
    # | Standalone_FBB |
    # | Other |
    # | Convergent |
    # | Mobile_only |
    # +--------------+


    from churn.utils.general_functions import init_spark
    spark = init_spark("run_segment_orders")


    start_time = time.time()

    df_tar_all = get_billing_module(spark, closing_day)

    print("Elapsed time {} minutes".format((time.time() - start_time)/60.0))

