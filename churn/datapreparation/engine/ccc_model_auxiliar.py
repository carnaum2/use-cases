
from pyspark.sql.functions import datediff, to_date, from_unixtime, unix_timestamp, desc, when, col, lit, udf, size, \
    array, isnan, upper, coalesce, length, lower, concat, create_map, sum as sql_sum, greatest, max as sql_max, sort_array
from pykhaos.utils.pyspark_utils import sum_horizontal
from pyspark.sql.types import ArrayType, FloatType, StringType, BooleanType, IntegerType
from itertools import chain
from pyspark.sql.window import Window


def prepare_car(df, closing_day):
    print("Call to prepare_car")
    print("Call to prepare_customer")
    df = prepare_customer(df, closing_day)
    print("Call to prepare_orders")
    df = prepare_orders(df, closing_day)
    print("Call to prepare_orders_agg")
    df = prepare_orders_agg(df, closing_day)
    print("Call to prepare_devices")
    df = prepare_devices(df, closing_day)
    print("Call to prepare_penalties")
    df = prepare_penalties(df, closing_day)
    print("Ended prepare_car")
    return df


def prepare_orders_agg(df, closing_day):

    to_drop = [col_ for col_ in df.columns if col_.endswith("fx_first_order") or  col_.endswith("fx_last_order")]

    for col_ in to_drop:
        if col_ in df.columns: df = df.drop(col_)
    return df

def prepare_penalties(df, closing_day):

    # NOTE THESE VARIABLES ARE BY NUM CLIENT
    #num_client_window = Window.partitionBy("num_cliente")

    remove_zeroes_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] != 0]), ArrayType(FloatType()))
    to_drop = []


    for t in ["CUST", "SRV"]:

        for state in ["APPLIED", "FINISHED", "PENDING"]:

            df = (df.withColumn("aux_{}".format(state), array([_col for _col in df.columns if _col.endswith("_days_to") and _col.startswith("Penal_{}_{}".format(t, state))]))
                    .withColumn("penal_{}_nb_{}_penalties".format(t, state), size(remove_zeroes_udf("aux_{}".format(state))))
                    .withColumn("penal_{}_{}_max_days_to".format(t, state), when(coalesce(size(col("aux_{}".format(state))), lit(0)) == 0, -1).otherwise(greatest(*["Penal_{}_pending_N{}_days_to".format(t, n) for n in range(1, 6)])))
                    .withColumn("aux_{}_amount".format(state), array([_col for _col in df.columns if _col.endswith("penal_amount") and _col.lower().startswith("penal_{}_{}".format(t, state).lower())]))
                    .withColumn("penal_{}_{}_max_amount_penalties".format(t, state), when(coalesce(size(col("aux_{}".format(state))), lit(0)) == 0, -1).otherwise(greatest(*["Penal_{}_{}_N{}_penal_amount".format(t, state, n) for n in range(1, 6)])))
                  ).drop(*["aux_{}".format(state), "aux_{}_amount".format(state)])

            # Num Client variables are not requiered
            # df = (df.withColumn("penal_{}_pending_n{}_penal_amount".format(t, n), coalesce(col("penal_{}_pending_n{}_penal_amount".format(t, n)), lit(0.0)))
            #         .withColumn("total_penal_{}_pending_n{}_penal_amount".format(t, n),
            #                          sql_sum(col("penal_{}_pending_n{}_penal_amount".format(t, n))).over(num_client_window)))
            # df = (df.withColumn("billing_{}_max_days_to_nc".format(state).format(t),
            #                     sql_max(col("billing_{}_max_days_to".format(state))).over(num_client_window))
            # )

            to_drop += ["Penal_{}_{}_N{}_cod_penal".format(t, state, n) for n in range(1,6)] + \
                      ["Penal_{}_{}_N{}_cod_promo".format(t, state, n) for n in range(1,6)] + \
                      ["Penal_{}_{}_N{}_desc_penal".format(t, state, n) for n in range(1, 6)] + \
                      ["Penal_{}_{}_N{}_desc_promo".format(t, state, n) for n in range(1, 6)] + \
                      ["Penal_{}_{}_N{}_end_date".format(t, state, n) for n in range(1, 6)] + \
                      ["Penal_{}_{}_N{}_start_date".format(t, state, n) for n in range(1, 6)]

    for col_ in to_drop:
        if col_ in df.columns: df = df.drop(col_)
    return df




def prepare_customer(df, closing_day):

    to_drop = ["NOMBRE", "PRIM_APELLIDO", "SEG_APELLIDO", "CLASE_CLI_COD_CLASE_CLIENTE", "DIR_LINEA1", "DIR_LINEA2",
               "DIR_LINEA3", "NOM_COMPLETO", "DIR_FACTURA1", "DIR_FACTURA2", "DIR_FACTURA3", "DIR_FACTURA4",
               "TRAT_FACT", "NOMBRE_CLI_FACT", "APELLIDO1_CLI_FACT", "APELLIDO2_CLI_FACT", "CTA_CORREO", "CODIGO_POSTAL",
               "DIR_NUM_DIRECCION", "age", "FECHA_NACI", "FECHA_MIGRACION", "birth_date", 'x_user_facebook',
               'x_user_twitter', 'nacionalidad', "NIF_FACTURACION", "CTA_CORREO_CONTACTO"]

    df = df.withColumn("TRATAMIENTO", when(upper(col("TRATAMIENTO")).isin(["SRTA", "OTHERS", "UNKNOWN", "SRA", "SR"]), upper(col("TRATAMIENTO"))).otherwise("OTHERS"))
    df = df.withColumn("METODO_PAGO", when(upper(col("METODO_PAGO")).isin(["SUBCUENTA", "VENTANILLA", "OTHERS",  "DOMICILIADO", "PREPAGO"]), upper(col("METODO_PAGO"))).otherwise("OTHERS"))
    df = df.withColumn("days_since_fecha_migracion", when(col("FECHA_MIGRACION").isNotNull(), datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col("FECHA_MIGRACION"))).otherwise(-1))
    df = df.withColumn("X_IDIOMA_FACTURA", when(upper(col("X_IDIOMA_FACTURA")).isin(["CATALAN", "INGLES", "CASTELLANO", "GALLEGO", "EUSKERA"]), upper(col("X_IDIOMA_FACTURA"))).otherwise("OTHERS"))
    df = df.withColumn("x_formato_factura", when(upper(col("x_formato_factura")).isin(["UNKNOWN", "PAPEL - ESTANDAR", "ELECTRONICA - ESTANDAR", "PAPEL - REDUCIDA", "PAPEL - SEMIREDUCIDA"]), upper(col("x_formato_factura"))).otherwise("OTHERS"))
    df = df.withColumn("flg_robinson", when(coalesce(length(col("flg_robinson")), lit(0)) == 0, "OTHERS").otherwise(col("flg_robinson")))
    df = df.withColumn("FLG_LORTAD", when(coalesce(length(col("FLG_LORTAD")), lit(0)) == 0, "OTHERS").otherwise(col("FLG_LORTAD")))
    df = df.withColumn("gender2hgbst_elm", when(upper(col("gender2hgbst_elm")).isin(["MALE", "FEMALE", "OTHERS"]), upper(col("gender2hgbst_elm"))).otherwise("OTHERS"))
    df = df.withColumn("marriage2hgbst_elm", when(upper(col("marriage2hgbst_elm")).isin(["MARRIED", "SINGLE"]), upper(col("marriage2hgbst_elm"))).otherwise("OTHERS"))
    df = df.withColumn("TIPO_DOCUMENTO", when(upper(col("TIPO_DOCUMENTO")).isin(["N.I.F.", "PASAPORTE", "N.I.E."]), upper(col("TIPO_DOCUMENTO"))).otherwise("OTHERS"))
    df = df.withColumn("SUPEROFERTA", when(upper(col("SUPEROFERTA")).isin(["ON15"]), upper(col("SUPEROFERTA"))).otherwise("OTHERS"))
    df = df.withColumn("customer_twitter_user", when(coalesce(length(col("x_user_twitter")), lit(0)) == 0, 0).otherwise(1))
    df = df.withColumn("customer_facebook_user", when(coalesce(length(col("x_user_facebook")), lit(0)) == 0, 0).otherwise(1))
    df = (df.withColumn("customer_nacionalidad_spain", when(coalesce(col("nacionalidad"), lit(0)) ==0, 0)
                                      .when(lower(col("nacionalidad")).like("%espa%"), 1).otherwise(0)))
    df = df.withColumn("FACTURA_ELECTRONICA", when(col("FACTURA_ELECTRONICA").isNull(), 0).otherwise(col("FACTURA_ELECTRONICA")))
    df = df.withColumn("FACTURA_CATALAN", when(coalesce(length(col("FACTURA_CATALAN")), lit(0))==0, 0).otherwise(col("FACTURA_CATALAN")))
    df = df.withColumn("ENCUESTAS", when( col("ENCUESTAS")=="S", 1).otherwise(0))
    df = df.withColumn("PUBLICIDAD", when( col("PUBLICIDAD")=="S", 1).otherwise(0))

    # -- Compute days until next bill
    import datetime as dt
    current_month = dt.datetime.strptime(closing_day[:6], "%Y%m") # YYYYMM
    next_yearmonth = (dt.datetime(current_month.year, current_month.month, 28) + dt.timedelta(days=4)).strftime("%Y%m") # next YYYYMM
    replace_dict = {1: "30", 8: "07", 15: "15", 22: "21"} # mapping cycle --> billing day
    mapping_expr = create_map([lit(x) for x in chain(*replace_dict.items())])

    df = (df.withColumn("date_next_bill", when( (col("ciclo")==0) | (col("ciclo").isNull()), "unknown").when(lit(closing_day[6:]).cast(IntegerType()) > mapping_expr[df['ciclo']],
                from_unixtime(unix_timestamp(concat(lit(next_yearmonth),  mapping_expr[df['ciclo']]),"yyyyMMdd"))).otherwise(
                from_unixtime(unix_timestamp(concat(lit(closing_day[:6]), mapping_expr[df['ciclo']]),"yyyyMMdd"))))
            .withColumn("days_until_next_bill", when(col("date_next_bill")=="unknown", -1).otherwise(datediff("date_next_bill", from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd"))))))

    to_drop += ["date_next_bill", "ciclo"]
    #
    # to_select = ['NUM_CLIENTE', 'TRATAMIENTO', 'COD_ESTADO_GENERAL',  'NIF_CLIENTE', 'METODO_PAGO', 'PUBLICIDAD',
    #              'ENCUESTAS', 'FACTURA_CATALAN', 'FACTURA_ELECTRONICA', 'SUPEROFERTA',  'TIPO_DOCUMENTO', 'X_PUBLICIDAD_EMAIL',
    #              'x_tipo_cuenta_corp', 'x_antiguedad_cuenta', 'x_datos_navegacion', 'x_datos_trafico', 'x_cesion_datos',
    #              'marriage2hgbst_elm', 'gender2hgbst_elm', 'FLG_LORTAD', 'flg_robinson', 'x_formato_factura', 'X_IDIOMA_FACTURA',
    #              'ENCUESTAS2', 'cta_correo_flag', 'cta_correo_server', 'days_since_fecha_migracion', 'customer_twitter_user',
    #              'customer_facebook_user', 'customer_nacionalidad_spain', 'days_until_next_bill']

    for col_ in to_drop:
        if col_ in df.columns: df = df.drop(col_)
    return df




def prepare_billing(df, closing_day):

    NUM_BILLS = 5

    for m in range(1, NUM_BILLS + 1):
        df = (
              #df.withColumn("Bill_N{}_net".format(m), when(~isnan("Bill_N{}_Amount_To_Pay".format(m)) & ~isnan("Bill_N{}_Tax_Amount".format(m)),
              #                                            col("Bill_N{}_Amount_To_Pay".format(m)) - col("Bill_N{}_Tax_Amount".format(m))).otherwise(-1))
               df.withColumn("billing_days_since_Bill_N{}".format(m), when(col('Bill_N{}_Bill_Date'.format(m)).isNotNull(),
                                                                datediff(from_unixtime(
                                                                            unix_timestamp(lit(closing_day),
                                                                                           "yyyyMMdd")),
                                                                                 col('Bill_N{}_Bill_Date'.format(m))).cast("double")).otherwise(-1)))


    remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] != -1]), ArrayType(FloatType()))
    import numpy as np
    avg_days_bw_bills_udf = udf(lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())


    stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
    #mean_udf = udf(lambda milista: float(np.mean(milista)), FloatType())


    mean_range_udf = udf(lambda lista,idx: float(np.mean(lista[:idx])), FloatType())
    negative_udf = udf(lambda lista: 1 if any([ii<0 for ii in lista]) else 0, IntegerType())

    # debt is negative when Vodafone has the debt
    df = (df.withColumn("aux_completed", array([_col for _col in df.columns if _col.startswith("billing_days_since_Bill_N")]))
            .withColumn("aux_completed", remove_undefined_udf(col("aux_completed")))
            .withColumn("billing_nb_last_bills", when(col("aux_completed").isNotNull(), size(col("aux_completed"))).otherwise(0))
            .withColumn("billing_avg_days_bw_bills", when(col("billing_nb_last_bills") > 1, avg_days_bw_bills_udf(col("aux_completed"))).otherwise(-1.0))

           .withColumn("aux_invoice", array([_col for _col in df.columns if _col.endswith("_InvoiceCharges") and _col.startswith("Bill_N")]))
           #.withColumn("aux_invoice", remove_undefined_udf(col("aux_invoice")))
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

          #.withColumn("num_last_bills"),
         ).drop(*["aux_invoice", "aux_completed", "aux_debt"])


    to_drop = [col_.format(m) for m in range(1, NUM_BILLS + 1) for col_ in  [#'Bill_N{}_InvoiceCharges',
                                                                              #'Bill_N{}_Amount_To_Pay',
                                                                              #'Bill_N{}_Tax_Amount',
                                                                              #'Bill_N{}_Debt_Amount',
                                                                              'Bill_N{}_Bill_Date']
               ]

    # to_select = ['customeraccount', 'Bill_N1_InvoiceCharges', 'Bill_N1_Amount_To_Pay', 'Bill_N1_Tax_Amount',
    #              'Bill_N1_Debt_Amount', 'Bill_N1_Bill_Date', 'Bill_N2_InvoiceCharges', 'Bill_N2_Amount_To_Pay',  'Bill_N2_Tax_Amount',
    #              'Bill_N2_Debt_Amount', 'Bill_N2_Bill_Date', 'Bill_N3_InvoiceCharges', 'Bill_N3_Amount_To_Pay', 'Bill_N3_Tax_Amount',
    #              'Bill_N3_Debt_Amount', 'Bill_N3_Bill_Date', 'Bill_N4_InvoiceCharges', 'Bill_N4_Amount_To_Pay', 'Bill_N4_Tax_Amount',
    #              'Bill_N4_Debt_Amount', 'Bill_N4_Bill_Date', 'Bill_N5_InvoiceCharges', 'Bill_N5_Amount_To_Pay', 'Bill_N5_Tax_Amount',
    #              'Bill_N5_Debt_Amount', 'Bill_N5_Bill_Date', 'billing_days_since_Bill_N1', 'billing_days_since_Bill_N2',
    #              'billing_days_since_Bill_N3', 'billing_days_since_Bill_N4', 'billing_days_since_Bill_N5', 'billing_nb_last_bills',
    #              'billing_avg_days_bw_bills', 'billing_std', 'billing_mean',  'billing_current_debt', 'billing_any_vf_debt',
    #              'billing_current_vf_debt', 'billing_current_client_debt',  'billing_settle_debt']

    for col_ in to_drop:
        if col_ in df.columns: df = df.drop(col_)
    return df



def prepare_orders(df, closing_day):


    values= ['Order_N{}'.format(n) for n in range(1,11)] # 1 to 10


    for vv in values:
        df = (df.withColumn("days_since_{}".format(vv + "_StartDate"), when(col(vv + "_StartDate").isNotNull(),
                                                                            datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col(vv + "_StartDate"))).otherwise(-1))
                 # replace 1773 dates of PEN orders, by null
                .withColumn(vv + "_CompletedDate", when(datediff(lit("1900-01-01 00:00:00"), col(vv + "_CompletedDate")).cast("double") > 0, None).otherwise( col(vv + "_CompletedDate")))
                .withColumn("days_since_{}".format(vv + "_CompletedDate"), when(col(vv + "_CompletedDate").isNotNull(),
                                                                           datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col(vv + "_CompletedDate"))).otherwise(-1)))

    import numpy as np
    remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] != -1]), ArrayType(FloatType()))
    keep_pen_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] == "PEN"]), ArrayType(StringType()))


    avg_days_bw_orders_udf = udf(
        lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())

    df = (df.withColumn("aux_array", array([_col for _col in df.columns if _col.endswith("StartDate") and _col.startswith("days_since_Order")]))
            .withColumn("aux_array", remove_undefined_udf(col("aux_array")))
            .withColumn("nb_started_orders", when(col("aux_array").isNotNull(), size(col("aux_array"))).otherwise(0))
            .withColumn("avg_days_bw_orders", when(col("nb_started_orders") > 1, avg_days_bw_orders_udf(col("aux_array"))).otherwise(-1.0))
            .withColumn("aux_array", array([_col for _col in df.columns if _col.endswith("_Status")]))
            .withColumn("aux_array", keep_pen_udf(col("aux_array")))
            .withColumn("nb_pending_orders", when(col("aux_array").isNotNull(), size(col("aux_array"))).otherwise(0))
            .drop("aux_array")
          )

    fill_none =  ['Order_N{}_Class'.format(n) for n in range(1,11)] + ['Order_N{}_Status'.format(n) for n in range(1,11)]
    fill_m1   =  ['days_since_Order_N{}_StartDate'.format(n) for n in range(1,11)] +  ['days_since_Order_N{}_CompletedDate'.format(n) for n in range(1,11)] + ['avg_days_bw_orders']
    fill_zero =  ['nb_started_orders',  'nb_pending_orders']

    df = df.fillna(-1.0, subset=fill_m1)
    df = df.fillna(0, subset=fill_zero)
    df = df.fillna("None", subset=fill_none)


    # to_select = ['NUM_CLIENTE', 'Order_N1_Class', 'Order_N1_Status', 'Order_N2_Class', 'Order_N2_Status', 'Order_N3_Class',
    #              'Order_N3_Status', 'Order_N4_Class', 'Order_N4_Status', 'Order_N5_Class', 'Order_N5_Status', 'Order_N6_Class',
    #              'Order_N6_Status', 'Order_N7_Class', 'Order_N7_Status', 'Order_N8_Class', 'Order_N8_Status', 'Order_N9_Class',
    #              'Order_N9_Status',  'Order_N10_Class',  'Order_N10_Status',  'days_since_Order_N1_StartDate',
    #              'days_since_Order_N1_CompletedDate', 'days_since_Order_N2_StartDate',  'days_since_Order_N2_CompletedDate',
    #              'days_since_Order_N3_StartDate',  'days_since_Order_N3_CompletedDate', 'days_since_Order_N4_StartDate',
    #              'days_since_Order_N4_CompletedDate',  'days_since_Order_N5_StartDate',  'days_since_Order_N5_CompletedDate',
    #              'days_since_Order_N6_StartDate',  'days_since_Order_N6_CompletedDate',  'days_since_Order_N7_StartDate',
    #              'days_since_Order_N7_CompletedDate', 'days_since_Order_N8_StartDate',  'days_since_Order_N8_CompletedDate',
    #              'days_since_Order_N9_StartDate',  'days_since_Order_N9_CompletedDate', 'days_since_Order_N10_StartDate',
    #              'days_since_Order_N10_CompletedDate',  'nb_started_orders',  'avg_days_bw_orders', 'nb_pending_orders']

    # to_drop = [col_ for col_ in df.columns if col_.lower().startswith("order") and (col_.lower().endswith("id") or
    #                                                                                    col_.lower().endswith("date") or
    #                                                                                    col_.lower().endswith("description"))]
    # for col_ in to_drop:
    #     if col_ in df.columns: df = df.drop(col_)
    return df



def prepare_devices(df, closing_day, impute_nulls=True):

    df = (df.withColumn("t1", when(col('Device_N1_imei').isNotNull(),1).otherwise(0))
                   .withColumn("t2", when(col('Device_N2_imei').isNotNull(),1).otherwise(0))
                   .withColumn("t3", when(col('Device_N3_imei').isNotNull(),1).otherwise(0))
                   .withColumn("t4", when(col('Device_N4_imei').isNotNull(),1).otherwise(0))
                   .withColumn("num_devices", sum_horizontal(["t1", "t2", "t3", "t4"]))
                   .drop(*["t1", "t2", "t3", "t4"]))

    for t in [1,2,3,4]:
        df = df.withColumn("days_since_Device_N{}_change_date".format(t), when(col("Device_N{}_change_date".format(t)).isNotNull(),
                                                                               datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col("Device_N{}_change_date".format(t)))).otherwise(-1))

    to_drop = ['Device_N1_imei', 'Device_N1_change_date', 'Device_N1_change_time', 'Device_N1_manufacturer',
                 'Device_N1_model', 'Device_N1_device_type', 'Device_N1_OS', 'Device_N1_OS_initial_version',
                 'Device_N2_imei', 'Device_N2_change_date', 'Device_N2_change_time', 'Device_N2_manufacturer',
                 'Device_N2_model', 'Device_N2_device_type', 'Device_N2_OS', 'Device_N2_OS_initial_version',
                 'Device_N3_imei', 'Device_N3_change_date', 'Device_N3_change_time', 'Device_N3_manufacturer',
                 'Device_N3_model', 'Device_N3_device_type', 'Device_N3_OS', 'Device_N3_OS_initial_version',
                 'Device_N4_imei', 'Device_N4_change_date', 'Device_N4_change_time', 'Device_N4_manufacturer',
                 'Device_N4_model', 'Device_N4_device_type', 'Device_N4_OS', 'Device_N4_OS_initial_version']


    #df = df.fillna(-1.0, subset=["Device_tenure_days_from_N1", "Device_tenure_days_N2", "Device_tenure_days_N3", "Device_tenure_days_N4"])
    #df = df.fillna(0, subset=["Devices_month_imei_changes", "Devices_month_device_changes"])

    # to_select = ['msisdn', 'Device_tenure_days_from_N1', 'Device_tenure_days_N2', 'Device_tenure_days_N3',
    #              'Device_tenure_days_N4', 'Devices_month_imei_changes', 'Devices_month_device_changes',
    #              'num_devices', 'days_since_Device_N1_change_date', 'days_since_Device_N2_change_date',
    #              'days_since_Device_N3_change_date', 'days_since_Device_N4_change_date']

    for col_ in df.columns:
        if col_.startswith("Devices_"):
            df = df.withColumnRenamed(col_, col_.replace("Devices_", "Device_"))


    for col_ in df.columns:
        if col_.lower().startswith("msisdn") or col_.lower().startswith("num_cliente") or col_.lower().startswith("nif_cliente") or col_.startswith("Device_"):
            continue
        df = df.withColumnRenamed(col_, "device_" + col_)


    for col_ in to_drop:
        if col_ in df.columns: df = df.drop(col_)

    for col_ in df.columns:
        df = df.withColumnRenamed(col_, col_.lower())

    if impute_nulls:
        df = impute_null_devices(df)

    return df


def impute_null_devices(df):

    fill_minus_one = ["device_tenure_days_from_n1", "device_tenure_days_n2",
                       "device_tenure_days_n3", "device_tenure_days_n4",
                       "device_days_since_device_n1_change_date", "device_days_since_device_n2_change_date",
                       "device_days_since_device_n3_change_date", "device_days_since_device_n4_change_date",
                       ]

    fill_zero = ['device_month_device_changes', 'device_month_imei_changes', "device_month_imei_changes",
                 "device_month_device_changes", "device_num_devices"]

    df = df.fillna(-1.0, subset=fill_minus_one)
    df = df.fillna(0, subset=fill_zero)

    return df