#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import lit, col, when

def get_navcomp_metadata(spark, sample=None):

    import itertools

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown"]

    count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["sum_count", "max_count"]))]

    days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]

    null_imp_dict = dict([(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if ("min_days" in x)])

    feats = null_imp_dict.keys()

    na_vals = null_imp_dict.values()

    na_vals = [str(x) for x in na_vals]

    cat_feats = []

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark\
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('navcomp'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('level', lit('nif'))

    return metadata_df




def get_customer_metadata(spark, sample=None):

    feats = ['tgs_days_until_fecha_fin_dto', 'nb_rgus', 'nb_tv_services_nif', 'segment_nif']

    cat_feats = ['segment_nif']

    na_vals = [-1, 0, 0, 'unknown']

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data))\
        .withColumn('source', lit('customer'))\
        .withColumn('type', lit('numeric'))\
        .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type')))\
        .withColumn('level', lit('nif'))

    return metadata_df

def get_ccc_metadata(spark, sample=None):

    from churn.analysis.triggers.ccc_utils.ccc_utils import get_bucket_info

    bucket = get_bucket_info(spark)

    bucket_list = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect() + ['num_calls']

    suffixes = ['w2', 'w4', 'w8']

    import itertools

    agg_feats = [p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, suffixes))]

    incs = ['w2vsw2', 'w4vsw4']

    inc_feats = ["inc_" + p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, incs))]

    feats = agg_feats + inc_feats

    na_vals = len(feats)*[0]

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('ccc')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    return metadata_df


def get_tickets_additional_metadata(spark, sample=None):

    dict_ = get_tickets_additional_metadata_dict(spark)

    feats = dict_.keys()

    na_vals = dict_.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('tickets_additional')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))
    return metadata_df




def get_tickets_additional_metadata_dict(spark, sample=None):

    tick_add_map = {'num_tickets_tipo_tramitacion_opened': 0.0, \
                    'num_tickets_tipo_reclamacion_opened': 0.0, \
                    'num_tickets_tipo_averia_opened': 0.0, \
                    'num_tickets_tipo_incidencia_opened': 0.0, \
                    'num_tickets_opened': 0.0, \
                    'max_time_opened_tipo_tramitacion': -1.0, \
                    'max_time_opened_tipo_reclamacion': -1.0, \
                    'max_time_opened_tipo_averia': -1.0, \
                    'max_time_opened_tipo_incidencia': -1.0, \
                    'min_time_opened_tipo_tramitacion': -1.0, \
                    'min_time_opened_tipo_reclamacion': -1.0, \
                    'min_time_opened_tipo_averia': -1.0, \
                    'min_time_opened_tipo_incidencia': -1.0, \
                    'std_time_opened_tipo_tramitacion': -1.0, \
                    'std_time_opened_tipo_reclamacion': -1.0, \
                    'std_time_opened_tipo_averia': -1.0, \
                    'std_time_opened_tipo_incidencia': -1.0, \
                    'mean_time_opened_tipo_tramitacion': -1.0, \
                    'mean_time_opened_tipo_reclamacion': -1.0, \
                    'mean_time_opened_tipo_averia': -1.0, \
                    'mean_time_opened_tipo_incidencia': -1.0, \
                    'mean_time_opened': -1.0, \
                    'max_time_opened': -1.0, \
                    'min_time_opened': -1.0, \
                    'std_time_opened': -1.0, \
                    'num_tickets_tipo_tramitacion_closed': 0.0, \
                    'num_tickets_tipo_reclamacion_closed': 0.0, \
                    'num_tickets_tipo_averia_closed': 0.0, \
                    'num_tickets_tipo_incidencia_closed': 0.0, \
                    'num_tickets_closed': 0.0, \
                    'max_time_closed_tipo_tramitacion': -1.0, \
                    'max_time_closed_tipo_reclamacion': -1.0, \
                    'max_time_closed_tipo_averia': -1.0, \
                    'max_time_closed_tipo_incidencia': -1.0, \
                    'min_time_closed_tipo_tramitacion': -1.0, \
                    'min_time_closed_tipo_reclamacion': -1.0, \
                    'min_time_closed_tipo_averia': -1.0, \
                    'min_time_closed_tipo_incidencia': -1.0, \
                    'std_time_closed_tipo_tramitacion': -1.0, \
                    'std_time_closed_tipo_reclamacion': -1.0, \
                    'std_time_closed_tipo_averia': -1.0, \
                    'std_time_closed_tipo_incidencia': -1.0, \
                    'mean_time_closed_tipo_tramitacion': -1.0, \
                    'mean_time_closed_tipo_reclamacion': -1.0, \
                    'mean_time_closed_tipo_averia': -1.0, \
                    'mean_time_closed_tipo_incidencia': -1.0, \
                    'weeks_averias': 0.0, \
                    'weeks_facturacion': 0.0, \
                    'weeks_reclamacion': 0.0, \
                    'weeks_incidencias': 0.0, \
                    'count_X220_no_signal': 0.0, \
                    'count_M5_problema_RF': 0.0, \
                    'count_M3_Fallo_CM': 0.0, \
                    'count_T6_no_hay_tono': 0.0, \
                    'count_Incomunicacion': 0.0, \
                    'count_P05_deco_defectuoso': 0.0, \
                    'count_Portabilidad': 0.0, \
                    'count_T7_conexiones': 0.0}

    return tick_add_map


def get_reimbursements_metadata(spark, sample=None):

    reimb_map = {'Reimbursement_adjustment_net': 0.0,\
                 'Reimbursement_adjustment_debt': 0.0,\
                 'Reimbursement_num': 0.0,\
                 'Reimbursement_num_n8': 0.0,\
                 'Reimbursement_num_n6': 0.0,\
                 'Reimbursement_days_since': 10000.0,\
                 'Reimbursement_num_n4': 0.0,\
                 'Reimbursement_num_n5': 0.0,\
                 'Reimbursement_num_n2': 0.0,\
                 'Reimbursement_num_n3': 0.0,\
                 'Reimbursement_days_2_solve': -1.0,\
                 'Reimbursement_num_month_2': 0.0,\
                 'Reimbursement_num_n7': 0.0,\
                 'Reimbursement_num_n1': 0.0,\
                 'Reimbursement_num_month_1': 0.0}

    feats = reimb_map.keys()

    na_vals = reimb_map.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('reimbursements')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    return metadata_df


def get_tickets_metadata(spark, sample=None):
    types = ['tramitacion', 'reclamacion', 'averia', 'incidencia']
    weeks = ['_w','_w2', '_w4', '_w8', '_w4w2', '_w8w4']
    weeks_inc = ['_w2w2', '_w4w4']

    tick_feats = ['num_tickets_tipo_' + typ + w for typ in types for w in weeks] + ['INC_num_tickets_tipo_' + typ + w
                                                                                    for typ in types for w in weeks_inc]
    null_imp_dict = dict([(x, 0.0) for x in tick_feats])

    feats = null_imp_dict.keys()

    na_vals = null_imp_dict.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('tickets')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    return metadata_df


def get_billing_metadata(spark, sample=None):
    from pyspark.sql.functions import avg as sql_avg
    billing_columns = reduce((lambda x, y: x + y), [
        ["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount",
         "Bill_N" + str(n) + "_Debt_Amount"] for n in range(1, 6)])

    repartdf = sample.select(billing_columns).repartition(400).cache()

    [avg_bill_n1_invoicecharges, \
     avg_bill_n1_bill_amount, \
     avg_bill_n1_tax_amount, \
     avg_bill_n1_debt_amount, \
     avg_bill_n2_invoicecharges, \
     avg_bill_n2_bill_amount, \
     avg_bill_n2_tax_amount, \
     avg_bill_n2_debt_amount, \
     avg_bill_n3_invoicecharges, \
     avg_bill_n3_bill_amount, \
     avg_bill_n3_tax_amount, \
     avg_bill_n3_debt_amount, \
     avg_bill_n4_invoicecharges, \
     avg_bill_n4_bill_amount, \
     avg_bill_n4_tax_amount, \
     avg_bill_n4_debt_amount, \
     avg_bill_n5_invoicecharges, \
     avg_bill_n5_bill_amount, \
     avg_bill_n5_tax_amount, \
     avg_bill_n5_debt_amount] = repartdf \
        .filter((col("Bill_N1_InvoiceCharges").isNotNull()) \
                & (col("Bill_N1_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N1_Tax_Amount").isNotNull()) \
                & (col("Bill_N1_Debt_Amount").isNotNull()) \
                & (col("Bill_N2_InvoiceCharges").isNotNull()) \
                & (col("Bill_N2_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N2_Tax_Amount").isNotNull()) \
                & (col("Bill_N2_Debt_Amount").isNotNull()) \
                & (col("Bill_N3_InvoiceCharges").isNotNull()) \
                & (col("Bill_N3_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N3_Tax_Amount").isNotNull()) \
                & (col("Bill_N3_Debt_Amount").isNotNull()) \
                & (col("Bill_N4_InvoiceCharges").isNotNull()) \
                & (col("Bill_N4_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N4_Tax_Amount").isNotNull()) \
                & (col("Bill_N4_Debt_Amount").isNotNull()) \
                & (col("Bill_N5_InvoiceCharges").isNotNull()) \
                & (col("Bill_N5_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N5_Tax_Amount").isNotNull()) \
                & (col("Bill_N5_Debt_Amount").isNotNull())) \
        .select(billing_columns) \
        .distinct() \
        .select(*[sql_avg("Bill_N1_InvoiceCharges").alias("avg_Bill_N1_InvoiceCharges"), \
                  sql_avg("Bill_N1_Amount_To_Pay").alias("avg_Bill_N1_Amount_To_Pay"), \
                  sql_avg("Bill_N1_Tax_Amount").alias("avg_Bill_N1_Tax_Amount"), \
                  sql_avg("Bill_N1_Debt_Amount").alias("avg_Bill_N1_Debt_Amount"), \
                  sql_avg("Bill_N2_InvoiceCharges").alias("avg_Bill_N2_InvoiceCharges"), \
                  sql_avg("Bill_N2_Amount_To_Pay").alias("avg_Bill_N2_Amount_To_Pay"), \
                  sql_avg("Bill_N2_Tax_Amount").alias("avg_Bill_N2_Tax_Amount"), \
                  sql_avg("Bill_N2_Debt_Amount").alias("avg_Bill_N2_Debt_Amount"), \
                  sql_avg("Bill_N3_InvoiceCharges").alias("avg_Bill_N3_InvoiceCharges"), \
                  sql_avg("Bill_N3_Amount_To_Pay").alias("avg_Bill_N3_Amount_To_Pay"), \
                  sql_avg("Bill_N3_Tax_Amount").alias("avg_Bill_N3_Tax_Amount"), \
                  sql_avg("Bill_N3_Debt_Amount").alias("avg_Bill_N3_Debt_Amount"), \
                  sql_avg("Bill_N4_InvoiceCharges").alias("avg_Bill_N4_InvoiceCharges"), \
                  sql_avg("Bill_N4_Amount_To_Pay").alias("avg_Bill_N4_Amount_To_Pay"), \
                  sql_avg("Bill_N4_Tax_Amount").alias("avg_Bill_N4_Tax_Amount"), \
                  sql_avg("Bill_N4_Debt_Amount").alias("avg_Bill_N4_Debt_Amount"), \
                  sql_avg("Bill_N5_InvoiceCharges").alias("avg_Bill_N5_InvoiceCharges"), \
                  sql_avg("Bill_N5_Amount_To_Pay").alias("avg_Bill_N5_Amount_To_Pay"), \
                  sql_avg("Bill_N5_Tax_Amount").alias("avg_Bill_N5_Tax_Amount"), \
                  sql_avg("Bill_N5_Debt_Amount").alias("avg_Bill_N5_Debt_Amount")]) \
        .rdd \
        .map(lambda r: [r["avg_Bill_N1_InvoiceCharges"], r["avg_Bill_N1_Amount_To_Pay"], r["avg_Bill_N1_Tax_Amount"],
                        r["avg_Bill_N1_Debt_Amount"], r["avg_Bill_N2_InvoiceCharges"], r["avg_Bill_N2_Amount_To_Pay"],
                        r["avg_Bill_N2_Tax_Amount"], r["avg_Bill_N2_Debt_Amount"], r["avg_Bill_N3_InvoiceCharges"],
                        r["avg_Bill_N3_Amount_To_Pay"], r["avg_Bill_N3_Tax_Amount"], r["avg_Bill_N3_Debt_Amount"],
                        r["avg_Bill_N4_InvoiceCharges"], r["avg_Bill_N4_Amount_To_Pay"], r["avg_Bill_N4_Tax_Amount"],
                        r["avg_Bill_N4_Debt_Amount"], r["avg_Bill_N5_InvoiceCharges"], r["avg_Bill_N5_Amount_To_Pay"],
                        r["avg_Bill_N5_Tax_Amount"], r["avg_Bill_N5_Debt_Amount"]]) \
        .first()

    val_net = -1.0
    val_inc = -1.0

    bill_na_fill = {"Bill_N1_InvoiceCharges": avg_bill_n1_invoicecharges, \
                    "Bill_N1_Amount_To_Pay": avg_bill_n1_bill_amount, \
                    "Bill_N1_Tax_Amount": avg_bill_n1_tax_amount, \
                    "Bill_N1_Debt_Amount": avg_bill_n1_debt_amount, \
                    "Bill_N2_InvoiceCharges": avg_bill_n2_invoicecharges, \
                    "Bill_N2_Amount_To_Pay": avg_bill_n2_bill_amount, \
                    "Bill_N2_Tax_Amount": avg_bill_n2_tax_amount, \
                    "Bill_N2_Debt_Amount": avg_bill_n2_debt_amount, \
                    "Bill_N3_InvoiceCharges": avg_bill_n3_invoicecharges, \
                    "Bill_N3_Amount_To_Pay": avg_bill_n3_bill_amount, \
                    "Bill_N3_Tax_Amount": avg_bill_n3_tax_amount, \
                    "Bill_N3_Debt_Amount": avg_bill_n3_debt_amount, \
                    "Bill_N4_InvoiceCharges": avg_bill_n4_invoicecharges, \
                    "Bill_N4_Amount_To_Pay": avg_bill_n4_bill_amount, \
                    "Bill_N4_Tax_Amount": avg_bill_n4_tax_amount, \
                    "Bill_N4_Debt_Amount": avg_bill_n4_debt_amount, \
                    "Bill_N5_InvoiceCharges": avg_bill_n5_invoicecharges, \
                    "Bill_N5_Amount_To_Pay": avg_bill_n5_bill_amount, \
                    "Bill_N5_Tax_Amount": avg_bill_n5_tax_amount, \
                    "Bill_N5_Debt_Amount": avg_bill_n5_debt_amount, \
                    "bill_n1_net": val_net, \
                    "bill_n2_net": val_net, \
                    "bill_n3_net": val_net, \
                    "bill_n4_net": val_net, \
                    "bill_n5_net": val_net, \
                    "inc_bill_n1_n2_net": val_inc, \
                    "inc_bill_n1_n3_net": val_inc, \
                    "inc_bill_n1_n4_net": val_inc, \
                    "inc_bill_n1_n5_net": val_inc, \
                    "inc_Bill_N1_N2_Amount_To_Pay": val_inc, \
                    "inc_Bill_N1_N3_Amount_To_Pay": val_inc, \
                    "inc_Bill_N1_N4_Amount_To_Pay": val_inc, \
                    "inc_Bill_N1_N5_Amount_To_Pay": val_inc}

    feats = [name for name in sample.columns if 'bill_' in name.lower()]

    na_vals = bill_na_fill.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd
    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('billing')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    return metadata_df


def get_orders_metadata(spark, sample=None):
    nb_orders_cols = [col_ for col_ in sample.columns if "orders" in col_]
    sla_orders_cols = list(set([col_ for col_ in sample.columns if not col_ in nb_orders_cols]) - set(['NIF_CLIENTE']))

    feats = nb_orders_cols + sla_orders_cols

    nb_null = 0
    ord_null = 0 #Check if 0 imputation value is OK for every feat after nrt changes

    tup_nb = [(val, nb_null) for val in nb_orders_cols]
    tup_sla = [(val, ord_null) for val in sla_orders_cols ]

    tup = tup_nb + tup_sla

    orders_na_fill = {key: value for (key, value) in tup}

    na_vals = orders_na_fill.values()

    na_vals = [str(x) for x in na_vals]

    data = {'feature': feats, 'imp_value': na_vals}

    import pandas as pd

    metadata_df = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('orders')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    return metadata_df


def get_full_billing_metadata(spark, sample=None):


    def merge_two_dicts(x, y):
        z = x.copy()
        z.update(y)
        return z

    from pyspark.sql.functions import avg as sql_avg

    billing_columns = reduce((lambda x, y: x + y), [
        ["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount",
         "Bill_N" + str(n) + "_Debt_Amount"] for n in range(1, 6)])
    feats_str = ['segment_nif'] + ['Bill_N' + str(i) + '_yearmonth_billing' for i in range(1, 5)]
    feats_bill = [name for name in sample.columns if
                  'bill' in name.lower() and 'w8' not in name.lower() and name not in billing_columns and name not in [
                      'diff_bw_days_bills', 'diff_bw_bills'] and name not in feats_str]
    feats_rgu = [name for name in sample.columns if 'rgu' in name.lower()]
    feats_tg = [name for name in sample.columns if 'tg' in name.lower()]
    feats_w8 = [name for name in sample.columns if 'w8' in name.lower()]
    feats_tar_arr = [name for name in sample.columns if 'tarifa_list' in name.lower()]
    feats_arr = feats_tar_arr + ['aux_invoice', 'aux_debt', 'aux_completed', 'diff_bw_days_bills', 'diff_bw_bills']
    feats_tar = [name for name in sample.columns if 'tarifa_' in name.lower() and name not in feats_tar_arr] + [
        'nb_superofertas']
    feats_add = ['nb_invoices', 'equal_c6_c14']
    feats_net = ['bill_n' + str(i) + '_net' for i in range(1, 6)]
    feats_inc = ['inc_bill_n1_n' + str(i) + '_net' for i in range(2, 6)] + ['inc_Bill_N1_N' + str(i) + '_Amount_To_Pay'
                                                                            for i in range(2, 6)]
    feats = feats_tar + feats_add + feats_w8 + feats_bill + feats_rgu + feats_tg + billing_columns + feats_net + feats_inc

    repartdf = sample.select(billing_columns).repartition(400).cache()

    [avg_bill_n1_invoicecharges, \
     avg_bill_n1_bill_amount, \
     avg_bill_n1_tax_amount, \
     avg_bill_n1_debt_amount, \
     avg_bill_n2_invoicecharges, \
     avg_bill_n2_bill_amount, \
     avg_bill_n2_tax_amount, \
     avg_bill_n2_debt_amount, \
     avg_bill_n3_invoicecharges, \
     avg_bill_n3_bill_amount, \
     avg_bill_n3_tax_amount, \
     avg_bill_n3_debt_amount, \
     avg_bill_n4_invoicecharges, \
     avg_bill_n4_bill_amount, \
     avg_bill_n4_tax_amount, \
     avg_bill_n4_debt_amount, \
     avg_bill_n5_invoicecharges, \
     avg_bill_n5_bill_amount, \
     avg_bill_n5_tax_amount, \
     avg_bill_n5_debt_amount] = repartdf \
        .filter((col("Bill_N1_InvoiceCharges").isNotNull()) \
                & (col("Bill_N1_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N1_Tax_Amount").isNotNull()) \
                & (col("Bill_N1_Debt_Amount").isNotNull()) \
                & (col("Bill_N2_InvoiceCharges").isNotNull()) \
                & (col("Bill_N2_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N2_Tax_Amount").isNotNull()) \
                & (col("Bill_N2_Debt_Amount").isNotNull()) \
                & (col("Bill_N3_InvoiceCharges").isNotNull()) \
                & (col("Bill_N3_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N3_Tax_Amount").isNotNull()) \
                & (col("Bill_N3_Debt_Amount").isNotNull()) \
                & (col("Bill_N4_InvoiceCharges").isNotNull()) \
                & (col("Bill_N4_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N4_Tax_Amount").isNotNull()) \
                & (col("Bill_N4_Debt_Amount").isNotNull()) \
                & (col("Bill_N5_InvoiceCharges").isNotNull()) \
                & (col("Bill_N5_Amount_To_Pay").isNotNull()) \
                & (col("Bill_N5_Tax_Amount").isNotNull()) \
                & (col("Bill_N5_Debt_Amount").isNotNull())) \
        .select(billing_columns) \
        .distinct() \
        .select(*[sql_avg("Bill_N1_InvoiceCharges").alias("avg_Bill_N1_InvoiceCharges"), \
                  sql_avg("Bill_N1_Amount_To_Pay").alias("avg_Bill_N1_Amount_To_Pay"), \
                  sql_avg("Bill_N1_Tax_Amount").alias("avg_Bill_N1_Tax_Amount"), \
                  sql_avg("Bill_N1_Debt_Amount").alias("avg_Bill_N1_Debt_Amount"), \
                  sql_avg("Bill_N2_InvoiceCharges").alias("avg_Bill_N2_InvoiceCharges"), \
                  sql_avg("Bill_N2_Amount_To_Pay").alias("avg_Bill_N2_Amount_To_Pay"), \
                  sql_avg("Bill_N2_Tax_Amount").alias("avg_Bill_N2_Tax_Amount"), \
                  sql_avg("Bill_N2_Debt_Amount").alias("avg_Bill_N2_Debt_Amount"), \
                  sql_avg("Bill_N3_InvoiceCharges").alias("avg_Bill_N3_InvoiceCharges"), \
                  sql_avg("Bill_N3_Amount_To_Pay").alias("avg_Bill_N3_Amount_To_Pay"), \
                  sql_avg("Bill_N3_Tax_Amount").alias("avg_Bill_N3_Tax_Amount"), \
                  sql_avg("Bill_N3_Debt_Amount").alias("avg_Bill_N3_Debt_Amount"), \
                  sql_avg("Bill_N4_InvoiceCharges").alias("avg_Bill_N4_InvoiceCharges"), \
                  sql_avg("Bill_N4_Amount_To_Pay").alias("avg_Bill_N4_Amount_To_Pay"), \
                  sql_avg("Bill_N4_Tax_Amount").alias("avg_Bill_N4_Tax_Amount"), \
                  sql_avg("Bill_N4_Debt_Amount").alias("avg_Bill_N4_Debt_Amount"), \
                  sql_avg("Bill_N5_InvoiceCharges").alias("avg_Bill_N5_InvoiceCharges"), \
                  sql_avg("Bill_N5_Amount_To_Pay").alias("avg_Bill_N5_Amount_To_Pay"), \
                  sql_avg("Bill_N5_Tax_Amount").alias("avg_Bill_N5_Tax_Amount"), \
                  sql_avg("Bill_N5_Debt_Amount").alias("avg_Bill_N5_Debt_Amount")]) \
        .rdd \
        .map(lambda r: [r["avg_Bill_N1_InvoiceCharges"], r["avg_Bill_N1_Amount_To_Pay"], r["avg_Bill_N1_Tax_Amount"],
                        r["avg_Bill_N1_Debt_Amount"], r["avg_Bill_N2_InvoiceCharges"], r["avg_Bill_N2_Amount_To_Pay"],
                        r["avg_Bill_N2_Tax_Amount"], r["avg_Bill_N2_Debt_Amount"], r["avg_Bill_N3_InvoiceCharges"],
                        r["avg_Bill_N3_Amount_To_Pay"], r["avg_Bill_N3_Tax_Amount"], r["avg_Bill_N3_Debt_Amount"],
                        r["avg_Bill_N4_InvoiceCharges"], r["avg_Bill_N4_Amount_To_Pay"], r["avg_Bill_N4_Tax_Amount"],
                        r["avg_Bill_N4_Debt_Amount"], r["avg_Bill_N5_InvoiceCharges"], r["avg_Bill_N5_Amount_To_Pay"],
                        r["avg_Bill_N5_Tax_Amount"], r["avg_Bill_N5_Debt_Amount"]]) \
        .first()

    var_bill = -1.0
    val_net = -1.0
    val_inc = -1.0
    val_rgu = 0.0
    val_tgs = 0.0
    val_w8 = 0.0
    val_tar = 0.0
    val_add = 0.0
    val_str = 'unknown'
    # val_arr = ['null']
    # TODO: Revisar los valores de los arrays, de los strings y de los tipos raros
    bill_tup = [(val, var_bill) for val in feats_bill + feats_net + feats_inc]
    bill_dict = {key: value for (key, value) in bill_tup}

    add_tup = [(val, val_rgu) for val in feats_tar + feats_add + feats_w8 + feats_tg + feats_rgu]
    add_dict = {key: value for (key, value) in add_tup}

    bill_na_fill_ini = {"Bill_N1_InvoiceCharges": avg_bill_n1_invoicecharges, \
                        "Bill_N1_Amount_To_Pay": avg_bill_n1_bill_amount, \
                        "Bill_N1_Tax_Amount": avg_bill_n1_tax_amount, \
                        "Bill_N1_Debt_Amount": avg_bill_n1_debt_amount, \
                        "Bill_N2_InvoiceCharges": avg_bill_n2_invoicecharges, \
                        "Bill_N2_Amount_To_Pay": avg_bill_n2_bill_amount, \
                        "Bill_N2_Tax_Amount": avg_bill_n2_tax_amount, \
                        "Bill_N2_Debt_Amount": avg_bill_n2_debt_amount, \
                        "Bill_N3_InvoiceCharges": avg_bill_n3_invoicecharges, \
                        "Bill_N3_Amount_To_Pay": avg_bill_n3_bill_amount, \
                        "Bill_N3_Tax_Amount": avg_bill_n3_tax_amount, \
                        "Bill_N3_Debt_Amount": avg_bill_n3_debt_amount, \
                        "Bill_N4_InvoiceCharges": avg_bill_n4_invoicecharges, \
                        "Bill_N4_Amount_To_Pay": avg_bill_n4_bill_amount, \
                        "Bill_N4_Tax_Amount": avg_bill_n4_tax_amount, \
                        "Bill_N4_Debt_Amount": avg_bill_n4_debt_amount, \
                        "Bill_N5_InvoiceCharges": avg_bill_n5_invoicecharges, \
                        "Bill_N5_Amount_To_Pay": avg_bill_n5_bill_amount, \
                        "Bill_N5_Tax_Amount": avg_bill_n5_tax_amount, \
                        "Bill_N5_Debt_Amount": avg_bill_n5_debt_amount, \
                        "bill_n1_net": val_net, \
                        "bill_n2_net": val_net, \
                        "bill_n3_net": val_net, \
                        "bill_n4_net": val_net, \
                        "bill_n5_net": val_net, \
                        "inc_bill_n1_n2_net": val_inc, \
                        "inc_bill_n1_n3_net": val_inc, \
                        "inc_bill_n1_n4_net": val_inc, \
                        "inc_bill_n1_n5_net": val_inc, \
                        "inc_Bill_N1_N2_Amount_To_Pay": val_inc, \
                        "inc_Bill_N1_N3_Amount_To_Pay": val_inc, \
                        "inc_Bill_N1_N4_Amount_To_Pay": val_inc, \
                        "inc_Bill_N1_N5_Amount_To_Pay": val_inc}

    aux_dict = merge_two_dicts(bill_na_fill_ini, bill_dict)
    bill_full_na_fill = merge_two_dicts(aux_dict, add_dict)

    no_feats = [name for name in feats if name not in bill_full_na_fill.keys()]
    print(no_feats)
    print(len(feats))
    print(len(bill_full_na_fill.values()))

    na_vals = bill_full_na_fill.values()
    na_vals = [str(x) for x in na_vals]
    data = {'feature': feats, 'imp_value': na_vals}

    data_str = {'feature': feats_str, 'imp_value': val_str}
    # data_arr = {'feature': feats_arr, 'imp_value': val_arr}

    import pandas as pd
    metadata_df_ = spark \
        .createDataFrame(pd.DataFrame(data)) \
        .withColumn('source', lit('billing')) \
        .withColumn('type', lit('numeric')) \
        .withColumn('level', lit('nif'))

    metadata_df_str = spark \
        .createDataFrame(pd.DataFrame(data_str)) \
        .withColumn('source', lit('billing')) \
        .withColumn('type', lit('categorical')) \
        .withColumn('level', lit('nif'))

    metadata_df = metadata_df_.union(metadata_df_str)

    return metadata_df