
from pyspark.sql.functions import when, udf, size, array, max as sql_max, count as sql_count, col, lit, desc,  \
    datediff, concat, lit, lpad, row_number, unix_timestamp, month, year, coalesce, from_unixtime, sum as sql_sum, min as sql_min, countDistinct, collect_list, avg as sql_avg

from pyspark.sql.window import Window

from pyspark.sql.types import FloatType, ArrayType, IntegerType

#from churn_nrt.src.data.ccc import CCC
from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data_utils.DataTemplate import DataTemplate

path_raw_base = '/data/raw/vf_es/'
PATH_RAW_BILLING = path_raw_base + 'billingtopsups/POSTBILLSUMOW/1.1/parquet/'
PATH_RAW_FICTIONAL_CUSTOMER = path_raw_base + 'customerprofilecar/CUSTFICTICALOW/1.0/parquet/'

from churn_nrt.src.utils.constants import HDFS_CHURN_NRT
from churn_nrt.src.utils.date_functions import move_date_n_cycles

import pandas as pd
import itertools

from churn_nrt.src.data_utils.Metadata import IMPUTE_WITH_AVG_LABEL

def _prepare_billing(df):

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
            .withColumn("billing_current_vf_debt",     when(col("billing_current_debt") < 0, 1).otherwise(0))
            .withColumn("billing_current_client_debt", when(col("billing_current_debt") > 0, 1).otherwise(0))
            .withColumn("billing_settle_debt", when( (col('Bill_N1_Debt_Amount') > col('billing_current_debt')) & (col("billing_current_vf_debt")==0),1) # deb is decreasing
                                            .otherwise(0))

          ).drop("aux_completed", "aux_invoice", "aux_debt", 'diff_bw_days_bills', 'diff_bw_bills')


    return df



def _get_billing_df(spark, closing_day):


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


class Billing(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "billing")


    def build_module(self, closing_day, save_others, **kwargs):
        '''
        Call "get_billing_module" instead of this function.
        :param spark:
        :param closing_day:
        :param labeled_mini_ids:
        :param force_gen:
        :return: a dataframe with following columns
         ['NUM_CLIENTE', 'Bill_N1_InvoiceCharges', 'Bill_N1_Amount_To_Pay', 'Bill_N1_Tax_Amount', 'Bill_N1_Debt_Amount', 'Bill_N1_net_charges', 'Bill_N1_num_ids_fict', 'Bill_N1_num_facturas',
          'Bill_N1_days_since', 'Bill_N1_yearmonth_billing', 'Bill_N2_InvoiceCharges', 'Bill_N2_Amount_To_Pay', 'Bill_N2_Tax_Amount', 'Bill_N2_Debt_Amount', 'Bill_N2_net_charges',
          'Bill_N2_num_ids_fict', 'Bill_N2_num_facturas', 'Bill_N2_days_since', 'Bill_N2_yearmonth_billing', 'Bill_N3_InvoiceCharges', 'Bill_N3_Amount_To_Pay', 'Bill_N3_Tax_Amount',
          'Bill_N3_Debt_Amount', 'Bill_N3_net_charges', 'Bill_N3_num_ids_fict', 'Bill_N3_num_facturas', 'Bill_N3_days_since', 'Bill_N3_yearmonth_billing', 'Bill_N4_InvoiceCharges',
          'Bill_N4_Amount_To_Pay', 'Bill_N4_Tax_Amount', 'Bill_N4_Debt_Amount', 'Bill_N4_net_charges', 'Bill_N4_num_ids_fict', 'Bill_N4_num_facturas', 'Bill_N4_days_since', 'Bill_N4_yearmonth_billing',
          'Bill_N5_InvoiceCharges', 'Bill_N5_Amount_To_Pay', 'Bill_N5_Tax_Amount', 'Bill_N5_Debt_Amount', 'Bill_N5_net_charges', 'Bill_N5_num_ids_fict', 'Bill_N5_num_facturas', 'Bill_N5_days_since',
          'Bill_N5_yearmonth_billing', 'aux_completed', 'billing_nb_last_bills', 'billing_avg_days_bw_bills', 'diff_bw_days_bills', 'billing_max_days_bw_bills', 'billing_min_days_bw_bills',
          'aux_invoice', 'nb_invoices', 'diff_bw_bills', 'greatest_diff_bw_bills', 'least_diff_bw_bills', 'billing_std', 'billing_mean', 'billing_current_debt', 'aux_debt', 'billing_any_vf_debt',
          'billing_current_vf_debt', 'billing_current_client_debt', 'billing_settle_debt']
        '''


        df_billing = _get_billing_df(self.SPARK, closing_day)
        df_billing_p = _prepare_billing(df_billing)

        return df_billing_p

    def get_metadata(self):

        bill_cols_template = ['Bill_N{}_days_since', 'Bill_N{}_net_charges', 'Bill_N{}_num_facturas', 'Bill_N{}_num_ids_fict', 'Bill_N{}_yearmonth_billing', "Bill_N{}_InvoiceCharges",
                              "Bill_N{}_Amount_To_Pay", "Bill_N{}_Tax_Amount", "Bill_N{}_Debt_Amount"]

        feats_gen = [p[0].format(p[1]) for p in list(itertools.product(bill_cols_template, list(range(1, 6))))]
        feats_inc = []
        feats_const = ['billing_any_vf_debt', 'billing_avg_days_bw_bills', 'billing_current_client_debt', 'billing_current_debt', 'billing_current_vf_debt', 'billing_max_days_bw_bills',
                       'billing_mean', 'billing_min_days_bw_bills', 'billing_nb_last_bills', 'billing_settle_debt', 'billing_std', 'greatest_diff_bw_bills',
                       'least_diff_bw_bills', 'nb_invoices']
        feats = feats_gen + feats_inc + feats_const

        na_vals = [
            IMPUTE_WITH_AVG_LABEL if ("InvoiceCharges" in col_ or "Amount_To_Pay" in col_ or "Debt_Amount" in col_ or "Tax_Amount" in col_) else (str(-1) if ("_num_" in col_ or "_days_" in col_) else str(0)) for
            col_ in feats]

        data = {'feature': feats, 'imp_value': na_vals}


        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(data))\
            .withColumn('source', lit('billing')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('num_cliente'))

        return metadata_df
