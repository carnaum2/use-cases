from pyspark.sql.functions import (udf, col, array, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull, substring, size, length, desc)
from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles
from pyspark.sql import functions as F
import datetime as dt

from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data_utils.DataTemplate import DataTemplate

reimb_map = {'Reimbursement_adjustment_net': 0.0, \
             'Reimbursement_adjustment_debt': 0.0, \
             'Reimbursement_num': 0.0, \
             'Reimbursement_num_n8': 0.0, \
             'Reimbursement_num_n6': 0.0, \
             'Reimbursement_days_since': 1000.0, \
             'Reimbursement_num_n4': 0.0, \
             'Reimbursement_num_n5': 0.0, \
             'Reimbursement_num_n2': 0.0, \
             'Reimbursement_num_n3': 0.0, \
             'Reimbursement_days_2_solve': -1.0, \
             'Reimbursement_num_month_2': 0.0, \
             'Reimbursement_num_n7': 0.0, \
             'Reimbursement_num_n1': 0.0, \
             'Reimbursement_num_month_1': 0.0}

class Reimbursements(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "reimbursements")


    def build_module(self, closing_day, save_others, **kwargs):

        print'Generating reimbursements module for ' + closing_day

        df_reembolsos = self.SPARK.read.load('hdfs://nameservice1/data/raw/vf_es/billingtopsups/AJUSTESGNV/1.0/parquet')

        date_col = 'FTX_BILL_DTM'
        c_date = dt.datetime(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
        df_reim = df_reembolsos.where((col(date_col) < c_date))
        from churn_nrt.src.utils.date_functions import move_date_n_days
        starting_day = move_date_n_days(closing_day, n=-60)
        s_date = dt.datetime(int(starting_day[:4]), int(starting_day[4:6]), int(starting_day[6:8]))
        df_reim = df_reim.where((col(date_col) > s_date))

        ClosingDay = closing_day
        #ClosingDay_date = datetime.date(int(ClosingDay[:4]), int(ClosingDay[4:6]), int(ClosingDay[6:8]))
        #hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))
        #hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'
        #path_customer = hdfs_write_path_common + 'customer/' + hdfs_partition_path

        customerDF_load = CustomerBase(self.SPARK).get_module(ClosingDay, save=save_others, save_others=save_others) \
            .select('NUM_CLIENTE', 'NIF_CLIENTE') \
            .filter((~isnull(col('NUM_CLIENTE'))) & (~isnull(col('NIF_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')) & (~col('NIF_CLIENTE').isin('', ' '))) \
            .dropDuplicates()

        df_reim_nif = customerDF_load.select('NUM_CLIENTE', 'NIF_CLIENTE').join(df_reim, customerDF_load.NUM_CLIENTE == df_reim.CO_ID_CLIENTE_CRM, 'inner')

        print("############ Extra Feats Generation ############")

        timeFmt = "%Y-%m-%d %H:%M:%S"
        timeFmt2 = "%Y%m%d"
        days_since = ((F.unix_timestamp('closing_day', format=timeFmt2) - F.unix_timestamp('FTX_BILL_DTM', format=timeFmt)) / (3600 * 24))
        solving_time = ((F.unix_timestamp('service_processed_at', format=timeFmt) - F.unix_timestamp('FTX_BILL_DTM',
                                                                                                     format=timeFmt)) / (3600 * 24))

        counts = df_reim_nif.groupBy("NIF_CLIENTE").count().alias('count')

        from pyspark.sql.functions import count as sql_count, min as sql_min, sum as sql_sum, avg as sql_avg, max as sql_max
        reimbursements = (
            df_reim_nif.join(counts,
              ['NIF_CLIENTE'],
              'left') \
            .na.fill({'count': 0.0}).withColumn(
                "Reimbursement_type", df_reim["CO_ADJUSTMENT_TYPE_ID"]
            )
                .withColumn("Reimbursement_Paymaster", df_reim["CO_INVOICING_CO_NAME"])
                .withColumn("Reimbursement_status", df_reim["CO_ADJUSTMENT_STATUS"])
                .withColumn(
                "Reimbursement_adjustment_debt", df_reim["IM_ADJUSTMENT_DEBT_MNY"]
            )
                .withColumn(
                "Reimbursement_adjustment_net", df_reim["IM_ADJUSTMENT_NET_MNY"]
            )
                .withColumn("closing_day", lit(c_date))
                .withColumn("Reimbursement_days_since", days_since)
                .withColumn("Reimbursement_days_2_solve", solving_time)
                .drop(col("closing_day"))
                .groupBy("NIF_CLIENTE").agg(sql_min("Reimbursement_Paymaster").alias("Reimbursement_Paymaster"),
                                            sql_avg("Reimbursement_status").alias("Reimbursement_status"),
                                            sql_sum("Reimbursement_adjustment_debt").alias(
                                                "Reimbursement_adjustment_debt"),
                                            sql_max("Reimbursement_adjustment_net").alias(
                                                "Reimbursement_adjustment_net"),
                                            sql_avg("Reimbursement_days_since").alias("Reimbursement_days_since"),
                                            sql_max("Reimbursement_days_2_solve").alias("Reimbursement_days_2_solve"),
                                            sql_count('count').alias('Reimbursement_num'))
        )


        print 'Size of reimbursements table: {}'.format(df_reim.count())

        start_dates = []
        start_dates.append(c_date)

        print("############ Number of Reimbursements per week and month ############")

        start_dates = []

        closing_date = dt.datetime(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:9]))
        start_dates.append(closing_date)
        for i in range(1, 10):
            start_day = move_date_n_cycles(closing_day, n=-i)
            s_date = dt.datetime(int(start_day[:4]), int(start_day[4:6]), int(start_day[6:9]))
            start_dates.append(s_date)

        reimbursements_f = reimbursements

        num_r = reimbursements_f.count()
        print(num_r)

        date_col = 'FTX_BILL_DTM'
        for i in range(0, 8):
            counts = df_reim_nif.where(
                (col(date_col) < start_dates[i]) & (col(date_col) > start_dates[i + 1])).groupBy(
                "NIF_CLIENTE").count().alias('count')
            print(start_dates[i])
            reimbursements_f = reimbursements_f.join(counts, ['NIF_CLIENTE'], 'left') \
                .na.fill({'count': 0.0}).withColumnRenamed('count', 'Reimbursement_num_n' + str(i + 1))
            print'Generated column for cycle {}'.format(-(i + 1))
            reimbursements_f = reimbursements_f.cache()

        for j in range(1, 3):
            month_cols = ['Reimbursement_num_n' + str(i) for i in range(1, j * 4 + 1)]
            print(month_cols)
            reimbursements_f = reimbursements_f.withColumn('Reimbursement_num_month_' + str(j),
                                                           sum(reimbursements_f[col] for col in month_cols))
            print'Generated column for month {}'.format(-(j))

        extra_reim = [name for name in reimbursements_f.columns if name.lower().startswith('reimbursement')]
        non_val = ['Reimbursement_type', 'Reimbursement_Paymaster', 'Reimbursement_status', 'Reimbursement_code',
                   'Reimbursement_revenue_code']
        extra_f = list(set(extra_reim) - set(non_val))

        reimbursements_extra = reimbursements_f.select(['NIF_CLIENTE'] + extra_f)

        print("############ Generated feats ############")
        print(extra_reim)
        print("#########################################")

        print("############ Saving Stage ############")

        reimbursements_extra = reimbursements_extra.drop_duplicates(subset=['NIF_CLIENTE'])

        return reimbursements_extra

    def get_metadata(self):

        reimb_map = {'Reimbursement_adjustment_net': 0.0, 'Reimbursement_adjustment_debt': 0.0, 'Reimbursement_num': 0.0, 'Reimbursement_num_n8': 0.0, 'Reimbursement_num_n6': 0.0, \
                     'Reimbursement_days_since': 10000.0, 'Reimbursement_num_n4': 0.0, 'Reimbursement_num_n5': 0.0, 'Reimbursement_num_n2': 0.0, 'Reimbursement_num_n3': 0.0, \
                     'Reimbursement_days_2_solve': -1.0, 'Reimbursement_num_month_2': 0.0, 'Reimbursement_num_n7': 0.0, 'Reimbursement_num_n1': 0.0, 'Reimbursement_num_month_1': 0.0}

        feats = reimb_map.keys()

        na_vals = reimb_map.values()

        na_vals = [str(x) for x in na_vals]

        data = {'feature': feats, 'imp_value': na_vals}

        import pandas as pd

        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('reimbursements')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('nif'))

        return metadata_df