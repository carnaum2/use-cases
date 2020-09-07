from pyspark.sql.functions import (unix_timestamp, udf,col,max as sql_max, stddev as sql_stddev, when, count, isnull, concat, lpad, trim, lit, sum as sql_sum, length, upper)
from pyspark.sql.window import Window
from pyspark.sql.functions import asc, datediff, lag
from pyspark.sql.functions import asc, datediff, lag, min as sql_min, max as sql_max, avg as sql_avg
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import unix_timestamp, from_unixtime

class MegaAggTable:

    def __init__(self, spark_session, df_mega):
        """
        Class that facilitates the use of the mega table.
        :param spark_session: SparkSession.
        """
        self.spark = spark_session
        self.df_mega = df_mega


    def agg_topup_info(self):
        #         Method that adds aggregated top up data from a mega table to an input table. The fields added are:
        #         mega_tu_avg_diff_days - Average number of days between days where topups where made,
        #         mega_tu_min_diff_days - Minimum number of days between days where topups where made,
        #         mega_tu_stddev_diff_days - Standard deviation of number of days between days where topups where made,
        #         mega_tu_amount_avg - Average daily spending on topups, on days where topups were made,
        #         mega_tu_amount_sum - Total amount spent on topups,
        #         mega_tu_num_sum - Total number of topups in the period,
        #         mega_tu_num_distinct_days - Number of distinct days with topups,
        #         mega_tu_latest_ts - Most recent topup time stamp,
        #         mega_tu_next_ts - Prediction of next topup time stamp,
        #         mega_tu_diff_days_since_last_tu - Number of days since last top up,
        #         mega_tu_num_days_until_next_tu - Number of days until the the next top up.

        df_agg_topups = (self.df_mega.where(col("tu_bin")>0)
                         .withColumn("lag_period_ts", datediff(col('beginning_of_day_ts'), lag(col('beginning_of_day_ts')).over(Window.partitionBy("msisdn").orderBy(asc("beginning_of_day_ts")))))
                         .groupBy("msisdn")
                         .agg(sql_min("beginning_of_day_ts").alias("first_period_ts"),
                              sql_max("beginning_of_day_ts").alias("tu_latest_ts"),
                              sql_avg("tu_amount").alias("avg_tu_amount"),
                              sql_sum("tu_amount").alias("sum_tu_amount"),
                              sql_sum("tu_num").alias("sum_tu_num"),
                              sql_sum("tu_bin").alias("tu_num_distinct_days"),
                              sql_avg("lag_period_ts").alias("tu_avg_diff_days"),
                              sql_min("lag_period_ts").alias("tu_min_diff_days"),
                              sql_max("lag_period_ts").alias("tu_max_diff_days"),
                              sql_stddev("lag_period_ts").alias("tu_stddev_diff_days"),
                              sql_avg("bal_bod").alias("bal_avg_tu_day") # what I really want is balance before topup. this is an approximation
                          ).withColumnRenamed("msisdn", "msisdn_topups")).fillna(0)

        return df_agg_topups


    def agg_balance_info(self):
        # add_aggregated_balance_information

        #     Method that adds aggregated balance information from a mega input hive table to an input hive table.
        #     The fields added are:
        #     bal_diff_avg_num_days_with - Average number of days with a balance difference (diff!=0),
        #     bal_diff_avg_num_days_pos - Average number of days with a positive balance difference,
        #     bal_diff_avg_num_days_neg - Average number of days with a negative balance difference,
        #     bal_diff_avg_pos - Average positive balance difference,
        #     bal_diff_avg_neg - Average negative balance difference,
        #     bal_diff_avg - Average balance difference,
        #     abal_num_sum - Total number of balance advances made,
        #     abal_payment_sum - Total amount paid due to balance advances,
        #     abal_amount_sum - Total amount of balance advanced.
        #     tbal_rec_num_sum - Number of days where the msisdn received balance from a transfer,
        #     tbal_rec_amount_sum - Total number of transfers received by the msisdn.
        #     tbal_tra_num_sum - Total amount of balance received by the msisdn in balance transfers,
        #     tbal_tra_amount_sum - Number of days where the msisdn transferred balance

        df_agg_balance = (self.df_mega.groupBy("msisdn")
                          .agg(
            sql_sum(when((col("bal_diff") != 0), 1).otherwise(0)).alias("bal_diff_avg_num_days_with"),
            sql_sum(when((col("bal_diff") > 0), 1).otherwise(0)).alias("bal_diff_avg_num_days_pos"),
            sql_sum(when((col("bal_diff") < 0), 1).otherwise(0)).alias("bal_diff_avg_num_days_neg"),
            sql_avg(when((col("bal_diff") > 0), 1).otherwise(0)).alias("bal_diff_avg_pos"),
            sql_avg(when((col("bal_diff") < 0), 1).otherwise(0)).alias("bal_avg_neg_diff"),
            sql_avg("bal_bod").alias("bal_avg"),
            sql_avg(col("abal_amount")).alias("abal_amount_avg"),
            sql_sum(col("abal_num")).alias("abal_num_sum"),
            sql_sum(col("abal_payment")).alias("abal_payment_sum"),
            sql_sum(col("tbal_rec_amount")).alias("tbal_rec_amount_sum"),
            sql_sum(col("tbal_rec_num")).alias("tbal_rec_num_sum"),
            sql_sum(col("tbal_tra_amount")).alias("tbal_tra_amount_sum"),
            sql_sum(col("tbal_tra_num")).alias("tbal_tra_num_sum"))
                          .withColumnRenamed("msisdn", "msisdn_balance")
                          ).fillna(0)

        return df_agg_balance



    def agg_traffic_info(self):
        df_agg_traffic = (self.df_mega.groupBy("msisdn")
                          .agg(sql_sum(col("voice_num")).alias("voice_num_sum"),
                               sql_sum(col("voice_duration")).alias("voice_duration_sum"),
                               sql_avg(col("voice_duration")).alias("voice_duration_avg"),
                               sql_sum(col("voicesms_amount")).alias("voicesms_amount_sum"),
                               sql_sum(col("voice_amount")).alias("voice_amount_sum"),
                               sql_avg(col("voice_amount")).alias("voice_amount_avg"),
                               sql_sum(col("sms_num")).alias("sms_num_sum"),
                               sql_sum(col("sms_amount")).alias("sms_amount_sum"),
                               sql_avg(col("sms_amount")).alias("sms_amount_avg"),
                               sql_sum(col("data_mb")).alias("data_mb_sum"),
                               sql_avg(col("data_mb")).alias("data_mb_avg"),
                               sql_sum(col("data_amount")).alias("data_amount_sum"),
                               sql_avg(col("data_amount")).alias("data_amount_avg"),
                               count(col("data_amount")).alias("num_conexiones_sum")
                               # sql_sum(col("event_num")).alias("event_num_sum"),
                               # sql_sum(col("event_amount")).alias("event_amount_sum"),
                               # sql_avg(col("event_amount")).alias("event_amount_avg")
                               ).withColumnRenamed("msisdn", "msisdn_traffic")
                               .withColumn("amount_sum", col("voicesms_amount_sum") + col("data_amount_sum"))).fillna(0)

        return df_agg_traffic



    def agg_tariffs_info(self, analysis_start_obj, analysis_end_obj):

        df_tarifas = (self.spark.read.table("raw_es.vf_pre_info_tarif")
                      .withColumn('entry_ts',
                                  from_unixtime(unix_timestamp(concat_ws('/', lit("1"), "month", "year"), 'dd/MM/yyy')))
                      .where((col("entry_ts") >= analysis_start_obj) &
                             (col("entry_ts") <= analysis_end_obj))
                      )

        df_tarifas_agg = (df_tarifas.groupBy("msisdn")
                          .agg(sql_sum(col("TOTAL_LLAMADAS")).alias("tariffs_total_llamadas"),
                               sql_sum(col("TOTAL_SMS")).alias("tariffs_total_sms"),
                               sql_sum(col("MOU")).alias("tariffs_mou"),
                               sql_sum(col("MOU_Week")).alias("tariffs_mou_week"),
                               sql_sum(col("LLAM_Week")).alias("tariffs_llam_week"),
                               sql_sum(col("SMS_Week")).alias("tariffs_sms_week"),
                               sql_sum(col("MOU_Weekend")).alias("tariffs_mou_weekend"),
                               sql_sum(col("LLAM_Weekend")).alias("tariffs_llam_weekend"),
                               sql_sum(col("SMS_Weekend")).alias("tariffs_sms_weekend"),
                               sql_sum(col("MOU_VF")).alias("tariffs_mou_vf"),
                               sql_sum(col("LLAM_VF")).alias("tariffs_llam_vf"),
                               sql_sum(col("SMS_VF")).alias("tariffs_sms_vf"),
                               sql_sum(col("MOU_Fijo")).alias("tariffs_mou_fijo"),
                               sql_sum(col("LLAM_Fijo")).alias("tariffs_llam_fijo"),
                               sql_sum(col("SMS_Fijo")).alias("tariffs_sms_fijo"),
                               sql_sum(col("MOU_OOM")).alias("tariffs_mou_oom"),
                               sql_sum(col("SMS_OOM")).alias("tariffs_sms_oom"),
                               sql_sum(col("MOU_Internacional")).alias("tariffs_mou_internac"),
                               sql_sum(col("LLAM_Internacional")).alias("tariffs_llam_internac"),
                               sql_sum(col("SMS_Internacional")).alias("tariffs_sms_internac"),
                               sql_sum(col("ActualVolume")).alias("tariffs_actual_volume"),
                               sql_sum(col("Num_accesos")).alias("tariffs_num_accesos"),
                               sql_sum(col("Num_Cambio_Planes")).alias("tariffs_num_cambio_planes"),
                               sql_sum(col("LLAM_COMUNIDAD_SMART")).alias("tariffs_llam_smart"),
                               sql_sum(col("MOU_COMUNIDAD_SMART")).alias("tariffs_mou_smart"),
                               sql_sum(col("LLAM_SMS_COMUNIDAD_SMART")).alias("tariffs_llam_sms_smart")
                               ).withColumnRenamed("msisdn", "msisdn_tariff")).fillna(0)

        df_tarifas_agg = df_tarifas_agg.withColumn("type", when(
            col("tariffs_total_llamadas") + col("tariffs_num_accesos") + col("tariffs_total_sms") == 0, "dead_line")
                                                   .when(
            (col("tariffs_total_llamadas") > 0) & (col("tariffs_num_accesos") == 0) & (col("tariffs_total_sms") == 0),
            "only_calls")
                                                   .when(
            (col("tariffs_total_llamadas") == 0) & (col("tariffs_num_accesos") > 0) & (col("tariffs_total_sms") == 0),
            "only_data")
                                                   .when(
            (col("tariffs_total_llamadas") == 0) & (col("tariffs_num_accesos") == 0) & (col("tariffs_total_sms") >= 0),
            "only_sms")
                                                   .otherwise("used_line"))


        return df_tarifas_agg