from pyspark.sql import Window
from pyspark.sql.functions import (udf, col, decode, when, lit, lower, concat, translate, count,
                                   sum as sql_sum, max as sql_max, min as sql_min, avg as sql_avg, greatest,
                                   least, isnull, isnan, struct, substring, size, length, year, month,
                                   dayofmonth, unix_timestamp, date_format, from_unixtime, datediff, to_date,
                                   desc, asc, countDistinct, row_number, regexp_replace, upper, trim,
                                   array, create_map, randn)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType
from src.main.python.configuration.amdocs_ids_config import PATH_FINAL_TABLE
from src.main.python.utils.general_functions import get_all_metadata


def get_mobile_portout_requests(spark, start_port, end_port):
    """
    :param spark:
    :param start_port:
    :param end_port:
    :return:
    """
      
    # mobile portout
    window_mobile = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

    PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"

    df_sol_port = (spark.read.table(PORT_TABLE_NAME)
                   .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))
                   .withColumn('sopo_ds_fecha_solicitud', from_unixtime(unix_timestamp(col('sopo_ds_fecha_solicitud'), "yyyy-MM-dd")))
                   .where((col("sopo_ds_fecha_solicitud") >= from_unixtime(unix_timestamp(lit(start_port), 'yyyyMMdd'))) & (col("sopo_ds_fecha_solicitud") <= from_unixtime(unix_timestamp(lit(end_port), 'yyyyMMdd'))))
                   .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn")
                   .withColumnRenamed("sopo_ds_fecha_solicitud", "portout_date")
                   .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(start_port[:4]), lit(start_port[4:6]), lit(start_port[6:])), 'yyyyMMdd')))
                   .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
                   .withColumn("rank", row_number().over(window_mobile))
                   .where(col("rank") == 1))

    df_sol_port = df_sol_port\
      .withColumn("label_mob", lit(1.0))\
      .select("msisdn", "label_mob", "portout_date")\
      .withColumnRenamed("portout_date", "portout_date_mob")

    print("Port-out requests for mobile services during period "
          + start_port + "-" + end_port + ": " + str(df_sol_port.count()))

    return df_sol_port


def move_date_n_days(_date, n, str_fmt="%Y%m%d"):
    """
    Returns a date corresponding to the previous day. Keeps the input format
    """

    import datetime as dt

    if n == 0:
        return _date
    if isinstance(_date, str):
        date_obj = dt.datetime.strptime(_date, str_fmt)
    else:
        date_obj = _date
    yesterday_obj = (date_obj + dt.timedelta(days=n))

    return yesterday_obj.strftime(str_fmt) if isinstance(_date, str) else yesterday_obj


def get_noninf_features():

    non_inf_feats = (["Serv_L2_days_since_Serv_fx_data",
                        "Cust_Agg_L2_total_num_services_nc",
                        "Cust_Agg_L2_total_num_services_nif",
                        "GNV_Data_hour_0_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_0_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_0_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_0_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_1_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_1_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_1_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_1_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_1_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_2_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_2_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_2_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_2_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_2_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_3_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_3_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_3_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_3_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_3_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_3_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_4_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_4_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_4_W_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_4_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_4_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_4_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_5_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_5_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_5_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_5_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_5_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_5_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_5_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_6_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_6_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_6_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_6_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_6_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_7_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_7_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_7_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_7_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_7_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_7_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_8_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_8_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_8_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_8_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_8_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_8_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_9_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_9_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_9_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_9_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_9_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_10_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_10_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_10_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_10_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_10_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_10_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_11_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_11_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_11_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_11_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_11_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_11_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_12_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_12_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_12_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_12_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_12_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_12_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_13_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_13_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_13_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_13_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_13_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_14_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_14_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_14_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_14_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_14_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_14_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_14_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_15_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_15_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_15_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_15_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_15_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_15_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_16_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_16_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_16_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_16_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_16_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_16_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_17_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_17_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_17_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_17_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_17_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_17_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_18_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_18_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_18_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_18_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_18_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_18_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_19_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_19_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_19_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_19_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_19_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_19_WE_Video_Pass_Data_Volume_MB",
                        "GNV_Data_hour_19_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_19_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_20_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_20_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_20_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_20_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_20_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_20_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_21_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_21_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_21_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_21_W_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_21_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_21_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_21_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_22_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_22_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_22_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_22_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_22_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_22_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_23_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_23_W_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_23_W_Video_Pass_Data_Volume_MB",
                        "GNV_Data_hour_23_W_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_23_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_23_WE_VideoHD_Pass_Data_Volume_MB",
                        "GNV_Data_hour_23_WE_MasMegas_Data_Volume_MB",
                        "GNV_Data_hour_23_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_0_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_0_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_1_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_1_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_1_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_2_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_2_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_2_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_3_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_3_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_3_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_4_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_4_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_4_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_5_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_5_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_5_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_6_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_6_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_6_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_7_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_7_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_7_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_8_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_8_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_8_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_9_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_9_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_9_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_10_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_10_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_10_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_11_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_11_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_11_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_12_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_12_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_12_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_13_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_13_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_13_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_14_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_14_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_14_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_15_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_16_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_16_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_16_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_17_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_17_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_17_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_18_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_18_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_18_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_19_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_19_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_19_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_20_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_20_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_20_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_21_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_21_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_21_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_22_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_22_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_22_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_W_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_W_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_W_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_W_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_23_W_Music_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_WE_Maps_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_WE_VideoHD_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_WE_Video_Pass_Num_Of_Connections",
                        "GNV_Data_hour_23_WE_MasMegas_Num_Of_Connections",
                        "GNV_Data_hour_23_WE_Music_Pass_Num_Of_Connections",
                        "GNV_Data_L2_total_data_volume_W_0",
                        "GNV_Data_L2_total_data_volume_WE_0",
                        "GNV_Data_L2_total_data_volume_W_1",
                        "GNV_Data_L2_total_data_volume_WE_1",
                        "GNV_Data_L2_total_data_volume_W_2",
                        "GNV_Data_L2_total_data_volume_WE_2",
                        "GNV_Data_L2_total_data_volume_W_3",
                        "GNV_Data_L2_total_data_volume_WE_3",
                        "GNV_Data_L2_total_data_volume_W_5",
                        "GNV_Data_L2_total_data_volume_W_6",
                        "GNV_Data_L2_total_data_volume_WE_6",
                        "GNV_Data_L2_total_data_volume_W_13",
                        "GNV_Data_L2_total_data_volume_WE_13",
                        "GNV_Data_L2_total_data_volume_WE_14",
                        "GNV_Data_L2_total_data_volume_W_15",
                        "GNV_Data_L2_total_data_volume_WE_15",
                        "GNV_Data_L2_total_data_volume_W_16",
                        "GNV_Data_L2_total_data_volume_WE_16",
                        "GNV_Data_L2_total_data_volume_W_17",
                        "GNV_Data_L2_total_data_volume_WE_17",
                        "GNV_Data_L2_total_data_volume_W_18",
                        "GNV_Data_L2_total_data_volume_WE_18",
                        "GNV_Data_L2_total_data_volume_W_19",
                        "GNV_Data_L2_total_data_volume_WE_19",
                        "GNV_Data_L2_total_data_volume_W_20",
                        "GNV_Data_L2_total_data_volume_WE_20",
                        "GNV_Data_L2_total_data_volume_W_21",
                        "GNV_Data_L2_total_data_volume_WE_21",
                        "GNV_Data_L2_total_data_volume",
                        "GNV_Data_L2_total_connections",
                        "GNV_Data_L2_data_per_connection_W",
                        "GNV_Data_L2_data_per_connection",
                        "GNV_Data_L2_max_data_volume_W",
                        "Camp_NIFs_Delight_TEL_Target_0",
                        "Camp_NIFs_Delight_TEL_Universal_0",
                        "Camp_NIFs_Ignite_EMA_Target_0",
                        "Camp_NIFs_Ignite_SMS_Control_0",
                        "Camp_NIFs_Ignite_SMS_Target_0",
                        "Camp_NIFs_Ignite_TEL_Universal_0",
                        "Camp_NIFs_Legal_Informativa_SLS_Target_0",
                        "Camp_NIFs_Retention_HH_SAT_Target_0",
                        "Camp_NIFs_Retention_HH_TEL_Target_0",
                        "Camp_NIFs_Retention_Voice_EMA_Control_0",
                        "Camp_NIFs_Retention_Voice_EMA_Control_1",
                        "Camp_NIFs_Retention_Voice_EMA_Target_0",
                        "Camp_NIFs_Retention_Voice_EMA_Target_1",
                        "Camp_NIFs_Retention_Voice_SAT_Control_0",
                        "Camp_NIFs_Retention_Voice_SAT_Control_1",
                        "Camp_NIFs_Retention_Voice_SAT_Target_0",
                        "Camp_NIFs_Retention_Voice_SAT_Target_1",
                        "Camp_NIFs_Retention_Voice_SAT_Universal_0",
                        "Camp_NIFs_Retention_Voice_SAT_Universal_1",
                        "Camp_NIFs_Retention_Voice_SMS_Control_0",
                        "Camp_NIFs_Retention_Voice_SMS_Control_1",
                        "Camp_NIFs_Retention_Voice_SMS_Target_0",
                        "Camp_NIFs_Retention_Voice_SMS_Target_1",
                        "Camp_NIFs_Terceros_TEL_Universal_0",
                        "Camp_NIFs_Terceros_TER_Control_0",
                        "Camp_NIFs_Terceros_TER_Control_1",
                        "Camp_NIFs_Terceros_TER_Target_0",
                        "Camp_NIFs_Terceros_TER_Target_1",
                        "Camp_NIFs_Terceros_TER_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_NOT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_1",
                        "Camp_NIFs_Up_Cross_Sell_MLT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Universal_1",
                        "Camp_NIFs_Up_Cross_Sell_TER_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_TER_Universal_0",
                        "Camp_NIFs_Welcome_EMA_Target_0",
                        "Camp_NIFs_Welcome_TEL_Target_0",
                        "Camp_NIFs_Welcome_TEL_Universal_0",
                        "Camp_SRV_Delight_NOT_Universal_0",
                        "Camp_SRV_Delight_SMS_Control_0",
                        "Camp_SRV_Delight_SMS_Universal_0",
                        "Camp_SRV_Ignite_MMS_Target_0",
                        "Camp_SRV_Ignite_NOT_Target_0",
                        "Camp_SRV_Ignite_SMS_Target_0",
                        "Camp_SRV_Ignite_SMS_Universal_0",
                        "Camp_SRV_Legal_Informativa_EMA_Target_0",
                        "Camp_SRV_Legal_Informativa_MLT_Universal_0",
                        "Camp_SRV_Legal_Informativa_MMS_Target_0",
                        "Camp_SRV_Legal_Informativa_NOT_Target_0",
                        "Camp_SRV_Legal_Informativa_SMS_Target_0",
                        "Camp_SRV_Retention_Voice_EMA_Control_0",
                        "Camp_SRV_Retention_Voice_EMA_Control_1",
                        "Camp_SRV_Retention_Voice_EMA_Target_0",
                        "Camp_SRV_Retention_Voice_EMA_Target_1",
                        "Camp_SRV_Retention_Voice_MLT_Universal_0",
                        "Camp_SRV_Retention_Voice_NOT_Control_0",
                        "Camp_SRV_Retention_Voice_NOT_Target_0",
                        "Camp_SRV_Retention_Voice_NOT_Target_1",
                        "Camp_SRV_Retention_Voice_NOT_Universal_0",
                        "Camp_SRV_Retention_Voice_SAT_Control_0",
                        "Camp_SRV_Retention_Voice_SAT_Control_1",
                        "Camp_SRV_Retention_Voice_SAT_Target_0",
                        "Camp_SRV_Retention_Voice_SAT_Target_1",
                        "Camp_SRV_Retention_Voice_SAT_Universal_0",
                        "Camp_SRV_Retention_Voice_SAT_Universal_1",
                        "Camp_SRV_Retention_Voice_SLS_Control_0",
                        "Camp_SRV_Retention_Voice_SLS_Control_1",
                        "Camp_SRV_Retention_Voice_SLS_Target_0",
                        "Camp_SRV_Retention_Voice_SLS_Target_1",
                        "Camp_SRV_Retention_Voice_SLS_Universal_0",
                        "Camp_SRV_Retention_Voice_SLS_Universal_1",
                        "Camp_SRV_Retention_Voice_SMS_Control_0",
                        "Camp_SRV_Retention_Voice_SMS_Control_1",
                        "Camp_SRV_Retention_Voice_SMS_Target_1",
                        "Camp_SRV_Retention_Voice_SMS_Universal_0",
                        "Camp_SRV_Retention_Voice_SMS_Universal_1",
                        "Camp_SRV_Retention_Voice_TEL_Control_1",
                        "Camp_SRV_Retention_Voice_TEL_Target_1",
                        "Camp_SRV_Retention_Voice_TEL_Universal_0",
                        "Camp_SRV_Retention_Voice_TEL_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Control_0",
                        "Camp_SRV_Up_Cross_Sell_EMA_Control_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Target_0",
                        "Camp_SRV_Up_Cross_Sell_EMA_Target_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_MLT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_MMS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_NOT_Control_0",
                        "Camp_SRV_Up_Cross_Sell_NOT_Target_0",
                        "Camp_SRV_Up_Cross_Sell_NOT_Target_1",
                        "Camp_SRV_Up_Cross_Sell_NOT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SMS_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Control_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Control_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Target_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Target_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Universal_1",
                        "Camp_SRV_Welcome_EMA_Target_0",
                        "Camp_SRV_Welcome_MMS_Target_0",
                        "Camp_SRV_Welcome_SMS_Target_0",
                        "Camp_L2_srv_total_redem_Target",
                        "Camp_L2_srv_pcg_redem_Target",
                        "Camp_L2_srv_total_redem_Control",
                        "Camp_L2_srv_total_redem_Universal",
                        "Camp_L2_srv_total_redem_EMA",
                        "Camp_L2_srv_total_redem_TEL",
                        "Camp_L2_srv_total_camps_SAT",
                        "Camp_L2_srv_total_redem_SAT",
                        "Camp_L2_srv_total_redem_SMS",
                        "Camp_L2_srv_pcg_redem_SMS",
                        "Camp_L2_srv_total_camps_MMS",
                        "Camp_L2_srv_total_redem_MMS",
                        "Camp_L2_srv_total_redem_Retention_Voice",
                        "Camp_L2_srv_total_redem_Up_Cross_Sell",
                        "Camp_L2_nif_total_redem_Target",
                        "Camp_L2_nif_pcg_redem_Target",
                        "Camp_L2_nif_total_redem_Control",
                        "Camp_L2_nif_total_redem_Universal",
                        "Camp_L2_nif_total_redem_EMA",
                        "Camp_L2_nif_total_redem_TEL",
                        "Camp_L2_nif_total_camps_SAT",
                        "Camp_L2_nif_total_redem_SAT",
                        "Camp_L2_nif_total_redem_SMS",
                        "Camp_L2_nif_pcg_redem_SMS",
                        "Camp_L2_nif_total_camps_MMS",
                        "Camp_L2_nif_total_redem_MMS",
                        "Camp_L2_nif_total_redem_Retention_Voice",
                        "Camp_L2_nif_total_redem_Up_Cross_Sell",
                        "Serv_PRICE_TARIFF",
                        "GNV_Data_hour_0_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_0_WE_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_1_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_1_WE_Video_Pass_Data_Volume_MB",
                        "GNV_Data_hour_2_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_4_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_8_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_9_W_Maps_Pass_Data_Volume_MB",
                        "GNV_Data_hour_16_WE_Music_Pass_Data_Volume_MB",
                        "GNV_Data_hour_0_W_RegularData_Num_Of_Connections",
                        "GNV_Data_hour_0_WE_RegularData_Num_Of_Connections",
                        "GNV_Voice_hour_23_WE_MOU",                           ### solo un mou??
                        "GNV_Data_hour_7_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_8_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_9_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_12_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_13_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_14_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_15_WE_RegularData_Data_Volume_MB",
                        "GNV_Data_hour_16_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_17_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_18_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_18_W_RegularData_Num_Of_Connections",
                        "GNV_Data_hour_19_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_19_W_RegularData_Num_Of_Connections",
                        "GNV_Data_hour_20_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_20_W_RegularData_Num_Of_Connections",
                        "GNV_Data_hour_21_W_Chat_Zero_Num_Of_Connections",
                        "GNV_Data_hour_21_W_RegularData_Num_Of_Connections",
                        "GNV_Roam_Data_L2_total_connections_W",
                        "GNV_Roam_Data_L2_total_data_volume_WE",
                        "GNV_Roam_Data_L2_total_data_volume_W",
                        "Camp_NIFs_Delight_SMS_Control_0",
                        "Camp_NIFs_Delight_SMS_Target_0",
                        "Camp_NIFs_Legal_Informativa_EMA_Target_0",
                        "Camp_SRV_Delight_EMA_Target_0",
                        "Camp_SRV_Delight_MLT_Universal_0",
                        "Camp_SRV_Delight_NOT_Target_0",
                        "Camp_SRV_Delight_TEL_Universal_0",
                        "Camp_SRV_Retention_Voice_TEL_Control_0",
                        "Camp_L2_srv_total_camps_TEL",
                        "Camp_L2_srv_total_camps_Target",
                        "Camp_L2_srv_total_camps_Up_Cross_Sell",
                        "Camp_L2_nif_total_camps_TEL",
                        "Camp_L2_nif_total_camps_Target",
                        "Camp_L2_nif_total_camps_Up_Cross_Sell"
                        ])
    return non_inf_feats


def get_id_features():
    return ["msisdn", "rgu", "num_cliente", "CAMPO1", "CAMPO2", "CAMPO3", "NIF_CLIENTE", "IMSI", "Instancia_P",
            "nif_cliente_tgs", "num_cliente_tgs", "msisdn_d", "num_cliente_d", "nif_cliente_d"]


def get_no_input_feats():
    return ["Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_NOMBRE", "Cust_PRIM_APELLIDO", "Cust_SEG_APELLIDO", "Cust_DIR_LINEA1", "Cust_DIR_LINEA2",
            "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2", "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4",
            "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT", "Cust_APELLIDO1_CLI_FACT",
            "Cust_APELLIDO2_CLI_FACT", "Cust_DIR_LINEA1", "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2",
            "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4", "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT",
            "Cust_APELLIDO1_CLI_FACT", "Cust_APELLIDO2_CLI_FACT", "Cust_CTA_CORREO_CONTACTO", "Cust_CTA_CORREO",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE", "CCC_L2_bucket_list",
            "CCC_L2_bucket_set", "Serv_NUM_SERIE_DECO_TV", "Order_N1_Description", "Order_N2_Description",
            "Order_N5_Description", "Order_N7_Description", "Order_N8_Description", "Order_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "device_n5_imei", "Order_N1_Id", "Order_N2_Id", "Order_N3_Id", "Order_N4_Id", "Order_N5_Id", "Order_N6_Id",
            "Order_N7_Id", "Order_N8_Id", "Order_N9_Id", "Order_N10_Id"]

def getOrderedRelevantFeats(model, featCols, pca="f"):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances
        return getImportantFeaturesFromVector(featCols, impFeats)


def getImportantFeaturesFromVector(featCols, impFeats):
    feat_and_imp=zip(featCols, impFeats.toArray())
    return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)


def get_cumulative_lift_fix_step(spark, df, step_=5000, ord_col="scoring", label_col="label", verbose=True):
    """

    :param spark: spark session
    :param df: dataframe that includes both the target (label) and the numeric column (ord_col) used to sort the customers/services
    :param step_:
    :param ord_col: column of the dataframe that must be considered when ordering the customers/services
    :param label_col: target indicating the condition (churn or no churn) of the customer/service (it assumed to be a double with two possible values 1.0 or 0.0)
    :param verbose: print info or not
    :return: dataframe including cum_volume and cum_lift
    """

    from pyspark.ml.feature import QuantileDiscretizer

    import math

    n_samples = df.count()

    if verbose:
        print "[Info get_cumulative_lift2] Size of the input df: " + str(n_samples)

    tmp_num_buckets = int(math.floor(float(n_samples)/float(step_)))

    num_buckets = tmp_num_buckets if tmp_num_buckets <= 100 else 100

    if verbose:
        print "[Info get_cumulative_lift2] Number of buckets for a resolution of " + str(step_) + ": " + str(tmp_num_buckets) + " - Final number of buckets: " + str(num_buckets)

    qds = QuantileDiscretizer(numBuckets=num_buckets, inputCol=ord_col, outputCol="bucket", relativeError=0.0)

    bucketed_df = qds.fit(df).transform(df)

    # Num churners per bucket

    bucketed_df = bucketed_df\
        .groupBy('bucket')\
        .agg(count("*").alias("volume"), sql_sum(label_col).alias('num_churners'))\
        .withColumn('all_segment', lit('all'))

    windowval = (Window.partitionBy('all_segment').orderBy(desc('bucket')).rangeBetween(Window.unboundedPreceding, 0))

    result_df = bucketed_df \
        .withColumn('cum_num_churners', sql_sum('num_churners').over(windowval)) \
        .withColumn('cum_volume', sql_sum('volume').over(windowval)) \
        .withColumn('lift', col('num_churners') / col('volume')) \
        .withColumn('cum_lift', col('cum_num_churners') / col('cum_volume'))

    if verbose:
        result_df.orderBy(desc('bucket')).show(50, False)

    return result_df


def evaluate_model(spark, tr_date, tt_date):

    ##########################################################################################
    # 1. Training data
    ##########################################################################################

    # 1.1. Loading the IDS for tr_date

    hdfs_partition_path_tr = 'year=' + str(int(tr_date[:4])) +\
                             '/month=' + str(int(tr_date[4:6])) +\
                             '/day=' + str(int(tr_date[6:8]))
    print hdfs_partition_path_tr
    print "reading ids"
    unlab_tr_df = spark.read.load(PATH_FINAL_TABLE + hdfs_partition_path_tr)

    # 1.2. Port-out requests during the next 30 days since tr_date

    sols_port_tr = get_mobile_portout_requests(spark, tr_date, move_date_n_days(tr_date, 30))\
        .withColumnRenamed("label_mob", "label")

    # 1.3. Left join to add the column label. Those msisdn not found in sols_port_tr are labeled as 0.0

    lab_tr_df = unlab_tr_df.join(sols_port_tr, ['msisdn'], 'left').na.fill({'label': 0.0})

    ##########################################################################################
    # 3. Test set
    ##########################################################################################

    hdfs_partition_path_tt = 'year=' + str(int(tt_date[:4])) +\
                             '/month=' + str(int(tt_date[4:6])) +\
                             '/day=' + str(int(tt_date[6:8]))
    print hdfs_partition_path_tt
    # 3.1. Loading the IDS for tt_date

    unlab_tt_df = spark.read.load(PATH_FINAL_TABLE + hdfs_partition_path_tt)

    # 3.2. Port-out requests during the next 30 days since tr_date

    sols_port_tt = get_mobile_portout_requests(spark, tt_date, move_date_n_days(tt_date, 30))\
        .withColumnRenamed("label_mob", "label")

    # 3.3. Left join to add the column label. Those msisdn not found in sols_port_tt are labeled as 0.0

    lab_tt_df = unlab_tt_df.join(sols_port_tt, ['msisdn'], 'left').na.fill({'label': 0.0})

    ##########################################################################################
    # 4. Modeling
    ##########################################################################################

    # 4.1. Getting the metadata to identify numerical and categorical features

    non_inf_features = get_noninf_features() + get_no_input_feats()

    final_map, categ_map, numeric_map, date_map = get_all_metadata(tr_date)

    print "metadata amount:", len(final_map), len(categ_map), len(numeric_map), len(date_map)

    categorical_columns = list(set(categ_map.keys()) - set(non_inf_features))

    numeric_columns = list(set(numeric_map.keys()) - set(non_inf_features))

    # 4.2. Building the stages of the pipeline related to the input features

    stages = []

    for categorical_col in categorical_columns:
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index', handleInvalid='keep')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    assembler_inputs = [c + "_class_vec" for c in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    for cfeat in categorical_columns:
        print "[Info model_template] Categorical column: " + cfeat

    for nfeat in numeric_columns:
        print "[Info model_template] Numeric column: " + nfeat

    # 4.3. Adding the stage related to the classifier

    dt = RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol="label", seed=1234,
                                       maxBins=32, minInstancesPerNode=100, impurity='gini',
                                       featureSubsetStrategy='sqrt', subsamplingRate=0.7)
    stages += [dt]

    # 4.4. Pipeline is completed and used to train the model, which us stored in the variable pipeline_model

    pipeline = Pipeline(stages=stages)
    print "fit"
    pipeline_model = pipeline.fit(lab_tr_df)

    feat_importance_list = getOrderedRelevantFeats(pipeline_model, assembler_inputs, pca="f")

    for fimp in feat_importance_list:
        print "[Info model_template] Feat imp - " + fimp[0] + ": " + str(fimp[1])

    ###########################################################################################
    # 5. Evaluation
    ###########################################################################################

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    tr_predictions = pipeline_model\
          .transform(lab_tr_df) \
          .withColumn("model_score", getScore(col("probability")).cast(DoubleType())) \
          .withColumn('model_score', col('model_score') + lit(0.00001) * randn())

    tt_predictions = pipeline_model\
          .transform(lab_tt_df) \
          .withColumn("model_score", getScore(col("probability")).cast(DoubleType())) \
          .withColumn('model_score', col('model_score') + lit(0.00001) * randn())

    evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='label', metricName='areaUnderROC')

    tr_auc = evaluator.evaluate(tr_predictions)

    tt_auc = evaluator.evaluate(tt_predictions)

    print "[Info model_template] Result - tr_date = " + str(tr_date) + " - tt_date = " + str(tt_date) + " - AUC(tr) = " + str(tr_auc) + " - AUC(tt) = " + str(tt_auc)

    lift_df = get_cumulative_lift_fix_step(spark, tt_predictions, step_=10000, ord_col="model_score", label_col="label", verbose=True)

    return lift_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Testing of IDS with a model',
                                     epilog='Please report bugs and issues to Carlos or someone else')
    parser.add_argument('-tr', '--training-day', metavar='<training_day>', type=str, help='YearMonthDay (YYYYMMDD) of the training day to process')
    parser.add_argument('-tt', '--test-day',  metavar='<test_day>',  type=str, help='YearMonthDay (YYYYMMDD) of the test day to process')

    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.\
        appName("Model template ids ML testing").\
        master("yarn").\
        config("spark.submit.deployMode", "client").\
        config("spark.ui.showConsoleProgress", "true").\
        enableHiveSupport().\
        getOrCreate()

    evaluate_model(spark, args.training_day, args.test_day)

    print "Finished!!!"
