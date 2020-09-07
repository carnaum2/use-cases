from pyspark.sql.types import StringType, TimestampType, IntegerType, DoubleType, StructType, StructField, DateType
                               sep=";", decimal=".")
import pandas as pd
import os

from pyspark.sql.functions import (col)



table_speed_bucket = "tests_es.csanc109_SPEEDBUCKETS_PRE"
table_bonos = "tests_es.csanc109_BONOS_PRE"


def generate_tables(spark, USE_CASES_PATH):

    df_speedbucket = pd.read_csv \
        (os.path.join(USE_CASES_PATH, "Prepaid_Segmentation", "resources", "SPEEDBUCKETS_PRE_20180201_TODATE.TXT"),
                                 sep="|", decimal=".", parse_dates=[1, 2], infer_datetime_format=True)

    df_speedbucket = df_speedbucket[df_speedbucket["Codigo_SpeedBucket"]==df_speedbucket["Codigo_SpeedBucket"]]

    df_bonos = pd.read_csv \
        (os.path.join(USE_CASES_PATH, "Prepaid_Segmentation", "resources", "BONOS_PRE_20180201_TODATE.TXT"),
                           sep="|", decimal=".", parse_dates=[1, 2, 8], infer_datetime_format=True)


    df_bonos_pyspark = spark.createDataFrame(df_bonos)


    df_bonos_pyspark = (df_bonos_pyspark.withColumn("Fecha_Inicio", (col("Fecha_Inicio") / 1000000000).cast(TimestampType()))
                                        .withColumn("Fecha_Fin", (col("Fecha_Fin") / 1000000000).cast(TimestampType()))
                                        .withColumn("Fecha_Deactivation", (col("Fecha_Deactivation") / 1000000000).cast(TimestampType()))
                       )




    df_bonos_pyspark = (df_bonos_pyspark.withColumn("Fx_Inicio", (col("Fx_Inicio") / 1000000000).cast(TimestampType()))
                                        .withColumn("Fx_Fin", (col("Fx_Fin") / 1000000000).cast(TimestampType()))
                       )

    (df_bonos_pyspark.where(col('MSISDN').isNotNull())
                                 .write
                                 .format('parquet')
                                 .mode('overwrite')
                                 .saveAsTable(table_bonos))

    print("Created table {}".format(table_bonos))


    df_speed_pyspark = spark.createDataFrame(df_speedbucket)

    df_speed_pyspark = (df_speed_pyspark.withColumn("Fx_Inicio", (col("Fx_Inicio") / 1000000000).cast(TimestampType()))
                        .withColumn("Fx_Fin", (col("Fx_Fin") / 1000000000).cast(TimestampType()))
                        )

    (df_speed_pyspark.where(col('MSISDN').isNotNull())
     .write
     .format('parquet')
     .mode('overwrite')
     .saveAsTable(table_speed_bucket))

    print("Created table {}".format(table_speed_bucket))
