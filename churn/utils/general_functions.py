
import os
import re
import time
from pykhaos.utils.hdfs_functions import create_directory
from pyspark.sql.functions import col
import datetime as dt
import pykhaos.utils.pyspark_configuration as pyspark_config
from churn.utils.constants import APP_NAME, PROJECT_NAME
from pykhaos.utils.hdfs_functions import save_df_to_hdfs, create_directory, save_df_to_hdfs_csv

# Copied to use-cases/churn_nrt/src/utils/spark_session.py
def init_spark(app_name, log_level="OFF"):
    start_time = time.time()
    sc, spark, sql_context = pyspark_config.get_spark_session(app_name=app_name, log_level=log_level)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))

    return spark



def save_df(df, path_filename, csv=True, create_dir=True):

    if create_dir:
        import os
        dir_name, file_name = os.path.split(path_filename)
        ret_stat = create_directory(os.path.join(dir_name))
        print("Created directory returned {}".format(ret_stat))
    print("Started to save...")
    if csv:
        save_df_to_hdfs_csv(df, path_filename)
    else:
        save_df_to_hdfs(df, path_filename)
    print("Saved df successfully - '{}'".format(path_filename))


def amdocs_table_reader(spark, table_name, closing_day, new=True, verbose=False):
    if verbose:
        print("amdocs_table_reader", table_name, closing_day, new)
    path_car = "amdocs_inf_dataset" if new else "amdocs_ids"
    table_name = '/data/udf/vf_es/{}/{}/year={}/month={}/day={}'.format(path_car, table_name, int(closing_day[:4]),
                                                                                                    int(closing_day[4:6]),
                                                                                                    int(closing_day[6:]))
    if verbose:
        print("Loading {}".format(table_name))
    df_src = spark.read.load(table_name)
    return df_src


def get_nif_msisdn(spark, closing_day, new=True):
    '''
    This functions is intended to be use to obtain a df with nif|num_cliente|msisdn_a|msisdn_a
    :param spark:
    :param closing_day:
    :return:
    '''
    df_customer = (amdocs_table_reader(spark, "customer", closing_day, new)
                   .where(col("clase_cli_cod_clase_cliente") == "RS")  # customer
                   .where(col("cod_estado_general").isin(["01", "09"]))  # customer
                   .select("num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente", "NIF_CLIENTE")
                   .withColumnRenamed("num_cliente", "num_cliente_customer"))

    df_service = (amdocs_table_reader(spark, "service", closing_day, new)
                  .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
                  .where(col("rgu").isNotNull())
                  .select("msisdn", "num_cliente", "campo2", "rgu", "srv_basic")
                  .withColumnRenamed("num_cliente", "num_cliente_service")
                  .withColumnRenamed("campo2", "msisdn_d")
                  .withColumnRenamed("msisdn", "msisdn_a")
                  )

    df_services = df_customer.join(df_service,
                                   on=(df_customer["num_cliente_customer"] == df_service["num_cliente_service"]),
                                   how="inner")  # intersection
    df_services = df_services.withColumnRenamed("num_cliente_customer", "num_cliente")

    return df_services.select("msisdn_a", "msisdn_d", "NIF_CLIENTE", "num_cliente")


