
'''
def set_paths():
    import os
    import sys

    USECASES_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios", "use-cases")
    if USECASES_SRC not in sys.path:
        sys.path.append(USECASES_SRC)
'''

def set_paths():
    import os, re, sys

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/PEGA(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


if __name__ == "__main__":

    set_paths()

    from churn_nrt.src.utils.spark_session import get_spark_session
    from churn_nrt.src.utils.date_functions import get_next_dow

    from pyspark.sql.functions import (lit, col, lpad, max as sql_max,
                                       col, lit, concat, from_unixtime,
                                       unix_timestamp, regexp_replace, split)
    import datetime


    sc, spark, sql_context = get_spark_session(app_name="blindaje_variables_PEGA")


    print('[Info]: Get most recent blindaje data (we usually have data for the day before the execution date')

    path_blindaje = '/data/raw/vf_es/cvm/EXT_AC_CLIENTES/1.0/parquet/'

    def get_last_date(spark):
        last_date = (spark.read.load(path_blindaje + "year=2020/").withColumn("year", lit(2020))
            .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
            .select(sql_max(col('mydate')).alias('last_date'))
            .rdd.first()['last_date'])

        return last_date

    closing_day = get_last_date(spark)

    print("[Info] Data from closing_day={}".format(closing_day))

    print('[Info]: Loading blindaje data...')

    year = closing_day[0:4]
    month = str(int(closing_day[4:6]))
    day = str(int(closing_day[6:8]))

    tabla_blindaje = spark.read.load(
        '/data/raw/vf_es/cvm/EXT_AC_CLIENTES/1.0/parquet/year=' + year + '/month=' + month + '/day=' + day)

    print('[Info]: Select variables and prepare data...')

    var_blindaje = tabla_blindaje.select('NIF_CLIENTE',
                                         'AGRUPMAX_CODIGO',
                                         'AGRUPMAX_F_INICIO',
                                         'AGRUPMAX_F_FIN',
                                         'AGRUPMAX_NMESES',
                                         'FLAG_BLINDAJE',
                                         'FLAG_FINANCIACION',
                                         'FINAN_F_FIN')


    var_blindaje = var_blindaje.withColumn("FINAN_F_FIN",
                                           from_unixtime(unix_timestamp(var_blindaje.FINAN_F_FIN), "yyyy-MM-dd")) \
        .withColumn("AGRUPMAX_F_INICIO", from_unixtime(unix_timestamp(var_blindaje.AGRUPMAX_F_INICIO), "yyyy-MM-dd")) \
        .withColumn("AGRUPMAX_F_FIN", from_unixtime(unix_timestamp(var_blindaje.AGRUPMAX_F_FIN), "yyyy-MM-dd"))

    var_blindaje = var_blindaje.fillna('null', subset=['FINAN_F_FIN'])

    var_blindaje = var_blindaje.withColumn('extra_info', concat(lit('AGRUPMAX_CODIGO='), col("AGRUPMAX_CODIGO"),
                                                                lit(";AGRUPMAX_F_INICIO="), col('AGRUPMAX_F_INICIO'),
                                                                lit(";AGRUPMAX_F_FIN="), col('AGRUPMAX_F_FIN'),
                                                                lit(";AGRUPMAX_NMESES="), col('AGRUPMAX_NMESES'),
                                                                lit(";FLAG_BLINDAJE="), col('FLAG_BLINDAJE'),
                                                                lit(";FLAG_FINANCIACION="), col('FLAG_FINANCIACION'),
                                                                lit(";FINAN_F_FIN="), col('FINAN_F_FIN')))

    var_blindaje = var_blindaje.fillna({'extra_info': 'FLAG_BLINDAJE=0'})


    print('[Info]: Put variables in model_output format')

    model_output_cols = ["model_name",
                         "executed_at",
                         "model_executed_at",
                         "predict_closing_date",
                         "msisdn",
                         "client_id",
                         "nif",
                         "model_output",
                         "scoring",
                         "prediction",
                         "extra_info",
                         "year",
                         "month",
                         "day",
                         "time"]

    executed_at = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    partition_date = str(get_next_dow(5))  # get day of next friday
    partition_year = int(partition_date[0:4])
    partition_month = int(partition_date[5:7])
    partition_day = int(partition_date[8:10])

    df_model_scores = (var_blindaje
                       .withColumn("model_name", lit("var_blindaje").cast("string"))
                       .withColumn("executed_at",
                                   from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast(
                                       "string"))
                       .withColumn("model_executed_at", col("executed_at").cast("string"))
                       .withColumn("client_id", lit(""))
                       .withColumn("msisdn", lit(""))
                       .withColumn("nif", col("NIF_CLIENTE").cast("string"))
                       .withColumn("scoring", lit(""))
                       .withColumn("model_output", lit(""))
                       .withColumn("prediction", lit(""))
                       .withColumn("extra_info", col('extra_info').cast("string"))
                       .withColumn("predict_closing_date", lit(closing_day))
                       .withColumn("year", lit(partition_year).cast("integer"))
                       .withColumn("month", lit(partition_month).cast("integer"))
                       .withColumn("day", lit(partition_day).cast("integer"))
                       .withColumn("time",
                                   regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .select(*model_output_cols))


    print('[Info]: Saving data')

    df_model_scores \
                .write \
                .partitionBy('model_name', 'year', 'month', 'day') \
                .mode("append") \
                .format("parquet") \
                .save('/data/attributes/vf_es/model_outputs/model_scores/')


    print('[Info]: Dataframe saved in model output format')