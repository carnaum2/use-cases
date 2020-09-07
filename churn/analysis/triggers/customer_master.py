
from pyspark.sql.functions import greatest, lower, upper, trim
from pyspark.sql.functions import collect_set, col, lit, collect_list, desc, asc, \
    count as sql_count, substring, from_unixtime, unix_timestamp, \
    desc, when, col, lit, udf, upper, lower, concat, max as sql_max, min as sql_min, least, row_number

from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, FloatType, StringType, MapType, IntegerType
from pyspark.sql.functions import datediff

PATH_LABELED = "/data/attributes/vf_es/trigger_analysis/customer_master/"
PATH_UNLABELED = "/data/attributes/vf_es/trigger_analysis/customer_master_unlabeled/"


# udf to extract the difference of rgus, given a column of MapType
#df.select(*(["rgus_list", 'rgus_list_cycles_2', "diff_rgus"] +
#            [get_rgu_diff_udf(col("diff_rgus"), lit(rgu)).alias("{}_diff".format(rgu)) for rgu in ["fbb", "mobile"]])).show(truncate=False)
#get_rgu_diff_udf = udf(lambda x, rgu: x[rgu] if rgu in x else 0)


class UDFclass:

    @staticmethod
    def compute_rgus_diff(current_list, past_list):
        '''
            current_list: list of rgus
            past_list: past list of rgus

            if difference is negative, it means the service was disconnected
            if difference is positive, it means a new service was contracted
            if difference is zero, it means the service remains

            e.g. current_list  = ["fixed", "fbb"]
                 past_list = ["fixed", "fbb", "mobile"]
                 returns {'fbb': 0, 'fixed': 0, 'mobile': -1}

        '''

        import collections

        rgus_dict = collections.Counter(current_list) if current_list is not None else {}
        rgus_cycles_2_dict = collections.Counter(past_list) if past_list is not None else {}

        diff_dict = {}

        for k in set(rgus_dict.keys() + rgus_cycles_2_dict.keys()):

            if k in rgus_dict and k in rgus_cycles_2_dict:
                 diff_dict[k] =  rgus_cycles_2_dict[k] - rgus_dict[k]
            elif k in rgus_dict:
                 diff_dict[k] =  rgus_dict[k]
            elif k in rgus_cycles_2_dict:
                 diff_dict[k] = - rgus_cycles_2_dict[k]

        return diff_dict



def get_customer_saving_path(unlabeled):
    path_to_save = PATH_LABELED if not unlabeled else PATH_UNLABELED
    return path_to_save



def get_tgs(spark, closing_day):

    df_tgs = spark.read.load("/data/udf/vf_es/churn/extra_feats_mod/tgs/year={}/month={}/day={}".format(int(closing_day[:4]),
                                                                             int(closing_day[4:6]),
                                                                             int(closing_day[6:])))

    df_tgs = df_tgs.sort("nif_cliente", desc("tgs_days_until_fecha_fin_dto")).drop_duplicates(["nif_cliente"])
    return df_tgs



def getFbbDxsForCycleList_anyday(spark, closing_day, closing_day_target):

    import time

    current_base = (get_segment_msisdn_anyday(spark, closing_day)
                # .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))
                 .filter(col('rgu')=='fbb')
                 .select("msisdn")
                 .distinct())

    target_base = (get_segment_msisdn_anyday(spark, closing_day_target)
                 #   .filter((col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "09")) & (col("rgu").isNotNull()))
                    .filter(col('rgu')=='fbb')
                    .select("msisdn")
                    .distinct())

    from pykhaos.utils.date_functions import get_diff_days, move_date_n_days
    # set desconexion date to be in the middle point of both dates
    date_dx = move_date_n_days(closing_day, n=int(get_diff_days(closing_day, closing_day_target, format_date="%Y%m%d")/2))
    print("[getFbbDxsForCycleList_anyday] setting desconexion date to {}".format(date_dx))

    churn_base = current_base\
    .join(target_base.withColumn("tmp", lit(1)), "msisdn", "left")\
    .filter(col("tmp").isNull())\
    .select("msisdn").withColumn("label_dx", lit(1.0)).withColumn("date_dx", lit(date_dx))\
    .distinct()

    print("[getFbbDxsForCycleList_anyday] " + time.ctime() + " DXs for FBB services during the period: " + closing_day + "-"+closing_day_target+": " + str(churn_base.count()))

    return churn_base


def get_target(spark, closing_day):


    from pykhaos.utils.date_functions import move_date_n_cycles,move_date_n_days

    start_port = closing_day
    end_port = move_date_n_cycles(closing_day, n=4)

    import datetime as dt
    today_str = dt.datetime.today().strftime("%Y%m%d")
    closing_day_2 = end_port

    while closing_day_2 > move_date_n_days(today_str, n=-7):
        print("closing_day_2={} > {}".format(closing_day_2, move_date_n_days(today_str, n=-7)))
        closing_day_2 = move_date_n_cycles(closing_day_2, n=-1)

    print("Comparison of bases will be done with {} and {}".format(closing_day, closing_day_2))

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import getFixPortRequestsForCycleList
    #- Solicitudes de baja de fijo
    df_sopo_fix = (getFixPortRequestsForCycleList(spark, closing_day, end_port)\
                   .withColumn("date_srv", from_unixtime(unix_timestamp(col("FECHA_INSERCION_SGP")), "yyyyMMdd")))

    #- Porque dejen de estar en la lista de clientes
    df_baja_fix = getFbbDxsForCycleList_anyday(spark,closing_day, closing_day_2)

    # mobile portout
    window_mobile = Window.partitionBy("msisdn_a").orderBy(desc("days_from_portout"))  # keep the 1st portout

    from churn.utils.udf_manager import Funct_to_UDF

    start_date_obj = Funct_to_UDF.convert_to_date(start_port)
    end_date_obj = Funct_to_UDF.convert_to_date(end_port)

    convert_to_date_udf = udf(Funct_to_UDF.convert_to_date, StringType())

    from churn.utils.constants import PORT_TABLE_NAME

    df_sol_port = (spark.read.table(PORT_TABLE_NAME)
                   .where((col("sopo_ds_fecha_solicitud") >= start_date_obj) & (col("sopo_ds_fecha_solicitud") <= end_date_obj))
                   .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_a")
                   .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
                   .withColumn("portout_date", substring(col("portout_date"), 0, 10))
                   .withColumn("portout_date", convert_to_date_udf(col("portout_date")))
                   .withColumn("ref_date", convert_to_date_udf(concat(lit(closing_day[:4]), lit(closing_day[4:6]), lit(closing_day[6:]))))
                   .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
                   .withColumn("rank", row_number().over(window_mobile))
                   .where(col("rank") == 1))

    df_sol_port = df_sol_port.withColumn("label_mob", lit(1.0))\
                             .withColumnRenamed("msisdn_a", "msisdn")\
                             .withColumn("date_mob", from_unixtime(unix_timestamp(col("portout_date")), "yyyyMMdd")).select("msisdn", "label_mob", "date_mob")

    df_services = get_segment_msisdn_anyday(spark, anyday=closing_day)
    #     ['num_cliente_customer',
    #      'cod_estado_general',
    #      'clase_cli_cod_clase_cliente',
    #      'msisdn',
    #      'num_cliente_service',
    #      'campo2',
    #      'rgu',
    #      'srv_basic']

    # 1 if any of the services of this nif is 1
    window_nif = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn_d'], "left")
                      .na.fill({'label_srv': 0.0})
                      .join(df_baja_fix, ['msisdn'], "left")
                      .na.fill({'label_dx': 0.0})
                      .join(df_sol_port, ['msisdn'], "left")
                      .na.fill({'label_mob': 0.0})
                      .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0), 1.0).otherwise(0.0))
                      .withColumn('label', sql_max('tmp').over(window_nif))
                      .withColumn("tmp_date0", when(col("label") == 1.0, least(col("date_srv"), col("date_dx"), col("date_mob"))).otherwise(None))
                      .withColumn("churn_date", sql_min("tmp_date0").over(window_nif))
                      .drop("tmp", "tmp_date0"))

    # days_until_churn: days since closing_day to churn_event (first churn event)
    df_target_nifs = (df_target_nifs.withColumn("days_until_churn", when(col("churn_date").isNotNull(), datediff(from_unixtime(unix_timestamp(col("churn_date"), "yyyyMMdd")),
                                                                                                                from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")))).otherwise(-1)))

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", "churn_date", "days_until_churn").drop_duplicates()


    return df_target_nifs




def get_segment_msisdn_anyday(spark, anyday):

    from churn.datapreparation.general.customer_base_utils import  get_customer_base_segment

    df_base = get_customer_base_segment(spark, date_=anyday)
    # ['NIF_CLIENTE', 'NUM_CLIENTE', 'msisdn', 'rgu', 'seg_pospaid_nif']

    df_base = (df_base
                     .withColumnRenamed("seg_pospaid_nif", "segment_nif")
                     .withColumnRenamed("NIF_CLIENTE", "nif_cliente")
                   )


    return df_base


def get_segment_nif_anyday(spark, anyday):

    from churn.datapreparation.general.customer_base_utils import  get_customer_base_segment

    df_base = get_customer_base_segment(spark, date_=anyday)
    # ['NIF_CLIENTE', 'NUM_CLIENTE', 'msisdn', 'rgu', 'seg_pospaid_nif']

    df_base = (df_base.withColumnRenamed("seg_pospaid_nif", "segment_nif")
                     .withColumnRenamed("NIF_CLIENTE", "nif_cliente"))

    df_base_nif = df_base.groupBy("nif_cliente").agg(*([sql_count("*").alias("nb_rgus"), collect_list("rgu").alias("rgus_list")])).withColumn("nb_tv_services_nif", lit(-1))

    df_base = df_base.drop_duplicates(["nif_cliente"])

    df_base = df_base.join(df_base_nif, on=["nif_cliente"], how="left")

    return df_base



def get_customer_master_module(spark, closing_day, unlabeled=False, save=True):
    '''

    :param spark:
    :param closing_day:
    :param unlabeled: if True, label column is set to zero. Otherwise, it is computed
    :return:
    '''

    #df_cust_agg = get_segment(spark, closing_day)  # id=num_cliente, "nb_tv_services_nif", "segment_nif", "nb_rgus"

    df_base = get_segment_nif_anyday(spark, closing_day)  # id=num_cliente, "nb_tv_services_nif", "segment_nif", "nb_rgus"

    if unlabeled:
        print("Computing unlabeled car")
        df_target_nifs = df_base.select("nif_cliente").drop_duplicates()
        df_target_nifs = df_target_nifs.withColumn("label", lit(0))

    else:
        df_target_nifs = get_target(spark, closing_day)  # id=nif_cliente,num_cliente "label" (aggregada por nif)

    from pykhaos.utils.date_functions import move_date_n_cycles, is_cycle, get_previous_cycle
    closing_day_n2 = move_date_n_cycles(closing_day, n=-2)
    df_cust_agg_n2 = (get_segment_nif_anyday(spark, closing_day_n2)
                        .withColumnRenamed("nb_rgus", "nb_rgus_cycles_2")
                        .withColumnRenamed("rgus_list", "rgus_list_cycles_2")
                       .select("nif_cliente", "nb_rgus_cycles_2", "rgus_list_cycles_2"))


    #use closing_day if it is a cycle. Otherwise, get the previous one
    closing_day_tgs = closing_day if is_cycle(closing_day) else get_previous_cycle(closing_day)

    df_tgs = None
    for ii in range(0,3):
        try:
            df_tgs = get_tgs(spark, closing_day_tgs).select("nif_cliente", 'tgs_days_until_fecha_fin_dto', 'tgs_has_discount',
                                                    'tgs_target_accionamiento').drop_duplicates()
            break
        except Exception as e:
            print("Not found extra feats for tgs - {}".format(closing_day_tgs))
            print(e)
            print("trying with a previous cycle...")
            closing_day_tgs = move_date_n_cycles(closing_day_tgs, n=-1)

    if df_tgs is None:
        print("ERROR df_tgs could not be obtained. Please, review")
        sys.exit()
    # ----

    df_cust_agg = df_base.join(df_cust_agg_n2, on=["nif_cliente"], how="left")
    df_join1 = df_cust_agg.join(df_target_nifs, on=["nif_cliente"], how="left")
    df_join1 = df_join1.drop_duplicates()

    df_join2 = df_join1.join(df_tgs, on=["nif_cliente"], how="left")

    df_join2 = df_join2.fillna(-1, subset=["tgs_days_until_fecha_fin_dto", "tgs_has_discount"])
    df_join2 = df_join2.fillna("unknown", subset=["tgs_target_accionamiento", 'segment_nif'])
    df_join2 = df_join2.fillna(0, subset=["nb_rgus_cycles_2", "nb_rgus", "nb_tv_services_nif", "label"])
    df_join2 = df_join2.withColumn("diff_rgus_n_n2", col("nb_rgus") - col("nb_rgus_cycles_2"))

    # rgus_diff_udf = udf(lambda x, y: UDFclass.compute_rgus_diff(x, y), MapType(StringType(), IntegerType()))
    # df_join2 = df_join2.withColumn('diff_rgus_map', rgus_diff_udf(col('rgus_list'), col('rgus_list_cycles_2')))

    #df_cust_mast_AUX.select("rgus_list", 'rgus_list_cycles_2', 'diff_rgus').show(truncate=False)

    if save:
        path_to_save = get_customer_saving_path(unlabeled)
        save_customer_module(df_join2, closing_day, path_to_save)

    return df_join2


def save_customer_module(df_agg, closing_day, path_to_save):
    df_agg = df_agg.withColumn("day", lit(int(closing_day[6:])))
    df_agg = df_agg.withColumn("month", lit(int(closing_day[4:6])))
    df_agg = df_agg.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving customer module for closing_day={}".format(closing_day))
    (df_agg.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved {} for closing_day {}".format(path_to_save, closing_day))



# spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 churn/analysis/triggers/customer_master.py -c 20190621


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

    mypath = os.path.join(root_dir, "amdocs_inf_dataset") # NEW AMDOCS IDS
    if mypath not in sys.path:
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-u', '--unlabeled', action='store_true', help='generate unlabeled customer master')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    unlabeled = args.unlabeled

    if "," in closing_day:
        closing_day_list = closing_day.split(",")
    else:
        closing_day_list = [closing_day]


    print(closing_day_list)
    print("unlabeled", unlabeled)


    from churn.utils.general_functions import init_spark
    spark = init_spark("customer_master_mini_ids")



    path_to_save = PATH_LABELED if not unlabeled else PATH_UNLABELED


    print(path_to_save)

    for closing_day in closing_day_list:

        df_agg = get_customer_master_module(spark, closing_day, unlabeled, save=True)


