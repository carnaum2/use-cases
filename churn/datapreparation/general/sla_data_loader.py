
from pyspark.sql.functions import greatest, lower, upper, trim, collect_set, col, lpad, lit, collect_list, desc, asc, sum as sql_sum, datediff, count as sql_count, min as sql_min,\
    array, regexp_extract, datediff, to_date, from_unixtime, unix_timestamp, desc, when, col, lit, udf, size, \
    array, isnan, upper, coalesce, length, lower, concat, create_map, sum as sql_sum, greatest, max as sql_max, \
    sort_array, lag, mean as sql_avg, datediff, row_number
from pyspark.sql.window import Window
import re
from pyspark.sql.types import ArrayType, FloatType, StringType
import numpy as np


class UDFconversion:

    @staticmethod
    def convert(s_orig):
        import re
        if not s_orig: return ""
        s = s_orig
        s = re.sub("instalaci.n", "instalacion", s)
        s = re.sub("m.vil", "movil", s)
        s = re.sub("desconexi.n", "desconexion", s)
        s = re.sub("disminuci.n", "disminucion", s)
        s = re.sub("reconexi.n", "reconexion", s)
        s = re.sub("suspensi.n", "suspension", s)
        s = re.sub("reconexi.n", "reconexion", s)
        s = re.sub("aplicaci.n", "aplicacion", s)
        s = re.sub("numeraci.n", "numeracion", s)
        s = re.sub("tel.fono", "telefono", s)
        s = re.sub("b.sico", "basico", s)
        s = re.sub("telefon.a", "telefonia", s)
        s = re.sub("migraci.n", "migracion", s)
        s = re.sub("facturaci.n", "facturacion", s)
        s = re.sub("correcci.n", "correccion", s)
        s = re.sub("aver.a", "averia", s)
        s = re.sub("activaci.n", "activacion", s)
        s = re.sub("garant.a", "garantia", s)
        s = re.sub("modificaci.n", "modificacion", s)
        s = re.sub("n.mero", "numero", s)
        s = re.sub("fidelizaci.n", "fidelizacion", s)
        s = re.sub("t.cnica", "tecnica", s)
        s = re.sub("gesti.n", "gestion", s)
        s = re.sub("reclamaci.n", "reclamacion", s)
        s = re.sub("planificaci.n", "planificacion", s)
        s = re.sub("configuraci.n", "configuracion", s)
        s = re.sub("reprovisi.n", "reprovision", s)
        # s = re.sub("", "", s)
        # s = re.sub("", "", s)
        return s


def get_sla_master_df(spark):


    convert_udf = udf(UDFconversion.convert, StringType())

    df_sla = spark.read.option("header", True).option("delimiter", ";").option("encoding", "utf-8").csv \
        ("/data/udf/vf_es/churn/SLA/TorreControl_SLA.csv").where \
        (col("TITULO").isNotNull( ) &col("CLASIFICACION").isNotNull( ) &col("TIPO ORDEN").isNotNull())
    df_sla = df_sla.withColumn('TITULO', trim(lower(col('TITULO'))))
    df_sla = df_sla.withColumn('CLASIFICACION', trim(lower(col('CLASIFICACION'))))
    df_sla = df_sla.withColumn('TIPO ORDEN', trim(upper(col('TIPO ORDEN'))))
    df_sla = df_sla.withColumn("TITULO", convert_udf(col("TITULO")))
    df_sla = df_sla.withColumn("SLA Negocio presencial", when(col("SLA Negocio presencial").isNull() ,-1).otherwise(col("SLA Negocio presencial")))
    df_sla = df_sla.withColumn("SLA Negocio no presencial", when(col("SLA Negocio no presencial").isNull() ,-1).otherwise
        (col("SLA Negocio no presencial")))

    df_sla = df_sla.withColumn("SLA_master",  when( col("SLA Negocio presencial").isNotNull() & col("SLA Negocio no presencial").isNotNull(), greatest(col("SLA Negocio presencial"), col("SLA Negocio no presencial")))
                               .when( col("SLA Negocio presencial").isNotNull(), col("SLA Negocio presencial"))
                               .when( col("SLA Negocio no presencial").isNotNull(), col("SLA Negocio no presencial")).otherwise(-1))
    return df_sla


def get_orders_df(spark, closing_day, exclude_clasif_list=None):

    convert_udf = udf(UDFconversion.convert, StringType())

    from pykhaos.utils.date_functions import move_date_n_days
    starting_day = move_date_n_days(closing_day, n=-365)

    data_order_ori = (spark.read.load("/data/raw/vf_es/customerprofilecar/ORDERCRMOW/1.1/parquet/")
                      .where(col('fecha_entrada') <= closing_day)
                      .where(col('fecha_entrada') >= starting_day)
                      )

    w_orderclass = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

    data_orderclass_ori = (spark.read.load("/data/raw/vf_es/customerprofilecar/ORDERCLASOW/1.0/parquet/")
                           .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
                           .withColumn("rowNum", row_number().over(w_orderclass))
                           .where(col('rowNum') == 1)
                           .drop(*['year', 'month', 'day'])
                           )

    w_order = Window().partitionBy("orden_id").orderBy(desc("year"), desc("month"), desc("day"))

    data_orders_joined = (
        data_order_ori.join(data_orderclass_ori, on=col('clase_orden') == col('objid'), how='leftouter')
            .withColumn("rowNumorder", row_number().over(w_order))
            .where(col('rowNumorder') == 1)
    )

    data_orders_joined = data_orders_joined.cache()
    #print("before join df_customer={}".format(data_orders_joined.count()))


    #######
    # Join with car to obtain NIF
    from churn.datapreparation.general.customer_base_utils import get_customers
    df_customer = get_customers(spark, closing_day).select("num_cliente", "NIF_CLIENTE")
    #
    #
    # df_customer = spark.read.load(
    #     "/data/udf/vf_es/amdocs_ids/customer/year={}/month={}/day={}".format(int(closing_day[:4]),
    #                                                                          int(closing_day[4:6]),
    #                                                                          int(closing_day[6:])))
    # keep only the num_cliente in customer table
    data_orders_joined = data_orders_joined.join(df_customer.select("num_cliente", "NIF_CLIENTE"), on=["num_cliente"], how="inner")

    data_orders_joined = data_orders_joined.cache()
    #print("after data_orders_joined={}".format(data_orders_joined.count()))

    data_orders_joined = data_orders_joined.withColumn('X_TIPO_ORDEN', trim(upper(col('X_TIPO_ORDEN'))))
    data_orders_joined = data_orders_joined.withColumn('x_clasificacion', trim(lower(col('x_clasificacion'))))
    data_orders_joined = data_orders_joined.withColumn('X_Description', trim(lower(col('X_Description'))))
    data_orders_joined = data_orders_joined.withColumn("X_Description", convert_udf(col("X_Description")))
    data_orders_joined = (data_orders_joined.withColumn("days_since_start", when(col("FECHA_ENTRADA").isNotNull(),
                                                                                 datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col("FECHA_ENTRADA"))).otherwise(-1)))

    data_orders_joined = (data_orders_joined.withColumn("days_since_completed", when(
        col("FECHA_WO_COMPLETA").isNotNull() & (col("ESTADO_FIN_WO") == "C"),  # be sure is completed
        datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col("FECHA_WO_COMPLETA"))).otherwise(None))
                          # set to None, orders completed after closing_day
                          .withColumn("days_since_completed", when(col("days_since_completed") < 0, None).otherwise(col("days_since_completed")))
                          )

    # -----[----------------]-------
    #     a year        closing_day
    data_orders_joined = (data_orders_joined.where((col("days_since_start") <= 365) & (col("days_since_start") >= 0)))

    data_orders_joined = (data_orders_joined
                          .withColumn("duration",
                                      when((col("days_since_start") != -1) & (col("days_since_completed").isNotNull()),
                                           (col("days_since_start") - col("days_since_completed"))).otherwise(None))
                          )

    data_orders_joined = data_orders_joined.withColumn("x_clasificacion", trim(col("x_clasificacion")))
    data_orders_joined = data_orders_joined.withColumn("x_clasificacion", when(col("x_clasificacion").rlike("ord.*administrativas"), "ord_admin").otherwise(col("x_clasificacion")))
    data_orders_joined = data_orders_joined.withColumn("x_clasificacion", when(col("x_clasificacion").rlike("ord.*equipo"), "ord_equipo").otherwise(col("x_clasificacion")))
    data_orders_joined = data_orders_joined.withColumn("x_clasificacion", when(col("x_clasificacion").rlike("porta.*hz"), "porta_hz").otherwise(col("x_clasificacion")))
    data_orders_joined = data_orders_joined.withColumn("x_clasificacion", when(col("x_clasificacion").rlike("ordenes.*especiales"), "ord_esp").otherwise(col("x_clasificacion")))

    # FILTERING ENTRIES
    # remove orders with category "desconexion"
    data_orders_joined = data_orders_joined.where(~col("x_clasificacion").rlike("(?i)desconexion"))
    if exclude_clasif_list:
        for clasif in exclude_clasif_list:
            print(clasif)
            data_orders_joined = data_orders_joined.where(~col("x_clasificacion").rlike("(?i){}".format(clasif.lower())))
            print("Excluded x_clasification {}".format(clasif.lower()))

    # within the category "disminucion", remove those that contain "portabilidad" y "baja" inside the X_Description field
    data_orders_joined = data_orders_joined.where(~((col("x_clasificacion").rlike("(?i)disminucion")) &
                                                    (col("X_Description").rlike("(?i)portabilidad|baja|desconexion"))))

    data_orders_joined = data_orders_joined.withColumn("traslado", when(col("X_Description").rlike("(?i)traslado"), 1).otherwise(0))

    return data_orders_joined



def do_merge(spark, closing_day, exclude_clasif_list, days_range, deadlines_range):

    data_orders_joined = get_orders_df(spark, closing_day, exclude_clasif_list)
    df_sla = get_sla_master_df(spark)

    #deadlines_range = [5, 10, 15, 20, 25]

    df_order_master = data_orders_joined.join(df_sla.select("CLASIFICACION", 'TIPO ORDEN', "TITULO", "SLA_master"),
                                              on=(df_sla['TIPO ORDEN'] == data_orders_joined['X_TIPO_ORDEN']) &
                                                 (df_sla["CLASIFICACION"] == data_orders_joined["x_clasificacion"]) &
                                                 (df_sla["TITULO"] == data_orders_joined['X_Description']), how="left")

    # keep_gt_factor_udf = udf(lambda milista, factor: list([milista[ii] for ii in range(len(milista)) if milista[ii] > factor]), ArrayType(StringType()))
    #days_range = [30, 60, 90, 120, 180, 240, 365]
    # remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] != -1]), ArrayType(FloatType()))
    # avg_days_bw_orders_udf = udf(lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())

    # mean_udf = udf(lambda milista: float(np.mean(milista)), FloatType())

    df_order = df_order_master.cache()

    df_order = df_order.withColumn('running_days', when(col('days_since_completed').isNotNull(),
                                                                      col('duration')).otherwise(
        col('days_since_start')))

    df_order = (df_order.withColumn("SLA_factor_running",
                                                  when(col("running_days").isNotNull() & col("SLA_master").isNotNull(),
                                                       col("running_days") / col("SLA_master")).otherwise(None)))

    for dd in days_range:
        df_order = df_order.withColumn("duration_last{}".format(dd),
                                       when((col("days_since_completed") <= dd) & (col("days_since_completed") != -1),
                                            col("duration")).otherwise(None))

        df_order = df_order.withColumn("SLA_factor_last{}".format(dd),
                                       when(col("duration_last{}".format(dd)).isNotNull() & col(
                                           "SLA_master").isNotNull(),
                                            col("duration_last{}".format(dd)) / col("SLA_master")).otherwise(None))
        # to ignore an order, set the column "days_since..." to -1. following line, set "diff" value of days_since_started=-1 to Null
        # diff is the difference (in days) between an order and the next one
        df_order = df_order.withColumn("diff",
                                       when(col("days_since_start") != -1, lag(col("days_since_start"), -1).over(
                                           Window.partitionBy("NIF_CLIENTE").orderBy(asc("days_since_start"))) - col(
                                           "days_since_start")).otherwise(None))

        df_order = df_order.withColumn("days_since_start_traslados",
                                       when(col("traslado") == 1, col("days_since_start")).otherwise(None))
        df_order = df_order.withColumn("diff_traslados",
                                       when(((col("traslado") == 1) & (col("days_since_start_traslados") != -1)),
                                            lag(col("days_since_start_traslados"), -1).over(
                                                Window.partitionBy("NIF_CLIENTE").orderBy(
                                                    asc("days_since_start_traslados"))) - col(
                                                "days_since_start_traslados")).otherwise(None))

        df_order = df_order.withColumn("Flag_open_order{}".format(dd),
                                       when((col('days_since_completed').isNull()) & (col('running_days') < dd), 1).otherwise(0))

        df_order = df_order.withColumn("Flag_open_order_insideSLA{}".format(dd),
                                        when((col("Flag_open_order{}".format(dd)) == 1) & (col('running_days') < (col('SLA_master'))),1).otherwise(0))

        df_order = df_order.withColumn("Flag_open_order_outsideSLA{}".format(dd),
                                        when((col("Flag_open_order{}".format(dd)) == 1) & (col('running_days') > (col('SLA_master'))),1).otherwise(0))

        for ss in deadlines_range:
            # ordenes abiertas en los ultimos dd dias, que llevan abiertas mas de 'ss' dias
            df_order = df_order.withColumn("flag_last{}_gt{}".format(dd, ss), when(((col("days_since_start") <= dd) & (col("days_since_completed").isNull()) & (col("days_since_start") > ss)),1).otherwise(0))
            # ordenes abiertas en los ultimos dd dias, que llevan abiertas menos/o 'ss' dias
            df_order = df_order.withColumn("flag_last{}_lte{}".format(dd, ss), when(((col("days_since_start") <= dd) & (col("days_since_completed").isNull()) & (col("days_since_start") <= ss)),1).otherwise(0))

    return df_order


def get_orders_module(spark, closing_day, exclude_clasif_list=None, days_range=None, deadlines_range=None):

    print("get_orders_module")

    print(days_range)
    if not days_range:
        days_range = [30, 60, 90, 120, 180, 240, 365]
    if not deadlines_range:
        deadlines_range = [5, 10, 15, 20, 25]

    print("Using days_range {}".format(",".join(map(str, days_range))))
    print("Using deadlines_range {}".format(",".join(map(str, deadlines_range))))

    df_order = do_merge(spark, closing_day, exclude_clasif_list, days_range, deadlines_range)

    factor_list = [1, 2, 3]

    df_agg = (df_order.groupby("NIF_CLIENTE").agg(*([sql_count(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("nb_started_orders_last{}".format(dd)) for dd in days_range] +
                                                    [sql_sum(col("flag_last{}_gt{}".format(dd, ss))).alias("nb_running_last{}_gt{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                                    [sql_count(when(col("duration_last{}".format(dd)).isNotNull(), col("duration_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_last{}".format(dd)) for dd in days_range] +
                                                    [sql_count(when(col("SLA_factor_last{}".format(dd)) > ff, col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_last{}_{}SLA".format(dd, ff)) for dd in days_range for ff in factor_list] +
                                                    [sql_avg(col("diff")).alias("avg_days_bw_open_orders")] +
                                                    [sql_max(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("first_order_last{}".format(dd)) for dd in days_range] +
                                                    [sql_min(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("last_order_last{}".format(dd)) for dd in days_range] +
                                                    # ignore null factors (when SLA_master is null)
                                                    [sql_avg(col("SLA_factor_last{}".format(dd))).alias("mean_sla_factor_last{}".format(dd)) for dd in days_range] +
                                                    # traslados
                                                    [sql_count(when((  (col("days_since_start_traslados").isNotNull()) & (col("days_since_start_traslados") < dd) ), col("days_since_start_traslados")).otherwise(None)).alias("nb_started_orders_traslados_last{}".format(dd)) for dd in days_range] +
                                                    [sql_count(when( ((col("traslado") == 1) & col("duration_last{}".format(dd)).isNotNull()), col("duration_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_traslados_last{}".format(dd)) for dd in days_range] +
                                                    [sql_count(when( ((col("traslado") == 1) & (col("SLA_factor_last{}".format(dd)) > ff)), col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_traslados_last{}_{}SLA".format(dd, ff)) for dd in days_range for ff in factor_list] +
                                                    [sql_avg(col("diff_traslados")).alias("avg_days_bw_open_orders_traslado")] +
                                                    [sql_avg(when( col("traslado") == 1,  col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("mean_sla_factor_traslados_last{}".format(dd)) for dd in days_range]
                                                     )))


       

    df_orders_bytype = (df_order
                        .groupby('nif_cliente')
                        .pivot('x_clasificacion')
                        .agg(*( [sql_count(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("orders_last{}".format(dd)) for dd in days_range] +
                                [sql_max(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("first_order_last{}".format(dd)) for dd in days_range] +
                                [sql_min(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("last_order_last{}".format(dd)) for dd in days_range] +
                                [sql_sum(col("flag_last{}_gt{}".format(dd, ss))).alias("nb_running_last{}_gt{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                [sql_sum(col("flag_last{}_lte{}".format(dd, ss))).alias("nb_running_last{}_lte{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                [sql_sum(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1, 1).otherwise(None)).alias('NOrders_Opened{}_InsideSLA'.format(dd)) for dd in days_range] +
                                [sql_sum(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1, 1).otherwise(None)).alias('NOrders_Opened{}_OutsideSLA'.format(dd)) for dd in days_range] +
                                [sql_avg(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('Avg_runningDays{}_OutsideSLA'.format(dd)) for dd in days_range] +
                                [sql_avg(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('Avg_runningDays{}_InsideSLA'.format(dd)) for dd in days_range] +
                                [sql_max(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('Max_runningDays{}_OutsideSLA'.format(dd)) for dd in days_range] +
                                [sql_max(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('Max_runningDays{}_InsideSLA'.format(dd)) for dd in days_range] +
                                [sql_max(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('SLA_factor_running')).otherwise(None)).alias('Max_SLA_factor_running{}_OutsideSLA'.format(dd)) for dd in days_range] +
                                [sql_max(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('SLA_factor_running')).otherwise(None)).alias('Max_SLA_factor_running{}_InsideSLA'.format(dd)) for dd in days_range]
                                )))


    df_agg = df_agg.join(df_orders_bytype, on=["nif_cliente"], how="left")

    nb_orders_cols = [col_ for col_ in df_agg.columns if "orders" in col_]
    df_agg = df_agg.fillna(0, subset=nb_orders_cols)
    df_agg = df_agg.na.fill(-1)

    return df_agg


def save_module(df_agg, closing_day, path_to_save):
    df_agg = df_agg.withColumn("day", lit(int(closing_day[6:])))
    df_agg = df_agg.withColumn("month", lit(int(closing_day[4:6])))
    df_agg = df_agg.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving")
    (df_agg.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))
    print("Saved {} for closing_day {}".format(path_to_save, closing_day))


# spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 churn/datapreparation/general/orders_sla.py -c 20190521


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

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-e', '--exclude_clasif', metavar='blablabla>', type=str, required=False, default=None,
                        help='a value for x_clasificacion for exclude')



    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    exclude_clasif = args.exclude_clasif.split(" ")[0] if args.exclude_clasif else None

    if "," in closing_day:
        closing_day_list = closing_day.split(",")
    else:
        closing_day_list = [closing_day]

    if not exclude_clasif:
        exclude_clasif_list = None
    elif exclude_clasif and "," in exclude_clasif:
        exclude_clasif_list = exclude_clasif.split(",")
    else:
        exclude_clasif_list = [exclude_clasif]



    print(closing_day_list)
    print(exclude_clasif_list)


    if not exclude_clasif:
        path_to_save = "/data/attributes/vf_es/trigger_analysis/orders_sla/"
    else:
        path_to_save = "/data/attributes/vf_es/trigger_analysis/orders_sla_{}/".format("_".join(exclude_clasif_list))

    print("DIRECTORY FOR SAVING RESULTS '{}'".format(path_to_save))

    from churn.utils.general_functions import init_spark
    spark = init_spark("sla_data_loader_{}".format(closing_day))


    print(closing_day_list)
    print(exclude_clasif_list)
    print("DIRECTORY FOR SAVING RESULTS '{}'".format(path_to_save))

    #days_range = [30, 60, 90, 120, 180, 240, 365]

    import time

    start_time_total = time.time()

    for closing_day in closing_day_list:

        start_time_loop = time.time()

        df_agg = get_orders_module(spark, closing_day, exclude_clasif_list)

        save_module(df_agg, closing_day, path_to_save)


        print("Process duration = {}".format(   (time.time() - start_time_loop)/60.0))

    print("Process duration = {}".format(   (time.time() - start_time_total)/60.0))
