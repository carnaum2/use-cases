
from pyspark.sql.functions import greatest, lower, upper, trim, collect_set, col, lpad, lit, collect_list, desc, asc, sum as sql_sum, datediff, count as sql_count, min as sql_min,\
    array, regexp_extract, datediff, to_date, from_unixtime, unix_timestamp, desc, when, col, lit, udf, size, \
    array, isnan, upper, coalesce, length, lower, concat, create_map, sum as sql_sum, greatest, max as sql_max, \
    sort_array, lag, mean as sql_avg, datediff, row_number, regexp_replace
from pyspark.sql.window import Window
import os
from pyspark.sql.types import ArrayType, FloatType, StringType
import numpy as np
from churn_nrt.src.utils.date_functions import move_date_n_days
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
import itertools

DAYS_RANGE = [7, 14, 30, 60, 90, 120, 180, 240, 365]
DEADLINES_RANGE = [5, 10, 15, 20, 25]
X_CLASIFICATION_LIST = ["instalacion", "desconexion", "reconexion", "migracion", "ord_esp", "cambio", "disminucion", "ord_admin", "aumento", "porta_hz", "ord_equipo", "devolucion"]
FACTOR_LIST = [1, 2, 3]

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





############################################################
#### AUXILIARY - only for internal use (within this class)
###########################################################

def __get_sla_master_df(spark):


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

    df_sla = df_sla.withColumn("flag_impact", when(col("LANZADAS")=="IMPACTA", lit(1)).otherwise(lit(0)))
    df_sla = df_sla.withColumn("flag_low_impact", when(col("LANZADAS")=="BAJO_IMPACTO", lit(1)).otherwise(lit(0)))
    df_sla = df_sla.withColumn("flag_unknown_impact", when( (col("flag_impact") + col("flag_low_impact"))==0, lit(1)).otherwise(lit(0)))
    return df_sla


def __get_orders_df(spark, closing_day, save_others, exclude_clasif_list=None, force_gen=False):

    convert_udf = udf(UDFconversion.convert, StringType())

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
    from churn_nrt.src.data.customers_data import Customer
    df_customer = Customer(spark).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select("num_cliente", "NIF_CLIENTE")

    # keep only the num_cliente in customer table
    data_orders_joined = data_orders_joined.join(df_customer.select("num_cliente", "NIF_CLIENTE"), on=["num_cliente"], how="inner")

    data_orders_joined = data_orders_joined.cache()

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

    data_orders_joined = data_orders_joined.where(col("x_clasificacion").isin(*X_CLASIFICATION_LIST))

    if exclude_clasif_list:
        for clasif in exclude_clasif_list:
            print(clasif)
            data_orders_joined = data_orders_joined.where(~col("x_clasificacion").rlike("(?i){}".format(clasif.lower())))
            print("Excluded x_clasification {}".format(clasif.lower()))


    # NEW VERSION OF THIS MODULE: dont filter orders about desconexion and disminucion, instead add a counter and a flag
    # mark orders with category "desconexion"
    data_orders_joined = data_orders_joined.withColumn("flag_desconexion", when(col("x_clasificacion").rlike("(?i)desconexion"),1).otherwise(0))

    # within the category "disminucion", remove those that contain "portabilidad" y "baja" inside the X_Description field
    data_orders_joined = data_orders_joined.withColumn("flag_disminucion", when( ((col("x_clasificacion").rlike("(?i)disminucion")) &
                                                         (col("X_Description").rlike("(?i)portabilidad|baja|desconexion"))),1).otherwise(0))

    data_orders_joined = data_orders_joined.withColumn("flag_traslado", when(col("X_Description").rlike("(?i)traslado"), 1).otherwise(0))

    # desconexion, disminucion and traslado orders are not considered in counts
    data_orders_joined = data_orders_joined.withColumn("order_to_consider", when( (col("flag_desconexion") + col("flag_disminucion") + col("flag_traslado")) == 0, 1).otherwise(0))

    return data_orders_joined



def _do_merge(spark, closing_day, save_others, exclude_clasif_list, days_range, deadlines_range, force_gen=False):

    data_orders_joined = __get_orders_df(spark, closing_day, save_others, exclude_clasif_list, force_gen)
    df_sla = __get_sla_master_df(spark)

    #deadlines_range = [5, 10, 15, 20, 25]

    df_order_master = data_orders_joined.join(df_sla.select("CLASIFICACION", 'TIPO ORDEN', "TITULO", "SLA_master",
                                                            "flag_impact", "flag_low_impact", "flag_unknown_impact"),
                                              on=(df_sla['TIPO ORDEN'] == data_orders_joined['X_TIPO_ORDEN']) &
                                                 (df_sla["CLASIFICACION"] == data_orders_joined["x_clasificacion"]) &
                                                 (df_sla["TITULO"] == data_orders_joined['X_Description']), how="left")

    # keep_gt_factor_udf = udf(lambda milista, factor: list([milista[ii] for ii in range(len(milista)) if milista[ii] > factor]), ArrayType(StringType()))
    #days_range = [30, 60, 90, 120, 180, 240, 365]
    # remove_undefined_udf = udf(lambda milista: list([milista[ii] for ii in range(len(milista)) if milista[ii] != -1]), ArrayType(FloatType()))
    # avg_days_bw_orders_udf = udf(lambda milista: float(np.mean([milista[ii + 1] - milista[ii] for ii in range(len(milista) - 1)])), FloatType())

    # mean_udf = udf(lambda milista: float(np.mean(milista)), FloatType())

    df_order = df_order_master.cache()

    df_order = df_order.withColumn('running_days', when(col('days_since_completed').isNotNull(), col('duration')).otherwise(
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
                                       when(col("flag_traslado") == 1, col("days_since_start")).otherwise(None))

        df_order = df_order.withColumn("diff_traslados",
                                       when(((col("flag_traslado") == 1) & (col("days_since_start_traslados") != -1)),
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



class OrdersSLA(DataTemplate):

    EXCLUDE_CLASIF_LIST = None

    def __init__(self, spark, exclude_clasif_list=None):
        self.EXCLUDE_CLASIF_LIST = exclude_clasif_list if exclude_clasif_list else []
        DataTemplate.__init__(self, spark, "orders_sla" if not self.EXCLUDE_CLASIF_LIST else "orders_sla" + "_{}".format("_".join(self.EXCLUDE_CLASIF_LIST)))

    def build_module(self, closing_day, save_others, force_gen=False, days_range=None, deadlines_range=None, factor_list=None, **kwargs):

        if not days_range:
            days_range = DAYS_RANGE
        if not deadlines_range:
            deadlines_range = DEADLINES_RANGE
        if not self.EXCLUDE_CLASIF_LIST:
            exclude_clasif_list = []
        if not factor_list:
            factor_list = FACTOR_LIST

        print("[OrdersSLA] build_module | Using days_range {}".format(",".join(map(str, days_range))))
        print("[OrdersSLA] build_module | Using deadlines_range {}".format(",".join(map(str, deadlines_range))))
        print("[OrdersSLA] build_module | Using exclude_clasif_list {}".format(",".join(exclude_clasif_list) if exclude_clasif_list else "None"))

        df_order_all = _do_merge(self.SPARK, closing_day, save_others, exclude_clasif_list, days_range, deadlines_range, force_gen=force_gen)

        from pyspark.sql.functions import regexp_replace
        df_order = df_order_all.where(col("order_to_consider")==1)
        df_order = df_order.withColumn('x_clasificacion', regexp_replace('x_clasificacion', ' ', '_'))# remove spaces with underscore to avoid error when writing df



        df_agg = (df_order.groupby("NIF_CLIENTE").agg(*([sql_count(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("nb_started_orders_last{}".format(dd)) for dd in days_range] +
                                                        [sql_sum(col("flag_last{}_gt{}".format(dd, ss))).alias("nb_running_last{}_gt{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                                        [sql_count(when(col("duration_last{}".format(dd)).isNotNull(), col("duration_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_last{}".format(dd)) for dd in days_range] +
                                                        [sql_avg(when(col("duration_last{}".format(dd)).isNotNull(), col("duration_last{}".format(dd))).otherwise(None)).alias("avg_days_completed_orders_last{}".format(dd)) for dd in days_range] +
                                                        [sql_count(when(col("SLA_factor_last{}".format(dd)) > ff, col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_last{}_{}SLA".format(dd, ff)) for dd in days_range for ff in factor_list] +
                                                        [sql_avg(col("diff")).alias("avg_days_bw_open_orders")] +
                                                        [sql_max(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("first_order_last{}".format(dd)) for dd in days_range] +
                                                        [sql_min(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("last_order_last{}".format(dd)) for dd in days_range] +
                                                        # ignore null factors (when SLA_master is null)
                                                        [sql_avg(col("SLA_factor_last{}".format(dd))).alias("mean_sla_factor_last{}".format(dd)) for dd in days_range] +
                                                        # computing the orders by taking into account the impact
                                                        [sql_sum(when(( (col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_impact")).otherwise(lit(0))).alias("nb_started_orders_last{}_impact".format(dd)) for dd in days_range] +
                                                        [sql_sum(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_unknown_impact")).otherwise(lit(0))).alias("nb_started_orders_last{}_unknown_impact".format(dd)) for dd in days_range] +
                                                        [sql_sum(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_low_impact")).otherwise(lit(0))).alias("nb_started_orders_last{}_low_impact".format(dd)) for dd in  days_range])))

        #df_agg.where(col("nb_started_orders_last30")==10).select("nb_started_orders_last30_unknown_impact", "nb_started_orders_last30_impact", "nb_started_orders_last30_low_impact", "nb_started_orders_last30").show(100, truncate=False)

        impact_cols = [col_ for col_ in df_agg.columns if "impact" in col_]
        df_agg = df_agg.fillna(0, subset=impact_cols)

        pivot_types = list(set(X_CLASIFICATION_LIST) - set(exclude_clasif_list))

        df_orders_bytype = (df_order.where(col("x_clasificacion").isNotNull())
                            .groupby('nif_cliente')
                            .pivot('x_clasificacion', values=pivot_types)
                            .agg(*( [sql_count(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("orders_last{}".format(dd)) for dd in days_range] +
                                    [sql_max(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("first_order_last{}".format(dd)) for dd in days_range] +
                                    [sql_min(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("last_order_last{}".format(dd)) for dd in days_range] +
                                    [sql_avg(when(col("duration_last{}".format(dd)).isNotNull(), col("duration_last{}".format(dd))).otherwise(None)).alias("avg_days_completed_orders_last{}".format(dd)) for dd in days_range] +
                                    [sql_count(when(col("duration_last{}".format(dd)).isNotNull(), col("duration_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_last{}".format(dd)) for dd in days_range] +
                                    [sql_sum(col("flag_last{}_gt{}".format(dd, ss))).alias("nb_running_last{}_gt{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                    [sql_sum(col("flag_last{}_lte{}".format(dd, ss))).alias("nb_running_last{}_lte{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                    [sql_sum(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1, 1).otherwise(None)).alias('nb_orders_opened{}_insideSLA'.format(dd)) for dd in days_range] +
                                    [sql_sum(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1, 1).otherwise(None)).alias('nb_orders_opened{}_outsideSLA'.format(dd)) for dd in days_range] +
                                    [sql_avg(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('avg_running_days{}_outsideSLA'.format(dd)) for dd in days_range] +
                                    [sql_avg(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('avg_running_days{}_insideSLA'.format(dd)) for dd in days_range] +
                                    [sql_max(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('max_running_days{}_outsideSLA'.format(dd)) for dd in days_range] +
                                    [sql_max(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('running_days')).otherwise(None)).alias('max_running_days{}_insideSLA'.format(dd)) for dd in days_range] +
                                    [sql_max(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1,col('SLA_factor_running')).otherwise(None)).alias('max_SLA_factor_running{}_outsideSLA'.format(dd)) for dd in days_range] +
                                    [sql_max(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1,col('SLA_factor_running')).otherwise(None)).alias('max_SLA_factor_running{}_insideSLA'.format(dd)) for dd in days_range] +

                                    [sql_sum(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1, col("flag_impact")).otherwise(0)).alias('nb_orders_opened{}_insideSLA_impact'.format(dd)) for dd in days_range] +
                                    [sql_sum(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1, col("flag_impact")).otherwise(0)).alias('nb_orders_opened{}_outsideSLA_impact'.format(dd)) for dd in days_range] +

                                    [sql_sum(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1, col("flag_unknown_impact")).otherwise(0)).alias('nb_orders_opened{}_insideSLA_unknown_impact'.format(dd)) for dd in days_range] +
                                    [sql_sum(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1, col("flag_unknown_impact")).otherwise(0)).alias('nb_orders_opened{}_outsideSLA_unknown_impact'.format(dd)) for dd in days_range] +

                                    [sql_sum(when(col('Flag_open_order_insideSLA{}'.format(dd)) == 1, col("flag_low_impact")).otherwise(0)).alias('nb_orders_opened{}_insideSLA_low_impact'.format(dd)) for dd in days_range] +
                                    [sql_sum(when(col('Flag_open_order_outsideSLA{}'.format(dd)) == 1, col("flag_low_impact")).otherwise(0)).alias('nb_orders_opened{}_outsideSLA_low_impact'.format(dd)) for dd in days_range]


                                    )
                                 )
                            )

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        df_order_excluded = df_order_all.where(col("order_to_consider")==0)
        df_agg_exc = (df_order_excluded.groupby("NIF_CLIENTE").agg(*(
            #[sql_count(when(((col("days_since_start_traslados").isNotNull()) & (col("days_since_start_traslados") < dd)), col("days_since_start_traslados")).otherwise(None)).alias("nb_started_orders_traslado_last{}".format(dd)) for dd in days_range] +
            #[sql_count(when(((col("flag_traslado") == 1) & col("duration_last{}".format(dd)).isNotNull()), col("duration_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_traslado_last{}".format(dd)) for dd in days_range] +
            [sql_count(when(((col("flag_traslado") == 1) & (col("SLA_factor_last{}".format(dd)) > ff)), col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("nb_completed_orders_traslado_last{}_{}SLA".format(dd, ff)) for dd in days_range for ff in factor_list] +
            [sql_avg(col("diff_traslados")).alias("avg_days_bw_open_orders_traslado")] +
            [sql_avg(when(col("flag_traslado") == 1, col("SLA_factor_last{}".format(dd))).otherwise(None)).alias("mean_sla_factor_traslado_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_desconexion")).otherwise(0)).alias("nb_started_orders_desconexion_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_disminucion")).otherwise(0)).alias("nb_started_orders_disminucion_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(((col("days_since_start") <= dd) & (col("days_since_start") != -1)), col("flag_traslado")).otherwise(0)).alias("nb_started_orders_traslado_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(col("duration_last{}".format(dd)).isNotNull(), col("flag_desconexion")).otherwise(None)).alias("nb_completed_orders_desconexion_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(col("duration_last{}".format(dd)).isNotNull(), col("flag_disminucion")).otherwise(None)).alias("nb_completed_orders_disminucion_last{}".format(dd)) for dd in days_range] +
            [sql_sum(when(col("duration_last{}".format(dd)).isNotNull(), col("flag_traslado")).otherwise(None)).alias("nb_completed_orders_traslado_last{}".format(dd)) for dd in days_range]
        )))

        for dd in days_range:
            df_agg_exc = df_agg_exc.withColumn("nb_forbidden_orders_last{}".format(dd),  (col("nb_started_orders_desconexion_last{}".format(dd)) +
                                                                                          col("nb_started_orders_disminucion_last{}".format(dd)) +
                                                                                          col("nb_started_orders_traslado_last{}".format(dd))))
            df_agg_exc = df_agg_exc.withColumn("has_forbidden_orders_last{}".format(dd), when( col("nb_forbidden_orders_last{}".format(dd))>0,1).otherwise(0))

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        df_agg = df_agg.join(df_orders_bytype, on=["nif_cliente"], how="left")
        df_agg = df_agg.join(df_agg_exc, on=["nif_cliente"], how="left")
        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_agg = apply_metadata(self.get_metadata(days_range, deadlines_range, factor_list), df_agg)

        return df_agg

    def get_metadata(self, days_range = None, deadlines_range = None, factor_list = None):

        ordinary_types = ["instalacion", "reconexion", "migracion", "ord_esp", "cambio", "disminucion", "ord_admin", "aumento", "porta_hz", "ord_equipo", "devolucion"]
        traslado_orders = ["traslado"]
        forbidden_types = ["disminucion", "desconexion"]
        impact_types = ["impact", "low_impact", "unknown_impact"]
        sla_types = ["inside", "outside"]

        if not days_range:
            days_range = DAYS_RANGE
        if not deadlines_range:
            deadlines_range = DEADLINES_RANGE
        if not self.EXCLUDE_CLASIF_LIST:
            exclude_clasif_list = []
        if not factor_list:
            factor_list = FACTOR_LIST

        ordinary_types = list(set(ordinary_types) - set(exclude_clasif_list))
        traslado_orders = list(set(traslado_orders) - set(exclude_clasif_list))
        forbidden_types = list(set(forbidden_types) - set(exclude_clasif_list))


        type1 = ['nb_started_orders_last{}', 'nb_completed_orders_last{}', 'first_order_last{}', 'mean_sla_factor_last{}', 'nb_started_orders_last{}_impact', 'nb_started_orders_last{}_unknown_impact',
                 'nb_started_orders_last{}_low_impact', 'nb_forbidden_orders_last{}', 'avg_days_completed_orders_last{}']
        type1b = ['nb_started_orders_{}_last{}', 'nb_completed_orders_{}_last{}']
        type2 = ['nb_running_last{}_gt{}', 'has_forbidden_orders_last{}', 'last_order_last{}']
        type3 = ['nb_completed_orders_last{}_{}SLA']
        type3b = ['nb_completed_orders_{}_last{}_{}SLA']

        type4 = ['{}_orders_last{}', '{}_first_order_last{}', '{}_avg_running_days{}_outsideSLA', '{}_avg_running_days{}_insideSLA', '{}_last_order_last{}', '{}_max_running_days{}_insideSLA',
                 '{}_max_running_days{}_outsideSLA', '{}_nb_orders_opened{}_outsideSLA', '{}_nb_orders_opened{}_insideSLA', "{}_avg_days_completed_orders_last{}", "{}_nb_completed_orders_last{}"]
        type4b = ['mean_sla_factor_{}_last{}', 'nb_completed_orders_{}_last{}']
        type4c = ['{}_avg_running_days{}_insideSLA', '{}_avg_running_days{}_outsideSLA']
        type5 = ['{}_nb_running_last{}_gt{}', '{}_nb_running_last{}_lte{}']
        type6 = ['{}_nb_orders_opened{}_{}SLA_{}']
        type7 = ['{}_max_SLA_factor_running{}_{}SLA']

        cols1 = [p[0].format(p[1]) for p in list(itertools.product(type1, days_range))]
        cols1b = [p[0].format(p[1], p[2]) for p in list(itertools.product(type1b, traslado_orders + forbidden_types, days_range))]

        cols2 = [p[0].format(p[1], p[2]) for p in list(itertools.product(type2, days_range, deadlines_range))]
        cols3 = [p[0].format(p[1], p[2]) for p in list(itertools.product(type3, days_range, factor_list))]
        cols3b = [p[0].format(p[1], p[2], p[3]) for p in list(itertools.product(type3b, traslado_orders, days_range, factor_list))]

        cols4 = [p[0].format(p[1], p[2]) for p in list(itertools.product(type4, ordinary_types, days_range))]
        cols4b = [p[0].format(p[1], p[2]) for p in list(itertools.product(type4b, traslado_orders, days_range))]
        cols4c = [p[0].format(p[1], p[2]) for p in list(itertools.product(type4c, ["disminucion"], days_range))]

        cols5 = [p[0].format(p[1], p[2], p[3]) for p in list(itertools.product(type5, ordinary_types, days_range, deadlines_range))]
        cols6 = [p[0].format(p[1], p[2], p[3], p[4]) for p in list(itertools.product(type6, ordinary_types, days_range, sla_types, impact_types))]
        cols7 = [p[0].format(p[1], p[2], p[3]) for p in list(itertools.product(type7, ordinary_types, days_range, sla_types))]

        feats = cols1 + cols1b + cols2 + cols3 + cols4 + cols3b + cols4b + cols4c + cols5 + cols6 + cols7 + ['avg_days_bw_open_orders', 'avg_days_bw_open_orders_traslado']

        na_vals = [str(-1) if "avg_days" in col_ else (str(0) if "orders" in col_ else str(-1)) for col_ in feats]

        data = {'feature': feats, 'imp_value': na_vals}

        import pandas as pd

        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('orders')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('num_cliente'))

        return metadata_df
