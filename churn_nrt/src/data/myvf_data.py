
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from pyspark.sql.types import StringType

from pyspark.sql.functions import (udf,
                                   col,
                                   when,
                                   lit,
                                   lower,
                                   count,
                                   expr,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   count as sql_count,
                                   sum as sql_sum,
                                   lag,
                                   unix_timestamp,
                                   from_unixtime,
                                   datediff,
                                   desc,
                                   countDistinct,
                                   asc,
                                   row_number,
                                   upper,
                                   coalesce,
                                   concat,
                                   lpad,
                                   trim,
                                   split,
                                    length,
                                   regexp_replace
                                   )

from pyspark.sql.window import Window
import pandas as pd
import datetime as dt
import sys
import itertools
import re

from pyspark.sql.types import IntegerType


PLATFORM_APP = "app"
PLATFORM_WEB = "web"

TABLE_APP = "raw_es.customerprofilecar_adobe_sections"
TABLE_WEB = "raw_es.customerprofilecar_adobe_web"

PREFFIX_APP_COLS = "myvf"
PREFFIX_WEB_COLS = "myvfweb"

#TODO check if in the web data, these sections are the commonest
SECTIONS_APP = ["dashboard", "facturas", "productos_y_servicios", "que_tengo_contratado", "mi_cuenta", "mensajes", "prelogin",
            "childbrowser", "webview", "consumo", "screen_myrewards", "averias", "faqs"]

SECTIONS_WEB = ["mi_vodafone", "particulares", "apppostpago", "autonomos", "area_clientes", "porsertu", "empresas", "observatorio_empresas",
                "eforum", "url_mivodafone_area_privada_contrato", "vodafone_bit", "appautorizado", "login", "yu"]

DAYS_RANGE =  [7, 14, 30, 60, 90, 120, 240, 365]
PERIODS_INCREMENTALS = [30,60,90]


def get_last_date(spark, platform="app"):

    if platform == PLATFORM_APP:
        myvf_last_date = (spark.read.table(TABLE_APP)
                          .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
                          .select(sql_max(col('mydate')).alias('last_date'))
                         .rdd.first()['last_date'])

    elif platform == PLATFORM_WEB:
        myvf_last_date = (spark.read.table(TABLE_WEB)
                          .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
                          .select(sql_max(col('mydate')).alias('last_date'))
                         .rdd.first()['last_date'])


    else:
        print("Error! Platform {} is not supported in get_last_date function".format(platform))
        sys.exit()

    return int(myvf_last_date)


def compute_groupby_attributes(df_net, days_range, sections, level=None):

    if not level:
        level = ["msisdn"]

    df_net_agg = (df_net.groupby(level).agg(*(
            [sql_count(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("days_since_nav")).otherwise(None)).alias("nb_pages_last{}".format(dd)) for dd in days_range] + [
        countDistinct(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("date_")).otherwise(None)).alias("nb_days_access_last{}".format(dd)) for dd in days_range] + [
                sql_sum(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("flag_permanencia")).otherwise(0)).alias("permanencia_nb_pages_last{}".format(dd)) for dd in
                days_range] + [sql_avg(when(((col("days_since_nav") <= dd) & (col("diff") != 0) & (col("diff").isNotNull())), col("diff"))).alias("avg_days_bw_navigations_last{}".format(dd)) for dd in
                               days_range] + [sql_max(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("days_since_nav")).otherwise(None)).alias("1st_navig_last{}".format(dd))
                                              for dd in days_range] + [
                sql_min(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("days_since_nav")).otherwise(None)).alias("last_navig_last{}".format(dd)) for dd in days_range] + [
                sql_max(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0) & (col("flag_permanencia") == 1)), col("days_since_nav")).otherwise(None)).alias(
                    "permanencia_1st_navig_last{}".format(dd)) for dd in days_range] + [
                sql_min(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0) & (col("flag_permanencia") == 1)), col("days_since_nav")).otherwise(None)).alias(
                    "permanencia_last_navig_last{}".format(dd)) for dd in days_range] + [
                countDistinct(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0) & (col("flag_permanencia") == 1)), col("date_")).otherwise(None)).alias(
                    "permanencia_nb_days_access_last{}".format(dd)) for dd in days_range])))

    df_net_bysection = (df_net.where(col("section").isin(sections)).groupby(level).pivot('section', values=sections).agg(*(
            [sql_count(when(((col("days_since_nav") < dd) & (col("days_since_nav") != -1)), col("days_since_nav")).otherwise(None)).alias("nb_pages_last{}".format(dd)) for dd in days_range] + [
        countDistinct(when(((col("days_since_nav") <= dd) & (col("days_since_nav") >= 0)), col("date_")).otherwise(None)).alias("nb_days_access_last{}".format(dd)) for dd in days_range] + [
                sql_max(when(((col("days_since_nav") < dd) & (col("days_since_nav") != -1)), col("days_since_nav")).otherwise(None)).alias("1st_navig_last{}".format(dd)) for dd in days_range] + [
                sql_min(when(((col("days_since_nav") < dd) & (col("days_since_nav") != -1)), col("days_since_nav")).otherwise(None)).alias("last_navig_last{}".format(dd)) for dd in days_range])))

    level_join = "msisdn" if "msisdn" in level else "nif_cliente"
    if level_join == "msisdn":
        if "nif_cliente" in level: # avoid having "nif_cliente" in both dataframes
            df_net_bysection= df_net_bysection.drop("nif_cliente")
        df_net_agg = df_net_agg.join(df_net_bysection, on=["msisdn"], how="left")
    else:
        df_net_agg = df_net_agg.join(df_net_bysection, on=["nif_cliente"], how="left")


    return df_net_agg


def compute_df_net(spark, platform, closing_day):
    if platform == PLATFORM_APP:
        df_net = (spark.read.table(TABLE_APP)
                  .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
            .drop("service_processed_at", "service_file_id", "year", "month", "day").withColumn(
            "msisdn", expr("substring(msisdn, 3, length(msisdn)-2)")))
    elif platform == PLATFORM_WEB:
        df_net = (spark.read.table(TABLE_WEB)
                  .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
                  .drop("service_processed_at", "service_file_id", "year", "month", "day")
                  .withColumn("msisdn", expr("substring(msisdn, 3, length(msisdn)-2)")))
    else:
        print("[MyVFdata] build_module | Table for platform {} is nos prepared yet".format(platform))
        sys.exit()

    #Remove empty fields
    df_net = df_net.where(coalesce(length(col("Pages")), lit(0)) > 0)

    # - - - - - - - - - - -
    func_date = udf(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime("%Y%m%d"), StringType())
    df_net = df_net.withColumn('date_', func_date(col('Date'))).drop("Date")

    stages_name = ["section", "subsection", "page_name"]

    df_net = df_net.where(coalesce(length(col("Pages")), lit(0)) > 0)

    df_net = df_net.withColumn('Pages', regexp_replace('Pages', ', ', '_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', '-', '_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', ' ', '_'))
    # subsection exist with both names - join them
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'configuracion_notificacione_push', 'configuracion_notificaciones_push'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://foro.vodafone.es', 'url_foro'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://foro.vodafone.es/t5/', 'url_foro_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://manuales.vodafone.es/', 'url_manuales_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.vodafone.es/c/mivodafone/es/', 'url_mivodafone_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.vodafone.es/c/tienda_online/', 'url_tiendaonline_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.vodafone.es/', 'urlvf_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://oferta.vodafone.es/', 'url_oferta_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://ilead.itrack.it/clients/', 'urlilead_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://presupuesto_tarifas_adsl_fibra.rastreator.com/', 'url_rastreator_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.vodafoneofertas.com', 'url_vf_ofertas'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.vodafoneofertas.com/', 'url_vf_ofertas_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://vodafone.oferta_fibra.com', 'url_oferta_fibra_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://vodafone.altas_internet.com/', 'url_altas_internet_'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://vodafone.estufibra.com/', 'url_estufibra'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.ono.es/ficticio', 'url_onoficticio'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.adslred.com/', 'url_adslred'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://ayudacliente.vodafone.es/', 'url_ayudacliente'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://www.yu.vodafone.es/', 'url_yu'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://vodafone.comparatodo.es/', 'url_comparatodo'))
    df_net = df_net.withColumn('Pages', regexp_replace('Pages', 'https://roams.es/', 'url_roams'))

    #After replacing the main urls, remove other urls
    df_net = df_net.where(~col("Pages").rlike("^https:"))

    for ii, col_ in enumerate(stages_name):
        if col_ == "section":
            # Do not split if Pages is an url
            df_net = df_net.withColumn(col_, when(~col("Pages").rlike("^https://"), split("Pages", ":")[ii]).otherwise(col("Pages")))
        else:
            df_net = df_net.withColumn(col_, when(~col("Pages").rlike("^https://"), split("Pages", ":")[ii]).otherwise(None))

    df_net = df_net.withColumn('section', regexp_replace('section', ', ', '_'))
    df_net = df_net.withColumn('section', regexp_replace('section', ' ', '_'))  # remove spaces with underscore to avoid error when writing df
    df_net = df_net.withColumn('section', regexp_replace('section', 'https://m.vodafone.es/mves/', 'url_'))
    df_net = df_net.withColumn('section', regexp_replace('section', r'/|-', '_'))
    df_net = df_net.withColumn('section', when(col('section') == 'productsandservices', 'productos_y_servicios').otherwise(col('section')))

    # days_since_nav >=0 for dates <= closing_day
    # days_since_nav <0 for dates > closing_day (future dates) --> excluded in filter by year/month/day

    df_net = df_net.withColumn("days_since_nav",
                               when(col("date_").isNotNull(), datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), from_unixtime(unix_timestamp(col("date_"), "yyyyMMdd")))).otherwise(
                                   -1))
    df_net = df_net.withColumn("diff",
                               when(col("days_since_nav") >= 0, lag(col("days_since_nav"), -1).over(Window.partitionBy("msisdn").orderBy(asc("days_since_nav"))) - col("days_since_nav")).otherwise(
                                   None))

    df_net = df_net.withColumn("flag_permanencia", when(col("Pages").rlike("permanencia"), 1).otherwise(0))


    return df_net



def compute_df(spark, platform, closing_day, save_others, sections, days_range, version):

    df_net = compute_df_net(spark, platform, closing_day)

    # For average between days, only once navigation per day is taken. That means that a user with a navigation profile like this:
    # USER A: "20191021", "20191021", "20191022" --> [0,1] --> [1] --> avg_days_bw_navigations = 1.0
    # USER B: "20191012", "20191012", "20191013", "20191019" --> [0,1,6] --> [1,6] -->  avg_days_bw_navigations = 3.5

    if version > 1: #ATTRIBUTES BY NIF
        print("Adding nif attributes for version {}".format(version))
        # - - - - - - - - - - -
        # Keep only msisdns in our customer base + add nif cliente
        from churn_nrt.src.data.customer_base import CustomerBase
        df_cust = CustomerBase(spark).get_module(closing_day, save=save_others, save_others=save_others)
        df_net = df_net.join(df_cust.select("msisdn", "nif_cliente"), on=["msisdn"], how="inner")

        df_net_agg = compute_groupby_attributes(df_net, days_range, sections, level=["nif_cliente", "msisdn"])

        df_net_agg_nif = compute_groupby_attributes(df_net, days_range, sections, level=["nif_cliente"])

        new_suffixed_cols = [col_ + "_nif" if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_net_agg_nif.columns]
        df_net_agg_nif = df_net_agg_nif.toDF(*new_suffixed_cols)

        #print(df_net_agg_nif.columns)
        cols_metadata = MyVFdata(spark, platform, 2).get_metadata().rdd.map(lambda x: x['feature']).collect()
        # Remove preffix and select _nif columns
        cols_metadata = [re.sub(r"^{}_".format(PREFFIX_APP_COLS if platform == PLATFORM_APP else PREFFIX_WEB_COLS), "", col_) for col_ in cols_metadata if col_.endswith("_nif")] + ["msisdn"]

        for col_ in list(set(cols_metadata) - set(df_net_agg_nif.columns) - {"msisdn", "nif_cliente"}):
            print("col {} is not in df_net_agg_nif dataframe. Added as lit(0)".format(col_))
            df_net_agg_nif = df_net_agg_nif.withColumn(col_, lit(0))


        df_net_agg = df_net_agg.join(df_net_agg_nif, on=["nif_cliente"])#.drop("nif_cliente")
        if version == 2:
            df_net_agg = df_net_agg.drop("nif_cliente")

    else:
        df_net_agg = compute_groupby_attributes(df_net, days_range, sections, level=["msisdn"])

        to_remove = [col_ for col_ in df_net_agg.columns if "days_access" in col_]
        df_net_agg = df_net_agg.drop(*to_remove)


    if version == 3: # INCREMENTALS
        cols_metadata = MyVFdata(spark, platform, 2).get_metadata().rdd.map(lambda x: x['feature']).collect()
        nb_pages_nif = [re.sub(r"^{}_".format(PREFFIX_APP_COLS if platform == PLATFORM_APP else PREFFIX_WEB_COLS), "", col_) for col_ in cols_metadata if (col_.endswith("_nif") and "nb_pages" in col_)] + ["nif_cliente"]
        from churn_nrt.src.utils.date_functions import move_date_n_days
        dxx_diff_cols = []
        for dd in PERIODS_INCREMENTALS:
            print("prev {} days".format(dd))

            df_net_dd = compute_df_net(spark, platform, move_date_n_days(closing_day, n=-dd))

            df_net_dd = df_net_dd.join(df_cust.select("msisdn", "nif_cliente"), on=["msisdn"], how="inner")

            df_net_agg_dd_nif = compute_groupby_attributes(df_net_dd, days_range, sections, level=["nif_cliente"])
            new_suffixed_cols = [col_ + "_nif" if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_net_agg_dd_nif.columns]
            df_nif_dxx = df_net_agg_dd_nif.toDF(*new_suffixed_cols)

            for col_ in list(set(nb_pages_nif) - set(df_nif_dxx.columns) - {"msisdn", "nif_cliente"}):
                print("col {} is not in df_nif_dxx dataframe. Added as lit(0)".format(col_))
                df_nif_dxx = df_nif_dxx.withColumn(col_, lit(0))

            df_nif_dxx_sel = df_nif_dxx.select(*nb_pages_nif)
            new_suffixed_cols = [col_ + "_d{}".format(dd) if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_nif_dxx_sel.columns]
            df_nif_dxx_sel = df_nif_dxx_sel.toDF(*new_suffixed_cols)

            df_net_agg = df_net_agg.join(df_nif_dxx_sel, on=["nif_cliente"], how="left")
            print(df_net_agg.columns)

            dxx_diff_cols += [(col(col_) - col(col_ + "_d{}".format(dd))).alias("inc_" + col_ + "_d{}".format(dd)) for col_ in nb_pages_nif if col_ not in ["msisdn", "nif_cliente"]]

        df_net_agg = df_net_agg.select(*(df_net_agg.columns + dxx_diff_cols)).drop("nif_cliente")


    return df_net_agg




class MyVFdata(DataTemplate):

    PLATFORM = None
    VERSION = None

    def __init__(self, spark, platform=PLATFORM_APP, version=1):

        if platform not in [PLATFORM_APP, PLATFORM_WEB]:
            print("[MyVFdata] __init__ | Invalid platform {}".format(platform))
            sys.exit()

        self.PLATFORM = platform
        self.VERSION = version
        DataTemplate.__init__(self, spark, "myvf/{}/{}".format(self.PLATFORM, self.VERSION))

    def build_module(self, closing_day, save_others, sections=None, days_range=None, **kwargs):

        print("[MyVFdata] build_module | Requesting version {} for closing_day {} and days_range {}".format(self.VERSION, closing_day, ",".join(days_range) if days_range else "None"))

        if not sections:
            if self.PLATFORM == PLATFORM_APP:
                sections = SECTIONS_APP
            else:
                sections = SECTIONS_WEB

        if not days_range:
            days_range = DAYS_RANGE

        #print("[MyVFdata] build_module | Building module for days_range {}".format(",".join(days_range) if days_range else "None"))

        df_net_agg = compute_df(self.SPARK, self.PLATFORM, closing_day, save_others, sections, days_range, self.VERSION)

        # - - - - - - - - - - -

        preffix_col = PREFFIX_APP_COLS+"_" if self.PLATFORM == PLATFORM_APP else PREFFIX_WEB_COLS+"_"
        new_cols =[preffix_col+col_ if col_ not in ["msisdn"] else col_ for col_ in df_net_agg.columns]
        df_net_agg = df_net_agg.toDF(*new_cols)

        module_cols = self.get_metadata().rdd.map(lambda x: x['feature']).collect() + ["msisdn"]
        if len(module_cols) > len(df_net_agg.columns):
            # some columns were not generated. be sure the df has all the columns in the metadata
            df_net_agg = df_net_agg.select(*([col(col_) if col_ in df_net_agg.columns else (lit(None).cast(IntegerType())).alias(col_) for col_ in module_cols]))

        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_net_agg = apply_metadata(self.get_metadata(), df_net_agg)
        df_net_agg = df_net_agg.repartition(200)
        return df_net_agg

    def get_metadata(self, sections=None, days_range=None):

        if not sections:
            if self.PLATFORM == PLATFORM_APP:
                sections = SECTIONS_APP + ["permanencia"]
            else:
                sections = SECTIONS_WEB + ["permanencia"]

        if not days_range:
            days_range = DAYS_RANGE

        preffix_ = PREFFIX_APP_COLS if self.PLATFORM == PLATFORM_APP else PREFFIX_WEB_COLS

        METRIC = [preffix_+"_nb_pages_last",  preffix_+"_avg_days_bw_navigations_last", preffix_+"_1st_navig_last", preffix_+'_last_navig_last', preffix_+'_nb_days_access_last']
        SECTIONS_METRIC = [preffix_+'_{}_nb_pages_last{}', preffix_+'_{}_1st_navig_last{}', preffix_+'_{}_last_navig_last{}', preffix_+'_{}_nb_days_access_last{}']

        bysections = [p[0].format(p[1], p[2]) for p in list(itertools.product(SECTIONS_METRIC, sections, days_range))]
        general = ["{}{}".format(p[0], p[1]) for p in list(itertools.product(METRIC, days_range))]

        all_cols = bysections + general # + ["msisdn"]

        #if self.PLATFORM == PLATFORM_WEB:
        #    #when platform is web, then preffix of columns is myvfweb
        #    all_cols = [re.sub("^"+PREFFIX_APP_COLS+"_", PREFFIX_WEB_COLS+"_", m) for m in all_cols]

        if self.VERSION > 1:
            print("Adding columns with _nif suffix")
            all_cols = all_cols + [col_ + "_nif" for col_ in all_cols if col_ not in ["msisdn", "nif_cliente"]]
            if self.VERSION > 2 :
                nb_pages_nif = [col_ for col_ in all_cols if (col_.endswith("_nif") and "nb_pages" in col_)]
                all_cols = all_cols + [col_+"_d{}".format(dd) for col_ in nb_pages_nif for dd in PERIODS_INCREMENTALS] + \
                                      [preffix_ + "_inc_" + re.sub(r"^{}_".format(preffix_), "", col_) + "_d{}".format(dd) for col_ in nb_pages_nif for dd in PERIODS_INCREMENTALS]

        else:
            all_cols = [col_ for col_ in all_cols if not col_.endswith("_nif") and not "days_access" in col_]

        na_vals = [str(0) if "nb_pages" in col_ else str(-1) for col_ in all_cols]

        cat_feats = []
        data = {'feature': all_cols, 'imp_value': na_vals}

        source_ = PREFFIX_WEB_COLS if self.PLATFORM == PLATFORM_WEB else PREFFIX_APP_COLS
        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
            .withColumn('source', lit(source_))
            .withColumn('type', lit('numeric'))
            .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type')))
            .withColumn('level', lit('msisdn')))

        return metadata_df
