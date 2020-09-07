

from pyspark.sql.functions import col, lit, substring, asc, udf, datediff, row_number, concat, when, desc
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
# import pykhaos.utils.custom_logger as clogger
# logger = clogger.get_custom_logger()
from churn.utils.constants import *
from churn.utils.udf_manager import Funct_to_UDF
from churn.utils.general_functions import amdocs_table_reader
import sys


def get_port_requests_table(spark, config_obj=None, start_date=None, end_date=None, ref_date=None, select_cols=None):
    """
    Returns a dataframe with the structure (msisdn, label=1.0) for the services (movil or fixed)
    that have requested to be ported out during the days specified by the user
    """
    if not start_date:
        start_date = config_obj.get_start_port()
    if not end_date:
        end_date = config_obj.get_end_port()
    if not ref_date:
        ref_date=end_date

    if start_date == None or end_date==None:
        print("start_date and end_date should be different to None (Inserted {} and {})".format(start_date, end_date))
        sys.exit()


    print("Get port requests table: start={} end={} ref_date={}".format(start_date, end_date, ref_date))

    select_cols = ["msisdn_a", "label"] if not select_cols else select_cols

    window = Window.partitionBy("msisdn_a").orderBy(desc("days_from_portout")) # keep the 1st portout

    start_date_obj = Funct_to_UDF.convert_to_date(start_date)
    end_date_obj = Funct_to_UDF.convert_to_date(end_date)

    convert_to_date_udf = udf(Funct_to_UDF.convert_to_date, StringType())

    df_mobport = (spark.read.table(PORT_TABLE_NAME)
                  .where((col("sopo_ds_fecha_solicitud") >= start_date_obj) & (col("sopo_ds_fecha_solicitud") <= end_date_obj))
                  .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn_a")
                  .withColumnRenamed("SOPO_DS_FECHA_SOLICITUD", "portout_date")
                  .withColumn("portout_date", substring(col("portout_date"), 0, 10))
                  #.select("msisdn_a", "sopo_ds_msisdn2", "sopo_msisdn_authorizada", "portout_date", "SOPO_CO_RECEPTOR")
                  .withColumn("portout_date", convert_to_date_udf(col("portout_date")))
                  .withColumn("ref_date",
                              convert_to_date_udf(concat(lit(ref_date[:4]), lit(ref_date[4:6]), lit(ref_date[6:]))))
                  .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int"))
                  .withColumn("rank", row_number().over(window))
                  .where(col("rank") == 1))

    if config_obj:
        if config_obj.get_model_target() == "port":
            df_mobport = (df_mobport.withColumn("label", lit(1.0)))


        elif config_obj.get_model_target() == "masmovil":
            df_mobport = (df_mobport.withColumn("label", lit(-1.0))
                                    .withColumn("label", when(((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735014"))
                                        | ((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735044"))
                                        | ((col("SOPO_CO_RECEPTOR") == "AIRTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "725303"))
                                        | ((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735054"))
                                        | ((col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "715501"))
                                        | ((col("SOPO_CO_RECEPTOR") == "YOIGO") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")), 1.0).otherwise(col("label"))).select(select_cols))

        elif config_obj.get_model_target() == "orange":
            df_mobport = (df_mobport.withColumn("label", lit(-1.0))
                                    .withColumn("label", when(((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"))
                                                            | ((col("SOPO_CO_RECEPTOR") == "JAZZTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"))
                                                            | ((col("SOPO_CO_RECEPTOR") == "EPLUS") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")), 1.0).otherwise(col("label"))))
        elif config_obj.get_model_target() == "movistar":
            df_mobport = (df_mobport.withColumn("label", lit(-1.0))
                                    .withColumn("label", when(((col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")), 1.0).otherwise(col("label"))))
    else:
        df_mobport = (df_mobport.withColumn("label", lit(1.0)))


    return df_mobport.select(select_cols)



def get_active_services(spark, closing_day, new, customer_cols=None, service_cols=None):
    '''

    :param spark:
    :param closing_day:
    :param new:
    :param customer_cols: if not specified, ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"] are taken
    :param service_cols: if not specified, ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"] are taken
    :return:
    '''
    if not customer_cols:
        customer_cols = ["num_cliente", "cod_estado_general", "clase_cli_cod_clase_cliente"]
    if not service_cols:
        service_cols = ["msisdn", "num_cliente", "campo2", "rgu", "srv_basic"]

    df_customer = (amdocs_table_reader(spark, "customer", closing_day, new)
                   .where(col("clase_cli_cod_clase_cliente") == "RS")  # customer
                   .where(col("cod_estado_general").isin(["01", "09"]))  # customer
                   .select(customer_cols)
                   .withColumnRenamed("num_cliente", "num_cliente_customer"))

    df_service = (amdocs_table_reader(spark, "service", closing_day, new)
                  .where(~col("srv_basic").isin(["MRSUI", "MPSUI"]))  # service
                  .where(col("rgu").isNotNull())
                  .select(service_cols)
                  .withColumnRenamed("num_cliente", "num_cliente_service"))

    df_services = df_customer.join(df_service,
                                   on=(df_customer["num_cliente_customer"] == df_service["num_cliente_service"]),
                                   how="inner")  # intersection

    #print("df_customer&df_service", df_services.count())

    return df_services

def get_numclients_under_analysis(spark, config_obj, closing_day, segment_filter=None, new=False):

    '''
    Return a dataframe with column "num_client" with the clients of the segment
    :param spark:
    :param config_obj:
    :param closing_day
    :return:
    '''

    #closing_day = config_obj.get_closing_day()
    if not segment_filter:
        segment_filter =  config_obj.get_segment_filter() # segmento al que nos dirigimos


    #new = config_obj.get_amdocs_car_version()


    print("Calling get_numclients_under_analysis for closing_day={} and segment_filter={}".format(closing_day, segment_filter))

    df_services = get_active_services(spark, closing_day, new)


    df_customer_aggregations = (amdocs_table_reader(spark, "customer_agg", closing_day, new)).na.fill(0)

    print("*********** {}".format(",".join(df_customer_aggregations.columns)))

    if segment_filter == "onlymob":
        df_customer_aggregations = (df_customer_aggregations
                   .where((col("mobile_services_nc") >= 1) & (col("fbb_services_nc") == 0) & (col("tv_services_nc") == 0) &
                          (col("fixed_services_nc") == 0) & (col("bam_services_nc") == 0) & (col("bam_mobile_services_nc") == 0))
                                    .select("num_cliente"))
    elif segment_filter == "onlymobmono":
        df_customer_aggregations = (df_customer_aggregations
                   .where((col("mobile_services_nc") == 1) & (col("fbb_services_nc") == 0) & (col("tv_services_nc") == 0) &
                          (col("fixed_services_nc") == 0) & (col("bam_services_nc") == 0) & (col("bam_mobile_services_nc") == 0))
                                    .select("num_cliente"))
    elif segment_filter == "onlymobmulti":
        df_customer_aggregations = (df_customer_aggregations
                    .where((col("mobile_services_nc") > 1) & (col("fbb_services_nc") == 0) & (col("tv_services_nc") == 0) &
                           (col("fixed_services_nc") == 0) & (col("bam_services_nc") == 0) & (col("bam_mobile_services_nc") == 0))
                                    .select("num_cliente"))
    elif segment_filter == "allmob":
        df_customer_aggregations = df_customer_aggregations.where(col("mobile_services_nc") > 0).select("num_cliente")
    elif segment_filter == "mobileandfbb":
        df_customer_aggregations = (df_customer_aggregations
                    .where((col("mobile_services_nc") > 0) & (col("fbb_services_nc") > 0)).select("num_cliente"))
    else:
        print("[ERROR] segment_filter {} is not implemented yet".format(segment_filter))
        import sys
        sys.exit(1)


    df_join = (df_services.join(df_customer_aggregations, on=df_services["num_cliente_customer"] == df_customer_aggregations["num_cliente"], how="inner")
                .select("num_cliente")
                .distinct())

    print("!!!!!!!!get_numclients_under_analysis - segment {} has {} num_client ".format(segment_filter, df_join.count()))

    return df_join


def get_unlabeled_car(spark, config_obj):

    closing_day = config_obj.get_closing_day()

    new = config_obj.get_amdocs_car_version()

    print("Asked unlabeled car {} {}".format(closing_day, new))
    from pykhaos.utils.pyspark_utils import union_all

    dfs = []
    for c_day in closing_day: # in case, user specified more than one closing day
        df_cday = get_unlabeled_car_closing_day(spark, config_obj, c_day, apply_segment=True)
        dfs.append(df_cday)
    df_all = union_all(dfs)
    return df_all


def get_unlabeled_car_closing_day(spark, config_obj, closing_day, apply_segment=True):
    '''
    Return an unlabeled car for the sources specified in the internal config file.
    Only applies the segment filter if exists on the user config file
    No filter by rgu.
    :param spark:
    :param config_obj:
    :param closing_day
    :return:
    '''
    df_target_num_clients=None
    if config_obj.get_segment_filter() and apply_segment:
        # - - - - - - - - - - - - - - - - - - - - - -
        # SEGMENT UNDER ANALYSIS
        # Get the population(num_cliente)
        df_target_num_clients = get_numclients_under_analysis(spark, config_obj, closing_day)

    # - - - - - - - - - - - - - - - - - - - - - -
    # CAR
    # Load the CAR for the specified date

    df_car = read_amdocs_sources_closing_day(spark, config_obj, closing_day)
    df_car = df_car.cache()
    print("**** BEFORE SEGMENT FILTER {}".format(df_car.count()))

    if config_obj.get_segment_filter() and apply_segment:
        df_car = (df_car.join(df_target_num_clients, on=["num_cliente"], how="inner")
                  .withColumnRenamed("campo2", "msisdn_d")
                  )

    print("**** AFTER SEGMENT FILTER {}".format(df_car.count()))


    return df_car

def read_amdocs_sources_closing_day(spark, config_obj, closing_day, debug=False):
    df_join = None

    print("read_amdocs_sources", config_obj.get_default_and_selected_table_names())
    # order tables names according to priority in constants module
    sources_to_read = sorted(list(set(config_obj.get_default_and_selected_table_names())), key=lambda k: JOIN_ORDER[k][0])

    print("sources will be read following to this order: {}".format(",".join(sources_to_read)))

    new = config_obj.get_amdocs_car_version()

    for ids_src in sources_to_read:  # others tables??

        print("Reading '{}' loop sources_to_read".format(ids_src))

        df_src = amdocs_table_reader(spark, ids_src, closing_day, new)

        if len(df_src.take(1))== 0:
            continue

        if df_join == None:
            df_join = df_src
        else:
            if len(JOIN_ORDER[ids_src]) == 3:
                on_cols_left = [JOIN_ORDER[ids_src][1]] if isinstance(JOIN_ORDER[ids_src][1], str) else JOIN_ORDER[ids_src][1]
                on_cols_right = [JOIN_ORDER[ids_src][1]] if isinstance(JOIN_ORDER[ids_src][1], str) else JOIN_ORDER[ids_src][1]
            else:
                on_cols_left = JOIN_ORDER[ids_src][1]
                on_cols_right = JOIN_ORDER[ids_src][2]

            if debug: print("left='{}' right='{}'".format(",".join(on_cols_left), ",".join(on_cols_right)))
            how_join = JOIN_ORDER[ids_src][-1]

            for col_ in on_cols_right:
                df_src = df_src.withColumnRenamed(col_, ids_src + "_" + col_)

            on_cols_right = [ids_src + "_" + col_ for col_ in on_cols_right] # add preffix to join cols

            df_join = df_join.join(df_src, on=[col(f) == col(s) for (f, s) in zip(on_cols_left, on_cols_right)], how=how_join)

            df_join=df_join.drop(*on_cols_right)

            if debug: print(",".join(df_join.columns))

        if debug: print("end reading source {}".format(ids_src))
    print("Ended process of reading ids sources")

    #df_join = (df_join.withColumnRenamed("campo2", "msisdn_d").withColumnRenamed("msisdn", "msisdn_a"))

    return df_join

def read_amdocs_sources(spark, config_obj, debug=False):
    closing_day = config_obj.get_closing_day()
    from pykhaos.utils.pyspark_utils import union_all
    dfs = []
    for c_day  in closing_day: # in case, user specified more than one closing day
        if debug: print("Working on closing_day={}".format(c_day))
        df_cday = read_amdocs_sources_closing_day(spark, config_obj, closing_day, debug=False)
        dfs.append(df_cday)
    df_all = union_all(dfs)
    return df_all













