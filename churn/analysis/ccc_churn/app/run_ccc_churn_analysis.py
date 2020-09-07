import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, sum as sql_sum, count as sql_count, \
    datediff, unix_timestamp, from_unixtime


def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))


    ENGINE_SRC = "/var/SP/data/home/csanc109/src/devel/amdocs_informational_dataset/"
    if ENGINE_SRC not in sys.path:
        sys.path.append(ENGINE_SRC)
        print("Added '{}' to path".format(ENGINE_SRC))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "run_ccc_analysis_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger




def join_dfs(df_mobile_services, df_ccc, df_tgs, df_port, closing_day):

    from pyspark.sql.functions import datediff, unix_timestamp, from_unixtime, translate, substring

    df_base_tgs = df_mobile_services.join(df_tgs, on=["msisdn"], how="left")

    df_port_ccc = df_port.join(df_ccc, on=["msisdn"], how="outer")

    df_port_ccc = (df_port_ccc.withColumn("days_from_ccc_to_sopo", when(
        (col("portout_date").isNotNull()) & (col("fx_interaction").isNotNull()),
        datediff(
            from_unixtime(unix_timestamp(translate(substring(col("portout_date"), 1, 10), "-", ""), "yyyyMMdd")),
            from_unixtime(unix_timestamp("fx_interaction", "yyyyMMdd")),
        ).cast("double")).otherwise(None)))

    df_port_ccc = (df_port_ccc.withColumn("days_from_cday_to_sopo", when(
        col("portout_date").isNotNull(),
        datediff(
            from_unixtime(unix_timestamp(translate(substring(col("portout_date"), 1, 10), "-", ""), "yyyyMMdd")),
            from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
        ).cast("double")).otherwise(None)))


    # when "days_from_ccc_to_sopo" is negative, means that fx_interaction is later than sopo date --> it is not a trigger entry
    df_port_ccc = df_port_ccc.withColumn("VALID_CCC_SOPO", when((col("days_from_ccc_to_sopo").isNotNull()) &
        (col("days_from_ccc_to_sopo") >= 0) & (col("days_from_ccc_to_sopo") <= 30), 1).otherwise(0))

    df_port_ccc = df_port_ccc.withColumn("VALID_CDAY_SOPO", when((col("days_from_cday_to_sopo").isNotNull()) &
        (col("days_from_cday_to_sopo") >= 0) & (col("days_from_cday_to_sopo") <= 30), 1).otherwise(0))

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, asc, desc

    wndw_fx_interaction = Window().partitionBy("msisdn").orderBy(desc("VALID_CCC_SOPO"), asc("fx_interaction"))
    # In case of more df_port_ccc one interaction by msisdn, we keep the first one, putting first the valid ones
    df_port_ccc = (df_port_ccc.withColumn("rowNum", row_number().over(wndw_fx_interaction))
               .where(col('rowNum') == 1).drop("rowNum"))

    df_base_tgs_port_ccc = df_base_tgs.join(df_port_ccc, on=["msisdn"], how="left")

    tipis_cols = [col_ for col_ in df_ccc.columns if col_.upper().startswith("TIPIS")]

    df_base_tgs_port_ccc = df_base_tgs_port_ccc.fillna(0, subset=(["CCC", "SOPO"]+tipis_cols))

    # only valid entries are considered VALID=1
    df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("CHURN", when(col("VALID_CCC_SOPO")+col("VALID_CDAY_SOPO")>0,1).otherwise(0))

    return df_base_tgs_port_ccc


#  spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120
# --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150
# --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000 --queue root.BDPtenants.es.medium
# --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=1G
# --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3
# --executor-memory 12G --driver-memory 2G --conf spark.dynamicAllocation.maxExecutors=15 churn/analysis/ccc_churn/app/run_ccc_churn_analysis.py
# -c 20190131 -p 3



if __name__ == "__main__":

    logger = set_paths_and_logger()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description='Generate table of extra feats',
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<CLOSING_DAY>', type=str, required=True,
                        help='Closing day YYYYMMDD (end of port period)')
    parser.add_argument('-p', '--port_months_length', metavar='<port_months_length>', type=str, required=True,
                        help='length (in months) for the port period')
    # parser.add_argument('-b', '--by', metavar='<by>', type=str, required=False,
    #                     help='Aggregation parameter (msisdn/num_cliente). Default: msisdn')
    args = parser.parse_args()

    print(args)

    CLOSING_DAY = args.closing_day
    PORT_MONTHS_LENGTH = int(args.port_months_length)
    # BY = args.by

    # if not BY:
    #     BY = "msisdn"

    if logger: logger.info("Input params: CLOSING_DAY={} PORT_MONTHS_LENGTH={}".format(CLOSING_DAY, PORT_MONTHS_LENGTH))

    import time
    start_time = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # SPARK
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn.utils.general_functions import init_spark
    spark = init_spark("generate_table_extra_feats")
    sc = spark.sparkContext

    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles, move_date_n_yearmonths

    start_port = move_date_n_days(move_date_n_cycles(CLOSING_DAY, -4 * PORT_MONTHS_LENGTH), 1)
    end_port = CLOSING_DAY

    ccc_start = start_port
    ccc_end = move_date_n_cycles(end_port, -4)

    tgs_yyyymm = move_date_n_yearmonths(start_port[:6], -1)
    tgs_closing_day = move_date_n_days(ccc_start, -1)

    base_closing_day = move_date_n_days(ccc_start, -1)

    TRIGGER_LIST = ["FACTURA", "PERMS_DCTOS", "INFO", "INFO_COMP", "INFO_NOCOMP", "UCI", "ABONOS", "CCC"]
    #DAYS_EOP_LIST = [15, 30, 45, 60]


    from churn.analysis.ccc_churn.engine.data_loader import get_ccc_data, get_tgs, get_all_ports
    from churn.datapreparation.general.data_loader import get_active_services
    from churn.analysis.ccc_churn.engine.reporter import compute_results, SAVING_PATH, init_writer, print_sheet

    print("Ports   start={}   end={}".format(start_port, end_port))
    print("CCC     start={}   end={}".format(ccc_start, ccc_end))
    print("TGs    yyyymm={}   closing_day={}".format(tgs_yyyymm, tgs_closing_day))
    print("BASE closing_day={}".format(base_closing_day))
    #print("BY  = {}".format(BY))


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # P O R T O U T S
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    df_port = get_all_ports(spark, start_port, end_port, base_closing_day)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # C  C  C
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    from pykhaos.utils.date_functions import move_date_n_days
    df_ccc = get_ccc_data(spark, ccc_end, ccc_start)


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # T G s
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    df_tgs = get_tgs(spark, tgs_closing_day, tgs_yyyymm)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ACTIVE SERVICES
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    df_mobile_services = (get_active_services(spark, base_closing_day, new=False,
                                              service_cols=["msisdn", "num_cliente", "campo2", "rgu", "srv_basic",
                                                            "campo1"],
                                              customer_cols=["num_cliente", "nif_cliente"])
                          .withColumnRenamed("num_cliente_service", "num_cliente")
                          .where(col("rgu").rlike("^mobile$|^movil$")))
    df_mobile_services = df_mobile_services.drop_duplicates(["msisdn"])


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # JOIN DFS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    df_base_tgs_port_ccc = join_dfs(df_mobile_services, df_ccc, df_tgs, df_port, base_closing_day)

    df_base_tgs_port_ccc = (df_base_tgs_port_ccc.withColumn("days_from_endcccperiod_to_enddto", when(col("tgs_fecha_fin_dto").isNotNull(),
                                                                     datediff(
                                                                         from_unixtime(unix_timestamp(lit(ccc_end),"yyyyMMdd")),
                                                                         from_unixtime(unix_timestamp("tgs_fecha_fin_dto", "yyyyMMdd"))
                                                                   ).cast("double")).otherwise(None)))

    # EOP=1 if eod within ccc range
    df_base_tgs_port_ccc = (
        df_base_tgs_port_ccc.withColumn("EOP", when(col("days_from_endcccperiod_to_enddto").isNull(), 0)
                                              .when(col("days_from_endcccperiod_to_enddto") >= 0, 1).otherwise(0)))


    df_base_tgs_port_ccc = df_base_tgs_port_ccc.fillna(0, subset=["EOP", "CHURN", "TRIGGER"])


    #df_list = []
    import pandas as pd

    UNIQUE_DIRNAME = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    final_filename = os.path.join(SAVING_PATH,
                                  "reporter_c{}_m{}{}.xlsx".format(CLOSING_DAY, PORT_MONTHS_LENGTH,
                                                                        UNIQUE_DIRNAME))

    writer = init_writer(final_filename)
    workbook = writer.book
    worksheet = workbook.add_worksheet("SUMMARY")
    writer.sheets['SUMMARY'] = worksheet

    df_list_all = []
    df_list_eop = []

    for TRIGGER in TRIGGER_LIST:

        logger.info("TRIGGER = {}".format(TRIGGER))

        if TRIGGER == "FACTURA":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_FACTURA"))
        elif TRIGGER == "PERMS_DCTOS":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_PERMANENCIA_DCTOS"))
        elif TRIGGER == "INFO":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_INFO"))
        elif TRIGGER == "INFO_NOCOMP":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_INFO_NOCOMP"))
        elif TRIGGER == "INFO_COMP":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_INFO_COMP"))
        elif TRIGGER == "CCC":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("CCC"))
        elif TRIGGER == "UCI":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_UCI"))
        elif TRIGGER == "ABONOS":
            df_base_tgs_port_ccc = df_base_tgs_port_ccc.withColumn("TRIGGER", col("TIPIS_ABONOS"))
        else:
            print("TRIGGER {} format does not exist".format(TRIGGER))
            import sys
            sys.exit()



        content, df_churn, df_churn_eop = compute_results(df_base_tgs_port_ccc)

        df = pd.DataFrame(content, columns=content.keys(), index=["count"]).T

        df_churn["BY"] = "MSISDN"
        df_churn["TRIGGER NAME"] = TRIGGER
        df_churn_eop["BY"] = "MSISDN"
        df_churn_eop["TRIGGER NAME"] = TRIGGER

        df_list_all.append(df_churn)
        df_list_eop.append(df_churn_eop)

        BY="num_cliente"
        logger.info("Computing aggregates by {}".format(BY))
        df_base_tgs_port_ccc_agg = df_base_tgs_port_ccc.groupby("num_cliente").agg( (when(sql_sum(col("EOP"))>0,1).otherwise(0)).alias("EOP"),
                                                         (when(sql_sum(col("CHURN")) > 0, 1).otherwise(0)).alias("CHURN"),
                                                         (when(sql_sum(col("TRIGGER")) > 0, 1).otherwise(0)).alias("TRIGGER"),
                                                         (sql_count("msisdn").alias("num_services"))
                                                         )


        logger.info("Ended computation of aggregates by {}".format(BY))
        content_nc, df_churn_nc, df_churn_eop_nc = compute_results(df_base_tgs_port_ccc_agg)
        df_nc = pd.DataFrame(content, columns=content_nc.keys(), index=["count"]).T

        df_churn_nc["BY"] = "NUM_CLIENTE"
        df_churn_nc["TRIGGER NAME"] = TRIGGER
        df_churn_eop_nc["BY"] = "NUM_CLIENTE"
        df_churn_eop_nc["TRIGGER NAME"] = TRIGGER

        df_list_all.append(df_churn_nc)
        df_list_eop.append(df_churn_eop_nc)


        sheet = "{}".format(TRIGGER)
        print_sheet(sheet, df, writer)
        logger.info("Ended sheet - {}".format(sheet))
        # - -
        sheet = "NUMCLIENTE_{}".format(TRIGGER)
        print_sheet(sheet, df_nc, writer)
        logger.info("Ended sheet - {}".format(sheet))

    df_all_concat = pd.concat(df_list_all, ignore_index=True, sort=False)
    df_all_concat = df_all_concat.reindex(columns=["BY", "TRIGGER NAME", "TRIGGER", "NO TRIGGER", "LIFT"])
    df_all_concat.sort_values(by=["BY", "TRIGGER NAME"], ascending=[True,True], inplace=True)

    df_eop_concat = pd.concat(df_list_eop, ignore_index=True, sort=False)
    df_eop_concat = df_eop_concat.reindex(columns=["BY", "TRIGGER NAME", "TRIGGER", "NO TRIGGER", "LIFT"])
    df_eop_concat.sort_values(by=["BY", "TRIGGER NAME"], ascending=[True,True], inplace=True)

    df_all_concat.to_excel(writer, sheet_name="SUMMARY", startrow = 7, startcol=0, index=False, header=True)
    df_eop_concat.to_excel(writer, sheet_name="SUMMARY", startrow = 7+df_all_concat.shape[0]+5, startcol=0, index=False, header=True)


    writer.close()
    logger.info("Saved in '{}'".format(final_filename))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # FINISHED
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    logger.info("Process finished - {} minutes".format( (time.time()-start_time)/60.0))

    logger.info("Process ended successfully. Enjoy :)")


# days_to_port_filt = df_base_tgs_port_ccc\
# .filter(col('SOPO')==1)\
# .filter(col("TRIGGER")==1)\
# .filter(col("VALID_CCC_SOPO")==1)\
# .select('days_from_ccc_to_sopo')\
# .rdd\
# .map(lambda r:r['days_from_ccc_to_sopo'])\
# .collect()

# import matplotlib.pyplot as plt
# import numpy as np
#
# days_to_port_np_filt = np.asarray(days_to_port_filt)
# # the histogram of the data
# n, bins, patches = plt.hist(days_to_port_np_filt,30, density=True,facecolor='g',alpha=0.75)
# plt.xlabel('Days to port-out request since first call')
# plt.ylabel('Probability')
# plt.title('Distribution of the number of days to port-out request')
# plt.grid(True)
# plt.show()
