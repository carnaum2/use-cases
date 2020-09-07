from pykhaos.utils.date_functions import (get_last_day_of_month, get_next_month)


from pyspark.sql.functions import (udf,col,max as sql_max, when, isnull, concat, lpad, trim, lit, sum as sql_sum, length, upper)
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

exclude_srv_basic = ["MPSUI", "MRSUI", "MVSUS", "MVSUI"]

cols_campaigns    = ["msisdn", "prepaid_services", "movil_services", "srv_basic",
                          "camp_srv_retention_voice_tel_target_0",    "camp_srv_retention_voice_tel_target_1",
                          "camp_srv_retention_voice_tel_universal_0", "camp_srv_retention_voice_tel_universal_1",
                          "camp_srv_retention_voice_tel_control_0",   "camp_srv_retention_voice_tel_control_1"]

dict_df_pospago = {}




def get_data(spark, reference_month, only_msisdn=False):

    logger.info("\t\tAMDOCS - Getting postpaid data from month={}".format(reference_month))


    cols_to_select = cols_campaigns if not only_msisdn else ["msisdn"]

    # df_pospago_amdocs = (spark.read.table("tests_es.amdocs_ids_srv_v3")
    #                      .where(col("closingday" )==get_last_day_of_month(reference_month +"01").replace("-" ,""))
    #                      .where(col("rgu") == "movil")
    #                      .where(col("cod_estado_general").isin(["01" ,"09"]))
    #                      .where(col("clase_cli_cod_clase_cliente") == "RS")
    #                      .where(~col("srv_basic").isin(exclude_srv_basic))
    #                      .select(cols_to_select)
    #                      .na.drop()
    #                      ).withColumnRenamed("msisdn", "MSISDN")

    last_day = get_last_day_of_month(reference_month +"01").replace("-" ,"")
    print("In pospago: getting {}".format(last_day))

    table_name = '/data/udf/vf_es/amdocs_ids/service/year={}/month={}/day={}'.format(int(last_day[:4]),
                                                                           int(last_day[4:6]),
                                                                           int(last_day[6:]))
    print(table_name)
    df_pospago_amdocs_service = spark.read.load(table_name)

    table_name = '/data/udf/vf_es/amdocs_ids/customer/year={}/month={}/day={}'.format(int(last_day[:4]),
                                int(last_day[4:6]),
                                int(last_day[6:]))
    print(table_name)
    df_pospago_amdocs_customer = spark.read.load(table_name)

    df_pospago_amdocs = df_pospago_amdocs_service.join(df_pospago_amdocs_customer,
                                                       on=df_pospago_amdocs_service["num_cliente"]==df_pospago_amdocs_customer["num_cliente"])

    df_pospago_amdocs = (df_pospago_amdocs
                         #.where(col("closingday" )==get_last_day_of_month(reference_month +"01").replace("-" ,""))
                         .where(col("rgu").isin(["movil", "mobile"]))
                         .where(col("cod_estado_general").isin(["01" ,"09"]))
                         .where(col("clase_cli_cod_clase_cliente") == "RS")
                         .where(~col("srv_basic").isin(exclude_srv_basic))
                         .select(cols_to_select)
                         .na.drop()
                         ).withColumnRenamed("msisdn", "MSISDN")

    ### https://gitlab.rat.bdp.vodafone.com/rbuendi1/amdocs_informational_dataset/issues/2
    df_pospago_amdocs = df_pospago_amdocs.withColumn("MSISDN", trim(col("MSISDN")))

    return df_pospago_amdocs