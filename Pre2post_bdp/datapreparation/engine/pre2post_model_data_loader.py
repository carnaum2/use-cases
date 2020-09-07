

from pyspark.sql.functions import col
import Pre2post_bdp.datapreparation.engine.dp_amdocs as amdocs
import Pre2post_bdp.datapreparation.engine.dp_campaign_msisdncontacthist as campaign
import Pre2post_bdp.datapreparation.engine.dp_vf_pre_ac_final as vf_pre_ac_final
import Pre2post_bdp.datapreparation.engine.dp_vf_pre_info_tariff as vf_pre_info_tariff
from pyspark.sql.types import IntegerType

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

import sys
from Pre2post_bdp.utils.constants import *
from pykhaos.utils.date_functions import (get_last_day_of_month, get_next_month, months_range)
from pykhaos.utils.pyspark_utils import union_all

def get_labeled_or_unlabeled_car(spark, input_data, key=JSON_DATA_PREPARATION, logger=None):

    prepago_start_yearmonth = input_data[key][JSON_DATA_PREPAGO][0]
    prepago_end_yearmonth = input_data[key][JSON_DATA_PREPAGO][1] if len(input_data[key][JSON_DATA_PREPAGO])>1 else prepago_start_yearmonth
    pospago_month_tag = input_data[key][JSON_DATA_POSPAGO_TAG]
    classical_campaign_msisdn_hist = True # input_data[key][JSON_DATA_CLASSICAL_CAMP_HIST]
    num_samples = input_data[key][JSON_NUM_SAMPLES] if input_data[key][JSON_NUM_SAMPLES] != None else "complete"
    labeled = input_data[key][JSON_LABELED]
    yearmonths_list = months_range(prepago_start_yearmonth, prepago_end_yearmonth)

    df_all_list = []


    for prepago_yearmonth in yearmonths_list:

        logger.info("GET_DATA_LOOP - {}".format(prepago_yearmonth))

        df_month = get_data(spark, prepago_yearmonth, pospago_month_tag,
                                                classical_campaign_msisdn_hist, num_samples, labeled)

        df_all_list.append(df_month)

    if len(df_all_list) == 0:
        logger.info("df_all_list = 0")
        return None

    logger.info('GET_DATA returned {} complete months'.format(len(df_all_list)))
    df_all = union_all(df_all_list)
    return df_all


def get_data(spark, dd_prepago, pospago_month_tag, classical_campaign_msisdn_hist, num_samples, labeled=True):
    '''

    :param dd_prepago:
    :param pospago_month_tag:
    :param classical_campaign_msisdn_hist:
    :param num_samples:
    :param predict: when predict=True, no matter if pospago AMDOCS is available or not for the target column
    :return:
    '''
    logger.info("\t prepago={} | pospago_tag={}  | classical_camp={} | num_samples={}".format(dd_prepago,
                                                                                              pospago_month_tag,
                                                                                              classical_campaign_msisdn_hist,
                                                                                              num_samples))

    CAMPAIGN_TAG = CAMPAIGN_HIST_TABLE_TAG if classical_campaign_msisdn_hist else CAMPAIGN_NEW_CAR_TABLE_TAG
    columns_to_drop = []

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # P R E P A G O

    logger.debug("\tGetting prepaid - {}".format(dd_prepago))

    df_prepago = vf_pre_ac_final.get_data(spark, dd_prepago,
                                          num_samples)

    if len(df_prepago.take(1)) == 0:
        logger.warning("\t !!!! No data in prepaid {}".format(dd_prepago))
        sys.exit()

    prepago_columns = [(_column, PREPAID_TABLE_TAG + _column) for _column in df_prepago.columns]

    for existing, new in prepago_columns:
        df_prepago = df_prepago.withColumnRenamed(existing, new)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # T A R I F F S
    logger.debug("\tGetting tariffs - {}".format(dd_prepago))

    df_tarifas = vf_pre_info_tariff.get_data(spark, dd_prepago)
    df_tarifas = df_tarifas.withColumnRenamed("msisdn", "msisdn_" + dd_prepago)

    if len(df_tarifas.take(1)) == 0:
        logger.warning("\t !!!! No data in tariffs {}".format(dd_prepago))
        sys.exit()

    tarifas_columns = [(_column, TARIFFS_TABLE_TAG + _column) for _column in df_tarifas.columns]

    for existing, new in tarifas_columns:
        df_tarifas = df_tarifas.withColumnRenamed(existing, new)

    df_join = (df_prepago.join(df_tarifas,
                               how="left",
                               on=df_prepago[PREPAID_TABLE_TAG + "msisdn"] == df_tarifas[
                                   TARIFFS_TABLE_TAG + "msisdn_" + dd_prepago]))

    columns_to_drop += [TARIFFS_TABLE_TAG + "msisdn_" + dd_prepago]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # P O S P A G O

    if labeled:
        if pospago_month_tag == "M1":
            pospago_month_list = [get_next_month(dd_prepago)]
        elif pospago_month_tag == "M2":
            pospago_month_list = [get_next_month(get_next_month(dd_prepago))]
        elif pospago_month_tag == "M1M2":
            pospago_month_list = [get_next_month(dd_prepago), get_next_month(get_next_month(dd_prepago))]
        else:
            logger.error("Nos implemented pospago tag {}".format(pospago_month_tag))
            sys.exit()
    else:
        pospago_month_list = []

    for dd_pospago in pospago_month_list:

        logger.info("\t Getting postpaid - only_msisdn {}".format(dd_pospago))

        df_pospago = amdocs.get_data(spark, dd_pospago, only_msisdn=True) # only target variable

        if len(df_pospago.take(1)) == 0:
            logger.warning("\t !!!! No data in AMDOCS {}".format(dd_pospago))
            sys.exit()
        else:
            pospago_columns = [(_column, _column + "_" + dd_pospago) for _column in df_pospago.columns]

            for existing, new in pospago_columns:
                df_pospago = df_pospago.withColumnRenamed(existing, new)

            df_join = ((df_join
                        .join(df_pospago,
                              how="left",
                              on=(df_prepago[PREPAID_TABLE_TAG + "msisdn"] == df_pospago["msisdn_" + dd_pospago])
                              )
                        ).withColumn("in_pospago_{}".format(dd_pospago), (~col("msisdn_" + dd_pospago).isNull())))
            columns_to_drop.append("msisdn_" + dd_pospago)

    # if pospago_month_tag == "M1M2":
    #     df_join = df_join.withColumn("migrated_to_postpaid", (
    #             col("in_pospago_{}".format(pospago_month_list[0])) | col(
    #         "in_pospago_{}".format(pospago_month_list[1]))).cast(
    #         IntegerType()))
    #     logger.info("migrated_to_postpaid = {}|{}".format(pospago_month_list[0], pospago_month_list[1]))

    # elif num_pospago_col>=1:
    if labeled:
        df_join = df_join.withColumn("migrated_to_postpaid", (
            col("in_pospago_{}".format(pospago_month_list[0])).cast(IntegerType())))
        logger.info("\t\tmigrated_to_postpaid = {}".format(pospago_month_list[0]))
    else:
        logger.info("\t\tmigrated_to_postpaid not set - unlabeled car asked!!!")


    try:
        df_join = df_join.drop(*columns_to_drop)
    except Exception as e:
        logger.error("Impossible to drop columns: {}".format(",".join(columns_to_drop)))
        print(e)
        sys.exit()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # C A M P A I G N S

    # Take campaigns from classical campaign msisdn historic
    if classical_campaign_msisdn_hist:
        logger.debug("\tGetting campaign info from classical campaign msisdn hist")
        #campaign_end = dd_prepago[:4] + "-" + dd_prepago[4:6] + "-01 00:00:00"
        from pykhaos.utils.date_functions import move_date_n_days
        campaign_end = get_last_day_of_month(dd_prepago + "01")
        campaign_start = move_date_n_days(campaign_end,-60)
        campaign_start = campaign_start[:4] + "-" + campaign_start[4:6] + "-" + campaign_start[6:8] + " 00:00:00"
        campaign_end = campaign_end[:4] + "-" + campaign_end[4:6] + "-" + campaign_end[6:8] + " 00:00:00"


        df_contacts_responses = campaign.get_contacts_and_responses(spark, campaign_start, campaign_end)

        if len(df_contacts_responses.take(1)) == 0:
            logger.warning("\t !!!! No data in campaigns history {} {}".format(campaign_start, campaign_end))
            sys.exit()

    # Take campaigns from new CAR
    else:
        logger.info("\tGetting campaign info from new car - using month={}".format(dd_prepago))
        df_pospago_campaigns = amdocs.get_data(spark, dd_prepago, only_msisdn=False) # campaign info

        if len(df_pospago_campaigns.take(1)) == 0:
            logger.warning("\t !!!! No data in amdocs campaigns history {}".format(dd_prepago))
            sys.exit()

        cols_to_take = [_col for _col in df_pospago_campaigns.columns if
                        _col.startswith("camp_srv_retention_voice_tel_")] + [
                           "msisdn"]

        df_contacts_responses = (df_pospago_campaigns.select(cols_to_take)
                                 .withColumnRenamed("msisdn", "msisdn_contact"))

    responses_columns = [(_column, CAMPAIGN_TAG + _column) for _column in df_contacts_responses.columns]

    for existing, new in responses_columns:
        df_contacts_responses = df_contacts_responses.withColumnRenamed(existing, new)

    df_all = (df_join.join(df_contacts_responses,
                           how="left",
                           on=df_join[PREPAID_TABLE_TAG + "msisdn"] == df_contacts_responses[
                               CAMPAIGN_TAG + "msisdn_contact"]
                           )
              )
    columns_to_drop += [CAMPAIGN_TAG + "msisdn_contact"]

    for col_ in columns_to_drop:
        df_all = df_all.drop(col_)

    fill_zero = ["campaign_EsRespondedor", "tariffs_TOTAL_LLAMADAS", "tariffs_Num_accesos",
                  "tariffs_MOU_Week", "tariffs_LLAM_Week", "tariffs_MOU", "tariffs_TOTAL_SMS",
                  "tariffs_SMS_Week", "tariffs_MOU_Weekend", "tariffs_LLAM_Weekend", "tariffs_SMS_Weekend",
                  "tariffs_MOU_VF", "tariffs_LLAM_VF", "tariffs_SMS_VF", "tariffs_MOU_Fijo",
                  "tariffs_LLAM_Fijo", "tariffs_SMS_Fijo", "tariffs_MOU_OOM", "tariffs_LLAM_OOM",
                  "tariffs_SMS_OOM", "tariffs_MOU_Internacional", "tariffs_LLAM_Internacional",
                  "tariffs_SMS_Internacional", "tariffs_ActualVolume", "tariffs_Num_accesos",
                  "tariffs_Num_Cambio_Planes", "tariffs_LLAM_COMUNIDAD_SMART",
                  "tariffs_MOU_COMUNIDAD_SMART", "tariffs_LLAM_SMS_COMUNIDAD_SMART",
                  "tariffs_Flag_Uso_Etnica", "tariffs_cuota_SMART8", "tariffs_cuota_SMART12",
                  "tariffs_cuota_SMART16"]
    for col_ in fill_zero:
        df_all = df_all.withColumn(col_, col(col_).cast("float"))
        df_all = df_all.fillna(0, subset=[col_])

    fill_unknown = ["tariffs_Plan", "prepaid_tipo_documento_comprador", "campaign_CAMPAIGNCODE",
                    "campaign_CREATIVIDAD", "campaign_CELLCODE", "campaign_CANAL"]
    for col_ in fill_unknown:
        df_all = df_all.withColumn(col_, col(col_).cast("string"))
        df_all = df_all.fillna("UNKNOWN", subset=[col_])

    fill_minus1 = ["prepaid_x_fecha_nacimiento", "campaign_EsRespondedor", "prepaid_days_since_fx_1llamada", "campaign_days_since_DATEID"]
    for col_ in fill_minus1:
        if col_ in df_all.columns:
            df_all = df_all.withColumn(col_, col(col_).cast("float"))
            df_all = df_all.fillna(-1, subset=[col_])

    # df_all.show(15, False)
    logger.info("df_all ready!")
    logger.info(df_all.columns)

    print(df_all.select("prepaid_tipo_documento_comprador").distinct().rdd.collect())

    return df_all



def save_results(df, input_data, csv=True, logger=None):
    '''
    This function save the df and the yaml's (internal and user)
    :param df:
    :param path_filename:
    :return:
    '''
    storage_dir = build_storage_dir_name_obj(input_data)
    from pykhaos.utils.hdfs_functions import save_df
    if logger: logger.info("Starting saving df in '{}' format {}".format(storage_dir, "csv" if csv else "parquet"))
    if df:
        save_df(df, storage_dir, csv)
        if logger: logger.info("Saved df in '{}' format {}".format(storage_dir, "csv" if csv else "parquet"))

    from Pre2post_bdp.utils.constants import YAML_FILES_DIR
    from pykhaos.utils.hdfs_functions import create_directory
    #create directory in hdfs to store config files
    yaml_dir = os.path.join(storage_dir, YAML_FILES_DIR)
    create_directory(yaml_dir)
    from pykhaos.utils.hdfs_functions import move_local_file_to_hdfs
    from Pre2post_bdp.utils.constants import JSON_USER_CONFIG_FILE
    user_config_filename =  input_data[JSON_USER_CONFIG_FILE]
    if logger: logger.info("Moving config file '{}' to hdfs dir '{}'".format(user_config_filename, yaml_dir))
    move_local_file_to_hdfs(yaml_dir, user_config_filename)
    return storage_dir





def build_storage_dir_name_obj(input_data, key=JSON_DATA_PREPARATION):
    prepago_start_yearmonth = input_data[key][JSON_DATA_PREPAGO][0]

    if JSON_LATEST_MONTH_COMPLETE in input_data[key].keys():
        prepago_end_yearmonth = input_data[key][JSON_LATEST_MONTH_COMPLETE]
    elif len(input_data[key][JSON_DATA_PREPAGO]) == 1:  # it is not a range, but a single month
        prepago_end_yearmonth = input_data[key][JSON_DATA_PREPAGO][0]
    else:
        prepago_end_yearmonth = input_data[key][JSON_DATA_PREPAGO][1]

    pospago_month_tag = input_data[key][JSON_DATA_POSPAGO_TAG]
    classical_campaign_msisdn_hist = True #input_data[key][JSON_DATA_CLASSICAL_CAMP_HIST]
    num_samples = input_data[key][JSON_NUM_SAMPLES] if input_data[key][
                                                                JSON_NUM_SAMPLES] != None else "complete"
    labeled = input_data[key][JSON_LABELED]

    if not labeled:
        root_dir = os.path.join(HDFS_DIR_DATA,"unlabeled")
    else:
        root_dir = HDFS_DIR_DATA

    filename_df_all = os.path.join(root_dir, 'df_all_{}_{}_{}_{}_{}'.format(
                                                        prepago_start_yearmonth,
                                                        prepago_end_yearmonth,
                                                        pospago_month_tag,
                                                        "classical" if classical_campaign_msisdn_hist else "newcar",
                                                        num_samples))
    # if add_hdfs_preffix:
    #     filename_df_all = "hdfs://" + filename_df_all


    return filename_df_all



