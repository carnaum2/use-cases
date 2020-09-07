# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc
from pyspark.sql.utils import AnalysisException
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

def check_sql(spark, sql_query):
    try:
        df = spark.sql(sql_query)
        if df.take(1):
            return True
        else:
            return False
    except:
        print("check_sql '{}' exception".format(sql_query))
        return False


def check_car(spark, closing_day, verbose=False):
    if logger: logger.info("STEP CAR ) checking started...")
    everything_ok = True

    modules2generate_dict = {}
    non_existing_modules = []
    my_msg_other_errors_modules = []

    for mod_name in ["customer", "service", 'usage_geneva_voice', 'usage_geneva_data', 'billing', 'campaigns_customer',
                     'campaigns_service', 'usage_geneva_roam_voice', 'usage_geneva_roam_data', 'orders', 'orders_agg',
                     'penalties_customer', 'penalties_service', 'device_catalogue', 'call_centre_calls',
                     'tnps', 'customer_agg', 'perms_and_prefs', 'netscout_apps']:
        try:
            from churn.utils.general_functions import amdocs_table_reader
            amdocs_table_reader(spark, mod_name, closing_day, new=False, verbose=False)
            modules2generate_dict[mod_name] = False

        except AnalysisException as e:
            if "Path does not exist" in str(e):
                non_existing_modules.append(mod_name)
            else:
                my_msg_other_errors_modules.append("   MODULE '{}': {}".format(mod_name, str(e)))
            everything_ok = False
            modules2generate_dict[mod_name] = True

    my_msg, perc_completed = _format_msg(modules2generate_dict, non_existing_modules, my_msg_other_errors_modules)

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, modules2generate_dict, perc_completed


def check_join_car(spark, closing_day, verbose):
    everything_ok = True
    if logger: logger.info("STEP JOIN_CAR ) checking started...")
    join_car_path = "tests_es.jvmm_amdocs_ids_" + closing_day
    try:
        spark.read.table(join_car_path)
        my_msg = "   INFO - JOIN_CAR '{}' exists".format(join_car_path)

    except AnalysisException as e:
        my_msg = "   ERROR - JOIN_CAR. Exception: {}".format(str(e).split(";;")[0].replace("`", ""))
        everything_ok = False

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg


def _format_msg(modules2generate_dict, non_existing_modules, my_msg_other_errors_modules):
    my_msg = my_msg_nok = []
    existing_modules = [mod_name for mod_name, is_pending in modules2generate_dict.items() if not is_pending]
    perc_completed = int(100.0*len(existing_modules)/len(modules2generate_dict))

    existing_modules = ", ".join(existing_modules)
    non_existing_modules = ", ".join(non_existing_modules)

    from textwrap import wrap
    text_ok = "| OK - MODULES READY: "
    text_nok = "| FAILED - MODULES DOES NOT EXIST: "

    if existing_modules:
        my_msg = wrap(existing_modules, initial_indent="",
                      subsequent_indent="|" + " " * (len(text_ok) - 1),
                      replace_whitespace=False,
                      drop_whitespace=False)
        my_msg[0] = text_ok + my_msg[0]

    if non_existing_modules:
        my_msg_nok = wrap(non_existing_modules, initial_indent="",
                          subsequent_indent="|" + " " * (len(text_nok) - 1),
                          replace_whitespace=False,
                          drop_whitespace=False)
        my_msg_nok[0] = text_nok + my_msg_nok[0]

    if my_msg_other_errors_modules:
        my_msg_other_errors_modules = ["| FAILED - MODULES RAISED ERROR: {}".format(msg) for msg in my_msg_other_errors_modules]

    my_msg = my_msg + my_msg_nok + my_msg_other_errors_modules

    return my_msg, perc_completed


def check_extra_feats(spark, closing_day, metadata_version=None, verbose=True):
    if logger: logger.info("STEP EXTRA_FEATS ) checking started...")

    everything_ok = True

    modules2generate_dict = {}
    non_existing_modules = []
    my_msg_other_errors_modules = []

    from churn.datapreparation.app.generate_table_extra_feats_new import get_extra_feats_module_path, MODULE_JOIN, get_modules_list_by_version


    if not metadata_version:
        metadata_version = "1.1"

    module_names_list =  get_modules_list_by_version(metadata_version)

    module_names_list.append(MODULE_JOIN)

    if logger: logger.info("   | Check will include following modules: {}".format(",".join(module_names_list)))

    for mod_name in module_names_list:
        try:
            extra_feats_path = get_extra_feats_module_path(closing_day, mod_name)
            spark.read.load(extra_feats_path)
            #my_msg.append("   INFO  - MODULE '{}' exists".format(mod_name))
            modules2generate_dict[mod_name] = False

        except AnalysisException as e:
            if "Path does not exist" in str(e):
                non_existing_modules.append(mod_name)
            else:
                my_msg_other_errors_modules.append("'{}': {}".format(mod_name, str(e)))

            everything_ok = False
            modules2generate_dict[mod_name] = True

    my_msg, perc_completed = _format_msg(modules2generate_dict, non_existing_modules, my_msg_other_errors_modules)

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, modules2generate_dict, perc_completed

def check_car_preparado(spark, closing_day, verbose=True):
    everything_ok = True
    if logger: logger.info("STEP CAR PREPARADO ) checking started...")
    car_preparado_path = "tests_es.jvmm_amdocs_prepared_car_mobile_complete_" + closing_day
    try:
        spark.read.table(car_preparado_path)
        my_msg = "   INFO - CAR PREPARADO '{}' exists".format(car_preparado_path)

    except AnalysisException as e:
        my_msg = "   ERROR - CAR PREPARADO. Exception: {}".format(str(e).split(";;")[0].replace("`", ""))
        everything_ok = False

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg


# def check_scores(spark, closing_day, segment=None, verbose=True):
#     if logger and verbose: logger.info("STEP CHECK SCORES {}) checking started...".format("all" if not segment else segment))
#
#     df = spark.read.table("tests_es.jvmm_amdocs_automated_churn_scores").where(
#         col("pred_name").rlike("for" + closing_day))
#     if segment:
#         df = df.where(col("segment") == segment)
#
#     pred_names = df.select("pred_name").distinct().rdd.map(lambda pp: pp[0]).collect()
#
#     everything_ok = True if pred_names else False
#
#     if everything_ok:
#         my_msg = "   INFO - CHECK_SCORES segment={}: {}".format("all" if not segment else segment,
#                                                                 "Found pred_names {}".format(",".join(pred_names)))
#     else:
#         my_msg = "   ERROR - CHECK_SCORES segment={}: {}".format("all" if not segment else segment,
#                                                                               "Not found any pred_names for closing_day={}".format(closing_day))
#
#     if logger and verbose: logger.info(my_msg)
#
#     return everything_ok, my_msg


def check_scores_new(spark, closing_day, segment, tr_cycle_ini, tr_cycle_end, verbose=True):
    # Look for entries for this closing day. Take the list of model_executed_at
    # These entries should be cross with the model_parameters table, to check
    if logger: logger.info("STEP SCORES {} ) checking started...".format(segment))



    sql_query = "select msisdn, scoring, model_executed_at from parquet.`/data/attributes/vf_es/model_outputs/model_scores/model_name={}` where predict_closing_date = '{}'".format(
        "churn_preds_" + segment, closing_day)

    exe_list = None
    sql_scores_query = None
    df = spark.sql(sql_query)
    if df.take(1):
        # print("True '{}'".format(",'".join(df.select("model_executed_at").distinct().rdd.map(lambda xx: xx[0]).collect())))
        exe_list = df.select("model_executed_at").distinct().rdd.map(lambda xx: xx[0]).collect()
        everything_ok = True
    else:
        everything_ok = False


    if everything_ok == True:
        if tr_cycle_ini and tr_cycle_end:
            sql_query = "select * from parquet.`/data/attributes/vf_es/model_outputs/model_parameters/model_name={}` where executed_at in ('{}') and training_closing_date='{}' sort by executed_at".format(
                "churn_preds_" + segment,
                "','".join(exe_list),
                tr_cycle_ini + "to" + tr_cycle_end)
        else:
            sql_query = "select * from parquet.`/data/attributes/vf_es/model_outputs/model_parameters/model_name={}` where executed_at in ('{}') sort by executed_at".format(
                "churn_preds_" + segment,
                "','".join(exe_list),
                )

        df_param = spark.sql(sql_query)


        if df_param.take(1):
            everything_ok = True
            my_msg = "ERROR - undefined"

            if not tr_cycle_ini and not tr_cycle_end:
                mydict = df_param.take(1)[0].asDict()
                training_closing_date = mydict["training_closing_date"]
                tr_cycle_ini = training_closing_date[:8]
                tr_cycle_end = training_closing_date[10:]

            if tr_cycle_ini and tr_cycle_end:
                my_msg =  "   INFO - SCORES segment={}|closing_day={}|tr_ini={}|tr_end={}: Found scores for executed_at '{}'".format(segment,
                                                                                                                                   closing_day,
                                                                                                                                   tr_cycle_ini,
                                                                                                                                   tr_cycle_end,
                                                        ",'".join(df_param.select("executed_at").distinct().rdd.map(lambda xx: xx[0]).collect()))

            executed_at = df_param.select("executed_at").sort(desc("executed_at")).limit(1).collect()[0].asDict()["executed_at"]

            sql_scores_query = "select msisdn, scoring, model_executed_at from parquet.`/data/attributes/vf_es/model_outputs/model_scores/model_name={}` where predict_closing_date = '{}' and model_executed_at='{}'".format(
                "churn_preds_" + segment,
                closing_day,
                executed_at)

            #if logger and verbose: logger.info(my_msg)
            return everything_ok, my_msg, sql_scores_query


    everything_ok = False
    my_msg = "   ERROR - SCORES segment={}. Not found any scores for closing_day={} tr_ini={} tr_end={}".format(segment,
                                                                                                                  closing_day,
                                                                                                                  tr_cycle_ini,
                                                                                                                  tr_cycle_end)

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, sql_scores_query


def check_levers_model(spark, closing_day, n=60):

    dp_levers_ok = True
    dp_levers_msg = []
    dp_levers_dict = {}
    dp_levers_perc = 0
    for segment in ["onlymob", "mobileandfbb"]:
        hdfs_path =  "/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c{}_n{}_comercial_msisdn_{}".format(closing_day, n, segment)
        try:
            spark.read.option("delimeter", "|").option("header", True).csv(hdfs_path)
            dp_levers_dict[segment] = False # False means the next process has not to run it
            dp_levers_perc += 50
        except:
            dp_levers_msg.append("Missing '{}'".format(hdfs_path))
            dp_levers_ok = False
            dp_levers_dict[segment] = True # True means the next process has to run it


    dp_levers_msg.insert(0, "INFO - DP ready" if dp_levers_ok else "ERROR - Missing file(s):")

    return dp_levers_ok, dp_levers_msg, dp_levers_dict, dp_levers_perc


# def check_levers_model(closing_day, n=60):
#     hdfs_path_onlymob = "/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c{}_n{}_comercial_msisdn_{}".format(closing_day, n, "onlymob")
#     hdfs_path_mobileandfbb = "/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c{}_n{}_comercial_msisdn_{}".format(closing_day, n, "mobileandfbb")
#
#     from pykhaos.utils.hdfs_functions import check_hdfs_exists
#     dp_levers_ok =  all([check_hdfs_exists(hdfs_path_onlymob), check_hdfs_exists(hdfs_path_mobileandfbb)])
#     dp_levers_msg =  "INFO - DP ready" if dp_levers_ok else "ERROR - Missing files '/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c{}_n{}_comercial_msisdn_{}'".format(closing_day, n, "<segment>")
#     return dp_levers_ok, dp_levers_msg