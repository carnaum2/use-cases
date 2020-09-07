# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc, count as sql_count
from pyspark.sql.utils import AnalysisException
import pykhaos.utils.custom_logger as clogger
from churn.datapreparation.general.model_outputs_manager import get_complete_scores_model_name_closing_day
from churn.delivery.delivery_constants import DELIVERY_TABLE_PREPARED
logger = clogger.get_custom_logger()
from pykhaos.utils.date_functions import get_next_dow


class checker:
    status = False
    msg = "undefined"
    extra_info = {}
    perc_completed = -1
    spark = None
    custom_check_func = None
    custom_run_func = None
    custom_format_func = None
    closing_day = None
    section_name = None

    def __init__(self, spark, custom_check_func, custom_format_func, closing_day, section_name, custom_run_func=None):
        self.spark = spark
        self.custom_check_func  = custom_check_func
        self.custom_format_func  = custom_format_func
        self.custom_run_func = custom_run_func
        self.closing_day =  closing_day
        self.section_name = section_name


    def check_func(self, another_check_function=None, *args):

        try:
            if not another_check_function:
                status, msg, info_dict, perc_completed = self.custom_check_func()
            else:
                if logger: logger.info("Using custom checker function")
                if another_check_function == check_sql:
                    status = another_check_function(*args)
                    msg=""
                    info_dict = {}
                    perc_completed = None
                else:
                    status, msg, info_dict, perc_completed = another_check_function(*args)
        except AnalysisException as e:
            print(e)
            status = False
            msg = str(e)
            info_dict = {}
            perc_completed = None

        self.status = status
        self.msg = msg
        self.extra_info = info_dict
        self.perc_completed = perc_completed


    def run_func(self, *args, **kwargs):
        return self.custom_run_func(*args, **kwargs)

    def __repr__(self):
        return "status={}\n section_name={}\nclosing_day={}\n msg={}".format(self.status, self.section_name, self.closing_day, "\n".join(self.custom_format_func(self)))
    def __str__(self):
        return self.__repr__


    def print_msg(self):
        msg = self.custom_format_func(self)

        if isinstance(msg, str) and logger: logger.info(msg)
        elif isinstance(msg, list) and logger:
            for mm in msg:
                logger.info(mm)
        else:
            print("No logger or msg has unknown type {}".format(type(msg)))


def check_sql(spark, sql_query):
    try:
        df = spark.sql(sql_query)
        if df.take(1):
            return True
        else:
            return False
    except Exception as e:
        print("check_sql '{}' exception".format(sql_query))
        print(e)
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


def check_join_car(spark, closing_day, verbose=False):
    everything_ok = True
    if logger: logger.info("STEP JOIN_CAR ) checking started...")
    join_car_path = "tests_es.jvmm_amdocs_ids_" + closing_day
    try:
        spark.read.table(join_car_path)
        my_msg = "INFO - JOIN_CAR '{}' exists".format(join_car_path)

    except AnalysisException as e:
        my_msg = "ERROR - JOIN_CAR. Exception: {}".format(str(e).split(";;")[0].replace("`", ""))
        everything_ok = False

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, None, None


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


def check_extra_feats(spark, closing_day, metadata_version=None, verbose=False):
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

def check_car_preparado(spark, closing_day, verbose=False):
    everything_ok = True
    if logger: logger.info("STEP CAR PREPARADO ) checking started...")
    car_preparado_path = "tests_es.jvmm_amdocs_prepared_car_mobile_complete_" + closing_day
    try:
        spark.read.table(car_preparado_path)
        my_msg = "INFO - CAR PREPARADO '{}' exists".format(car_preparado_path)

    except AnalysisException as e:
        my_msg = "ERROR - CAR PREPARADO. Exception: {}".format(str(e).split(";;")[0].replace("`", ""))
        everything_ok = False

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, None, None

def check_delivery_table(spark, closing_day, training_closing_date, verbose=False):
    table_name = DELIVERY_TABLE_PREPARED.format(closing_day, training_closing_date)
    result = check_table(spark, table_name)
    if result == "":
        my_msg = "INFO - DELIVERY TABLE '{}' exists".format(table_name)
        everything_ok = True
    else:
        my_msg = "ERROR - DELIVERY TABLE . Exception: {}".format(str(result).split(";;")[0].replace("`", ""))
        everything_ok = False

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, None, None


def check_churn_reasons_table(spark, closing_day, scores_segments, training_closing_date, verbose=False):
    '''
    Check table for segment and the merged one
    :param spark:
    :param closing_day:
    :param verbose:
    :return:
    '''

    from churn.models.churn_reasons.churn_reasons_improved import TABLE_FULL

    extra_info = {}
    stage_result = True
    msg = ["Summary:"]


    for segment in scores_segments:

        df = spark.read.parquet(TABLE_FULL).where(col("closing_day") == closing_day).where(
            col("segment") == segment)
        if training_closing_date:
            df = df.where(col("training_closing_date") == training_closing_date)

        if df.take(1):
            my_msg = ["OK - Found content for segment={} closing_day={} training={}".format(segment, closing_day, training_closing_date)]
            everything_ok = True
        else:
            my_msg = ["KO - Path {} does not have any content for segment='{}' closing_day='{}' training='{}' exists".format(TABLE_FULL, segment, closing_day, training_closing_date)]
            everything_ok = False

        if logger and verbose: logger.info(my_msg)

        stage_result = everything_ok and stage_result

        msg = msg + my_msg
        extra_info[segment] = not everything_ok # True means "needs to generate it"

    return stage_result, msg, extra_info, 100.0 if stage_result else int(100.0 * len([result_ for _, result_ in extra_info.items() if not result_])/len(extra_info))

def check_table(spark, table_name):
    try:
        spark.read.table(table_name)
        return ""
    except AnalysisException as e:
        return e

def check_scores_new(spark, closing_day, segment, tr_cycle_ini, tr_cycle_end, verbose=True):
    # Look for entries for this closing day. Take the list of model_executed_at
    # These entries should be cross with the model_parameters table, to check
    if logger: logger.info("STEP SCORES {} ) checking started...".format(segment))



    sql_query = "select msisdn, scoring, model_executed_at from parquet.`/data/attributes/vf_es/model_outputs/model_scores/model_name={}` where predict_closing_date = '{}'".format(
        "churn_preds_" + segment, closing_day)

    exe_list = None
    sql_scores_query = None

    if logger: logger.info(sql_query)

    df = spark.sql(sql_query)
    if df.take(1):
        # print("True '{}'".format(",'".join(df.select("model_executed_at").distinct().rdd.map(lambda xx: xx[0]).collect())))
        exe_list = df.select("model_executed_at").distinct().rdd.map(lambda xx: xx[0]).collect()
        everything_ok = True
    else:
        everything_ok = False


    training_days = [tr_cycle_ini + "to" + tr_cycle_end]

    if tr_cycle_ini == tr_cycle_end: # adapted for fbb model
        training_days.append(tr_cycle_ini)

    if everything_ok == True:
        if tr_cycle_ini and tr_cycle_end:
            sql_query = "select * from parquet.`/data/attributes/vf_es/model_outputs/model_parameters/model_name={}` where executed_at in ('{}') and training_closing_date in ('{}') sort by executed_at desc".format(
                "churn_preds_" + segment,
                "','".join(exe_list),
                "','".join(training_days))
        else:
            sql_query = "select * from parquet.`/data/attributes/vf_es/model_outputs/model_parameters/model_name={}` where executed_at in ('{}') sort by training_closing_date desc, executed_at desc".format(
                "churn_preds_" + segment,
                "','".join(exe_list),
                )

        print(sql_query)
        df_param = spark.sql(sql_query)


        if df_param.take(1):

            everything_ok = True
            my_msg = "ERROR - undefined"

            if not tr_cycle_ini and not tr_cycle_end:

                my_msg = ["INFO - SCORES segment={}|closing_day={}. Found scores for:".format(segment, closing_day)]
                existing_models = df_param.select("training_closing_date", "executed_at").rdd.collect()
                for mm in existing_models:
                    mydict = mm.asDict()
                    #mydict = df_param.take(1)[0].asDict() # latest (training_closing_date, executed_at) for this closing_date
                    training_closing_date = mydict["training_closing_date"]
                    tr_cycle_ini = training_closing_date[:8]
                    tr_cycle_end = training_closing_date[10:]
                    executed_at = mydict["executed_at"]

                    my_msg += ["tr_ini={} tr_end={} executed_at='{}'".format(tr_cycle_ini, tr_cycle_end, executed_at)]


            elif tr_cycle_ini and tr_cycle_end:

                my_msg =  "INFO - SCORES segment={}|closing_day={}|tr_ini={}|tr_end={}: Found scores for executed_at '{}'".format(segment,
                                                                                                                                   closing_day,
                                                                                                                                   tr_cycle_ini,
                                                                                                                                   tr_cycle_end,
                                                        ",'".join(df_param.select("executed_at").distinct().rdd.map(lambda xx: xx[0]).collect()))

            executed_at = df_param.select("executed_at").sort(desc("executed_at")).limit(1).collect()[0].asDict()["executed_at"]

            sql_scores_query = "select msisdn, scoring, model_executed_at from parquet.`/data/attributes/vf_es/model_outputs/model_scores/model_name={}` where predict_closing_date = '{}' and model_executed_at='{}'".format(
                "churn_preds_" + segment,
                closing_day,
                executed_at)

            print(sql_scores_query)
            print(logger)
            if logger and verbose: logger.info(my_msg)
            print("return", everything_ok, my_msg, sql_scores_query, 100)
            return everything_ok, my_msg, sql_scores_query, 100


    everything_ok = False
    my_msg = "ERROR - SCORES segment={}. Not found any scores for closing_day={} tr_ini={} tr_end={}".format(segment,
                                                                                                                  closing_day,
                                                                                                                  tr_cycle_ini,
                                                                                                                  tr_cycle_end)

    if logger and verbose: logger.info(my_msg)

    return everything_ok, my_msg, sql_scores_query, None



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


def check_delivery_file(closing_day, tr_ini, tr_end, horizon, deliv_file_format):

    from churn.models.ccc.delivery.delivery_manager import DIR_DELIVERY
    from churn.utils.constants import DELIVERY_FILENAME, DELIVERY_FILENAME_CHURNREASONS, DELIVERY_FILENAME_EXTENDED, DELIVERY_FILENAME_CLASSIC, DELIVERY_FILENAME_REASONS

    stage_result = True
    files_format = {}

    if "extended" in deliv_file_format or "all" in deliv_file_format:
        files_format["extended"] = DELIVERY_FILENAME_EXTENDED
    if "classic" in deliv_file_format or "all" in deliv_file_format:
        files_format["classic"] = DELIVERY_FILENAME_CLASSIC
    if "extended" in deliv_file_format or "reasons" in deliv_file_format:
        files_format["reasons"] = DELIVERY_FILENAME_REASONS
    msg = ["Summary:"]

    extra_info = {}

    for format_label, myfile in files_format.items():

        delivery_filename = myfile.format(closing_day,
                                                     "*" if not tr_ini else tr_ini,
                                                     "*" if not tr_end else tr_end,
                                                     horizon)
        files_goal = os.path.join(DIR_DELIVERY, delivery_filename + "*.txt")


        if logger: logger.info("Looking for files with this pattern {}".format(files_goal))

        import glob
        files = glob.glob(files_goal)

        result = (True if files else False)
        stage_result = result and stage_result

        #msg  = "Delivery file exists" if result else "Delivery file does not exist!"

        msg_ok = ["OK - {} - Found following files with pattern {}: ".format(format_label, files_goal)] + [f for f in files]
        msg_ko = ["KO - {} - No found any files with pattern {}".format(format_label, files_goal)]

        msg = msg + (msg_ok if result else msg_ko)
        extra_info[format_label] = not result # True means "needs to generate it"

    return stage_result, msg, extra_info, 100.0 if stage_result else int(100.0 * len([result_ for _, result_ in extra_info.items() if not result_])/len(extra_info))


def check_model_evaluation(closing_day, nb_days, ref_date=None):
    '''
    Look for files older than ref_date (ref_date is search in the filename)
    :param closing_day:
    :param nb_days:
    :param ref_date:
    :return:
    '''

    from churn.models.EvaluarModelosChurn.python_code.churn_Preds_quality import OUTPUTFILE, FILENAME_EXCEL

    file_goal = os.path.join(OUTPUTFILE, FILENAME_EXCEL.format(closing_day, nb_days, "*"))

    import glob
    files = glob.glob(file_goal)

    if ref_date:
        # filter those files with date newer than ref_date
        files = [ff for ff in files if dt.datetime.fromtimestamp(os.path.getmtime(ff)).strftime("%Y%m%d") >= ref_date]

    result = (True if files else False)

    #msg  = "Delivery file exists" if result else "Delivery file does not exist!"

    msg_ok = ["OK - Found following file(s) with pattern {}: ".format(file_goal)] + [f for f in files]
    msg_ko = ["KO - No found any file(s) with pattern {}".format(file_goal)]

    msg = msg_ok if result else msg_ko

    return result, msg, {}, 100.0 if result else int(100) if result else 0



def check_model_output_entry(spark, model_name, closing_day, training_closing_date=None, historic=False):

    df = get_complete_scores_model_name_closing_day(spark, model_name=model_name, closing_day=closing_day)

    if training_closing_date:
        df = df.where(col('training_closing_date') == training_closing_date)

    if historic:
        return_feed_execution = closing_day
    else:
        return_feed_execution = get_next_dow(weekday=5).strftime("%Y%m%d")

    day_partition = int(return_feed_execution[6:])
    month_partition = int(return_feed_execution[4:6])
    year_partition = int(return_feed_execution[:4])

    print("Looking for partition year={}/month={}/day={}".format(year_partition, month_partition, day_partition))
    df = df.where( (col('year') == year_partition) & (col('month') == month_partition) & (col('day') == day_partition) )

    entries = df.select("year", "month", "day", "predict_closing_date", "model_executed_at").groupby("year", "month", "day", "model_executed_at", "predict_closing_date").agg(
        sql_count("*").alias("count")).collect()

    result = True if entries else False

    msg_ok = ["OK - Found following entries on model_outputs"] + entries
    msg_ko = ["KO - No found any entries on model_outputs"]

    msg = msg_ok if result else msg_ko

    return result, msg, {}, 100.0 if result else int(100) if result else 0
