#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import time
from pyspark.sql.functions import count as sql_count, avg as sql_avg, when, sum as sql_sum, abs as sql_abs, udf,\
    array, concat, lpad, collect_list, concat_ws, round as sql_round, countDistinct, regexp_extract, lag, desc
from pyspark.sql.functions import col, lit
from operator import and_
from functools import reduce
from pyspark.sql.types import FloatType
import numpy as np
import datetime as dt
import subprocess
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType, DateType, FloatType
from pyspark.sql.window import Window
from scipy.stats import skew
import datetime as dt
# spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/navcomp/analysis/test_navcomp.py  > /var/SP/data/home/csanc109/logging/volumes_nocalls.log

LEVELS_DICT = {"customer" : "num_cliente",
               "service"  : "msisdn",
               "customer_base" : "msisdn",
               "customer_additional/*" : "nif_cliente",
               "navcomp/*" : "msisdn",
               "billing" : "num_cliente",
               "callscomp/*" : "msisdn",
               "navcomp_adv" : "msisdn",
               "callscomp_adv" : "msisdn",
               "tgs" : "msisdn",
               "tickets" : "nif_cliente",
               "spinners" : "nif_cliente",
               "reimbursements" : "NIF_CLIENTE",
               "orders_sla" : "nif_cliente",
               'ccc/msisdn' : "msisdn",
               'ccc/nif' : "nif_cliente",
               'mob_port/*' : 'msisdn',
               'fix_port/*' : 'msisdn',
               'myvf/app/2' : "msisdn",
               'myvf/web/3' : "msisdn",
              }


def count_files_in_path(path_):
    cmd = "hdfs dfs -ls {} | wc -l".format(path_)
    output = subprocess.check_output(cmd, shell=True).strip().split('\n')
    num_files = int(output[0]) - 1  # the first entry is the summary
    return num_files


def get_partition_directories(root_path, from_date, to_date=None):
    '''
    Return the directories to check with partition date [from_date, to_date]
    This function ignores the creation date and only takes into account the partition date
    :param root_path:
    :param from_date:
    :param to_date:
    :return:
    '''
    if not from_date:
         print("from_date argument must not be None")
         return None
    cmd = "hdfs dfs -ls -R {} | grep drwx | grep -v _temporary | grep 'day=' ".format(root_path)  # | awk '$6 >= ".format(root_path) + '"' + from_date_str + '"' + "'"
    # if to_date:
    #     to_date_str = dt.datetime.strptime(to_date, "%Y%m%d").strftime("%Y-%m-%d")
    #     cmd += " | awk '$6 <= " + '"' + to_date_str + '"' + "'"

    print("[hdfs_functions] list_all_new_directories | Running command")
    print(cmd)
    output = subprocess.check_output(cmd, shell=True).strip().split('\n')
    # [dd.split(" ")[-1] for dd in output]

    valid_output = []
    for dd in output:
        # print(dd.split(" ")[-1])
        try:
            ss = re.search("^\/data\/udf\/vf_es\/churn_nrt\/.*\/year=(20[0-9]{2})\/month=([0-9]{1,2})\/day=([0-9]{1,2})$", dd.split(" ")[-1])
            c_day = "{0:04}{1:02}{2:02}".format(int(ss.group(1)), int(ss.group(2)), int(ss.group(3)))
            if to_date:
                if c_day >= from_date and c_day <= to_date:
                    valid_output.append(dd.split(" ")[-1])
            else:
                if c_day >= from_date:
                    valid_output.append(dd.split(" ")[-1])
        except:
            print("Invalid path {}".format(dd))

    return valid_output


def investigate_duplicates(path_):
    if not path_: return "path is none"

    num_files = count_files_in_path(path_)

    if num_files == 400:
        cmd = "hdfs dfs -ls -R {}".format(path_)
        output = subprocess.check_output(cmd, shell=True).strip().split('\n')

        pattern1 = re.search("^.+\/part-(\d{5})-(.{8})-.+parquet$", output[-2]).group(2)
        return "hdfs dfs -rm {}{}*".format(os.path.join(path_, "*"), pattern1)
    else:
        return "hdfs dfs -rm -r {}".format(path_)


def add_scores_metrics(scores_list):
    avg_score = np.mean(scores_list)
    sd_score = np.std(scores_list)
    sk_score = skew(scores_list)
    # ku_score = score_rdd.map(lambda x: pow((x - avg_score) / sd_score, 4.0)).mean()
    min_score = np.min(scores_list)
    max_score = np.max(scores_list)
    return "{0:.2f} | {1:.2f} | {2:.2f} | {3:.2f} | {4:.2f}".format(avg_score, sd_score, sk_score, min_score, max_score)


add_scores_udf = udf(add_scores_metrics, StringType())


def check_model_scores(spark, model_, level="msisdn", check_freq=None, from_month=None, from_year=None):
    '''
    check_freq: None --> no dot check freq
    '''

    df_scores = spark.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name={}".format(model_))

    if from_year:
        df_scores = df_scores.where(col("year") >= from_year)
    if from_month:
        df_scores = df_scores.where(col("month") >= from_month)

    df_scores = (
        df_scores.withColumn("deliv_partition", concat(col("year"), lpad(col("month"), 2, '0'), lpad(col("day"), 2, '0'))).select("executed_at", "predict_closing_date", "deliv_partition", level,
                                                                                                                                  "scoring").groupby("executed_at", "predict_closing_date",
                                                                                                                                                     "deliv_partition").agg(
            sql_count("*").alias(model_), countDistinct(level).alias("distinct"), add_scores_udf(collect_list("scoring")).alias("scores_metrics")))

    df_scores = df_scores.withColumn("distinct_verified", when(col(model_) - col("distinct") == 0, "yes").otherwise("no"))

    df_params = spark.read.load("/data/attributes/vf_es/model_outputs/model_parameters/model_name={}".format(model_)).select("executed_at", "training_closing_date")
    df_scores = df_scores.join(df_params, on=["executed_at"], how="outer")
    df_scores = df_scores.select("executed_at", "training_closing_date", "predict_closing_date", "deliv_partition", model_, "distinct_verified", "scores_metrics")

    if check_freq:

        my_window = Window.partitionBy().orderBy("deliv_partition")
        diff_days_udf = udf(lambda start_date, end_date: (dt.datetime.strptime(end_date, "%Y%m%d") - dt.datetime.strptime(start_date, "%Y%m%d")).days if end_date and start_date else -1000,
                            IntegerType())

        df_scores = df_scores.withColumn("prev_value", lag(df_scores["deliv_partition"]).over(my_window))
        df_scores = df_scores.withColumn("diff",
                                         when(col("prev_value").isNotNull() & col("deliv_partition").isNotNull(), diff_days_udf(col("prev_value"), col("deliv_partition"))).otherwise(None)).drop(
            "prev_value")
        df_scores = df_scores.withColumn("freq_verified", when(col("diff") == check_freq, "yes").otherwise("no"))

        df_scores = df_scores.withColumn("dist|freq", concat_ws("|", col("distinct_verified"), "freq_verified")).drop("distinct_verified", "freq_verified")
    else:
        df_scores = df_scores.withColumn("dist", col("distinct_verified")).drop("distinct_verified")

    my_window = Window.partitionBy().orderBy("deliv_partition")
    rel_inc_udf = udf(lambda prev, curr: np.float(100.0 * (curr - prev) / prev) if prev and curr else 0, FloatType())
    df_scores = df_scores.withColumn("prev_value", lag(df_scores[model_]).over(my_window))
    df_scores = df_scores.withColumn("rel_inc", when(col("prev_value").isNotNull() & col(model_).isNotNull(), rel_inc_udf(col("prev_value"), col(model_))).otherwise(0)).drop("prev_value")
    df_scores = df_scores.withColumn("rel_inc", sql_round(df_scores["rel_inc"], 2))

    df_scores = df_scores.withColumn("training_closing_date", regexp_extract(col('training_closing_date'), '(\w+)to(\w+)', 1)).withColumnRenamed("training_closing_date", "tr_date").withColumnRenamed(
        "predict_closing_date", "tt_date")

    df_scores = df_scores.withColumn("path_",
                                     concat(lit("/data/attributes/vf_es/model_outputs/model_scores/model_name={}".format(model_)), lit("/year="), col("deliv_partition").substr(0, 4), lit("/month="),
                                            col("deliv_partition").substr(5, 2).cast(IntegerType()).cast(StringType()), lit("/day="),
                                            col("deliv_partition").substr(7, 2).cast(IntegerType()).cast(StringType()))).drop("diff")

    user_udf = udf(get_user, StringType())
    df_scores = df_scores.withColumn("user", user_udf(col("path_")))
    df_scores = df_scores.withColumnRenamed("scores_metrics", " avg |  std |  sk |  min |  max")
    df_scores = df_scores.withColumnRenamed(model_, "volume")
    df_scores = df_scores.withColumnRenamed("deliv_partition", "delivery")


    return df_scores


def get_user(path_):
    if not path_: return "path is none"

    cmd = "hdfs dfs -ls {}".format(path_)
    output = subprocess.check_output(cmd, shell=True).strip().split('\n')
    try:
        username = re.search("^(.+) (.+) (.+) bdpmd(.+)parquet$", output[-2]).group(3)
    except:
        return "Problem getting user"
    return username.strip()




def check_churn_nrt_partition(path_):
    try:
        level = __get_level(module_name)
        if not level:
            return False
        count_ = spark.read.load(path_).select(level).count()
        distinct_ = spark.read.load(path_).select(level).distinct().count()

        if count_ == distinct_:
            # print("No issues found on module {}".format(path_))
            return True

    except Exception as e:
        print(" !! Exception analyzing module {}".format(path_))
        print(e)
        return False
    #print("Issues found on module {} - Hint {}".format(path_, investigate_duplicates(path_)))
    return False

def __get_level(module_name):
    level = [(k,v) for k,v in LEVELS_DICT.items() if re.compile(k).match(module_name)]
    # In case more than one key in LEVELS_DICT math the pattern, we return the longest one
    # E.g. if customer and customer_additional match the pattern, we return customer additional
    level = sorted(level, key=lambda k: len(k[0]), reverse=True)
    if level:
        return level[0][1]
    return None

if __name__ == "__main__":

    import os, re
    import sys
    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("data_checker_churn_nrt")
    sc.setLogLevel('WARN')

    import time
    import argparse
    import datetime as dt
    import re
    from churn_nrt.src.utils.hdfs_functions import list_all_new_directories
    from churn_nrt.src.utils.date_functions import move_date_n_days

    start_time_total = time.time()

    parser = argparse.ArgumentParser(description="churn_nrt checker", epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('--c_from_date', metavar='<YYYYMMDD>', type=str, required=False, default=None, help='Start date for checking data')
    parser.add_argument('--c_to_date', metavar='<YYYYMMDD>', type=str, required=False, default=None, help='End date for checking data')
    parser.add_argument('--p_from_date', metavar='<YYYYMMDD>', type=str, required=False, default=None, help='Start date for checking data')
    parser.add_argument('--p_to_date', metavar='<YYYYMMDD>', type=str, required=False, default=None, help='End date for checking data')
    parser.add_argument('--scope', metavar='all,churn_nrt,triggers', type=str, required=False, default="all", help='what to check')

    args = parser.parse_args()
    print(args)
    c_from_date = args.c_from_date
    c_to_date = args.c_to_date
    p_from_date = args.p_from_date
    p_to_date = args.p_to_date
    scope = args.scope

    print("ARGS c_from_date={} c_to_date={} p_from_date={} p_to_date={} scope={}".format(c_from_date, c_to_date, p_from_date, p_to_date, scope))


    if scope not in ["all", "triggers", "churn_nrt"]:
        print("[ERROR] Unknown value for scope parameter. Valid values: all, triggers, churn_nrt")
        import sys
        sys.exit(1)

    if c_from_date and p_from_date:
        print("[ERROR] only one of c_from_date and p_from_date has to be present!")
        import sys
        sys.exit(1)


    if not c_from_date and not p_from_date:
        print("No arguments passed to script - automatic mode enabled")
        c_to_date = dt.datetime.today().strftime("%Y%m%d")
        c_from_date = move_date_n_days(c_to_date, n=-2)

    if c_from_date:
        new_dirs = list_all_new_directories("/data/udf/vf_es/churn_nrt/", from_date=c_from_date, to_date=c_to_date)
        print("{} directories created from date {} to date {}".format(len(new_dirs), c_from_date, c_to_date))

    else: # p_from_date:
        new_dirs = get_partition_directories("/data/udf/vf_es/churn_nrt/", from_date=p_from_date, to_date=p_to_date)
        print("{} directories with partition date from {} to {}".format(len(new_dirs), p_from_date, p_to_date))


    if scope in ["all", "churn_nrt"]:

        # dd = '/data/udf/vf_es/churn_nrt/navcomp/15/year=2020/month=5/day=14'
        # list_all_new_directories
        unknown_modules = []
        correct_paths = []
        incorrect_paths = []

        for dd in new_dirs:
            try:
                module_name = re.match("^/data/udf/vf_es/churn_nrt/(.*)/year=.*$", dd).group(1)
            except:
                print("Error extracting module_name with dd '{}'".format(dd))
                continue


            if __get_level(module_name):
                res = check_churn_nrt_partition(dd)
                if res:
                    correct_paths.append(dd)
                else:
                    incorrect_paths.append(dd)
            else:
                unknown_modules.append(module_name)

        print("[WARNING] Unknown module names - checked was not run over them:")
        print(set(unknown_modules))


        if incorrect_paths:
            print("[ERROR] Problems found on following modules:")
            for dd in incorrect_paths:
                print("   Issues found on module {} | num_files={} | Hint '{}'".format(dd, count_files_in_path(dd), investigate_duplicates(dd)))
        else:
            print("[INFO] No found problems in churn_nrt module!")

        print("")
        print("")

    if scope in ["all", "triggers"]:

        ###################################################################################################
        # Daily triggers: myvf_app, myvf_web & navcomp
        ###################################################################################################

        print("###################################################################################################")
        print("# Daily triggers: myvf_app, myvf_web & navcomp")
        print("###################################################################################################")

        for model_ in ["myvf_app", "myvf_web", "triggers_navcomp"]:
            start_time = time.time()
            print("* * * * * * * * * {} * * * * * * * * *".format(model_))
            df_scores = check_model_scores(spark, model_, check_freq=1, from_month=None, from_year=2020)
            df_scores.sort(desc("delivery"), desc("executed_at"), desc("tt_date"), desc("tr_date")).show(10, truncate=False)
            print("Elapsed time: {} minutes".format((time.time() - start_time) / 60.0))
            print(" ")

        ###################################################################################################
        # Weekly triggers: Zhilabs, servicios and tnps
        ###################################################################################################

        print("###################################################################################################")
        print("# Weekly triggers: Zhilabs, servicios and tnps")
        print("###################################################################################################")

        level_dict = {"zhilabs_ftth": "client_id", "triggers_ml": "nif", "trigger_tnps": "msisdn"}

        for model_ in ["zhilabs_ftth", "triggers_ml", "trigger_tnps"]:
            print("* * * * * * * * * {} * * * * * * * * *".format(model_))
            df_scores = check_model_scores(spark, model_, level=level_dict[model_], check_freq=7)
            df_scores.sort(desc("delivery"), desc("executed_at"), desc("tt_date"), desc("tr_date")).show(10, truncate=False)
            print(" ")


        ###################################################################################################
        # Twice-a-month triggers: Social
        ###################################################################################################

        print("###################################################################################################")
        print("# Twice-a-month triggers: Social")
        print("###################################################################################################")

        for model_ in ["trigger_social"]:
            print("* * * * * * * * * {} * * * * * * * * *".format(model_))
            df_scores = check_model_scores(spark, model_)
            df_scores.sort(desc("delivery"), desc("executed_at"), desc("tt_date"), desc("tr_date")).show(10, truncate=False)
            print(" ")

        ###################################################################################################
        # Triggers not in production
        ###################################################################################################

        print("###################################################################################################")
        print("# Triggers not in production")
        print("###################################################################################################")

        level_dict = {"price_sens_high_potential": "client_id", "price_sens_glob_pot_classifier": "client_id", "prop_virgin": "msisdn"}

        for model_ in ["prop_virgin", "price_sens_high_potential", "price_sens_glob_pot_classifier"]:
            print("* * * * * * * * * {} * * * * * * * * *".format(model_))
            df_scores = check_model_scores(spark, model_, level=level_dict[model_], check_freq=None)
            df_scores.sort(desc("delivery"), desc("executed_at"), desc("tt_date"), desc("tr_date")).show(10, truncate=False)
            print(" ")

    print("Elapsed time in checker {} minutes".format( (time.time() - start_time_total)/60.0) )