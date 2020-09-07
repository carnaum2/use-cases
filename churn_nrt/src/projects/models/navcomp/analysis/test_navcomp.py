#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import time
from pyspark.sql.functions import count as sql_count, avg as sql_avg, when, sum as sql_sum, abs as sql_abs, udf, array
from pyspark.sql.functions import col, lit
from operator import and_
from functools import reduce
from pyspark.sql.types import FloatType
import numpy as np
import datetime as dt

#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/navcomp/analysis/test_navcomp.py > /var/SP/data/home/csanc109/logging/volumes_virgin_inc_top25k.log
# spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/navcomp/analysis/test_navcomp.py  > /var/SP/data/home/csanc109/logging/volumes_nocalls.log

def set_label_nocalls(spark, closing_day, tr_set, target_days, force_gen=False):

    closing_day_B = move_date_n_days(closing_day, n=target_days)

    if target_days > 0:
        print("Adding label with target days {} - [{},{}]".format(target_days, closing_day, closing_day_B))
    else:
        print("Adding label with target days {} - [{},{}]".format(target_days, closing_day_B, closing_day))


    df_navcomp_data2 = (
        NavCompData(spark, abs(target_days)).get_module(closing_day_B if target_days>0 else closing_day, save=True, force_gen=force_gen).select("msisdn", "VIRGINTELCO_sum_count", "VIRGINTELCO_distinct_days_with_navigation")
            .withColumnRenamed("VIRGINTELCO_sum_count", "label_VIRGINTELCO_sum_count_last{}".format(target_days))
            .withColumnRenamed("VIRGINTELCO_distinct_days_with_navigation", "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)))

    # df_calls2 = (CallsCompData(spark, abs(target_days)).get_module(closing_day_B if target_days>0 else closing_day, save=True, force_gen=False)
    #              .select("msisdn", "callscomp_VIRGIN_num_calls")
    #              .withColumnRenamed("callscomp_VIRGIN_num_calls", "label_callscomp_VIRGIN_num_calls_last{}".format(target_days)))

    tr_set = tr_set.join(df_navcomp_data2, on=["msisdn"], how="left")
    # tr_set = tr_set.join(df_calls2, on=["msisdn"], how="left").fillna({"label_VIRGINTELCO_sum_count_last{}".format(target_days): 0,
    #                                                                    "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days): 0,
    #                                                                    "label_callscomp_VIRGIN_num_calls_last{}".format(target_days): 0.0})

    tr_set = tr_set.withColumn("label",
                               when(((col("label_VIRGINTELCO_sum_count_last{}".format(target_days)) >= 2) | (col("label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)) >= 2)), 1).otherwise(0))

    return tr_set

def set_label_test(spark, closing_day, tr_set, target_days):

    closing_day_B = move_date_n_days(closing_day, n=target_days)

    if target_days > 0:
        print("Adding label with target days {} - [{},{}]".format(target_days, closing_day, closing_day_B))
    else:
        print("Adding label with target days {} - [{},{}]".format(target_days, closing_day_B, closing_day))

    df_navcomp_data2 = (NavCompData(spark, abs(target_days)).get_module(closing_day_B if target_days > 0 else closing_day, save=True, force_gen=False).select("msisdn", "VIRGINTELCO_sum_count",
                                                                                                                                                              "VIRGINTELCO_distinct_days_with_navigation").withColumnRenamed(
        "VIRGINTELCO_sum_count", "label_VIRGINTELCO_sum_count_last{}".format(target_days)).withColumnRenamed("VIRGINTELCO_distinct_days_with_navigation",
                                                                                                             "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)))

    df_calls2 = (CallsCompData(spark, abs(target_days)).get_module(closing_day_B if target_days > 0 else closing_day, save=True, force_gen=False).select("msisdn",
                                                                                                                                                         "callscomp_VIRGIN_num_calls").withColumnRenamed(
        "callscomp_VIRGIN_num_calls", "label_callscomp_VIRGIN_num_calls_last{}".format(target_days)))

    tr_set = tr_set.join(df_navcomp_data2, on=["msisdn"], how="left")
    tr_set = tr_set.join(df_calls2, on=["msisdn"], how="left").fillna(
        {"label_VIRGINTELCO_sum_count_last{}".format(target_days): 0, "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days): 0,
         "label_callscomp_VIRGIN_num_calls_last{}".format(target_days): 0.0})

    tr_set = tr_set.withColumn("label", when(((col("label_VIRGINTELCO_sum_count_last{}".format(target_days)) >= 2) | (
                col("label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)) >= 2) | (col("label_callscomp_VIRGIN_num_calls_last{}".format(target_days)) > 0)), 1).otherwise(
        0))

    return tr_set

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
    spark, sc = get_spark_session_noncommon("analysis_navcomp")
    sc.setLogLevel('WARN')

    import time
    start_time_total = time.time()


    import argparse

    parser = argparse.ArgumentParser(description="Run price sensitivity model - new product", epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    #parser.add_argument('--calls', metavar='<1>', type=int, required=False, default=1, help='Date to be used in training')


    args = parser.parse_args()
    print(args)
    # calls_req = args.calls
    calls_req = 1
    # print("CALLS REQ ", calls_req)
    # closing_day_list = args.tr_date
    # closing_day_list = closing_day_list.split(",")

    from churn_nrt.src.data.navcomp_data import NavCompAdvData, NavCompData
    from churn_nrt.src.data.calls_comps_data import CallsCompData, CallsCompAdvData
    from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles
    from churn_nrt.src.projects.models.virgin.model_classes import get_customer_base
    from churn_nrt.src.data_utils.base_filters import keep_active_services
    from churn_nrt.src.data.customer_base import CustomerAdditional
    from churn_nrt.src.data_utils.base_filters import get_forbidden_orders_filter, get_churn_call_filter, get_non_recent_customers_filter, get_disconnection_process_filter
    from churn_nrt.src.data.customer_base import add_ccaa
    from churn_nrt.src.data_utils.Metadata import apply_metadata
    import pprint
    from churn_nrt.src.data.navcomp_data import NavCompAdvData
    from churn_nrt.src.data.customer_base import CustomerBase

    # for closing_day in ["20200618", "20200619", "20200620", "20200621", "20200622", "20200623"]:
    #     print("*******", closing_day, "*********")
    #     df_navcomp_all = NavCompAdvData(spark).get_module(closing_day, save=True)
    #     #closing_day = move_date_n_days(closing_day, n=+1)
    #
    #
    # print("Elapsed time {} minutes".format((time.time()-start_time_total)/60.0))
    #
    # sys.exit(1)


    ############################################################################################################
    # ABSOLUTE METRICS
    ############################################################################################################
    '''
    from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles
    from pyspark.sql.functions import avg as sql_avg
    from churn_nrt.src.utils.date_functions import move_date_n_days
    from churn_nrt.src.projects.models.virgin.model_classes import set_label

    CHURN_WINDOW = 15
    step_days = 7
    verbose = True
    compute_lift = True
    top_ = 500

    paths_dict = {"20200621": "/user/csanc109/projects/churn/data/prop_virgin/df_20200621_20200812_193118",
                  "20200630": "/user/csanc109/projects/churn/data/prop_virgin/df_20200630_20200811_153824",
                  "20200707": "/user/csanc109/projects/churn/data/prop_virgin/df_20200707_20200811_153358",
                  "20200714": "/user/csanc109/projects/churn/data/prop_virgin/df_20200714_20200811_153915",
                  "20200721" : "/user/csanc109/projects/churn/data/prop_virgin/df_20200721_20200813_051210"}

    results_dict = {}

    for closing_day in ["20200621", "20200630", "20200707", "20200714", "20200721"]:

        print(" * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - *")
        if closing_day in paths_dict.keys():
            print(closing_day, paths_dict[closing_day])
        else:
            print("{} no existe".format(paths_dict[closing_day]))
            continue


        df_labels_A = spark.read.load(paths_dict[closing_day]).drop_duplicates(["msisdn"])
        vol_ = df_labels_A.count()
        print("volA={}".format(vol_))


        end_port = move_date_n_days(closing_day, n=CHURN_WINDOW)
        df_labels_A = set_label(spark, closing_day, df_labels_A, CHURN_WINDOW).na.fill({'label': 0.0})

        churn_rate_all = df_labels_A.select(sql_avg('label').alias('churn_ref')).rdd.first()['churn_ref']
        print("------", closing_day, "ChRate%", churn_rate_all * 100)

        tops = []
        for top_ in [500,1000,2000,5000]:
            qq_top = 1.0 * top_ / vol_
            print("Marking as top the {}%".format(100.0*qq_top))

            thresh_top = df_labels_A.approxQuantile("scoring", [1.0 - qq_top], 0.000001)[0]
            df_top = df_labels_A.where(col("scoring") >= thresh_top)
            tt_churn_rate = df_top.select(sql_avg('label').alias('churn_ref')).rdd.first()['churn_ref']
            print("__TOP__ ------ {} qq_top={} top count = {} tt_churn_rate%={} lift={}".format(closing_day,
                                                                                     qq_top, df_top.count(),
                                                                                     100.0*tt_churn_rate, tt_churn_rate/churn_rate_all))
            tops.append([top_, qq_top, df_top.count(), tt_churn_rate])

        from churn_nrt.src.projects_utils.models.modeler import get_cumulative_churn_rate_fix_step

        cum_churn_rate = get_cumulative_churn_rate_fix_step(spark, df_labels_A, ord_col='scoring', label_col='label', step_=5000)
        df_pandas = cum_churn_rate.toPandas()
        df_pandas.to_csv("/var/SP/data/home/csanc109/data/virgin/df_abs_{}.csv".format(closing_day), header=True, index=True)


    '''
    ############################################################################################################
    # INCREMENTALS
    ############################################################################################################

    from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles
    from pyspark.sql.functions import avg as sql_avg
    from churn_nrt.src.utils.date_functions import move_date_n_days
    from churn_nrt.src.projects.models.virgin.model_classes import set_label

    CHURN_WINDOW = 15
    step_days = 7
    verbose = True
    compute_lift = True
    #top_ = 500
    inc_top = 25000

    paths_dict = {"20200621": "/user/csanc109/projects/churn/data/prop_virgin/df_20200621_20200812_193118",
                  "20200630": "/user/csanc109/projects/churn/data/prop_virgin/df_20200630_20200811_153824",
                  "20200707": "/user/csanc109/projects/churn/data/prop_virgin/df_20200707_20200811_153358",
                  "20200714": "/user/csanc109/projects/churn/data/prop_virgin/df_20200714_20200811_153915",
                  "20200721" : "/user/csanc109/projects/churn/data/prop_virgin/df_20200721_20200813_051210"}

    results_dict = {}

    for closing_day in ["20200621", "20200630", "20200707", "20200714", "20200721"]:

        closing_day_B = move_date_n_cycles(closing_day, n=1)
        print(" * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - *")
        print("incremental {} vs {} - usando top {}".format(closing_day, closing_day_B, inc_top))
        if closing_day in paths_dict.keys():
            print(closing_day, paths_dict[closing_day])
        else:
            print("{} no existe".format(paths_dict[closing_day]))
            continue

        if closing_day_B in paths_dict.keys():
            print(closing_day_B, paths_dict[closing_day_B])
        else:
            print("{} no existe".format(paths_dict[closing_day_B]))
            continue

        df_labels_A = spark.read.load(paths_dict[closing_day]).drop_duplicates(["msisdn"])
        vol_A = df_labels_A.count()
        if inc_top:
            qq_top = 1.0 * inc_top / vol_A
            print("Marking as top the {}% of dataframe A".format(100.0*qq_top))

            thresh_top = df_labels_A.approxQuantile("scoring", [1.0 - qq_top], 0.000001)[0]
            df_labels_A = df_labels_A.where(col("scoring") >= thresh_top)

        df_labels_B = spark.read.load(paths_dict[closing_day_B]).drop_duplicates(["msisdn"])
        vol_B = df_labels_B.count()
        if inc_top:
            qq_top = 1.0 * inc_top / vol_B
            print("Marking as top the {}% of dataframe B".format(100.0*qq_top))

            thresh_top = df_labels_B.approxQuantile("scoring", [1.0 - qq_top], 0.000001)[0]
            df_labels_B = df_labels_B.where(col("scoring") >= thresh_top)

        df_labels_join = df_labels_A.select("msisdn").join(df_labels_B.select("msisdn", "label", "scoring"), ['msisdn'], 'right').where(df_labels_A['msisdn'].isNull())
        df_labels_join = df_labels_join.cache()
        vol_incremental = df_labels_join.count()
        print("volA={} volB={} volInc={}".format(df_labels_A.count(), df_labels_B.count(), vol_incremental))


        end_port = move_date_n_days(closing_day_B, n=CHURN_WINDOW)
        df_labels_join = set_label(spark, closing_day_B, df_labels_join, CHURN_WINDOW).na.fill({'label': 0.0})

        churn_rate_all_incremental = df_labels_join.select(sql_avg('label').alias('churn_ref')).rdd.first()['churn_ref']
        print("------", closing_day, closing_day_B, "ChRate%{}".format(vol_incremental), churn_rate_all_incremental * 100)

        myschema3 = df_labels_join.schema

        tops = []
        for top_ in [500,1000,2000,5000]:
            if top_ and top_ < vol_incremental:
                qq_top = 1.0 * top_ / vol_incremental
                print("Marking as top the {}%".format(100.0*qq_top))

                thresh_top = df_labels_join.approxQuantile("scoring", [1.0 - qq_top], 0.000001)[0]
                df_top = df_labels_join.where(col("scoring") >= thresh_top)
                tt_churn_rate = df_top.select(sql_avg('label').alias('churn_ref')).rdd.first()['churn_ref']
                print("__TOP__ ------ {} to {} qq_top={} top count = {} tt_churn_rate%={} lift={}".format(closing_day, closing_day_B,
                                                                                         qq_top, df_top.count(),
                                                                                         100.0*tt_churn_rate, tt_churn_rate/churn_rate_all_incremental))
                tops.append([top_, qq_top, df_top.count(), tt_churn_rate])

        from churn_nrt.src.projects_utils.models.modeler import get_cumulative_churn_rate_fix_step

        cum_churn_rate = get_cumulative_churn_rate_fix_step(spark, df_labels_join, ord_col='scoring', label_col='label', step_=2500)
        df_pandas = cum_churn_rate.toPandas()
        df_pandas.to_csv("/var/SP/data/home/csanc109/data/virgin/df_inc_top{}_{}.csv".format(inc_top, closing_day_B), header=True, index=True)


    ############################################################################################################
    ############################################################################################################

    '''
    force_gen = False
    results = {}

    target_days = 15

    for closing_day in [
        "20200521", "20200531"
        "20200607", "20200614",
        "20200621",
        "20200630",
        "20200707",
        "20200714"
        ]:

        start_time_iter = time.time()
        print("CSANC109__base", closing_day)

        base_df = CustomerBase(spark).get_module(closing_day, save=False, save_others=False, add_tgs=False).filter(col('rgu') == 'mobile').drop_duplicates(["msisdn"])
        base_df = base_df.where((col('rgu') == 'mobile') & (col("cod_estado_general").isin("01", "09")) & (col("srv_basic").isin("MRSUI", "MPSUI") == False))
        base_df = base_df.select("num_cliente", "msisdn", "nif_cliente")

        if calls_req == 1:
            from churn_nrt.src.projects.models.virgin.model_classes import set_label

            base_df = set_label(spark, closing_day, base_df, target_days, force_gen=force_gen)
        else:
            base_df = set_label_nocalls(spark, closing_day, base_df, target_days, force_gen=force_gen)

        base_df = base_df.cache()
        volA = base_df.count()
        label1A = base_df.where(col("label") == 1).count()

        print('CheckA  - Volume: {} - Number of label=1 {}'.format( volA,label1A))


        print("[TriggerMyVfModel] get_set | - - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))
        tr_active_filter = get_non_recent_customers_filter(spark, closing_day, 90)

        print("[TriggerMyVfModel] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))
        tr_disconnection_filter = get_disconnection_process_filter(spark, closing_day, 90)

        print("[TriggerMyVfModel] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))
        tr_churn_call_filter = get_churn_call_filter(spark, closing_day, 90, 'msisdn')

        tr_forbidden = get_forbidden_orders_filter(spark, closing_day, level="nif", verbose=False, only_active=True)

        tr_set = (base_df.join(tr_active_filter, ['msisdn'], 'inner'))

        tr_set = tr_set.cache()
        volB1 =  tr_set.count()
        label1B1 = tr_set.where(col("label") == 1).count()
        print('CheckB1  - Volume: {} - Number of label=1 {}'.format( volB1, label1B1))


        tr_set = (tr_set.join(tr_disconnection_filter, ['nif_cliente'], 'inner'))

        tr_set = tr_set.cache()
        volB2 =  tr_set.count()
        label1B2 = tr_set.where(col("label") == 1).count()
        print('CheckB2  - Volume: {} - Number of label=1 {}'.format( volB2, label1B2))

        tr_set = (tr_set.join(tr_churn_call_filter, ['msisdn'], 'inner'))

        tr_set = tr_set.cache()
        volB3 =  tr_set.count()
        label1B3 = tr_set.where(col("label") == 1).count()
        print('CheckB3  - Volume: {} - Number of label=1 {}'.format( volB3, label1B3))

        tr_set = (tr_set.join(tr_forbidden, ['nif_cliente'], 'inner'))


        tr_set = tr_set.cache()
        volB4 =  tr_set.count()
        label1B4 = tr_set.where(col("label") == 1).count()
        print('CheckB4  - Volume: {} - Number of label=1 {}'.format( volB4, label1B4))

        start_time = time.time()
        df_navcomp_data = NavCompData(spark, 30).get_module(closing_day, save=True, force_gen=force_gen).select("msisdn", "VIRGINTELCO_sum_count").drop_duplicates(["msisdn"])
        df_calls = CallsCompData(spark, 30).get_module(closing_day, save=True, force_gen=force_gen).select("msisdn", "callscomp_VIRGIN_num_calls").drop_duplicates(["msisdn"])

        tr_set = tr_set.join(df_navcomp_data, on=["msisdn"], how="left")
        if calls_req == 1:
            tr_set = tr_set.join(df_calls, on=["msisdn"], how="left")

        tr_set = apply_metadata(NavCompData(spark, 30).get_metadata(), tr_set)
        if calls_req == 1:
            tr_set = apply_metadata(CallsCompData(spark, 30).get_metadata(), tr_set)
            tr_set = tr_set.fillna({"VIRGINTELCO_sum_count": 0, "callscomp_VIRGIN_num_calls": 0})
        else:
            tr_set = tr_set.fillna({"VIRGINTELCO_sum_count": 0})

        # tr_set = tr_set.cache()
        # print('CheckC  - Volume: {} - Number of msisdn: {} - Number of label=1 {}'.format( tr_set.count(),
        #                                                                                    tr_set.select('msisdn').distinct().count(),
        #                                                                                    tr_set.where(col("label") == 1).count()))
        if calls_req == 1:
            tr_set = tr_set.where((col("VIRGINTELCO_sum_count") == 0) & (col("callscomp_VIRGIN_num_calls") == 0))
        else:
            tr_set = tr_set.where((col("VIRGINTELCO_sum_count") == 0))
        tr_set = tr_set.cache()
        volC =  tr_set.count()
        label1C = tr_set.where(col("label") == 1).count()
        print('CheckC  - Volume: {} - Number of label=1 {}'.format( volC, label1C))



        print("+++", closing_day, volA, label1A, volB1, label1B1, volB2, label1B2, volB3, label1B3, volB4, label1B4, volC, label1C)
        results[closing_day] = [volA, label1A, volB1, label1B1, volB2, label1B2, volB3, label1B3, volB4, label1B4, volC, label1C]

        print("RESULTADOS - {}".format(closing_day))
        pprint.pprint(results)
        print("ITER Elapsed time {} minutes".format((time.time() - start_time_iter) / 60.0))

    
        print("RESULTADOS - {} - CALLS_REQ={}".format(closing_day, calls_req))
        print("closing_day: volA, label1A, volB1, label1B1, volB2, label1B2, volB3, label1B3, volB4, label1B4, volC, label1C")
        pprint.pprint(results)
        
    print("Elapsed time {} minutes".format((time.time()-start_time_total)/60.0))
    '''
