#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import desc, asc, sum as sql_sum, avg as sql_avg, isnull, when, col, isnan, count

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


if __name__ == "__main__":

    date_ = sys.argv[1]

    set_paths()

    sc, sparkSession, _ = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    verbose = True

    from pykhaos.utils.date_functions import move_date_n_days

    from churn.analysis.triggers.navcomp.navcomp_utils import get_navcomp_attributes

    from churn.analysis.triggers.navcomp.navcomp_utils import get_customer_base_navcomp

    from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests

    from churn.analysis.triggers.base_utils.base_utils import get_active_filter, get_disconnection_process_filter, get_churn_call_filter

    from churn.analysis.triggers.navcomp.navcomp_model import filter_population

    # Modeling filters

    tr_active_filter = get_active_filter(spark, date_, 90)

    tr_disconnection_filter = get_disconnection_process_filter(spark, date_, 90)

    tr_churn_call_filter = get_churn_call_filter(spark, date_, 90, 'msisdn')

    year_ = date_[0:4]

    month_ = str(int(date_[4:6]))

    day_ = str(int(date_[6:8]))

    starting_date = move_date_n_days(date_, n=-15)

    orig_navcomp_tr_df = get_navcomp_attributes(spark, starting_date, date_, 'msisdn')
    orig_navcomp_tr_df = orig_navcomp_tr_df.repartition(400)
    orig_navcomp_tr_df.cache()


    ############# Current process ###############

    navcomp_tr_df = orig_navcomp_tr_df


    #navcomp_tr_df = get_navcomp_attributes(spark, starting_date, date_, 'msisdn')
    #navcomp_tr_df = navcomp_tr_df.repartition(400)

    print '[Info navcomp_test] Current process - Tr set - The volume of navcomp_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())



    base_df = get_customer_base_navcomp(spark, date_, verbose)

    navcomp_tr_df = navcomp_tr_df.join(base_df, ['msisdn'], 'inner')



    end_port = move_date_n_days(date_, 15)

    target = get_mobile_portout_requests(spark, date_, end_port).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')

    navcomp_tr_df = navcomp_tr_df.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

    print '[Info navcomp_test] Current process - Total volume of the labeled set for ' + str(date_) + ' is ' + str(navcomp_tr_df.count()) + ' - Number of MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info navcomp_test] Current process - Churn proportion above (before filtering)'





    navcomp_tr_df = navcomp_tr_df \
        .join(tr_active_filter, ['msisdn'], 'inner') \
        .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tr_churn_call_filter, ['msisdn'], 'inner')



    navcomp_tr_df = filter_population(spark, navcomp_tr_df, 'comps')

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info navcomp_test] Current process - Churn proportion above (after filtering) '


    ############# Old process ###############

    navcomp_tr_df = None

    navcomp_tr_df = orig_navcomp_tr_df

    print '[Info navcomp_test] Old process - Tr set - The volume of navcomp_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    # Adding NIF and num_cliente IDs to the reference DF: only MSISDNs in the base are retained (inner join)

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    base_df = get_customer_base(spark, date_).filter(col('rgu') == 'mobile').select('msisdn', 'nif_cliente', 'num_cliente').distinct()

    navcomp_tr_df = navcomp_tr_df.join(base_df, ['msisdn'], 'inner')

    navcomp_tr_df = navcomp_tr_df.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

    print '[Info navcomp_test] Old process - Total volume of the labeled set for ' + str(date_) + ' is ' + str(navcomp_tr_df.count()) + ' - Number of MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info navcomp_test] Old process - Churn proportion above (before adding customer feats)'

    # Adding customer feats

    from churn.analysis.triggers.orders.customer_master import get_customer_master

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    customer_metadata = get_metadata(spark, sources=['customer'])

    customer_feats = customer_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['nif_cliente']

    customer_feats = ["segment_nif", "nb_rgus"]

    customer_map_tmp = customer_metadata.select("feature", "imp_value", "type").filter(col("feature").isin(customer_feats)).rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    customer_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in customer_map_tmp])

    customer_tr_df = get_customer_master(spark, date_, unlabeled=True) \
        .filter((col('segment_nif') != 'Pure_prepaid') & (col("segment_nif").isNotNull())) \
        .select(customer_feats + ['nif_cliente'])

    print '[Info navcomp_test] Old process - Tr set - The volume of customer_tr_df is ' + str(customer_tr_df.count()) + ' - The number of distinct NIFs is ' + str(customer_tr_df.select('nif_cliente').distinct().count())

    navcomp_tr_df = navcomp_tr_df.join(customer_tr_df, ['nif_cliente'], 'inner').na.fill(customer_map)


    print '[Info navcomp_test] Old process - Tr set - Volume after adding customer_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct NIFs is ' + str(navcomp_tr_df.select('nif_cliente').distinct().count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info navcomp_test] Old process - Churn proportion above (before filtering)'

    navcomp_tr_df = navcomp_tr_df \
        .join(tr_active_filter, ['msisdn'], 'inner') \
        .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tr_churn_call_filter, ['msisdn'], 'inner')

    from churn.analysis.triggers.navcomp.navcomp_model import filter_population

    navcomp_tr_df = filter_population(spark, navcomp_tr_df, 'comps')

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info navcomp_test] Old process - Churn proportion above (after filtering) '











