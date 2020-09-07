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

NEW_PROD_FBB = "fbb"
NEW_PROD_TV = "tv"
NEW_PROD_MOB = "mobile"
NEW_PROD_FBB_TV = "fbb_tv"
NEW_PROD_PREPAID = "prepaid"
NEW_PROD_BAM_MOBILE = "bam_mobile"
NEW_PROD_FIXED = "fixed"
NEW_PROD_BAM = "bam"


PROD_TO_COL_DICT = {
                      NEW_PROD_FBB        : ['Cust_Agg_fbb_services_nc'],
                      NEW_PROD_MOB        : ['Cust_Agg_mobile_services_nc'],
                      NEW_PROD_TV         : ['Cust_Agg_tv_services_nc'],
                      NEW_PROD_FBB_TV     : ['Cust_Agg_fbb_services_nc', 'Cust_Agg_tv_services_nc'],
                      #NEW_PROD_PREPAID    : ['Cust_Agg_prepaid_services_nc'],
                      NEW_PROD_BAM_MOBILE : ['Cust_Agg_bam_mobile_services_nc'],
                      NEW_PROD_FIXED      : ['Cust_Agg_fixed_services_nc'],
                      NEW_PROD_BAM        : ['Cust_Agg_bam_services_nc']
                   }



PROD_TO_RGU_DICT = {
                      NEW_PROD_FBB        : ['fbb'],
                      NEW_PROD_MOB        : ['mobile'],
                      NEW_PROD_TV         : ['tv'],
                      NEW_PROD_FBB_TV     : ['fbb', 'tv'],
                   #   NEW_PROD_PREPAID    : ['prepaid'],
                      NEW_PROD_BAM_MOBILE : ['bam_mobile'],
                      NEW_PROD_FIXED      : ['fixed'],
                      NEW_PROD_BAM        : ['bam']
                   }


MAP_CHURN_NRT_TO_IDS = { 'nb_fbb_services_nc' : 'Cust_Agg_fbb_services_nc',
                         'nb_mobile_services_nc' : 'Cust_Agg_mobile_services_nc',
                         'nb_tv_services_nc' : 'Cust_Agg_tv_services_nc',
                    #     'nb_prepaid_services_nc' : 'Cust_Agg_prepaid_services_nc',
                         'nb_bam_mobile_services_nc' : 'Cust_Agg_bam_mobile_services_nc',
                         'nb_fixed_services_nc' : 'Cust_Agg_fixed_services_nc',
                         'nb_bam_services_nc' : 'Cust_Agg_bam_services_nc'}



#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 22G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096  /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/price_sensitivity/new_product/analysis/test_navcomp.py  > /var/SP/data/home/csanc109/logging/volumes_test.log

def compute_target_population(SPARK, closing_day, HORIZON_COMPARISON, HORIZON_CHECK, NEW_PRODUCT, VERSION):

    print("[NewProductData] build_module | date={} horizon_comp={} horizon_check={} product={} version={}".format(closing_day, HORIZON_COMPARISON, HORIZON_CHECK,
                                                                                                                                 NEW_PRODUCT, VERSION))

    closing_day_t2 = move_date_n_cycles(closing_day, n=HORIZON_COMPARISON)
    closing_day_t3 = move_date_n_cycles(closing_day, n=HORIZON_CHECK)

    print("[NewProductData] build_module | comparing sets from dates {} and {} and {}".format(closing_day, closing_day_t2, closing_day_t3))

    ####################################################################################
    # IDS for date closing_day
    ####################################################################################
    print("[NewProductData] build_module | Asking IDS for date1 = {}".format(closing_day))
    #df_date1 = get_ids_nc(SPARK, closing_day)

    all_products_cols = list(set(MAP_CHURN_NRT_TO_IDS.keys()))


    from churn_nrt.src.data.customer_base import CustomerBase
    df_date1 = CustomerBase(SPARK).get_module(closing_day, level="NUM_CLIENTE", save=False, save_others=False, add_tgs=False).select(*(["NUM_CLIENTE"] + all_products_cols)).drop_duplicates(["NUM_CLIENTE"])
    new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date1.columns]
    df_date1 = df_date1.toDF(*new_suffixed_cols)


    from churn_nrt.src.data.billing import Billing
    print("[NewProductData] build_module | Asking for billing module of date {}".format(closing_day))
    df_bill1 = Billing(SPARK).get_module(closing_day, save=False, save_others=False).select(*["NUM_CLIENTE", 'Bill_N1_Amount_To_Pay', 'Bill_N2_Amount_To_Pay', 'Bill_N3_Amount_To_Pay',
                                                                                              'Bill_N1_num_facturas', 'Bill_N2_num_facturas', 'Bill_N3_num_facturas'])

    df_date1 = df_date1.join(df_bill1, ['NUM_CLIENTE'], 'inner')

    ######################################################################################################################################
    # CLASSICS FILTERS - get_disconnection_process_filter and get_churn_call_filter
    ######################################################################################################################################

    print("[NewProductData] build_module | get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_active_filter = get_non_recent_customers_filter(SPARK, closing_day, 90, level="NUM_CLIENTE")

    print("[NewProductData] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

    tr_disconnection_filter = get_disconnection_process_filter(SPARK, closing_day, 90, level="num_cliente")

    print("[NewProductData] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

    tr_churn_call_filter = get_churn_call_filter(SPARK, closing_day, 90, 'num_cliente')

    df_date1 = df_date1.join(tr_active_filter, ['NUM_CLIENTE'], 'inner').join(tr_disconnection_filter, ['num_cliente'], 'inner').join(tr_churn_call_filter, ['num_cliente'], 'inner')

    df_date1 = df_date1.drop_duplicates(["NUM_CLIENTE"])
    ####################################################################################
    # Check number of bills > 0 for every client
    ####################################################################################

    df_date1 = df_date1.withColumn("num_bills_1_3", reduce(lambda a, b: a + b,
                                                           [when(((col("Bill_N{}_num_facturas".format(i)) > 0) & (col("Bill_N{}_Amount_To_Pay".format(i)) > 0)), 1.0).otherwise(0.0) for i in
                                                            range(1, 4)]))
    df_date1 = df_date1.where(col("num_bills_1_3") == 3)  # discard clients that had more than one bill per yearmonth or bills = 0
    #print("[NewProductData] build_module | After filter by num_bills_>_0 == 3 - count() = {} distinct() = {}".format(df_date1.count(), df_date1.select("num_cliente").distinct().count()))

    df_date1 = df_date1.withColumn("sum_bills_1_3", reduce(lambda a, b: a + b, [col("Bill_N{}_Amount_To_Pay".format(i)) for i in range(1, 4)]))
    df_date1 = df_date1.withColumn("avg_bill_d1", col("sum_bills_1_3") / 3.0)  # avg of the last three bills

    #######################################################################################################
    # KEEP CLIENTES WITH SOME KIND OF STABILITY IN THE BILLING - 2 out of the last 3 bills must be the same
    #######################################################################################################
    '''
    count_distinct_udf = udf(lambda milista: int(len(set(milista))) if milista else 0, IntegerType())
    df_date1 = df_date1.withColumn("num_diff_bills", count_distinct_udf(array(['Bill_N1_Amount_To_Pay', 'Bill_N2_Amount_To_Pay', 'Bill_N3_Amount_To_Pay'])))
    df_date1 = df_date1.where(col("num_diff_bills") == 2)
    print("[NewProductData] build_module | After filter by 2 different bills - count() = {} distinct() = {}".format(df_date1.count(), df_date1.select("num_cliente").distinct().count()))
    # This can be done due to previous condition (num_diff_bills=2)
    if VERSION != 4:
        df_date1 = df_date1.withColumn("last_diff_before_increment",
                                       when(col('Bill_N1_Amount_To_Pay') == col('Bill_N2_Amount_To_Pay'), col('Bill_L2_N1_N3_Amount_To_Pay')).otherwise(col('Bill_L2_N1_N2_Amount_To_Pay')))
    '''
    ####################################################################################
    # IDS for date closing_day_t2
    ####################################################################################


    print("[NewProductData] build_module | Asking IDS for date2 = {}".format(closing_day_t2))
    # df_date2 = get_ids_nc(self.SPARK, closing_day_t2)
    from churn_nrt.src.data.customer_base import CustomerBase
    df_date2 = CustomerBase(SPARK).get_module(closing_day_t2, save=False, save_others=False, level="NUM_CLIENTE", add_tgs=False).select(
        *(["NUM_CLIENTE"] + all_products_cols)).drop_duplicates(["NUM_CLIENTE"])

    #df_date2 = df_date2.select(*(["NUM_CLIENTE"] + all_products_cols))

    new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] + "_d2" if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date2.columns]
    df_date2 = df_date2.toDF(*new_suffixed_cols)
    #print("[NewProductData] build_module | df_date2 - count() = {} distinct() = {}".format(df_date2.count(), df_date2.select("num_cliente").distinct().count()))

    #######################################################################################################
    # SECOND INNER JOIN - to obtain the users who increased the number of products self.NEW_PRODUCT
    #######################################################################################################
    df = df_date1.join(df_date2, on=["NUM_CLIENTE"], how="inner")

    all_products_cols_ids = list(set([item for list_ in PROD_TO_COL_DICT.values() for item in list_]))

    # Compute differences in the number rv
    inc_d_d2_cols = [(col(col_ + "_d2") - col(col_)).alias("inc_" + col_) for col_ in all_products_cols_ids]
    print(inc_d_d2_cols)
    df = df.select(*(df.columns + inc_d_d2_cols))
    df = df.cache()
    # Check that quantity of products self.NEW_PRODUCT were increased
    print("[NewProductData] build_module | Before filter by new products segment - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))
    # FIXME check the number of increased products!
    df = df.where(reduce(and_, [col("inc_" + c) == 1 for c in PROD_TO_COL_DICT[NEW_PRODUCT]], lit(True)))
    print("[NewProductData] build_module | After filter by new products segment - count() = {}".format(df.count()))

    # Check that other products are not increased
    df = df.cache()
    rest_of_products = list(set(all_products_cols_ids) - set(PROD_TO_COL_DICT[NEW_PRODUCT]))
    print("[NewProductData] build_module | Checking that rest of products remain in the same quantity. Rest of products: {}".format(",".join(rest_of_products)))
    df = df.where(reduce(and_, [col("inc_" + c) == 0 for c in rest_of_products], lit(True)))
    print("[NewProductData] build_module | After filter by rest of products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

    #############
    ### CHECK STD BILL closing_day
    ############
    stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
    df = df.withColumn("aux_", array(["Bill_N{}_Amount_To_Pay".format(i) for i in range(1, 4)]))
    df = df.withColumn("billing_std", stddev_udf("aux_"))
    df = df.where(col("billing_std") <= 5)
    df = df.drop("billing_std", "aux_")

    ######################################################################################################################################
    # CHECK THAT USER PAID FOR THAT NEW PRODUCTS - check that the billing increased N1 vs N2 or N1 vs N3
    ######################################################################################################################################
    # Bill_L2_N1_N2_Amount_To_Pay = Bill_N1_Amount_To_Pay - Bill_N2_Amount_To_Pay
    # Bill_L2_N1_N3_Amount_To_Pay = Bill_N1_Amount_To_Pay - Bill_N3_Amount_To_Pay
    from churn_nrt.src.data.billing import Billing
    closing_day_billing = move_date_n_cycles(closing_day, n=20)
    print("[NewProductData] build_module | Asking for billing module of date {}".format(closing_day_billing))
    df_bill = Billing(spark).get_module(closing_day_billing, save=False, save_others=False).select(*["NUM_CLIENTE",
                                                        "Bill_N1_Amount_To_Pay", "Bill_N2_Amount_To_Pay", "Bill_N3_Amount_To_Pay",
                                                        "Bill_N1_num_facturas", "Bill_N2_num_facturas", "Bill_N3_num_facturas"])
    df_bill = df_bill.withColumn("num_bills_1_3_c16", reduce(lambda a, b: a + b,
                                                             [when(((col("Bill_N{}_num_facturas".format(i)) > 0) & (col("Bill_N{}_Amount_To_Pay".format(i)) > 0)), 1.0).otherwise(0.0) for i in
                                                              range(1, 4)]))
    df_bill = df_bill.where(col("num_bills_1_3_c16") == 3)  # discard clients that had more than one bill per yearmonth or bills = 0

    df_bill = df_bill.withColumn("sum_bills_1_3", reduce(lambda a, b: a + b, [col("Bill_N{}_Amount_To_Pay".format(i)) for i in range(1, 4)]))
    df_bill = df_bill.withColumn("avg_bill_c16", col("sum_bills_1_3") / 3.0)  # avg of the last three bills

    # from pyspark.sql.functions import udf
    stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
    df_bill = df_bill.withColumn("aux_", array(["Bill_N{}_Amount_To_Pay".format(i) for i in range(1, 4)]))
    df_bill = df_bill.withColumn("billing_std", stddev_udf("aux_"))

    df = df.join(df_bill.select("NUM_CLIENTE", "avg_bill_c16", "billing_std"), on=["NUM_CLIENTE"], how="inner")
    df = df.where((col("avg_bill_c16") - col("avg_bill_d1")) > 1)

    print("[NewProductData] build_module | After filter by users that had an increase in their billing - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

    ######################################################################################################################################
    # THIRD INNER JOIN - to obtain the users who kept the number of increased products for some cycles (self.HORIZON_CHECK) later
    ######################################################################################################################################
    print("[NewProductData] build_module | Asking IDS for date3 = {}".format(closing_day_t3))
    from churn_nrt.src.data.customer_base import CustomerBase
    df_date3 = CustomerBase(SPARK).get_module(closing_day_t3, level="NUM_CLIENTE", save=False, save_others=False, add_tgs=False).select(
        *(["NUM_CLIENTE"] + all_products_cols)).drop_duplicates(["NUM_CLIENTE"])
    print("[NewProductData] build_module | df_date3 - count() = {} distinct() = {}".format(df_date3.count(), df_date3.select("num_cliente").distinct().count()))

    # df_date3 = get_ids_nc(self.SPARK, closing_day_t3).select(*( ["NUM_CLIENTE"] + all_products_cols))
    new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] + "_d3" if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date3.columns]
    df_date3 = df_date3.toDF(*new_suffixed_cols)

    df = df.join(df_date3, on=["NUM_CLIENTE"], how="inner")
    inc_d2_d3_cols = [(col(col_ + "_d3") - col(col_ + "_d2")).alias("inc_check_" + col_) for col_ in all_products_cols_ids]
    print(inc_d2_d3_cols)
    df = df.select(*(df_date1.columns + inc_d2_d3_cols + ["avg_bill_c16", "billing_std"]))
    #df = df.coalesce(10)
    df = df.cache()
    print("[NewProductData] build_module | Before filter by users that keep the new products - count() = {}".format(df.count()))
    #df = df.cache()
    df = df.where(reduce(and_, (col("inc_check_" + c) == 0 for c in PROD_TO_COL_DICT[NEW_PRODUCT]), lit(True)))
    #print("[NewProductData] build_module | After filter by users that keep the new products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

    print("[NewProductData] build_module | Checking that rest of products remain in the same quantity. Rest of products: {}".format(",".join(rest_of_products)))
    df = df.where(reduce(and_, [col("inc_check_" + c) == 0 for c in rest_of_products], lit(True)))
    df = df.cache()
    print("[NewProductData] build_module | After filter by rest of products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

    df = df.drop_duplicates(["NUM_CLIENTE"])  # In case a module is stored more than once
    df = df.repartition(200)
    #print('[NewProductData] build_module | Tr set - Volume at the end of build_module is count()={} distinct() = {}'.format(df.count(), df.select("num_cliente").distinct().count()))
    df = df.cache()
    return df




#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 15G --driver-memory 30G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096  /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/price_sensitivity/new_product/analysis/test_navcomp.py --new_product mobile  > /var/SP/data/home/csanc109/logging/volumes_test_mobile_v5_6.log



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
    spark, sc = get_spark_session_noncommon("price_sens_new_product")
    sc.setLogLevel('WARN')

    import time
    start_time = time.time()

    from churn_nrt.src.data_utils.DataTemplate import DataTemplate
    from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate
    from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter
    from churn_nrt.src.utils.constants import MODE_EVALUATION
    from churn_nrt.src.utils.date_functions import move_date_n_cycles, move_date_n_days

    import argparse

    parser = argparse.ArgumentParser(description="Run price sensitivity model - new product", epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('--new_product', metavar='<new product>', type=str, required=True, default=None, help='product')
    args = parser.parse_args()
    print(args)

    NEW_PRODUCT = args.new_product

    closing_day = "20190714"
    HORIZON_COMPARISON = 6
    HORIZON_CHECK = 16
    #NEW_PRODUCT = "mobile"
    VERSION = 5

    results = []


    print(closing_day, HORIZON_CHECK, HORIZON_CHECK, NEW_PRODUCT, VERSION)




    while closing_day <= '20191207':

        # if closing_day in results.keys():
        #     closing_day = move_date_n_cycles(closing_day, n=1)
        #     continue

        print("***************", closing_day)
        df = compute_target_population(spark, closing_day, HORIZON_COMPARISON, HORIZON_CHECK, NEW_PRODUCT, VERSION)
        vol = df.count()
        results.append((closing_day, -1, vol))
        for ii in [0, 5, 10]:#, 15, 20, 25, 50, 75, 100, 125, 150, 175, 200]:
            results.append( (closing_day, ii, df.where(col("billing_std") <= ii).count()))

        print("----------")
        for ee in results:
            print("{}|{}|{}".format(ee[0], ee[1], ee[2]))
        #results[closing_day] = vol
        closing_day = move_date_n_cycles(closing_day, n=1)



    print(results)


    print("Elapsed time {} minutes".format((time.time()-start_time)/60.0))

