#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import time
from pyspark.sql.functions import count as sql_count, avg as sql_avg, when, sum as sql_sum, abs as sql_abs, udf, array, col, lit, udf
from operator import and_
from functools import reduce
from pyspark.sql.types import FloatType
import numpy as np

from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate, SET_NAME_TRAIN, SET_NAME_TEST
from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter, get_forbidden_orders_filter
from churn_nrt.src.utils.constants import MODE_EVALUATION, MODE_PRODUCTION
from churn_nrt.src.utils.date_functions import move_date_n_cycles, move_date_n_days
from churn_nrt.src.data_utils.ids_utils import get_ids_nc, get_ids
from churn_nrt.src.data.services_data import Service
from churn_nrt.src.projects.models.price_sensitivity.new_product.metadata import get_version
from pyspark.sql.types import IntegerType

NEW_PROD_FBB = "fbb"
NEW_PROD_TV = "tv"
NEW_PROD_MOB = "mobile"
NEW_PROD_FBB_TV = "fbb_tv"
NEW_PROD_PREPAID = "prepaid"
NEW_PROD_BAM_MOBILE = "bam_mobile"
NEW_PROD_FIXED = "fixed"
NEW_PROD_BAM = "bam"
NEW_PROD_FBB_MOBILE = "fbb_mobile"

PROD_TO_COL_DICT = {
                      NEW_PROD_FBB        : ['Cust_Agg_fbb_services_nc'],
                      NEW_PROD_MOB        : ['Cust_Agg_mobile_services_nc'],
                      NEW_PROD_TV         : ['Cust_Agg_tv_services_nc'],
                      NEW_PROD_FBB_TV     : ['Cust_Agg_fbb_services_nc', 'Cust_Agg_tv_services_nc'],
                      NEW_PROD_FBB_MOBILE : ['Cust_Agg_fbb_services_nc', 'Cust_Agg_mobile_services_nc'],
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



BASE_EVAL = "eval"
BASE_COMP = "comp"

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class NewProductData(DataTemplate): # unlabeled


    VERSION = None
    NEW_PRODUCT = None
    HORIZON_COMPARISON = None # in cycles, horizon to compare T with T+horizon_comparison
    # version < 10 : in cycles, horizon to compare T with T+horizon_check to check if the user still keeps the new products
    # version >= 10 : in cycles, T+horizon to compare billing
    HORIZON_CHECK = None
    BASE = None

    def __init__(self, spark, new_product, horizon_comp, horizon_check, version=1, base=BASE_EVAL):

        self.VERSION = version
        self.NEW_PRODUCT = new_product
        self.HORIZON_COMPARISON = horizon_comp
        self.HORIZON_CHECK = horizon_check
        self.BASE = base

        if self.BASE not in [BASE_COMP, BASE_EVAL]:
            print("[NewProductData] __init__ | Unknown value {} for parameter base".format(base))
            import sys
            sys.exit(1)

        path_to_save = "pricesens_newproduct/{}/h{}/h{}/v{}".format(self.NEW_PRODUCT, self.HORIZON_COMPARISON, self.HORIZON_CHECK, self.VERSION)
        if self.BASE == BASE_COMP:
            path_to_save += "_dist"

        DataTemplate.__init__(self, spark, path_to_save)

    def build_module(self, closing_day, save_others, force_gen=False, select_cols=None, sources=None, *kwargs):

        print("[NewProductData] build_module | date={} horizon_comp={} horizon_check={} product={} version={} base={} and verbose={}".format(closing_day,
                                                                                                    self.HORIZON_COMPARISON,
                                                                                                    self.HORIZON_CHECK,
                                                                                                    self.NEW_PRODUCT,
                                                                                                    self.VERSION,
                                                                                                    self.BASE,
                                                                                                    self.VERBOSE))

        closing_day_t2 = move_date_n_cycles(closing_day, n=self.HORIZON_COMPARISON)
        closing_day_t3 = move_date_n_cycles(closing_day, n=self.HORIZON_CHECK)

        if self.VERSION >= 10 and self.HORIZON_COMPARISON != 0:
            print("[NewProductData] build_module | version {} must be executed with horizon_comparison=0".format(self.VERSION))
            sys.exit(1)

        if self.VERSION < 10:
            print("[NewProductData] build_module | comparing sets from dates {} and {} and {}".format(closing_day, closing_day_t2, closing_day_t3))

        ####################################################################################
        # IDS for date closing_day
        ####################################################################################
        print("[NewProductData] build_module | Asking IDS for date1 = {}".format(closing_day))
        df_date1 = get_ids_nc(self.SPARK, closing_day)

        if self.VERSION > 0:
            ######################################################################################################################################
            # CLASSICS FILTERS - get_disconnection_process_filter and get_churn_call_filter
            ######################################################################################################################################

            print("[NewProductData] build_module | get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

            tr_active_filter = get_non_recent_customers_filter(self.SPARK, closing_day, 90,  level="NUM_CLIENTE")

            print("[NewProductData] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

            tr_disconnection_filter = get_disconnection_process_filter(self.SPARK, closing_day, 90, level="num_cliente")

            print("[NewProductData] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

            tr_churn_call_filter = get_churn_call_filter(self.SPARK, closing_day, 90, 'num_cliente')

            print("[NewProductData] get_set | - - - - - - - -  get_forbidden_orders_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

            tr_forbidden = get_forbidden_orders_filter(self.SPARK, closing_day, level="num_cliente", verbose=False, only_active=True)

            df_date1 = df_date1.join(tr_active_filter, ['NUM_CLIENTE'], 'inner').join(tr_disconnection_filter, ['num_cliente'], 'inner').join(tr_churn_call_filter, ['num_cliente'], 'inner').join(tr_forbidden, ['num_cliente'], 'inner')

            print("[NewProductData] build_module | After all the classic filters - count() = {} distinct() = {}".format(df_date1.count(), df_date1.select("num_cliente").distinct().count()))

            #######################################################################################################
            # CHECKS - remove num_clients that ends and starts a discount within the analysis period
            #######################################################################################################
            # Remove users that have a discount that ends before the billing comparison
            # n_cycles_billing = 20 if self.VERSION < 10 else self.HORIZON_CHECK
            # df_date1 = df_date1.where(col("tgs_days_until_fecha_fin_dto_agg_mean") > (n_cycles_billing*7))
            #
            # print("[NewProductData] build_module | After removing clientes that ends a discount - count() = {}".format(df_date1.count()))
            #
            # # Remove users that have a discount that ended before the billing comparison
            # closing_day_billing = move_date_n_cycles(closing_day, n=n_cycles_billing)
            # from churn_nrt.src.data.customer_base import CustomerBase
            # df_cb = CustomerBase(self.SPARK).get_module(closing_day_billing, level="NUM_CLIENTE", save=True, save_others=False, add_tgs=True).select(*(["NUM_CLIENTE", "tgs_days_until_f_fin_bi"])).drop_duplicates(["NUM_CLIENTE"])
            # df_cb = df_cb.withColumnRenamed("tgs_days_until_f_fin_bi", "tgs_days_until_f_fin_bi_billing")
            # df_date1 = df_date1.join(df_cb, on=["NUM_CLIENTE"], how="left").fillna({"tgs_days_until_f_fin_bi_billing" : -10000}) # imputation value is -10000
            # df_date1 = df_date1.where(   (col("tgs_days_until_fecha_fin_dto_agg_mean") < -1000) & (col("tgs_days_until_f_fin_bi_billing") > 0))
            # print("[NewProductData] build_module | After removing clientes that starts a discount - count() = {} distinct() = {}".format(df_date1.count(), df_date1.select("num_cliente").distinct().count()))


        # List of all columns with the number of services by rgu
        all_products_cols = list(set([item for list_ in PROD_TO_COL_DICT.values() for item in list_]))

        if self.VERSION == 11 or self.VERSION==7: # Global potential + check no purchases in previous 12 cycles

            closing_day_pruchases = move_date_n_cycles(closing_day, n=-12)
            print("[NewProductData] build_module | Getting services info for closing_day - 12 cycles.".format(closing_day_pruchases))

            print("[NewProductData] build_module | Asking IDS for date4 = {}".format(closing_day_pruchases))
            from churn_nrt.src.data.customer_base import CustomerBase
            df_date4 = CustomerBase(self.SPARK).get_module(closing_day_pruchases, level="NUM_CLIENTE", save=False, save_others=False, add_tgs=False).select(*(["NUM_CLIENTE"] + MAP_CHURN_NRT_TO_IDS.keys())).drop_duplicates(["NUM_CLIENTE"])
            new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] + "_d4" if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date4.columns]
            df_date4 = df_date4.toDF(*new_suffixed_cols)

            df = df_date1.join(df_date4, on=["NUM_CLIENTE"], how="inner")
            inc_d_d4_cols = [(col(col_ + "_d4") - col(col_)).alias("inc_purchase_" + col_) for col_ in all_products_cols]
            print(inc_d_d4_cols)
            df = df.select(* (df.columns + inc_d_d4_cols) )
            df = df.cache()
            print("[NewProductData] build_module | Before filter by users that purchase new products before closing_day - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))
            df = df.where(reduce(and_, (col("inc_purchase_"+c) == 0 for c in all_products_cols), lit(True)))
            print("[NewProductData] build_module | After filter by users that purchase new products before closing_day  - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

        else:
            df =df_date1


        if self.BASE == BASE_COMP:

            print("[NewProductData] get_set | base={} no more filters are going to be applied".format(BASE_COMP))

            if select_cols:
                print("[NewProductData] build_module | Selecting columns: {}".format(",".join(select_cols)))
                df = df.select(*(select_cols))

            df = df.drop_duplicates(["NUM_CLIENTE"])  # In case a module is stored more than once
            df = df.repartition(200)
            return df



        ####################################################################################
        # Check number of bills > 0 for every client
        ####################################################################################
        df = df.withColumn("num_bills_1_3", reduce(lambda a, b: a + b, [when(((col("Bill_N{}_num_facturas".format(i)) > 0) & (col("Bill_N{}_Amount_To_Pay".format(i)) > 0)), 1.0).otherwise(0.0) for i in range(1, 4)]))
        if self.VERSION >= 4 and not self.VERSION in [101]:
            df = df.where(col("num_bills_1_3")==3) # discard clients that had more than one bill per yearmonth or bills = 0
            print("[NewProductData] build_module | After filter by num_bills_>_0 == 3 - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))


        df = df.fillna({"Bill_N1_Amount_To_Pay" : 0.0, "Bill_N2_Amount_To_Pay" : 0.0, "Bill_N3_Amount_To_Pay" : 0.0})
        df = df.withColumn("sum_bills_1_3", reduce(lambda a, b: a + b, [col("Bill_N{}_Amount_To_Pay".format(i)) for i in range(1, 4)]))
        df = df.withColumn("avg_bill_d1", col("sum_bills_1_3") / 3.0) # avg of the last three bills


        if self.VERSION < 10:

            ####################################################################################
            # IDS for date closing_day_t2
            ####################################################################################
            print("[NewProductData] build_module | Asking IDS for date2 = {}".format(closing_day_t2))
            from churn_nrt.src.data.customer_base import CustomerBase
            df_date2 = CustomerBase(self.SPARK).get_module(closing_day_t2, save=False, save_others=False, level="NUM_CLIENTE", add_tgs=False).select(*(["NUM_CLIENTE"] + MAP_CHURN_NRT_TO_IDS.keys())).drop_duplicates(["NUM_CLIENTE"])

            new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] + "_d2" if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date2.columns]
            df_date2 = df_date2.toDF(*new_suffixed_cols)

            #######################################################################################################
            # SECOND INNER JOIN - to obtain the users who increased the number of products self.NEW_PRODUCT
            #######################################################################################################
            df = df.join(df_date2, on=["NUM_CLIENTE"], how="inner")

            inc_d_d2_cols = [(col(col_ + "_d2") - col(col_)).alias("inc_" + col_) for col_ in all_products_cols]
            print(inc_d_d2_cols)
            df = df.select(*( df.columns + inc_d_d2_cols ))
            df = df.where(reduce(and_, [col("inc_"+c) == 1 for c in PROD_TO_COL_DICT[self.NEW_PRODUCT]], lit(True)))
            df = df.cache()
            print("[NewProductData] build_module | After filter by new products segment - count() = {}".format(df.count()))

            # Check that other products are not increased
            rest_of_products = list(set(all_products_cols) - set(PROD_TO_COL_DICT[self.NEW_PRODUCT]))
            print("[NewProductData] build_module | Checking that rest of products remain in the same quantity. Rest of products: {}".format(",".join(rest_of_products)))
            df = df.where(reduce(and_, [col("inc_"+c)==0 for c in rest_of_products], lit(True)))
            print("[NewProductData] build_module | After filter by rest of products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))


        exit_if_version(self.VERSION, [1], df, select_cols=select_cols)


        ######################################################################################################################################
        # CHECK THAT Billing did not vary between the last three cycles
        ######################################################################################################################################
        if self.VERSION >= 5 and not self.VERSION in [101, 102]:
            stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
            df = df.withColumn("aux_", array(["Bill_N{}_Amount_To_Pay".format(i) for i in range(1, 4)]))
            df = df.withColumn("billing_std", stddev_udf("aux_"))
            df = df.where(col("billing_std") <= 5)
            df = df.drop("billing_std", "aux_")
            print("[NewProductData] build_module | After filter by similar bill - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))


        ######################################################################################################################################
        # CHECK THAT BILLING INCREASED
        ######################################################################################################################################
        if self.VERSION >= 2:
            n_cycles_billing = 20 if self.VERSION < 10 else self.HORIZON_CHECK

            print("[NewProductData] build_module | Getting bill info after {} cycles.".format(n_cycles_billing))

            from churn_nrt.src.data.billing import Billing
            closing_day_billing = move_date_n_cycles(closing_day, n=n_cycles_billing)

            print("[NewProductData] build_module | Asking for billing module of date {}".format(closing_day_billing))
            df_bill = Billing(self.SPARK).get_module(closing_day_billing, save=False, save_others=False).select(*["NUM_CLIENTE", "Bill_N1_Amount_To_Pay", "Bill_N2_Amount_To_Pay", "Bill_N3_Amount_To_Pay",
                                                                                                                  "Bill_N1_num_facturas", "Bill_N2_num_facturas", "Bill_N3_num_facturas"])
            df_bill = df_bill.withColumn("num_bills_1_3_cbilling", reduce(lambda a, b: a + b,
                                                                     [when(((col("Bill_N{}_num_facturas".format(i)) > 0) & (col("Bill_N{}_Amount_To_Pay".format(i)) > 0)), 1.0).otherwise(0.0) for i in
                                                                      range(1, 4)]))
            df_bill = df_bill.fillna({"Bill_N1_Amount_To_Pay": 0.0, "Bill_N2_Amount_To_Pay": 0.0, "Bill_N3_Amount_To_Pay": 0.0})

            if self.VERSION >= 6 and not self.VERSION in [101, 102, 103]:
                df_bill = df_bill.where(col("num_bills_1_3_cbilling") == 3)  # discard clients that had more than one bill per yearmonth or bills = 0

            df_bill = df_bill.withColumn("sum_bills_1_3", reduce(lambda a, b: a + b, [col("Bill_N{}_Amount_To_Pay".format(i)) for i in range(1, 4)]))
            df_bill = df_bill.withColumn("avg_bill_after", col("sum_bills_1_3") / 3.0) # avg of the last three bills

            stddev_udf = udf(lambda milista: float(np.std(milista)), FloatType())
            df_bill = df_bill.withColumn("aux_", array(["Bill_N{}_Amount_To_Pay".format(i) for i in range(1, 4)]))
            df_bill = df_bill.withColumn("billing_std", stddev_udf("aux_"))
            if self.VERSION >= 7 and not self.VERSION in [101, 102, 103, 104]:
                df_bill = df_bill.where(col("billing_std") <= 5)

            df = df.join(df_bill.select("NUM_CLIENTE", "avg_bill_after", "billing_std"), on=["NUM_CLIENTE"], how="inner")
            if self.VERSION >= 3 and self.VERSION<10:
                # be sure there is an increment in the bill
                df = df.where((col("avg_bill_after") - col("avg_bill_d1")) > 1)
                #print("[NewProductData] build_module | After filter by users that had an increase in their billing - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

        if self.VERSION < 10 and self.VERSION >=2:
            ######################################################################################################################################
            # THIRD INNER JOIN - to obtain the users who kept the number of increased products for some cycles (self.HORIZON_CHECK) later
            ######################################################################################################################################
            print("[NewProductData] build_module | Asking IDS for date3 = {}".format(closing_day_t3))
            from churn_nrt.src.data.customer_base import CustomerBase
            df_date3 = CustomerBase(self.SPARK).get_module(closing_day_t3, level="NUM_CLIENTE", save=False, save_others=False, add_tgs=False).select(*(["NUM_CLIENTE"] + MAP_CHURN_NRT_TO_IDS.keys())).drop_duplicates(["NUM_CLIENTE"])
            new_suffixed_cols = [MAP_CHURN_NRT_TO_IDS[col_] + "_d3" if col_ not in ["NUM_CLIENTE"] else col_ for col_ in df_date3.columns]
            df_date3 = df_date3.toDF(*new_suffixed_cols)

            df = df.join(df_date3, on=["NUM_CLIENTE"], how="inner")
            inc_d2_d3_cols = [(col(col_ + "_d3") - col(col_ + "_d2")).alias("inc_check_" + col_) for col_ in all_products_cols]
            print(inc_d2_d3_cols)
            df = df.select(* (df_date1.columns + inc_d2_d3_cols + ["avg_bill_after", "billing_std"]) )
            df = df.cache()
            print("[NewProductData] build_module | Before filter by users that keep the new products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))
            df = df.where(reduce(and_, (col("inc_check_"+c) == 0 for c in PROD_TO_COL_DICT[self.NEW_PRODUCT]), lit(True)))
            print("[NewProductData] build_module | After filter by users that keep the new products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

            print("[NewProductData] build_module | Checking that rest of products remain in the same quantity. Rest of products: {}".format(",".join(rest_of_products)))
            df = df.where(reduce(and_, [col("inc_check_"+c)==0 for c in rest_of_products], lit(True)))
            print("[NewProductData] build_module | After filter by rest of products - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))


        # if self.VERSION == 3:
        #     rgu_tag = PROD_TO_RGU_DICT[self.NEW_PRODUCT]
        #     print("[NewProductData] build_module | Looking for new products for rgu = {}".format(rgu_tag))
        #     df_new_serv = get_new_services(self.SPARK, closing_day, closing_day_t2, rgu_tag)
        #     df = df.join(df_new_serv, on=["num_cliente"], how="inner")
        #     df = df.cache()
        #     print("[NewProductData] build_module | After filter by new services - count() = {} distinct() = {}".format(df.count(), df.select("num_cliente").distinct().count()))

        if select_cols:
            print("[NewProductData] build_module | Selecting columns: {}".format(",".join(select_cols)))
            df = df.select(*(select_cols))

        df = df.drop_duplicates(["NUM_CLIENTE"]) # In case a module is stored more than once

        if self.VERSION > 10:
            print('[NewProductData] build_module | Tr set - Volume at the end of build_module is count()={} distinct() = {}'.format(df.count(), df.select("num_cliente").distinct().count()))

        return df


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class NewProductModel(ModelTemplate):

    VERSION = None
    NEW_PRODUCT = None
    HORIZON_COMPARISON = None # in cycles, horizon to compare T with T+horizon_comparison
    HORIZON_CHECK = None # in cycles, horizon to compare T with T+horizon_check to check if the user still keeps the new products
    BASE = BASE_EVAL # If "eval", test set is build in the same conditions as train set. If "distr" the whole base is passed through filters
    LABEL_TYPE = 1

    def __init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, new_product,
                 horizon_comp, horizon_check, base=BASE_EVAL, label_type=1, force_gen=False):
        '''

        :param spark:
        :param tr_date:
        :param tt_date:
        :param mode_:
        :param owner_login:
        :param metadata_obj:
        :param force_gen:
        '''
        ModelTemplate.__init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, force_gen=force_gen)

        self.VERSION = get_version(metadata_obj.METADATA_SOURCES)
        self.NEW_PRODUCT = new_product
        self.HORIZON_COMPARISON = horizon_comp
        self.HORIZON_CHECK = horizon_check
        self.BASE = base
        self.LABEL_TYPE = label_type

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None, *args, **kwargs):

        print("- - - - - - - - get_set for closing_day={} labeled={} set_name={} label_type={} - - - - - - - -".format(closing_day, labeled, set_name, self.LABEL_TYPE))

        # Note: BASE_COMP only applies to test set
        if set_name == SET_NAME_TRAIN:
            base_ = BASE_EVAL
        else: # test
            if self.MODE == MODE_PRODUCTION:
                base_ = BASE_COMP # whole base
            else: # test set in evaluation mode follows the parameter
                base_ = self.BASE


        if self.VERSION >= 10 and self.HORIZON_COMPARISON != 0:
            print("[NewProductModel] get_set | version {} must be executed with horizon_comparison=0".format(self.VERSION))
            import sys
            sys.exit(1)

        df_tr_newprod = NewProductData(self.SPARK, self.NEW_PRODUCT, self.HORIZON_COMPARISON, self.HORIZON_CHECK, version=self.VERSION, base=base_).get_module(closing_day,
                                                                             force_gen=self.FORCE_GEN,
                                                                             save_others=False,
                                                                             save=True,
                                                                             sources=self.METADATA.METADATA_SOURCES)
        # old versions has this column name -
        if "avg_bill_c16" in df_tr_newprod.columns:
            df_tr_newprod = df_tr_newprod.withColumn("avg_bill_after", col("avg_bill_c16"))



        if labeled:
            if self.VERSION < 10 and self.VERSION >= 5:
                #only when labeled, we have the std in D+20 cycles
                print("[NewProductModel] get_set | Applying filter by std <=5")
                df_tr_newprod = df_tr_newprod.where(col("billing_std") <= 5)

        print("[NewProductModel] get_set | Applying cache() after all the filters")
        df_tr_newprod = df_tr_newprod.cache()

        vol_set = df_tr_newprod.count()

        if  vol_set == 0:
            print("[NewProductModel] get_set | After all the filters dataframe for date {} is empty".format(closing_day))
            import sys
            sys.exit(1)

        print "[NewProductModel] get_set | After all the filters - count on set - date = {} -- count = {}".format(closing_day, vol_set)


        if labeled:

            print("[NewProductModel] get_set | Adding label column with label type {}".format(self.LABEL_TYPE))

            if self.LABEL_TYPE == 1:
                df_tr_newprod = df_tr_newprod.withColumn("label", col("avg_bill_after") - col("avg_bill_d1"))
            elif self.LABEL_TYPE == 2:
                df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                df_tr_newprod = df_tr_newprod.withColumn("label", 100.0 * col("bill_inc") /  col("avg_bill_d1")) # relative increment
            elif self.LABEL_TYPE == 3:
                if self.VERSION == 7 and self.NEW_PRODUCT == NEW_PROD_MOB:
                    print("[NewProductModel] get_set | label_type=3 Fixed threshold for categories [-inf, 10.487, 20.504, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<10.487,0).when(col("label_cont")>20.504,2).otherwise(1))

                elif self.VERSION == 7 and self.NEW_PRODUCT == NEW_PROD_TV:
                    print("[NewProductModel] get_set | label_type=3 Fixed threshold for categories [-inf, 5.585, 11.051, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<5.585,0).when(col("label_cont")>11.051,2).otherwise(1))

                elif self.VERSION == 10:
                    print("[NewProductModel] get_set | label_type=3 Fixed threshold for categories [-inf, 0.402, 3.724, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<0.402,0).when(col("label_cont")>3.724,2).otherwise(1))
                else: # self.VERSION != 7:
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))

                    var = "label_cont"
                    var_disc = 'label'
                    num_buckets = 3 if self.LABEL_TYPE == 3 else 2

                    from pyspark.ml.feature import QuantileDiscretizer
                    discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=var, outputCol=var_disc, relativeError=0)
                    bucketizer = discretizer.fit(df_tr_newprod)
                    df_tr_newprod = bucketizer.transform(df_tr_newprod)
                    #mysplit = bucketizer.getSplits()
                    print("[NewProductModel] get_set | Discretization of the label var")
                    print(bucketizer.getSplits())

            elif self.LABEL_TYPE == 6.1 or  self.LABEL_TYPE == 6.2: # discretize label
                df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))

                if self.LABEL_TYPE == 6.1:  # keep only positive samples
                    print("__CSANC109_MODEL_LABEL_{} fixed threshold in 5 euros".format(self.LABEL_TYPE))
                    avg_label_cont = 5.0
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont") > avg_label_cont, 1).otherwise(0))
                else:
                    print("__CSANC109_MODEL_LABEL_{} fixed threshold in -5 euros".format(self.LABEL_TYPE))
                    avg_label_cont = -5.0
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont") > avg_label_cont, 1).otherwise(0))
                # var = "label_cont"
                # var_disc = 'label'
                # num_buckets = 2
                #
                # from pyspark.ml.feature import QuantileDiscretizer
                # discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=var, outputCol=var_disc, relativeError=0)
                # bucketizer = discretizer.fit(df_tr_newprod)
                # df_tr_newprod = bucketizer.transform(df_tr_newprod)
                # #mysplit = bucketizer.getSplits()
                # print("[NewProductModel] get_set | Discretization of the label var")
                # print(bucketizer.getSplits())

            elif self.LABEL_TYPE == 4: #

                if self.VERSION == 7  and self.NEW_PRODUCT == NEW_PROD_MOB:
                    print("[NewProductModel] get_set | label_type=4 Fixed threshold for categories [-inf, 17.776, 41.741, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", 100.0 * col("bill_inc") /  col("avg_bill_d1")) # relative increment
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<17.776,0).when(col("label_cont")>41.741,2).otherwise(1))
                elif self.VERSION == 7  and self.NEW_PRODUCT == NEW_PROD_TV:
                    print("[NewProductModel] get_set | label_type=4 Fixed threshold for categories [-inf, 10.296, 21.444, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", 100.0 * col("bill_inc") /  col("avg_bill_d1")) # relative increment
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<10.296,0).when(col("label_cont")>21.444,2).otherwise(1))
                elif self.VERSION == 10:
                    print("[NewProductModel] get_set | label_type=4 Fixed threshold for categories [-inf, 0.916, 6.771, inf]​")
                    df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", 100.0 * col("bill_inc") /  col("avg_bill_d1")) # relative increment
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont")<0.916,0).when(col("label_cont")>6.771,2).otherwise(1))
                else:
                    df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", 100.0 * col("bill_inc") /  col("avg_bill_d1")) # relative increment

                    var = "label_cont"
                    var_disc = 'label'
                    num_buckets = 3

                    from pyspark.ml.feature import QuantileDiscretizer
                    discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=var, outputCol=var_disc, relativeError=0)
                    bucketizer = discretizer.fit(df_tr_newprod)
                    df_tr_newprod = bucketizer.transform(df_tr_newprod)
                    #mysplit = bucketizer.getSplits()
                    print("[NewProductModel] get_set | Discretization of the label var")
                    print(bucketizer.getSplits())
            elif self.LABEL_TYPE == 5:
                df_tr_newprod = df_tr_newprod.withColumn("label", col("avg_bill_after") - col("avg_bill_d1"))
                df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label")>0,1).otherwise(0))

            elif int(self.LABEL_TYPE) == 7 or int(self.LABEL_TYPE) == 8: # 7.x means absolute increment and 8.x means relative increment

                if int(self.LABEL_TYPE) == 7:
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", col("avg_bill_after") - col("avg_bill_d1"))
                elif int(self.LABEL_TYPE) == 8:
                    df_tr_newprod = df_tr_newprod.withColumn("bill_inc", col("avg_bill_after") - col("avg_bill_d1"))
                    df_tr_newprod = df_tr_newprod.withColumn("label_cont", 100.0 * col("bill_inc") / col("avg_bill_d1"))  # relative increment

                # train: 1: top5-10%
                #        0: bottom 50%
                # test:  1: top5-10%
                #        0: resto

                # the percentage of bottom is the float part of the label type... trick...
                qq_top = self.LABEL_TYPE - int(self.LABEL_TYPE)
                print("[NewProductModel] get_set | label_type={} Marking as top the {}%".format(self.LABEL_TYPE, 100.0*qq_top))
                thresh_top = df_tr_newprod.approxQuantile("label_cont", [1.0 - qq_top], 0.000001)[0]
                print("[NewProductModel] get_set | label_type={} thresh_top={}".format(self.LABEL_TYPE, thresh_top))

                if set_name == SET_NAME_TRAIN:

                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont") >= thresh_top, 1.0).otherwise(-1.0))

                    qq_bottom = 0.5
                    print("[NewProductModel] get_set | label_type={} Marking as bottom the {}%".format(self.LABEL_TYPE, 100.0 * qq_bottom))
                    thresh_bottom = df_tr_newprod.approxQuantile("label_cont", [qq_bottom], 0.000001)[0]
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont") <= thresh_bottom, 0.0).otherwise(col("label")))
                    print("[NewProductModel] get_set | label_type={} thresh_bottom={}".format(self.LABEL_TYPE, thresh_bottom))

                    volume_before = df_tr_newprod.count()
                    print("[NewProductModel] get_set | label_type={} set_name={} Before filtering unset labels".format(self.LABEL_TYPE, set_name))
                    df_tr_newprod.groupBy('label').agg(sql_count('*').alias('num_samples')).show()

                    df_tr_newprod = df_tr_newprod.where(col("label")>=0)
                    print("[NewProductModel] get_set | label_type={} set_name={} filtering entries without label. Before={} After={}".format(self.LABEL_TYPE, set_name,
                                                                                                                                             volume_before, df_tr_newprod.count()))

                else:
                    df_tr_newprod = df_tr_newprod.withColumn("label", when(col("label_cont") >= thresh_top, 1.0).otherwise(0.0))



            else:
                print("[NewProductModel] get_set | Unknown label type {}. Program will exit here!".format(self.LABEL_TYPE))
                import sys
                sys.exit(1)

            if self.LABEL_TYPE in [1, 2] and self.VERSION < 10:  # regression problem
                print("WITHOUT FILTERING OUTLIERS")
                show_label_distribution(df_tr_newprod.select("label"))
                print("[NewProductModel] get_set | without removing outliers: METRICS for label in closing_day {} ".format(closing_day))
                print(" | ".join(df_tr_newprod.select("label").describe().rdd.map(lambda x: "{}: {:.2f}".format(x[0], float(x[1]))).collect()))


            # --- show a summary of the label columns
            if self.LABEL_TYPE in [1,2] and self.VERSION<10: # regression problem

                qq = 0.90
                qq1 = df_tr_newprod.approxQuantile("label", [qq], 0.000001)[0]

                # if self.VERSION >=10:
                #     #remove outliers in negative
                #     qq = 0.10
                #     print("[NewProductModel] get_set | Removing outliers: qq2 {} %".format(100.0 * qq))
                #     #compute this quantile before removing by qq1
                #     qq2 = df_tr_newprod.approxQuantile("label", [qq], 0.000001)[0]
                #     df_tr_newprod = df_tr_newprod.where(col("label") >= qq2)

                print("[NewProductModel] get_set | Removing outliers: qq1 {} %".format(100.0 * qq1))
                df_tr_newprod = df_tr_newprod.where(col("label") <= qq1)


                #--- show a summary of the label columns
                print("AFTER FILTERING OUTLIERS")
                show_label_distribution(df_tr_newprod.select("label"))
                df_tr_newprod.select("label").describe().show()
                print("[NewProductModel] get_set | After removing outliers: METRICS for label in closing_day {} ".format(closing_day))
                print(" | ".join(df_tr_newprod.select("label").describe().rdd.map(lambda x: "{}: {}".format(x[0], x[1])).collect()))



        if self.MODE == MODE_EVALUATION and (self.LABEL_TYPE in [3,4,5,6, 6.1, 6.2] or (int(self.LABEL_TYPE)==7) or (int(self.LABEL_TYPE)==8)):
            print "[NewProductModel] get_set | After all the filters - Label {} count on set - date = {}".format(self.LABEL_TYPE, closing_day)
            df_tr_newprod.groupBy('label').agg(sql_count('*').alias('num_samples')).show()


        df_tr_newprod = self.METADATA.fillna(df_tr_newprod, sources=['pricesens_newprod{}'.format(self.VERSION)])

        #all_cols = get_feat_cols(self.SPARK, closing_day, self.VERSION)
        all_cols = self.METADATA.get_cols(type_="all")
        # IDS columns varies from time to time... just to be sure "all_cols" contains columns existing in df
        all_cols = list(set(all_cols) & set(df_tr_newprod.columns))

        if labeled:
            df_tr_newprod = df_tr_newprod.select(all_cols + [self.LABEL_COL])
        else:
            df_tr_newprod = df_tr_newprod.select(all_cols)

        df_tr_newprod = df_tr_newprod.repartition(400)
        df_tr_newprod = df_tr_newprod.cache()
        print("[NewProductData] get_set | At the end count() = {} ".format(df_tr_newprod.count()))

        return df_tr_newprod



def show_label_distribution(df):
    df_sum = df.select("label")
    var = 'label'
    var_disc = 'label_disc'
    num_buckets = 10

    from pyspark.ml.feature import QuantileDiscretizer
    discretizer = QuantileDiscretizer(numBuckets=num_buckets, inputCol=var, outputCol=var_disc, relativeError=0)
    bucketizer = discretizer.fit(df_sum)
    df_sum = bucketizer.transform(df_sum)
    mysplit = bucketizer.getSplits()
    print(bucketizer.getSplits())

    df_sum = df_sum.withColumn("label_disc_", col(var_disc))
    labels_dict = {ii: "[{0:.2f},{1:.2f})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
    for num, label in labels_dict.items():
        df_sum = df_sum.withColumn("label_disc_", when(col("label_disc_") == num, labels_dict[num]).otherwise(col("label_disc_")))

    print("[modeler] get_set | label distribution:")
    df_sum.groupby("label_disc", "label_disc_").agg(sql_count("*").alias("count")).sort("label_disc").show()

def add_gnv_volume(spark, df, closing_day):

    df_msisdn = get_ids(spark, closing_day)
    cols_gnv_calls = [col_ for col_ in df_msisdn.columns if re.match("GNV_Type_Voice_MOVILES_(.*)_Num_Of_Calls", col_)]
    cols_gnv_mou = [col_ for col_ in df_msisdn.columns if re.match("GNV_Type_Voice_MOVILES_(.*)_MOU", col_)]

    df_msisdn = df_msisdn.withColumn("num_calls_operators", reduce(lambda a, b: a + b, [col(col_) for col_ in cols_gnv_calls]))
    df_msisdn = df_msisdn.withColumn("mou_operators", reduce(lambda a, b: a + b, [col(col_) for col_ in cols_gnv_mou]))

    df_agg_gnv = df_msisdn.select("num_cliente", "num_calls_operators", "mou_operators").groupby("num_cliente").agg(
        *([sql_sum("num_calls_operators").alias("num_calls_operators"), sql_sum("mou_operators").alias("mou_operators")]))

    return df.join(df_agg_gnv, on=["num_cliente"], how="left").fillna({"mou_operators" : 0, "num_calls_operators" : 0})


def get_new_services(spark, closing_day1, closing_day2, rgu):
    '''
    Compare services base and get the new ones
    :param spark:
    :param closing_day1:
    :param closing_day2:
    :param rgu:
    :return:
    '''
    # FIXME do something when rgu is a list of rgus

    df_servA = Service(spark).get_module(closing_day1, save=False, save_others=False)
    #df_servA2 = df_servA.join(df_price.select("NUM_CLIENTE"), on=["NUM_CLIENTE"], how="inner")

    df_servB = Service(spark).get_module(closing_day2, save=False, save_others=False)
    #df_servB2 = df_servB.join(df_price.select("NUM_CLIENTE"), on=["NUM_CLIENTE"], how="inner")

    df_serv = df_servA.select("msisdn").join(df_servB, ['msisdn'], 'right').where(df_servA['msisdn'].isNull())
    df_serv = df_serv.where(col("rgu").isin(rgu))

    TARIFAS = ["TPVLO", "TPMID", "TPLOW", "TPHIG", "TS215", "TILOF", "TIL4G", "TPVHI"]
    df_serv = df_serv.withColumn("new_srv_tariff", when(col("TARIFF").isin(TARIFAS), col("TARIFF")).otherwise("other"))
    df_serv = df_serv.withColumnRenamed("PRICE_TARIFF", "new_srv_price")
    df_serv = df_serv.fillna({"new_srv_price": -1.0})
    return df_serv.select("NUM_CLIENTE", "new_srv_tariff", "new_srv_price")


def exit_if_version(running4version, versions4exit, df, select_cols=None):
    if running4version in versions4exit:
        print("[NewProductData] get_set | Emptying function in version {}".format(running4version))

        if select_cols:
            print("[NewProductData] build_module | Selecting columns: {}".format(",".join(select_cols)))
            df = df.select(*(select_cols))

        df = df.drop_duplicates(["NUM_CLIENTE"])  # In case a module is stored more than once
        df = df.repartition(200)
        return df