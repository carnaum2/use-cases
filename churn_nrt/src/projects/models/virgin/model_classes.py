#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import re
import time
from pyspark.sql.functions import count as sql_count, avg as sql_avg, when, sum as sql_sum
from pyspark.sql.functions import col, lit

from churn_nrt.src.data_utils.DataTemplate import TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate
from churn_nrt.src.data.navcomp_data import NavCompData
from churn_nrt.src.data.myvf_data import MyVFdata
from churn_nrt.src.data.ccc import CCC
from churn_nrt.src.data.spinners import Spinners
from churn_nrt.src.projects.models.myvf.metadata import METADATA_STANDARD_MODULES
from churn_nrt.src.data.scores import Scores
from churn_nrt.src.utils.constants import MODE_EVALUATION
from churn_nrt.src.data.myvf_data import PLATFORM_WEB, PLATFORM_APP, PREFFIX_WEB_COLS, PREFFIX_APP_COLS
import datetime as dt
from churn_nrt.src.data.navcomp_data import NavCompAdvData, NavCompData
from churn_nrt.src.data.calls_comps_data import CallsCompData, CallsCompAdvData
from churn_nrt.src.utils.date_functions import move_date_n_days
from churn_nrt.src.data_utils.base_filters import keep_active_services
from churn_nrt.src.data.customer_base import CustomerAdditional
from churn_nrt.src.data_utils.base_filters import get_forbidden_orders_filter, get_churn_call_filter, get_non_recent_customers_filter, get_disconnection_process_filter
from churn_nrt.src.data.customer_base import add_ccaa
from churn_nrt.src.data_utils.Metadata import apply_metadata
from churn_nrt.src.data.billing import Billing


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class TriggerPropVirgin(TriggerDataTemplate): # unlabeled


    def __init__(self, spark, metadata_obj):
        print("[TriggerPropVirgin] build_module | Requested TriggerPropVirgin".format())

        TriggerDataTemplate.__init__(self, spark, "trigger_virgin/", metadata_obj)

    def build_module(self, closing_day, save_others, force_gen=False, select_cols=None, sources=None, *kwargs):

        sources = METADATA_STANDARD_MODULES if not sources else sources

        print("[TriggerPropVirgin] build_module | date={} and verbose={}".format(closing_day, self.VERBOSE))
        print("[TriggerPropVirgin] build_module | SOURCES {}".format(sources))

        print("[trigger_virgin/model_classes]  get_customer_base_myvf | Before saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        from churn_nrt.src.data.customer_base import CustomerBase

        #base_df = CustomerBase(self.SPARK).get_module(closing_day, save=False, save_others=False).drop_duplicates(["msisdn"])  # .filter(col('rgu') == 'mobile')
        #print("[trigger_virgin/model_classes]  get_customer_base_myvf | After saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        # base_df = keep_active_services(base_df)

        #     base_add_df = CustomerAdditional(spark, days_before=90).get_module(date_, save=save, save_others=save)
        #     base_df = base_df.join(base_add_df, on=["nif_cliente"], how="left")

        #     base_df = base_df.filter((col("segment_nif").isNotNull()) & (col('segment_nif').isin("Other", "Convergent", "Mobile_only")))

        base_df = get_customer_base(self.SPARK, closing_day, False, self.METADATA, self.VERBOSE)

        print("[TriggerPropVirgin] get_set | - - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_active_filter = get_non_recent_customers_filter(self.SPARK, closing_day, 90)

        print("[TriggerPropVirgin] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_disconnection_filter = get_disconnection_process_filter(self.SPARK, closing_day, 90)

        print("[TriggerPropVirgin] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

        tr_churn_call_filter = get_churn_call_filter(self.SPARK, closing_day, 90, 'msisdn')

        tr_forbidden = get_forbidden_orders_filter(self.SPARK, closing_day, level="msisdn", verbose=False, only_active=True)

        tr_set = (base_df.join(tr_active_filter, ['msisdn'], 'inner')
                  .join(tr_disconnection_filter, ['nif_cliente'], 'inner')
                  .join(tr_churn_call_filter, ['msisdn'], 'inner')
                  .join(tr_forbidden, ['msisdn'], 'inner'))

        print('[TriggerPropVirgin] build_module | Tr set - Volume after four filters is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))

        print("CSANC109__base", closing_day)

        df_navcomp_data = NavCompAdvData(self.SPARK).get_module(closing_day, save=True, force_gen=False)#.select("msisdn", "VIRGINTELCO_sum_count")
        df_calls = CallsCompAdvData(self.SPARK).get_module(closing_day, save=True, force_gen=False)#.select("msisdn", "VIRGIN_num_calls")

        tr_set = tr_set.join(df_navcomp_data, on=["msisdn"], how="left")
        tr_set = tr_set.join(df_calls, on=["msisdn"], how="left")

        print('[TriggerPropVirgin] build_module | Tr set - Volume after join navcomp_adv and callscomp_adv is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))

        tr_set = apply_metadata(NavCompAdvData(self.SPARK).get_metadata(), tr_set)
        tr_set = apply_metadata(CallsCompAdvData(self.SPARK).get_metadata(), tr_set)

        tr_set = tr_set.where((col("VIRGINTELCO_sum_count_last30") == 0) & (col("callscomp_VIRGIN_num_calls_last30") == 0))

        print('[TriggerPropVirgin] build_module | Tr set - Volume after condition no calls and navigations is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))



        if "ccc" in sources:
            # Adding CCC feats
            ccc_feats = self.METADATA.get_cols(sources=["ccc"])
            ccc_tr_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(ccc_feats)
            ccc_tr_df = ccc_tr_df.cache()
            tr_set = tr_set.join(ccc_tr_df, ['msisdn'], 'left')
            tr_set = self.METADATA.fillna(tr_set, sources=['ccc'])

            if self.VERBOSE:
                tr_set = tr_set.cache()
                print('[TriggerPropVirgin] build_module | Tr set - Volume after adding ccc_tr_df is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))
        else:
            print("[TriggerPropVirgin] build_module | Skipped ccc attributes")


        if "myvf_v2" in sources:
            print("CSANC109_DEBUG_myvf_v2")
            df_myvf = MyVFdata(self.SPARK, platform="app", version=2).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_myvf = df_myvf.repartition(200)

            tr_set = tr_set.join(df_myvf, ['msisdn'], 'left')

            tr_set = self.METADATA.fillna(tr_set, sources=['myvf_v2'])
            tr_set = tr_set.cache()

            if self.VERBOSE:# do not get msisdn column since Spinners data is by nif
                print('[TriggerPropVirgin] build_module | Tr set - Volume after adding myvf_app is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))

        if "myvfweb_v3" in sources:
            print("CSANC109_DEBUG_myvfweb_v3")

            df_myvf = MyVFdata(self.SPARK, platform="web", version=3).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_myvf = df_myvf.repartition(200)

            tr_set = tr_set.join(df_myvf, ['msisdn'], 'left')

            tr_set = self.METADATA.fillna(tr_set, sources=['myvfweb_v3'])
            tr_set = tr_set.cache()

            if self.VERBOSE:# do not get msisdn column since Spinners data is by nif
                print('[TriggerPropVirgin] build_module | Tr set - Volume after adding myvf_web is {}  - The number of distinct MSISDNs is {}'.format(tr_set.count(),
                                                                                                                                                     tr_set.select('msisdn').distinct().count()))
        if "spinners" in sources:
            # Adding spinners
            spinners_feats = self.METADATA.get_cols(sources=["spinners"], type_="feats") + ['nif_cliente']
            spinners_df = Spinners(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(spinners_feats)
            tr_set = tr_set.join(spinners_df, ['nif_cliente'], 'left')
            tr_set = self.METADATA.fillna(tr_set, sources=['spinners'])
        else:
            print("[TriggerPropVirgin] build_module | Skipped spinners attributes")

        if "billing" in sources:
            # Adding spinners
            billing_feats = self.METADATA.get_cols(sources=["billing"], type_="feats") + ['num_cliente']
            billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen).select(billing_feats)
            tr_set = tr_set.join(billing_df, ['num_cliente'], 'left')
            tr_set = self.METADATA.fillna(tr_set, sources=['billing'])
        else:
            print("[TriggerPropVirgin] build_module | Skipped billing attributes")

        if self.VERBOSE:
            tr_set = tr_set.cache()
            print('[TriggerPropVirgin] build_module | Tr set - Volume after adding spinners_df is {} - The number of distinct MSISDNs is {}'.format(tr_set.count(), tr_set.select('msisdn').distinct().count()))

        if select_cols:
            print("[TriggerPropVirgin] build_module | Selecting columns: {}".format(",".join(select_cols)))
            tr_set = tr_set.select(*(select_cols))

        tr_set = tr_set.drop_duplicates(["msisdn"]) # In case a module is stored more than once
        tr_set = tr_set.repartition(200)
        print('[TriggerPropVirgin] build_module | Tr set - Volume at the end of build_module is {}'.format(tr_set.count()))

        return tr_set


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class PropVirginModel(ModelTemplate):

    TARGET_DAYS = None
    PLATFORM = None

    def __init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, target_days, force_gen=False):
        '''

        :param spark:
        :param tr_date:
        :param tt_date:
        :param mode_:
        :param algorithm:
        :param owner_login:
        :param metadata_obj:
        :param platform:
        :param navig_days:
        :param target_days:
        :param force_gen:
        :param navig_sections: list of sections or None for using all sections
        '''
        ModelTemplate.__init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, force_gen=force_gen)
        self.TARGET_DAYS = target_days

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, *args, **kwargs):

        print("- - - - - - - - get_set for closing_day={} labeled={} - - - - - - - -".format(closing_day, labeled))


        tr_set = TriggerPropVirgin(self.SPARK, self.METADATA).get_module(closing_day,
                                                                         force_gen=self.FORCE_GEN,
                                                                         save_others=False,
                                                                         save=True,
                                                                         sources=self.METADATA.METADATA_SOURCES)

        if labeled:

            tr_set = set_label(self.SPARK, closing_day, tr_set, self.TARGET_DAYS)


        print("[PropVirginModel] get_set | There are {} navigators users".format(tr_set.count()))

        if tr_set.count() == 0:
            print("[PropVirginModel] get_set | After all the filters dataframe for date {} is empty".format(closing_day))
            import sys
            sys.exit()

        if self.MODE == MODE_EVALUATION:
            print("[PropVirginModel] get_set | After all the filters - Label count on set - date = {}".format(closing_day))
            tr_set.groupBy('label').agg(sql_count('*').alias('num_samples')).show()

        all_cols = self.METADATA.get_cols(type_="all")


        # when working with different versions  just to be sure "all_cols" contains columns existing in df
        all_cols = list(set(all_cols) & set(tr_set.columns))

        if labeled:
            tr_set = tr_set.select(all_cols + [self.LABEL_COL])
        else:
            tr_set = tr_set.select(all_cols)

        if self.MODE == MODE_EVALUATION:
            print("[PropVirginModel] get_set | count on blindaje")
            tr_set.groupby("blindaje").agg(sql_count("*").alias("count"), sql_sum("label").alias("label1")).show()


        return tr_set



def set_label(spark, closing_day, tr_set, target_days, force_gen=False):

    closing_day_B = move_date_n_days(closing_day, n=target_days)

    print("Adding label with target days {} - [{},{}]".format(target_days, closing_day, closing_day_B))


    df_navcomp_data2 = (
        NavCompData(spark, target_days).get_module(closing_day_B, save=True, force_gen=force_gen).select("msisdn", "VIRGINTELCO_sum_count", "VIRGINTELCO_distinct_days_with_navigation")
            .withColumnRenamed("VIRGINTELCO_sum_count", "label_VIRGINTELCO_sum_count_last{}".format(target_days))
            .withColumnRenamed("VIRGINTELCO_distinct_days_with_navigation", "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)))

    df_calls2 = (CallsCompData(spark, target_days).get_module(closing_day_B, save=True, force_gen=force_gen)
                 .select("msisdn", "callscomp_VIRGIN_num_calls")
                 .withColumnRenamed("callscomp_VIRGIN_num_calls", "label_callscomp_VIRGIN_num_calls_last{}".format(target_days)))

    tr_set = tr_set.join(df_navcomp_data2, on=["msisdn"], how="left")
    tr_set = tr_set.join(df_calls2, on=["msisdn"], how="left").fillna({"label_VIRGINTELCO_sum_count_last{}".format(target_days): 0,
                                                                       "label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days): 0,
                                                                       "label_callscomp_VIRGIN_num_calls_last{}".format(target_days): 0.0})

    tr_set = tr_set.withColumn("label",
                               when(((col("label_VIRGINTELCO_sum_count_last{}".format(target_days)) >= 2) | (col("label_VIRGINTELCO_distinct_days_with_navigation_last{}".format(target_days)) >= 2) | (col("label_callscomp_VIRGIN_num_calls_last{}".format(target_days)) > 0)), 1).otherwise(0))


    return tr_set






def get_customer_base(spark, date_, save, metadata_obj, verbose=False):
    '''
    Build the customer base for the myvf model. This module is build as a join of customer_base and customer_master modules
    :param spark:
    :param date_:
    :return:
    '''
    print("[trigger_virgin/model_classes]  get_customer_base_myvf | Before saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(date_, save=save, save_others=save).drop_duplicates(["msisdn"]).filter(col('rgu') == 'mobile')
    print("[trigger_virgin/model_classes]  get_customer_base_myvf | After saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    from churn_nrt.src.data_utils.base_filters import keep_active_services
    base_df = keep_active_services(base_df)

    from churn_nrt.src.data.customer_base import CustomerAdditional
    base_add_df = CustomerAdditional(spark, days_before=90).get_module(date_, save=save, save_others=save)
    base_df = base_df.join(base_add_df, on=["nif_cliente"], how="left")
    base_df = base_df.filter((col("segment_nif").isNotNull()) & (col('segment_nif').isin("Other", "Convergent", "Mobile_only")))


    base_df = metadata_obj.fillna(base_df, sources=['customer'])
    customer_feats = list(set(metadata_obj.get_cols(sources=['customer'])) | {'nif_cliente', "msisdn", "NUM_CLIENTE"})

    base_df = base_df.withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60), 'soft')
                                            .when((col('tgs_days_until_f_fin_bi') > 60), 'hard')
                                            .otherwise(lit('unknown')))

    tr_set = add_ccaa(spark, base_df, date_)
    north_ccaa = ["galicia", "cataluna", "navarra", "asturias", "rioja"]
    base_df = tr_set.withColumn("flag_north_ccaa", when(col("ccaa").isin(*north_ccaa), 1).otherwise(0))

    base_df = base_df.select(*(customer_feats)).drop_duplicates(["msisdn"])

    if verbose:
        base_df = base_df.cache()
        print('[PropVirginModel] Tr set - The volume of base_df is {} - The number of distinct NIFs is {}'.format(base_df.count(),
                                                                                                                 base_df.select('nif_cliente').distinct().count()))


    return base_df
