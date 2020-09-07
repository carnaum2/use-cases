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
from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter, get_forbidden_orders_filter
from churn_nrt.src.utils.constants import MODE_EVALUATION
from churn_nrt.src.data.myvf_data import PLATFORM_WEB, PLATFORM_APP, PREFFIX_WEB_COLS, PREFFIX_APP_COLS
import datetime as dt

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class TriggerMyVfData(TriggerDataTemplate): # unlabeled

    PLATFORM = "app"
    VERSION = 1
    def __init__(self, spark, metadata_obj, platform):
        if platform not in [PLATFORM_APP, PLATFORM_WEB]:
            print("[TriggerMyVfData] __init__ | Invalid platform {}".format(platform))
            sys.exit()

        self.PLATFORM = platform

        if platform == PLATFORM_APP:
            if "myvf" in metadata_obj.METADATA_SOURCES:
                self.VERSION = 1
            else:
                self.VERSION = int(re.search("myvf_v([1-9])", [ss for ss in metadata_obj.METADATA_SOURCES if ss.startswith("myvf")][0]).group(1))
        elif platform == PLATFORM_WEB:
            if "myvfweb" in metadata_obj.METADATA_SOURCES:
                self.VERSION = 1
            else:
                self.VERSION = int(re.search("myvfweb_v([1-9])", [ss for ss in metadata_obj.METADATA_SOURCES if ss.startswith("myvfweb")][0]).group(1))

        print("[TriggerMyVfData] build_module | Requested myvf data for version {} and platform {}".format(self.VERSION, self.PLATFORM))

        TriggerDataTemplate.__init__(self, spark, "trigger_myvf/{}/v{}".format(self.PLATFORM, self.VERSION), metadata_obj)

    def build_module(self, closing_day, save_others, force_gen=False, select_cols=None, sources=None, *kwargs):

        sources = METADATA_STANDARD_MODULES if not sources else sources

        print("[TriggerMyVfData] build_module | date={} and verbose={}".format(closing_day, self.VERBOSE))
        print("[TriggerMyVfData] build_module | SOURCES {}".format(sources))


        base_df = get_customer_base_myvf(self.SPARK, closing_day, save_others, self.METADATA, self.VERBOSE)
        df_myvf = MyVFdata(self.SPARK, platform=self.PLATFORM, version=self.VERSION).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        df_myvf = df_myvf.repartition(200)
        df_myvf.cache()

        if self.VERBOSE:
            print('[TriggerMyVfData] build_module | Tr set - The volume of df_myvf is ' + str(df_myvf.count()) + ' - The number of distinct MSISDNs is ' + str(df_myvf.select('msisdn').distinct().count()))

        print('[TriggerMyVfData] build_module | Tr set - The volume of df_myvf is {}'.format(df_myvf.count()))

        df_myvf_tr = df_myvf.join(base_df, ['msisdn'], 'inner')
        df_myvf_tr = self.METADATA.fillna(df_myvf_tr, sources=['customer', '{}_v{}'.format(PREFFIX_APP_COLS if self.PLATFORM == PLATFORM_APP else PREFFIX_WEB_COLS,
                                                                                           self.VERSION)])

        if "navcomp" in sources:
            ndays_length_period = 15
            df_navcomp = NavCompData(self.SPARK, window_length=ndays_length_period).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_navcomp = df_navcomp.repartition(200)
            df_navcomp.cache()

            if self.VERBOSE:
                print('[TriggerMyVfData] build_module | Tr set - The volume of navcomp_tr_df is {} - The number of distinct MSISDNs is {}'.format(df_navcomp.count(),
                                                                                                                                                  df_navcomp.select('msisdn').distinct().count()))

            df_myvf_tr = df_myvf_tr.join(df_navcomp, ['msisdn'], 'left')
            df_myvf_tr = self.METADATA.fillna(df_myvf_tr, sources=['navcomp'])

            if self.VERBOSE:
                print('[TriggerMyVfData] build_module | Tr set - The volume of navcomp_tr_df after adding IDs from the base is {} - The number of distinct MSISDNs is {}'.format(df_myvf_tr.count(),
                                                                                                                                                                                 df_myvf_tr.select('msisdn').distinct().count()))
        else:
            print("[TriggerMyVfData] build_module | Skipped navcomp attributes")

        if "ccc" in sources:
            # Adding CCC feats
            ccc_feats = self.METADATA.get_cols(sources=["ccc"])
            ccc_tr_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(ccc_feats)
            ccc_tr_df = ccc_tr_df.cache()
            df_myvf_tr = df_myvf_tr.join(ccc_tr_df, ['msisdn'], 'left')
            df_myvf_tr = self.METADATA.fillna(df_myvf_tr, sources=['ccc'])

            if self.VERBOSE:
                df_myvf_tr = df_myvf_tr.cache()
                print('[TriggerMyVfData] build_module | Tr set - Volume after adding ccc_tr_df is {}  - The number of distinct MSISDNs is {}'.format(df_myvf_tr.count(),
                                                                                                                                                     df_myvf_tr.select('msisdn').distinct().count()))
        else:
            print("[TriggerMyVfData] build_module | Skipped ccc attributes")

        if "spinners" in sources:
            # Adding spinners
            # do not get msisdn column since Spinners data is by nif
            spinners_feats = self.METADATA.get_cols(sources=["spinners"], type_="feats") + ['nif_cliente']
            spinners_df = Spinners(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(spinners_feats)
            df_myvf_tr = df_myvf_tr.join(spinners_df, ['nif_cliente'], 'left')
            df_myvf_tr = self.METADATA.fillna(df_myvf_tr, sources=['spinners'])

        else:
            print("[TriggerMyVfData] build_module | Skipped spinners attributes")

        if self.VERBOSE:
            df_myvf_tr = df_myvf_tr.cache()
            print('[TriggerMyVfData] build_module | Tr set - Volume after adding spinners_df is {} - The number of distinct MSISDNs is {}'.format(df_myvf_tr.count(), df_myvf_tr.select('msisdn').distinct().count()))

        if "scores" in sources:
            print("[TriggerMyVfData] build_module | About to add latest scores stored in model_outputs")
            df_scores = Scores(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_myvf_tr = df_myvf_tr.join(df_scores.select("msisdn", "latest_score"), ['msisdn'], 'left')
            df_myvf_tr = self.METADATA.fillna(df_myvf_tr, sources=['scores'])
        else:
            print("[TriggerMyVfData] build_module | Skipped adding scores attributes")

        if select_cols:
            print("[TriggerMyVfData] build_module | Selecting columns: {}".format(",".join(select_cols)))
            df_myvf_tr = df_myvf_tr.select(*(select_cols))

        df_myvf_tr = df_myvf_tr.drop_duplicates(["msisdn"]) # In case a module is stored more than once
        df_myvf_tr = df_myvf_tr.repartition(200)
        print('[TriggerMyVfData] build_module | Tr set - Volume at the end of build_module is {}'.format(df_myvf_tr.count()))

        return df_myvf_tr


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class TriggerMyVfModel(ModelTemplate):

    NAVIG_DAYS = None
    TARGET_DAYS = None
    NAVIG_SECTIONS = None
    PLATFORM = None

    def __init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, platform, navig_days, target_days, force_gen=False,
                 navig_sections=None):
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
        self.NAVIG_DAYS = navig_days
        self.TARGET_DAYS = target_days
        self.PLATFORM = platform
        self.NAVIG_SECTIONS = navig_sections

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, *args, **kwargs):

        print("- - - - - - - - get_set for closing_day={} labeled={} - - - - - - - -".format(closing_day, labeled))


        df_tr_myvfdata = TriggerMyVfData(self.SPARK, self.METADATA, platform=self.PLATFORM).get_module(closing_day,
                                                                                                     force_gen=self.FORCE_GEN,
                                                                                                     save_others=False,
                                                                                                     save=True,
                                                                                                     sources=self.METADATA.METADATA_SOURCES)

        if labeled:
            # Labeling

            from churn_nrt.src.data.sopos_dxs import MobPort
            target = MobPort(self.SPARK, churn_window=self.TARGET_DAYS).get_module(closing_day, save=True, force_gen=self.FORCE_GEN).withColumnRenamed('label_mob', 'label').select('msisdn', 'label').drop_duplicates(["msisdn"])

            df_tr_myvfdata = df_tr_myvfdata.join(target, ['msisdn'], 'left').na.fill({'label': 0.0}).drop_duplicates(["msisdn"])

            if self.MODE == MODE_EVALUATION:
                print('[TriggerMyVfModel] get_set Total volume of the labeled set for ' + str(closing_day) + ' is ' + str(df_tr_myvfdata.count()) + ' - Number of MSISDNs is ' + str(
                    df_tr_myvfdata.select('msisdn').distinct().count()))

                df_tr_myvfdata.groupBy('label').agg(sql_count('*').alias('num_services')).show()
                print('[TriggerMyVfModel] get_set | Churn proportion above')


        print("- - - - - - - - navigated users for closing_day={} sections={} - - - - - - - -".format(closing_day, "+".join(self.NAVIG_SECTIONS) if self.NAVIG_SECTIONS else "all"))

        preffix_ = PREFFIX_APP_COLS if self.PLATFORM == PLATFORM_APP else PREFFIX_WEB_COLS
        # Navigating users
        if self.NAVIG_SECTIONS:
            cols_sections_list = ["{}_{}_nb_pages_last{}".format(preffix_, sect, self.NAVIG_DAYS) for sect in self.NAVIG_SECTIONS]
            print('[TriggerMyVfData] build_module | Tr set - Filtering myvf df for {}>0'.format("+".join(cols_sections_list)))
            from churn_nrt.src.utils.pyspark_utils import sum_horizontal
            df_tr_myvfdata = df_tr_myvfdata.withColumn("nb_pages_sections", sum_horizontal(cols_sections_list))
            df_navig_users = df_tr_myvfdata.where(col("nb_pages_sections") > 0).select("msisdn")
        else:
            # keep msisdns that are navigated by app/web in the last N_DAYS days
            df_navig_users = df_tr_myvfdata.where(col("{}_nb_pages_last{}".format(preffix_, self.NAVIG_DAYS)) > 0).select("msisdn")

        print("[TriggerMyVfModel] get_set | There are {} navigators users".format(df_navig_users.count()))

        print("[TriggerMyVfModel] get_set | - - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_active_filter = get_non_recent_customers_filter(self.SPARK, closing_day, 90)

        print("[TriggerMyVfModel] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_disconnection_filter = get_disconnection_process_filter(self.SPARK, closing_day, 90)

        print("[TriggerMyVfModel] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

        tr_churn_call_filter = get_churn_call_filter(self.SPARK, closing_day, 90, 'msisdn')

        print("[TriggerMyVfModel] get_set | - - - - - - - -  get_forbidden_orders_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

        tr_forbidden = get_forbidden_orders_filter(self.SPARK, closing_day, level="msisdn", verbose=False, only_active=True)



        tr_set = (df_tr_myvfdata.join(df_navig_users, ['msisdn'], 'inner').join(tr_active_filter, ['msisdn'], 'inner') \
            .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
            .join(tr_churn_call_filter, ['msisdn'], 'inner')\
            .join(tr_forbidden, ['msisdn'], 'inner'))


        if tr_set.count() == 0:
            print("[TriggerMyVfModel] get_set | After all the filters dataframe for date {} is empty".format(closing_day))
            import sys
            sys.exit()

        if self.MODE == MODE_EVALUATION:
            print("[TriggerMyVfModel] get_set | After all the filters - Label count on set - date = {}".format(closing_day))
            tr_set.groupBy('label').agg(sql_count('*').alias('num_samples')).show()

        all_cols = self.METADATA.get_cols(type_="all")


        # when working with different versions  just to be sure "all_cols" contains columns existing in df
        all_cols = list(set(all_cols) & set(tr_set.columns))

        if labeled:
            tr_set = tr_set.select(all_cols + [self.LABEL_COL])
            print("[TriggerMyVfModel] get_set | count on blindaje")
            tr_set.groupby("blindaje").agg(sql_count("*").alias("count"), sql_sum(self.LABEL_COL).alias("label1")).show()
        else:
            tr_set = tr_set.select(all_cols)
            tr_set.groupby("blindaje").agg(sql_count("*").alias("count")).show()


        return tr_set




def get_customer_base_myvf(spark, date_, save, metadata_obj, verbose=False):
    '''
    Build the customer base for the myvf model. This module is build as a join of customer_base and customer_master modules
    :param spark:
    :param date_:
    :return:
    '''
    print("[myvf/model_classes]  get_customer_base_myvf | Before saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(date_, save=save, save_others=save).filter(col('rgu') == 'mobile').drop_duplicates(["msisdn"])
    print("[myvf/model_classes]  get_customer_base_myvf | After saving CustomerBase {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    from churn_nrt.src.data_utils.base_filters import keep_active_services
    base_df = keep_active_services(base_df)

    from churn_nrt.src.data.customer_base import CustomerAdditional
    base_add_df = CustomerAdditional(spark, days_before=90).get_module(date_, save=save, save_others=save)
    base_df = base_df.join(base_add_df, on=["nif_cliente"], how="left")

    base_df = base_df.filter((col("segment_nif").isNotNull()) & (col('segment_nif').isin("Other", "Convergent", "Mobile_only")))
    base_df = metadata_obj.fillna(base_df, sources=['customer'])
    customer_feats = list(set(metadata_obj.get_cols(sources=['customer']) + ['nif_cliente', "msisdn"]))

    base_df = base_df.withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60), 'soft')
                                            .when((col('tgs_days_until_f_fin_bi') > 60), 'hard')
                                            .otherwise(lit('unknown')))

    base_df = base_df.select(*(customer_feats)).drop_duplicates(["msisdn"])

    if verbose:
        base_df = base_df.cache()
        print('[TriggerMyVfModel] Tr set - The volume of base_df is {} - The number of distinct NIFs is {}'.format(base_df.count(),
                                                                                                                 base_df.select('nif_cliente').distinct().count()))




    return base_df
