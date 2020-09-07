#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import re
import time
from pyspark.sql.functions import count as sql_count, avg as sql_avg, when, expr
from pyspark.sql.functions import col, lit

from churn_nrt.src.data_utils.DataTemplate import TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate
from churn_nrt.src.data.navcomp_data import NavCompData
from churn_nrt.src.data.myvf_data import MyVFdata
from churn_nrt.src.data.ccc import CCC
from churn_nrt.src.data.spinners import Spinners
from churn_nrt.src.projects.models.myvf.metadata import METADATA_STANDARD_MODULES
from churn_nrt.src.data.scores import Scores
from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter
from churn_nrt.src.utils.constants import MODE_EVALUATION
from churn_nrt.src.data.myvf_data import PLATFORM_WEB, PLATFORM_APP


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class TriggerSocialData(TriggerDataTemplate): # unlabeled



    def __init__(self, spark, metadata_obj):
        # if platform not in [PLATFORM_APP, PLATFORM_WEB]:
        #     print("[TriggerMyVfData] __init__ | Invalid platform {}".format(platform))
        #     sys.exit()
        #
        # self.PLATFORM = platform
        # if "myvf" in metadata_obj.METADATA_SOURCES:
        #     self.VERSION_MYVFAPP = 1
        # elif any([ss.startswith("myvf") for ss in metadata_obj.METADATA_SOURCES]):
        #     self.VERSION_MYVFAPP = int(re.search("myvf_v([1-9])", [ss for ss in metadata_obj.METADATA_SOURCES if ss.startswith("myvf")][0]).group(1))
        # else:
        #     self.VERSION_MYVFAPP = 0
        #
        # if self.VERSION_MYVFAPP> 0:
        #     print("[TriggerMyVfData] build_module | Requested myvf data for version {}".format(self.VERSION_MYVFAPP))

        TriggerDataTemplate.__init__(self, spark, "trigger_social", metadata_obj)

    def build_module(self, closing_day, save_others, force_gen=False, select_cols=None, sources=None, *kwargs):

        sources = METADATA_STANDARD_MODULES if not sources else sources

        print("[TriggerSocialData] build_module | date={} and verbose={}".format(closing_day, self.VERBOSE))
        print("[TriggerSocialData] build_module | SOURCES {}".format(sources))

        base_df = get_customer_base(self.SPARK, closing_day, save_others, self.METADATA, self.VERBOSE)

        from churn_nrt.src.data.geneva_data import GenevaData
        # FIXME detect days incremental from the rule_nb
        df_geneva = GenevaData(self.SPARK, 90).get_module(closing_day, save=True, save_others=save_others, force_gen=force_gen)

        df_tr_social = df_geneva.join(base_df, on=["msisdn"], how="inner")

        #FIXME distinguish between web and app
        if any([ss.startswith("myvf") for ss in sources]):
            df_myvf = MyVFdata(self.SPARK, platform="app", version=2).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_myvf = df_myvf.repartition(200)
            df_myvf.cache()

            if self.VERBOSE:
                print('[TriggerSocialData] build_module | Tr set - The volume of df_myvf is ' + str(df_myvf.count()) + ' - The number of distinct MSISDNs is ' + str(
                    df_myvf.select('msisdn').distinct().count()))

            df_tr_social = df_tr_social.join(df_myvf, ['msisdn'], 'inner')
            df_tr_social = self.METADATA.fillna(df_tr_social, sources=['customer', 'myvf_v{}'.format(2)])

        if "navcomp" in sources:
            ndays_length_period = 15
            df_navcomp = NavCompData(self.SPARK, window_length=ndays_length_period).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_navcomp = df_navcomp.repartition(200)
            df_navcomp.cache()

            if self.VERBOSE:
                print('[TriggerSocialData] build_module | Tr set - The volume of navcomp_tr_df is {} - The number of distinct MSISDNs is {}'.format(df_navcomp.count(),
                                                                                                                                                  df_navcomp.select('msisdn').distinct().count()))

            df_tr_social = df_tr_social.join(df_navcomp, ['msisdn'], 'left')
            df_tr_social = self.METADATA.fillna(df_tr_social, sources=['navcomp'])

            if self.VERBOSE:
                print('[TriggerMyVfData] build_module | Tr set - The volume of df_tr_social after adding IDs from the base is {} - The number of distinct MSISDNs is {}'.format(df_tr_social.count(),
                                                                                                                                                                                 df_tr_social.select('msisdn').distinct().count()))
        else:
            print("[TriggerSocialData] build_module | Skipped navcomp attributes")

        if "ccc" in sources:
            # Adding CCC feats
            ccc_feats = self.METADATA.get_cols(sources=["ccc"])
            ccc_tr_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(ccc_feats)
            ccc_tr_df = ccc_tr_df.cache()
            df_tr_social = df_tr_social.join(ccc_tr_df, ['msisdn'], 'left')
            df_tr_social = self.METADATA.fillna(df_tr_social, sources=['ccc'])

            if self.VERBOSE:
                df_tr_social = df_tr_social.cache()
                print('[TriggerSocialData] build_module | Tr set - Volume after adding ccc_tr_df is {}  - The number of distinct MSISDNs is {}'.format(df_tr_social.count(),
                                                                                                                                                     df_tr_social.select('msisdn').distinct().count()))
        else:
            print("[TriggerSocialData] build_module | Skipped ccc attributes")

        if "spinners" in sources:
            # Adding spinners
            # do not get msisdn column since Spinners data is by nif
            spinners_feats = self.METADATA.get_cols(sources=["spinners"], type_="feats") + ['nif_cliente']
            spinners_df = Spinners(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(spinners_feats)
            df_tr_social = df_tr_social.join(spinners_df, ['nif_cliente'], 'left')
            df_tr_social = self.METADATA.fillna(df_tr_social, sources=['spinners'])

        else:
            print("[TriggerSocialData] build_module | Skipped spinners attributes")

        if self.VERBOSE:
            df_tr_social = df_tr_social.cache()
            print('[TriggerSocialData] build_module | Tr set - Volume after adding spinners_df is {} - The number of distinct MSISDNs is {}'.format(df_tr_social.count(), df_tr_social.select('msisdn').distinct().count()))

        if "scores" in sources:
            print("[TriggerMyVfData] build_module | About to add latest scores stored in model_outputs")
            df_scores = Scores(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            df_tr_social = df_tr_social.join(df_scores.select("msisdn", "latest_score"), ['msisdn'], 'left')
            df_tr_social = self.METADATA.fillna(df_tr_social, sources=['scores'])
        else:
            print("[TriggerSocialData] build_module | Skipped adding scores attributes")

        if select_cols:
            print("[TriggerMyVfData] build_module | Selecting columns: {}".format(",".join(select_cols)))
            df_tr_social = df_tr_social.select(*(select_cols))

        df_tr_social = df_tr_social.drop_duplicates(["msisdn"]) # In case a module is stored more than once
        df_tr_social = df_tr_social.repartition(200)
        df_tr_social = df_tr_social.cache()

        print('[TriggerSocialData] build_module | Tr set - Volume at the end of build_module is {}'.format(df_tr_social.count()))

        return df_tr_social


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class TriggerSocialModel(ModelTemplate):

    TARGET_DAYS = None
    SEGMENT_RULE = 1

    def __init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, target_days, segment_rule, force_gen=False,
                 ):
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
        #self.NAVIG_DAYS = navig_days
        self.TARGET_DAYS = target_days
        #self.PLATFORM = platform
        #self.NAVIG_SECTIONS = navig_sections
        self.SEGMENT_RULE = segment_rule

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, *args, **kwargs):

        print("- - - - - - - - get_set for closing_day={} labeled={} - - - - - - - -".format(closing_day, labeled))


        df_tr_myvfdata = TriggerSocialData(self.SPARK, self.METADATA).get_module(closing_day,
                                                                                                     force_gen=self.FORCE_GEN,
                                                                                                     save_others=False,
                                                                                                     save=True,
                                                                                                     sources=self.METADATA.METADATA_SOURCES)

        if labeled:
            # Labeling

            from churn_nrt.src.data.sopos_dxs import MobPort
            target = MobPort(self.SPARK, churn_window=self.TARGET_DAYS).get_module(closing_day, save=True, force_gen=self.FORCE_GEN).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')

            df_tr_myvfdata = df_tr_myvfdata.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

            if self.MODE == MODE_EVALUATION:
                print('[TriggerSocialModel] get_set Total volume of the labeled set for ' + str(closing_day) + ' is ' + str(df_tr_myvfdata.count()) + ' - Number of MSISDNs is ' + str(
                    df_tr_myvfdata.select('msisdn').distinct().count()))

                df_tr_myvfdata.groupBy('label').agg(sql_count('*').alias('num_services')).show()
                print('[TriggerSocialModel] get_set | Churn proportion above')


        print("- - - - - - - - geneva users for closing_day={} rule={} - - - - - - - -".format(closing_day, self.SEGMENT_RULE))

        df_geneva_filt = base_social_trigger(self.SPARK, self.SEGMENT_RULE, closing_day)
        df_geneva_filt = df_geneva_filt.cache()
        print("[TriggerSocialModel] get_set | There are {} geneva users".format(df_geneva_filt.count()))

        print("[TriggerSocialModel] get_set | - - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_active_filter = get_non_recent_customers_filter(self.SPARK, closing_day, 90)

        print("[TriggerSocialModel] get_set | - - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))

        tr_disconnection_filter = get_disconnection_process_filter(self.SPARK, closing_day, 90)

        print("[TriggerSocialModel] get_set | - - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))

        tr_churn_call_filter = get_churn_call_filter(self.SPARK, closing_day, 90, 'msisdn')

        tr_set = df_tr_myvfdata.join(df_geneva_filt, ['msisdn'], 'inner').join(tr_active_filter, ['msisdn'], 'inner') \
            .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
            .join(tr_churn_call_filter, ['msisdn'], 'inner')


        tr_set = tr_set.cache()
        vol_ = tr_set.count()
        print("[TriggerSocialModel] get_set | Volume after filters {}".format(vol_))

        if vol_ == 0:
            print("[TriggerSocialModel] get_set | After all the filters dataframe for date {} is empty".format(closing_day))
            import sys
            sys.exit()

        if self.MODE == MODE_EVALUATION:
            print("[TriggerSocialModel] get_set | After all the filters - Label count on set - date = {}".format(closing_day))
            tr_set.groupBy('label').agg(sql_count('*').alias('num_samples')).show()

        all_cols = self.METADATA.get_cols(type_="all")

        # when working with different versions  just to be sure "all_cols" contains columns existing in df
        all_cols = list(set(all_cols) & set(tr_set.columns))
        if labeled:
            tr_set = tr_set.select(all_cols + [self.LABEL_COL])
        else:
            tr_set = tr_set.select(all_cols)


        tr_set = tr_set.repartition(200)
        tr_set = tr_set.cache()

        print('[TriggerSocialData] build_module | Tr set - Volume at the end of get_set is {}'.format(tr_set.count()))


        return tr_set




def get_customer_base(spark, date_, save, metadata_obj, verbose=False):
    '''
    Build the customer base for the myvf model. This module is build as a join of customer_base and customer_master modules
    :param spark:
    :param date_:
    :return:
    '''

    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(date_, save=save, save_others=save).filter(col('rgu') == 'mobile').drop_duplicates(["msisdn"])

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

    base_df = base_df.select(*(customer_feats))

    if verbose:
        base_df = base_df.cache()
        print('[TriggerSocialModel] Tr set - The volume of base_df is {} - The number of distinct NIFs is {}'.format(base_df.count(),
                                                                                                                 base_df.select('nif_cliente').distinct().count()))




    return base_df


def base_social_trigger(spark, rule_segment, closing_day):
    from pyspark.sql.functions import expr
    from churn_nrt.src.data.geneva_data import PREFFIX_COLS

    from churn_nrt.src.data.geneva_data import GenevaData
    df_geneva = GenevaData(spark, 90).get_module(closing_day, save=True, save_others=False, force_gen=False)
    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(closing_day, save=False, save_others=False).filter(col('rgu') == 'mobile').drop_duplicates(["msisdn"])

    df_geneva = df_geneva.join(base_df, on=["msisdn"], how="inner")

    df_geneva_filt = df_geneva.where(col(PREFFIX_COLS + "_total_calls") > 0)


    median_total_calls = df_geneva_filt.select(expr('percentile_approx(gnv_total_calls, 0.5)').alias("median_value")).rdd.first()["median_value"]
    print("median_total_calls", median_total_calls)

    df_geneva_filt = df_geneva_filt.where(col(PREFFIX_COLS + "_total_calls") > median_total_calls)

    if rule_segment == 1:

        print("rule_segment=1 | {0}_inc_num_calls_MAS_MOVIL_d90 > 0 or {0}_inc_total_calls_competitors_d90 > 175".format(PREFFIX_COLS))
        df_geneva_filt = (df_geneva_filt.where(  (col(PREFFIX_COLS + '_inc_num_calls_MAS_MOVIL_d90') > 0) | (col(PREFFIX_COLS + '_inc_total_calls_competitors_d90') > 175)))

    elif rule_segment == 2:
        print("rule_segment=2 | {0}_num_calls_YOIGO > 53.5 & {0}_perc_calls_DIGI_SPAIN > 0.004".format(PREFFIX_COLS))
        df_geneva_filt = (df_geneva_filt.where((col(PREFFIX_COLS + '_num_calls_YOIGO') > 53.5) & (col(PREFFIX_COLS + '_perc_calls_DIGI_SPAIN') > 0.004)))

    return df_geneva_filt.select("msisdn")
