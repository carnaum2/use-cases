#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import count as sql_count, avg as sql_avg
from pyspark.sql.functions import col, when, lit

from churn_nrt.src.data_utils.DataTemplate import TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate
from churn_nrt.src.utils.constants import MODE_EVALUATION
from churn_nrt.src.projects.models.navcomp.metadata import METADATA_STANDARD_MODULES
from churn_nrt.src.data.ccc import CCC
from churn_nrt.src.data.spinners import Spinners
from churn_nrt.src.data.scores import Scores


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class TriggerNavCompData(TriggerDataTemplate): # unlabeled

    NDAYS_LENGTH_PERIOD = 15

    def __init__(self, spark, metadata_obj, period_length=15):
        TriggerDataTemplate.__init__(self, spark, "trigger_navcomp", metadata_obj)
        self.NDAYS_LENGTH_PERIOD = period_length

    def build_module(self, closing_day, save_others, force_gen=False, select_cols=None, sources=None, *kwargs):

        sources = METADATA_STANDARD_MODULES if not sources else sources

        print("[TriggerNavCompData] build_module | date={} and verbose={}".format(closing_day, self.VERBOSE))
        print("[TriggerNavCompData] build_module | SOURCES = {}", ",".join(sources))

        from churn_nrt.src.data.navcomp_data import NavCompData
        navcomp_tr_df = NavCompData(self.SPARK, window_length=self.NDAYS_LENGTH_PERIOD).get_module(closing_day, force_gen=force_gen)
        navcomp_tr_df.cache()

        if self.VERBOSE:
            print('[TriggerNavCompData] build_module | Tr set - The volume of navcomp_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count()))

        # get_customer_base_navcomp: active customers and segment_nif in Other, Convergent, Onlymob + customer feats

        base_df = get_customer_base_navcomp(self.SPARK, closing_day, save=save_others, metadata_obj=self.METADATA, verbose=self.VERBOSE)

        navcomp_tr_df = navcomp_tr_df.join(base_df, ['msisdn'], 'inner')

        if self.VERBOSE:
            print('[TriggerNavCompData] build_module | Tr set - The volume of navcomp_tr_df after adding IDs from the base is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count()))

        if "ccc" in sources:
            # Adding CCC feats
            ccc_feats = self.METADATA.get_cols(sources=["ccc"]) + ['msisdn']
            ccc_tr_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(ccc_feats)
            ccc_tr_df = ccc_tr_df.cache()

            if (self.VERBOSE):
                print('[TriggerNavCompData] build_module | Tr set - The volume of ccc_tr_df is ' + str(ccc_tr_df.count()) + ' - The number of distinct MSISDNs in ccc_tr_df is ' + str(
                    ccc_tr_df.select('msisdn').distinct().count()))

            navcomp_tr_df = navcomp_tr_df.join(ccc_tr_df, ['msisdn'], 'left')#.na.fill(ccc_map)
            navcomp_tr_df = self.METADATA.fillna(navcomp_tr_df, sources=['ccc'])

            if (self.VERBOSE):
                print('[TriggerNavCompData] build_module | Tr set - Volume after adding ccc_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(
                    navcomp_tr_df.select('msisdn').distinct().count()))

        else:
            print("Skipped ccc attributes")

        if "spinners" in sources:
            # Adding spinners
            # do not get msisdn column since Spinners data is by nif
            spinners_feats = self.METADATA.get_cols(sources=["spinners"], type_="feats") + ['nif_cliente']
            spinners_df = Spinners(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select(spinners_feats)
            navcomp_tr_df = navcomp_tr_df.join(spinners_df, ['nif_cliente'], 'left')#.na.fill(spinners_map)
            navcomp_tr_df = self.METADATA.fillna(navcomp_tr_df, sources=['spinners'])
        else:
            print("[TriggerNavCompData] build_module | Skipped spinners attributes")

        if (self.VERBOSE):
            print('[TriggerNavCompData] build_module | Tr set - Volume after adding spinners_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(
                navcomp_tr_df.select('msisdn').distinct().count()))

        if "scores" in sources:
            print("[TriggerNavCompData] build_module | About to add latest scores stored in model_outputs")
            df_scores = Scores(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
            navcomp_tr_df = navcomp_tr_df.join(df_scores.select("msisdn", "latest_score"), ['msisdn'], 'left')
            navcomp_tr_df = self.METADATA.fillna(navcomp_tr_df, sources=['scores'])
            navcomp_tr_df = navcomp_tr_df.cache()
        else:
            print("[TriggerNavCompData] build_module | Skipped adding scores attributes")

        if select_cols:
            print("[TriggerNavCompData] build_module | Selecting columns: {}".format(",".join(select_cols)))
            navcomp_tr_df = navcomp_tr_df.select(*(select_cols))

        return navcomp_tr_df



def filter_population(df, filter_ = 'none'):

    def get_filter(x):
        return {
            'only_comps_nocancel': (col('sum_count_vdf')==0) & (col('sum_count_comps')>0) & (col('CHURN_CANCELLATIONS_w8')==0),
            'only_comps': (col('sum_count_vdf') == 0) & (col('sum_count_comps') > 0),
            'comps': (col('sum_count_comps') > 0),
            'only_vdf': (col('sum_count_vdf') > 0) & (col('sum_count_comps') == 0),
            'only_vdf_nocancel': (col('sum_count_vdf') > 0) & (col('sum_count_comps') == 0) & (col('CHURN_CANCELLATIONS_w8') == 0),
            'none': (col('sum_count_vdf') >= 0)
        }[x]


    condition_ = get_filter(filter_)

    filter_df = df.filter(condition_)

    print "[Info filter_population] Population filter: " + str(filter_) + " - Size of the output DF: " + str(filter_df.count())

    return filter_df

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class TriggerNavCompModel(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, *args, **kwargs):

        print("- - - - - - - - get_set for closing_day={} labeled={} - - - - - - - -".format(closing_day, labeled))

        navcomp_tr_df = TriggerNavCompData(self.SPARK, self.METADATA).get_module(closing_day, force_gen=self.FORCE_GEN)

        if labeled:
            # Labeling

            from churn_nrt.src.data.sopos_dxs import MobPort
            target = MobPort(self.SPARK, churn_window=15).get_module(closing_day, save=True).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')
            #target = get_mobile_portout_requests(self.SPARK, closing_day, end_port).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')

            navcomp_tr_df = navcomp_tr_df.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

            if self.MODE == MODE_EVALUATION:
                print('TriggerNavCompModel | get_set Total volume of the labeled set for ' + str(closing_day) + ' is ' + str(navcomp_tr_df.count()) + ' - Number of MSISDNs is ' + str(
                    navcomp_tr_df.select('msisdn').distinct().count()))

                navcomp_tr_df.groupBy('label').agg(sql_count('*').alias('num_services')).show()
                print('TriggerNavCompModel | get_set | Churn proportion above')

        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter, get_forbidden_orders_filter

        # Modeling filters
        print("- - - - - - - - get_non_recent_customers_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))
        tr_active_filter = get_non_recent_customers_filter(self.SPARK, closing_day, 90)

        print("- - - - - - - - get_disconnection_process_filter for closing_day={} n_days={} - - - - - - - -".format(closing_day, 90))
        tr_disconnection_filter = get_disconnection_process_filter(self.SPARK, closing_day, 90)

        print("- - - - - - - -  get_churn_call_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))
        tr_churn_call_filter = get_churn_call_filter(self.SPARK, closing_day, 90, 'msisdn')

        print("- - - - - - - -  get_forbidden_orders_filter for closing_day={} n_days={} level={} - - - - - - - -".format(closing_day, 90, "msisdn"))
        tr_forbidden = get_forbidden_orders_filter(self.SPARK, closing_day, level="msisdn", verbose=False, only_active=True)




        tr_set = (navcomp_tr_df.join(tr_active_filter, ['msisdn'], 'inner')
            .join(tr_disconnection_filter, ['nif_cliente'], 'inner')
            .join(tr_churn_call_filter, ['msisdn'], 'inner')
            .join(tr_forbidden, ['msisdn'], 'inner'))

        tr_set = filter_population(tr_set, filter_="comps").drop_duplicates(["msisdn"])

        if self.MODE == MODE_EVALUATION:
            print("TriggerNavCompModel | get_set | After all the filters - Label count on set - date = {}".format(closing_day))
            tr_set.groupBy('label').agg(sql_count('*').alias('num_samples')).show()

        all_cols = self.METADATA.get_cols(type_="all")

        # when working with different versions  just to be sure "all_cols" contains columns existing in df
        all_cols = list(set(all_cols) & set(tr_set.columns))

        if labeled:
            tr_set = tr_set.select(all_cols + [self.LABEL_COL])
        else:
            tr_set = tr_set.select(all_cols)

        return tr_set




def get_customer_base_navcomp(spark, date_, save, metadata_obj, verbose=False):
    '''
    Build the customer base for the navcomp model. This module is build as a join of customer_base and customer_master modules
    :param spark:
    :param date_:
    :return:
    '''
    from churn_nrt.src.data.customer_base import CustomerBase
    base_df = CustomerBase(spark).get_module(date_, save=save, save_others=save).filter(col('rgu') == 'mobile')#.select('msisdn', 'nif_cliente', 'num_cliente', 'birth_date', 'fecha_naci')

    from churn_nrt.src.data_utils.base_filters import keep_active_services
    base_df = keep_active_services(base_df)

    from churn_nrt.src.data.customer_base import CustomerAdditional
    base_add_df = CustomerAdditional(spark, days_before=90).get_module(date_, save=save, save_others=save)
    base_df = base_df.join(base_add_df, on=["nif_cliente"], how="left")

    base_df = base_df.filter((col('segment_nif').isin("Other", "Convergent", "Mobile_only")) & (col("segment_nif").isNotNull()))
    customer_feats = metadata_obj.get_cols(sources=['customer']) + ['nif_cliente']
    base_df = metadata_obj.fillna(base_df, sources=['customer'])
    base_df = base_df.select(*(customer_feats + ["msisdn"]))

    if verbose:
        base_df = base_df.cache()
        print('[Info navcomp_model] Tr set - The volume of base_df is {} - The number of distinct NIFs is {}'.format(base_df.count(),
                                                                                                                     base_df.select('nif_cliente').distinct().count()))

    return base_df
