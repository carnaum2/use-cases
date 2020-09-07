#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import count as sql_count, avg as sql_avg
from pyspark.sql.functions import col, when, lit

from churn_nrt.src.data_utils.DataTemplate import TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate
from churn_nrt.src.utils.constants import MODE_EVALUATION

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inherites the functions to deal with modules.
# Only required to implement the build_module.
class SfidAnalysisData(TriggerDataTemplate): # unlabeled

    def __init__(self, spark, metadata_obj):
        TriggerDataTemplate.__init__(self, spark, "sfid_analysis", metadata_obj)

    def build_module(self, closing_day, save_others, force_gen=True, select_cols=None, sources=None, *kwargs):

        from churn_nrt.src.projects.analysis.digital_scoring.metadata import METADATA_STANDARD_MODULES

        sources = METADATA_STANDARD_MODULES if not sources else sources

        print("[Info sfid analysis] build_module | date={} and verbose={}".format(closing_day, self.VERBOSE))
        print("SOURCES", sources)

        # 0. The base

        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK)\
            .get_module(closing_day, save=False, save_others=False, force_gen=True)\
            .select('msisdn', 'num_cliente', 'nif_cliente')

        base_df = base_df.cache()
        print('Size of the customer base (MSISDN): ' + str(base_df.select('msisdn').distinct().count()) + ' - Size of the customer base (NUM_CLIENTE): ' + str(base_df.select('num_cliente').distinct().count()) + ' - Size of the customer base (NIF): ' + str(base_df.select('nif_cliente').distinct().count()))

        # 1. Billing

        from churn_nrt.src.data.billing import Billing
        billing_metadata = Billing(self.SPARK).get_metadata()
        billing_feats = billing_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['num_cliente']
        billing_df = Billing(self.SPARK).get_module(closing_day, save=False, save_others=False, force_gen=True).select(billing_feats)
        billing_df = billing_df.repartition(400)
        billing_df.cache()

        base_df = base_df.join(billing_df, ['num_cliente'], 'left')
        base_df = self.METADATA.fillna(base_df, sources=['billing'])

        # 2. CCC

        from churn_nrt.src.data.ccc import CCC
        ccc_metadata = CCC(self.SPARK).get_metadata()
        ccc_feats = ccc_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['msisdn']
        ccc_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=False, save_others=False, force_gen=True).select(ccc_feats)
        ccc_df = ccc_df.repartition(400)
        ccc_df.cache()

        base_df = base_df.join(ccc_df, ['msisdn'], 'left')
        base_df = self.METADATA.fillna(base_df, sources=['ccc'])

        # 3. Churn label

        from churn_nrt.src.data.sopos_dxs import MobPort

        df_mob_port = MobPort(self.SPARK, 30)\
            .get_module(closing_day, save=False, save_others=False, force_gen=True) \
            .select("msisdn", "label_mob")\
            .distinct()\
            .withColumnRenamed("label_mob", "churn30")

        df_mob_port.cache()

        base_df = base_df.join(df_mob_port, ['msisdn'], 'left').na.fill({'churn30': 0.0})



        ###############################
        '''

        from churn_nrt.src.data.navcomp_data import NavCompData
        navcomp_tr_df = NavCompData(self.SPARK, window_length=self.NDAYS_LENGTH_PERIOD).get_module(closing_day)
        navcomp_tr_df = navcomp_tr_df.repartition(400)
        navcomp_tr_df.cache()

        if self.VERBOSE:
            print '[Info navcomp_model] Tr set - The volume of navcomp_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

        base_df = get_customer_base_navcomp(self.SPARK, closing_day, save=save_others, metadata_obj=self.METADATA, verbose=self.VERBOSE)

        navcomp_tr_df = navcomp_tr_df.join(base_df, ['msisdn'], 'inner')

        if self.VERBOSE:
            print '[Info navcomp_model] Tr set - The volume of navcomp_tr_df after adding IDs from the base is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

        if "ccc" in sources:
            # Adding CCC feats

            from churn_nrt.src.data.ccc import CCC
            ccc_metadata = CCC(self.SPARK, level="msisdn").get_metadata()
            ccc_feats = ccc_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['msisdn']
            ccc_map_tmp = ccc_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
            ccc_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in ccc_map_tmp])
            ccc_tr_df = CCC(self.SPARK, level="msisdn").get_module(closing_day, save=save_others, save_others=save_others).select(ccc_feats)

            ccc_tr_df = ccc_tr_df.cache()
            if (self.VERBOSE):
                print '[Info navcomp_model] Tr set - The volume of ccc_tr_df is ' + str(ccc_tr_df.count()) + ' - The number of distinct MSISDNs in ccc_tr_df is ' + str(
                    ccc_tr_df.select('msisdn').distinct().count())

            navcomp_tr_df = navcomp_tr_df.join(ccc_tr_df, ['msisdn'], 'left').na.fill(ccc_map)

            if (self.VERBOSE):
                print '[Info navcomp_model] Tr set - Volume after adding ccc_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(
                    navcomp_tr_df.select('msisdn').distinct().count())

        else:
            print("Skipped ccc attributes")

        if "spinners" in sources:
            # Adding spinners

            from churn_nrt.src.data.spinners import Spinners
            #spinners_metadata = get_metadata(self.SPARK, sources=['spinners'])
            spinners_metadata = Spinners(self.SPARK).get_metadata()
            spinners_feats = spinners_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['nif_cliente']

            #spinners_map_tmp = spinners_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
            #spinners_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in spinners_map_tmp])
            #spinners_df = get_spinners_attributes_nif(spark, date_).select(spinners_feats)
            spinners_df = Spinners(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others).select(spinners_feats)

            navcomp_tr_df = navcomp_tr_df.join(spinners_df, ['nif_cliente'], 'left')#.na.fill(spinners_map)
            navcomp_tr_df = self.METADATA.fillna(navcomp_tr_df, sources=['spinners'])

        else:
            print("Skipped spinners attributes")

        if (self.VERBOSE):
            print '[Info navcomp_model] Tr set - Volume after adding spinners_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(
                navcomp_tr_df.select('msisdn').distinct().count())

        if "scores" in sources:
            print("CSANC109 about to add latest scores stored in model_outputs")
            from churn_nrt.src.data.scores import Scores
            df_scores = Scores(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others)
            mean_score = df_scores.select(sql_avg("latest_score").alias('mean_score')).rdd.first()['mean_score']

            # FIXME add in metadata that scoring has to be imputed with mean
            navcomp_tr_df = navcomp_tr_df.join(df_scores.select("msisdn", "latest_score"), ['msisdn'], 'left')
            print("Imputing null values of latest_score column with mean = {}".format(mean_score))
            navcomp_tr_df = self.METADATA.fillna(navcomp_tr_df, sources=['scores'])
            #navcomp_tr_df = navcomp_tr_df.fillna({"latest_score": mean_score})
            navcomp_tr_df = navcomp_tr_df.cache()
        else:
            print("Skipped adding scores attributes")

        if select_cols:
            print("Selecting columns: {}".format(",".join(select_cols)))
            navcomp_tr_df = navcomp_tr_df.select(*(select_cols))
        
        '''

        return base_df