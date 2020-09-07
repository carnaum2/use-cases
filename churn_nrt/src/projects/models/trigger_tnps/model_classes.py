#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit#, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce

from churn_nrt.src.data_utils.DataTemplate import DataTemplate, TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inheritates the functions to deal with modules.
# Only required to implement the build_module.

class TNPSTriggerCar(TriggerDataTemplate):  # unlabeled

    def __init__(self, spark, metadata_obj, period_length=30):
        TriggerDataTemplate.__init__(self, spark, "tnps_trigger_car", metadata_obj)
        self.NDAYS_LENGTH_PERIOD = period_length

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.data.tnps_data import TNPS

        df_tnps = TNPS(self.SPARK).get_module(closing_day, force_gen=False)

        print'Size of tnps table: ' + str(df_tnps.count())
        print'Number of distinct msisdn on tnps table: ' + str(df_tnps.select('msisdn').distinct().count())

        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter, \
            get_disconnection_process_filter, \
            get_churn_call_filter, get_forbidden_orders_filter

        ############### Customer ###############
        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK).get_module(closing_day, force_gen=force_gen)
        base_df = base_df.cache()
        base_df = base_df.filter(col('segment_nif') != 'Pure_prepaid').withColumn('blindaje', lit('none')).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60),
        'soft').otherwise(col('blindaje'))).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje')))

        print'Size of the customer base (NIFs): ' + str(base_df.select('nif_cliente').distinct().count())
        df_tnps_cust = df_tnps.join(base_df, ['msisdn'], 'inner')
        print'Number of msisdn after customer base join: ' + str(df_tnps_cust.select('msisdn').distinct().count())

        df_non_recent = get_non_recent_customers_filter(self.SPARK, closing_day, 90, level='msisdn', verbose=True, only_active=True)
        valid_nifs = get_disconnection_process_filter(self.SPARK, closing_day, 90, verbose=False)
        df_churn_calls = get_churn_call_filter(self.SPARK, closing_day, 56, level='msisdn', verbose=False)
        df_forb = get_forbidden_orders_filter(self.SPARK, closing_day, level='msisdn', verbose=False, only_active=True)

        df_filt = df_non_recent.join(df_churn_calls,['msisdn'],'inner').join(df_forb,['msisdn'],'inner')

        df_tnps_cust = df_tnps_cust.join(df_filt, ['msisdn'], 'inner')
        print'Size after first filters: ' + str(df_tnps_cust.count())
        df_tnps_cust= df_tnps_cust.join(valid_nifs, ['nif_cliente'], 'inner')
        print'Size after second filters: ' + str(df_tnps_cust.count())
        #.join(valid_nifs, ['nif_cliente'], 'inner')

        df_tnps_cust = df_tnps_cust.cache()
        print'Number of valid  msisdn for chosen date: ' + str(df_tnps_cust.select('msisdn').distinct().count())

        ############### Billing ###############
        from churn_nrt.src.data.billing import Billing
        tr_billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_tnps_bill = df_tnps_cust.join(tr_billing_df, ['num_cliente'], 'left')

        df_tnps_bill = df_tnps_bill.cache()
        print'Size after Billing join: ' + str(df_tnps_bill.count())

        ############### CCC ###############
        from churn_nrt.src.data.ccc import CCC
        tr_ccc_all = CCC(self.SPARK, level="msisdn").get_module(closing_day, force_gen=force_gen)
        df_tnps_ccc = df_tnps_bill.join(tr_ccc_all, ['msisdn'], 'left')

        df_tnps_ccc = df_tnps_ccc.cache()
        print'Size after CCC join: ' + str(df_tnps_ccc.count())

        ############### Spinners ###############
        from churn_nrt.src.data.spinners import Spinners
        df_spinners = Spinners(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_tnps_spinners = df_tnps_ccc.join(df_spinners, ['nif_cliente'], 'left')

        df_tnps_spinners = df_tnps_spinners.cache()
        print'Size after Spinners join: ' + str(df_tnps_spinners.count())


        from churn_nrt.src.projects.models.trigger_tnps.metadata import get_metadata
        metadata_df = get_metadata(self.SPARK)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols] + ['tnps_TNPS2DET']

        df_tnps = df_tnps_spinners#.select(['nif_cliente', 'num_cliente'] + sel_cols)

        print('Feature columns:')

        print(sel_cols)
        ############## Fill NA ##############
        df_tnps_trigger = self.METADATA.fillna(df_tnps, sources=None, cols=sel_cols)

        df_tnps_trigger = df_tnps_trigger.where(col('tnps_TNPS2DET')=='DETRACTOR').drop_duplicates(['msisdn'])
        df_tnps_trigger = df_tnps_trigger.cache()
        print'Final df size: ' + str(df_tnps_trigger.count())

        return df_tnps_trigger

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class TriggerTNPSModel(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj,force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)

    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled,feats_cols=None, set_name=None,  *args, **kwargs):

        df = TNPSTriggerCar(self.SPARK,  self.METADATA).get_module(closing_day)

        if labeled:
            from churn_nrt.src.data.sopos_dxs import Target
            target_msisdn = Target(self.SPARK, churn_window=30, level='msisdn').get_module(closing_day, save=False, save_others=False, force_gen=False)

            df = df.join(target_msisdn, on=["msisdn"], how="inner")#.fillna({'label':0.0})

            from pyspark.sql.functions import count
            df.groupBy('label').agg(count('*')).show()

        from churn_nrt.src.projects.models.trigger_tnps.metadata import get_metadata

        from churn_nrt.src.projects.models.trigger_tnps.metadata import METADATA_STANDARD_MODULES
        sources = METADATA_STANDARD_MODULES

        metadata_df = get_metadata(self.SPARK, sources)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]


        if labeled:
            df = df.select(['msisdn', 'nif_cliente', 'label'] + sel_cols)
        else:
            df = df.select(['msisdn', 'nif_cliente'] + sel_cols)

        return df
