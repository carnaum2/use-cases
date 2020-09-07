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

class MobileSegmentCar(DataTemplate):  # unlabeled

    def __init__(self, spark, segment):
        self.SEGMENT = segment
        DataTemplate.__init__(self, spark, "mobile_car/{}".format(self.SEGMENT))

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.data_utils.ids_utils import get_ids_segments, get_ids

        df_train = get_ids_segments(self.SPARK, closing_day, col_bi="tgs_days_until_f_fin_bi", level='msisdn', verbose=False)
        segment_train = df_train.where(col('blindaje') == self.SEGMENT).dropDuplicates(['msisdn'])

        df_ids_train = get_ids(self.SPARK, closing_day).where(col('serv_rgu') == 'mobile').dropDuplicates(['msisdn'])

        df_train_msisdn = segment_train.select('msisdn').join(df_ids_train, ['msisdn'], 'inner')

        return df_train_msisdn

class FBBSegmentCar(DataTemplate):  # unlabeled

    def __init__(self, spark, segment):
        self.SEGMENT = segment
        DataTemplate.__init__(self, spark, "fbb_car/{}".format(self.SEGMENT))

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.data_utils.ids_utils import get_ids_segments, get_ids_nc

        df_train = get_ids_segments(self.SPARK, closing_day, col_bi="tgs_days_until_f_fin_bi", level='num_cliente', verbose=False)
        segment_train = df_train.where(col('blindaje') == self.SEGMENT).drop_duplicates(['num_cliente'])

        df_ids_train = get_ids_nc(self.SPARK, closing_day).where(col('Cust_Agg_fbb_services_nc')==1).dropDuplicates(['num_cliente'])

        df_train_msisdn = segment_train.select('num_cliente').join(df_ids_train, ['num_cliente'], 'inner')

        return df_train_msisdn

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class RevampMobile(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, segment, horizon, operator, force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)
        self.SEGMENT = segment
        self.OPERATOR = operator
        self.HORIZON = horizon
    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None,  *args, **kwargs):
        segment = self.SEGMENT
        df_mobile = MobileSegmentCar(self.SPARK, segment).get_module(closing_day, save=False, save_others=False).fillna(0.0)
        df_mobile = df_mobile.cache()
        print('Final prepared df size: {}'.format(df_mobile.count()))
        from pyspark.sql.functions import count
        if labeled:
            from churn_nrt.src.data.sopos_dxs import MobPort
            target_mob = MobPort(self.SPARK, churn_window=7*self.HORIZON).get_module(closing_day, save=False, save_others=False, force_gen=False).withColumnRenamed('label_mob', 'label')
            if self.OPERATOR:
                target_mob = target_mob.where((col("target_operator") == self.OPERATOR)|(col("label")==0))
                df = df_mobile.join(target_mob, ["msisdn"], how="left").fillna({'label':0.0})
                df.groupBy('label').agg(count('*')).show()
            else:
                df = df_mobile.join(target_mob, on=["msisdn"], how="left").fillna({'label':0.0}).drop_duplicates(['msisdn'])
                df.groupBy('label').agg(count('*')).show()
        else:
            df = df_mobile
        return df

class RevampFBB(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, segment, horizon, force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)
        self.SEGMENT = segment
        self.HORIZON = horizon
    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None,  *args, **kwargs):
        segment = self.SEGMENT
        df = FBBSegmentCar(self.SPARK, segment).get_module(closing_day, save=False, save_others=False).fillna(0.0)
        df = df.cache()
        print('Final prepared df size: {}'.format(df.count()))
        if labeled:
            from churn_nrt.src.data.sopos_dxs import Target_FBB
            target_msisdn = Target_FBB(self.SPARK, churn_window=7*self.HORIZON, level='num_cliente').get_module(closing_day, save=False, save_others=False, force_gen=False)
            df = df.join(target_msisdn, on=["num_cliente"], how="inner").fillna({'label':0.0})

            from pyspark.sql.functions import count
            df.groupBy('label').agg(count('*')).show()

        return df
