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


class FTTHTriggerCar(TriggerDataTemplate):  # unlabeled

    CRITICAL = "critical"

    def __init__(self, spark, metadata_obj, period_length=30, critical="critical"):
        self.CRITICAL = critical
        self.NDAYS_LENGTH_PERIOD = period_length
        TriggerDataTemplate.__init__(self, spark, "zhilabs_ftth_car", metadata_obj)


    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.projects.models.trigger_zhilabs.model_classes import ZhilabsFTTHCar


        df_ftth = ZhilabsFTTHCar(self.SPARK, self.CRITICAL).get_module(closing_day, force_gen=False).fillna(0.0)

        ############### Zhilabs Incremental ###############
        print'Generating incremental Zhilabs data'
        from churn_nrt.src.data.zhilabs_data import ZhilabsFTTHIncrementalData
        incremental_feats = ZhilabsFTTHIncrementalData(self.SPARK, self.CRITICAL).get_module(closing_day, preparation_=True, force_gen=force_gen)
        df_ftth = df_ftth.join(incremental_feats, ['serviceid'], 'left').fillna(0.0)
        df_ftth = df_ftth.cache()
        df_ftth.count()

        ############### Customer ###############
        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK).get_module(closing_day, force_gen=force_gen)
        base_df = base_df.cache()
        base_df = base_df.filter(col('segment_nif') != 'Pure_prepaid').withColumn('blindaje', lit('none')).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60),
        'soft').otherwise(col('blindaje'))).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje'))).drop_duplicates(['num_cliente'])

        print'Size of the customer base (NIFs): ' + str(base_df.select('nif_cliente').distinct().count())
        df_ftth_cust = df_ftth.join(base_df.drop('nif_cliente'), ['num_cliente'], 'left')
        print'Size of the fbb customer base (num_clientes): ' + str(df_ftth_cust.select('num_cliente').distinct().count())

        ############### Billing ###############
        from churn_nrt.src.data.billing import Billing
        tr_billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_ftth_bill = df_ftth_cust.join(tr_billing_df, ['num_cliente'], 'left')

        df_ftth_bill = df_ftth_bill.cache()
        print'Size after Billing join: ' + str(df_ftth_bill.count())

        ############### CCC ###############
        from churn_nrt.src.data.ccc import CCC
        tr_ccc_all = CCC(self.SPARK, level="nif").get_module(closing_day, force_gen=force_gen)
        df_ftth_ccc = df_ftth_bill.join(tr_ccc_all, ['nif_cliente'], 'left')

        df_ftth_ccc = df_ftth_ccc.cache()
        print'Size after CCC join: ' + str(df_ftth_ccc.count())

        ############### Reimbursements ###############
        from churn_nrt.src.data.reimbursements import Reimbursements
        tr_reimb_df_nrt = Reimbursements(self.SPARK).get_module(closing_day, force_gen=force_gen).withColumnRenamed("NIF_CLIENTE", "nif_cliente")
        tr_reimb_df_nrt = tr_reimb_df_nrt.drop_duplicates(["nif_cliente"])
        df_ftth_reimb = df_ftth_ccc.join(tr_reimb_df_nrt, ['nif_cliente'], 'left')

        df_ftth_reimb = df_ftth_reimb.cache()
        print'Size after Reimbursements join: ' + str(df_ftth_reimb.count())

        ############### Tickets ###############
        from churn_nrt.src.data.tickets import Tickets
        df_tickets = Tickets(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_ftth_tickets = df_ftth_reimb.join(df_tickets, ['nif_cliente'], 'left')

        df_ftth_tickets = df_ftth_tickets.cache()
        print'Size after Tickets join: ' + str(df_ftth_tickets.count())

        ############### Spinners ###############
        from churn_nrt.src.data.spinners import Spinners
        df_spinners = Spinners(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_ftth_spinners = df_ftth_tickets.join(df_spinners, ['nif_cliente'], 'left')

        df_ftth_spinners = df_ftth_spinners.cache()
        print'Size after Spinners join: ' + str(df_ftth_spinners.count())

        ############### Orders ###############
        from churn_nrt.src.data.orders_sla import OrdersSLA
        tr_orders = OrdersSLA(self.SPARK).get_module(closing_day, save=False, save_others=False, force_gen=force_gen)
        df_ftth_orders = df_ftth_spinners.join(tr_orders, ['nif_cliente'], 'left')

        df_ftth_orders = df_ftth_orders.cache()
        print'Size after Orders join: ' + str(df_ftth_orders.count())

        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_metadata
        metadata_df = get_metadata(self.SPARK)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]

        df_ftth_orders = df_ftth_orders#.select(['nif_cliente', 'num_cliente'] + sel_cols)

        print('Feature columns:')

        print(sel_cols)
        ############## Fill NA ##############
        df_ftth_trigger = self.METADATA.fillna(df_ftth_orders, sources=None, cols=sel_cols)

        df_ftth_trigger = df_ftth_trigger.cache()
        print'Final df size: ' + str(df_ftth_trigger.count())

        return df_ftth_trigger

class HFCTriggerCar(TriggerDataTemplate):  # unlabeled

    def __init__(self, spark, metadata_obj, period_length=30):
        TriggerDataTemplate.__init__(self, spark, "zhilabs_hfc_car", metadata_obj)
        self.NDAYS_LENGTH_PERIOD = period_length

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.projects.models.trigger_zhilabs.model_classes import ZhilabsHFCCar

        df_hfc = ZhilabsHFCCar(self.SPARK).get_module(closing_day, force_gen=False).fillna(0.0)

        ############### Zhilabs Incremental ###############
        print'Generating incremental Zhilabs data'
        from churn_nrt.src.data.zhilabs_data import ZhilabsHFCIncrementalData
        incremental_feats = ZhilabsHFCIncrementalData(self.SPARK).get_module(closing_day, preparation_=True, force_gen=force_gen)
        df_hfc = df_hfc.join(incremental_feats, ['serviceid'], 'left').fillna(0.0)
        df_hfc = df_hfc.cache()
        df_hfc.count()

        ############### Customer ###############
        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK).get_module(closing_day, force_gen=force_gen)
        base_df = base_df.cache()
        base_df = base_df.filter(col('segment_nif') != 'Pure_prepaid').withColumn('blindaje', lit('none')).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60),
        'soft').otherwise(col('blindaje'))).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje'))).drop_duplicates(['num_cliente'])

        print'Size of the customer base (NIFs): ' + str(base_df.select('nif_cliente').distinct().count())
        df_hfc_cust = df_hfc.join(base_df.drop('nif_cliente'), ['num_cliente'], 'left')
        print'Size of the fbb customer base (num_clientes): ' + str(df_hfc_cust.select('num_cliente').distinct().count())

        ############### Billing ###############
        from churn_nrt.src.data.billing import Billing
        tr_billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_hfc_bill = df_hfc_cust.join(tr_billing_df, ['num_cliente'], 'left')

        df_hfc_bill = df_hfc_bill.cache()
        print'Size after Billing join: ' + str(df_hfc_bill.count())

        ############### CCC ###############
        from churn_nrt.src.data.ccc import CCC
        tr_ccc_all = CCC(self.SPARK, level="nif").get_module(closing_day, force_gen=force_gen)
        df_hfc_ccc = df_hfc_bill.join(tr_ccc_all, ['nif_cliente'], 'left')

        df_hfc_ccc = df_hfc_ccc.cache()
        print'Size after CCC join: ' + str(df_hfc_ccc.count())

        ############### Reimbursements ###############
        from churn_nrt.src.data.reimbursements import Reimbursements
        tr_reimb_df_nrt = Reimbursements(self.SPARK).get_module(closing_day, force_gen=force_gen).withColumnRenamed("NIF_CLIENTE", "nif_cliente")
        tr_reimb_df_nrt = tr_reimb_df_nrt.drop_duplicates(["nif_cliente"])
        df_hfc_reimb = df_hfc_ccc.join(tr_reimb_df_nrt, ['nif_cliente'], 'left')

        df_hfc_reimb = df_hfc_reimb.cache()
        print'Size after Reimbursements join: ' + str(df_hfc_reimb.count())

        ############### Tickets ###############
        from churn_nrt.src.data.tickets import Tickets
        df_tickets = Tickets(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_hfc_tickets = df_hfc_reimb.join(df_tickets, ['nif_cliente'], 'left')

        df_hfc_tickets = df_hfc_tickets.cache()
        print'Size after Tickets join: ' + str(df_hfc_tickets.count())

        ############### Spinners ###############
        from churn_nrt.src.data.spinners import Spinners
        df_spinners = Spinners(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_hfc_spinners = df_hfc_tickets.join(df_spinners, ['nif_cliente'], 'left')

        df_hfc_spinners = df_hfc_spinners.cache()
        print'Size after Spinners join: ' + str(df_hfc_spinners.count())

        ############### Orders ###############
        from churn_nrt.src.data.orders_sla import OrdersSLA
        tr_orders = OrdersSLA(self.SPARK).get_module(closing_day, save=False, save_others=False, force_gen=force_gen)
        df_hfc_orders = df_hfc_spinners.join(tr_orders, ['nif_cliente'], 'left')

        df_hfc_orders = df_hfc_orders.cache()
        print'Size after Orders join: ' + str(df_hfc_orders.count())

        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_metadata
        metadata_df = get_metadata(self.SPARK)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]

        df_hfc_orders = df_hfc_orders#.select(['nif_cliente', 'num_cliente'] + sel_cols)

        print('Feature columns:')

        print(sel_cols)
        ############## Fill NA ##############
        df_hfc_trigger = self.METADATA.fillna(df_hfc_orders, sources=None, cols=sel_cols)

        df_hfc_trigger = df_hfc_trigger.cache()
        print'Final df size: ' + str(df_hfc_trigger.count())

        return df_hfc_trigger

class DSLTriggerCar(TriggerDataTemplate):  # unlabeled

    def __init__(self, spark, metadata_obj, period_length=30):
        TriggerDataTemplate.__init__(self, spark, "zhilabs_dsl_car", metadata_obj)
        self.NDAYS_LENGTH_PERIOD = period_length

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.projects.models.trigger_zhilabs.model_classes import ZhilabsDSLCar

        df_dsl = ZhilabsDSLCar(self.SPARK).get_module(closing_day, force_gen=False).fillna(0.0)

        ############### Zhilabs Incremental ###############
        print'Generating incremental Zhilabs data'
        from churn_nrt.src.data.zhilabs_data import ZhilabsDSLIncrementalData
        incremental_feats = ZhilabsDSLIncrementalData(self.SPARK).get_module(closing_day, preparation_=True, force_gen=force_gen)
        df_dsl = df_dsl.join(incremental_feats, ['serviceid'], 'left').fillna(0.0)
        df_dsl = df_dsl.cache()
        df_dsl.count()

        ############### Customer ###############
        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK).get_module(closing_day, force_gen=force_gen)
        base_df = base_df.cache()
        base_df = base_df.filter(col('segment_nif') != 'Pure_prepaid').withColumn('blindaje', lit('none')).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60),
        'soft').otherwise(col('blindaje'))).withColumn('blindaje', when((col('tgs_days_until_f_fin_bi') > 60), 'hard').otherwise(col('blindaje'))).drop_duplicates(['num_cliente'])

        print'Size of the customer base (NIFs): ' + str(base_df.select('nif_cliente').distinct().count())
        df_dsl_cust = df_dsl.join(base_df.drop('nif_cliente'), ['num_cliente'], 'left')
        print'Size of the fbb customer base (num_clientes): ' + str(df_dsl_cust.select('num_cliente').distinct().count())

        ############### Billing ###############
        from churn_nrt.src.data.billing import Billing
        tr_billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_dsl_bill = df_dsl_cust.join(tr_billing_df, ['num_cliente'], 'left')

        df_dsl_bill = df_dsl_bill.cache()
        print'Size after Billing join: ' + str(df_dsl_bill.count())

        ############### CCC ###############
        from churn_nrt.src.data.ccc import CCC
        tr_ccc_all = CCC(self.SPARK, level="nif").get_module(closing_day, force_gen=force_gen)
        df_dsl_ccc = df_dsl_bill.join(tr_ccc_all, ['nif_cliente'], 'left')

        df_dsl_ccc = df_dsl_ccc.cache()
        print'Size after CCC join: ' + str(df_dsl_ccc.count())

        ############### Reimbursements ###############
        from churn_nrt.src.data.reimbursements import Reimbursements
        tr_reimb_df_nrt = Reimbursements(self.SPARK).get_module(closing_day, force_gen=force_gen).withColumnRenamed("NIF_CLIENTE", "nif_cliente")
        tr_reimb_df_nrt = tr_reimb_df_nrt.drop_duplicates(["nif_cliente"])
        df_dsl_reimb = df_dsl_ccc.join(tr_reimb_df_nrt, ['nif_cliente'], 'left')

        df_dsl_reimb = df_dsl_reimb.cache()
        print'Size after Reimbursements join: ' + str(df_dsl_reimb.count())

        ############### Tickets ###############
        from churn_nrt.src.data.tickets import Tickets
        df_tickets = Tickets(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_dsl_tickets = df_dsl_reimb.join(df_tickets, ['nif_cliente'], 'left')

        df_dsl_tickets = df_dsl_tickets.cache()
        print'Size after Tickets join: ' + str(df_dsl_tickets.count())

        ############### Spinners ###############
        from churn_nrt.src.data.spinners import Spinners
        df_spinners = Spinners(self.SPARK).get_module(closing_day, force_gen=force_gen)
        df_dsl_spinners = df_dsl_tickets.join(df_spinners, ['nif_cliente'], 'left')

        df_dsl_spinners = df_dsl_spinners.cache()
        print'Size after Spinners join: ' + str(df_dsl_spinners.count())

        ############### Orders ###############
        from churn_nrt.src.data.orders_sla import OrdersSLA
        tr_orders = OrdersSLA(self.SPARK).get_module(closing_day, save=False, save_others=False, force_gen=force_gen)
        df_dsl_orders = df_dsl_spinners.join(tr_orders, ['nif_cliente'], 'left')

        df_dsl_orders = df_dsl_orders.cache()
        print'Size after Orders join: ' + str(df_dsl_orders.count())

        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_metadata
        metadata_df = get_metadata(self.SPARK)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]

        df_dsl_orders = df_dsl_orders#.select(['nif_cliente', 'num_cliente'] + sel_cols)

        print('Feature columns:')

        print(sel_cols)
        ############## Fill NA ##############
        df_dsl_trigger = self.METADATA.fillna(df_dsl_orders, sources=None, cols=sel_cols)

        df_dsl_trigger = df_dsl_trigger.cache()
        print'Final df size: ' + str(df_dsl_trigger.count())

        return df_dsl_trigger

class ZhilabsFTTHCar(DataTemplate):  # unlabeled

    CRITICAL = "critical"

    def __init__(self, spark, critical="critical"):
        self.CRITICAL = critical
        DataTemplate.__init__(self, spark, "ftth_car/{}".format(self.CRITICAL))


    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True, force_gen=False):
        from churn_nrt.src.data.zhilabs_data import get_ftth_population
        df_ftth = get_ftth_population(self.SPARK, closing_day, n_days=n_days,critical=self.CRITICAL, active=True, extra_filters=extra_filt, pre_pro=True, force_gen=force_gen)


        return df_ftth

class ZhilabsHFCCar(DataTemplate):  # unlabeled

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "hfc_car")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True,  force_gen=False):
        from churn_nrt.src.data.zhilabs_data import get_hfc_population
        df_hfc = get_hfc_population(self.SPARK, closing_day, n_days=n_days, active=True, extra_filters=extra_filt, pre_pro=True,  force_gen=force_gen)


        return df_hfc

class ZhilabsDSLCar(DataTemplate):  # unlabeled

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "dsl_car")

    def build_module(self, closing_day, save_others, select_cols=None, n_days=30, extra_filt = True,  force_gen=False):
        from churn_nrt.src.data.zhilabs_data import get_adsl_population
        df_dsl = get_adsl_population(self.SPARK, closing_day, n_days=n_days, active=True, extra_filters=extra_filt, pre_pro=True,  force_gen=force_gen)


        return df_dsl

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class TriggerZhilabsModel(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, tech, critical,  force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)
        self.TECH = tech
        self.CRITICAL = critical
    # Build the set for training (labeled) or predicting (unlabeled) max_cpe_cpu_usage
    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None,  *args, **kwargs):
        tech = self.TECH
        critical = self.CRITICAL
        if tech == 'ftth':
            from pyspark.sql.functions import concat, col, lit, count
            df = FTTHTriggerCar(self.SPARK,  self.METADATA, 30, critical).get_module(closing_day).fillna({'blindaje': 'unknown'}).fillna({'cpe_model': 'unknown'}).fillna({'network_sharing': 'unknown'})
            '''
            df = df.withColumn("min_memory", col('zhilabs_ftth_min_cpe_memory_free') / col('zhilabs_ftth_max_cpe_memory_total'))
            df = df.withColumn('flag_sharing', when(col('network_sharing')!='', concat(lit('flag_sharing='),col('network_sharing'))).otherwise('flag_sharing=0'))\
            .where((col('zhilabs_ftth_mean_cpe_quick_restarts')> 0.0035)|(col('zhilabs_ftth_mean_wlan_2_4_stats_errors_received')> 1)|(col('zhilabs_ftth_max_ftth_ont_prx_average')> -18)|(col('zhilabs_ftth_min_ftth_olt_prx_average')> -20)|(col('zhilabs_ftth_min_ftth_ont_prx_average')<-29)|(col('zhilabs_ftth_mean_cpe_memory_total')>124e3)| (col('zhilabs_ftth_max_cpe_cpu_usage') > 85)| (col('zhilabs_ftth_max_mod_ftth_ber_up_average') > 300) | (col("min_memory") < 0.25))
            df = df.withColumn('flag_router', when(((col('zhilabs_ftth_mean_cpe_quick_restarts')> 0.0035)|(col('zhilabs_ftth_mean_cpe_memory_total')> 124e3)),'flag_router=1').otherwise('flag_router=0'))
            df = df.withColumn('flag_error', when(((col('zhilabs_ftth_mean_wlan_2_4_stats_errors_received')> 1)),'flag_error=1').otherwise('flag_error=0'))
            df = df.withColumn('flag_net', when(((col('zhilabs_ftth_min_ftth_olt_prx_average')> -20)|(col('zhilabs_ftth_max_ftth_ont_prx_average')> -18)|(col('zhilabs_ftth_min_ftth_ont_prx_average')<-29)),'flag_net=1').otherwise('flag_net=0'))
            df = df.where(col('num_tickets_tipo_tramitacion_w8')==0).fillna(0.0).withColumn('network_sharing', when(col('network_sharing')=='','None').otherwise(col('network_sharing')))\
            .withColumn('cpe_model', when(col('cpe_model')=='','Others').otherwise(col('cpe_model'))) \
            .withColumn('network_sharing', when(col('network_sharing') == 'MASMOVIL', 'None').otherwise(col('network_sharing'))).withColumn("flag_issue", when(((col('zhilabs_ftth_min_ftth_ont_prx_average') < -27) | (
            col('zhilabs_ftth_max_cpe_cpu_usage') > 85) | (col('zhilabs_ftth_max_mod_ftth_ber_up_average') > 300) | (col("min_memory") < 0.25)),"flag_issue=1").otherwise("flag_issue=0"))
            '''
            from churn_nrt.src.projects.models.trigger_zhilabs.constants import DICT_CPU, DICT_CPE, DICT_CPU_WAR, DICT_CPE_WAR
            if critical == "critical":
                cpe_dict = DICT_CPE
            else:
                cpe_dict = DICT_CPE_WAR

            names = cpe_dict.keys()
            vals = cpe_dict.values()
            data_cpe = {'cpe_model': names, 'threshold_memory': vals}
            import pandas as pd
            cpe_df = self.SPARK.createDataFrame(pd.DataFrame(data_cpe))

            if critical == "critical":
                cpu_dict = DICT_CPU
                ber_up = 'zhilabs_ftth_count_flag_ber_up_critical'
                ber_down = 'zhilabs_ftth_count_flag_ber_down_critical'
                val =3
                val_px = -35
            else:
                cpu_dict = DICT_CPU_WAR
                ber_up = 'zhilabs_ftth_count_flag_ber_up_warning'
                ber_down = 'zhilabs_ftth_count_flag_ber_down_warning'
                val =1
                val_px = -32
            names = cpu_dict.keys()
            vals = cpu_dict.values()
            data_cpu = {'cpe_model': names, 'threshold_cpu': vals}
            import pandas as pd
            cpu_df = self.SPARK.createDataFrame(pd.DataFrame(data_cpu))

            df = df.join(cpu_df, ['cpe_model'], 'left').fillna({'threshold_cpu': 85.0}) \
                .join(cpe_df, ['cpe_model'], 'left').fillna({'threshold_memory': 0.25})

            df_ftth = df.withColumn("min_memory", col('zhilabs_ftth_min_cpe_memory_free') / col('zhilabs_ftth_max_cpe_memory_total'))

            df_target = df_ftth.where((col('min_memory') < col('threshold_memory') / 100) | (col('zhilabs_ftth_max_cpe_cpu_usage') > col('threshold_cpu')) | \
                (col(ber_up) > 3) | (col(ber_down) > 3) | (col('zhilabs_ftth_min_ftth_ont_prx_average') < val_px) | \
                (col('zhilabs_ftth_days_with_quick_restarts') > 5) | (col('zhilabs_ftth_max_num_restarts_day') > val))
            df_target = df_target.cache()
            df = df_target.withColumn('flag_sharing', when(col('network_sharing') != '',concat(lit('flag_sharing='), col('network_sharing'))).otherwise('flag_sharing=0'))
            df = df.withColumn('flag_router', when(((col('zhilabs_ftth_mean_cpe_quick_restarts') > 0.0035) | (col('zhilabs_ftth_mean_cpe_memory_total') > 124e3)), 'flag_router=1').otherwise('flag_router=0'))
            df = df.withColumn('flag_error', when(((col('zhilabs_ftth_mean_wlan_2_4_stats_errors_received') > 1)),'flag_error=1').otherwise('flag_error=0'))
            df = df.withColumn('flag_net', when(((col('zhilabs_ftth_min_ftth_olt_prx_average') > -20) | (col('zhilabs_ftth_max_ftth_ont_prx_average') > -18) | (col('zhilabs_ftth_min_ftth_ont_prx_average') < -29)),'flag_net=1').otherwise('flag_net=0'))
            #.where(col('num_tickets_tipo_tramitacion_w8') == 0)
            df = df.fillna(0.0).withColumn('network_sharing', when(
                col('network_sharing') == '', 'None').otherwise(col('network_sharing'))) \
                .withColumn('cpe_model', when(col('cpe_model') == '', 'Others').otherwise(col('cpe_model'))) \
                .withColumn('network_sharing', when(col('network_sharing') == 'MASMOVIL', 'None').otherwise(
                col('network_sharing'))).withColumn("flag_issue",when(((col('zhilabs_ftth_min_ftth_ont_prx_average') < -27) | (
                col('zhilabs_ftth_max_cpe_cpu_usage') > 85) | (col('zhilabs_ftth_max_mod_ftth_ber_up_average') > 300) | (col("min_memory") < 0.25)),
                "flag_issue=1").otherwise("flag_issue=0"))
            print"Volume of customers under conditions: {}".format(df.count())

        elif tech == 'hfc':
            from pyspark.sql.functions import concat, col, lit
            df = HFCTriggerCar(self.SPARK, self.METADATA).get_module(closing_day)
            df = df.where((col('zhilabs_hfc_mean_wlan_2_4_stats_errors_received')> 0.023)|(col('zhilabs_hfc_max_lan_ethernet_stats_mbytes_sent')> 7780)|\
           (col('zhilabs_hfc_max_wlan_2_4_stats_packets_sent')> 3.9e8)|(col('zhilabs_hfc_mean_hfc_percent_words_uncorrected_upstream')> 0.05)|\
            (col('zhilabs_hfc_mean_issue_hfc_fec_upstream_critical_current')> 0.003)|\
            (col('inc_zhilabs_hfc_max_hfc_number_of_flaps_current_w1w1')> 5)|(col('zhilabs_hfc_mean_hfc_percent_words_uncorrected_upstream')> 0.05)|\
            (col('zhilabs_hfc_std_issue_hfc_fec_upstream_critical_current')> 0.065)|\
            (col('zhilabs_hfc_max_lan_host_802_11_num__entries')> 8)|(col('zhilabs_hfc_mean_cpe_quick_restarts')> 0.008)|\
            (col('inc_zhilabs_hfc_mean_cpe_quick_restarts_w1w1')> 0.01))
            df = df.where(col('num_tickets_tipo_tramitacion_w8') == 0).fillna(0.0)
        elif tech == 'dsl':
            df = DSLTriggerCar(self.SPARK, self.METADATA).get_module(closing_day)
        else:
            print('Error: tech must be ftth, hfc or dsl')
            import sys
            sys.exit(0)

        if labeled:
            from churn_nrt.src.data.sopos_dxs import Target
            target_ncs = Target(self.SPARK, churn_window=30, level='num_cliente').get_module(closing_day, save=False, save_others=False, force_gen=False)
            df = df.join(target_ncs, on=["num_cliente"], how="inner").fillna({'label':0.0})

            from pyspark.sql.functions import count
            df.groupBy('label').agg(count('*')).show()
        from churn_nrt.src.projects.models.trigger_zhilabs.metadata import get_metadata
        if tech == "dsl":
            from churn_nrt.src.projects.models.trigger_zhilabs.metadata import METADATA_STANDARD_MODULES_DSL
            sources = METADATA_STANDARD_MODULES_DSL
        elif tech == "ftth":
            from churn_nrt.src.projects.models.trigger_zhilabs.metadata import METADATA_STANDARD_MODULES
            sources = METADATA_STANDARD_MODULES
        elif tech == "hfc":
            from churn_nrt.src.projects.models.trigger_zhilabs.metadata import METADATA_STANDARD_MODULES_HFC
            sources = METADATA_STANDARD_MODULES_HFC

        metadata_df = get_metadata(self.SPARK, sources)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]


        if labeled:
            if tech == 'ftth':
                sel_cols = sel_cols + ['flag_net','flag_error','flag_router','flag_sharing', 'flag_issue']
            df = df.select(['num_cliente', 'nif_cliente','label'] + sel_cols)
        else:
            if tech == 'ftth':
                sel_cols = sel_cols + ['flag_net', 'flag_error', 'flag_router', 'flag_sharing', 'flag_issue']
            df = df.select(['num_cliente', 'nif_cliente'] + sel_cols)

        return df
