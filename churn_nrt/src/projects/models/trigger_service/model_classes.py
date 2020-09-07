#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, when, lit#, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce

from churn_nrt.src.data_utils.DataTemplate import TriggerDataTemplate
from churn_nrt.src.projects_utils.models.ModelTemplate import ModelTemplate



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR DATA STRUCTURE
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This class extends for DataTemplate to inheritates the functions to deal with modules.
# Only required to implement the build_module.
class TriggerServiceData(TriggerDataTemplate): # unlabeled

    NDAYS_LENGTH_PERIOD = 30

    def __init__(self, spark, metadata_obj, period_length=30):
        TriggerDataTemplate.__init__(self, spark, "trigger_service", metadata_obj)
        self.NDAYS_LENGTH_PERIOD = period_length

    def build_module(self, closing_day, save_others, select_cols=None, force_gen=False, *args):

        from churn_nrt.src.data.customer_base import CustomerBase
        base_df = CustomerBase(self.SPARK).get_module(closing_day, force_gen=force_gen)

        base_df = base_df.cache()
        print'Size of the customer base (NIFs): ' + str(base_df.select('nif_cliente').distinct().count())

        base_df = base_df.filter(col('segment_nif') != 'Pure_prepaid').withColumn('blindaje', lit('none')) \
        .withColumn('blindaje',when((col('tgs_days_until_f_fin_bi') >= 0) & (col('tgs_days_until_f_fin_bi') <= 60),'soft').otherwise(col('blindaje')))\
        .withColumn('blindaje',when((col('tgs_days_until_f_fin_bi') > 60),'hard').otherwise(col('blindaje'))) \
        .select('nif_cliente', 'blindaje','tgs_days_until_f_fin_bi', 'nb_rgus_nif', 'nb_tv_services_nif', 'rgus_list','segment_nif', 'tgs_has_discount').drop_duplicates(['nif_cliente'])

        ############## Filters ##############
        from churn_nrt.src.data_utils.base_filters import keep_active_services, get_non_recent_customers_filter, get_disconnection_process_filter, get_churn_call_filter

        base_df = keep_active_services(base_df)
        print'Size of the active customer base (NIFs): ' + str(base_df.count())

        df_non_recent = get_non_recent_customers_filter(self.SPARK, closing_day, 90, level='nif', verbose=True, only_active=True)
        df_disc = get_disconnection_process_filter(self.SPARK, closing_day, 90, verbose=False)
        df_churn_calls = get_churn_call_filter(self.SPARK, closing_day, 56, level='nif', filter_function=keep_active_services, verbose=False)

        base_df_non_recent = base_df.join(df_non_recent, ['nif_cliente'], 'inner')
        print'Size of the stable active customer base (NIFs): ' + str(base_df_non_recent.count())
        base_df_disc = base_df_non_recent.join(df_disc, ['nif_cliente'], 'inner')
        print'Size of the stable active customer base (NIFs) without recent dx: ' + str(base_df_disc.count())
        base_df = base_df_disc.join(df_churn_calls, ['nif_cliente'], 'inner')
        print'Size of the stable active customer base (NIFs) without recent churn cancellations and dx: ' + str(base_df.count())

        ############## Customer Additional ##############
        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_98 = move_date_n_days(closing_day, n=-98)
        tr_base_98 = CustomerBase(self.SPARK).get_module(closing_day_98, force_gen = force_gen).drop_duplicates(['nif_cliente'])
        tr_base_98 = keep_active_services(tr_base_98)
        tr_base_98 = tr_base_98.withColumnRenamed('nb_rgus_nif', 'nb_rgus_d98')
        base_df = base_df.join(tr_base_98.select('nif_cliente', 'nb_rgus_d98'), ['nif_cliente'], 'left').withColumn('diff_rgus_d_d98', col('nb_rgus_nif') - col('nb_rgus_d98'))
        base_df = base_df.where(col('diff_rgus_d_d98')>=0.0)
        ############## Services ##############
        from churn_nrt.src.data.customers_data import Customer
        df_services = Customer(self.SPARK).get_module(closing_day,force_gen=force_gen)
        df_services = df_services.withColumn("is_superoferta", when(col("SUPEROFERTA") == "ON19", 1).otherwise(0))
        from pyspark.sql.functions import sum as sql_sum
        base_df_ = CustomerBase(self.SPARK).get_module(closing_day,force_gen=force_gen).select('nif_cliente', 'num_cliente').distinct()
        df_services = df_services.select('num_cliente', 'is_superoferta').join(base_df_, ['num_cliente'], 'left')
        df_on19_nif = (df_services.groupBy("nif_cliente").agg(sql_sum("is_superoferta").alias("nb_superofertas")))
        base_df = base_df.join(df_on19_nif, ['nif_cliente'], 'left')

        ############## CCC ##############
        from churn_nrt.src.data.ccc import CCC
        tr_ccc_all = CCC(self.SPARK, level="nif").get_module(closing_day, force_gen=force_gen)
        print'Number of distinct NIFs in CCC: ' + str(tr_ccc_all.select('nif_cliente').distinct().count())
        #bill_cols = [c for c in tr_ccc_all.columns if 'billing' in c.lower()]
        print('[Info build_trigger_service_car] CCC info loaded')
        tr_ccc_base = base_df.join(tr_ccc_all, on=["nif_cliente"], how="left")

        ############## Orders ##############
        from churn_nrt.src.data.orders_sla import OrdersSLA
        tr_orders = OrdersSLA(self.SPARK).get_module(closing_day, force_gen=force_gen).select('nif_cliente', 'nb_started_orders_last30','nb_running_last30_gt5','has_forbidden_orders_last90','nb_started_orders_last14', 'nb_started_orders_last7')
        print'Number of distinct NIFs in orders: ' + str(tr_orders.select('nif_cliente').distinct().count())
        print('[Info build_trigger_service_car] ORDERS info loaded')

        ############## Forbidden orders filter ##############
        # TODO: create forbidden orders filter function
        tr_input_df_ = tr_ccc_base.join(tr_orders, on=["nif_cliente"], how="left").fillna({"has_forbidden_orders_last90": 0})
        print'Size after orders join: ' + str(tr_input_df_.count())
        tr_input_df_ = tr_input_df_.cache()
        print("Filtering clients with forbidden orders last 90 days")
        before_cust = tr_input_df_.count()
        tr_input_df_filt = tr_input_df_.where(~(col("has_forbidden_orders_last90") > 0))
        after_cust = tr_input_df_filt.count()
        print("Filtered NIFs with forbidden orders before={} after={} eliminated={}".format(before_cust, after_cust, before_cust - after_cust))

        ############## Reimbursements ##############
        from churn_nrt.src.data.reimbursements import Reimbursements, reimb_map
        tr_reimb_df_nrt = Reimbursements(self.SPARK).get_module(closing_day, force_gen=force_gen).withColumnRenamed("NIF_CLIENTE", "nif_cliente")
        tr_reimb_df_nrt = tr_reimb_df_nrt.drop_duplicates(["nif_cliente"])
        print('[Info build_trigger_service_car] REIMBURSEMENTS info loaded')
        print'Number of distinct NIFs in reimbursements: ' + str(tr_reimb_df_nrt.select('nif_cliente').distinct().count())

        tr_input_df = tr_input_df_filt.join(tr_reimb_df_nrt, ['nif_cliente'], 'left')
        tr_input_df = tr_input_df.cache()
        print'Size after reimbursements join: ' + str(tr_input_df.count())
        ############## Billing ##############
        from churn_nrt.src.data.billing import Billing

        tr_billing_df = Billing(self.SPARK).get_module(closing_day, force_gen=force_gen)


        tr_billing_df = tr_billing_df.drop('nif_cliente').join(base_df_, ['num_cliente'], 'left').drop_duplicates(["nif_cliente"])


        print'Number of distinct NIFs in billing: ' + str(tr_billing_df.select('nif_cliente').count())

        tr_billing_df = tr_billing_df.drop(*['DEVICE_DELIVERY_REPAIR_w8',
                                                           'INTERNET_EN_EL_MOVIL_w8',
                                                           'QUICK_CLOSING_w8',
                                                           'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w8',
                                                           'TARIFF_MANAGEMENT_w8',
                                                           'PRODUCT_AND_SERVICE_MANAGEMENT_w8',
                                                           'MI_VODAFONE_w8',
                                                           'CHURN_CANCELLATIONS_w8',
                                                           'DSL_FIBER_INCIDENCES_AND_SUPPORT_w8',
                                                           'PREPAID_BALANCE_w8',
                                                           'COLLECTIONS_w8',
                                                           'NEW_ADDS_PROCESS_w8',
                                                           'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w8',
                                                           'DEVICE_UPGRADE_w8',
                                                           'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w8',
                                                           'BILLING_POSTPAID_w8',
                                                           'BUCKET_w8',
                                                           'Reimbursement_days_since',
                                                           'segment_nif'])

        tr_billing_df = tr_billing_df
        tr_billing_df = tr_billing_df.cache()

        print('[Info build_trigger_service_car] BILLING info loaded')

        tr_input_df = tr_input_df.join(tr_billing_df.drop("num_cliente"), ['nif_cliente'], 'left')
        tr_input_df = tr_input_df.cache()
        print'Size after billing join: ' + str(tr_input_df.count())

        ############## Tickets ##############
        from churn_nrt.src.data.tickets import Tickets
        df_tickets = Tickets(self.SPARK).get_module(closing_day, force_gen=force_gen)
        print'Number of distinct NIFs in tickets: ' + str(df_tickets.select('nif_cliente').distinct().count())
        tr_input_df = tr_input_df.join(df_tickets.drop("num_cliente"), ['nif_cliente'], 'left')
        tr_input_df = tr_input_df.cache()
        ############## Diff rgus ##############
        tr_input_df = tr_input_df.withColumn('flag_incremento_rgus', when(col('diff_rgus_d_d98')> 0, 1.0).otherwise(0.0))
        print'Size after tickets join: ' + str(tr_input_df.count())

        from churn_nrt.src.projects.models.trigger_service.metadata import get_metadata

        metadata_df = get_metadata(self.SPARK)
        sel_cols = metadata_df.select("feature").rdd.flatMap(lambda x: x).collect()
        sel_cols = [str(f) for f in sel_cols]

        tr_input_df = tr_input_df.select(['nif_cliente'] + sel_cols + ['nb_started_orders_last7'])

        print('Feature columns:')

        print(sel_cols)
        ############## Fill NA ##############
        tr_input_df = self.METADATA.fillna(tr_input_df, sources=None, cols=sel_cols)

        tr_input_df = tr_input_df.cache()
        print'Final df size: ' + str(tr_input_df.count())

        return tr_input_df


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CLASS FOR MODELING
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


class TriggerServiceModel(ModelTemplate):

    def __init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj,force_gen=False):
        ModelTemplate.__init__(self, spark, tr_date, mode_, algorithm, owner_login, metadata_obj, force_gen=force_gen)


    # Build the set for training (labeled) or predicting (unlabeled)
    def get_set(self, closing_day,labeled, feats_cols=None, set_name=None, *args, **kwargs):

        df = TriggerServiceData(self.SPARK, self.METADATA).get_module(closing_day, force_gen=self.FORCE_GEN)

        df = df.withColumn('num_tickets',col('num_tickets_tipo_incidencia_w8') + col('num_tickets_tipo_reclamacion_w8') + col('num_tickets_tipo_averia_w8'))

        df = df.withColumn('flag_reimb', when(col('Reimbursement_adjustment_net') != 0, 1).otherwise(0))
        df = df.withColumn('flag_billshock', when((col('greatest_diff_bw_bills') > 35) & (col('diff_rgus_d_d98') <= 0), 1).otherwise(0))
        df = df.withColumn('flag_fact', when(col('num_tickets_tipo_tramitacion_w8') > 0, 1).otherwise(0))
        df = df.withColumn('flag_tickets', when(col('num_tickets') > 0, 1).otherwise(0))
        df = df.withColumn('flag_orders', when(col('nb_running_last30_gt5') > 0, 1).otherwise(0))

        df = df.where((col('flag_reimb') > 0) | (col('flag_billshock') > 0) | (col('flag_fact') > 0) | (col('flag_tickets') > 0) | (col('flag_orders') > 0))
        print'Size of the tr car (only with problems): ' + str(df.count())
        #| (col('flag_billshock') > 0)
        #df = df.where((col('blindaje')=='none')|(col('blindaje')=='soft'))
        #print'Size of the tr car (only with problems and unbounded): ' + str(df.count())

        print'Number of customers with reimbursements: ' + str(
            df.where(col('flag_reimb') > 0).count())
        print'Number of customers with billshock: ' + str(
            df.where(col('flag_billshock') > 0).count())
        print'Number of customers with orders: ' + str(
            df.where(col('flag_orders') > 0).count())
        print'Number of customers with tickets: ' + str(
            df.where(col('flag_tickets') > 0).count())
        print'Number of customers with fact tickets: ' + str(
            df.where(col('flag_fact') > 0).count())


        if labeled:
            from churn_nrt.src.data.sopos_dxs import Target
            target_nifs = Target(self.SPARK, churn_window=30, level='nif').get_module(closing_day, save=False, save_others=False, force_gen=True)

            df = df.join(target_nifs, on=["nif_cliente"], how="left").fillna({"label": 0.0})

            from pyspark.sql.functions import count
            df.groupBy('label').agg(count('*')).show()

        return df