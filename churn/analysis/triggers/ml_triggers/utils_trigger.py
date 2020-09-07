# -*- coding: utf-8 -*-
import sys
from datetime import datetime as dt
import imp
import os
from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull,substring, size, length, desc)
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql.functions import avg as sql_avg


def get_trigger_minicar3(spark, closing_day):
    from churn.models.fbb_churn_amdocs.utils_fbb_churn import pathExist
    from churn.analysis.triggers.ml_triggers.metadata_triggers import get_billing_metadata, get_tickets_metadata, \
        get_reimbursements_metadata, get_ccc_metadata, get_customer_metadata, get_orders_metadata

    path = '/data/udf/vf_es/churn/triggers/car_test_filtered/year={}/month={}/day={}'.format(
        int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    if pathExist(path):

        print "[Info Main Trigger Loader]:  File " + str(path) + " already exists. Reading it."
        df_car = spark.read.parquet(path)
        return df_car

    else:

        print("############ Loading customer base #############")

        from pykhaos.utils.date_functions import move_date_n_days
        closing_day_ = move_date_n_days(closing_day, n=-98)
        from churn.analysis.triggers.orders.customer_master import get_customer_master
        df_cust_c = get_customer_master(spark, closing_day, unlabeled=False).select("nif_cliente", "nb_rgus",
                                                                                    'segment_nif')

        df_cust_c_98 = get_customer_master(spark, closing_day_, unlabeled=False).withColumnRenamed('nb_rgus','nb_rgus_98').filter(col('segment_nif') != 'Pure_prepaid')
        df_rgus = df_cust_c.join(df_cust_c_98.select('nif_cliente', 'nb_rgus_98'), ['nif_cliente'], 'left').withColumn('diff_rgus_d_d98', col('nb_rgus') - col('nb_rgus_98')).select('nif_cliente', 'diff_rgus_d_d98','segment_nif').filter(col('segment_nif') != 'Pure_prepaid')

        print('Applying diff rgus filtering:')
        num_ori = df_rgus.count()
        df_rgus = df_rgus.where( ~ ( (col('diff_rgus_d_d98')<0) & (col("segment_nif")=="Standalone_FBB")))
        num_fin = df_rgus.count()
        print'Number of filtered customers: ' + str(num_ori - num_fin)

        from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
        df_base_msisdn_ = get_customer_base_segment(spark, date_=closing_day)
        base_filtered = df_base_msisdn_.join(df_rgus, ['nif_cliente'], 'inner')
        df_base_msisdn = base_filtered.filter(col('seg_pospaid_nif') != 'Pure_prepaid').select('nif_cliente','msisdn').distinct()
        print'Size of customer base: {}'.format(df_base_msisdn.count())
        print'Number of NIFs in customer base: {}'.format(df_base_msisdn.select('NIF_CLIENTE').distinct().count())

        print("############ Adding CCC info to filter the CAR ############")
        # CCC feats are required for filtering
        from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_period_attributes
        from pykhaos.utils.date_functions import move_date_n_days

        ccc_df = get_nif_ccc_period_attributes(spark, move_date_n_days(closing_day, n=-60), closing_day, df_base_msisdn, suffix="_w8")

        df_tar = df_base_msisdn \
            .join(ccc_df, ['nif_cliente'], 'left') \
            .na.fill(0) \
            .filter(col('CHURN_CANCELLATIONS_w8') == 0) \
            .select('nif_cliente', 'msisdn')

        print'Size of filtered customer base: {}'.format(df_tar.count())

        print("############ Adding Billing features ############")
        ###################
        # get_billing_car calls to getIDS
        ##################
        billingdf_f = get_billing_car(spark, closing_day)

        billing_meta = get_billing_metadata(spark, billingdf_f)

        billing_map_tmp = billing_meta.select("feature", "imp_value", "type").rdd.map(
            lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        billing_map = dict(
            [(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in billing_map_tmp if
             x[0] in billingdf_f.columns])

        billing_tar = df_tar.drop('segment_nif', 'diff_rgus_d_d98').join(billingdf_f, ['NIF_CLIENTE'],
                                                                              'left').na.fill(billing_map)

        print("############ Adding Reimbursements features ############")
        ###################
        # Apparently, get_reimbursements_car could be used for any date
        ##################
        from churn.analysis.triggers.billing.reimbursements_funct import get_reimbursements_car
        reimb_tr = get_reimbursements_car(spark, closing_day)
        reimb_meta = get_reimbursements_metadata(spark)

        reimb_map_tmp = reimb_meta.select("feature", "imp_value", "type").rdd.map(
            lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        reimb_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in reimb_map_tmp if
                          x[0] in reimb_tr.columns])

        billing_reimb = billing_tar.join(reimb_tr, ['NIF_CLIENTE'], 'left').na.fill(reimb_map)

        print("############ Adding Tickets features ############")
        tickets = get_tickets_car(spark, closing_day)
        tickets_meta = get_tickets_metadata(spark)
        tickets_map_tmp = tickets_meta.select("feature", "imp_value", "type").rdd.map(
            lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        from pyspark.sql.functions import sum
        from pyspark.sql import Window
        window = Window.partitionBy("NIF_CLIENTE")
        df = tickets.where(((col('TIPO_TICKET') != 'Activación/Desactivación') & (col('TIPO_TICKET') != 'Baja') & (
                col('TIPO_TICKET') != 'Portabilidad') & (col('TIPO_TICKET') != 'Problemas con portabilidad')) | (
                               col('TIPO_TICKET').isNull()))

        df_tickets = df.withColumn('tmp_fact', when(((col('X_TIPO_OPERACION') == 'Tramitacion')), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_FACT', sum('tmp_fact').over(window)) \
            .withColumn('tmp_rec', when((col('X_TIPO_OPERACION') == 'Reclamacion'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_REC', sum('tmp_rec').over(window)) \
            .withColumn('tmp_av', when((col('X_TIPO_OPERACION') == 'Averia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_AV', sum('tmp_av').over(window)) \
            .withColumn('tmp_inc', when((col('X_TIPO_OPERACION') == 'Incidencia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_INC', sum('tmp_inc').over(window))

        df_tickets_sel = df_tickets.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT', 'NUM_TICKETS_TIPO_REC',
                                           'NUM_TICKETS_TIPO_AV', 'NUM_TICKETS_TIPO_INC',
                                           'NUM_TICKETS_TIPO_INF').drop_duplicates(subset=['NIF_CLIENTE'])

        tickets_map = dict(
            [(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in tickets_map_tmp if
             x[0] in df_tickets_sel.columns])

        billing_reimb_tickets = billing_reimb.join(df_tickets_sel, ['NIF_CLIENTE'], 'left').na.fill(tickets_map)

        #.withColumn('tmp_inf', when((((col('X_TIPO_OPERACION') == 'Informacion'))), 1.0).otherwise(0.0)) \
        #    .withColumn('NUM_TICKETS_TIPO_INF', sum('tmp_inf').over(window)) \
        print("############ Adding Orders features ############")
        ###################
        # Apparently, get_orders_module could be used for any date
        ##################
        orders_path = '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/year={}/month={}/day={}'.format(
            int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))

        from pykhaos.utils.hdfs_functions import check_hdfs_exists
        if check_hdfs_exists(orders_path) == False:
            from churn.datapreparation.general.sla_data_loader import get_orders_module, save_module
            df_orders = get_orders_module(spark, closing_day, None)
            for order_col in df_orders.columns:
                print '[Info order_col: ]' + order_col
            save_module(df_orders, closing_day, '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/')

        orders_tr = spark.read.load(orders_path)

        orders_tr = orders_tr.cache()

        orders_meta = get_orders_metadata(spark, orders_tr)
        orders_map_tmp = orders_meta.select("feature", "imp_value", "type").rdd.map(
            lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        orders_map = dict(
            [(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in orders_map_tmp if
             x[0] in orders_tr.columns])

        df_car = billing_reimb_tickets.join(orders_tr, ['NIF_CLIENTE'], 'left').na.fill(orders_map)  # .na.fill(-10.0)

        # print'Size of final car: {}'.format(df_car.count())
        # print'Number of different NIFs in final car: {}'.format(df_car.select('NIF_CLIENTE').distinct().count())
        df_car = df_car.drop('num_cliente').drop('msisdn').distinct()
        print'Size of final car (after drop): {}'.format(df_car.count())
        print'Number of different NIFs in final car (after drop): {}'.format(
            df_car.select('NIF_CLIENTE').distinct().count())
        df_car.repartition(200).write.save(path, format='parquet', mode='append')
        return df_car

def get_trigger_minicar2(spark, closing_day):
    from churn.models.fbb_churn_amdocs.utils_fbb_churn import pathExist
    from churn.analysis.triggers.ml_triggers.metadata_triggers import get_billing_metadata, get_tickets_metadata, \
        get_reimbursements_metadata, get_ccc_metadata, get_customer_metadata, get_orders_metadata

    path = '/data/udf/vf_es/churn/triggers/car_test/year={}/month={}/day={}'.format(
            int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    if pathExist(path):

        print "[Info Main Trigger Loader]:  File " + str(path) + " already exists. Reading it."
        df_car = spark.read.parquet(path)
        return df_car

    else:

        print("############ Loading customer base #############")
        from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
        df_base_msisdn = get_customer_base_segment(spark, date_=closing_day)
        df_base_msisdn = df_base_msisdn.filter(col('seg_pospaid_nif')!='Pure_prepaid').select('nif_cliente', 'msisdn').distinct()
        print'Size of customer base: {}'.format(df_base_msisdn.count())
        print'Number of NIFs in customer base: {}'.format(df_base_msisdn.select('NIF_CLIENTE').distinct().count())

        print("############ Adding CCC info to filter the CAR ############")
        # CCC feats are required for filtering
        from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_period_attributes
        from pykhaos.utils.date_functions import move_date_n_days

        ccc_df = get_nif_ccc_period_attributes(spark, move_date_n_days(closing_day, n=-60), closing_day, df_base_msisdn, suffix="_w8")

        df_tar = df_base_msisdn\
            .join(ccc_df, ['nif_cliente'], 'left')\
            .na.fill(0)\
            .filter(col('CHURN_CANCELLATIONS_w8') == 0)\
            .select('nif_cliente', 'msisdn')

        print'Size of filtered customer base: {}'.format(df_tar.count())

        print("############ Second filtering ############")
        from churn.analysis.triggers.billing.run_segment_billing import get_billing_module
        tr_billing_df, _ = get_billing_module(spark, closing_day, labeled_mini_ids=True)

        tr_input_df = df_tar.join(tr_billing_df.select('NIF_CLIENTE','diff_rgus_d_d98','segment_nif'), ['NIF_CLIENTE'], 'left')



        tr_input_df = tr_input_df.where(~ ((col('diff_rgus_d_d98') < 0) & (col("segment_nif") == "Standalone_FBB")))

        print("############ Adding Billing features ############")
        ###################
        # get_billing_car calls to getIDS
        ##################
        billingdf_f = get_billing_car(spark, closing_day)

        billing_meta = get_billing_metadata(spark, billingdf_f)

        billing_map_tmp = billing_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        billing_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in billing_map_tmp if x[0] in billingdf_f.columns])

        billing_tar = tr_input_df.drop('segment_nif','diff_rgus_d_d98').join(billingdf_f, ['NIF_CLIENTE'], 'left').na.fill(billing_map)



        print("############ Adding Reimbursements features ############")
        ###################
        # Apparently, get_reimbursements_car could be used for any date
        ##################
        from churn.analysis.triggers.billing.reimbursements_funct import get_reimbursements_car
        reimb_tr = get_reimbursements_car(spark, closing_day)
        reimb_meta = get_reimbursements_metadata(spark)

        reimb_map_tmp = reimb_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        reimb_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in reimb_map_tmp if x[0] in reimb_tr.columns])

        billing_reimb = billing_tar.join(reimb_tr, ['NIF_CLIENTE'], 'left').na.fill(reimb_map)

        print("############ Adding Tickets features ############")
        tickets = get_tickets_car(spark, closing_day)
        tickets_meta = get_tickets_metadata(spark)
        tickets_map_tmp = tickets_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()


        from pyspark.sql.functions import sum
        from pyspark.sql import Window
        window = Window.partitionBy("NIF_CLIENTE")
        df = tickets.where(((col('TIPO_TICKET') != 'Activación/Desactivación') & (col('TIPO_TICKET') != 'Baja') & (
                col('TIPO_TICKET') != 'Portabilidad') & (col('TIPO_TICKET') != 'Problemas con portabilidad')) | (
                               col('TIPO_TICKET').isNull()))

        df_tickets = df.withColumn('tmp_fact', when(((col('X_TIPO_OPERACION') == 'Tramitacion')), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_FACT', sum('tmp_fact').over(window)) \
            .withColumn('tmp_rec', when((col('X_TIPO_OPERACION') == 'Reclamacion'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_REC', sum('tmp_rec').over(window)) \
            .withColumn('tmp_inf', when((((col('X_TIPO_OPERACION') == 'Informacion'))), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_INF', sum('tmp_inf').over(window)) \
            .withColumn('tmp_av', when((col('X_TIPO_OPERACION') == 'Averia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_AV', sum('tmp_av').over(window)) \
            .withColumn('tmp_inc', when((col('X_TIPO_OPERACION') == 'Incidencia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_INC', sum('tmp_inc').over(window))

        df_tickets_sel = df_tickets.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT', 'NUM_TICKETS_TIPO_REC',
                                           'NUM_TICKETS_TIPO_AV', 'NUM_TICKETS_TIPO_INC',
                                           'NUM_TICKETS_TIPO_INF').drop_duplicates(subset=['NIF_CLIENTE'])

        tickets_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in tickets_map_tmp if x[0] in df_tickets_sel.columns])

        billing_reimb_tickets = billing_reimb.join(df_tickets_sel, ['NIF_CLIENTE'], 'left').na.fill(tickets_map)

        print("############ Adding Orders features ############")
        ###################
        # Apparently, get_orders_module could be used for any date
        ##################
        orders_path = '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/year={}/month={}/day={}'.format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))

        from pykhaos.utils.hdfs_functions import check_hdfs_exists
        if check_hdfs_exists(orders_path) == False:
            from churn.datapreparation.general.sla_data_loader import get_orders_module, save_module
            df_orders = get_orders_module(spark, closing_day, None)
            for order_col in df_orders.columns:
                print '[Info order_col: ]' + order_col
            save_module(df_orders, closing_day, '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/')

        orders_tr = spark.read.load(orders_path)

        orders_tr = orders_tr.cache()

        orders_meta = get_orders_metadata(spark, orders_tr)
        orders_map_tmp = orders_meta.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        orders_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in orders_map_tmp if x[0] in orders_tr.columns])

        df_car = billing_reimb_tickets.join(orders_tr, ['NIF_CLIENTE'], 'left').na.fill(orders_map)#.na.fill(-10.0)

        #print'Size of final car: {}'.format(df_car.count())
        #print'Number of different NIFs in final car: {}'.format(df_car.select('NIF_CLIENTE').distinct().count())
        df_car = df_car.drop('num_cliente').drop('msisdn').distinct()
        print'Size of final car (after drop): {}'.format(df_car.count())
        print'Number of different NIFs in final car (after drop): {}'.format(df_car.select('NIF_CLIENTE').distinct().count())
        df_car.repartition(200).write.save(path, format='parquet', mode='append')
        return df_car


def get_trigger_minicar(spark, closing_day):
    from churn.models.fbb_churn_amdocs.utils_fbb_churn import pathExist
    path = '/data/udf/vf_es/churn/triggers/car_test/year={}/month={}/day={}'.format(
            int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
    if pathExist(path):

        print "[Info Main Trigger Loader]:  File " + str(path) + " already exists. Reading it."
        df_car = spark.read.parquet(path)
        return df_car

    else:

        print'Prediction date: {}'.format(closing_day)
        print'Loading tickets source for closing_day: {}'.format(closing_day)
        tickets = get_tickets_car(spark, closing_day)
        print("############ Loading customer base #############")
        from churn.datapreparation.general.customer_base_utils import get_customer_base_segment
        df_base_msisdn = get_customer_base_segment(spark, date_=closing_day)
        print'Size of customer base: {}'.format(df_base_msisdn.count())
        print'Number of NIFs in customer base: {}'.format(df_base_msisdn.select('NIF_CLIENTE').distinct().count())
        print("############ Filtering Car ############")
        ###################
        # CCC feats are required for filtering
        ##################
        df_tar = get_filtered_car(spark, closing_day, df_base_msisdn)
        print'Size of filtered customer base: {}'.format(df_tar.count())
        print("############ Loading billing car ############")
        ###################
        # get_billing_car calls to getIDS
        ##################
        billingdf_f = get_billing_car(spark, closing_day)
        billing_tar = df_tar.select('NIF_CLIENTE').join(billingdf_f.drop_duplicates(subset=['NIF_CLIENTE']),
                                                        ['NIF_CLIENTE'], 'left')


        from pyspark.sql.functions import countDistinct, count, sum
        from pyspark.sql import Row, DataFrame, Column, Window
        window = Window.partitionBy("NIF_CLIENTE")
        df = tickets.where(((col('TIPO_TICKET') != 'Activación/Desactivación') & (col('TIPO_TICKET') != 'Baja') & (
                    col('TIPO_TICKET') != 'Portabilidad') & (col('TIPO_TICKET') != 'Problemas con portabilidad')) | (
                               col('TIPO_TICKET').isNull()))

        df_tickets = df.withColumn('tmp_fact', when(((col('X_TIPO_OPERACION') == 'Tramitacion')), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_FACT', sum('tmp_fact').over(window)) \
            .withColumn('tmp_rec', when((col('X_TIPO_OPERACION') == 'Reclamacion'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_REC', sum('tmp_rec').over(window)) \
            .withColumn('tmp_inf', when((((col('X_TIPO_OPERACION') == 'Informacion'))), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_INF', sum('tmp_inf').over(window)) \
            .withColumn('tmp_av', when((col('X_TIPO_OPERACION') == 'Averia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_AV', sum('tmp_av').over(window)) \
            .withColumn('tmp_inc', when((col('X_TIPO_OPERACION') == 'Incidencia'), 1.0).otherwise(0.0)) \
            .withColumn('NUM_TICKETS_TIPO_INC', sum('tmp_inc').over(window))

        print("############ Loading Reimbursements base ############")
        ###################
        # Apparently, get_reimbursements_car could be used for any date
        ##################
        from churn.analysis.triggers.billing.reimbursements_funct import get_reimbursements_car
        reimb_tr = get_reimbursements_car(spark, closing_day)

        billing_reimb = billing_tar.join(reimb_tr, ['NIF_CLIENTE'], 'left').fillna(0)
        df_tickets_sel = df_tickets.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT', 'NUM_TICKETS_TIPO_REC',
                                           'NUM_TICKETS_TIPO_AV', 'NUM_TICKETS_TIPO_INC',
                                           'NUM_TICKETS_TIPO_INF').drop_duplicates(subset=['NIF_CLIENTE'])
        billing_reimb_tickets = billing_reimb.join(df_tickets_sel, ['NIF_CLIENTE'], 'left').fillna(-1)
        print("############ Loading orders base ############")
        ###################
        # Apparently, get_orders_module could be used for any date
        ##################
        orders_path = '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/year={}/month={}/day={}'.format(
            int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))

        from pykhaos.utils.hdfs_functions import check_hdfs_exists
        if check_hdfs_exists(orders_path) == False:
            from churn.datapreparation.general.sla_data_loader import get_orders_module, save_module
            df_orders = get_orders_module(spark, closing_day, None)
            for order_col in df_orders.columns:
                print '[Info order_col: ]' + order_col
            save_module(df_orders, closing_day, '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/')

        orders_tr = spark.read.load(orders_path)

        df_car = billing_reimb_tickets.join(orders_tr, ['NIF_CLIENTE'], 'left').fillna(0)

        print'Size of final car: {}'.format(df_car.count())
        print'Number of different NIFs in final car: {}'.format(df_car.select('NIF_CLIENTE').distinct().count())
        df_car.repartition(200).write.save(path, format='parquet', mode='append')
        return df_car

def getIds(spark, ClosingDay):
    import datetime
    ClosingDay_date=datetime.date(int(ClosingDay[:4]),int(ClosingDay[4:6]),int(ClosingDay[6:8 ]))
    hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))

    # BASIC PATH:
    #- Old Version (without data preparation):
    hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'
    #- New Version (with data preparation):
    #hdfs_write_path_common = '/data/udf/vf_es/amdocs_inf_dataset/'

    path_customer = hdfs_write_path_common +'customer/'+hdfs_partition_path
    path_service = hdfs_write_path_common +'service/'+hdfs_partition_path
    path_customer_agg = hdfs_write_path_common +'customer_agg/'+hdfs_partition_path
    path_voiceusage = hdfs_write_path_common +'usage_geneva_voice/'+hdfs_partition_path
    path_datausage = hdfs_write_path_common +'usage_geneva_data/'+hdfs_partition_path
    path_billing = hdfs_write_path_common +'billing/'+hdfs_partition_path
    path_campaignscustomer = hdfs_write_path_common +'campaigns_customer/'+hdfs_partition_path
    path_campaignsservice = hdfs_write_path_common +'campaigns_service/'+hdfs_partition_path
    #path_roamvoice = hdfs_write_path_common +'usage_geneva_roam_voice/'+hdfs_partition_path !!JOIN WITH IMSI
    path_roamdata = hdfs_write_path_common +'usage_geneva_roam_data/'+hdfs_partition_path
    path_orders_hist = hdfs_write_path_common +'orders/'+hdfs_partition_path
    path_orders_agg = hdfs_write_path_common +'orders_agg/'+hdfs_partition_path
    path_penal_cust = hdfs_write_path_common +'penalties_customer/'+hdfs_partition_path
    path_penal_srv = hdfs_write_path_common +'penalties_service/'+hdfs_partition_path
    path_devices = hdfs_write_path_common +'device_catalogue/'+hdfs_partition_path
    path_ccc = hdfs_write_path_common +'call_centre_calls/'+hdfs_partition_path
    path_tnps = hdfs_write_path_common +'tnps/'+hdfs_partition_path
    path_perms_and_prefs = hdfs_write_path_common +'perms_and_prefs/'+hdfs_partition_path

    # Load HDFS files
    customerDF_load = (spark.read.load(path_customer))
    serviceDF_load = (spark.read.load(path_service))
    customerAggDF_load = (spark.read.load(path_customer_agg))
    voiceUsageDF_load = (spark.read.load(path_voiceusage))
    dataUsageDF_load = (spark.read.load(path_datausage))
    billingDF_load = (spark.read.load(path_billing))
    customerCampaignsDF_load = (spark.read.load(path_campaignscustomer))
    serviceCampaignsDF_load = (spark.read.load(path_campaignsservice))
    #RoamVoiceUsageDF_load = (spark.read.load(path_roamvoice))
    RoamDataUsageDF_load = (spark.read.load(path_roamdata))
    customer_orders_hist_load = (spark.read.load(path_orders_hist))
    customer_orders_agg_load = (spark.read.load(path_orders_agg))
    penalties_cust_level_df_load = (spark.read.load(path_penal_cust))
    penalties_serv_level_df_load = (spark.read.load(path_penal_srv))
    devices_srv_df_load = (spark.read.load(path_devices))
    df_ccc_load = (spark.read.load(path_ccc))
    df_tnps_load = (spark.read.load(path_tnps))
    df_perms_and_prefs=(spark.read.load(path_perms_and_prefs))

    # JOIN
    df_amdocs_ids_service_level=(customerDF_load
        .join(serviceDF_load, 'NUM_CLIENTE', 'inner')
        .join(customerAggDF_load, 'NUM_CLIENTE', 'inner')
        .join(voiceUsageDF_load, (col('msisdn') == col('id_msisdn')), 'leftouter')
        .join(dataUsageDF_load, (col('msisdn')==col('id_msisdn_data')), 'leftouter')
        .join(billingDF_load, col('customeraccount') == customerDF_load.NUM_CLIENTE, 'leftouter')
        .join(customerCampaignsDF_load, col('nif_cliente')==col('cif_nif'), 'leftouter')
        .join(serviceCampaignsDF_load, 'msisdn', 'leftouter')
        #.join(RoamVoiceUsageDF_load, (col('msisdn')==col('id_msisdn_voice_roam')), 'leftouter') NEEDED IMSI FOR JOIN!!!!!!
        .join(RoamDataUsageDF_load, (col('msisdn')==col('id_msisdn_data_roam')), 'leftouter')
        .join(customer_orders_hist_load, 'NUM_CLIENTE', 'leftouter')
        .join(customer_orders_agg_load, 'NUM_CLIENTE', 'leftouter')
        .join(penalties_cust_level_df_load,'NUM_CLIENTE','leftouter')
        .join(penalties_serv_level_df_load, ['NUM_CLIENTE','Instancia_P'], 'leftouter')
        .join(devices_srv_df_load, 'msisdn','leftouter')
        .join(df_ccc_load, 'msisdn','leftouter')
        .join(df_tnps_load, 'msisdn','leftouter')
        .join(df_perms_and_prefs, customerDF_load.NUM_CLIENTE==df_perms_and_prefs.CUSTOMER_ID,'leftouter')
        .drop(*['id_msisdn_data', 'id_msisdn', 'cif_nif', 'customeraccount','id_msisdn_data_roam','id_msisdn_voice_roam','rowNum'])
        .withColumn('ClosingDay',lit(ClosingDay))
        )
    
    return df_amdocs_ids_service_level

def get_billing_car(spark, closing_day):
    # TODO: reemplazar por la función 'def get_billing_customer_df(spark,closing_day):' del IDS
    from pyspark.sql.functions import avg as sql_avg
    ClosingDay = closing_day
    import datetime
    ClosingDay_date=datetime.date(int(ClosingDay[:4]),int(ClosingDay[4:6]),int(ClosingDay[6:8 ]))
    hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))

    #ids = getIds(spark, ClosingDay)

    from churn.analysis.triggers.billing.run_segment_billing import __get_billing_df2
    from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment

    # The obtained base only includes active customers (i.e., filters has been already applied)

    base_df = get_customer_base_segment(spark, closing_day)

    billing_df = __get_billing_df2(spark, closing_day)

    ids = base_df.select('num_cliente').distinct().join(billing_df, ['num_cliente'], 'inner')

    billing_columns = reduce((lambda x, y: x + y), [["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount", "Bill_N" + str(n) + "_Debt_Amount"] for n in range(1,6)]) + ["num_cliente"]

    repartdf = ids.select(billing_columns).repartition(400).cache()

    repartdf= repartdf.withColumn('ClosingDay', lit(ClosingDay))

    refdate = ClosingDay
    [avg_bill_n1_invoicecharges,\
        avg_bill_n1_bill_amount,\
        avg_bill_n1_tax_amount,\
        avg_bill_n1_debt_amount,\
        avg_bill_n2_invoicecharges,\
        avg_bill_n2_bill_amount,\
        avg_bill_n2_tax_amount,\
        avg_bill_n2_debt_amount,\
        avg_bill_n3_invoicecharges,\
        avg_bill_n3_bill_amount,\
        avg_bill_n3_tax_amount,\
        avg_bill_n3_debt_amount,\
        avg_bill_n4_invoicecharges,\
        avg_bill_n4_bill_amount,\
        avg_bill_n4_tax_amount,\
        avg_bill_n4_debt_amount,\
        avg_bill_n5_invoicecharges,\
        avg_bill_n5_bill_amount,\
        avg_bill_n5_tax_amount,\
        avg_bill_n5_debt_amount] = repartdf\
        .filter((col("ClosingDay") == refdate))\
        .filter((col("Bill_N1_InvoiceCharges").isNotNull())\
            & (col("Bill_N1_Amount_To_Pay").isNotNull())\
            & (col("Bill_N1_Tax_Amount").isNotNull())\
            & (col("Bill_N1_Debt_Amount").isNotNull())\
            & (col("Bill_N2_InvoiceCharges").isNotNull())\
            & (col("Bill_N2_Amount_To_Pay").isNotNull())\
            & (col("Bill_N2_Tax_Amount").isNotNull())\
            & (col("Bill_N2_Debt_Amount").isNotNull())\
            & (col("Bill_N3_InvoiceCharges").isNotNull())\
            & (col("Bill_N3_Amount_To_Pay").isNotNull())\
            & (col("Bill_N3_Tax_Amount").isNotNull())\
            & (col("Bill_N3_Debt_Amount").isNotNull())\
            & (col("Bill_N4_InvoiceCharges").isNotNull())\
            & (col("Bill_N4_Amount_To_Pay").isNotNull())\
            & (col("Bill_N4_Tax_Amount").isNotNull())\
            & (col("Bill_N4_Debt_Amount").isNotNull())\
            & (col("Bill_N5_InvoiceCharges").isNotNull())\
            & (col("Bill_N5_Amount_To_Pay").isNotNull())\
            & (col("Bill_N5_Tax_Amount").isNotNull())\
            & (col("Bill_N5_Debt_Amount").isNotNull()))\
        .select(billing_columns)\
        .distinct()\
        .select(*[sql_avg("Bill_N1_InvoiceCharges").alias("avg_Bill_N1_InvoiceCharges"),\
            sql_avg("Bill_N1_Amount_To_Pay").alias("avg_Bill_N1_Amount_To_Pay"),\
            sql_avg("Bill_N1_Tax_Amount").alias("avg_Bill_N1_Tax_Amount"),\
            sql_avg("Bill_N1_Debt_Amount").alias("avg_Bill_N1_Debt_Amount"),\
            sql_avg("Bill_N2_InvoiceCharges").alias("avg_Bill_N2_InvoiceCharges"),\
            sql_avg("Bill_N2_Amount_To_Pay").alias("avg_Bill_N2_Amount_To_Pay"),\
            sql_avg("Bill_N2_Tax_Amount").alias("avg_Bill_N2_Tax_Amount"),\
            sql_avg("Bill_N2_Debt_Amount").alias("avg_Bill_N2_Debt_Amount"),\
            sql_avg("Bill_N3_InvoiceCharges").alias("avg_Bill_N3_InvoiceCharges"),\
            sql_avg("Bill_N3_Amount_To_Pay").alias("avg_Bill_N3_Amount_To_Pay"),\
            sql_avg("Bill_N3_Tax_Amount").alias("avg_Bill_N3_Tax_Amount"),\
            sql_avg("Bill_N3_Debt_Amount").alias("avg_Bill_N3_Debt_Amount"),\
            sql_avg("Bill_N4_InvoiceCharges").alias("avg_Bill_N4_InvoiceCharges"),\
            sql_avg("Bill_N4_Amount_To_Pay").alias("avg_Bill_N4_Amount_To_Pay"),\
            sql_avg("Bill_N4_Tax_Amount").alias("avg_Bill_N4_Tax_Amount"),\
            sql_avg("Bill_N4_Debt_Amount").alias("avg_Bill_N4_Debt_Amount"),\
            sql_avg("Bill_N5_InvoiceCharges").alias("avg_Bill_N5_InvoiceCharges"),\
            sql_avg("Bill_N5_Amount_To_Pay").alias("avg_Bill_N5_Amount_To_Pay"),\
            sql_avg("Bill_N5_Tax_Amount").alias("avg_Bill_N5_Tax_Amount"),\
            sql_avg("Bill_N5_Debt_Amount").alias("avg_Bill_N5_Debt_Amount")])\
        .rdd\
        .map(lambda r: [r["avg_Bill_N1_InvoiceCharges"], r["avg_Bill_N1_Amount_To_Pay"], r["avg_Bill_N1_Tax_Amount"], r["avg_Bill_N1_Debt_Amount"], r["avg_Bill_N2_InvoiceCharges"], r["avg_Bill_N2_Amount_To_Pay"], r["avg_Bill_N2_Tax_Amount"], r["avg_Bill_N2_Debt_Amount"], r["avg_Bill_N3_InvoiceCharges"], r["avg_Bill_N3_Amount_To_Pay"], r["avg_Bill_N3_Tax_Amount"], r["avg_Bill_N3_Debt_Amount"], r["avg_Bill_N4_InvoiceCharges"], r["avg_Bill_N4_Amount_To_Pay"], r["avg_Bill_N4_Tax_Amount"], r["avg_Bill_N4_Debt_Amount"], r["avg_Bill_N5_InvoiceCharges"], r["avg_Bill_N5_Amount_To_Pay"], r["avg_Bill_N5_Tax_Amount"], r["avg_Bill_N5_Debt_Amount"]])\
        .first()
    
    
    bill_na_fill = {"Bill_N1_InvoiceCharges": avg_bill_n1_invoicecharges,\
    "Bill_N1_Amount_To_Pay": avg_bill_n1_bill_amount,\
    "Bill_N1_Tax_Amount": avg_bill_n1_tax_amount,\
    "Bill_N1_Debt_Amount": avg_bill_n1_debt_amount,\
    "Bill_N2_InvoiceCharges": avg_bill_n2_invoicecharges,\
    "Bill_N2_Amount_To_Pay": avg_bill_n2_bill_amount,\
    "Bill_N2_Tax_Amount": avg_bill_n2_tax_amount,\
    "Bill_N2_Debt_Amount": avg_bill_n2_debt_amount,\
    "Bill_N3_InvoiceCharges": avg_bill_n3_invoicecharges,\
    "Bill_N3_Amount_To_Pay": avg_bill_n3_bill_amount,\
    "Bill_N3_Tax_Amount": avg_bill_n3_tax_amount,\
    "Bill_N3_Debt_Amount": avg_bill_n3_debt_amount,\
    "Bill_N4_InvoiceCharges": avg_bill_n4_invoicecharges,\
    "Bill_N4_Amount_To_Pay": avg_bill_n4_bill_amount,\
    "Bill_N4_Tax_Amount": avg_bill_n4_tax_amount,\
    "Bill_N4_Debt_Amount": avg_bill_n4_debt_amount,\
    "Bill_N5_InvoiceCharges": avg_bill_n5_invoicecharges,\
    "Bill_N5_Amount_To_Pay": avg_bill_n5_bill_amount,\
    "Bill_N5_Tax_Amount": avg_bill_n5_tax_amount,\
    "Bill_N5_Debt_Amount": avg_bill_n5_debt_amount}
    
    billingdf = repartdf\
    .filter(col("ClosingDay") == refdate)\
    .select(billing_columns)\
    .distinct()\
    .na\
    .fill(bill_na_fill)\
    .withColumn("bill_n1_net", col("Bill_N1_Amount_To_Pay") - col("Bill_N1_Tax_Amount")  )\
    .withColumn("bill_n2_net", col("Bill_N2_Amount_To_Pay") - col("Bill_N2_Tax_Amount")  )\
    .withColumn("bill_n3_net", col("Bill_N3_Amount_To_Pay") - col("Bill_N3_Tax_Amount")  )\
    .withColumn("bill_n4_net", col("Bill_N4_Amount_To_Pay") - col("Bill_N4_Tax_Amount")  )\
    .withColumn("bill_n5_net", col("Bill_N5_Amount_To_Pay") - col("Bill_N5_Tax_Amount")  )\
    .withColumn("inc_bill_n1_n2_net", col("bill_n1_net") - col("bill_n2_net")  )\
    .withColumn("inc_bill_n1_n3_net", col("bill_n1_net") - col("bill_n3_net")  )\
    .withColumn("inc_bill_n1_n4_net", col("bill_n1_net") - col("bill_n4_net")  )\
    .withColumn("inc_bill_n1_n5_net", col("bill_n1_net") - col("bill_n5_net")  )\
    .withColumn("inc_Bill_N1_N2_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N2_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N3_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N3_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N4_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N4_Amount_To_Pay")  )\
    .withColumn("inc_Bill_N1_N5_Amount_To_Pay", col("Bill_N1_Amount_To_Pay") - col("Bill_N5_Amount_To_Pay")  )\
    .na\
    .fill(-1.0)
    
    #hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))
    #hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'
    #path_customer = hdfs_write_path_common +'customer/'+hdfs_partition_path
    #customerDF_load = (spark.read.load(path_customer))
    #billingdf_f = billingdf.join(customerDF_load.select('NUM_CLIENTE', 'NIF_CLIENTE'), ['num_cliente'], 'left')

    billingdf_f = billingdf.distinct()

    print '[Info get_billing_car] Billing info generated - Volume: ' + str(billingdf_f.count()) + ' - Num NUM_CLIENTEs: ' + str(billingdf_f.select('num_cliente').distinct().count())

    # Adding NIF

    from pyspark.sql import Window
    window = Window.partitionBy("NIF_CLIENTE")

    bill_cols = [c for c in billingdf_f.columns if 'bill_' in c.lower()]

    billingdf_f = billingdf_f.join(base_df.select('num_cliente', 'nif_cliente').distinct(), ['num_cliente'], 'inner')

    for c in bill_cols:
        billingdf_f = billingdf_f.withColumn(c + '_nif', sql_max(c).over(window))

    sel_cols = [c + '_nif' for c in bill_cols] + ['nif_cliente']

    billingdf_f = billingdf_f\
        .select(sel_cols)\
        .distinct()\
        .repartition(200)

    for c in bill_cols:
        billingdf_f = billingdf_f.withColumnRenamed(c + '_nif', c)

    billingdf_f.cache()

    print '[Info get_billing_car] Billing info generated - Volume: ' + str(billingdf_f.count()) + ' - Num NIFs: ' + str(billingdf_f.select('nif_cliente').distinct().count())

    return billingdf_f

def save_tickets_timing_car(spark, starting_day, closing_day):
    ga_tickets_ = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(
        col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')

    print'Starting day: ' + starting_day
    print'Closing day: ' + closing_day
    print("############ Loading ticket sources ############")

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    ga_tickets_ = ga_tickets_.withColumn('CREATION_TIME',
                                         to_timestamp(ga_tickets_.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
    close = str(closing_day[0:4]) + '-' + str(closing_day[4:6]) + '-' + str(closing_day[6:8]) + ' 00:00:00'
    start = str(starting_day[0:4]) + '-' + str(starting_day[4:6]) + '-' + str(starting_day[6:8]) + ' 00:00:00'
    dates = (start, close)
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

    ga_tickets = ga_tickets_.where((ga_tickets_.CREATION_TIME < date_to) & (ga_tickets_.CREATION_TIME > date_from))
    ga_tickets_detalle = ga_tickets_detalle.drop_duplicates(subset=['OBJID', 'FECHA_1ER_CIERRE'])
    tickets_ = ga_tickets.withColumn('ticket_date',
                                     concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')) \
        .withColumn('DURACION', regexp_replace('DURACION', ',', '.')) \
        .join(ga_tickets_detalle, ['OBJID'], 'left_outer')
    clientes = clientes.withColumnRenamed('OBJID', 'CLIENTES_ID')

    tickets = tickets_.join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.CLIENTES_ID, 'inner') \
        .filter(col('NIF_CLIENTE').isNotNull())
    print'Size of tickets df: '+str(tickets.count())


    tickets = tickets.where((col('X_TIPO_OPERACION')== 'Tramitacion')|(col('X_TIPO_OPERACION')== 'Reclamacion')|(col('X_TIPO_OPERACION')== 'Averia')|(col('X_TIPO_OPERACION')== 'Incidencia'))
    print'Size of filtered tickets df: ' + str(tickets.count())
    print'Creating timing ticket features'
    from pyspark.sql.functions import unix_timestamp
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    timeDiff = (unix_timestamp('FECHA_1ER_CIERRE', format=timeFmt)
                - unix_timestamp('CREATION_TIME', format=timeFmt))
    timeDiff_closing = (unix_timestamp('closing_date', format=timeFmt)
                        - unix_timestamp('CREATION_TIME', format=timeFmt))
    tickets = tickets.withColumn("Ticket_Time", timeDiff)
    tickets = tickets.withColumn('closing_date', lit(date_to)).withColumn("Ticket_Time_closing", timeDiff_closing)

    tickets = tickets.withColumn("Ticket_Time_hours", col("Ticket_Time") / 3600)
    tickets = tickets.withColumn("Ticket_Time_opened_hours", col("Ticket_Time_closing") / 3600)

    codigos = ['Tramitacion', 'Reclamacion', 'Averia', 'Incidencia']
    from pyspark.sql.functions import count as sql_count
    print'Creating ticket count (per type and state) features'
    df_opened = tickets.where(col('FECHA_1ER_CIERRE').isNull())
    df_opened_count = df_opened \
        .groupby('nif_cliente').agg(*([countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias(
        'num_tickets_tipo_' + TIPO.lower() + '_opened') for TIPO in codigos] + [
                                          countDistinct(col("OBJID")).alias('num_tickets_opened')]))

    df_closed = tickets.where(~col('FECHA_1ER_CIERRE').isNull())
    df_closed_count = df_closed \
        .groupby('nif_cliente').agg(*([countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias(
        'num_tickets_tipo_' + TIPO.lower() + '_closed') for TIPO in codigos] + [
                                          countDistinct(col("OBJID")).alias('num_tickets_closed')]))

    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev

    tickets_closed = df_closed
    df_stats_cl = tickets_closed.groupby('nif_cliente').agg(
        *([sql_max(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('max_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_min(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('min_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              stddev(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('std_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('mean_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos])).fillna(0)

    tickets_opened = df_opened
    df_stats_op = tickets_opened.groupby('nif_cliente').agg(
        *([sql_max(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('max_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_min(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('min_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              stddev(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('std_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('mean_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(col("Ticket_Time_opened_hours")) \
          .alias('mean_time_opened')] + [sql_max(col("Ticket_Time_opened_hours")).alias('max_time_opened')] + [
              sql_min(col("Ticket_Time_opened_hours")) \
          .alias('min_time_opened')] + [stddev(col("Ticket_Time_opened_hours")).alias('std_time_opened')])).fillna(0)

    print'Creating week repetition features'
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    from pykhaos.utils.date_functions import move_date_n_days
    vec_dates = []

    aux = closing_day
    for i in range(1, 9):
        date_ = move_date_n_days(closing_day, n=-7 * i)
        close = str(aux[0:4]) + '-' + str(aux[4:6]) + '-' + str(aux[6:8]) + ' 00:00:00'
        start = str(date_[0:4]) + '-' + str(date_[4:6]) + '-' + str(date_[6:8]) + ' 00:00:00'
        dates = (start, close)
        date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
        vec_dates.append([date_from, date_to, i, i + 1])
        aux = date_

    df_reit = tickets \
        .groupby('nif_cliente').agg(*[countDistinct(
        when((col("X_TIPO_OPERACION") == TIPO) & (col("CREATION_TIME") > x[0]) & (col("CREATION_TIME") < x[1]),
             col("OBJID"))).alias('num_tickets_' + TIPO.lower() + '_' + str(x[2] - 1) + '_' + str(x[3] - 1)) for TIPO in
                                      codigos for x in vec_dates])

    aux_cols = df_reit.columns[1:]

    for name in aux_cols:
        df_reit = df_reit.withColumn(name + '_', when(col(name) > 0, 1.0).otherwise(0.0))

    av_cols = [name + '_' for name in aux_cols if 'averia' in name]
    tram_cols = [name + '_' for name in aux_cols if 'tramitacion' in name]
    rec_cols = [name + '_' for name in aux_cols if 'reclamacion' in name]
    inc_cols = [name + '_' for name in aux_cols if 'incidencia' in name]


    from operator import add
    from functools import reduce

    df_reit = df_reit.withColumn('weeks_averias', reduce(add, [col(x) for x in av_cols]))
    df_reit = df_reit.withColumn('weeks_facturacion', reduce(add, [col(x) for x in tram_cols]))
    df_reit = df_reit.withColumn('weeks_reclamacion', reduce(add, [col(x) for x in rec_cols]))
    df_reit = df_reit.withColumn('weeks_incidencias', reduce(add, [col(x) for x in inc_cols]))

    save_cols = [name for name in df_reit.columns if 'weeks' in name] + ['NIF_CLIENTE']

    df_reit = df_reit.select(save_cols)

    codigos_iso = [('X220 No llega señal', 'X220_no_signal'), ('M5 Problema RF', 'M5_problema_RF'),
                   ('M3 Fallo CM', 'M3_Fallo_CM'), ('T6 No hay tono', 'T6_no_hay_tono'),
                   ('Incomunicacion', 'Incomunicacion'),
                   ('P05 Deco defectuoso', 'P05_deco_defectuoso'), ('Portabilidad', 'Portabilidad'),
                   ('T7 Conexiones', 'T7_conexiones')]

    print'Creating isolated customer features'
    from pyspark.sql.functions import count as sql_count
    df_iso = tickets.groupby('nif_cliente').agg(
        *[sql_count(when(col("X_CODIGO_APERTURA") == x[0], col("X_CODIGO_APERTURA"))).alias('count_' + x[1]) for x in
          codigos_iso])

    df1 = df_opened_count.join(df_stats_op, ['NIF_CLIENTE'], 'full_outer')
    df2 = df_closed_count.join(df_stats_cl, ['NIF_CLIENTE'], 'full_outer')

    from churn.analysis.triggers.ml_triggers.metadata_triggers import get_tickets_additional_metadata, get_tickets_additional_metadata_dict
    tickets_map_add = get_tickets_additional_metadata_dict(spark)

    df_final = df1.join(df2, ['NIF_CLIENTE'], 'full_outer').join(df_reit, ['NIF_CLIENTE'], 'full_outer') \
        .join(df_iso, ['NIF_CLIENTE'], 'full_outer').fillna(tickets_map_add)

    partition_date = "year={}/month={}/day={}".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    path_timing = '/data/udf/vf_es/churn/triggers/tickets_attributes/full_df/' + partition_date

    print'Saving stage'
    #df_iso, df_reit, df_stats_op, df_stats_cl, df_closed_count, df_closed_open
    df_final.repartition(300).write.save(path_timing, format='parquet', mode='append')
    print'Saved as:'
    print(path_timing)


def get_module_tickets(spark, starting_day, closing_day):
    ga_tickets_ = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(
        col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')

    print'Starting day: ' + starting_day
    print'Closing day: ' + closing_day
    print("############ Loading ticket sources ############")

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    ga_tickets_ = ga_tickets_.withColumn('CREATION_TIME',
                                         to_timestamp(ga_tickets_.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
    close = str(closing_day[0:4]) + '-' + str(closing_day[4:6]) + '-' + str(closing_day[6:8]) + ' 00:00:00'
    start = str(starting_day[0:4]) + '-' + str(starting_day[4:6]) + '-' + str(starting_day[6:8]) + ' 00:00:00'
    dates = (start, close)
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

    ga_tickets = ga_tickets_.where((ga_tickets_.CREATION_TIME < date_to) & (ga_tickets_.CREATION_TIME > date_from))
    ga_tickets_detalle = ga_tickets_detalle.drop_duplicates(subset=['OBJID', 'FECHA_1ER_CIERRE'])
    tickets_ = ga_tickets.withColumn('ticket_date',
                                     concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')) \
        .withColumn('DURACION', regexp_replace('DURACION', ',', '.')) \
        .join(ga_tickets_detalle, ['OBJID'], 'left_outer')
    clientes = clientes.withColumnRenamed('OBJID', 'CLIENTES_ID')

    tickets = tickets_.join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.CLIENTES_ID, 'inner') \
        .filter(col('NIF_CLIENTE').isNotNull())

    #print'Number of tickets during period: ' + str(tickets.count())
    from pyspark.sql.functions import sum
    from pyspark.sql import Window
    window = Window.partitionBy("NIF_CLIENTE")
    df = tickets
    '''
    df = tickets.where(((col('TIPO_TICKET') != 'Activación/Desactivación') & (col('TIPO_TICKET') != 'Baja') & (
            col('TIPO_TICKET') != 'Portabilidad') & (col('TIPO_TICKET') != 'Problemas con portabilidad')) | (
                           col('TIPO_TICKET').isNull()))
    '''
    codigos = ['Tramitacion', 'Reclamacion', 'Averia', 'Incidencia']

    from pyspark.sql.functions import countDistinct
    df_tickets = df.groupby('nif_cliente').agg(
        *[countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias('num_tickets_tipo_' + TIPO.lower())
          for TIPO in codigos])

    base_cols = [col_ for col_ in df_tickets.columns if col_.startswith('num_tickets_tipo_')]
    save_cols = base_cols
    return df_tickets.select(['NIF_CLIENTE'] + base_cols).drop_duplicates(["nif_cliente"])


def get_tickets_car(spark, closing_day):
    from pykhaos.utils.date_functions import move_date_n_days
    closing_day_2w = move_date_n_days(closing_day, n=-15)
    closing_day_4w = move_date_n_days(closing_day, n=-30)
    closing_day_8w = move_date_n_days(closing_day, n=-60)

    tickets_w2 = get_module_tickets(spark, closing_day_2w, closing_day)
    tickets_w4 = get_module_tickets(spark, closing_day_4w, closing_day)
    tickets_w8 = get_module_tickets(spark, closing_day_8w, closing_day)
    tickets_w4w2 = get_module_tickets(spark, closing_day_4w, closing_day_2w)
    tickets_w8w4 = get_module_tickets(spark, closing_day_8w, closing_day_4w)

    base_cols = [col_ for col_ in tickets_w8.columns if col_.startswith('num_tickets_tipo_')]

    for col_ in base_cols:
        tickets_w8 = tickets_w8.withColumnRenamed(col_, col_ + '_w8')
        tickets_w4 = tickets_w4.withColumnRenamed(col_, col_ + '_w4')
        tickets_w2 = tickets_w2.withColumnRenamed(col_, col_ + '_w2')
        tickets_w4w2 = tickets_w4w2.withColumnRenamed(col_, col_ + '_w4w2')
        tickets_w8w4 = tickets_w8w4.withColumnRenamed(col_, col_ + '_w8w4')

    df_tickets = tickets_w8.join(tickets_w4, ['NIF_CLIENTE'], 'left').join(tickets_w2, ['NIF_CLIENTE'], 'left') \
        .join(tickets_w4w2, ['NIF_CLIENTE'], 'left').join(tickets_w8w4, ['NIF_CLIENTE'], 'left').fillna(0)

    for col_ in base_cols:
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w4w4', col(col_ + '_w4') - col(col_ + '_w8w4'))

    return df_tickets.fillna(0)



def get_filtered_car(spark, closing_day, df_base_msisdn):
    from churn.analysis.triggers.orders.run_segment_orders import get_ccc_attrs_w8

    df_ccc = get_ccc_attrs_w8(spark, closing_day, df_base_msisdn)
    num_ccc = df_ccc.count()
    print'Num CCC:' + str(num_ccc)
    df_tar = df_ccc.filter(col('CHURN_CANCELLATIONS_w8') == 0)
    num_tar = df_tar.count()
    print'Num TAR:' + str(num_tar)
    print'Total number of customers with recent churn cancellations: {}'.format(num_ccc - num_tar)

    return df_tar
    
    
def set_paths_and_logger():

    print(os.path.abspath(imp.find_module('churn')[1]))
    USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))
    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))
    logger = None

    return logger

def get_next_dow(weekday, from_date=None):
    '''
    weekday: weekday is 1 for monday; 2 for tuesday; ...; 7 for sunday.
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=5) [get next friday] and the function returns datetime.date(2019, 6, 14) [14-June-2019]
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=2) [get next tuesday] and the function returns datetime.date(2019, 6, 11) [11-June-2019, Today]

    Note: weekday=0 is the same as weekday=7
    Note: this function runs with isoweekday (monday is 1 not 0)

    from_date: if from_date != None, instead of using today uses this day.

    '''

    from_date = from_date if from_date else dt.date.today()
    import datetime as dt2
    return from_date + dt2.timedelta( (weekday-from_date.isoweekday()) % 7 )



def get_partitions_path_range(path, start, end):
    """
    Returns a list of complete paths with data to read of the source between two dates
    :param path: string path
    :param start: string date start
    :param end: string date end
    :return: list of paths
    """
    from datetime import timedelta, datetime
    star_date = datetime.strptime(start, '%Y%m%d')
    delta = datetime.strptime(end, '%Y%m%d') - datetime.strptime(start, '%Y%m%d')
    days_list = [star_date + timedelta(days=i) for i in range(delta.days + 1)]
    return [path + "year={}/month={}/day={}".format(d.year, d.month, d.day) for d in days_list]

def check_hdfs_exists(path_to_file):
    try:
        cmd = 'hdfs dfs -ls {}'.format(path_to_file).split()
        files = subprocess.check_output(cmd).strip().split('\n')
        return True if files and len(files[0])>0 else False
    except:
        return False

def get_zhilabs_adsl_feats(spark, starting_day, closing_day):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_adsl = spark.read.option("basePath", zhilabs_path).load(paths)

    adsl_cols = ['cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries', 'lan_host_ethernet_num__entries',
                 'lan_ethernet_stats_mbytes_received', 'lan_ethernet_stats_mbytes_sent',
                 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received', 'wlan_2_4_stats_errors_sent',
                 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received', 'wlan_5_stats_errors_sent',
                 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent', 'cpe_cpu_usage', 'cpe_memory_free',
                 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    df = df_adsl.select(['serviceid'] + adsl_cols)

    df_zhilabs_adsl = df.withColumn("wlan_2_4_errors_received_rate",
                                    col('wlan_2_4_stats_errors_received') / col('wlan_2_4_stats_packets_received')) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') / col('wlan_2_4_stats_packets_sent')) \
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') / col('wlan_5_stats_packets_received')) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_packets_sent')) \
        .withColumn("adsl_max_vel__down_alcanzable", col('adsl_max_vel__down_alcanzable').cast(IntegerType()))

    # feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate', 'wlan_5_errors_sent_rate', 'issue_hfc_docsis_3_1_equivalent_modulation_critical_current', 'issue_hfc_fec_upstream_critical_current', 'issue_hfc_fec_upstream_warning_current', 'issue_hfc_flaps_critical_current', 'issue_hfc_flaps_warning_current', 'issue_hfc_prx_downstream_critical_current', 'issue_hfc_prx_downstream_warning_current', 'issue_hfc_ptx_upstream_critical_current', 'issue_hfc_ptx_upstream_warning_current', 'issue_hfc_snr_downstream_critical_current', 'issue_hfc_snr_downstream_warning_current', 'issue_hfc_snr_upstream_critical_current', 'issue_hfc_snr_upstream_warning_current', 'issue_hfc_status_critical_current', 'issue_hfc_status_warning_current', 'hfc_percent_words_uncorrected_downstream', 'hfc_percent_words_uncorrected_upstream', 'hfc_prx_dowstream_average', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average', 'hfc_number_of_flaps_current','cpe_quick_restarts','cpe_restarts','lan_host_802_11_num__entries','lan_host_ethernet_num__entries','lan_ethernet_stats_mbytes_received','lan_ethernet_stats_mbytes_sent','wlan_2_4_stats_errors_received','wlan_2_4_stats_packets_received','wlan_2_4_stats_errors_sent','wlan_2_4_stats_packets_sent','wlan_5_stats_errors_received','wlan_5_stats_errors_sent','wlan_5_stats_packets_received','wlan_5_stats_packets_sent','cpe_cpu_usage','cpe_memory_free','cpe_memory_total','wlan_2_4_configuration_autochannel_enable','wlan_5_configuration_autochannel_enable']
    feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable', 'adsl_cortes_adsl_1_hora', 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev
    df_zhilabs_adsl_feats = df_zhilabs_adsl.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feat_cols] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feat_cols] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                     feat_cols] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feat_cols]))

    return df_zhilabs_adsl_feats


def get_zhilabs_hfc_feats(spark, starting_day, closing_day):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_hfc = spark.read.option("basePath", zhilabs_path).load(paths)

    df_zhilabs_thot = df_hfc.withColumn("wlan_2_4_errors_received_rate",
                                        col('wlan_2_4_stats_errors_received') / col('wlan_2_4_stats_packets_received')) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') / col('wlan_2_4_stats_packets_sent')) \
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') / col('wlan_5_stats_packets_received')) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_packets_sent'))

    feat_cols = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'issue_hfc_docsis_3_1_equivalent_modulation_critical_current',
                 'issue_hfc_fec_upstream_critical_current', 'issue_hfc_fec_upstream_warning_current',
                 'issue_hfc_flaps_critical_current', 'issue_hfc_flaps_warning_current',
                 'issue_hfc_prx_downstream_critical_current', 'issue_hfc_prx_downstream_warning_current',
                 'issue_hfc_ptx_upstream_critical_current', 'issue_hfc_ptx_upstream_warning_current',
                 'issue_hfc_snr_downstream_critical_current', 'issue_hfc_snr_downstream_warning_current',
                 'issue_hfc_snr_upstream_critical_current', 'issue_hfc_snr_upstream_warning_current',
                 'issue_hfc_status_critical_current', 'issue_hfc_status_warning_current',
                 'hfc_percent_words_uncorrected_downstream', 'hfc_percent_words_uncorrected_upstream',
                 'hfc_prx_dowstream_average', 'hfc_snr_downstream_average', 'hfc_snr_upstream_average',
                 'hfc_number_of_flaps_current', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                 'wlan_5_configuration_autochannel_enable']
    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev
    df_zhilabs_thot_feats = df_zhilabs_thot.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feat_cols] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feat_cols] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                     feat_cols] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feat_cols]))

    return df_zhilabs_thot_feats


def get_zhilabs_ftth_feats(spark, starting_day, closing_day):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    zhilabs_path = '/data/raw/vf_es/fixnetprobes/ZHILABS_HOURLY_ACC/1.0/parquet/'
    from churn.analysis.triggers.ml_triggers.utils_trigger import get_partitions_path_range
    paths_ = get_partitions_path_range(zhilabs_path, starting_day, closing_day)
    paths = []
    for p in paths_:
        if check_hdfs_exists(p) == True:
            paths.append(p)

    df_ftth = spark.read.option("basePath", zhilabs_path).load(paths)
    feats_ftth = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                  'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'wlan_2_4_configuration_autochannel_enable',
                  'wlan_5_configuration_autochannel_enable']
    id_ = ['serviceid']

    df_ftth = df_ftth.withColumn("wlan_2_4_errors_received_rate",
                                 col('wlan_2_4_stats_errors_received') / col('wlan_2_4_stats_packets_received')) \
        .withColumn("wlan_2_4_errors_sent_rate", col('wlan_2_4_stats_errors_sent') / col('wlan_2_4_stats_packets_sent')) \
        .withColumn("wlan_5_errors_received_rate",
                    col('wlan_5_stats_errors_received') / col('wlan_5_stats_packets_received')) \
        .withColumn("wlan_5_errors_sent_rate", col('wlan_5_stats_errors_sent') / col('wlan_5_stats_packets_sent'))
    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev, abs
    df_ftth_feats = df_ftth.groupby('serviceid').agg(*([sql_max(kpi).alias("max_" + kpi) for kpi in feats_ftth] + \
                                                       [sql_min(kpi).alias("min_" + kpi) for kpi in feats_ftth] + [
                                                           sql_avg(kpi).alias("mean_" + kpi) for kpi in feats_ftth] \
                                                       + [stddev(kpi).alias("std_" + kpi) for kpi in feats_ftth]))

    # DAILY
    zhilabs_path_daily = '/data/raw/vf_es/fixnetprobes/ZHILABS_DAILY/1.0/parquet/'
    paths_daily_ = get_partitions_path_range(zhilabs_path_daily, starting_day, closing_day)
    paths_daily = []
    for p in paths_daily_:
        if check_hdfs_exists(p) == True:
            paths_daily.append(p)

    df_ftth_daily = spark.read.option("basePath", zhilabs_path_daily).load(paths_daily)

    df_ftth_daily = df_ftth_daily.withColumn("mod_ftth_ber_down_average", abs(col('ftth_ber_down_average'))) \
        .withColumn("mod_ftth_ber_up_average", abs(col('ftth_ber_up_average')))

    feats_daily = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_ber_critical', 'issue_ftth_ber_warning',
                   'issue_ftth_degradation', 'issue_ftth_prx']

    df_ftth_feats_daily = df_ftth_daily.groupby('serviceid').agg(
        *([sql_max(kpi).alias("max_" + kpi) for kpi in feats_daily] + \
          [sql_min(kpi).alias("min_" + kpi) for kpi in feats_daily] + [sql_avg(kpi).alias("mean_" + kpi) for kpi in
                                                                       feats_daily] \
          + [stddev(kpi).alias("std_" + kpi) for kpi in feats_daily]))

    df_ftth_feats_all = df_ftth_feats.join(df_ftth_feats_daily, ['serviceid'], 'left_outer')

    return df_ftth_feats_all
