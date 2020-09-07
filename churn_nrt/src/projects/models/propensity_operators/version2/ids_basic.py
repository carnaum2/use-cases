
from pyspark.sql.functions import (col, when)



def creacion_ids_basic(year_, month_, day_,spark):

    ids_completo = (spark.read.load(
        '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year_ + '/month=' + month_ + '/day=' + day_))

    # Calculo variable CCC_total (suma de las 16 CCC_bucket)

    ids_completo_2 = ids_completo.withColumn('CCC_total',
                                             col('CCC_Bucket_Other_customer_infoSatis__Start_Averia') +
                                             col('CCC_Bucket_Churn_Cancellations') + col(
                                                 'CCC_Bucket_Internet_en_el_movil') +
                                             col('CCC_Bucket_Collections') + col('CCC_Bucket_Billing___Postpaid') + col(
                                                 'CCC_Bucket_Device_upgrade') +
                                             col('CCC_Bucket_Prepaid_balance') + col('CCC_Bucket_Quick_Closing') + col(
                                                 'CCC_Bucket_New_adds_process') +
                                             col('CCC_Bucket_Other_customer_information_management') + col(
                                                 'CCC_Bucket_Mi_vodafone') +
                                             col('CCC_Bucket_Product_and_Service_management') +
                                             col('CCC_Bucket_DSL_FIBER_incidences_and_support') + col(
                                                 'CCC_Bucket_Tariff_management') +
                                             col('CCC_Bucket_Voice_and_mobile_data_incidences_and_support') + col(
                                                 'CCC_Bucket_Device_delivery_repair'))

    # Calculo variable de minutos de llamadas totales (week+weekend): tengo en cuenta los nulos (-1)

    ids_completo_2 = ids_completo_2.withColumn('llamadas_total', when(
        (col('GNV_Voice_L2_total_mou_we') == -1) & (col('GNV_Voice_L2_total_mou_w') == -1), -1).otherwise(
        when((col('GNV_Voice_L2_total_mou_w') == -1) & (col('GNV_Voice_L2_total_mou_we') != -1),
             col('GNV_Voice_L2_total_mou_we')).otherwise(
            when((col('GNV_Voice_L2_total_mou_we') == -1) & (col('GNV_Voice_L2_total_mou_w') != -1),
                 col('GNV_Voice_L2_total_mou_w')).otherwise(
                col('GNV_Voice_L2_total_mou_we') + col('GNV_Voice_L2_total_mou_w')))))

    # Creo IDS basic seleccionando variables que nos interesan

    ids_basic = ids_completo_2.select('msisdn',
                                      'NUM_CLIENTE',
                                      'NIF_CLIENTE',
                                      'Serv_RGU',
                                      'Cust_COD_ESTADO_GENERAL',
                                      'Cust_Agg_L2_fbb_fx_first_days_since_nc',
                                      'Cust_Agg_L2_fixed_fx_first_days_since_nc',
                                      'Cust_Agg_L2_mobile_fx_first_days_since_nc',
                                      'Cust_Agg_L2_tv_fx_first_days_since_nc',
                                      'Bill_N1_Amount_To_Pay',
                                      'GNV_Data_L2_total_data_volume',
                                      'GNV_Data_L2_total_connections',
                                      'llamadas_total',
                                      'Cust_Agg_fbb_services_nc',
                                      'Cust_Agg_fixed_services_nc',
                                      'Cust_Agg_mobile_services_nc',
                                      'Cust_Agg_tv_services_nc',
                                      'Cust_Agg_L2_total_num_services_nc',
                                      'tgs_days_until_f_fin_bi',
                                      'Serv_L2_mobile_tariff_proc',
                                      'Serv_L2_desc_tariff_proc',
                                      'Cust_SUPEROFERTA',
                                      'Serv_PRICE_TARIFF',
                                      'Serv_voice_tariff',
                                      'Serv_L2_real_price',
                                      'Penal_L2_CUST_PENDING_end_date_total_max_days_until',
                                      'Comp_sum_count_comps',
                                      'Comp_num_distinct_comps',
                                      'CCC_total',
                                      'Spinners_total_acan',
                                      'Spinners_num_distinct_operators',
                                      'Spinners_nif_port_freq_per_msisdn')

    return ids_basic