#!/usr/bin/env python
# -*- coding: utf-8 -*-

FEATS_TNPS = ['tnps_' + f + '_VDN' for f in ['min', 'max', 'std', 'mean']]
CATEGORICAL_COLS = ['tnps_TNPS2HDET', 'tnps_TNPS2DET', 'tnps_TNPS2SDET']

NON_INFO_COLS = ["msisdn", "nif_cliente"] #FIXME autodetect with metadata project
OWNER_LOGIN = "asaezco"

MODEL_OUTPUT_NAME = "trigger_tnps"

FEAT_COLS = ['BILLING_POSTPAID_w8', 'total_acan', 'total_orange', 'Bill_N1_Tax_Amount', 'Bill_N1_Tax_Amount', 'orange_APOR'\
'inc_num_calls_w2vsw2', 'inc_num_calls_w4vsw4', 'Bill_N2_Amount_To_Pay', 'Bill_N2_InvoiceCharges', 'inc_PRODUCT_AND_SERVICE_MANAGEMENT_w4vsw4',\
'Bill_N1_Debt_Amount', 'nif_var_days_since_port', 'nif_distinct_msisdn', 'COLLECTIONS_w8', 'nif_port_freq_per_msisdn',\
'orange_ACAN', 'PRODUCT_AND_SERVICE_MANAGEMENT_w8'] + ['inc_DEVICE_DELIVERY_REPAIR_w4vsw4', 'nif_min_days_since_port', 'Bill_N1_InvoiceCharges', 'Bill_N2_num_facturas',\
'Bill_N5_Debt_Amount', 'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w8', 'TARIFF_MANAGEMENT_w4', 'NEW_ADDS_PROCESS_w2', 'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w8', \
 'inc_OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w2vsw2', 'masmovil_ASOL', 'NEW_ADDS_PROCESS_w4', 'Bill_N4_num_facturas','nb_tv_services_nif', 'reuskal_AREC', \
'BILLING_POSTPAID_w2', 'orange_ACON', 'billing_mean', 'billing_current_vf_debt', 'least_diff_bw_bills', 'Bill_N4_Tax_Amount', 'billing_current_client_debt', \
'movistar_APOR', 'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w4', 'inc_INTERNET_EN_EL_MOVIL_w4vsw4', 'inc_BILLING_POSTPAID_w2vsw2', 'Bill_N1_num_ids_fict', 'nif_port_number',\
'jazztel_ACON', 'nif_avg_days_since_port', 'inc_BUCKET_w2vsw2', 'BUCKET_w8', 'pepephone_AENV', 'Bill_N5_num_ids_fict', 'INTERNET_EN_EL_MOVIL_w8', 'tgs_has_discount',\
'Bill_N4_Debt_Amount', 'pepephone_ASOL', 'blindaje', 'tgs_days_until_f_fin_bi', 'tgs_has_discount', 'segment_nif']

HIGHLY_CORRELATED = ['tnps_mean_VDN_w4w8',\
'tnps_max_VDN_w4w8',\
'tnps_max_VDN_w2',\
'tnps_mean_VDN_w2w4',\
'tnps_max_VDN_w2w4',\
'inc_tnps_max_VDN_w2w2',\
'nb_fixed_services_nif',\
'nb_rgus_nif',\
'Bill_N1_Amount_To_Pay',\
'Bill_N1_net_charges',\
'Bill_N1_num_facturas',\
'Bill_N2_Amount_To_Pay',\
'Bill_N2_net_charges',\
'Bill_N2_num_ids_fict',\
'Bill_N2_num_facturas',\
'Bill_N2_days_since',\
'Bill_N3_InvoiceCharges',\
'Bill_N3_Amount_To_Pay',\
'Bill_N3_Tax_Amount',\
'Bill_N3_net_charges',\
'Bill_N3_num_ids_fict',\
'Bill_N3_num_facturas',\
'Bill_N3_days_since',\
'Bill_N4_Amount_To_Pay',\
'Bill_N4_net_charges',\
'Bill_N4_num_ids_fict',\
'Bill_N4_num_facturas',\
'Bill_N4_days_since',\
'Bill_N5_Amount_To_Pay',\
'Bill_N5_net_charges',\
'Bill_N5_num_ids_fict',\
'Bill_N5_num_facturas',\
'Bill_N5_days_since',\
'greatest_diff_bw_bills',\
'least_diff_bw_bills',\
'billing_std',\
'billing_mean',\
'billing_current_debt',\
'DEVICE_DELIVERY_REPAIR_w4',\
'PREPAID_BALANCE_w4',\
'PRODUCT_AND_SERVICE_MANAGEMENT_w8',\
'COLLECTIONS_w8',\
'BILLING_POSTPAID_w8',\
'inc_PREPAID_BALANCE_w2vsw2',\
'nif_avg_days_since_port',\
'total_movistar',\
'total_simyo',\
'total_jazztel',\
'total_yoigo',\
'total_unknown',\
'total_otros',\
'num_distinct_operators',\
'tnps_min_VDN',\
'tnps_mean_VDN',\
'tnps_max_VDN',\
'tnps_min_VDN_w2',\
'inc_tnps_min_VDN_w2w2',\
'inc_tnps_min_VDN_w4w4',\
'inc_tnps_mean_VDN_w4w4',\
'inc_tnps_mean_VDN_w2w2',\
'inc_tnps_max_VDN_w4w4',\
'Bill_N1_InvoiceCharges',\
'Bill_N2_InvoiceCharges',\
'Bill_N1_Debt_Amount',\
'Bill_N4_InvoiceCharges',\
'Bill_N3_Debt_Amount',\
'Bill_N5_InvoiceCharges',\
'Bill_N4_Debt_Amount',\
'billing_avg_days_bw_bills',\
'billing_nb_last_bills',\
'QUICK_CLOSING_w2',\
'PRODUCT_AND_SERVICE_MANAGEMENT_w2',\
'DSL_FIBER_INCIDENCES_AND_SUPPORT_w2',\
'NEW_ADDS_PROCESS_w2',\
'DEVICE_UPGRADE_w2',\
'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w2',\
'BILLING_POSTPAID_w2',\
'num_calls_w2',\
'QUICK_CLOSING_w4',\
'TARIFF_MANAGEMENT_w4',\
'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT_w4',\
'DSL_FIBER_INCIDENCES_AND_SUPPORT_w4',\
'NEW_ADDS_PROCESS_w4',\
'DEVICE_UPGRADE_w4',\
'OTHER_CUSTOMER_INFORMATION_MANAGEMENT_w4',\
'num_calls_w4',\
'DEVICE_DELIVERY_REPAIR_w2',\
'PRODUCT_AND_SERVICE_MANAGEMENT_w4',\
'CHURN_CANCELLATIONS_w8',\
'nif_port_number',\
'nif_distinct_msisdn',\
'nb_prepaid_services_nif',\
'INTERNET_EN_EL_MOVIL_w2',\
'MI_VODAFONE_w2',\
'CHURN_CANCELLATIONS_w2',\
'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w2',\
'BUCKET_w2',\
'INTERNET_EN_EL_MOVIL_w4',\
'MI_VODAFONE_w4',\
'CHURN_CANCELLATIONS_w4',\
'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w4',\
'BUCKET_w4',\
'INTERNET_EN_EL_MOVIL_w8',\
'MI_VODAFONE_w8',\
'OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w8',\
'BUCKET_w8',\
'inc_INTERNET_EN_EL_MOVIL_w2vsw2',\
'inc_MI_VODAFONE_w2vsw2',\
'inc_CHURN_CANCELLATIONS_w2vsw2',\
'inc_OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w2vsw2',\
'inc_BUCKET_w2vsw2',\
'inc_INTERNET_EN_EL_MOVIL_w4vsw4',\
'inc_MI_VODAFONE_w4vsw4',\
'inc_OTHER_CUSTOMER_INFOSATIS_START_AVERIA_w4vsw4',\
'inc_BUCKET_w4vsw4',\
'movistar_PCAN',\
'movistar_AACE',\
'movistar_AENV',\
'simyo_PCAN',\
'simyo_AACE',\
'simyo_AENV',\
'orange_AACE',\
'jazztel_PCAN',\
'jazztel_AACE',\
'jazztel_AENV',\
'yoigo_PCAN',\
'yoigo_AACE',\
'masmovil_PCAN',\
'masmovil_AACE',\
'masmovil_AENV',\
'pepephone_PCAN',\
'pepephone_AACE',\
'pepephone_AENV',\
'reuskal_ACON',\
'reuskal_ASOL',\
'reuskal_PCAN',\
'reuskal_ACAN',\
'reuskal_AACE',\
'reuskal_AENV',\
'reuskal_APOR',\
'reuskal_AREC',\
'unknown_PCAN',\
'unknown_AACE',\
'otros_AACE',\
'otros_AENV',\
'total_reuskal']


