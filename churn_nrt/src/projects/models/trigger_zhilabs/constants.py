#!/usr/bin/env python
# -*- coding: utf-8 -*-

FEATS_HFC = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
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
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total'] + ['num_meas_online','num_meas_assign_epk_','num_meas_w-online_pk_','num_meas_w-online_pkd_','num_meas_offline','num_meas_dhcp-c','num_meas_init_r2_',
 'num_meas_init_dr_','num_meas_expire_pkd_','num_meas_reject_pt_','num_meas_init6_o_','num_meas_init_d_','num_meas_init_o_','num_meas_online_pt_','num_meas_p-online_pt_','num_meas_','num_meas_w-online_pt_','num_meas_w-online_ptd_']

FEATS_FTTH = ['mod_ftth_ber_down_average', 'mod_ftth_ber_up_average', 'ftth_olt_prx_average',
                   'ftth_ont_prx_average', 'issue_ftth_degradation', 'issue_ftth_prx'] + ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                  'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                  'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                  'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                  'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                  'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                  'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total']

FEATS_DSL = ['wlan_2_4_errors_received_rate', 'wlan_2_4_errors_sent_rate', 'wlan_5_errors_received_rate',
                 'wlan_5_errors_sent_rate', 'cpe_quick_restarts', 'cpe_restarts', 'lan_host_802_11_num__entries',
                 'lan_host_ethernet_num__entries', 'lan_ethernet_stats_mbytes_received',
                 'lan_ethernet_stats_mbytes_sent', 'wlan_2_4_stats_errors_received', 'wlan_2_4_stats_packets_received',
                 'wlan_2_4_stats_errors_sent', 'wlan_2_4_stats_packets_sent', 'wlan_5_stats_errors_received',
                 'wlan_5_stats_errors_sent', 'wlan_5_stats_packets_received', 'wlan_5_stats_packets_sent',
                 'cpe_cpu_usage', 'cpe_memory_free', 'cpe_memory_total', 'adsl_cortes_adsl_1_hora',
                 'adsl_max_vel__down_alcanzable',
                 'adsl_max_vel__up_alcanzable', 'adsl_mejor_vel__down', 'adsl_mejor_vel__up', 'adsl_vel__down_actual',
                 'adsl_vel__up_actual']

FEAT_COLS_HFC = [n + feat for feat in FEATS_HFC for n in ['max_', 'min_', 'mean_', 'std_']]
FEAT_COLS_FTTTH = [n + feat for feat in FEATS_FTTH for n in ['max_', 'min_', 'mean_', 'std_']]
FEAT_COLS_DSL = [n + feat for feat in FEATS_DSL for n in ['max_', 'min_', 'mean_', 'std_']]

HIGH_CORRELATED_VARS_FTTH = ["zhilabs_ftth_min_cpe_memory_total",\
"zhilabs_ftth_mean_wlan_5_errors_sent_rate",\
"zhilabs_ftth_mean_wlan_2_4_stats_errors_sent",\
"zhilabs_ftth_mean_wlan_5_stats_errors_received",\
"zhilabs_ftth_mean_cpe_memory_total",\
"zhilabs_ftth_std_wlan_2_4_errors_received_rate",\
"zhilabs_ftth_std_cpe_quick_restarts",\
"zhilabs_ftth_std_cpe_restarts",\
"zhilabs_ftth_std_wlan_2_4_stats_errors_received",\
"zhilabs_ftth_std_wlan_2_4_stats_packets_received",\
"zhilabs_ftth_std_wlan_2_4_stats_errors_sent",\
"zhilabs_ftth_std_wlan_2_4_stats_packets_sent",\
"zhilabs_ftth_std_wlan_5_stats_errors_received",\
"zhilabs_ftth_std_wlan_5_stats_errors_sent",\
"zhilabs_ftth_std_wlan_5_stats_packets_received",\
"zhilabs_ftth_std_wlan_5_stats_packets_sent",\
"zhilabs_ftth_max_ftth_ont_prx_average",\
"zhilabs_ftth_min_ftth_olt_prx_average",\
"zhilabs_ftth_min_ftth_ont_prx_average",\
"zhilabs_ftth_mean_mod_ftth_ber_down_average",\
"zhilabs_ftth_mean_ftth_ont_prx_average",\
"zhilabs_ftth_mean_issue_ftth_degradation",\
"zhilabs_ftth_mean_issue_ftth_prx",\
"zhilabs_ftth_max_wlan_2_4_stats_packets_received",\
"zhilabs_ftth_max_cpe_memory_free",\
"zhilabs_ftth_max_cpe_memory_total",\
"zhilabs_ftth_max_lan_host_802_11_num__entries",\
"zhilabs_ftth_max_lan_host_ethernet_num__entries",\
"zhilabs_ftth_mean_wlan_2_4_stats_packets_received",\
"zhilabs_ftth_max_wlan_5_stats_errors_sent",\
"zhilabs_ftth_mean_wlan_5_stats_packets_received",\
"zhilabs_ftth_min_cpe_memory_free",\
"zhilabs_ftth_max_issue_ftth_prx",\
"zhilabs_ftth_max_ftth_olt_prx_average",\
"zhilabs_ftth_max_issue_ftth_degradation",\
"zhilabs_ftth_max_mod_ftth_ber_down_average",\
"zhilabs_ftth_max_mod_ftth_ber_up_average",\
"zhilabs_ftth_std_ftth_olt_prx_average",\
"zhilabs_ftth_max_wlan_2_4_stats_errors_received",\
"zhilabs_ftth_max_wlan_5_stats_errors_received",\
"zhilabs_ftth_max_wlan_5_stats_packets_received",\
"zhilabs_ftth_mean_wlan_2_4_stats_packets_sent",\
"zhilabs_ftth_mean_cpe_cpu_usage",\
"zhilabs_ftth_min_cpe_quick_restarts",\
"zhilabs_ftth_min_cpe_restarts"]

HIGH_CORRELATED_VARS_INC = ["inc_zhilabs_ftth_max_cpe_restarts_w1w1",\
"inc_zhilabs_ftth_max_lan_host_ethernet_num__entries_w1w1",\
"inc_zhilabs_ftth_min_wlan_2_4_stats_errors_sent_w1w1",\
"inc_zhilabs_ftth_min_wlan_5_stats_errors_received_w1w1",\
"inc_zhilabs_ftth_min_wlan_5_stats_packets_received_w1w1",\
"inc_zhilabs_ftth_min_cpe_memory_free_w1w1",\
"inc_zhilabs_ftth_min_cpe_memory_free_w2w2",\
"inc_zhilabs_ftth_min_cpe_memory_total_w1w1",\
"inc_zhilabs_ftth_min_cpe_memory_total_w2w2",\
"inc_zhilabs_ftth_mean_wlan_5_errors_sent_rate_w1w1",\
"inc_zhilabs_ftth_mean_wlan_5_errors_sent_rate_w2w2",\
"inc_zhilabs_ftth_mean_lan_host_802_11_num__entries_w1w1",\
"inc_zhilabs_ftth_mean_lan_host_ethernet_num__entries_w1w1",\
"inc_zhilabs_ftth_mean_wlan_2_4_stats_errors_sent_w1w1",\
"inc_zhilabs_ftth_mean_wlan_2_4_stats_errors_sent_w2w2",\
"inc_zhilabs_ftth_mean_wlan_2_4_stats_packets_sent_w2w2",\
"inc_zhilabs_ftth_mean_wlan_5_stats_errors_received_w1w1",\
"inc_zhilabs_ftth_mean_wlan_5_stats_packets_sent_w2w2",\
"inc_zhilabs_ftth_mean_cpe_memory_total_w1w1",\
"inc_zhilabs_ftth_std_wlan_2_4_errors_received_rate_w1w1",\
"inc_zhilabs_ftth_std_wlan_2_4_errors_received_rate_w2w2",\
"inc_zhilabs_ftth_std_cpe_quick_restarts_w1w1",\
"inc_zhilabs_ftth_std_cpe_quick_restarts_w2w2",\
"inc_zhilabs_ftth_std_cpe_restarts_w1w1",\
"inc_zhilabs_ftth_std_cpe_restarts_w2w2",\
"inc_zhilabs_ftth_std_lan_host_802_11_num__entries_w1w1",\
"inc_zhilabs_ftth_std_lan_host_802_11_num__entries_w2w2",\
"inc_zhilabs_ftth_std_lan_host_ethernet_num__entries_w1w1",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_errors_received_w1w1",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_errors_received_w2w2",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_errors_sent_w1w1",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_errors_sent_w2w2",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_packets_sent_w1w1",\
"inc_zhilabs_ftth_std_wlan_5_stats_errors_received_w1w1",\
"inc_zhilabs_ftth_std_wlan_5_stats_errors_sent_w1w1",\
"inc_zhilabs_ftth_std_wlan_5_stats_errors_sent_w2w2",\
"inc_zhilabs_ftth_max_ftth_ont_prx_average_w1w1",\
"inc_zhilabs_ftth_max_ftth_ont_prx_average_w2w2",\
"inc_zhilabs_ftth_min_ftth_olt_prx_average_w2w2",\
"inc_zhilabs_ftth_min_ftth_ont_prx_average_w1w1",\
"inc_zhilabs_ftth_min_ftth_ont_prx_average_w2w2",\
"inc_zhilabs_ftth_mean_mod_ftth_ber_down_average_w1w1",\
"inc_zhilabs_ftth_std_mod_ftth_ber_down_average_w1w1",\
"inc_zhilabs_ftth_std_mod_ftth_ber_down_average_w2w2",\
"inc_zhilabs_ftth_std_mod_ftth_ber_up_average_w1w1",\
"inc_zhilabs_ftth_std_mod_ftth_ber_up_average_w2w2",\
"inc_zhilabs_ftth_std_ftth_ont_prx_average_w1w1",\
"inc_zhilabs_ftth_std_ftth_ont_prx_average_w2w2",\
"inc_zhilabs_ftth_std_issue_ftth_degradation_w1w1",\
"inc_zhilabs_ftth_max_wlan_2_4_stats_errors_received_w1w1",\
"inc_zhilabs_ftth_max_wlan_2_4_stats_packets_received_w1w1",\
"inc_zhilabs_ftth_max_wlan_2_4_stats_packets_received_w2w2",\
"inc_zhilabs_ftth_max_cpe_memory_free_w1w1",\
"inc_zhilabs_ftth_max_cpe_memory_free_w2w2",\
"inc_zhilabs_ftth_max_lan_host_802_11_num__entries_w1w1",\
"inc_zhilabs_ftth_max_lan_host_802_11_num__entries_w2w2",\
"inc_zhilabs_ftth_mean_wlan_2_4_stats_packets_received_w1w1",\
"inc_zhilabs_ftth_max_wlan_5_stats_errors_sent_w1w1",\
"inc_zhilabs_ftth_max_wlan_5_stats_errors_sent_w2w2",\
"inc_zhilabs_ftth_max_cpe_memory_total_w1w1",\
"inc_zhilabs_ftth_mean_cpe_memory_free_w2w2",\
"inc_zhilabs_ftth_max_wlan_2_4_errors_sent_rate_w1w1",\
"inc_zhilabs_ftth_max_wlan_5_errors_received_rate_w1w1",\
"inc_zhilabs_ftth_max_wlan_5_errors_sent_rate_w1w1",\
"inc_zhilabs_ftth_max_wlan_2_4_stats_packets_sent_w2w2",\
"inc_zhilabs_ftth_std_wlan_2_4_stats_packets_received_w2w2",\
"inc_zhilabs_ftth_max_wlan_5_stats_errors_received_w2w2",\
"inc_zhilabs_ftth_mean_wlan_5_stats_errors_received_w2w2",\
"inc_zhilabs_ftth_max_wlan_5_stats_packets_received_w2w2",\
"inc_zhilabs_ftth_std_wlan_5_stats_packets_received_w1w1",\
"inc_zhilabs_ftth_max_wlan_5_stats_packets_sent_w2w2",\
"inc_zhilabs_ftth_max_ftth_olt_prx_average_w1w1",\
"inc_zhilabs_ftth_min_mod_ftth_ber_up_average_w1w1",\
"inc_zhilabs_ftth_min_ftth_olt_prx_average_w1w1",\
"inc_zhilabs_ftth_max_ftth_olt_prx_average_w2w2",\
"inc_zhilabs_ftth_mean_ftth_olt_prx_average_w1w1",\
"inc_zhilabs_ftth_mean_ftth_olt_prx_average_w2w2",\
"inc_zhilabs_ftth_max_issue_ftth_degradation_w2w2",\
"inc_zhilabs_ftth_max_issue_ftth_prx_w1w1",\
"inc_zhilabs_ftth_max_issue_ftth_prx_w2w2",\
"inc_zhilabs_ftth_min_cpe_quick_restarts_w1w1",\
"inc_zhilabs_ftth_min_cpe_quick_restarts_w2w2",\
"inc_zhilabs_ftth_min_cpe_restarts_w1w1"]

HIGH_CORRELATED_VARS_FTTH = HIGH_CORRELATED_VARS_INC + HIGH_CORRELATED_VARS_FTTH

HIGH_CORRELATED_VARS_HFC = ["num_tickets_tipo_reclamacion_w8",\
"zhilabs_hfc_std_issue_hfc_snr_upstream_critical_current",\
"first_order_last365",\
"inc_zhilabs_hfc_std_wlan_5_errors_received_rate_w1w1",\
"Bill_N5_net_charges",\
"zhilabs_hfc_min_wlan_2_4_stats_packets_sent",\
"Bill_N1_net_charges",\
"inc_zhilabs_hfc_mean_wlan_5_stats_packets_sent_w2w2",\
"Bill_N5_days_since",\
"nb_started_orders_last120_impact",\
"Bill_N3_InvoiceCharges",\
"inc_zhilabs_hfc_std_wlan_5_stats_errors_sent_w1w1",\
"inc_zhilabs_hfc_std_hfc_percent_words_uncorrected_downstream_w1w1",\
"inc_zhilabs_hfc_min_hfc_percent_words_uncorrected_upstream_w2w2",\
"zhilabs_hfc_std_wlan_5_stats_errors_sent",\
"inc_zhilabs_hfc_std_wlan_2_4_errors_sent_rate_w1w1",\
"nb_started_orders_last240",\
"Bill_N4_num_facturas",\
"zhilabs_hfc_max_lan_host_802_11_num__entries",\
"inc_zhilabs_hfc_std_wlan_5_stats_errors_received_w1w1",\
"Bill_N2_num_facturas",\
"inc_zhilabs_hfc_max_wlan_5_stats_packets_received_w1w1",\
"zhilabs_hfc_max_issue_hfc_docsis_3_1_equivalent_modulation_critical_current",\
"inc_zhilabs_hfc_std_issue_hfc_docsis_3_1_equivalent_modulation_critical_current_w1w1",\
"cambio_orders_last240",\
"zhilabs_hfc_mean_issue_hfc_docsis_3_1_equivalent_modulation_critical_current",\
"inc_zhilabs_hfc_std_cpe_quick_restarts_w2w2",\
"zhilabs_hfc_std_issue_hfc_docsis_3_1_equivalent_modulation_critical_current",\
"Bill_N3_Tax_Amount",\
"inc_zhilabs_hfc_std_cpe_memory_free_w2w2",\
"Bill_N1_InvoiceCharges",\
"inc_zhilabs_hfc_std_wlan_5_errors_received_rate_w2w2",\
"zhilabs_hfc_min_hfc_prx_dowstream_average",\
"zhilabs_hfc_mean_hfc_snr_upstream_average",\
"Bill_N2_Debt_Amount",\
"inc_zhilabs_hfc_min_wlan_2_4_stats_errors_received_w2w2",\
"cambio_orders_last90",\
"nb_started_orders_last14",\
"inc_zhilabs_hfc_min_wlan_2_4_stats_packets_received_w1w1",\
"nb_started_orders_last120",\
"zhilabs_hfc_max_cpe_memory_free",\
"inc_zhilabs_hfc_min_wlan_5_stats_packets_received_w1w1",\
"zhilabs_hfc_mean_lan_host_802_11_num__entries",\
"zhilabs_hfc_min_cpe_memory_total",\
"inc_zhilabs_hfc_std_cpe_memory_total_w2w2",\
"Bill_N3_num_ids_fict",\
"zhilabs_hfc_std_wlan_5_errors_sent_rate",\
"inc_zhilabs_hfc_max_cpe_restarts_w2w2",\
"inc_zhilabs_hfc_max_hfc_number_of_flaps_current_w2w2",\
"zhilabs_hfc_max_lan_host_ethernet_num__entries",\
"Bill_N3_net_charges",\
"inc_zhilabs_hfc_std_wlan_5_stats_packets_sent_w2w2",\
"num_tickets_tipo_tramitacion_w8",\
"Bill_N5_num_facturas",\
"nb_mobile_services_nif",\
"inc_zhilabs_hfc_std_cpe_quick_restarts_w1w1",\
"inc_zhilabs_hfc_min_cpe_restarts_w2w2",\
"inc_zhilabs_hfc_min_issue_hfc_docsis_3_1_equivalent_modulation_critical_current_w1w1",\
"zhilabs_hfc_mean_cpe_memory_total",\
"Bill_N5_num_ids_fict",\
"inc_zhilabs_hfc_mean_wlan_5_stats_errors_sent_w1w1",\
"zhilabs_hfc_mean_hfc_number_of_flaps_current",\
"Bill_N4_InvoiceCharges",\
"zhilabs_hfc_std_lan_ethernet_stats_mbytes_sent",\
"zhilabs_hfc_mean_cpe_memory_free",\
"inc_zhilabs_hfc_mean_cpe_memory_total_w1w1",\
"zhilabs_hfc_std_hfc_percent_words_uncorrected_downstream",\
"inc_zhilabs_hfc_mean_wlan_5_stats_errors_received_w1w1",\
"inc_zhilabs_hfc_std_issue_hfc_flaps_warning_current_w1w1",\
"nif_distinct_msisdn",\
"zhilabs_hfc_max_wlan_2_4_stats_errors_sent",\
"inc_zhilabs_hfc_max_hfc_prx_dowstream_average_w2w2",\
"nb_completed_orders_last14",\
"zhilabs_hfc_max_cpe_memory_total",\
"inc_zhilabs_hfc_std_issue_hfc_fec_upstream_warning_current_w1w1",\
"zhilabs_hfc_max_issue_hfc_prx_downstream_warning_current",\
"inc_zhilabs_hfc_mean_hfc_prx_dowstream_average_w1w1",\
"disminucion_nb_completed_orders_last90",\
"zhilabs_hfc_mean_issue_hfc_prx_downstream_critical_current",\
"Bill_N2_Amount_To_Pay",\
"inc_zhilabs_hfc_std_wlan_2_4_errors_received_rate_w1w1",\
"Reimbursement_days_since",\
"Bill_N4_Amount_To_Pay",\
"inc_zhilabs_hfc_mean_wlan_5_stats_errors_sent_w2w2",\
"zhilabs_hfc_mean_issue_hfc_status_critical_current",\
"inc_zhilabs_hfc_std_cpe_restarts_w1w1",\
"num_tickets_tipo_tramitacion_closed",\
"zhilabs_hfc_mean_hfc_prx_dowstream_average",\
"zhilabs_hfc_std_hfc_number_of_flaps_current",\
"inc_zhilabs_hfc_min_cpe_memory_total_w1w1",\
"Bill_N3_num_facturas",\
"zhilabs_hfc_std_issue_hfc_fec_upstream_critical_current",\
"zhilabs_hfc_std_issue_hfc_flaps_critical_current",\
"Bill_N2_Tax_Amount",\
"nif_port_number",\
"inc_zhilabs_hfc_max_cpe_memory_total_w2w2",\
"inc_zhilabs_hfc_std_hfc_number_of_flaps_current_w2w2",\
"zhilabs_hfc_max_issue_hfc_snr_downstream_critical_current",\
"inc_zhilabs_hfc_mean_wlan_5_stats_packets_sent_w1w1",\
"inc_zhilabs_hfc_std_wlan_5_stats_errors_sent_w2w2",\
"zhilabs_hfc_mean_hfc_percent_words_uncorrected_upstream",\
"inc_zhilabs_hfc_std_issue_hfc_status_critical_current_w1w1",\
"zhilabs_hfc_max_wlan_5_stats_packets_sent",\
"zhilabs_hfc_max_wlan_5_errors_sent_rate",\
"zhilabs_hfc_max_wlan_5_stats_packets_received",\
"inc_zhilabs_hfc_std_wlan_2_4_errors_received_rate_w2w2",\
"inc_zhilabs_hfc_mean_wlan_5_stats_errors_received_w2w2",\
"zhilabs_hfc_max_wlan_5_errors_received_rate",\
"zhilabs_hfc_std_wlan_5_stats_errors_received",\
"zhilabs_hfc_max_wlan_5_stats_errors_sent",\
"zhilabs_hfc_mean_wlan_5_stats_errors_sent",\
"inc_zhilabs_hfc_max_cpe_restarts_w1w1",\
"Reimbursement_adjustment_debt",\
"inc_zhilabs_hfc_min_hfc_percent_words_uncorrected_upstream_w1w1",\
"inc_zhilabs_hfc_mean_issue_hfc_docsis_3_1_equivalent_modulation_critical_current_w2w2",\
"nb_completed_orders_last240",\
"zhilabs_hfc_mean_hfc_snr_downstream_average",\
"inc_zhilabs_hfc_mean_wlan_2_4_stats_errors_received_w1w1",\
"inc_zhilabs_hfc_std_wlan_2_4_stats_errors_sent_w1w1",\
"disminucion_orders_last120",\
"inc_zhilabs_hfc_std_issue_hfc_fec_upstream_critical_current_w1w1",\
"billing_avg_days_bw_bills",\
"zhilabs_hfc_min_wlan_5_stats_packets_received",\
"Bill_N3_Debt_Amount",\
"zhilabs_hfc_mean_cpe_restarts",\
"zhilabs_hfc_std_wlan_2_4_stats_errors_received",\
"inc_zhilabs_hfc_std_issue_hfc_flaps_critical_current_w1w1",\
"zhilabs_hfc_min_cpe_cpu_usage",\
"zhilabs_hfc_min_issue_hfc_status_critical_current",\
"nb_started_orders_last90_impact",\
"zhilabs_hfc_std_issue_hfc_flaps_warning_current",\
"Bill_N4_num_ids_fict",\
"inc_zhilabs_hfc_std_wlan_5_errors_sent_rate_w2w2",\
"zhilabs_hfc_std_issue_hfc_status_critical_current",\
"Bill_N3_Amount_To_Pay",\
"inc_zhilabs_hfc_std_wlan_2_4_stats_errors_received_w2w2",\
"inc_zhilabs_hfc_std_issue_hfc_snr_upstream_critical_current_w2w2",\
"inc_zhilabs_hfc_mean_wlan_5_errors_sent_rate_w2w2",\
"zhilabs_hfc_std_issue_hfc_fec_upstream_warning_current",\
"billing_mean",\
"inc_zhilabs_hfc_std_issue_hfc_status_critical_current_w2w2",\
"Bill_N1_Amount_To_Pay",\
"inc_zhilabs_hfc_std_hfc_percent_words_uncorrected_upstream_w1w1",\
"Reimbursement_days_2_solve",\
"zhilabs_hfc_max_issue_hfc_prx_downstream_critical_current",\
"inc_zhilabs_hfc_std_cpe_restarts_w2w2",\
"inc_zhilabs_hfc_std_issue_hfc_flaps_warning_current_w2w2",\
"inc_zhilabs_hfc_std_issue_hfc_flaps_critical_current_w2w2",\
"zhilabs_hfc_max_wlan_2_4_errors_received_rate",\
"Bill_N4_net_charges",\
"inc_zhilabs_hfc_min_issue_hfc_snr_upstream_critical_current_w2w2",\
"inc_zhilabs_hfc_mean_cpe_memory_total_w2w2",\
"Bill_N4_days_since",\
"nb_invoices",\
"inc_zhilabs_hfc_std_issue_hfc_snr_upstream_critical_current_w1w1",\
"inc_zhilabs_hfc_mean_hfc_number_of_flaps_current_w2w2",\
"inc_zhilabs_hfc_max_wlan_2_4_stats_errors_sent_w2w2",\
"zhilabs_hfc_max_wlan_2_4_errors_sent_rate",\
"inc_zhilabs_hfc_std_wlan_5_errors_sent_rate_w1w1",\
"zhilabs_hfc_std_cpe_quick_restarts",\
"inc_zhilabs_hfc_std_wlan_2_4_stats_errors_sent_w2w2",\
"zhilabs_hfc_max_wlan_2_4_stats_packets_received",\
"Bill_N5_InvoiceCharges",\
"zhilabs_hfc_max_issue_hfc_snr_downstream_warning_current",\
"zhilabs_hfc_std_wlan_2_4_stats_errors_sent",\
"Bill_N2_net_charges",\
"inc_zhilabs_hfc_std_wlan_5_stats_errors_received_w2w2",\
"inc_zhilabs_hfc_max_cpe_memory_free_w1w1",\
"Bill_N2_num_ids_fict",\
"inc_zhilabs_hfc_min_wlan_2_4_stats_errors_sent_w1w1",\
"num_tickets_closed",\
"inc_zhilabs_hfc_std_hfc_percent_words_uncorrected_downstream_w2w2",\
"inc_zhilabs_hfc_std_wlan_2_4_errors_sent_rate_w2w2",\
"zhilabs_hfc_max_cpe_restarts",\
"inc_zhilabs_hfc_std_hfc_percent_words_uncorrected_upstream_w2w2",\
"zhilabs_hfc_std_hfc_percent_words_uncorrected_upstream",\
"billing_current_debt",\
"Bill_N1_num_facturas",\
"inc_zhilabs_hfc_max_hfc_prx_dowstream_average_w1w1",\
"Bill_N3_days_since",\
"inc_zhilabs_hfc_std_issue_hfc_snr_upstream_warning_current_w1w1",\
"zhilabs_hfc_mean_issue_hfc_snr_upstream_critical_current",\
"Bill_N4_Debt_Amount",\
"Reimbursement_num",\
"Reimbursement_num_month_2",\
"zhilabs_hfc_std_issue_hfc_snr_downstream_warning_current",\
"zhilabs_hfc_max_wlan_2_4_stats_packets_sent",\
"Bill_N5_Amount_To_Pay",\
"Bill_N2_InvoiceCharges",\
"num_calls_w8",\
"max_time_closed_tipo_tramitacion",\
"inc_zhilabs_hfc_max_lan_host_802_11_num__entries_w1w1",\
"Bill_N2_days_since",\
"inc_zhilabs_hfc_std_wlan_5_stats_packets_sent_w1w1",\
"zhilabs_hfc_std_wlan_2_4_stats_packets_received",\
"inc_zhilabs_hfc_max_wlan_2_4_stats_errors_sent_w1w1",\
"zhilabs_hfc_std_wlan_5_stats_packets_received",\
"billing_std",\
"inc_zhilabs_hfc_mean_cpe_memory_free_w2w2",\
"yoigo_ACAN",\
"zhilabs_hfc_std_wlan_2_4_stats_packets_sent",\
"least_diff_bw_bills",\
"inc_zhilabs_hfc_min_wlan_5_stats_packets_received_w2w2",\
"num_distinct_operators",\
"zhilabs_hfc_std_cpe_restarts",\
"zhilabs_hfc_min_wlan_5_errors_received_rate",\
"inc_zhilabs_hfc_min_wlan_5_stats_errors_sent_w1w1",\
"inc_zhilabs_hfc_min_wlan_5_stats_errors_sent_w2w2",\
"inc_zhilabs_hfc_min_wlan_5_stats_errors_received_w1w1",\
"inc_zhilabs_hfc_min_issue_hfc_fec_upstream_warning_current_w2w2",\
"inc_zhilabs_hfc_min_wlan_5_errors_received_rate_w1w1",\
"inc_zhilabs_hfc_min_issue_hfc_fec_upstream_warning_current_w1w1",\
"inc_zhilabs_hfc_min_wlan_5_stats_errors_received_w2w2",\
"zhilabs_hfc_min_wlan_5_stats_errors_received",\
"inc_zhilabs_hfc_min_wlan_5_errors_sent_rate_w1w1",\
"zhilabs_hfc_min_wlan_5_errors_sent_rate",\
"zhilabs_hfc_min_cpe_quick_restarts",\
"nb_prepaid_services_nif",\
"zhilabs_hfc_min_wlan_5_stats_errors_sent",\
"inc_zhilabs_hfc_min_wlan_5_errors_received_rate_w2w2",\
"inc_zhilabs_hfc_min_wlan_5_errors_sent_rate_w2w2"]

CATEGORICAL_COLS = ['cpe_model', 'network_access_type']
NON_INFO_COLS = ["num_cliente"] #FIXME autodetect with metadata project
OWNER_LOGIN = "asaezco"


MODEL_OUTPUT_NAME = "zhilabs_ftth"
MODEL_OUTPUT_NAME_HFC = "zhilabs_hfc"
MODEL_OUTPUT_NAME_DSL = "zhilabs_dsl"

CAR_PATH_HFC = "/data/udf/vf_es/churn/triggers/zhilabs_hfc/"
CAR_PATH_UNLABELED_HFC = "/data/udf/vf_es/churn/triggers/zhilabs_hfc/"

CAR_PATH_FTTH = "/data/udf/vf_es/churn_nrt/zhilabs_ftth_car/"
CAR_PATH_UNLABELED_FTTH = "/data/udf/vf_es/churn_nrt/zhilabs_ftth_car/"

CAR_PATH_DSL = "/data/udf/vf_es/churn/triggers/zhilabs_dsl/"
CAR_PATH_UNLABELED_DSL = "/data/udf/vf_es/churn/triggers/zhilabs_dsl/"

EXTRA_INFO_COLS = ['decil', 'flag_propension','flag_router', 'flag_net', 'flag_error', 'cpe_model', 'network_access_type', 'flag_sharing', 'flag_issue']

DICT_CPU_WAR = {"Sercomm_Vodafone-H-500-s":70,
"Sercomm_VOX2.5":70,
"Sercomm FG824CD":50,
"Technicolor_Vodafone-H-500-t":60,
"Sercomm VOX30ONT":50,
"Technicolor_TC7230":70,
"SAGEMCOM_FAST3686":70,
"Technicolor_CGA4233VDF":40,
"Cisco_EPC3825":70,
"Huawei HG253 V2":50,
"Sercomm_FD1018":50,
"Sercomm_AD1018":50,
"HUAWEI_HG556a":70,
"Observa_BRA14NR":90,
"Sercomm_VD1018":50}

DICT_CPE_WAR = {"Sercomm_Vodafone-H-500-s":20,
"Sercomm_VOX2.5":20,
"Sercomm FG824CD":10,
"Technicolor_Vodafone-H-500-t":10,
"Sercomm VOX30ONT":10,
"Technicolor_TC7230":20,
"SAGEMCOM_FAST3686":20,
"Technicolor_CGA4233VDF":10,
"Cisco_EPC3825":20,
"Huawei HG253 V2 Ver.C":5,
"Huawei HG253 V2 Ver.D":20,
"Sercomm_FD1018":20,
"Sercomm_AD1018 v1":20,
"Sercomm_AD1018 v2":20,
"HUAWEI_HG556a (ver.A, B, C)":20,
"Observa_BRA14NR (ver H1, H2)":20,
"Sercomm_VD1018":20}

DICT_CPU = {"Sercomm_Vodafone-H-500-s":90,
"Sercomm_VOX2.5":90,
"Sercomm FG824CD":70,
"Technicolor_Vodafone-H-500-t":80,
"Sercomm VOX30ONT":70,
"Technicolor_TC7230":90,
"SAGEMCOM_FAST3686":90,
"Technicolor_CGA4233VDF":60,
"Cisco_EPC3825":80,
"Huawei HG253 V2":70,
"Sercomm_FD1018":70,
"Sercomm_AD1018":70,
"HUAWEI_HG556a":90,
"Observa_BRA14NR":95,
"Sercomm_VD1018":70}

DICT_CPE = {"Sercomm_Vodafone-H-500-s":10,
"Sercomm_VOX2.5":10,
"Sercomm FG824CD":5,
"Technicolor_Vodafone-H-500-t":5,
"Sercomm VOX30ONT":5,
"Technicolor_TC7230":10,
"SAGEMCOM_FAST3686":10,
"Technicolor_CGA4233VDF":5,
"Cisco_EPC3825":10,
"Huawei HG253 V2 Ver.C":3,
"Huawei HG253 V2 Ver.D":10,
"Sercomm_FD1018":10,
"Sercomm_AD1018 v1":10,
"Sercomm_AD1018 v2":10,
"HUAWEI_HG556a (ver.A, B, C)":10,
"Observa_BRA14NR (ver H1, H2)":10,
"Sercomm_VD1018":10}