
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
import os

from churn.utils.constants import HDFS_DIR_DATA

HDFS_METADATA_DIR = os.path.join(HDFS_DIR_DATA, "metadata")


class Metadata:

    # def get_id_feats(self):
    #
    #     ids_found = []
    #     for col_ in self.get_df().columns:
    #         if any([ col_.endswith(id_) for id_ in ["msisdn", "rgu", "num_cliente", "campo1", "campo2", "campo3"]]):
    #             ids_found.append(col_)
    #     print("FOUND {} IDENTIFIERS".format(",".join(ids_found)))
    #     return ids_found


    # def get_cols_by_type(self, type, table_name=""):
    #     '''
    #     Return the columns name with type 'type'. If table_name is passed, then
    #     only returns columns from this table.
    #     :param type: "categorical", "numeric" or "identifier"
    #     :param table_name: name of the table to look for the columns
    #     :return: list of columns that satisfies the conditions (type and table_name)
    #     '''
    #     dtypes = self.get_df().dtypes
    #     return [col_name for col_name, col_type in dtypes if col_type == type and table_name.startswith(table_name)]

    # def get_categoricals(self, table_name=""):
    #     return self.get_cols_by_type(type="string", table_name=table_name)
    #
    #
    # def get_numeric(self, table_name=""):
    #     return self.get_cols_by_type(type="int", table_name=table_name) + self.get_cols_by_type(type="double", table_name=table_name)


    #######################################################################
    # TGs
    #######################################################################


    @staticmethod
    def get_tgs_cols():
        # return the expected columns
        # removed intentionally tgs_descuento
        return ['tgs_stack', 'tgs_decil', 'tgs_sum_ind_under_use',
          'tgs_sum_ind_over_use', 'tgs_clasificacion_uso', 'tgs_blindaje_bi', 'tgs_blinda_bi_n2',
          'tgs_blinda_bi_n4', 'tgs_blindaje_bi_expirado', 'tgs_target_accionamiento',
          'tgs_days_since_f_inicio_bi', 'tgs_days_since_f_inicio_bi_exp', 'tgs_days_until_f_fin_bi',
          'tgs_days_until_f_fin_bi_exp', 'tgs_days_until_fecha_fin_dto', 'tgs_has_discount', 'tgs_discount_proc']

    @staticmethod
    def get_tgs_impute_na():

        fill_no_discount = ["tgs_descuento", 'tgs_discount_proc']
        fill_minus_one = ['tgs_has_discount', 'tgs_sum_ind_over_use', 'tgs_sum_ind_under_use',
                          'tgs_days_since_f_inicio_bi', 'tgs_days_since_f_inicio_bi_exp', 'tgs_days_until_f_fin_bi',
                          'tgs_days_until_f_fin_bi_exp', 'tgs_days_until_fecha_fin_dto',
                          ]
        fill_unknown = ['tgs_stack', 'tgs_clasificacion_uso', 'tgs_blindaje_bi_expirado', 'tgs_blindaje_bi',
                        'tgs_blinda_bi_n4', 'tgs_blinda_bi_n2', 'tgs_decil', 'tgs_target_accionamiento']

        impute_na = [(col_, 'tgs', "string", "NO_DISCOUNT") for col_ in fill_no_discount]
        impute_na += [(col_, 'tgs', "double", -1.0) for col_ in fill_minus_one]
        impute_na += [(col_, 'tgs', "string", "UNKNOWN") for col_ in fill_unknown]

        return impute_na


    #######################################################################
    # CCCs
    #######################################################################




    @staticmethod
    def get_ccc_cols():
        return [ 'ccc_num_pbma_srv',
                 'ccc_num_abonos_tipis',
                 'ccc_num_tipis_factura',
                 'ccc_num_tipis_perm_dctos',
                 'ccc_num_tipis_info_nocomp',
                 'ccc_num_tipis_info_comp',
                 'ccc_num_tipis_info',
                 'ccc_ind_tipif_uci',
                 'ccc_ind_tipif_abonos',
                 'ccc_ind_tipif_factura',
                 'ccc_ind_tipif_perm_dctos',
                 'ccc_ind_tipif_info',
                 'ccc_ind_tipif_info_comp',
                 'ccc_ind_tipif_info_nocomp',
                 'ccc_raw_pagar_menos',
                 'ccc_raw_cobro',
                 'ccc_raw_precios',
                 'ccc_raw_averia_dsl',
                 'ccc_raw_averia_fibra',
                 'ccc_raw_averia_tv',
                 'ccc_raw_averia_resto',
                 'ccc_raw_averia_neba',
                 'ccc_raw_averia_modem_router',
                 'ccc_raw_averia_fijo',
                 'ccc_raw_averia_movil',
                 'ccc_raw_alta',
                 'ccc_raw_desactivacion_ba_movil_tv',
                 'ccc_raw_desactivacion_tv',
                 'ccc_raw_desactivacion_movil',
                 'ccc_raw_desactivacion_total',
                 'ccc_raw_desactivacion_net',
                 'ccc_raw_desactivacion_fijo',
                 'ccc_raw_desactivacion_usb',
                 'ccc_raw_desactivacion_resto',
                 'ccc_raw_factura',
                 'ccc_raw_ofrecimiento',
                 'ccc_bucket_sub_bucket_churn_cancellations_other_churn_issues',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_sim',
                 'ccc_bucket_sub_bucket_churn_cancellations_churn_cancellations_process',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_fibra_support',
                 'ccc_bucket_sub_bucket_tariff_management_voice_line',
                 'ccc_bucket_sub_bucket_product_and_service_management_standard_products',
                 'ccc_bucket_sub_bucket__',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_transfers',
                 'ccc_bucket_sub_bucket_device_delivery_repair_referrals',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_tv',
                 'ccc_bucket_sub_bucket_collections_referrals',
                 'ccc_bucket_sub_bucket_collections_collections_process',
                 'ccc_bucket_sub_bucket_collections_debt_recovery',
                 'ccc_bucket_sub_bucket_new_adds_process_prepaid_initial_balance',
                 'ccc_bucket_sub_bucket_device_delivery_repair_network',
                 'ccc_bucket_sub_bucket_tariff_management_tv',
                 'ccc_bucket_sub_bucket_other_customer_information_management_referrals',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_modem_router_support',
                 'ccc_bucket_sub_bucket_billing___postpaid_other_billing_issues',
                 'ccc_bucket_sub_bucket_other_customer_information_management_tv',
                 'ccc_bucket_sub_bucket_other_customer_information_management_prepaid_initial_balance',
                 'ccc_bucket_sub_bucket_quick_closing_quick_closing',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_referrals',
                 'ccc_bucket_sub_bucket_new_adds_process_device_upgrade_order',
                 'ccc_bucket_sub_bucket_tariff_management_referrals',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_fibra_incidences',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_dsl_support',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_mobile_data_incidences',
                 'ccc_bucket_sub_bucket_collections_negotiation',
                 'ccc_bucket_sub_bucket_billing___postpaid_transfers',
                 'ccc_bucket_sub_bucket_new_adds_process_tv',
                 'ccc_bucket_sub_bucket_device_delivery_repair_transfers',
                 'ccc_bucket_sub_bucket_billing___postpaid_invoice_clarification',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_network',
                 'ccc_bucket_sub_bucket_device_upgrade_transfers',
                 'ccc_bucket_sub_bucket_product_and_service_management_tv',
                 'ccc_bucket_sub_bucket_product_and_service_management_network',
                 'ccc_bucket_sub_bucket_device_upgrade_tv',
                 'ccc_bucket_sub_bucket_tariff_management_voice_tariff',
                 'ccc_bucket_sub_bucket_device_upgrade_device_delivery_repair',
                 'ccc_bucket_sub_bucket_new_adds_process_transfers',
                 'ccc_bucket_sub_bucket_device_delivery_repair_device_repair',
                 'ccc_bucket_sub_bucket_device_delivery_repair_new_adds',
                 'ccc_bucket_sub_bucket_other_customer_information_management_pin_puk',
                 'ccc_bucket_sub_bucket_product_and_service_management_prepaid_initial_balance',
                 'ccc_bucket_sub_bucket_tariff_management_data',
                 'ccc_bucket_sub_bucket_other_customer_information_management_customer_service_process',
                 'ccc_bucket_sub_bucket_collections_transfers',
                 'ccc_bucket_sub_bucket_prepaid_balance_top_up_process',
                 'ccc_bucket_sub_bucket_new_adds_process_new_adds',
                 'ccc_bucket_sub_bucket_device_delivery_repair_sim',
                 'ccc_bucket_sub_bucket_churn_cancellations_negotiation',
                 'ccc_bucket_sub_bucket_other_customer_information_management_simlock',
                 'ccc_bucket_sub_bucket_billing___postpaid_tv',
                 'ccc_bucket_sub_bucket_product_and_service_management_transfers',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_modem_router_support',
                 'ccc_bucket_sub_bucket_device_delivery_repair_device_delivery_repair',
                 'ccc_bucket_sub_bucket_churn_cancellations_referrals',
                 'ccc_bucket_sub_bucket_other_customer_information_management_customer_data',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_referrals',
                 'ccc_bucket_sub_bucket_churn_cancellations_transfers',
                 'ccc_bucket_sub_bucket_device_upgrade_device_upgrade',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_modem_router_incidences',
                 'ccc_bucket_sub_bucket_churn_cancellations_network',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_network',
                 'ccc_bucket_sub_bucket_product_and_service_management_contracted_products_dsl',
                 'ccc_bucket_sub_bucket_other_customer_information_management_churn_cancellations_process',
                 'ccc_bucket_sub_bucket_new_adds_process_voice',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_tv',
                 'ccc_bucket_sub_bucket_device_delivery_repair_device_information',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_transfers',
                 'ccc_bucket_sub_bucket_bucket_sub_bucket',
                 'ccc_bucket_sub_bucket_billing___postpaid_billing_errors',
                 'ccc_bucket_sub_bucket_new_adds_process_referrals',
                 'ccc_bucket_sub_bucket_billing___postpaid_',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_dsl_incidences',
                 'ccc_bucket_sub_bucket_tariff_management_network',
                 'ccc_bucket_sub_bucket_new_adds_process_voice_line',
                 'ccc_bucket_sub_bucket_tariff_management_data_tariff',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_mobile_data_incidences',
                 'ccc_bucket_sub_bucket_device_upgrade_device_information',
                 'ccc_bucket_sub_bucket_other_customer_information_management_transfers',
                 'ccc_bucket_sub_bucket_product_and_service_management_referrals',
                 'ccc_bucket_sub_bucket_voice_and_mobile_data_incidences_and_support_mobile_data_support',
                 'ccc_bucket_sub_bucket_dsl_fiber_incidences_and_support_modem_router_incidences',
                 'ccc_bucket_sub_bucket_device_delivery_repair_device_upgrade_order',
                 'ccc_bucket_sub_bucket_tariff_management_voice',
                 'ccc_bucket_sub_bucket_product_and_service_management_contracted_products',
                 'ccc_bucket_sub_bucket_device_upgrade_device_upgrade_order',
                 'ccc_bucket_sub_bucket_product_and_service_management_voice_tariff',
                 'ccc_bucket_sub_bucket_tariff_management_transfers',
                 'ccc_bucket_sub_bucket_product_and_service_management_simlock',
                 'ccc_bucket_sub_bucket_device_delivery_repair_device_upgrade',
                 'ccc_bucket_sub_bucket_new_adds_process_fix_line',
                 'ccc_bucket_sub_bucket_prepaid_balance_transfers',
                 'ccc_bucket_sub_bucket_device_upgrade_referrals',
                 'ccc_bucket_sub_bucket_churn_cancellations_tv',
                 'ccc_bucket_sub_bucket_device_upgrade_device_repair',
                 'ccc_bucket_sub_bucket_collections_tv',
                 'ccc_raw_incidencia_provision_neba',
                 'ccc_raw_incidencia_provision_fibra',
                 'ccc_raw_incidencia_provision_dsl',
                 'ccc_raw_incidencia_tecnica',
                 'ccc_raw_incidencia_sgi',
                 'ccc_raw_incidencia_resto',
                 'ccc_raw_incidencia_provision_movil',
                 'ccc_raw_consulta_tecnica_tv',
                 'ccc_raw_consulta_tecnica_fibra',
                 'ccc_raw_consulta_tecnica_neba',
                 'ccc_raw_consulta_tecnica_dsl',
                 'ccc_raw_consulta_tecnica_movil',
                 'ccc_raw_consulta_tecnica_modem_router',
                 'ccc_raw_consulta_tecnica_resto',
                 'ccc_raw_consulta_ficha',
                 'ccc_raw_consulta_resto',
                 'ccc_raw_informacion',
                 'ccc_raw_portabilidad_inversa',
                 'ccc_raw_portabilidad',
                 'ccc_raw_cierre',
                 'ccc_raw_productos_voz',
                 'ccc_raw_productos_datos',
                 'ccc_raw_productos_resto',
                 'ccc_raw_resultado_no_aplica',
                 'ccc_raw_resultado_informacion',
                 'ccc_raw_resultado_solucionado',
                 'ccc_raw_resultado_retenido',
                 'ccc_raw_resultado_no_retenido',
                 'ccc_raw_resultado_escalo',
                 'ccc_raw_resultado_envio_tecnico',
                 'ccc_raw_resultado_transferencia',
                 'ccc_raw_resultado_abono',
                 'ccc_raw_resultado_bajas',
                 'ccc_raw_resultado_reclamacion',
                 'ccc_raw_provision_neba',
                 'ccc_raw_provision_fibra',
                 'ccc_raw_provision_dsl',
                 'ccc_raw_provision_resto',
                 'ccc_raw_provision_movil',
                 'ccc_bucket_churn_cancellations',
                 'ccc_bucket_collections',
                 'ccc_bucket_billing___postpaid',
                 'ccc_bucket_',
                 'ccc_bucket_device_upgrade',
                 'ccc_bucket_prepaid_balance',
                 'ccc_bucket_quick_closing',
                 'ccc_bucket_new_adds_process',
                 'ccc_bucket_other_customer_information_management',
                 'ccc_bucket_bucket',
                 'ccc_bucket_product_and_service_management',
                 'ccc_bucket_dsl_fiber_incidences_and_support',
                 'ccc_bucket_tariff_management',
                 'ccc_bucket_voice_and_mobile_data_incidences_and_support',
                 'ccc_bucket_device_delivery_repair',
                 'ccc_raw_transferencia',
                 'ccc_raw_baja',
                 'ccc_num_averias',
                 'ccc_num_incidencias',
                 'ccc_num_interactions',
                 'ccc_num_na_buckets',
                 'ccc_num_ivr_interactions',
                 'ccc_bucket_1st_interaction',
                 'ccc_bucket_latest_interaction',
                 'ccc_num_diff_buckets',
                 'ccc_days_since_first_interaction',
                 'ccc_days_since_latest_interaction',
                 'ccc_most_common_bucket_with_ties',
                 'ccc_most_common_bucket',
                 'ccc_most_common_bucket_interactions']

    @staticmethod
    def get_ccc_impute_na():

        import re
        fill_minus_one = ['ccc_most_common_bucket_interactions', 'ccc_days_since_first_interaction', 'ccc_days_since_latest_interaction']
        fill_unknown = ['ccc_most_common_bucket_with_ties', 'ccc_most_common_bucket']
        fill_zero = [col_ for col_ in Metadata.get_ccc_cols() if re.match("^ccc_bucket_|^ccc_raw_|^ccc_num_|^ccc_ind_tipif", col_)]

        impute_na = [(col_, 'ccc', "double", -1.0) for col_ in fill_minus_one]
        impute_na += [(col_, 'ccc', "string", "UNKNOWN") for col_ in fill_unknown]
        impute_na += [(col_, 'ccc', "double", 0) for col_ in fill_zero]

        return impute_na


    #######################################################################
    # OTHERS - DO NOT APPLY TO A SPECIFIC MODULE
    #######################################################################
    # IND_PBMA_SRV is a mix of col("ccc_ind_tipif_uci") +  col("pbms_srv_ind_averias") + col('pbms_srv_ind_soporte') +  col('pbms_srv_ind_reclamaciones'
    # FIXME this column shoould be lower case
    @staticmethod
    def get_others_cols():
        return ['others_ind_pbma_srv']

    @staticmethod
    def get_others_impute_na():
        fill_zero = ['others_ind_pbma_srv']
        impute_na = [(col_, 'others', "double", 0) for col_ in fill_zero]
        return impute_na

    #######################################################################
    # PROBMA SRV
    #######################################################################

    @staticmethod
    def get_pbma_srv_cols():
        return ['pbms_srv_ind_averias', 'pbms_srv_ind_soporte',
                 'pbms_srv_ind_reclamaciones', 'pbms_srv_ind_degrad_adsl',
                 'pbms_srv_num_averias', 'pbms_srv_num_soporte_tecnico',
                 'pbms_srv_num_reclamaciones']

    @staticmethod
    def get_pbma_srv_impute_na():
        from churn.datapreparation.general.problemas_servicio_data_loader import EXTRA_FEATS_PREFIX as prefix_pbma_srv
        fill_zero = [col_ for col_ in Metadata.get_pbma_srv_cols() if col_.lower().startswith(prefix_pbma_srv)]
        impute_na = [(col_, 'tgs', "double", 0) for col_ in fill_zero]
        return impute_na

    #######################################################################
    # DEVICES
    #######################################################################

    @staticmethod
    def get_devices_cols():
        return ['device_tenure_days_from_n1',
             'device_tenure_days_n2',
             'device_tenure_days_n3',
             'device_tenure_days_n4',
             'device_month_imei_changes',
             'device_month_device_changes',
             'device_num_devices',
             'device_days_since_device_n1_change_date',
             'device_days_since_device_n2_change_date',
             'device_days_since_device_n3_change_date',
             'device_days_since_device_n4_change_date']

    @staticmethod
    def get_devices_impute_na():

        fill_minus_one = ["device_tenure_days_from_n1", "device_tenure_days_n2",
                          "device_tenure_days_n3", "device_tenure_days_n4",
                          "device_days_since_device_n1_change_date", "device_days_since_device_n2_change_date",
                          "device_days_since_device_n3_change_date", "device_days_since_device_n4_change_date",
                          ]

        fill_zero = ['device_month_device_changes', "device_month_imei_changes", "device_num_devices"]

        impute_na = [(col_, 'devices', "double", -1.0) for col_ in fill_minus_one]
        impute_na += [(col_, 'devices', "double", 0) for col_ in fill_zero]

        return impute_na

    #######################################################################
    # DEVICES
    #######################################################################

    @staticmethod
    def get_scores_cols():
        return [
            'scores_scoring1',
            'scores_scoring2',
            'scores_diff_scoring_1_2',
            'scores_slope_scoring_1_2',
            'scores_scoring3',
            'scores_diff_scoring_2_3',
            'scores_slope_scoring_2_3',
            'scores_scoring4',
            'scores_diff_scoring_3_4',
            'scores_slope_scoring_3_4',
            'scores_scoring5',
            'scores_diff_scoring_4_5',
            'scores_slope_scoring_4_5',
            'scores_scoring6',
            'scores_diff_scoring_5_6',
            'scores_slope_scoring_5_6',
            'scores_scoring7',
            'scores_diff_scoring_6_7',
            'scores_slope_scoring_6_7',
            'scores_scoring8',
            'scores_diff_scoring_7_8',
            'scores_slope_scoring_7_8',
            'scores_scoring9',
            'scores_diff_scoring_8_9',
            'scores_slope_scoring_8_9',
            'scores_scoring10',
            'scores_diff_scoring_9_10',
            'scores_slope_scoring_9_10',
            'scores_scoring11',
            'scores_diff_scoring_10_11',
            'scores_slope_scoring_10_11',
            'scores_scoring12',
            'scores_diff_scoring_11_12',
            'scores_slope_scoring_11_12',
            'scores_num_scores_historic',
            'scores_mean_historic',
            'scores_mean_1_4',
            'scores_mean_5_8',
            'scores_mean_9_12',
            'scores_std_historic',
            'scores_std_1_4',
            'scores_std_5_8',
            'scores_std_9_12',
            'scores_size_diff',
            'scores_mean_diff_scoring_array',
            'scores_stddev_diff_scoring_array',
            'scores_max_score',
            'scores_min_score',
            'scores_max_diff_score',
            'scores_min_diff_score',
            'scores_max_q_score',
            'scores_pos_max_q_score',
            'scores_min_q_score',
            'scores_pos_min_q_score',
            'scores_new_client',
            'scores_nb_cycles_same_trend'
                ]

    @staticmethod
    def get_scores_impute_na():

        fill_minus_one = [
            'scores_scoring1',
            'scores_scoring2',
            'scores_scoring3',
            'scores_scoring4',
            'scores_scoring5',
            'scores_scoring6',
            'scores_scoring7',
            'scores_scoring8',
            'scores_scoring9',
            'scores_scoring10',
            'scores_scoring11',
            'scores_scoring12',
            'scores_mean_historic',

            'scores_mean_1_4',
            'scores_mean_5_8',
            'scores_mean_9_12',

            'scores_max_score',
            'scores_min_score',

            'scores_pos_min_q_score',
            'scores_pos_max_q_score',


            'scores_min_q_score',
            'scores_max_q_score',

        ]

        fill_zero = [
            'scores_diff_scoring_1_2',
            'scores_diff_scoring_2_3',
            'scores_diff_scoring_3_4',
            'scores_diff_scoring_4_5',
            'scores_diff_scoring_5_6',
            'scores_diff_scoring_6_7',
            'scores_diff_scoring_7_8',
            'scores_diff_scoring_9_10',
            'scores_diff_scoring_10_11',
            'scores_diff_scoring_11_12',

            'scores_slope_scoring_1_2',
            'scores_slope_scoring_2_3',
            'scores_slope_scoring_3_4',
            'scores_slope_scoring_4_5',
            'scores_slope_scoring_5_6',
            'scores_slope_scoring_6_7',
            'scores_slope_scoring_7_8',
            'scores_diff_scoring_8_9',
            'scores_slope_scoring_8_9',
            'scores_slope_scoring_9_10',
            'scores_slope_scoring_10_11',
            'scores_slope_scoring_11_12',
            'scores_num_scores_historic',

            'scores_std_historic',

            'scores_std_1_4',
            'scores_std_5_8',
            'scores_std_9_12',

            'scores_size_diff',

            'scores_mean_diff_scoring',
            'scores_stddev_diff_scoring',

            'scores_nb_consec_slopes'

            'scores_max_diff_score',
            'scores_min_diff_score',

            'scores_new_client',

            'scores_nb_cycles_same_trend'

        ]

        impute_na = [(col_, 'scores', "double", -1.0) for col_ in fill_minus_one]
        impute_na += [(col_, 'scores', "double", 0) for col_ in fill_zero]

        return impute_na




    #######################################################################
    # GENERAL
    #######################################################################

    @staticmethod
    def create_metadata_df(spark, impute_na):
        from pyspark.sql.types import StructField, StringType, StructType

        schema = StructType([StructField('feature', StringType(), True),
                             StructField('source', StringType(), True),
                             StructField('type', StringType(), True),
                             StructField('imputation', StringType(), True),
                             ])

        # create dataframe
        return spark.createDataFrame(impute_na, schema)

    @staticmethod
    def apply_metadata(df, df_metadata):
        from pyspark.sql.functions import col

        # - - - -
        # Remove from metadata values of 'feature' that do not exist on df columns
        if logger: logger.info("df_metadata={}".format(df_metadata))
        df_metadata = df_metadata.where(col("feature").isin(list(df.columns)))
        if logger: logger.info("df_metadata={}".format(df_metadata))

        map_types = dict(df_metadata.rdd.map(lambda x: (x[0], float(x[3]) if x[2] == "double" else x[3])).collect())

        if logger: logger.info("about to call fillna")
        df = df.fillna(map_types)
        if logger: logger.info("after call to fillna")

        return df

    @staticmethod
    def get_metadata_path(version):
        metadata_dir = os.path.join(HDFS_METADATA_DIR, "metadata") + "_v" + version if version else os.path.join(HDFS_METADATA_DIR, "metadata")
        return metadata_dir


    @staticmethod
    def load_metadata_table(spark, version):
        metadata_dir = Metadata.get_metadata_path(version)
        df_metadata = spark.read.option("delimiter", "|").option("header", True).csv(metadata_dir)
        return df_metadata