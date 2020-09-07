from itertools import chain

def flatten(listOfLists):
    return list(chain.from_iterable(listOfLists))


def getNumerical(df):

    numeric = ["bigint", "double", "int", "long"]
    columnList = []
    for n in numeric:
        columnList.append([item[0] for item in df.dtypes if item[1].startswith(n)])
    return (flatten(columnList))

def get_noninf_features():

    #DROP ORD_SLA

    non_inf_feats = (["Serv_L2_days_since_Serv_fx_data",
                        "Cust_Agg_L2_total_num_services_nc",
                        "Cust_Agg_L2_total_num_services_nif",
                        "Camp_NIFs_Delight_TEL_Target_0",
                        "Camp_NIFs_Delight_TEL_Universal_0",
                        "Camp_NIFs_Ignite_EMA_Target_0",
                        "Camp_NIFs_Ignite_SMS_Control_0",
                        "Camp_NIFs_Ignite_SMS_Target_0",
                        "Camp_NIFs_Ignite_TEL_Universal_0",
                        "Camp_NIFs_Legal_Informativa_SLS_Target_0",
                        "Camp_NIFs_Retention_HH_SAT_Target_0",
                        "Camp_NIFs_Retention_HH_TEL_Target_0",
                        "Camp_NIFs_Retention_Voice_EMA_Control_0",
                        "Camp_NIFs_Retention_Voice_EMA_Control_1",
                        "Camp_NIFs_Retention_Voice_EMA_Target_0",
                        "Camp_NIFs_Retention_Voice_EMA_Target_1",
                        "Camp_NIFs_Retention_Voice_SAT_Control_0",
                        "Camp_NIFs_Retention_Voice_SAT_Control_1",
                        "Camp_NIFs_Retention_Voice_SAT_Target_0",
                        "Camp_NIFs_Retention_Voice_SAT_Target_1",
                        "Camp_NIFs_Retention_Voice_SAT_Universal_0",
                        "Camp_NIFs_Retention_Voice_SAT_Universal_1",
                        "Camp_NIFs_Retention_Voice_SMS_Control_0",
                        "Camp_NIFs_Retention_Voice_SMS_Control_1",
                        "Camp_NIFs_Retention_Voice_SMS_Target_0",
                        "Camp_NIFs_Retention_Voice_SMS_Target_1",
                        "Camp_NIFs_Terceros_TEL_Universal_0",
                        "Camp_NIFs_Terceros_TER_Control_0",
                        "Camp_NIFs_Terceros_TER_Control_1",
                        "Camp_NIFs_Terceros_TER_Target_0",
                        "Camp_NIFs_Terceros_TER_Target_1",
                        "Camp_NIFs_Terceros_TER_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_EMA_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_NOT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_SMS_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_1",
                        "Camp_NIFs_Up_Cross_Sell_MLT_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_MMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_SMS_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Control_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Control_1",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Target_1",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Universal_0",
                        "Camp_NIFs_Up_Cross_Sell_TEL_Universal_1",
                        "Camp_NIFs_Up_Cross_Sell_TER_Target_0",
                        "Camp_NIFs_Up_Cross_Sell_TER_Universal_0",
                        "Camp_NIFs_Welcome_EMA_Target_0",
                        "Camp_NIFs_Welcome_TEL_Target_0",
                        "Camp_NIFs_Welcome_TEL_Universal_0",
                        "Camp_SRV_Delight_NOT_Universal_0",
                        "Camp_SRV_Delight_SMS_Control_0",
                        "Camp_SRV_Delight_SMS_Universal_0",
                        "Camp_SRV_Ignite_MMS_Target_0",
                        "Camp_SRV_Ignite_NOT_Target_0",
                        "Camp_SRV_Ignite_SMS_Target_0",
                        "Camp_SRV_Ignite_SMS_Universal_0",
                        "Camp_SRV_Legal_Informativa_EMA_Target_0",
                        "Camp_SRV_Legal_Informativa_MLT_Universal_0",
                        "Camp_SRV_Legal_Informativa_MMS_Target_0",
                        "Camp_SRV_Legal_Informativa_NOT_Target_0",
                        "Camp_SRV_Legal_Informativa_SMS_Target_0",
                        "Camp_SRV_Retention_Voice_EMA_Control_0",
                        "Camp_SRV_Retention_Voice_EMA_Control_1",
                        "Camp_SRV_Retention_Voice_EMA_Target_0",
                        "Camp_SRV_Retention_Voice_EMA_Target_1",
                        "Camp_SRV_Retention_Voice_MLT_Universal_0",
                        "Camp_SRV_Retention_Voice_NOT_Control_0",
                        "Camp_SRV_Retention_Voice_NOT_Target_0",
                        "Camp_SRV_Retention_Voice_NOT_Target_1",
                        "Camp_SRV_Retention_Voice_NOT_Universal_0",
                        "Camp_SRV_Retention_Voice_SAT_Control_0",
                        "Camp_SRV_Retention_Voice_SAT_Control_1",
                        "Camp_SRV_Retention_Voice_SAT_Target_0",
                        "Camp_SRV_Retention_Voice_SAT_Target_1",
                        "Camp_SRV_Retention_Voice_SAT_Universal_0",
                        "Camp_SRV_Retention_Voice_SAT_Universal_1",
                        "Camp_SRV_Retention_Voice_SLS_Control_0",
                        "Camp_SRV_Retention_Voice_SLS_Control_1",
                        "Camp_SRV_Retention_Voice_SLS_Target_0",
                        "Camp_SRV_Retention_Voice_SLS_Target_1",
                        "Camp_SRV_Retention_Voice_SLS_Universal_0",
                        "Camp_SRV_Retention_Voice_SLS_Universal_1",
                        "Camp_SRV_Retention_Voice_SMS_Control_0",
                        "Camp_SRV_Retention_Voice_SMS_Control_1",
                        "Camp_SRV_Retention_Voice_SMS_Target_1",
                        "Camp_SRV_Retention_Voice_SMS_Universal_0",
                        "Camp_SRV_Retention_Voice_SMS_Universal_1",
                        "Camp_SRV_Retention_Voice_TEL_Control_1",
                        "Camp_SRV_Retention_Voice_TEL_Target_1",
                        "Camp_SRV_Retention_Voice_TEL_Universal_0",
                        "Camp_SRV_Retention_Voice_TEL_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Control_0",
                        "Camp_SRV_Up_Cross_Sell_EMA_Control_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Target_0",
                        "Camp_SRV_Up_Cross_Sell_EMA_Target_1",
                        "Camp_SRV_Up_Cross_Sell_EMA_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_HH_SMS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_MLT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_MMS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_MMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_NOT_Control_0",
                        "Camp_SRV_Up_Cross_Sell_NOT_Target_0",
                        "Camp_SRV_Up_Cross_Sell_NOT_Target_1",
                        "Camp_SRV_Up_Cross_Sell_NOT_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
                        "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Target_1",
                        "Camp_SRV_Up_Cross_Sell_SMS_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_SMS_Universal_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Control_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Control_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Target_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Target_1",
                        "Camp_SRV_Up_Cross_Sell_TEL_Universal_0",
                        "Camp_SRV_Up_Cross_Sell_TEL_Universal_1",
                        "Camp_SRV_Welcome_EMA_Target_0",
                        "Camp_SRV_Welcome_MMS_Target_0",
                        "Camp_SRV_Welcome_SMS_Target_0",
                        "Camp_L2_srv_total_redem_Target",
                        "Camp_L2_srv_pcg_redem_Target",
                        "Camp_L2_srv_total_redem_Control",
                        "Camp_L2_srv_total_redem_Universal",
                        "Camp_L2_srv_total_redem_EMA",
                        "Camp_L2_srv_total_redem_TEL",
                        "Camp_L2_srv_total_camps_SAT",
                        "Camp_L2_srv_total_redem_SAT",
                        "Camp_L2_srv_total_redem_SMS",
                        "Camp_L2_srv_pcg_redem_SMS",
                        "Camp_L2_srv_total_camps_MMS",
                        "Camp_L2_srv_total_redem_MMS",
                        "Camp_L2_srv_total_redem_Retention_Voice",
                        "Camp_L2_srv_total_redem_Up_Cross_Sell",
                        "Camp_L2_nif_total_redem_Target",
                        "Camp_L2_nif_pcg_redem_Target",
                        "Camp_L2_nif_total_redem_Control",
                        "Camp_L2_nif_total_redem_Universal",
                        "Camp_L2_nif_total_redem_EMA",
                        "Camp_L2_nif_total_redem_TEL",
                        "Camp_L2_nif_total_camps_SAT",
                        "Camp_L2_nif_total_redem_SAT",
                        "Camp_L2_nif_total_redem_SMS",
                        "Camp_L2_nif_pcg_redem_SMS",
                        "Camp_L2_nif_total_camps_MMS",
                        "Camp_L2_nif_total_redem_MMS",
                        "Camp_L2_nif_total_redem_Retention_Voice",
                        "Camp_L2_nif_total_redem_Up_Cross_Sell",
                        "Serv_PRICE_TARIFF",
                        "GNV_Roam_Data_L2_total_connections_W",
                        "GNV_Roam_Data_L2_total_data_volume_WE",
                        "GNV_Roam_Data_L2_total_data_volume_W",
                        "Camp_NIFs_Delight_SMS_Control_0",
                        "Camp_NIFs_Delight_SMS_Target_0",
                        "Camp_NIFs_Legal_Informativa_EMA_Target_0",
                        "Camp_SRV_Delight_EMA_Target_0",
                        "Camp_SRV_Delight_MLT_Universal_0",
                        "Camp_SRV_Delight_NOT_Target_0",
                        "Camp_SRV_Delight_TEL_Universal_0",
                        "Camp_SRV_Retention_Voice_TEL_Control_0",
                        "Camp_L2_srv_total_camps_TEL",
                        "Camp_L2_srv_total_camps_Target",
                        "Camp_L2_srv_total_camps_Up_Cross_Sell",
                        "Camp_L2_nif_total_camps_TEL",
                        "Camp_L2_nif_total_camps_Target",
                        "Camp_L2_nif_total_camps_Up_Cross_Sell"
                        ])
    return non_inf_feats


def get_id_features():

    return ["msisdn", "rgu", "num_cliente", "CAMPO1", "CAMPO2", "CAMPO3", "NIF_CLIENTE", "IMSI", "Instancia_P",
            "nif_cliente_tgs", "num_cliente_tgs", "msisdn_d", "num_cliente_d", "nif_cliente_d"]


def get_no_input_feats():
    return ["Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_NOMBRE", "Cust_PRIM_APELLIDO", "Cust_SEG_APELLIDO", "Cust_DIR_LINEA1", "Cust_DIR_LINEA2",
            "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2", "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4",
            "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT", "Cust_APELLIDO1_CLI_FACT",
            "Cust_APELLIDO2_CLI_FACT", "Cust_DIR_LINEA1", "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2",
            "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4", "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT",
            "Cust_APELLIDO1_CLI_FACT", "Cust_APELLIDO2_CLI_FACT", "Cust_CTA_CORREO_CONTACTO", "Cust_CTA_CORREO",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE", "CCC_L2_bucket_list",
            "CCC_L2_bucket_set", "Serv_NUM_SERIE_DECO_TV", "Order_N1_Description", "Order_N2_Description",
            "Order_N5_Description", "Order_N7_Description", "Order_N8_Description", "Order_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "device_n5_imei", "Order_N1_Id", "Order_N2_Id", "Order_N3_Id", "Order_N4_Id", "Order_N5_Id", "Order_N6_Id",
            "Order_N7_Id", "Order_N8_Id", "Order_N9_Id", "Order_N10_Id"]



def get_campaigns_to_drop():
    return ["Camp_NIFs_Up_Cross_Sell_SAT_Control_0",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_1",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_2",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_3",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_4",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_5",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_6",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_7",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_8",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_9",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_10",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_11",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_12",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_13",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_14",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_15",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_16",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_17",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_18",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_19",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_20",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_21",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_22",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_23",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_24",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_25",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_26",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_27",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_28",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_29",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_30",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_31",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_32",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_33",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_34",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_35",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_36",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_37",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_38",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_39",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_40",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_41",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_42",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_43",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_44",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_45",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_46",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_47",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_48",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_49",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_50",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_51",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_52",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_53",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_54",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_55",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_56",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_57",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_58",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_59",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_60",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_61",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_62",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_63",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_64",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_65",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_66",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_67",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_68",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_69",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_70",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_71",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_72",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_73",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_74",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_75",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_76",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_77",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_78",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_79",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_80",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_81",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_82",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_83",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_84",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_85",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_86",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_87",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_88",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_89",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_90",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_91",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_92",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_93",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_94",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_95",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_96",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_97",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_98",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_99",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_100",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_101",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_102",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_103",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_104",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_105",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_106",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_107",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_108",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_109",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_110",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_111",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_112",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_113",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_114",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_115",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_116",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_117",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_118",
    "Camp_NIFs_Up_Cross_Sell_SAT_Control_119"]



def get_services_to_drop():

    return ["Serv_CONSUM_MIN",
    "Serv_PRICE_DECO_TV",
    "Serv_PRICE_TV_SVA",
    "Serv_data",
    "Serv_PRICE_PROMO_HBO",
    "Serv_PRICE_DTO_LEV3",
    "Serv_PRICE_FOOTBALL_TV",
    "Serv_FX_TV_LOYALTY",
    "Serv_SERIES",
    "Serv_tariff",
    "Serv_PRICE_TRYBUY_AUTOM_TV",
    "Serv_PRICE_DATA_ADDITIONAL",
    "Serv_TV_LOYALTY",
    "Serv_voice_tariff",
    "Serv_PRICE_HOMEZONE",
    "Serv_PRICE_DATA",
    "Serv_PRICE_TV_ABONOS",
    "Serv_FX_MAPSPASS",
    "Serv_FX_DATA",
    "Serv_PEQUES",
    "Serv_PRICE_JUEGOSPASS",
    "Serv_VIDEOPASS",
    "Serv_PRICE_NETFLIX_NAPSTER",
    "Serv_HBO",
    "Serv_FX_PROMO_FILMIN",
    "Serv_FX_OOB",
    "Serv_PRICE_TV_TARIFF",
    "Serv_FX_PEQUES",
    "Serv_TV_PROMO_USER",
    "Serv_PRICE_CINE",
    "Serv_PRICE_VIDEOPASS",
    "Serv_FX_DECO_TV",
    "Serv_PRICE_SERIES",
    "Serv_PRICE_FILMIN",
    "Serv_DECO_TV",
    "Serv_PRICE_MAPSPASS",
    "Serv_PRICE_SERIEFANS",
    "Serv_PRICE_PROMO_FILMIN",
    "Serv_CINEFANS",
    "Serv_price_dto_lev2",
    "Serv_tipo_sim",
    "Serv_PRICE_ZAPPER_TV",
    "Serv_FX_DTO_LEV3",
    "Serv_FX_DTO_LEV2",
    "Serv_FX_AMAZON",
    "Serv_ROAM_USA_EUR",
    "Serv_PROMO_FILMIN",
    "Serv_FX_ROAM_ZONA_2",
    "Serv_PRICE_DTO_LEV2",
    "Serv_PRICE_DTO_LEV1",
    "Serv_PRICE_TIDAL",
    "Serv_FX_TV_PROMO_USER",
    "Serv_FX_TIDAL",
    "Serv_FX_FBB_UPGRADE",
    "Serv_PRICE_SERIELOVERS",
    "Serv_FX_FILMIN",
    "Serv_FX_FOOTBALL_TV",
    "Serv_PROMO_TIDAL",
    "Serv_PVR_TV",
    "Serv_FX_DOCUMENTALES",
    "Serv_TV_ABONOS",
    "Serv_PRICE_FBB_UPGRADE",
    "Serv_PRICE_ROAM_ZONA_2",
    "Serv_TV_TARIFF",
    "Serv_MAPSPASS",
    "Serv_FX_ZAPPER_TV",
    "Serv_MOTOR_TV",
    "Serv_FX_HBO",
    "Serv_FX_PROMO_HBO",
    "Serv_SOCIALPASS",
    "Serv_OOB",
    "Serv_PROMO_AMAZON",
    "Serv_FX_CINE",
    "Serv_flag_msisdn_err",
    "Serv_PRICE_CHATPASS",
    "Serv_SERIELOVERS",
    "Serv_TV_CUOTA_ALTA",
    "IMSI",
    "Serv_FX_MOTOR_TV",
    "Serv_NUM_SERIE_DECO_TV",
    "Serv_PRICE_DOCUMENTALES",
    "Serv_TV_CUOT_CHARGES",
    "Serv_FX_TRYBUY_TV",
    "Serv_TV_PROMO",
    "Serv_PRICE_MOTOR_TV",
    "Serv_PRICE_TV_PROMO_USER",
    "Serv_ROAMING_BASIC",
    "Serv_FX_CINEFANS",
    "Serv_HOMEZONE",
    "Serv_VIDEOHDPASS",
    "Serv_PRICE_TV_CUOTA_ALTA",
    "Serv_PRICE_SOCIALPASS",
    "Serv_FX_JUEGOSPASS",
    "Serv_ZAPPER_TV",
    "Serv_AMAZON",
    "Serv_PROMO_HBO",
    "Serv_FX_DTO_LEV1",
    "Serv_PRICE_VOICE_TARIFF",
    "Serv_PRICE_PVR_TV",
    "Serv_RGU",
    "Serv_FX_ROAMING_BASIC",
    "Serv_PRICE_TV_LOYALTY",
    "Serv_PRICE_PROMO_AMAZON",
    "Serv_FX_VIDEOHDPASS",
    "Serv_PRICE_AMAZON",
    "Serv_data_additional",
    "Serv_TRYBUY_TV",
    "Serv_CHATPASS",
    "Serv_PRICE_VIDEOHDPASS",
    "Serv_FX_SRV_BASIC",
    "Serv_TV_TOTAL_CHARGES",
    "Serv_FX_SERIEFANS",
    "Serv_dto_lev3",
    "Serv_dto_lev2",
    "Serv_dto_lev1",
    "Serv_PRICE_CINEFANS",
    "Serv_FX_HOMEZONE",
    "Serv_FX_PROMO_TIDAL",
    "Serv_PRICE_MUSICPASS",
    "Serv_FX_SERIES",
    "Serv_PRICE_PEQUES",
    "Serv_TV_SVA",
    "Serv_FX_PVR_TV",
    "Serv_DESC_TARIFF",
    "Serv_FX_TV_SVA",
    "Serv_CINE",
    "Serv_FX_SOCIALPASS",
    "Serv_DESC_SRV_BASIC",
    "Serv_PRICE_CONSUM_MIN",
    "Serv_SRV_BASIC",
    "Serv_FX_MUSICPASS",
    "Serv_FOOTBALL_TV",
    "Serv_sim_vf",
    "Serv_FX_TV_ABONOS",
    "Serv_FX_ROAM_USA_EUR",
    "Serv_MUSICPASS",
    "Serv_PRICE_TV_CUOT_CHARGES",
    "Serv_FX_VIDEOPASS",
    "Serv_FX_CONSUM_MIN",
    "Serv_PRICE_SRV_BASIC",
    "Serv_JUEGOSPASS",
    "Serv_PRICE_TRYBUY_TV",
    "Serv_FILMIN",
    "Serv_SERIEFANS",
    "Serv_PRICE_PROMO_TIDAL",
    "Serv_FX_TRYBUY_AUTOM_TV",
    "Serv_MOBILE_BAM_TOTAL_CHARGES",
    "Serv_PRICE_ROAM_USA_EUR",
    "Serv_FX_SERIELOVERS",
    "Serv_FX_TV_CUOTA_ALTA",
    "Serv_FX_TV_PROMO",
    "Serv_FX_CHATPASS",
    "Serv_PRICE_HBO",
    "Serv_price_dto_lev3",
    "Serv_price_dto_lev1",
    "Serv_PRICE_TV_PROMO",
    "Serv_MOBILE_HOMEZONE",
    "Serv_FX_VOICE_TARIFF",
    "Serv_FBB_UPGRADE",
    "Serv_FX_TV_CUOT_CHARGES",
    "Serv_FX_NETFLIX_NAPSTER",
    "Serv_DOCUMENTALES",
    "Serv_FX_TARIFF",
    "Serv_TIDAL",
    "Serv_ROAM_ZONA_2",
    "Serv_FX_TV_TARIFF",
    "Serv_NETFLIX_NAPSTER",
    "Serv_PRICE_ROAMING_BASIC",
    "Serv_PRICE_TARIFF",
    "Serv_PRICE_OOB",
    "Serv_TRYBUY_AUTOM_TV",
    "Serv_FX_DATA_ADDITIONAL",
    "Serv_FX_PROMO_AMAZON"]


def get_penalties_srv_to_drop():

    return ["Penal_SRV_PENDING_N2_days_to",
    "Penal_SRV_PENDING_N4_end_date",
    "Penal_SRV_FINISHED_N4_desc_promo",
    "Penal_SRV_PENDING_N4_desc_promo",
    "Penal_SRV_PENDING_N2_desc_penal",
    "Penal_SRV_FINISHED_N3_desc_penal",
    "Penal_SRV_FINISHED_N2_desc_promo",
    "Penal_SRV_PENDING_N1_cod_promo",
    "Penal_SRV_PENDING_N5_end_date",
    "Penal_SRV_PENDING_N5_desc_promo",
    "Penal_SRV_FINISHED_N5_cod_penal",
    "Penal_SRV_APPLIED_N2_end_date",
    "Instancia_P",
    "Penal_SRV_FINISHED_N2_cod_penal",
    "Penal_SRV_APPLIED_N5_cod_penal",
    "Penal_SRV_FINISHED_N1_days_to",
    "Penal_SRV_APPLIED_N3_cod_promo",
    "Penal_SRV_FINISHED_N4_cod_penal",
    "Penal_SRV_PENDING_N1_penal_amount",
    "Penal_SRV_APPLIED_N4_desc_promo",
    "Penal_SRV_FINISHED_N4_desc_penal",
    "Penal_SRV_PENDING_N3_days_to",
    "Penal_SRV_FINISHED_N1_end_date",
    "Penal_SRV_FINISHED_N4_days_to",
    "Penal_SRV_FINISHED_N1_start_date",
    "Penal_SRV_FINISHED_N5_desc_promo",
    "Penal_SRV_PENDING_N2_end_date",
    "Penal_SRV_FINISHED_N4_cod_promo",
    "Penal_SRV_APPLIED_N4_start_date",
    "Penal_SRV_FINISHED_N3_desc_promo",
    "Penal_SRV_FINISHED_N2_end_date",
    "Penal_SRV_APPLIED_N5_desc_penal",
    "Penal_SRV_FINISHED_N2_start_date",
    "Penal_SRV_FINISHED_N3_end_date",
    "Penal_SRV_PENDING_N4_cod_promo",
    "Penal_SRV_APPLIED_N4_desc_penal",
    "Penal_SRV_PENDING_N4_start_date",
    "Penal_SRV_APPLIED_N1_cod_penal",
    "Penal_SRV_FINISHED_N2_days_to",
    "Penal_SRV_PENDING_N4_penal_amount",
    "Penal_SRV_PENDING_N1_end_date",
    "Penal_SRV_FINISHED_N1_desc_penal",
    "Penal_SRV_PENDING_N3_cod_promo",
    "Penal_SRV_APPLIED_N4_days_to",
    "Penal_SRV_PENDING_N5_desc_penal",
    "Penal_SRV_PENDING_N3_penal_amount",
    "Penal_SRV_PENDING_N2_cod_penal",
    "Penal_SRV_APPLIED_N2_start_date",
    "Penal_SRV_FINISHED_N1_cod_penal",
    "Penal_SRV_APPLIED_N4_end_date",
    "Penal_SRV_FINISHED_N5_end_date",
    "Penal_SRV_APPLIED_N2_cod_promo",
    "Penal_SRV_APPLIED_N2_cod_penal",
    "Penal_SRV_APPLIED_N3_penal_amount",
    "Penal_SRV_FINISHED_N3_penal_amount",
    "Penal_SRV_APPLIED_N3_end_date",
    "Penal_SRV_FINISHED_N5_penal_amount",
    "Penal_SRV_PENDING_N2_cod_promo",
    "Penal_SRV_FINISHED_N4_start_date",
    "Penal_SRV_FINISHED_N3_cod_penal",
    "Penal_SRV_FINISHED_N4_end_date",
    "Penal_SRV_PENDING_N1_days_to",
    "Penal_SRV_PENDING_N3_end_date",
    "Penal_SRV_FINISHED_N2_penal_amount",
    "Penal_SRV_PENDING_N1_desc_promo",
    "Penal_SRV_FINISHED_N2_desc_penal",
    "Penal_SRV_APPLIED_N1_desc_penal",
    "Penal_SRV_FINISHED_N2_cod_promo",
    "Penal_SRV_PENDING_N5_cod_penal",
    "Penal_SRV_APPLIED_N3_desc_promo",
    "Penal_SRV_APPLIED_N5_start_date",
    "Penal_SRV_PENDING_N1_start_date",
    "Penal_SRV_APPLIED_N5_days_to",
    "Penal_SRV_APPLIED_N2_penal_amount",
    "Penal_SRV_APPLIED_N1_start_date",
    "Penal_SRV_PENDING_N4_cod_penal",
    "Penal_SRV_FINISHED_N3_days_to",
    "Penal_SRV_PENDING_N4_days_to",
    "Penal_SRV_APPLIED_N2_desc_penal",
    "Penal_SRV_FINISHED_N1_penal_amount",
    "Penal_SRV_PENDING_N1_cod_penal",
    "Penal_SRV_FINISHED_N4_penal_amount",
    "Penal_SRV_PENDING_N5_days_to",
    "Penal_SRV_FINISHED_N5_desc_penal",
    "Penal_SRV_APPLIED_N5_cod_promo",
    "Penal_SRV_PENDING_N2_start_date",
    "Penal_SRV_FINISHED_N5_days_to",
    "Penal_SRV_APPLIED_N5_desc_promo",
    "Penal_SRV_APPLIED_N3_cod_penal",
    "Penal_SRV_APPLIED_N5_end_date",
    "Penal_SRV_FINISHED_N3_cod_promo",
    "Penal_SRV_FINISHED_N5_start_date",
    "Penal_SRV_APPLIED_N3_desc_penal",
    "Penal_SRV_PENDING_N3_desc_promo",
    "Penal_SRV_APPLIED_N1_penal_amount",
    "Penal_SRV_APPLIED_N4_penal_amount",
    "Penal_SRV_PENDING_N5_start_date",
    "Penal_SRV_FINISHED_N1_cod_promo",
    "Penal_SRV_FINISHED_N1_desc_promo",
    "Penal_SRV_APPLIED_N2_desc_promo",
    "Penal_SRV_APPLIED_N4_cod_promo",
    "Penal_SRV_APPLIED_N3_start_date",
    "Penal_SRV_APPLIED_N3_days_to",
    "Penal_SRV_PENDING_N5_cod_promo",
    "Penal_SRV_FINISHED_N5_cod_promo",
    "Penal_SRV_APPLIED_N1_end_date",
    "Penal_SRV_PENDING_N4_desc_penal",
    "Penal_SRV_APPLIED_N1_desc_promo",
    "Penal_SRV_APPLIED_N1_days_to",
    "Penal_SRV_APPLIED_N5_penal_amount",
    "Penal_SRV_APPLIED_N1_cod_promo",
    "Penal_SRV_PENDING_N3_desc_penal",
    "Penal_SRV_FINISHED_N3_start_date",
    "Penal_SRV_PENDING_N1_desc_penal",
    "Penal_SRV_APPLIED_N2_days_to",
    "Penal_SRV_PENDING_N3_start_date",
    "Penal_SRV_PENDING_N5_penal_amount",
    "Penal_SRV_APPLIED_N4_cod_penal",
    "Penal_SRV_PENDING_N2_penal_amount",
    "Penal_SRV_PENDING_N3_cod_penal",
    "Penal_SRV_PENDING_N2_desc_promo"]



def get_penalties_cust_to_drop():

    return ["Penal_CUST_PENDING_N2_days_to",
    "Penal_CUST_PENDING_N1_cod_penal",
    "Penal_CUST_PENDING_N5_start_date",
    "Penal_CUST_APPLIED_N5_desc_penal",
    "Penal_CUST_APPLIED_N4_desc_penal",
    "Penal_CUST_APPLIED_N4_cod_penal",
    "Penal_CUST_APPLIED_N2_days_to",
    "Penal_CUST_PENDING_N3_cod_penal",
    "Penal_CUST_FINISHED_N4_start_date",
    "Penal_CUST_PENDING_N5_desc_penal",
    "Penal_CUST_APPLIED_N2_start_date",
    "Penal_CUST_FINISHED_N1_cod_penal",
    "Penal_CUST_FINISHED_N5_penal_amount",
    "Penal_CUST_PENDING_N1_cod_promo",
    "Penal_CUST_PENDING_N3_desc_promo",
    "Penal_CUST_FINISHED_N4_end_date",
    "Penal_CUST_FINISHED_N3_days_to",
    "Penal_CUST_APPLIED_N2_penal_amount",
    "Penal_CUST_PENDING_N3_end_date",
    "Penal_CUST_PENDING_N5_days_to",
    "Penal_CUST_FINISHED_N2_penal_amount",
    "Penal_CUST_APPLIED_N1_penal_amount",
    "Penal_CUST_APPLIED_N4_penal_amount",
    "Penal_CUST_FINISHED_N2_desc_penal",
    "Penal_CUST_APPLIED_N1_cod_promo",
    "Penal_CUST_APPLIED_N5_cod_penal",
    "Penal_CUST_PENDING_N3_days_to",
    "Penal_CUST_APPLIED_N4_days_to",
    "Penal_CUST_APPLIED_N1_desc_penal",
    "Penal_CUST_APPLIED_N3_cod_promo",
    "Penal_CUST_FINISHED_N2_cod_promo",
    "Penal_CUST_FINISHED_N5_days_to",
    "Penal_CUST_APPLIED_N5_start_date",
    "Penal_CUST_APPLIED_N5_end_date",
    "Penal_CUST_PENDING_N1_start_date",
    "Penal_CUST_FINISHED_N5_end_date",
    "Penal_CUST_APPLIED_N3_desc_promo",
    "Penal_CUST_FINISHED_N1_end_date",
    "Penal_CUST_APPLIED_N1_start_date",
    "Penal_CUST_FINISHED_N5_desc_penal",
    "Penal_CUST_APPLIED_N5_penal_amount",
    "Penal_CUST_FINISHED_N1_penal_amount",
    "Penal_CUST_APPLIED_N2_desc_penal",
    "Penal_CUST_PENDING_N5_penal_amount",
    "Penal_CUST_FINISHED_N2_end_date",
    "Penal_CUST_FINISHED_N4_penal_amount",
    "Penal_CUST_PENDING_N2_penal_amount",
    "Penal_CUST_FINISHED_N3_end_date",
    "Penal_CUST_PENDING_N4_cod_promo",
    "Penal_CUST_APPLIED_N3_days_to",
    "Penal_CUST_APPLIED_N1_cod_penal",
    "Penal_CUST_FINISHED_N5_start_date",
    "Penal_CUST_PENDING_N3_cod_promo",
    "Penal_CUST_PENDING_N2_cod_penal",
    "Penal_CUST_APPLIED_N1_end_date",
    "Penal_CUST_APPLIED_N2_cod_promo",
    "Penal_CUST_APPLIED_N2_cod_penal",
    "Penal_CUST_APPLIED_N5_desc_promo",
    "Penal_CUST_FINISHED_N1_desc_promo",
    "Penal_CUST_PENDING_N1_days_to",
    "Penal_CUST_FINISHED_N5_desc_promo",
    "Penal_CUST_APPLIED_N3_desc_penal",
    "Penal_CUST_PENDING_N4_end_date",
    "Penal_CUST_FINISHED_N1_cod_promo",
    "Penal_CUST_APPLIED_N2_desc_promo",
    "Penal_CUST_PENDING_N5_end_date",
    "Penal_CUST_FINISHED_N3_penal_amount",
    "Penal_CUST_FINISHED_N3_start_date",
    "Penal_CUST_APPLIED_N3_start_date",
    "Penal_CUST_APPLIED_N2_end_date",
    "Penal_CUST_PENDING_N1_penal_amount",
    "Penal_CUST_FINISHED_N5_cod_promo",
    "Penal_CUST_PENDING_N4_desc_penal",
    "Penal_CUST_FINISHED_N3_cod_penal",
    "Penal_CUST_APPLIED_N1_desc_promo",
    "Penal_CUST_PENDING_N5_cod_penal",
    "Penal_CUST_PENDING_N4_days_to",
    "Penal_CUST_PENDING_N3_desc_penal",
    "Penal_CUST_PENDING_N1_desc_penal",
    "Penal_CUST_FINISHED_N4_desc_promo",
    "Penal_CUST_PENDING_N2_start_date",
    "Penal_CUST_FINISHED_N3_desc_penal",
    "Penal_CUST_PENDING_N3_start_date",
    "Penal_CUST_FINISHED_N3_desc_promo",
    "Penal_CUST_FINISHED_N2_desc_promo",
    "Penal_CUST_PENDING_N4_cod_penal",
    "Penal_CUST_APPLIED_N5_days_to",
    "Penal_CUST_APPLIED_N3_cod_penal",
    "Penal_CUST_PENDING_N4_start_date",
    "Penal_CUST_PENDING_N2_desc_promo",
    "Penal_CUST_PENDING_N4_penal_amount",
    "Penal_CUST_PENDING_N4_desc_promo",
    "Penal_CUST_PENDING_N2_desc_penal",
    "Penal_CUST_PENDING_N3_penal_amount",
    "Penal_CUST_PENDING_N5_desc_promo",
    "Penal_CUST_PENDING_N1_desc_promo",
    "Penal_CUST_APPLIED_N3_penal_amount",
    "Penal_CUST_FINISHED_N5_cod_penal",
    "Penal_CUST_FINISHED_N4_days_to",
    "Penal_CUST_PENDING_N2_end_date",
    "Penal_CUST_APPLIED_N5_cod_promo",
    "Penal_CUST_FINISHED_N2_days_to",
    "Penal_CUST_FINISHED_N2_cod_penal",
    "Penal_CUST_PENDING_N1_end_date",
    "Penal_CUST_FINISHED_N1_days_to",
    "Penal_CUST_FINISHED_N1_start_date",
    "Penal_CUST_FINISHED_N4_desc_penal",
    "Penal_CUST_APPLIED_N4_end_date",
    "Penal_CUST_FINISHED_N4_cod_penal",
    "Penal_CUST_PENDING_N5_cod_promo",
    "Penal_CUST_APPLIED_N4_cod_promo",
    "Penal_CUST_APPLIED_N3_end_date",
    "Penal_CUST_APPLIED_N1_days_to",
    "Penal_CUST_APPLIED_N4_desc_promo",
    "Penal_CUST_FINISHED_N2_start_date",
    "Penal_CUST_FINISHED_N3_cod_promo",
    "Penal_CUST_FINISHED_N1_desc_penal"]


def get_gnv_data_to_drop():
    return ["GNV_Data_hour_22_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_21_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_20_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_20_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_0_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_20_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_16_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_14_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_12_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_8_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_11_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_20_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_16_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_0_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_0_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_15_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_8_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_22_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_8_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_19_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_21_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_19_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_4_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_18_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_10_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_0_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_8_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_22_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_17_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_8_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_1_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_5_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_15_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_19_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_20_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_20_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_19_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_5_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_16_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_16_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_2_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_4_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_11_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_13_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_21_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_1_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_16_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_3_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_18_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_5_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_6_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_15_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_7_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_14_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_5_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_3_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_12_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_2_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_8_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_12_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_5_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_14_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_0_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_4_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_11_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_20_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_19_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_13_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_19_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_19_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_10_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_16_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_12_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_10_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_18_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_8_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_20_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_19_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_5_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_9_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_10_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_22_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_21_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_22_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_5_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_9_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_6_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_4_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_13_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_18_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_0_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_18_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_3_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_6_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_13_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_23_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_16_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_17_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_16_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_17_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_1_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_18_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_8_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_0_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_22_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_9_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_7_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_8_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_17_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_12_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_13_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_19_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_19_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_7_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_17_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_3_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_1_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_1_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_8_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_21_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_12_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_8_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_11_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_18_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_23_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_8_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_9_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_2_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_0_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_3_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_17_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_23_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_1_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_7_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_23_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_4_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_1_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_21_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_4_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_23_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_13_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_1_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_10_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_5_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_6_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_11_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_4_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_13_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_3_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_0_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_3_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_4_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_7_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_15_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_16_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_18_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_9_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_15_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_23_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_20_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_14_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_6_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_8_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_4_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_2_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_10_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_21_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_14_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_8_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_0_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_6_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_14_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_4_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_2_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_15_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_6_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_7_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_22_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_2_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_13_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_8_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_14_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_0_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_7_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_9_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_2_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_4_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_15_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_20_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_5_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_5_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_9_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_8_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_10_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_1_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_1_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_12_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_21_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_14_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_14_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_4_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_6_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_5_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_14_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_8_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_12_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_16_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_2_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_11_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_22_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_23_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_11_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_10_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_5_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_22_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_22_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_7_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_11_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_14_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_16_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_4_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_20_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_8_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_14_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_1_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_15_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_7_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_0_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_15_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_18_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_4_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_10_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_16_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_7_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_0_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_5_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_19_WE_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_17_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_19_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_8_WE_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_3_WE_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_21_W_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_3_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_11_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_2_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_14_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_8_W_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_15_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_2_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_W_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_21_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_14_WE_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_9_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_17_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_WE_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_12_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_6_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_WE_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_19_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_1_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_16_WE_Music_Pass_Data_Volume_MB",
    "GNV_Data_hour_11_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_8_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_1_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_10_WE_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_11_W_RegularData_Num_Of_Connections",
    "GNV_Data_hour_7_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_1_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_13_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_16_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_13_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_9_W_Video_Pass_Num_Of_Connections",
    "GNV_Data_hour_6_WE_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_9_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_1_W_VideoHD_Pass_Num_Of_Connections",
    "GNV_Data_hour_0_WE_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_20_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_13_WE_RegularData_Num_Of_Connections",
    "GNV_Data_hour_7_W_RegularData_Data_Volume_MB",
    "GNV_Data_hour_11_W_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_7_W_Chat_Zero_Num_Of_Connections",
    "GNV_Data_hour_22_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_15_W_Maps_Pass_Data_Volume_MB",
    "GNV_Data_hour_18_W_VideoHD_Pass_Data_Volume_MB",
    "GNV_Data_hour_23_WE_Social_Pass_Num_Of_Connections",
    "GNV_Data_hour_12_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_13_W_MasMegas_Num_Of_Connections",
    "GNV_Data_hour_16_WE_Social_Pass_Data_Volume_MB",
    "GNV_Data_hour_6_WE_Music_Pass_Num_Of_Connections",
    "GNV_Data_hour_2_WE_Maps_Pass_Num_Of_Connections",
    "GNV_Data_hour_23_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_4_W_Video_Pass_Data_Volume_MB",
    "GNV_Data_hour_9_W_Chat_Zero_Data_Volume_MB",
    "GNV_Data_hour_4_W_MasMegas_Data_Volume_MB",
    "GNV_Data_hour_9_WE_RegularData_Data_Volume_MB",
    "GNV_Data_hour_14_WE_RegularData_Num_Of_Connections",
    "GNV_Data_L2_data_per_connection_W_12",
    "GNV_Data_L2_total_connections_WE_8",
    "GNV_Data_L2_total_connections_WE_6",
    "GNV_Data_L2_total_connections_WE_7",
    "GNV_Data_L2_total_connections_WE_4",
    "GNV_Data_L2_total_connections_WE_5",
    "GNV_Data_L2_total_connections_WE_2",
    "GNV_Data_L2_total_connections_WE_3",
    "GNV_Data_L2_total_connections_WE_0",
    "GNV_Data_L2_total_connections_WE_1",
    "GNV_Data_L2_total_data_volume_W_2",
    "GNV_Data_L2_total_data_volume_W_3",
    "GNV_Data_L2_total_data_volume_W_0",
    "GNV_Data_L2_total_data_volume_W_1",
    "GNV_Data_L2_total_data_volume_W_6",
    "GNV_Data_L2_total_data_volume_W_7",
    "GNV_Data_L2_total_data_volume_W_4",
    "GNV_Data_L2_total_data_volume_W_5",
    "GNV_Data_L2_total_data_volume_W_8",
    "GNV_Data_L2_total_data_volume_W_9",
    "GNV_Data_L2_total_data_volume_W_21",
    "GNV_Data_L2_total_data_volume_W_20",
    "GNV_Data_L2_total_data_volume_W_23",
    "GNV_Data_L2_total_data_volume_W_22",
    "GNV_Data_L2_data_per_connection_WE_19",
    "GNV_Data_L2_data_per_connection_WE_12",
    "GNV_Data_L2_data_per_connection_WE_16",
    "GNV_Data_L2_total_connections_W_21",
    "GNV_Data_L2_total_data_volume_W_14",
    "GNV_Data_L2_total_data_volume_W_15",
    "GNV_Data_L2_total_data_volume_W_16",
    "GNV_Data_L2_total_data_volume_W_17",
    "GNV_Data_L2_total_data_volume_W_10",
    "GNV_Data_L2_total_data_volume_W_11",
    "GNV_Data_L2_total_data_volume_W_12",
    "GNV_Data_L2_total_data_volume_W_13",
    "GNV_Data_L2_total_data_volume_W_18",
    "GNV_Data_L2_total_data_volume_W_19",
    "GNV_Data_L2_data_per_connection_W_20",
    "GNV_Data_L2_total_connections_WE_14",
    "GNV_Data_L2_total_connections_WE_15",
    "GNV_Data_L2_total_connections_WE_16",
    "GNV_Data_L2_total_connections_WE_10",
    "GNV_Data_L2_total_connections_WE_11",
    "GNV_Data_L2_total_connections_WE_12",
    "GNV_Data_L2_total_connections_WE_13",
    "GNV_Data_L2_total_connections_WE_18",
    "GNV_Data_L2_total_connections_WE_19",
    "GNV_Data_L2_total_connections_W_1",
    "GNV_Data_L2_total_connections_WE_21",
    "GNV_Data_L2_total_connections_WE_20",
    "GNV_Data_L2_total_connections_WE_23",
    "GNV_Data_L2_total_connections_WE_22",
    "GNV_Data_L2_total_connections_WE_9",
    "GNV_Data_L2_total_connections_WE_17",
    "GNV_Data_L2_total_data_volume_WE_7",
    "GNV_Data_L2_data_per_connection_W_21",
    "GNV_Data_L2_data_per_connection_W_22",
    "GNV_Data_L2_data_per_connection_W_23",
    "GNV_Data_L2_total_data_volume_WE_23",
    "GNV_Data_L2_total_data_volume_WE_21",
    "GNV_Data_L2_total_data_volume_WE_20",
    "GNV_Data_L2_data_per_connection_W_11",
    "GNV_Data_L2_data_per_connection_W_10",
    "GNV_Data_L2_data_per_connection_W_13",
    "GNV_Data_L2_total_connections_W_19",
    "GNV_Data_L2_data_per_connection_W_14",
    "GNV_Data_L2_data_per_connection_W_17",
    "GNV_Data_L2_data_per_connection_W_16",
    "GNV_Data_L2_data_per_connection_W_19",
    "GNV_Data_L2_data_per_connection_W_18",
    "GNV_Data_L2_total_connections_W_13",
    "GNV_Data_L2_total_data_volume_WE_12",
    "GNV_Data_L2_total_data_volume_WE_13",
    "GNV_Data_L2_total_data_volume_WE_10",
    "GNV_Data_L2_total_data_volume_WE_11",
    "GNV_Data_L2_total_data_volume_WE_16",
    "GNV_Data_L2_total_data_volume_WE_17",
    "GNV_Data_L2_total_data_volume_WE_14",
    "GNV_Data_L2_total_data_volume_WE_15",
    "GNV_Data_L2_total_data_volume_WE_18",
    "GNV_Data_L2_data_per_connection_WE_18",
    "GNV_Data_L2_data_per_connection_WE_11",
    "GNV_Data_L2_data_per_connection_WE_10",
    "GNV_Data_L2_data_per_connection_WE_13",
    "GNV_Data_L2_data_per_connection_WE_15",
    "GNV_Data_L2_data_per_connection_WE_14",
    "GNV_Data_L2_data_per_connection_WE_17",
    "GNV_Data_L2_total_connections_W_8",
    "GNV_Data_L2_total_connections_W_9",
    "GNV_Data_L2_total_connections_W_0",
    "GNV_Data_L2_total_connections_W_2",
    "GNV_Data_L2_total_connections_W_3",
    "GNV_Data_L2_total_connections_W_4",
    "GNV_Data_L2_total_connections_W_5",
    "GNV_Data_L2_total_connections_W_6",
    "GNV_Data_L2_total_connections_W_7",
    "GNV_Data_L2_data_per_connection_WE_21",
    "GNV_Data_L2_data_per_connection_WE_22",
    "GNV_Data_L2_data_per_connection_WE_23",
    "GNV_Data_L2_data_per_connection_WE_1",
    "GNV_Data_L2_data_per_connection_WE_3",
    "GNV_Data_L2_data_per_connection_WE_5",
    "GNV_Data_L2_data_per_connection_WE_7",
    "GNV_Data_L2_total_connections_W_18",
    "GNV_Data_L2_total_connections_W_12",
    "GNV_Data_L2_total_connections_W_10",
    "GNV_Data_L2_total_connections_W_11",
    "GNV_Data_L2_total_connections_W_16",
    "GNV_Data_L2_total_connections_W_17",
    "GNV_Data_L2_total_connections_W_14",
    "GNV_Data_L2_total_connections_W_15",
    "GNV_Data_L2_total_data_volume_WE_22",
    "GNV_Data_L2_data_per_connection_WE_20",
    "GNV_Data_L2_data_per_connection_W_15",
    "GNV_Data_L2_data_per_connection_W_5",
    "GNV_Data_L2_data_per_connection_W_4",
    "GNV_Data_L2_data_per_connection_W_7",
    "GNV_Data_L2_data_per_connection_W_6",
    "GNV_Data_L2_data_per_connection_W_1",
    "GNV_Data_L2_data_per_connection_W_0",
    "GNV_Data_L2_data_per_connection_W_3",
    "GNV_Data_L2_data_per_connection_W_9",
    "GNV_Data_L2_data_per_connection_W_2",
    "GNV_Data_L2_data_per_connection_W_8",
    "GNV_Data_L2_total_connections_W_23",
    "GNV_Data_L2_total_connections_W_22",
    "GNV_Data_L2_total_connections_W_20",
    "GNV_Data_L2_total_data_volume_WE_8",
    "GNV_Data_L2_total_data_volume_WE_9",
    "GNV_Data_L2_total_data_volume_WE_0",
    "GNV_Data_L2_total_data_volume_WE_1",
    "GNV_Data_L2_total_data_volume_WE_2",
    "GNV_Data_L2_total_data_volume_WE_3",
    "GNV_Data_L2_total_data_volume_WE_4",
    "GNV_Data_L2_total_data_volume_WE_5",
    "GNV_Data_L2_total_data_volume_WE_6",
    "GNV_Data_L2_data_per_connection_WE_0",
    "GNV_Data_L2_data_per_connection_WE_2",
    "GNV_Data_L2_data_per_connection_WE_4",
    "GNV_Data_L2_data_per_connection_WE_6",
    "GNV_Data_L2_data_per_connection_WE_9",
    "GNV_Data_L2_data_per_connection_WE_8",
    "GNV_Data_L2_total_data_volume_WE_19"]



def get_gnv_voice_to_drop():

    return ["GNV_Voice_hour_22_W_MOU",
    "GNV_Voice_hour_17_W_MOU",
    "GNV_Voice_hour_19_WE_MOU",
    "GNV_Voice_hour_22_WE_Num_Of_Calls",
    "GNV_Voice_hour_19_W_MOU",
    "GNV_Voice_hour_19_WE_Num_Of_Calls",
    "GNV_Voice_hour_8_W_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_we_0",
    "GNV_Voice_L2_mou_per_call_we_1",
    "GNV_Voice_L2_mou_per_call_we_2",
    "GNV_Voice_L2_mou_per_call_we_3",
    "GNV_Voice_L2_mou_per_call_we_5",
    "GNV_Voice_L2_mou_per_call_we_7",
    "GNV_Voice_L2_mou_per_call_we_8",
    "GNV_Voice_L2_mou_per_call_we_9",
    "GNV_Voice_hour_2_W_MOU",
    "GNV_Voice_hour_9_WE_MOU",
    "GNV_Voice_hour_4_W_MOU",
    "GNV_Voice_hour_10_W_MOU",
    "GNV_Voice_hour_7_WE_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_we_17",
    "GNV_Voice_hour_2_WE_MOU",
    "GNV_Voice_hour_0_WE_MOU",
    "GNV_Voice_hour_9_WE_Num_Of_Calls",
    "GNV_Voice_hour_3_WE_MOU",
    "GNV_Voice_hour_7_W_MOU",
    "GNV_Voice_hour_20_WE_MOU",
    "GNV_Voice_hour_12_WE_MOU",
    "GNV_Voice_hour_10_W_Num_Of_Calls",
    "GNV_Voice_hour_14_W_MOU",
    "GNV_Voice_hour_8_W_MOU",
    "GNV_Voice_hour_18_W_MOU",
    "GNV_Voice_hour_6_WE_Num_Of_Calls",
    "GNV_Voice_hour_1_WE_Num_Of_Calls",
    "GNV_Voice_hour_3_WE_Num_Of_Calls",
    "GNV_Voice_hour_23_WE_Num_Of_Calls",
    "GNV_Voice_hour_1_WE_MOU",
    "GNV_Voice_hour_3_W_MOU",
    "GNV_Voice_hour_13_WE_MOU",
    "GNV_Voice_hour_1_W_Num_Of_Calls",
    "GNV_Voice_hour_20_W_Num_Of_Calls",
    "GNV_Voice_hour_0_WE_Num_Of_Calls",
    "GNV_Voice_hour_16_WE_MOU",
    "GNV_Voice_hour_11_W_MOU",
    "GNV_Voice_hour_10_WE_MOU",
    "GNV_Voice_hour_5_WE_MOU",
    "GNV_Voice_hour_10_WE_Num_Of_Calls",
    "GNV_Voice_hour_23_WE_MOU",
    "GNV_Voice_hour_11_WE_Num_Of_Calls",
    "GNV_Voice_hour_4_WE_MOU",
    "GNV_Voice_hour_1_W_MOU",
    "GNV_Voice_hour_15_W_MOU",
    "GNV_Voice_hour_14_WE_Num_Of_Calls",
    "GNV_Voice_hour_15_WE_Num_Of_Calls",
    "GNV_Voice_hour_14_WE_MOU",
    "GNV_Voice_hour_23_W_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_we_20",
    "GNV_Voice_hour_2_W_Num_Of_Calls",
    "GNV_Voice_hour_12_W_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_we_4",
    "GNV_Voice_L2_mou_per_call_we_6",
    "GNV_Voice_hour_20_W_MOU",
    "GNV_Voice_hour_2_WE_Num_Of_Calls",
    "GNV_Voice_hour_21_W_MOU",
    "GNV_Voice_L2_mou_per_call_w_21",
    "GNV_Voice_L2_mou_per_call_w_20",
    "GNV_Voice_L2_mou_per_call_w_23",
    "GNV_Voice_L2_mou_per_call_w_22",
    "GNV_Voice_hour_11_WE_MOU",
    "GNV_Voice_hour_22_WE_MOU",
    "GNV_Voice_hour_13_WE_Num_Of_Calls",
    "GNV_Voice_hour_0_W_MOU",
    "GNV_Voice_hour_19_W_Num_Of_Calls",
    "GNV_Voice_hour_22_W_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_w_18",
    "GNV_Voice_L2_mou_per_call_w_19",
    "GNV_Voice_L2_mou_per_call_w_14",
    "GNV_Voice_L2_mou_per_call_w_15",
    "GNV_Voice_L2_mou_per_call_w_16",
    "GNV_Voice_L2_mou_per_call_w_17",
    "GNV_Voice_L2_mou_per_call_w_10",
    "GNV_Voice_L2_mou_per_call_w_11",
    "GNV_Voice_L2_mou_per_call_w_12",
    "GNV_Voice_L2_mou_per_call_w_13",
    "GNV_Voice_hour_16_WE_Num_Of_Calls",
    "GNV_Voice_hour_18_WE_MOU",
    "GNV_Voice_hour_14_W_Num_Of_Calls",
    "GNV_Voice_hour_5_W_MOU",
    "GNV_Voice_hour_4_WE_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_w_8",
    "GNV_Voice_L2_mou_per_call_w_9",
    "GNV_Voice_L2_mou_per_call_w_2",
    "GNV_Voice_L2_mou_per_call_w_3",
    "GNV_Voice_L2_mou_per_call_w_0",
    "GNV_Voice_L2_mou_per_call_w_1",
    "GNV_Voice_L2_mou_per_call_w_7",
    "GNV_Voice_L2_mou_per_call_w_4",
    "GNV_Voice_L2_mou_per_call_w_5",
    "GNV_Voice_hour_21_WE_MOU",
    "GNV_Voice_hour_5_W_Num_Of_Calls",
    "GNV_Voice_hour_21_WE_Num_Of_Calls",
    "GNV_Voice_hour_6_W_Num_Of_Calls",
    "GNV_Voice_hour_17_W_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_we_23",
    "GNV_Voice_L2_mou_per_call_we_22",
    "GNV_Voice_L2_mou_per_call_we_21",
    "GNV_Voice_hour_15_W_Num_Of_Calls",
    "GNV_Voice_hour_5_WE_Num_Of_Calls",
    "GNV_Voice_hour_4_W_Num_Of_Calls",
    "GNV_Voice_hour_15_WE_MOU",
    "GNV_Voice_hour_16_W_MOU",
    "GNV_Voice_hour_8_WE_MOU",
    "GNV_Voice_hour_0_W_Num_Of_Calls",
    "GNV_Voice_hour_7_WE_MOU",
    "GNV_Voice_hour_17_WE_MOU",
    "GNV_Voice_hour_7_W_Num_Of_Calls",
    "GNV_Voice_hour_9_W_MOU",
    "GNV_Voice_hour_16_W_Num_Of_Calls",
    "GNV_Voice_hour_12_WE_Num_Of_Calls",
    "GNV_Voice_hour_17_WE_Num_Of_Calls",
    "GNV_Voice_hour_12_W_MOU",
    "GNV_Voice_L2_mou_per_call_we_12",
    "GNV_Voice_L2_mou_per_call_we_13",
    "GNV_Voice_L2_mou_per_call_we_10",
    "GNV_Voice_L2_mou_per_call_we_11",
    "GNV_Voice_L2_mou_per_call_we_16",
    "GNV_Voice_L2_mou_per_call_we_14",
    "GNV_Voice_L2_mou_per_call_we_15",
    "GNV_Voice_L2_mou_per_call_we_18",
    "GNV_Voice_L2_mou_per_call_we_19",
    "GNV_Voice_hour_6_W_MOU",
    "GNV_Voice_hour_9_W_Num_Of_Calls",
    "GNV_Voice_hour_8_WE_Num_Of_Calls",
    "GNV_Voice_hour_21_W_Num_Of_Calls",
    "GNV_Voice_hour_13_W_MOU",
    "GNV_Voice_hour_3_W_Num_Of_Calls",
    "GNV_Voice_hour_20_WE_Num_Of_Calls",
    "GNV_Voice_L2_mou_per_call_w_6",
    "GNV_Voice_hour_18_W_Num_Of_Calls",
    "GNV_Voice_hour_11_W_Num_Of_Calls",
    "GNV_Voice_hour_18_WE_Num_Of_Calls",
    "GNV_Voice_hour_13_W_Num_Of_Calls",
    "GNV_Voice_hour_6_WE_MOU",
    "GNV_Voice_hour_23_W_MOU"]


def get_num_vars(ids):

    print('[Info]: Get numerical variables of the ids...')

    numerical = getNumerical(ids)

    print('[Info]: Get non informative variables to drop...')

    no_input_feats = get_no_input_feats()
    non_inf_feats = get_noninf_features()
    id_feats = get_id_features()
    campaigns_to_drop = get_campaigns_to_drop()
    services_to_drop = get_services_to_drop()
    penalties_srv_to_drop = get_penalties_srv_to_drop()
    penalties_cust_to_drop = get_penalties_cust_to_drop()
    gnv_data_to_drop = get_gnv_data_to_drop()
    gnv_voice_to_drop = get_gnv_voice_to_drop()

    tnps = [col for col in ids.columns if col.startswith('TNPS')]
    address = [col for col in ids.columns if col.startswith('address')]
    ord_sla = [col for col in ids.columns if col.startswith('Ord_sla')]
    ccc = [col for col in ids.columns if col.startswith('CCC')]

    vars_to_drop = no_input_feats + non_inf_feats + id_feats + campaigns_to_drop + services_to_drop + penalties_srv_to_drop + penalties_cust_to_drop + gnv_data_to_drop + gnv_voice_to_drop + tnps +ccc + address + ord_sla


    print('[Info]: Select variables for the model')

    var_final = [variable for variable in numerical if variable not in vars_to_drop]

    print('[Info]: Number of input variables: ', len(var_final))

    return var_final



def drop_corr_vars_85():


    corr_vars=["Cust_L2_days_until_next_bill"
  , "Cust_Agg_fixed_services_nc"
  , "Cust_Agg_football_services"
  , "Cust_Agg_motor_services"
  , "Cust_Agg_total_price_motor"
  , "Cust_Agg_pvr_services"
  , "Cust_Agg_total_price_pvr"
  , "Cust_Agg_zapper_services"
  , "Cust_Agg_total_price_zapper"
 , "Cust_Agg_trybuy_services"
 , "Cust_Agg_trybuy_autom_services"
 , "Cust_Agg_num_tariff_maslineasmini"
 , "Cust_Agg_total_tv_total_charges"
 , "Cust_Agg_L2_mobile_fx_first_days_since_nif"
 , "GNV_Data_L2_total_connections_WE"
 , "GNV_Data_L2_total_data_volume"
 , "GNV_Data_L2_total_connections"
 , "GNV_Data_L2_data_per_connection"
 , "GNV_Type_Voice_LLAMADAS_VPN_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_ALEMANIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_COSTA_RICA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_CROACIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_DINAMARCA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_EL_SALVADOR_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_GEORGIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_GIBRALTAR_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_HONG_KONG_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_ISLANDIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_ISLAS_CAIMANES_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_ITALIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_KUWAIT_MOU"
 , "GNV_Type_Voice_INTERNACIONALES_KUWAIT_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_MEJICO_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_PANAMA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_REPUBLICA_ESLOVACA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_TUNEZ_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_TURQUIA_Num_Of_Calls"
 , "GNV_Type_Voice_INTERNACIONALES_UCRANIA_Num_Of_Calls"
 , "GNV_Type_Voice_MOVILES_AIRE_Num_Of_Calls"
 , "GNV_Type_Voice_MOVILES_PEPEPHONE_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_AMENA_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_CRUZ_ROJA_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_TELE2_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_VALENCIA_CABLE_Num_Of_Calls"
 , "GNV_Type_Voice_SERVICIOS_GRATUITOS_XTRA_TELECOM_SA_Num_Of_Calls"
 , "GNV_Type_Voice_OTROS_DESTINOS_OTHER_MOU"
 , "GNV_Type_Voice_OTROS_DESTINOS_ADM_GENERAL_ESTADO_Num_Of_Calls"
 , "GNV_Type_Voice_OTROS_DESTINOS_ATENCION_CIUDADANA_Num_Of_Calls"
 , "GNV_Type_Voice_OTROS_DESTINOS_LINEA_902_Num_Of_Calls"
 , "GNV_Type_Voice_OTROS_DESTINOS_LINEA_905_SOPORTE_Num_Of_Calls"
 , "Bill_N1_Amount_To_Pay"
 , "Bill_N2_Amount_To_Pay"
 , "Bill_N2_num_ids_fict"
 , "Bill_N3_Amount_To_Pay"
 , "Bill_N3_num_ids_fict"
 , "Bill_N4_num_ids_fict"
 , "Bill_N4_num_facturas"
 , "Bill_N5_num_ids_fict"
 , "Bill_L2_n1_net"
 , "Bill_L2_n2_net"
 , "Bill_L2_n3_net"
 , "Bill_L2_N1_N2_Amount_To_Pay"
 , "Bill_L2_N1_N3_Amount_To_Pay"
 , "Bill_L2_N1_N4_Amount_To_Pay"
 , "Bill_L2_N1_N5_Amount_To_Pay"
 , "Bill_L2_N2_N3_Amount_To_Pay"
 , "Bill_L2_N3_N4_Amount_To_Pay"
 , "Bill_L2_N4_N5_Amount_To_Pay"
 , "Camp_L2_srv_total_camps_Retention_Voice"
 , "Order_L2_N4_StartDate_days_since"
 , "Order_L2_N5_StartDate_days_since"
 , "Order_L2_N6_StartDate_days_since"
 , "Order_L2_N7_StartDate_days_since"
 , "Order_L2_avg_time_bw_orders"
 , "Order_days_since_last_order"
 , "Order_avg_days_per_order"
 , "Penal_L2_CUST_PENDING_N3_end_date_days_until"
 , "Penal_L2_CUST_PENDING_N4_end_date_days_until"
 , "Penal_L2_CUST_PENDING_N5_end_date_days_until"
 , "Penal_L2_CUST_PENDING_end_date_total_max_days_until"
 , "Penal_L2_SRV_PENDING_N3_end_date_days_until"
 , "Penal_L2_SRV_PENDING_N4_end_date_days_until"
 , "Penal_L2_SRV_PENDING_N5_end_date_days_until"
 , "PER_PREFS_TRAFFIC"
 , "Spinners_nif_avg_days_since_port"
 , "netscout_ns_apps_alipay_total_effective_ul_mb"
 , "netscout_ns_apps_apple_timestamps"
 , "netscout_ns_apps_facebook_total_effective_dl_mb"
 , "netscout_ns_apps_foursquare_total_effective_dl_mb"
 , "netscout_ns_apps_gmail_total_effective_dl_mb"
 , "netscout_ns_apps_googleplay_days"
 , "netscout_ns_apps_hacking_timestamps"
 , "netscout_ns_apps_itunes_days"
 , "netscout_ns_apps_itunes_timestamps"
 , "netscout_ns_apps_legal_total_effective_dl_mb"
, "netscout_ns_apps_linkedin_total_effective_dl_mb"
, "netscout_ns_apps_shopping_timestamps"
, "netscout_ns_apps_tumblr_total_effective_dl_mb"
, "netscout_ns_apps_video_timestamps"
, "netscout_ns_apps_web_jazztel_https_total_effective_dl_mb"
, "netscout_ns_apps_web_jazztel_https_timestamps"
, "netscout_ns_apps_web_lowi_http_total_effective_dl_mb"
, "netscout_ns_apps_web_lowi_https_total_effective_ul_mb"
, "netscout_ns_apps_web_lowi_https_timestamps"
, "netscout_ns_apps_web_o2_https_total_effective_ul_mb"
, "netscout_ns_apps_web_o2_https_timestamps"
, "netscout_ns_apps_web_pepephone_https_timestamps"
, "netscout_ns_apps_web_yoigo_https_days"
, "netscout_ns_apps_web_yomvi_http_total_effective_ul_mb"
, "netscout_ns_apps_web_yomvi_http_timestamps"
, "netscout_ns_apps_web_yomvi_https_total_effective_ul_mb"
, "netscout_ns_apps_wechat_total_effective_ul_mb"
, "netscout_ns_apps_wechat_timestamps"
, "netscout_ns_apps_whatsapp_voice_calling_total_effective_ul_mb"
, "netscout_ns_apps_youtube_days"
, "netscout_ns_apps_youtube_timestamps"
, "tgs_days_until_f_fin_bi"
, "tgs_days_until_fecha_fin_dto"
, "Reimbursement_adjustment_debt"
, "Comp_PEPEPHONE_max_count"
, "Comp_PEPEPHONE_min_days_since_navigation"
, "Comp_ORANGE_min_days_since_navigation"
, "Comp_JAZZTEL_sum_count"
, "Comp_JAZZTEL_max_days_since_navigation"
, "Comp_JAZZTEL_min_days_since_navigation"
, "Comp_MOVISTAR_min_days_since_navigation"
, "Comp_MASMOVIL_max_days_since_navigation"
, "Comp_MASMOVIL_min_days_since_navigation"
, "Comp_YOIGO_max_count"
, "Comp_YOIGO_max_days_since_navigation"
, "Comp_YOIGO_min_days_since_navigation"
, "Comp_VODAFONE_min_days_since_navigation"
, "Comp_LOWI_sum_count"
, "Comp_LOWI_max_days_since_navigation"
, "Comp_LOWI_min_days_since_navigation"
, "Comp_LOWI_distinct_days_with_navigation"
, "Comp_O2_max_count"
, "Comp_O2_max_days_since_navigation"
, "Comp_O2_min_days_since_navigation"
, "Comp_unknown_max_days_since_navigation"
, "Comp_unknown_min_days_since_navigation"
, "Comp_sum_count_comps"
, "Comp_max_count_comps"
, "Tickets_max_time_opened_tipo_tramitacion"
, "Tickets_max_time_opened_tipo_reclamacion"
, "Tickets_max_time_opened_tipo_averia"
, "Tickets_max_time_opened_tipo_incidencia"
, "Tickets_min_time_opened_tipo_tramitacion"
, "Tickets_min_time_opened_tipo_reclamacion"
, "Tickets_min_time_opened_tipo_averia"
, "Tickets_min_time_opened_tipo_incidencia"
, "Tickets_std_time_opened_tipo_tramitacion"
, "Tickets_std_time_opened_tipo_reclamacion"
, "Tickets_std_time_opened_tipo_averia"
, "Tickets_std_time_opened_tipo_incidencia"
, "Tickets_mean_time_opened_tipo_tramitacion"
, "Tickets_mean_time_opened_tipo_reclamacion"
, "Tickets_mean_time_opened_tipo_averia"
, "Tickets_mean_time_opened_tipo_incidencia"
, "Tickets_mean_time_opened"
, "Tickets_max_time_opened"
, "Tickets_min_time_opened"
, "Tickets_std_time_opened"
, "Tickets_num_tickets_tipo_tramitacion"
, "Tickets_num_tickets_tipo_averia"
, "Cust_L2_days_since_fecha_migracion"
, "Cust_Agg_fbb_services_nc"
, "Cust_Agg_mobile_services_nc"
, "Cust_Agg_tv_services_nc"
, "Cust_Agg_fbb_services_nif"
, "Cust_Agg_bam_services_nc"
, "Cust_Agg_num_football_nc"
, "Cust_Agg_max_dias_desde_fx_football_tv"
, "Cust_Agg_max_dias_desde_fx_motor_tv"
, "Cust_Agg_max_dias_desde_fx_pvr_tv"
, "Cust_Agg_max_dias_desde_fx_zapper_tv"
, "Cust_Agg_max_dias_desde_fx_trybuy_tv"
, "Cust_Agg_L2_bam_fx_first_days_since_nc"
, "Cust_Agg_L2_fbb_fx_first_days_since_nc"
, "Cust_Agg_L2_fixed_fx_first_days_since_nc"
, "Cust_Agg_L2_tv_fx_first_days_since_nc"
, "GNV_Voice_L2_total_mou_w"
, "GNV_Voice_L2_total_mou_we"
, "GNV_Voice_L2_hour_max_mou_WE"
, "GNV_Data_L2_total_data_volume_W"
, "GNV_Data_L2_total_data_volume_WE"
, "GNV_Data_L2_data_per_connection_W"
, "GNV_Data_L2_data_per_connection_WE"
, "GNV_Type_Voice_INTERNACIONALES_ANGOLA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BANGLADESH_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BELICE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BIELORRUSIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BOTSWANA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BURKINA_FASO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CHAD_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CHIPRE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_COSTA_DE_MARFIL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ARABIA_SAUDITA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_EGIPTO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GAMBIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GHANA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUADALUPE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_IRAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_COOK_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISRAEL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_JAPON_MOU"
, "GNV_Type_Voice_INTERNACIONALES_KENYA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MADAGASCAR_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MARTINICA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MAURITANIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MOLDAVIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MOZAMBIQUE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_NUEVA_CALEDONIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_REUNION_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SINGAPUR_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SUDAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MOZAMBIQUE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SUDAFRICA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TAILANDIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ZAIRE_MOU"
, "GNV_Type_Voice_MOVILES_DIA_MOU"
, "GNV_Type_Voice_MOVILES_ORBITEL_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_ASESOR_COMERCIAL_EMPRESAS_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_BOMBEROS_PROV_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_EMERGENCIA_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_ATENCION_AL_CLIENTE_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_MOU"
, "Bill_N1_InvoiceCharges"
, "Bill_N1_num_ids_fict"
, "Bill_N1_Debt_Amount"
, "Bill_N2_InvoiceCharges"
, "Bill_N2_Debt_Amount"
, "Bill_N3_InvoiceCharges"
, "Bill_N4_InvoiceCharges"
, "Bill_N4_Amount_To_Pay"
, "Bill_N3_Debt_Amount"
, "Bill_N5_InvoiceCharges"
, "Bill_N4_Debt_Amount"
, "Bill_N5_Amount_To_Pay"
, "Bill_N4_net_charges"
, "Bill_N5_net_charges"
, "Order_L2_N8_StartDate_days_since"
, "Order_L2_N9_StartDate_days_since"
, "Order_L2_N1_StartDate_days_since"
, "Order_L2_nb_orders"
, "Order_L2_N10_StartDate_days_since"
, "Penal_L2_CUST_PENDING_N1_penal_amount"
, "Penal_L2_CUST_PENDING_N2_penal_amount"
, "Penal_L2_CUST_PENDING_N3_penal_amount"
, "Penal_L2_CUST_PENDING_N4_penal_amount"
, "Penal_L2_CUST_PENDING_N5_penal_amount"
, "Penal_L2_CUST_PENDING_N1_end_date_days_until"
, "Penal_L2_SRV_PENDING_N1_end_date_days_until"
, "device_tenure_days_from_n1"
, "Spinners_nif_max_days_since_port"
, "Spinners_nif_port_number"
, "Spinners_nif_distinct_msisdn"
, "Spinners_yoigo_ACAN"
, "netscout_ns_apps_accuweather_data_mb"
, "netscout_ns_apps_adult_data_mb"
, "netscout_ns_apps_adultweb_data_mb"
, "netscout_ns_apps_alcohol_data_mb"
, "netscout_ns_apps_alibaba_data_mb"
, "netscout_ns_apps_alipay_data_mb"
, "netscout_ns_apps_amazon_data_mb"
, "netscout_ns_apps_apple_data_mb"
, "netscout_ns_apps_arts_data_mb"
, "netscout_ns_apps_astrology_data_mb"
, "netscout_ns_apps_audio_data_mb"
, "netscout_ns_apps_audio_days"
, "netscout_ns_apps_auto_data_mb"
, "netscout_ns_apps_badoo_data_mb"
, "netscout_ns_apps_badoo_days"
, "netscout_ns_apps_baidu_data_mb"
, "netscout_ns_apps_baidu_days"
, "netscout_ns_apps_bbc_data_mb"
, "netscout_ns_apps_booking_data_mb"
, "netscout_ns_apps_books_data_mb"
, "netscout_ns_apps_business_data_mb"
, "netscout_ns_apps_chats_data_mb"
, "netscout_ns_apps_classified_data_mb"
, "netscout_ns_apps_dating_data_mb"
, "netscout_ns_apps_ebay_data_mb"
, "netscout_ns_apps_education_data_mb"
, "netscout_ns_apps_facebookmessages_data_mb"
, "netscout_ns_apps_facebook_days"
, "netscout_ns_apps_facebook_timestamps"
, "netscout_ns_apps_facebook_video_data_mb"
, "netscout_ns_apps_facebook_video_total_effective_dl_mb"
, "netscout_ns_apps_facebook_video_days"
, "netscout_ns_apps_family_data_mb"
, "netscout_ns_apps_fashion_data_mb"
, "netscout_ns_apps_finance_data_mb"
, "netscout_ns_apps_food_data_mb"
, "netscout_ns_apps_gambling_data_mb"
, "netscout_ns_apps_github_data_mb"
, "netscout_ns_apps_gmail_days"
, "netscout_ns_apps_googledrive_data_mb"
, "netscout_ns_apps_googleearth_data_mb"
, "netscout_ns_apps_googlemaps_data_mb"
, "netscout_ns_apps_googleplay_data_mb"
, "netscout_ns_apps_groupon_data_mb"
, "netscout_ns_apps_groupon_days"
, "netscout_ns_apps_hacking_data_mb"
, "netscout_ns_apps_home_data_mb"
, "netscout_ns_apps_instagram_data_mb"
, "netscout_ns_apps_itunes_data_mb"
, "netscout_ns_apps_jobs_data_mb"
, "netscout_ns_apps_kids_data_mb"
, "netscout_ns_apps_line_data_mb"
, "netscout_ns_apps_linkedin_data_mb"
, "netscout_ns_apps_medical_data_mb"
, "netscout_ns_apps_music_data_mb"
, "netscout_ns_apps_netflix_data_mb"
, "netscout_ns_apps_netflixvideo_data_mb"
, "netscout_ns_apps_netflix_total_effective_ul_mb"
, "netscout_ns_apps_netflix_timestamps"
, "netscout_ns_apps_news_data_mb"
, "netscout_ns_apps_paypal_data_mb"
, "netscout_ns_apps_pets_data_mb"
, "netscout_ns_apps_pinterest_data_mb"
, "netscout_ns_apps_politics_data_mb"
, "netscout_ns_apps_pregnancy_data_mb"
, "netscout_ns_apps_qq_data_mb"
, "netscout_ns_apps_reddit_data_mb"
, "netscout_ns_apps_samsung_data_mb"
, "netscout_ns_apps_samsung_days"
, "netscout_ns_apps_science_data_mb"
, "netscout_ns_apps_shopping_data_mb"
, "netscout_ns_apps_shopping_days"
, "netscout_ns_apps_skype_data_mb"
, "netscout_ns_apps_snapchat_data_mb"
, "netscout_ns_apps_socialnetwork_data_mb"
, "netscout_ns_apps_sports_data_mb"
, "netscout_ns_apps_spotify_data_mb"
, "netscout_ns_apps_spotify_days"
, "netscout_ns_apps_steam_data_mb"
, "netscout_ns_apps_taobao_data_mb"
, "netscout_ns_apps_technology_data_mb"
, "netscout_ns_apps_travel_data_mb"
, "netscout_ns_apps_twitch_data_mb"
, "netscout_ns_apps_twitter_data_mb"
, "netscout_ns_apps_video_data_mb"
, "netscout_ns_apps_video_total_effective_dl_mb"
, "netscout_ns_apps_videostreaming_data_mb"
, "netscout_ns_apps_vimeo_data_mb"
, "netscout_ns_apps_violence_data_mb"
, "netscout_ns_apps_webgames_data_mb"
, "netscout_ns_apps_webmobile_data_mb"
, "netscout_ns_apps_web_jazztel_http_data_mb"
, "netscout_ns_apps_web_jazztel_http_timestamps"
, "netscout_ns_apps_web_lowi_http_data_mb"
, "netscout_ns_apps_web_lowi_https_data_mb"
, "netscout_ns_apps_web_masmovil_http_data_mb"
, "netscout_ns_apps_web_masmovil_https_data_mb"
, "netscout_ns_apps_web_masmovil_http_timestamps"
, "netscout_ns_apps_web_movistar_http_data_mb"
, "netscout_ns_apps_web_movistar_https_data_mb"
, "netscout_ns_apps_web_o2_http_data_mb"
, "netscout_ns_apps_web_o2_http_days"
, "netscout_ns_apps_web_o2_https_data_mb"
, "netscout_ns_apps_web_o2_https_days"
, "netscout_ns_apps_web_orange_http_data_mb"
, "netscout_ns_apps_web_orange_http_total_effective_dl_mb"
, "netscout_ns_apps_web_orange_https_data_mb"
, "netscout_ns_apps_web_pepephone_http_data_mb"
, "netscout_ns_apps_web_pepephone_https_data_mb"
, "netscout_ns_apps_web_vodafone_http_data_mb"
, "netscout_ns_apps_web_vodafone_https_data_mb"
, "netscout_ns_apps_web_yoigo_http_data_mb"
, "netscout_ns_apps_web_yoigo_https_data_mb"
, "netscout_ns_apps_web_yoigo_http_timestamps"
, "netscout_ns_apps_sports_total_effective_dl_mb"
, "netscout_ns_apps_web_yomvi_http_data_mb"
, "netscout_ns_apps_web_yomvi_https_data_mb"
, "netscout_ns_apps_web_yomvi_https_days"
, "netscout_ns_apps_wechat_data_mb"
, "netscout_ns_apps_whatsapp_data_mb"
, "netscout_ns_apps_whatsapp_media_message_data_mb"
, "netscout_ns_apps_whatsapp_media_message_days"
, "netscout_ns_apps_whatsapp_voice_calling_data_mb"
, "netscout_ns_apps_wikipedia_data_mb"
, "netscout_ns_apps_yandex_data_mb"
, "netscout_ns_apps_youtube_data_mb"
, "netscout_ns_apps_zynga_data_mb"
, "tgs_days_since_f_inicio_dto"
, "Reimbursement_days_since"
, "netscout_ns_apps_web_pepephone_https_total_effective_dl_mb"
, "Comp_PEPEPHONE_max_days_since_navigation"
, "netscout_ns_apps_web_movistar_https_timestamps"
, "Comp_ORANGE_max_days_since_navigation"
, "Comp_MOVISTAR_max_days_since_navigation"
, "netscout_ns_apps_web_o2_https_total_effective_dl_mb"
, "Comp_VODAFONE_sum_count"
, "Comp_VODAFONE_distinct_days_with_navigation"
, "Comp_sum_distinct_days_with_navigation_vdf"
, "Comp_sum_distinct_days_with_navigation_comps"
, "Comp_min_days_since_navigation_comps"
, "Comp_max_days_since_navigation_comps"
, "Tickets_max_time_closed_tipo_tramitacion"
, "Tickets_max_time_closed_tipo_reclamacion"
, "Tickets_max_time_closed_tipo_averia"
, "Tickets_max_time_closed_tipo_incidencia"
, "Tickets_min_time_closed_tipo_tramitacion"
, "Tickets_min_time_closed_tipo_reclamacion"
, "Tickets_min_time_closed_tipo_averia"
, "Tickets_min_time_closed_tipo_incidencia"
, "Tickets_std_time_closed_tipo_tramitacion"
, "Tickets_std_time_closed_tipo_reclamacion"
, "Tickets_std_time_closed_tipo_averia"
, "Tickets_std_time_closed_tipo_incidencia"
, "Tickets_mean_time_closed_tipo_tramitacion"
, "Tickets_mean_time_closed_tipo_reclamacion"
, "Tickets_mean_time_closed_tipo_averia"
, "Cust_Agg_bam_mobile_services_nc"
, "Cust_Agg_bam_mobile_services_nif"
, "Cust_Agg_total_football_price_nc"
, "Cust_Agg_total_football_price_nif"
, "Cust_Agg_total_price_football"
, "Cust_Agg_total_price_trybuy"
, "Cust_Agg_total_price_trybuy_autom"
, "Cust_Agg_num_tariff_unknown"
, "Cust_Agg_L2_bam_mobile_fx_first_days_since_nc"
, "Cust_Agg_L2_bam_mobile_fx_first_days_since_nif"
, "GNV_Type_Voice_INTERNACIONALES_AFGANISTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_AFGANISTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ALASKA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ALASKA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ANGUILLA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ANGUILLA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ANTIGUA_Y_BARBUDA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ANTIGUA_Y_BARBUDA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ANTILLAS_HOLANDESAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ANTILLAS_HOLANDESAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ARMENIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ARMENIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ARUBA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ARUBA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_AZERBAIYAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_AZERBAIYAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BAHAMAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BAHAMAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BAHREIN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BAHREIN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BARBADOS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BARBADOS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BENIN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BENIN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BERMUDAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BERMUDAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BHUTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BHUTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BIRMANIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BIRMANIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOSTAR_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOSTAR_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_SRPSKE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BOSNIA_SRPSKE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_BRUNEI_MOU"
, "GNV_Type_Voice_INTERNACIONALES_BRUNEI_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_CABO_VERDE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CABO_VERDE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_CAMBOYA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CAMBOYA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_CAMERUN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CAMERUN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_CONGO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_CONGO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ECUADOR__PORTA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ECUADOR__PORTA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_MOVIL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_MOVIL_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ETIOPIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ETIOPIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_FILIPINAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_FILIPINAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_FIYI_MOU"
, "GNV_Type_Voice_INTERNACIONALES_FIYI_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GABON_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GABON_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUAM_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUAM_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUATEMALA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUATEMALA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUAYANA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUAYANA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUAYANA_FRANCESA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUAYANA_FRANCESA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUINEA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUINEA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_GUINEABISSAU_MOU"
, "GNV_Type_Voice_INTERNACIONALES_GUINEABISSAU_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_HAITI_MOU"
, "GNV_Type_Voice_INTERNACIONALES_HAITI_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_HAWAI_MOU"
, "GNV_Type_Voice_INTERNACIONALES_HAWAI_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_AMERICANAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_AMERICANAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_BRITANICAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_BRITANICAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_INDONESIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_INDONESIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_IRAQ_MOU"
, "GNV_Type_Voice_INTERNACIONALES_IRAQ_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ISLA_NIUE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLA_NIUE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_FEROE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_FEROE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_MALVINAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_MALVINAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_MARSHALL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_MARSHALL_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_SALOMON_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ISLAS_SALOMON_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_JAMAICA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_JAMAICA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_JORDANIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_JORDANIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_KAZAJASTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_KAZAJASTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_KIRGUIZISTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_KIRGUIZISTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_KOSOVO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_KOSOVO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LIBANO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LIBANO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LAOS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LAOS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LESOTHO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LESOTHO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LIBERIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LIBERIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LIBIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LIBIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_LIECHTENSTEIN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_LIECHTENSTEIN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MONACO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MONACO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MACAO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MACAO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MACEDONIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MACEDONIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MALASIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MALASIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MALDIVAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MALDIVAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MAURICIO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MAURICIO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MAYOTTE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MAYOTTE_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MONGOLIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MONGOLIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_MONTENEGRO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_MONTENEGRO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_NAMIBIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_NAMIBIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_NEPAL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_NEPAL_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_NIGER_MOU"
, "GNV_Type_Voice_INTERNACIONALES_NIGER_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_OMAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_OMAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_PALAU_MOU"
, "GNV_Type_Voice_INTERNACIONALES_PALAU_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_PALESTINA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_PALESTINA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_PAPUA_NUEVA_GUINEA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_PAPUA_NUEVA_GUINEA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_POLINESIA_FRANCESA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_POLINESIA_FRANCESA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_REPUBLICA_CENTROAFRICANA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_REPUBLICA_CENTROAFRICANA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_RUANDA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_RUANDA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SAN_MARINO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SAN_MARINO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SAN_MARTIN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SAN_MARTIN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SAN_PEDRO_Y_MIQUELON_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SAN_PEDRO_Y_MIQUELON_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SAN_VICENTE_Y_GRANADINAS_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SAN_VICENTE_Y_GRANADINAS_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SANTO_TOME_Y_PRINCIPE_MOVIL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SANTO_TOME_Y_PRINCIPE_MOVIL_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SERBIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SERBIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SEYCHELLES_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SEYCHELLES_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SIERRA_LEONA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SIERRA_LEONA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SIRIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SIRIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SOMALIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SOMALIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_STA_LUCIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_STA_LUCIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SURINAM_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SURINAM_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_SWAZILANDIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_SWAZILANDIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TADJIKISTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TADJIKISTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TAIWAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TAIWAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TANZANIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TANZANIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TIMOR_ORIENTAL_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TIMOR_ORIENTAL_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TOGO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TOGO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TRINIDAD_Y_TOBAGO_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TRINIDAD_Y_TOBAGO_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_TURKMENISTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_TURKMENISTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_UGANDA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_UGANDA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_UZBEKISTAN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_UZBEKISTAN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_VIETNAM_MOU"
, "GNV_Type_Voice_INTERNACIONALES_VIETNAM_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_YEMEN_MOU"
, "GNV_Type_Voice_INTERNACIONALES_YEMEN_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ZAMBIA_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ZAMBIA_Num_Of_Calls"
, "GNV_Type_Voice_INTERNACIONALES_ZIMBABWE_MOU"
, "GNV_Type_Voice_INTERNACIONALES_ZIMBABWE_Num_Of_Calls"
, "GNV_Type_Voice_MOVILES_BARABLU_MOU"
, "GNV_Type_Voice_MOVILES_BARABLU_Num_Of_Calls"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_MOU"
, "GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_Num_Of_Calls"
, "GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_MOU"
, "GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_Num_Of_Calls"
, "Camp_NIFs_Retention_Voice_EMA_Universal_0"
, "Camp_NIFs_Retention_Voice_EMA_Universal_1"
, "Camp_NIFs_Retention_Voice_TEL_Target_0"
, "Camp_NIFs_Retention_Voice_TEL_Target_1"
, "Camp_NIFs_Retention_Voice_TEL_Control_0"
, "Camp_NIFs_Retention_Voice_TEL_Control_1"
, "Camp_NIFs_Retention_Voice_TEL_Universal_0"
, "Camp_NIFs_Retention_Voice_TEL_Universal_1"
, "Camp_NIFs_Retention_Voice_SMS_Universal_0"
, "Camp_NIFs_Retention_Voice_SMS_Universal_1"
, "Camp_NIFs_Retention_Voice_MMS_Target_0"
, "Camp_NIFs_Retention_Voice_MMS_Target_1"
, "Camp_NIFs_Retention_Voice_MMS_Control_0"
, "Camp_NIFs_Retention_Voice_MMS_Control_1"
, "Camp_NIFs_Retention_Voice_MMS_Universal_0"
, "Camp_NIFs_Retention_Voice_MMS_Universal_1"
, "Camp_NIFs_Up_Cross_Sell_EMA_Universal_0"
, "Camp_NIFs_Up_Cross_Sell_EMA_Universal_1"
, "Camp_NIFs_Up_Cross_Sell_SAT_Target_0"
, "Camp_NIFs_Up_Cross_Sell_SAT_Target_1"
, "Camp_NIFs_Up_Cross_Sell_SAT_Universal_0"
, "Camp_NIFs_Up_Cross_Sell_SAT_Universal_1"
, "Camp_NIFs_Up_Cross_Sell_SMS_Target_0"
, "Camp_NIFs_Up_Cross_Sell_SMS_Universal_1"
, "Camp_NIFs_Up_Cross_Sell_MMS_Universal_0"
, "Camp_NIFs_Up_Cross_Sell_MMS_Universal_1"
, "Camp_L2_nif_total_camps_Control"
, "Camp_L2_nif_pcg_redem_Control"
, "Camp_L2_nif_total_camps_Universal"
, "Camp_L2_nif_pcg_redem_Universal"
, "Camp_L2_nif_total_camps_Retention_Voice"
, "Camp_L2_nif_pcg_redem_Retention_Voice"
, "Camp_L2_nif_pcg_redem_Up_Cross_Sell"
, "Camp_L2_nif_total_camps_EMA"
, "Camp_L2_nif_pcg_redem_EMA"
, "Camp_L2_nif_pcg_redem_TEL"
, "Camp_L2_nif_pcg_redem_SAT"
, "Camp_L2_nif_total_camps_SMS"
, "Camp_L2_nif_pcg_redem_MMS"
, "Camp_SRV_Retention_Voice_EMA_Universal_0"
, "Camp_SRV_Retention_Voice_EMA_Universal_1"
, "Camp_SRV_Retention_Voice_SMS_Target_0"
, "Camp_SRV_Retention_Voice_MMS_Target_0"
, "Camp_SRV_Retention_Voice_MMS_Target_1"
, "Camp_SRV_Retention_Voice_MMS_Control_0"
, "Camp_SRV_Retention_Voice_MMS_Control_1"
, "Camp_SRV_Retention_Voice_MMS_Universal_0"
, "Camp_SRV_Retention_Voice_MMS_Universal_1"
, "Camp_SRV_Up_Cross_Sell_EMA_Universal_1"
, "Camp_SRV_Up_Cross_Sell_SAT_Target_0"
, "Camp_SRV_Up_Cross_Sell_SAT_Target_1"
, "Camp_SRV_Up_Cross_Sell_SAT_Control_0"
, "Camp_SRV_Up_Cross_Sell_SAT_Control_1"
, "Camp_SRV_Up_Cross_Sell_SAT_Universal_0"
, "Camp_SRV_Up_Cross_Sell_SAT_Universal_1"
, "Camp_SRV_Up_Cross_Sell_SMS_Target_0"
, "Camp_SRV_Up_Cross_Sell_MMS_Universal_0"
, "Camp_SRV_Up_Cross_Sell_MMS_Universal_1"
, "Camp_L2_srv_pcg_redem_Control"
, "Camp_L2_srv_total_camps_Universal"
, "Camp_L2_srv_pcg_redem_Universal"
, "Camp_L2_srv_pcg_redem_Retention_Voice"
, "Camp_L2_srv_pcg_redem_Up_Cross_Sell"
, "Camp_L2_srv_total_camps_EMA"
, "Camp_L2_srv_pcg_redem_EMA"
, "Camp_L2_srv_pcg_redem_TEL"
, "Camp_L2_srv_pcg_redem_SAT"
, "Camp_L2_srv_total_camps_SMS"
, "Camp_L2_srv_pcg_redem_MMS"
, "Order_Agg_Activa_Promo_CAN_orders"
, "Order_Agg_Descon_Prepago_CAN_orders"
, "Order_Agg_Descon_Prepago_COM_orders"
, "Order_Agg_Descon_Prepago_PEN_orders"
, "Order_Agg_Factura_Canal_Presencial_CAN_orders"
, "Order_Agg_Factura_Terminal_CAN_orders"
, "Order_Agg_Factura_Terminal_PEN_orders"
, "Order_Agg_Fideliza_cliente_PEN_orders"
, "Order_Agg_Instalacion_SecureNet_CAN_orders"
, "Order_Agg_Instalacion_SecureNet_COM_orders"
, "Order_Agg_Instalacion_SecureNet_PEN_orders"
, "PER_PREFS_LORTAD"
, "Spinners_movistar_PCAN"
, "Spinners_movistar_AACE"
, "Spinners_movistar_AENV"
, "Spinners_simyo_PCAN"
, "Spinners_simyo_AACE"
, "Spinners_simyo_AENV"
, "Spinners_orange_PCAN"
, "Spinners_orange_AACE"
, "Spinners_jazztel_PCAN"
, "Spinners_jazztel_AACE"
, "Spinners_jazztel_AENV"
, "Spinners_yoigo_AACE"
, "Spinners_masmovil_PCAN"
, "Spinners_masmovil_AACE"
, "Spinners_masmovil_AENV"
, "Spinners_pepephone_PCAN"
, "Spinners_pepephone_AACE"
, "Spinners_pepephone_AENV"
, "Spinners_reuskal_ACON"
, "Spinners_reuskal_ASOL"
, "Spinners_reuskal_PCAN"
, "Spinners_reuskal_ACAN"
, "Spinners_reuskal_AACE"
, "Spinners_reuskal_AENV"
, "Spinners_reuskal_APOR"
, "Spinners_reuskal_AREC"
, "Spinners_unknown_PCAN"
, "Spinners_unknown_AACE"
, "Spinners_otros_AACE"
, "Spinners_otros_AENV"
, "Spinners_total_reuskal"
, "netscout_ns_apps_web_jazztel_data_mb"
, "netscout_ns_apps_web_jazztel_total_effective_dl_mb"
, "netscout_ns_apps_web_jazztel_total_effective_ul_mb"
, "netscout_ns_apps_web_jazztel_days"
, "netscout_ns_apps_web_jazztel_timestamps"
, "netscout_ns_apps_web_lowi_data_mb"
, "netscout_ns_apps_web_lowi_total_effective_dl_mb"
, "netscout_ns_apps_web_lowi_total_effective_ul_mb"
, "netscout_ns_apps_web_lowi_days"
, "netscout_ns_apps_web_lowi_timestamps"
, "netscout_ns_apps_web_marcacom_http_data_mb"
, "netscout_ns_apps_web_marcacom_http_total_effective_dl_mb"
, "netscout_ns_apps_web_marcacom_http_total_effective_ul_mb"
, "netscout_ns_apps_web_marcacom_http_days"
, "netscout_ns_apps_web_marcacom_http_timestamps"
, "netscout_ns_apps_web_marcacom_https_data_mb"
, "netscout_ns_apps_web_marcacom_https_total_effective_dl_mb"
, "netscout_ns_apps_web_marcacom_https_total_effective_ul_mb"
, "netscout_ns_apps_web_marcacom_https_days"
, "netscout_ns_apps_web_marcacom_https_timestamps"
, "netscout_ns_apps_web_masmovil_data_mb"
, "netscout_ns_apps_web_masmovil_total_effective_dl_mb"
, "netscout_ns_apps_web_masmovil_total_effective_ul_mb"
, "netscout_ns_apps_web_masmovil_days"
, "netscout_ns_apps_web_masmovil_timestamps"
, "netscout_ns_apps_web_movistar_data_mb"
, "netscout_ns_apps_web_movistar_total_effective_dl_mb"
, "netscout_ns_apps_web_movistar_total_effective_ul_mb"
, "netscout_ns_apps_web_movistar_days"
, "netscout_ns_apps_web_movistar_timestamps"
, "netscout_ns_apps_web_o2_data_mb"
, "netscout_ns_apps_web_o2_total_effective_dl_mb"
, "netscout_ns_apps_web_o2_total_effective_ul_mb"
, "netscout_ns_apps_web_o2_days"
, "netscout_ns_apps_web_o2_timestamps"
, "netscout_ns_apps_web_orange_data_mb"
, "netscout_ns_apps_web_orange_total_effective_dl_mb"
, "netscout_ns_apps_web_orange_total_effective_ul_mb"
, "netscout_ns_apps_web_orange_days"
, "netscout_ns_apps_web_orange_timestamps"
, "netscout_ns_apps_web_pepephone_data_mb"
, "netscout_ns_apps_web_pepephone_total_effective_dl_mb"
, "netscout_ns_apps_web_pepephone_total_effective_ul_mb"
, "netscout_ns_apps_web_pepephone_days"
, "netscout_ns_apps_web_pepephone_timestamps"
, "netscout_ns_apps_web_vodafone_data_mb"
, "netscout_ns_apps_web_vodafone_total_effective_dl_mb"
, "netscout_ns_apps_web_vodafone_total_effective_ul_mb"
, "netscout_ns_apps_web_vodafone_days"
, "netscout_ns_apps_web_vodafone_timestamps"
, "netscout_ns_apps_web_yoigo_data_mb"
, "netscout_ns_apps_web_yoigo_total_effective_dl_mb"
, "netscout_ns_apps_web_yoigo_total_effective_ul_mb"
, "netscout_ns_apps_web_yoigo_days"
, "netscout_ns_apps_web_yoigo_timestamps"
, "tgs_sum_ind_under_use"
, "tgs_sum_ind_over_use"
, "tgs_days_since_f_inicio_bi_exp"
, "tgs_days_until_f_fin_bi_exp"
, "Pbms_srv_num_reclamaciones_ini_w2"
, "Pbms_srv_num_reclamaciones_prev_w2"
, "Pbms_srv_num_reclamaciones_w2vsw2"
, "Pbms_srv_num_reclamaciones_ini_w4"
, "Pbms_srv_num_reclamaciones_prev_w4"
, "Pbms_srv_num_reclamaciones_w4vsw4"
, "Pbms_srv_num_reclamaciones_ini_w8"
, "Pbms_srv_num_reclamaciones_prev_w8"
, "Pbms_srv_num_reclamaciones_w8vsw8"
, "Pbms_srv_num_averias_ini_w2"
, "Pbms_srv_num_averias_prev_w2"
, "Pbms_srv_num_averias_w2vsw2"
, "Pbms_srv_num_averias_ini_w4"
, "Pbms_srv_num_averias_prev_w4"
, "Pbms_srv_num_averias_w4vsw4"
, "Pbms_srv_num_averias_ini_w8"
, "Pbms_srv_num_averias_prev_w8"
, "Pbms_srv_num_averias_w8vsw8"
, "Pbms_srv_num_soporte_tecnico_ini_w2"
, "Pbms_srv_num_soporte_tecnico_prev_w2"
, "Pbms_srv_num_soporte_tecnico_w2vsw2"
, "Pbms_srv_num_soporte_tecnico_ini_w4"
, "Pbms_srv_num_soporte_tecnico_prev_w4"
, "Pbms_srv_num_soporte_tecnico_w4vsw4"
, "Pbms_srv_num_soporte_tecnico_ini_w8"
, "Pbms_srv_num_soporte_tecnico_prev_w8"
, "Pbms_srv_num_soporte_tecnico_w8vsw8"
, "Pbms_srv_ind_reclamaciones"
, "Pbms_srv_ind_averias"
, "Pbms_srv_ind_soporte"
, "NUM_RECLAMACIONES_NIF_ini_w2"
, "NUM_RECLAMACIONES_NIF_prev_w2"
, "NUM_RECLAMACIONES_NIF_w2vsw2"
, "NUM_RECLAMACIONES_NIF_ini_w4"
, "NUM_RECLAMACIONES_NIF_prev_w4"
, "NUM_RECLAMACIONES_NIF_w4vsw4"
, "NUM_RECLAMACIONES_NIF_ini_w8"
, "NUM_RECLAMACIONES_NIF_prev_w8"
, "NUM_RECLAMACIONES_NIF_w8vsw8"
, "NUM_AVERIAS_NIF_ini_w2"
, "NUM_AVERIAS_NIF_prev_w2"
, "NUM_AVERIAS_NIF_w2vsw2"
, "NUM_AVERIAS_NIF_ini_w4"
, "NUM_AVERIAS_NIF_prev_w4"
, "NUM_AVERIAS_NIF_w4vsw4"
, "NUM_AVERIAS_NIF_ini_w8"
, "NUM_AVERIAS_NIF_prev_w8"
, "NUM_AVERIAS_NIF_w8vsw8"
, "NUM_SOPORTE_TECNICO_NIF_ini_w2"
, "NUM_SOPORTE_TECNICO_NIF_prev_w2"
, "NUM_SOPORTE_TECNICO_NIF_w2vsw2"
, "NUM_SOPORTE_TECNICO_NIF_ini_w4"
, "NUM_SOPORTE_TECNICO_NIF_prev_w4"
, "NUM_SOPORTE_TECNICO_NIF_w4vsw4"
, "NUM_SOPORTE_TECNICO_NIF_ini_w8"
, "NUM_SOPORTE_TECNICO_NIF_prev_w8"
, "NUM_SOPORTE_TECNICO_NIF_w8vsw8"
, "Reimbursement_num_n8"
, "Reimbursement_num_n6"
, "Reimbursement_num_n4"
, "Reimbursement_num_n5"
, "Reimbursement_num_n2"
, "Reimbursement_num_n3"
, "Reimbursement_num_month_2"
, "Reimbursement_num_n7"
, "Reimbursement_num_n1"
, "Reimbursement_num_month_1"
, "Comp_unknown_sum_count"
, "Comp_unknown_max_count"
, "Comp_unknown_distinct_days_with_navigation"
, "Tickets_num_tickets_tipo_incidencia_opened"
, "Tickets_num_tickets_tipo_incidencia_closed"
, "Tickets_weeks_averias"
, "Tickets_weeks_facturacion"
, "Tickets_weeks_reclamacion"
, "Tickets_weeks_incidencias"
, "Tickets_num_tickets_tipo_incidencia"]

    return corr_vars

def drop_corr_vars_70_new():

    corr_vars=['GNV_Voice_evening_W_Num_Of_Calls',
'GNV_Voice_L2_max_num_calls_W',
'GNV_Voice_L2_max_num_calls_WE',
'GNV_Data_morning_WE_VideoHD_Pass_Num_Of_Connections',
'GNV_Data_evening_W_VideoHD_Pass_Num_Of_Connections',
'GNV_Data_evening_W_Video_Pass_Num_Of_Connections',
'GNV_Data_evening_W_Social_Pass_Num_Of_Connections',
'GNV_Data_evening_WE_VideoHD_Pass_Num_Of_Connections',
'GNV_Data_evening_WE_Social_Pass_Num_Of_Connections',
'GNV_Data_night_W_VideoHD_Pass_Num_Of_Connections',
'GNV_Data_night_W_Social_Pass_Num_Of_Connections',
'GNV_Data_night_W_RegularData_Num_Of_Connections',
'GNV_Data_night_WE_VideoHD_Pass_Num_Of_Connections',
'GNV_Data_night_WE_Social_Pass_Num_Of_Connections',
'GNV_Data_night_WE_RegularData_Num_Of_Connections',
'GNV_Data_L2_total_connections_W_morning',
'GNV_Data_L2_total_connections_W_evening',
'GNV_Data_L2_total_connections_W_night',
'GNV_Data_L2_total_connections_WE_morning',
'GNV_Data_L2_total_connections_WE_evening',
'GNV_Data_L2_total_connections_WE_night',
'GNV_Data_L2_max_connections_W',
'GNV_Data_L2_max_connections_WE',
'GNV_Type_Voice_INTERNACIONALES_OTHER_COUNTRY_Num_Of_Calls',
'Bill_N1_num_facturas',
'Bill_N2_num_facturas',
'Bill_N3_num_ids_fict',
'Bill_N4_num_ids_fict',
'Bill_N4_num_facturas',
'Bill_N5_num_ids_fict',
'Bill_N5_num_facturas',
'Camp_nif_L2_total_camps_Control',
'Camp_nif_L2_total_camps_Retention_Voice',
'Camp_nif_L2_total_camps_SAT',
'Camp_nif_L2_total_camps_SMS',
'Camp_srv_L2_total_camps_Target',
'Camp_srv_L2_total_redem_Target',
'Camp_srv_L2_total_camps_Universal',
'Camp_srv_L2_total_camps_Retention_Voice',
'Camp_srv_L2_total_redem_Retention_Voice',
'Camp_srv_L2_total_camps_TEL',
'Camp_srv_L2_total_camps_SAT',
'Camp_srv_L2_total_camps_SMS',
'Spinners_nif_distinct_msisdn',
'Spinners_total_acan',
'Spinners_total_movistar',
'Spinners_total_simyo',
'Spinners_total_orange',
'Spinners_total_jazztel',
'Spinners_total_yoigo',
'Spinners_total_masmovil',
'Spinners_total_pepephone',
'Spinners_total_reuskal',
'Spinners_total_unknown',
'Spinners_total_otros',
'netscout_ns_apps_adultweb_days',
'netscout_ns_apps_amazon_days',
'netscout_ns_apps_booking_days',
'netscout_ns_apps_business_days',
'netscout_ns_apps_facebook_days',
'netscout_ns_apps_facebook_video_days',
'netscout_ns_apps_googledrive_days',
'netscout_ns_apps_googlemaps_days',
'netscout_ns_apps_googleplay_days',
'netscout_ns_apps_googleplay_timestamps',
'netscout_ns_apps_itunes_days',
'netscout_ns_apps_itunes_timestamps',
'netscout_ns_apps_medical_days',
'netscout_ns_apps_netflixvideo_days',
'netscout_ns_apps_news_days',
'netscout_ns_apps_paypal_days',
'netscout_ns_apps_paypal_timestamps',
'netscout_ns_apps_shopping_days',
'netscout_ns_apps_shopping_timestamps',
'netscout_ns_apps_snapchat_days',
'netscout_ns_apps_sports_days',
'netscout_ns_apps_technology_days',
'netscout_ns_apps_travel_days',
'netscout_ns_apps_twitch_days',
'netscout_ns_apps_twitter_days',
'netscout_ns_apps_web_o2_https_timestamps',
'netscout_ns_apps_web_pepephone_http_timestamps',
'netscout_ns_apps_wechat_timestamps',
'netscout_ns_apps_whatsapp_days',
'netscout_ns_apps_whatsapp_timestamps',
'netscout_ns_apps_whatsapp_voice_calling_days',
'netscout_ns_apps_youtube_days',
'netscout_ns_apps_youtube_timestamps',
'Pbms_srv_num_reclamaciones_ini_w4',
'Pbms_srv_num_reclamaciones_ini_w8',
'Pbms_srv_num_reclamaciones_w8vsw8',
'Pbms_srv_num_averias_ini_w4',
'Pbms_srv_num_averias_ini_w8',
'Pbms_srv_num_soporte_tecnico_ini_w4',
'Pbms_srv_num_soporte_tecnico_ini_w8',
'NUM_RECLAMACIONES_NIF_ini_w2',
'NUM_RECLAMACIONES_NIF_ini_w4',
'NUM_RECLAMACIONES_NIF_ini_w8',
'NUM_RECLAMACIONES_NIF_w8vsw8',
'NUM_AVERIAS_NIF_ini_w2',
'NUM_AVERIAS_NIF_ini_w4',
'NUM_AVERIAS_NIF_ini_w8',
'NUM_AVERIAS_NIF_prev_w8',
'NUM_SOPORTE_TECNICO_NIF_ini_w4',
'NUM_SOPORTE_TECNICO_NIF_ini_w8',
'Comp_PEPEPHONE_max_count',
'Comp_PEPEPHONE_distinct_days_with_navigation',
'Comp_ORANGE_max_count',
'Comp_JAZZTEL_max_count',
'Comp_JAZZTEL_distinct_days_with_navigation',
'Comp_MOVISTAR_max_count',
'Comp_YOIGO_max_count',
'Comp_YOIGO_distinct_days_with_navigation',
'Comp_VODAFONE_max_count',
'Comp_LOWI_max_count',
'Comp_LOWI_distinct_days_with_navigation',
'Comp_sum_count_comps',
'Comp_max_count_comps',
'Comp_sum_distinct_days_with_navigation_comps',
'Tickets_num_tickets_opened',
'Tickets_num_tickets_tipo_averia_closed',
'Tickets_num_tickets_closed',
'Tickets_num_tickets_tipo_tramitacion',
'Tickets_num_tickets_tipo_reclamacion',
'Tickets_num_tickets_tipo_averia',
'Cust_Agg_football_services',
'Cust_Agg_motor_services',
'Cust_Agg_total_price_motor',
'Cust_Agg_max_dias_desde_fx_pvr_tv',
'Cust_Agg_pvr_services',
'Cust_Agg_total_price_pvr',
'Cust_Agg_max_dias_desde_fx_zapper_tv',
'Cust_Agg_zapper_services',
'Cust_Agg_total_price_zapper',
'Cust_Agg_trybuy_services',
'Cust_Agg_trybuy_autom_services',
'Cust_Agg_num_tariff_maslineasmini',
'Cust_Agg_num_tariff_otros',
'GNV_Voice_L2_total_mou_w',
'GNV_Voice_L2_total_mou_we',
'GNV_Voice_L2_max_mou_W',
'GNV_Voice_L2_max_mou_per_call_W',
'GNV_Voice_L2_max_mou_WE',
'GNV_Voice_L2_max_mou_per_call_WE',
'GNV_Data_morning_W_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_morning_WE_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_evening_W_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_evening_WE_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_night_W_Maps_Pass_Data_Volume_MB',
'GNV_Data_night_W_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_night_WE_VideoHD_Pass_Data_Volume_MB',
'GNV_Data_L2_total_data_volume_W_morning',
'GNV_Data_L2_total_data_volume_W_evening',
'GNV_Data_L2_total_data_volume_W_night',
'GNV_Data_L2_data_per_connection_W_night',
'GNV_Data_L2_total_data_volume_WE_morning',
'GNV_Data_L2_total_data_volume_WE_evening',
'GNV_Data_L2_total_data_volume_WE_night',
'GNV_Data_L2_data_per_connection_WE_night',
'GNV_Data_L2_total_data_volume_W',
'GNV_Data_L2_total_connections_W',
'GNV_Data_L2_data_per_connection_W',
'GNV_Data_L2_total_data_volume_WE',
'GNV_Data_L2_total_connections_WE',
'GNV_Data_L2_data_per_connection_WE',
'GNV_Data_L2_total_data_volume',
'GNV_Data_L2_total_connections',
'GNV_Data_L2_data_per_connection',
'GNV_Data_L2_max_data_volume_W',
'GNV_Data_L2_max_data_per_connection_W',
'GNV_Data_L2_max_data_volume_WE',
'GNV_Data_L2_max_data_per_connection_WE',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ATT_AL_CLIENTE_MOU',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_INF_MOVISTAR_MOU',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_TELE2_MOU',
'GNV_Type_Voice_OTROS_DESTINOS_OTHER_MOU',
'GNV_Type_Voice_INTERNACIONALES_RUMANIA_MOU',
'Bill_N1_net_charges',
'Bill_N2_net_charges',
'Bill_N3_InvoiceCharges',
'Bill_N3_Amount_To_Pay',
'Bill_N3_net_charges',
'Bill_N4_InvoiceCharges',
'Bill_N4_Amount_To_Pay',
'Bill_N4_net_charges',
'Bill_N5_InvoiceCharges',
'Bill_N5_Amount_To_Pay',
'Bill_N5_net_charges',
'Bill_L2_n1_net',
'Bill_L2_n2_net',
'Bill_L2_n3_net',
'Bill_L2_n4_net',
'Bill_L2_n5_net',
'Bill_L2_N1_N2_Amount_To_Pay',
'Bill_L2_N1_N3_Amount_To_Pay',
'Bill_L2_N1_N4_Amount_To_Pay',
'Bill_L2_N1_N5_Amount_To_Pay',
'Bill_L2_N2_N3_Amount_To_Pay',
'Bill_L2_N3_N4_Amount_To_Pay',
'Bill_L2_N4_N5_Amount_To_Pay',
'Camp_nif_L2_pcg_redem_SMS',
'Camp_srv_L2_pcg_redem_Target',
'Camp_srv_L2_pcg_redem_Retention_Voice',
'Camp_srv_L2_pcg_redem_SMS',
'Order_h_L2_N3_StartDate_days_since',
'Order_h_L2_N4_StartDate_days_since',
'Order_h_L2_N5_StartDate_days_since',
'Order_h_L2_N6_StartDate_days_since',
'Order_h_L2_avg_time_bw_orders',
'Penal_CUST_L2_PENDING_N2_end_date_days_until',
'Penal_CUST_L2_PENDING_N3_end_date_days_until',
'Penal_CUST_L2_PENDING_N4_end_date_days_until',
'Penal_CUST_L2_PENDING_N5_end_date_days_until',
'Penal_CUST_L2_PENDING_end_date_total_max_days_until',
'Penal_SRV_L2_PENDING_N2_end_date_days_until',
'Penal_SRV_L2_PENDING_N3_end_date_days_until',
'Penal_SRV_L2_PENDING_N4_end_date_days_until',
'Penal_SRV_L2_PENDING_N5_end_date_days_until',
'Penal_SRV_L2_PENDING_end_date_total_max_days_until',
'Spinners_nif_max_days_since_port',
'Spinners_nif_avg_days_since_port',
'Spinners_num_distinct_operators',
'netscout_ns_apps_badoo_total_effective_ul_mb',
'netscout_ns_apps_facebook_data_mb',
'netscout_ns_apps_facebook_total_effective_dl_mb',
'netscout_ns_apps_facebookmessages_total_effective_ul_mb',
'netscout_ns_apps_fashion_total_effective_dl_mb',
'netscout_ns_apps_food_total_effective_dl_mb',
'netscout_ns_apps_gambling_total_effective_dl_mb',
'netscout_ns_apps_gmail_total_effective_dl_mb',
'netscout_ns_apps_home_total_effective_dl_mb',
'netscout_ns_apps_itunes_data_mb',
'netscout_ns_apps_itunes_total_effective_dl_mb',
'netscout_ns_apps_kids_data_mb',
'netscout_ns_apps_kids_total_effective_dl_mb',
'netscout_ns_apps_linkedin_total_effective_dl_mb',
'netscout_ns_apps_medical_total_effective_dl_mb',
'netscout_ns_apps_netflix_total_effective_ul_mb',
'netscout_ns_apps_netflixvideo_total_effective_ul_mb',
'netscout_ns_apps_pinterest_total_effective_ul_mb',
'netscout_ns_apps_qq_total_effective_ul_mb',
'netscout_ns_apps_reddit_total_effective_ul_mb',
'netscout_ns_apps_snapchat_total_effective_dl_mb',
'netscout_ns_apps_spotify_data_mb',
'netscout_ns_apps_spotify_total_effective_dl_mb',
'netscout_ns_apps_twitch_total_effective_dl_mb',
'netscout_ns_apps_video_total_effective_ul_mb',
'netscout_ns_apps_videostreaming_total_effective_dl_mb',
'netscout_ns_apps_web_jazztel_https_total_effective_ul_mb',
'netscout_ns_apps_web_lowi_http_data_mb',
'netscout_ns_apps_web_lowi_http_total_effective_dl_mb',
'netscout_ns_apps_web_lowi_https_total_effective_ul_mb',
'netscout_ns_apps_web_masmovil_http_total_effective_dl_mb',
'netscout_ns_apps_web_o2_http_data_mb',
'netscout_ns_apps_web_o2_http_total_effective_dl_mb',
'netscout_ns_apps_web_o2_http_total_effective_ul_mb',
'netscout_ns_apps_web_o2_https_data_mb',
'netscout_ns_apps_web_o2_https_total_effective_dl_mb',
'netscout_ns_apps_web_o2_https_total_effective_ul_mb',
'netscout_ns_apps_web_pepephone_http_data_mb',
'netscout_ns_apps_web_pepephone_http_total_effective_ul_mb',
'netscout_ns_apps_web_pepephone_https_total_effective_ul_mb',
'netscout_ns_apps_web_yomvi_http_data_mb',
'netscout_ns_apps_web_yomvi_http_total_effective_dl_mb',
'netscout_ns_apps_web_yomvi_http_total_effective_ul_mb',
'netscout_ns_apps_web_yomvi_https_total_effective_ul_mb',
'netscout_ns_apps_wechat_total_effective_ul_mb',
'netscout_ns_apps_zynga_total_effective_dl_mb',
'tgs_days_until_f_fin_bi',
'tgs_days_until_fecha_fin_dto',
'Reimbursement_adjustment_debt',
'Reimbursement_days_since',
'Reimbursement_days_2_solve',
'Comp_PEPEPHONE_min_days_since_navigation',
'Comp_ORANGE_min_days_since_navigation',
'Comp_JAZZTEL_max_days_since_navigation',
'Comp_JAZZTEL_min_days_since_navigation',
'Comp_MOVISTAR_min_days_since_navigation',
'Comp_MASMOVIL_max_days_since_navigation',
'Comp_MASMOVIL_min_days_since_navigation',
'Comp_YOIGO_max_days_since_navigation',
'Comp_YOIGO_min_days_since_navigation',
'Comp_VODAFONE_max_days_since_navigation',
'Comp_VODAFONE_min_days_since_navigation',
'Comp_LOWI_max_days_since_navigation',
'Comp_LOWI_min_days_since_navigation',
'Comp_O2_max_days_since_navigation',
'Comp_O2_min_days_since_navigation',
'Comp_unknown_max_days_since_navigation',
'Comp_unknown_min_days_since_navigation',
'Comp_norm_sum_distinct_days_with_navigation_comps',
'Comp_norm_min_days_since_navigation_comps',
'Tickets_max_time_opened_tipo_tramitacion',
'Tickets_max_time_opened_tipo_reclamacion',
'Tickets_max_time_opened_tipo_averia',
'Tickets_max_time_opened_tipo_incidencia',
'Tickets_min_time_opened_tipo_tramitacion',
'Tickets_min_time_opened_tipo_reclamacion',
'Tickets_min_time_opened_tipo_averia',
'Tickets_min_time_opened_tipo_incidencia',
'Tickets_std_time_opened_tipo_tramitacion',
'Tickets_std_time_opened_tipo_reclamacion',
'Tickets_std_time_opened_tipo_averia',
'Tickets_std_time_opened_tipo_incidencia',
'Tickets_mean_time_opened_tipo_tramitacion',
'Tickets_mean_time_opened_tipo_reclamacion',
'Tickets_mean_time_opened_tipo_averia',
'Tickets_mean_time_opened_tipo_incidencia',
'Tickets_mean_time_opened',
'Tickets_max_time_opened',
'Tickets_min_time_opened',
'Tickets_std_time_opened',
'Tickets_max_time_closed_tipo_tramitacion',
'Tickets_max_time_closed_tipo_reclamacion',
'Tickets_max_time_closed_tipo_averia',
'Tickets_max_time_closed_tipo_incidencia',
'Tickets_min_time_closed_tipo_tramitacion',
'Tickets_min_time_closed_tipo_reclamacion',
'Tickets_min_time_closed_tipo_averia',
'Tickets_min_time_closed_tipo_incidencia',
'Tickets_std_time_closed_tipo_tramitacion',
'Tickets_std_time_closed_tipo_reclamacion',
'Tickets_std_time_closed_tipo_averia',
'Tickets_std_time_closed_tipo_incidencia',
'Tickets_mean_time_closed_tipo_tramitacion',
'Tickets_mean_time_closed_tipo_reclamacion',
'Tickets_mean_time_closed_tipo_averia',
'Tickets_mean_time_closed_tipo_incidencia',
'Order_h_days_since_last_order',
'Order_h_num_orders',
'device_days_since_device_n1_change_date',
'device_days_since_device_n2_change_date',
'device_days_since_device_n3_change_date',
'tgs_has_discount',
'Cust_Agg_fbb_services_nc',
'Cust_Agg_fixed_services_nc',
'Cust_Agg_mobile_services_nc',
'Cust_Agg_tv_services_nc',
'Cust_Agg_prepaid_services_nc',
'Cust_Agg_bam_mobile_services_nc',
'Cust_Agg_fbb_services_nif',
'Cust_Agg_bam_services_nc',
'GNV_Voice_morning_W_Num_Of_Calls',
'GNV_Data_morning_W_Chat_Zero_Num_Of_Connections',
'GNV_Data_morning_W_Maps_Pass_Num_Of_Connections',
'GNV_Data_morning_W_Video_Pass_Num_Of_Connections',
'GNV_Data_morning_W_Music_Pass_Num_Of_Connections',
'GNV_Data_morning_W_Social_Pass_Num_Of_Connections',
'GNV_Data_morning_WE_Chat_Zero_Num_Of_Connections',
'GNV_Data_morning_W_RegularData_Num_Of_Connections',
'GNV_Data_evening_W_Chat_Zero_Num_Of_Connections',
'GNV_Data_evening_W_Maps_Pass_Num_Of_Connections',
'GNV_Data_evening_W_Music_Pass_Num_Of_Connections',
'GNV_Data_evening_W_RegularData_Num_Of_Connections',
'GNV_Data_night_W_Chat_Zero_Num_Of_Connections',
'GNV_Data_night_W_Maps_Pass_Num_Of_Connections',
'GNV_Data_night_W_Video_Pass_Num_Of_Connections',
'GNV_Data_night_W_Music_Pass_Num_Of_Connections',
'GNV_Data_evening_WE_RegularData_Num_Of_Connections',
'GNV_Type_Voice_TARIFICACION_ADICIONAL_PRESTADOR_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_EU_EAST_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_AMERICA_NORTH_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_AFRICA_NORTH_Num_Of_Calls',
'Bill_N1_num_ids_fict',
'Camp_nif_Retention_Voice_SMS_Target_1',
'Camp_nif_L2_total_redem_Target',
'Camp_nif_Retention_Voice_TEL_Target_0',
'Camp_nif_L2_total_redem_Retention_Voice',
'Camp_srv_Retention_Voice_TEL_Target_1',
'Camp_srv_Retention_Voice_SMS_Target_1',
'device_month_imei_changes',
'PER_PREFS_NAVIGATION',
'Spinners_nif_port_number',
'netscout_ns_apps_accuweather_days',
'netscout_ns_apps_alcohol_days',
'netscout_ns_apps_apple_days',
'netscout_ns_apps_arts_days',
'netscout_ns_apps_astrology_days',
'netscout_ns_apps_audio_days',
'netscout_ns_apps_auto_days',
'netscout_ns_apps_badoo_days',
'netscout_ns_apps_baidu_days',
'netscout_ns_apps_bbc_days',
'netscout_ns_apps_dating_days',
'netscout_ns_apps_ebay_days',
'netscout_ns_apps_facebook_timestamps',
'netscout_ns_apps_facebookmessages_days',
'netscout_ns_apps_facebookmessages_timestamps',
'netscout_ns_apps_family_days',
'netscout_ns_apps_foursquare_days',
'netscout_ns_apps_gambling_days',
'netscout_ns_apps_github_days',
'netscout_ns_apps_gmail_days',
'netscout_ns_apps_googleearth_days',
'netscout_ns_apps_hacking_days',
'netscout_ns_apps_instagram_days',
'netscout_ns_apps_jobs_days',
'netscout_ns_apps_linkedin_days',
'netscout_ns_apps_music_days',
'netscout_ns_apps_netflix_days',
'netscout_ns_apps_netflix_timestamps',
'netscout_ns_apps_pets_days',
'netscout_ns_apps_pinterest_days',
'netscout_ns_apps_politics_days',
'netscout_ns_apps_pregnancy_days',
'netscout_ns_apps_samsung_days',
'netscout_ns_apps_skype_days',
'netscout_ns_apps_spotify_days',
'netscout_ns_apps_alibaba_days',
'netscout_ns_apps_alipay_days',
'netscout_ns_apps_vimeo_days',
'netscout_ns_apps_violence_days',
'netscout_ns_apps_webgames_days',
'netscout_ns_apps_webmobile_days',
'netscout_ns_apps_web_jazztel_http_days',
'netscout_ns_apps_web_lowi_http_days',
'netscout_ns_apps_web_lowi_https_days',
'netscout_ns_apps_web_masmovil_http_days',
'netscout_ns_apps_web_masmovil_https_days',
'netscout_ns_apps_web_movistar_http_days',
'netscout_ns_apps_web_o2_http_days',
'netscout_ns_apps_web_o2_http_timestamps',
'netscout_ns_apps_web_movistar_https_timestamps',
'netscout_ns_apps_web_vodafone_http_days',
'netscout_ns_apps_web_vodafone_https_days',
'netscout_ns_apps_web_yoigo_http_days',
'netscout_ns_apps_web_yomvi_http_days',
'netscout_ns_apps_web_yomvi_https_days',
'netscout_ns_apps_whatsapp_media_message_days',
'netscout_ns_apps_wikipedia_days',
'Pbms_srv_num_reclamaciones_ini_w2',
'Pbms_srv_num_averias_ini_w2',
'Pbms_srv_num_averias_prev_w8',
'Pbms_srv_num_reclamaciones_prev_w2',
'Pbms_srv_num_reclamaciones_w2vsw2',
'Pbms_srv_num_reclamaciones_prev_w4',
'Pbms_srv_num_reclamaciones_w4vsw4',
'Pbms_srv_num_reclamaciones_prev_w8',
'Pbms_srv_num_averias_prev_w2',
'Pbms_srv_num_averias_w2vsw2',
'Pbms_srv_num_averias_prev_w4',
'Pbms_srv_num_averias_w4vsw4',
'Pbms_srv_num_averias_w8vsw8',
'Pbms_srv_num_soporte_tecnico_ini_w2',
'Pbms_srv_num_soporte_tecnico_prev_w2',
'Pbms_srv_num_soporte_tecnico_w2vsw2',
'Pbms_srv_num_soporte_tecnico_prev_w4',
'Pbms_srv_num_soporte_tecnico_w4vsw4',
'Pbms_srv_num_soporte_tecnico_prev_w8',
'Pbms_srv_num_soporte_tecnico_w8vsw8',
'Reimbursement_num',
'Reimbursement_num_month_2',
'netscout_ns_apps_web_pepephone_https_timestamps',
'netscout_ns_apps_web_orange_http_timestamps',
'netscout_ns_apps_web_movistar_http_timestamps',
'Comp_MASMOVIL_sum_count',
'netscout_ns_apps_web_yoigo_https_timestamps',
'netscout_ns_apps_web_vodafone_https_timestamps',
'netscout_ns_apps_web_lowi_http_timestamps',
'Comp_O2_sum_count',
'Comp_VODAFONE_sum_count',
'Comp_VODAFONE_distinct_days_with_navigation',
'Cust_Agg_mobile_services_nif',
'Cust_Agg_num_football_nc',
'Cust_Agg_max_dias_desde_fx_football_tv',
'Cust_Agg_max_dias_desde_fx_motor_tv',
'Cust_Agg_max_dias_desde_fx_trybuy_tv',
'Cust_Agg_tv_services_nif',
'Cust_Agg_bam_mobile_services_nif',
'Cust_Agg_L2_bam_fx_first_days_since_nc',
'Cust_Agg_L2_bam_mobile_fx_first_days_since_nc',
'Cust_Agg_L2_fbb_fx_first_days_since_nc',
'Cust_Agg_L2_fixed_fx_first_days_since_nc',
'Cust_Agg_L2_mobile_fx_first_days_since_nc',
'Cust_Agg_L2_prepaid_fx_first_days_since_nc',
'Cust_Agg_L2_tv_fx_first_days_since_nc',
'GNV_Voice_morning_W_MOU',
'GNV_Voice_evening_W_MOU',
'GNV_Voice_night_WE_MOU',
'GNV_Data_morning_W_Music_Pass_Data_Volume_MB',
'GNV_Data_morning_W_Maps_Pass_Data_Volume_MB',
'GNV_Data_night_W_Chat_Zero_Data_Volume_MB',
'GNV_Data_night_W_Video_Pass_Data_Volume_MB',
'GNV_Data_night_W_Music_Pass_Data_Volume_MB',
'GNV_Data_night_W_Social_Pass_Data_Volume_MB',
'GNV_Data_night_W_RegularData_Data_Volume_MB',
'GNV_Data_evening_W_RegularData_Data_Volume_MB',
'GNV_Data_morning_WE_RegularData_Data_Volume_MB',
'GNV_Data_evening_WE_RegularData_Data_Volume_MB',
'GNV_Type_Voice_NACIONALES_Num_Of_Calls',
'GNV_Type_Voice_VIDELOTELEFONIA_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_AIRE_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_CARREFOUR_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_DIA_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_DIGI_SPAIN_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_EROSKI_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_HAPPY_MOVIL_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_AMENA_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_AT_COMERCIAL_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_ATENCION_AL_CLIENTE_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_EUSKALTEL_SA_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_800_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_800_INTERNACIONAL_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_TONOS_DE_ESPERA_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_OTHER_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_ADM_GENERAL_ESTADO_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_ATENCION_CIUDADANA_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_INF_AUTONOMICA_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_EUSKALTEL_SA_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_HELLO_TV_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_LINEA_902_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_LINEA_902_VODAFONE_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_LINEA_905_SOPORTE_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_MARRUECOS_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_ASIA_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_OCEANIA_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_REINO_UNIDO_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_PORTUGAL_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_EU_EAST_MOU',
'GNV_Type_Voice_INTERNACIONALES_AMERICA_NORTH_MOU',
'GNV_Type_Voice_INTERNACIONALES_AFRICA_NORTH_MOU',
'Bill_N1_InvoiceCharges',
'Bill_N1_Amount_To_Pay',
'Bill_N1_Tax_Amount',
'Bill_N2_InvoiceCharges',
'Bill_N2_Amount_To_Pay',
'Bill_N1_Debt_Amount',
'Bill_N2_Tax_Amount',
'Bill_N2_Debt_Amount',
'Bill_N3_Tax_Amount',
'Bill_N3_Debt_Amount',
'Bill_N4_Tax_Amount',
'Bill_N4_Debt_Amount',
'Camp_nif_L2_total_redem_SMS',
'Camp_nif_L2_pcg_redem_Target',
'Camp_srv_L2_total_redem_TEL',
'Order_h_L2_N7_StartDate_days_since',
'Order_h_L2_N8_StartDate_days_since',
'Order_h_L2_N9_StartDate_days_since',
'Order_h_L2_N10_StartDate_days_since',
'Penal_CUST_L2_PENDING_N1_penal_amount',
'Penal_CUST_L2_PENDING_N2_penal_amount',
'Penal_CUST_L2_PENDING_N3_penal_amount',
'Penal_CUST_L2_PENDING_N3_penal_amount_total',
'Penal_CUST_L2_PENDING_N4_penal_amount',
'Penal_CUST_L2_PENDING_N1_end_date_days_until',
'Penal_SRV_L2_PENDING_N1_end_date_days_until',
'netscout_ns_apps_accuweather_data_mb',
'netscout_ns_apps_adult_data_mb',
'netscout_ns_apps_adultweb_data_mb',
'netscout_ns_apps_adultweb_total_effective_dl_mb',
'netscout_ns_apps_alcohol_data_mb',
'netscout_ns_apps_alibaba_data_mb',
'netscout_ns_apps_alipay_data_mb',
'netscout_ns_apps_amazon_data_mb',
'netscout_ns_apps_apple_data_mb',
'netscout_ns_apps_arts_data_mb',
'netscout_ns_apps_astrology_data_mb',
'netscout_ns_apps_audio_data_mb',
'netscout_ns_apps_audio_total_effective_dl_mb',
'netscout_ns_apps_auto_data_mb',
'netscout_ns_apps_badoo_data_mb',
'netscout_ns_apps_badoo_timestamps',
'netscout_ns_apps_baidu_data_mb',
'netscout_ns_apps_bbc_data_mb',
'netscout_ns_apps_booking_data_mb',
'netscout_ns_apps_books_data_mb',
'netscout_ns_apps_business_data_mb',
'netscout_ns_apps_chats_data_mb',
'netscout_ns_apps_classified_data_mb',
'netscout_ns_apps_dating_data_mb',
'netscout_ns_apps_dropbox_data_mb',
'netscout_ns_apps_ebay_data_mb',
'netscout_ns_apps_education_data_mb',
'netscout_ns_apps_facebook_video_timestamps',
'netscout_ns_apps_facebookmessages_data_mb',
'netscout_ns_apps_facebook_video_data_mb',
'netscout_ns_apps_facebook_video_total_effective_dl_mb',
'netscout_ns_apps_family_data_mb',
'netscout_ns_apps_fashion_timestamps',
'netscout_ns_apps_fashion_data_mb',
'netscout_ns_apps_finance_data_mb',
'netscout_ns_apps_food_data_mb',
'netscout_ns_apps_foursquare_data_mb',
'netscout_ns_apps_gambling_timestamps',
'netscout_ns_apps_github_data_mb',
'netscout_ns_apps_gmail_timestamps',
'netscout_ns_apps_gmail_data_mb',
'netscout_ns_apps_googledrive_data_mb',
'netscout_ns_apps_googleearth_data_mb',
'netscout_ns_apps_googlemaps_data_mb',
'netscout_ns_apps_googleplay_data_mb',
'netscout_ns_apps_groupon_data_mb',
'netscout_ns_apps_hacking_data_mb',
'netscout_ns_apps_hacking_timestamps',
'netscout_ns_apps_home_data_mb',
'netscout_ns_apps_instagram_timestamps',
'netscout_ns_apps_instagram_data_mb',
'netscout_ns_apps_instagram_total_effective_dl_mb',
'netscout_ns_apps_jobs_data_mb',
'netscout_ns_apps_kids_timestamps',
'netscout_ns_apps_legal_data_mb',
'netscout_ns_apps_line_data_mb',
'netscout_ns_apps_linkedin_timestamps',
'netscout_ns_apps_linkedin_data_mb',
'netscout_ns_apps_medical_data_mb',
'netscout_ns_apps_music_data_mb',
'netscout_ns_apps_netflix_data_mb',
'netscout_ns_apps_netflixvideo_timestamps',
'netscout_ns_apps_netflixvideo_data_mb',
'netscout_ns_apps_news_data_mb',
'netscout_ns_apps_paypal_data_mb',
'netscout_ns_apps_pets_data_mb',
'netscout_ns_apps_pinterest_data_mb',
'netscout_ns_apps_politics_data_mb',
'netscout_ns_apps_pregnancy_data_mb',
'netscout_ns_apps_pregnancy_total_effective_dl_mb',
'netscout_ns_apps_qq_data_mb',
'netscout_ns_apps_qq_timestamps',
'netscout_ns_apps_reddit_data_mb',
'netscout_ns_apps_samsung_data_mb',
'netscout_ns_apps_science_data_mb',
'netscout_ns_apps_shopping_data_mb',
'netscout_ns_apps_skype_data_mb',
'netscout_ns_apps_socialnetwork_timestamps',
'netscout_ns_apps_socialnetwork_data_mb',
'netscout_ns_apps_socialnetwork_total_effective_dl_mb',
'netscout_ns_apps_web_yomvi_http_timestamps',
'netscout_ns_apps_sports_data_mb',
'netscout_ns_apps_spotify_timestamps',
'netscout_ns_apps_apple_total_effective_dl_mb',
'netscout_ns_apps_steam_data_mb',
'netscout_ns_apps_steam_timestamps',
'netscout_ns_apps_taobao_data_mb',
'netscout_ns_apps_alibaba_total_effective_dl_mb',
'netscout_ns_apps_technology_data_mb',
'netscout_ns_apps_travel_data_mb',
'netscout_ns_apps_tumblr_data_mb',
'netscout_ns_apps_twitch_timestamps',
'netscout_ns_apps_twitch_data_mb',
'netscout_ns_apps_twitter_data_mb',
'netscout_ns_apps_video_timestamps',
'netscout_ns_apps_video_data_mb',
'netscout_ns_apps_vimeo_data_mb',
'netscout_ns_apps_violence_data_mb',
'netscout_ns_apps_violence_timestamps',
'netscout_ns_apps_webgames_data_mb',
'netscout_ns_apps_webmobile_data_mb',
'netscout_ns_apps_web_jazztel_http_data_mb',
'Comp_JAZZTEL_sum_count',
'netscout_ns_apps_web_jazztel_http_total_effective_dl_mb',
'netscout_ns_apps_web_jazztel_https_data_mb',
'Comp_LOWI_sum_count',
'netscout_ns_apps_web_lowi_https_data_mb',
'netscout_ns_apps_web_lowi_https_timestamps',
'netscout_ns_apps_web_masmovil_https_data_mb',
'netscout_ns_apps_web_movistar_http_data_mb',
'netscout_ns_apps_web_movistar_https_data_mb',
'netscout_ns_apps_web_orange_http_data_mb',
'netscout_ns_apps_web_orange_https_data_mb',
'netscout_ns_apps_web_pepephone_https_data_mb',
'netscout_ns_apps_web_pepephone_https_total_effective_dl_mb',
'netscout_ns_apps_web_vodafone_http_data_mb',
'netscout_ns_apps_web_vodafone_https_data_mb',
'netscout_ns_apps_web_yoigo_http_data_mb',
'netscout_ns_apps_web_yoigo_https_data_mb',
'netscout_ns_apps_web_yomvi_https_data_mb',
'netscout_ns_apps_web_yomvi_https_timestamps',
'netscout_ns_apps_wechat_data_mb',
'netscout_ns_apps_whatsapp_data_mb',
'netscout_ns_apps_whatsapp_media_message_data_mb',
'netscout_ns_apps_whatsapp_voice_calling_data_mb',
'netscout_ns_apps_whatsapp_voice_calling_total_effective_dl_mb',
'netscout_ns_apps_wikipedia_data_mb',
'netscout_ns_apps_yandex_data_mb',
'netscout_ns_apps_youtube_data_mb',
'netscout_ns_apps_zynga_timestamps',
'netscout_ns_apps_zynga_data_mb',
'Comp_PEPEPHONE_max_days_since_navigation',
'Comp_ORANGE_max_days_since_navigation',
'Comp_sum_distinct_days_with_navigation_vdf',
'Comp_MOVISTAR_max_days_since_navigation',
'Comp_num_distinct_comps',
'Comp_max_days_since_navigation_comps',
'Cust_L2_days_since_fecha_migracion',
'Order_h_L2_N1_StartDate_days_since',
'Order_h_avg_days_per_order',
'device_tenure_days_from_n1',
'GNV_Data_morning_W_MasMegas_Num_Of_Connections',
'GNV_Data_morning_WE_MasMegas_Num_Of_Connections',
'GNV_Data_evening_W_MasMegas_Num_Of_Connections',
'GNV_Data_evening_WE_MasMegas_Num_Of_Connections',
'GNV_Data_night_W_MasMegas_Num_Of_Connections',
'GNV_Data_night_WE_MasMegas_Num_Of_Connections',
'GNV_Type_Voice_LLAMADAS_VPN_Num_Of_Calls',
'GNV_Type_Voice_MOVILES_BARABLU_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ASESOR_COMERCIAL_EMPRESAS_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_Num_Of_Calls',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_Num_Of_Calls',
'GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_Num_Of_Calls',
'GNV_Type_Voice_INTERNACIONALES_OTHER_AREAS_Num_Of_Calls',
'Camp_nif_Retention_Voice_EMA_Target_0',
'Camp_nif_Retention_Voice_EMA_Target_1',
'Camp_nif_Retention_Voice_EMA_Control_0',
'Camp_nif_Retention_Voice_EMA_Control_1',
'Camp_nif_Retention_Voice_EMA_Universal_0',
'Camp_nif_Retention_Voice_EMA_Universal_1',
'Camp_nif_Retention_Voice_TEL_Target_1',
'Camp_nif_Retention_Voice_TEL_Control_0',
'Camp_nif_Retention_Voice_TEL_Control_1',
'Camp_nif_Retention_Voice_TEL_Universal_0',
'Camp_nif_Retention_Voice_TEL_Universal_1',
'Camp_nif_Retention_Voice_SAT_Target_1',
'Camp_nif_Retention_Voice_SAT_Control_1',
'Camp_nif_Retention_Voice_SAT_Universal_0',
'Camp_nif_Retention_Voice_SAT_Universal_1',
'Camp_nif_Retention_Voice_SMS_Control_1',
'Camp_nif_Retention_Voice_SMS_Universal_0',
'Camp_nif_Retention_Voice_SMS_Universal_1',
'Camp_nif_Retention_Voice_MMS_Target_0',
'Camp_nif_Retention_Voice_MMS_Target_1',
'Camp_nif_Retention_Voice_MMS_Control_0',
'Camp_nif_Retention_Voice_MMS_Control_1',
'Camp_nif_Retention_Voice_MMS_Universal_0',
'Camp_nif_Retention_Voice_MMS_Universal_1',
'Camp_nif_Up_Cross_Sell_EMA_Target_0',
'Camp_nif_Up_Cross_Sell_EMA_Target_1',
'Camp_nif_Up_Cross_Sell_EMA_Control_0',
'Camp_nif_Up_Cross_Sell_EMA_Control_1',
'Camp_nif_Up_Cross_Sell_EMA_Universal_0',
'Camp_nif_Up_Cross_Sell_EMA_Universal_1',
'Camp_nif_Up_Cross_Sell_TEL_Target_0',
'Camp_nif_Up_Cross_Sell_TEL_Target_1',
'Camp_nif_Up_Cross_Sell_TEL_Control_0',
'Camp_nif_Up_Cross_Sell_TEL_Control_1',
'Camp_nif_Up_Cross_Sell_TEL_Universal_0',
'Camp_nif_Up_Cross_Sell_TEL_Universal_1',
'Camp_nif_Up_Cross_Sell_SAT_Target_0',
'Camp_nif_Up_Cross_Sell_SAT_Target_1',
'Camp_nif_Up_Cross_Sell_SAT_Control_0',
'Camp_nif_Up_Cross_Sell_SAT_Control_1',
'Camp_nif_Up_Cross_Sell_SAT_Universal_0',
'Camp_nif_Up_Cross_Sell_SAT_Universal_1',
'Camp_nif_Up_Cross_Sell_SMS_Target_0',
'Camp_nif_Up_Cross_Sell_SMS_Target_1',
'Camp_nif_Up_Cross_Sell_SMS_Control_0',
'Camp_nif_Up_Cross_Sell_SMS_Control_1',
'Camp_nif_Up_Cross_Sell_SMS_Universal_0',
'Camp_nif_Up_Cross_Sell_SMS_Universal_1',
'Camp_nif_Up_Cross_Sell_MMS_Target_0',
'Camp_nif_Up_Cross_Sell_MMS_Target_1',
'Camp_nif_Up_Cross_Sell_MMS_Control_0',
'Camp_nif_Up_Cross_Sell_MMS_Control_1',
'Camp_nif_Up_Cross_Sell_MMS_Universal_0',
'Camp_nif_Up_Cross_Sell_MMS_Universal_1',
'Camp_nif_L2_total_redem_Control',
'Camp_nif_L2_total_camps_Universal',
'Camp_nif_L2_total_redem_Universal',
'Camp_nif_L2_total_camps_Up_Cross_Sell',
'Camp_nif_L2_total_redem_Up_Cross_Sell',
'Camp_nif_L2_total_camps_EMA',
'Camp_nif_L2_total_redem_EMA',
'Camp_nif_L2_total_redem_TEL',
'Camp_nif_L2_total_redem_SAT',
'Camp_nif_L2_total_camps_MMS',
'Camp_nif_L2_total_redem_MMS',
'Camp_srv_Retention_Voice_EMA_Target_0',
'Camp_srv_Retention_Voice_EMA_Target_1',
'Camp_srv_Retention_Voice_EMA_Control_0',
'Camp_srv_Retention_Voice_EMA_Control_1',
'Camp_srv_Retention_Voice_EMA_Universal_0',
'Camp_srv_Retention_Voice_EMA_Universal_1',
'Camp_srv_Retention_Voice_TEL_Control_1',
'Camp_srv_Retention_Voice_TEL_Universal_1',
'Camp_srv_Retention_Voice_SAT_Target_1',
'Camp_srv_Retention_Voice_SAT_Control_1',
'Camp_srv_Retention_Voice_SAT_Universal_1',
'Camp_srv_Retention_Voice_SMS_Control_1',
'Camp_srv_Retention_Voice_SMS_Universal_1',
'Camp_srv_Retention_Voice_MMS_Target_0',
'Camp_srv_Retention_Voice_MMS_Target_1',
'Camp_srv_Retention_Voice_MMS_Control_0',
'Camp_srv_Retention_Voice_MMS_Control_1',
'Camp_srv_Retention_Voice_MMS_Universal_0',
'Camp_srv_Retention_Voice_MMS_Universal_1',
'Camp_srv_Up_Cross_Sell_EMA_Target_0',
'Camp_srv_Up_Cross_Sell_EMA_Target_1',
'Camp_srv_Up_Cross_Sell_EMA_Control_0',
'Camp_srv_Up_Cross_Sell_EMA_Control_1',
'Camp_srv_Up_Cross_Sell_EMA_Universal_0',
'Camp_srv_Up_Cross_Sell_EMA_Universal_1',
'Camp_srv_Up_Cross_Sell_TEL_Target_0',
'Camp_srv_Up_Cross_Sell_TEL_Target_1',
'Camp_srv_Up_Cross_Sell_TEL_Control_0',
'Camp_srv_Up_Cross_Sell_TEL_Control_1',
'Camp_srv_Up_Cross_Sell_TEL_Universal_0',
'Camp_srv_Up_Cross_Sell_TEL_Universal_1',
'Camp_srv_Up_Cross_Sell_SAT_Target_0',
'Camp_srv_Up_Cross_Sell_SAT_Target_1',
'Camp_srv_Up_Cross_Sell_SAT_Control_0',
'Camp_srv_Up_Cross_Sell_SAT_Control_1',
'Camp_srv_Up_Cross_Sell_SAT_Universal_0',
'Camp_srv_Up_Cross_Sell_SAT_Universal_1',
'Camp_srv_Up_Cross_Sell_SMS_Target_0',
'Camp_srv_Up_Cross_Sell_SMS_Target_1',
'Camp_srv_Up_Cross_Sell_SMS_Control_0',
'Camp_srv_Up_Cross_Sell_SMS_Control_1',
'Camp_srv_Up_Cross_Sell_SMS_Universal_0',
'Camp_srv_Up_Cross_Sell_SMS_Universal_1',
'Camp_srv_Up_Cross_Sell_MMS_Target_0',
'Camp_srv_Up_Cross_Sell_MMS_Target_1',
'Camp_srv_Up_Cross_Sell_MMS_Control_0',
'Camp_srv_Up_Cross_Sell_MMS_Control_1',
'Camp_srv_Up_Cross_Sell_MMS_Universal_0',
'Camp_srv_Up_Cross_Sell_MMS_Universal_1',
'Camp_srv_L2_total_redem_Control',
'Camp_srv_L2_total_redem_Universal',
'Camp_srv_L2_total_camps_Up_Cross_Sell',
'Camp_srv_L2_total_redem_Up_Cross_Sell',
'Camp_srv_L2_total_camps_EMA',
'Camp_srv_L2_total_redem_EMA',
'Camp_srv_L2_total_redem_SAT',
'Camp_srv_L2_total_camps_MMS',
'Camp_srv_L2_total_redem_MMS',
'Order_Agg_Activa_Promo_CAN_orders',
'Order_Agg_Activa_Promo_PEN_orders',
'Order_Agg_Correccion_CAN_orders',
'Order_Agg_Correccion_PEN_orders',
'Order_Agg_Descon_Prepago_CAN_orders',
'Order_Agg_Descon_Prepago_PEN_orders',
'Order_Agg_Factura_Canal_Presencial_CAN_orders',
'Order_Agg_Factura_Terminal_CAN_orders',
'Order_Agg_Factura_Terminal_PEN_orders',
'Order_Agg_Fideliza_cliente_CAN_orders',
'Order_Agg_Fideliza_cliente_PEN_orders',
'Order_Agg_Instalacion_SecureNet_CAN_orders',
'Order_Agg_Instalacion_SecureNet_COM_orders',
'Order_Agg_Instalacion_SecureNet_PEN_orders',
'Order_Agg_Otros_PEN_orders',
'PER_PREFS_LORTAD',
'Spinners_movistar_PCAN',
'Spinners_movistar_AACE',
'Spinners_simyo_PCAN',
'Spinners_simyo_AACE',
'Spinners_simyo_AENV',
'Spinners_orange_PCAN',
'Spinners_orange_AACE',
'Spinners_jazztel_PCAN',
'Spinners_jazztel_AACE',
'Spinners_jazztel_AENV',
'Spinners_yoigo_AACE',
'Spinners_yoigo_AENV',
'Spinners_masmovil_ASOL',
'Spinners_masmovil_PCAN',
'Spinners_masmovil_AACE',
'Spinners_masmovil_AENV',
'Spinners_pepephone_PCAN',
'Spinners_pepephone_AACE',
'Spinners_pepephone_AENV',
'Spinners_reuskal_PCAN',
'Spinners_reuskal_AACE',
'Spinners_reuskal_AENV',
'Spinners_unknown_PCAN',
'Spinners_unknown_AACE',
'Spinners_otros_PCAN',
'Spinners_otros_AACE',
'Spinners_otros_AENV',
'netscout_ns_apps_web_jazztel_days',
'netscout_ns_apps_web_jazztel_timestamps',
'netscout_ns_apps_web_lowi_days',
'netscout_ns_apps_web_lowi_timestamps',
'netscout_ns_apps_web_marcacom_http_days',
'netscout_ns_apps_web_marcacom_http_timestamps',
'netscout_ns_apps_web_marcacom_https_days',
'netscout_ns_apps_web_marcacom_https_timestamps',
'netscout_ns_apps_web_masmovil_days',
'netscout_ns_apps_web_masmovil_timestamps',
'netscout_ns_apps_web_movistar_days',
'netscout_ns_apps_web_movistar_timestamps',
'netscout_ns_apps_web_o2_days',
'netscout_ns_apps_web_o2_timestamps',
'netscout_ns_apps_web_orange_days',
'netscout_ns_apps_web_orange_timestamps',
'netscout_ns_apps_web_pepephone_days',
'netscout_ns_apps_web_pepephone_timestamps',
'netscout_ns_apps_web_vodafone_days',
'netscout_ns_apps_web_vodafone_timestamps',
'netscout_ns_apps_web_yoigo_days',
'netscout_ns_apps_web_yoigo_timestamps',
'Comp_unknown_sum_count',
'Comp_unknown_max_count',
'Comp_unknown_distinct_days_with_navigation',
'Tickets_num_tickets_tipo_incidencia_opened',
'Tickets_num_tickets_tipo_incidencia_closed',
'Tickets_num_tickets_tipo_incidencia',
'Cust_Agg_total_football_price_nc',
'Cust_Agg_total_football_price_nif',
'Cust_Agg_total_price_football',
'Cust_Agg_total_price_trybuy',
'Cust_Agg_total_price_trybuy_autom',
'Cust_Agg_num_tariff_unknown',
'GNV_Data_morning_W_MasMegas_Data_Volume_MB',
'GNV_Data_morning_WE_MasMegas_Data_Volume_MB',
'GNV_Data_evening_W_MasMegas_Data_Volume_MB',
'GNV_Data_evening_WE_MasMegas_Data_Volume_MB',
'GNV_Data_night_W_MasMegas_Data_Volume_MB',
'GNV_Data_night_WE_MasMegas_Data_Volume_MB',
'GNV_Type_Voice_LLAMADAS_VPN_MOU',
'GNV_Type_Voice_MOVILES_BARABLU_MOU',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ASESOR_COMERCIAL_EMPRESAS_MOU',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_MOU',
'GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_MOU',
'GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_MOU',
'GNV_Type_Voice_INTERNACIONALES_OTHER_AREAS_MOU',
'Camp_nif_L2_pcg_redem_Control',
'Camp_nif_L2_pcg_redem_Universal',
'Camp_nif_L2_pcg_redem_Up_Cross_Sell',
'Camp_nif_L2_pcg_redem_EMA',
'Camp_nif_L2_pcg_redem_TEL',
'Camp_nif_L2_pcg_redem_SAT',
'Camp_nif_L2_pcg_redem_MMS',
'Camp_srv_L2_pcg_redem_Control',
'Camp_srv_L2_pcg_redem_Universal',
'Camp_srv_L2_pcg_redem_Up_Cross_Sell',
'Camp_srv_L2_pcg_redem_EMA',
'Camp_srv_L2_pcg_redem_SAT',
'Camp_srv_L2_pcg_redem_MMS',
'Penal_CUST_L2_PENDING_N5_penal_amount',
'Penal_CUST_L2_PENDING_N5_penal_amount_total',
'netscout_ns_apps_web_jazztel_data_mb',
'netscout_ns_apps_web_jazztel_total_effective_dl_mb',
'netscout_ns_apps_web_jazztel_total_effective_ul_mb',
'netscout_ns_apps_web_lowi_data_mb',
'netscout_ns_apps_web_lowi_total_effective_dl_mb',
'netscout_ns_apps_web_lowi_total_effective_ul_mb',
'netscout_ns_apps_web_marcacom_http_data_mb',
'netscout_ns_apps_web_marcacom_http_total_effective_dl_mb',
'netscout_ns_apps_web_marcacom_http_total_effective_ul_mb',
'netscout_ns_apps_web_marcacom_https_data_mb',
'netscout_ns_apps_web_marcacom_https_total_effective_dl_mb',
'netscout_ns_apps_web_marcacom_https_total_effective_ul_mb',
'netscout_ns_apps_web_masmovil_data_mb',
'netscout_ns_apps_web_masmovil_total_effective_dl_mb',
'netscout_ns_apps_web_masmovil_total_effective_ul_mb',
'netscout_ns_apps_web_movistar_data_mb',
'netscout_ns_apps_web_movistar_total_effective_dl_mb',
'netscout_ns_apps_web_movistar_total_effective_ul_mb',
'netscout_ns_apps_web_o2_data_mb',
'netscout_ns_apps_web_o2_total_effective_dl_mb',
'netscout_ns_apps_web_o2_total_effective_ul_mb',
'netscout_ns_apps_web_orange_data_mb',
'netscout_ns_apps_web_orange_total_effective_dl_mb',
'netscout_ns_apps_web_orange_total_effective_ul_mb',
'netscout_ns_apps_web_pepephone_data_mb',
'netscout_ns_apps_web_pepephone_total_effective_dl_mb',
'netscout_ns_apps_web_pepephone_total_effective_ul_mb',
'netscout_ns_apps_web_vodafone_data_mb',
'netscout_ns_apps_web_vodafone_total_effective_dl_mb',
'netscout_ns_apps_web_vodafone_total_effective_ul_mb',
'netscout_ns_apps_web_yoigo_data_mb',
'netscout_ns_apps_web_yoigo_total_effective_dl_mb',
'netscout_ns_apps_web_yoigo_total_effective_ul_mb',
'tgs_days_since_f_inicio_bi_exp',
'tgs_days_until_f_fin_bi_exp',
'Tickets_weeks_averias',
'Tickets_weeks_facturacion',
'Tickets_weeks_reclamacion',
'Tickets_weeks_incidencias',
'tgs_sum_ind_under_use',
'tgs_sum_ind_over_use']

    return corr_vars


def drop_corr_vars_70():


    corr_vars=["Cust_L2_days_until_next_bill",
"Serv_L2_segunda_linea",
"Cust_Agg_fixed_services_nc",
"Cust_Agg_num_football_nc",
"Cust_Agg_num_football_nif",
"Cust_Agg_football_services",
"Cust_Agg_motor_services",
"Cust_Agg_total_price_motor",
"Cust_Agg_pvr_services",
"Cust_Agg_total_price_pvr",
"Cust_Agg_zapper_services",
"Cust_Agg_total_price_zapper",
"Cust_Agg_trybuy_services",
"Cust_Agg_trybuy_autom_services",
"Cust_Agg_num_tariff_maslineasmini",
"Cust_Agg_num_tariff_otros",
"Cust_Agg_total_tv_total_charges",
"Cust_Agg_L2_mobile_fx_first_days_since_nif",
"GNV_Data_L2_total_connections_WE",
"GNV_Data_L2_total_data_volume",
"GNV_Data_L2_total_connections",
"GNV_Data_L2_data_per_connection",
"GNV_Data_L2_max_connections_WE",
"GNV_Data_L2_max_data_per_connection_WE",
"GNV_Type_Voice_COMUNIDAD_YU_VOZ_Num_Of_Calls",
"GNV_Type_Voice_LLAMADAS_VPN_Num_Of_Calls",
"GNV_Type_Voice_NACIONALES_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_OTHER_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ALBANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ALEMANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ARABIA_SAUDITA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ARGENTINA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_AUSTRALIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BULGARIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_COSTA_RICA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CROACIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CUBA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_DINAMARCA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_EL_SALVADOR_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_FRANCIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GEORGIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GIBRALTAR_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_HONG_KONG_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLANDIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_CAIMANES_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ITALIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_KUWAIT_MOU",
"GNV_Type_Voice_INTERNACIONALES_KUWAIT_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LETONIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LITUANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MEJICO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MALI_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MALTA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PAKISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PANAMA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PERU_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PORTUGAL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_QATAR_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_REPUBLICA_CHECA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_REPUBLICA_ESLOVACA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_RUMANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SRI_LANKA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SUECIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TUNEZ_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TURQUIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_UCRANIA_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_AIRE_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_CARREFOUR_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_PEPEPHONE_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_PROCONO_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_TELECABLE_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_TELEFONICA_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_OTHER_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_AMENA_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_AT_EMPRESAS_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ATENCION_AL_CLIENTE_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ATT_AL_CLIENTE_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_CRUZ_ROJA_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_INF_UNI2_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_INFORMACION_ORANGE_1515_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_800_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_800_INTERNACIONAL_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_800_VODAFONE_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_CONFIDENCIAL_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_N1_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_R_GALICIA_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_TELE2_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_VALENCIA_CABLE_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_XTRA_TELECOM_SA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_OTHER_MOU",
"GNV_Type_Voice_OTROS_DESTINOS_OTHER_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_ADM_GENERAL_ESTADO_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_ATENCION_CIUDADANA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_ATT_MUNICIPAL_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_BOMBEROS_LOCAL_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_INF_AUTONOMICA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_EUSKALTEL_SA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_HELLO_TV_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_LINEA_902_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_LINEA_902_N1_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_LINEA_905_SOPORTE_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_POLICIA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_POLICMUNICIPAL_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_URGENC_INSALUD_Num_Of_Calls",
"Bill_N1_Amount_To_Pay",
"Bill_N1_net_charges",
"Bill_N2_Amount_To_Pay",
"Bill_N2_net_charges",
"Bill_N2_num_ids_fict",
"Bill_N3_Amount_To_Pay",
"Bill_N3_net_charges",
"Bill_N3_num_ids_fict",
"Bill_N4_InvoiceCharges",
"Bill_N4_Amount_To_Pay",
"Bill_N4_net_charges",
"Bill_N4_num_ids_fict",
"Bill_N4_num_facturas",
"Bill_N5_net_charges",
"Bill_N5_num_ids_fict",
"Bill_N5_num_facturas",
"Bill_L2_n1_net",
"Bill_L2_n2_net",
"Bill_L2_n3_net",
"Bill_L2_n4_net",
"Bill_L2_n5_net",
"Bill_L2_N1_N2_Amount_To_Pay",
"Bill_L2_N1_N3_Amount_To_Pay",
"Bill_L2_N1_N4_Amount_To_Pay",
"Bill_L2_N1_N5_Amount_To_Pay",
"Bill_L2_N2_N3_Amount_To_Pay",
"Bill_L2_N3_N4_Amount_To_Pay",
"Bill_L2_N4_N5_Amount_To_Pay",
"Camp_L2_srv_total_camps_Retention_Voice",
"Order_L2_N2_StartDate_days_since",
"Order_L2_N3_StartDate_days_since",
"Order_L2_N4_StartDate_days_since",
"Order_L2_N5_StartDate_days_since",
"Order_L2_N6_StartDate_days_since",
"Order_L2_N7_StartDate_days_since",
"Order_L2_N8_StartDate_days_since",
"Order_L2_avg_time_bw_orders",
"Order_days_since_last_order",
"Order_avg_days_per_order",
"Penal_L2_CUST_PENDING_N2_end_date_days_until",
"Penal_L2_CUST_PENDING_N3_end_date_days_until",
"Penal_L2_CUST_PENDING_N4_end_date_days_until",
"Penal_L2_CUST_PENDING_N5_end_date_days_until",
"Penal_L2_CUST_PENDING_end_date_total_max_days_until",
"Penal_L2_SRV_PENDING_N2_end_date_days_until",
"Penal_L2_SRV_PENDING_N3_end_date_days_until",
"Penal_L2_SRV_PENDING_N4_end_date_days_until",
"Penal_L2_SRV_PENDING_N4_penal_amount_total",
"Penal_L2_SRV_PENDING_N5_end_date_days_until",
"device_days_since_device_n3_change_date",
"PER_PREFS_INFO_TRAVELS",
"PER_PREFS_TRAFFIC",
"Spinners_nif_max_days_since_port",
"Spinners_nif_avg_days_since_port",
"Spinners_total_acan",
"Spinners_total_apor",
"Spinners_total_arec",
"Spinners_total_movistar",
"Spinners_total_simyo",
"Spinners_total_jazztel",
"Spinners_total_masmovil",
"Spinners_total_pepephone",
"Spinners_total_unknown",
"Spinners_total_otros",
"Spinners_num_distinct_operators",
"netscout_ns_apps_alipay_total_effective_ul_mb",
"netscout_ns_apps_apple_timestamps",
"netscout_ns_apps_badoo_total_effective_ul_mb",
"netscout_ns_apps_ebay_timestamps",
"netscout_ns_apps_facebook_total_effective_dl_mb",
"netscout_ns_apps_facebook_timestamps",
"netscout_ns_apps_facebook_video_days",
"netscout_ns_apps_facebook_video_timestamps",
"netscout_ns_apps_family_timestamps",
"netscout_ns_apps_foursquare_total_effective_dl_mb",
"netscout_ns_apps_gmail_total_effective_dl_mb",
"netscout_ns_apps_googleplay_days",
"netscout_ns_apps_googleplay_timestamps",
"netscout_ns_apps_hacking_timestamps",
"netscout_ns_apps_instagram_timestamps",
"netscout_ns_apps_itunes_days",
"netscout_ns_apps_itunes_timestamps",
"netscout_ns_apps_legal_total_effective_dl_mb",
"netscout_ns_apps_linkedin_total_effective_dl_mb",
"netscout_ns_apps_linkedin_timestamps",
"netscout_ns_apps_netflix_timestamps",
"netscout_ns_apps_netflixvideo_total_effective_ul_mb",
"netscout_ns_apps_netflixvideo_days",
"netscout_ns_apps_netflixvideo_timestamps",
"netscout_ns_apps_pinterest_total_effective_ul_mb",
"netscout_ns_apps_shopping_days",
"netscout_ns_apps_shopping_timestamps",
"netscout_ns_apps_spotify_timestamps",
"netscout_ns_apps_steam_timestamps",
"netscout_ns_apps_taobao_total_effective_ul_mb",
"netscout_ns_apps_technology_days",
"netscout_ns_apps_tumblr_total_effective_dl_mb",
"netscout_ns_apps_twitter_total_effective_ul_mb",
"netscout_ns_apps_video_timestamps",
"netscout_ns_apps_web_jazztel_https_data_mb",
"netscout_ns_apps_web_jazztel_https_total_effective_dl_mb",
"netscout_ns_apps_web_jazztel_https_timestamps",
"netscout_ns_apps_web_lowi_http_total_effective_dl_mb",
"netscout_ns_apps_web_lowi_https_total_effective_ul_mb",
"netscout_ns_apps_web_lowi_https_days",
"netscout_ns_apps_web_lowi_https_timestamps",
"netscout_ns_apps_web_masmovil_https_total_effective_ul_mb",
"netscout_ns_apps_web_masmovil_https_days",
"netscout_ns_apps_web_o2_https_total_effective_ul_mb",
"netscout_ns_apps_web_o2_https_days",
"netscout_ns_apps_web_o2_https_timestamps",
"netscout_ns_apps_web_pepephone_https_days",
"netscout_ns_apps_web_pepephone_https_timestamps",
"netscout_ns_apps_web_yoigo_https_days",
"netscout_ns_apps_web_yoigo_https_timestamps",
"netscout_ns_apps_web_yomvi_http_total_effective_ul_mb",
"netscout_ns_apps_web_yomvi_http_days",
"netscout_ns_apps_web_yomvi_http_timestamps",
"netscout_ns_apps_web_yomvi_https_total_effective_ul_mb",
"netscout_ns_apps_web_yomvi_https_timestamps",
"netscout_ns_apps_wechat_total_effective_ul_mb",
"netscout_ns_apps_wechat_timestamps",
"netscout_ns_apps_whatsapp_timestamps",
"netscout_ns_apps_whatsapp_voice_calling_total_effective_ul_mb",
"netscout_ns_apps_wikipedia_days",
"netscout_ns_apps_youtube_days",
"netscout_ns_apps_youtube_timestamps",
"tgs_days_until_f_fin_bi",
"tgs_days_until_fecha_fin_dto",
"Reimbursement_adjustment_debt",
"Comp_PEPEPHONE_max_count",
"Comp_PEPEPHONE_min_days_since_navigation",
"Comp_ORANGE_sum_count",
"Comp_ORANGE_max_count",
"Comp_ORANGE_min_days_since_navigation",
"Comp_JAZZTEL_sum_count",
"Comp_JAZZTEL_max_count",
"Comp_JAZZTEL_max_days_since_navigation",
"Comp_JAZZTEL_min_days_since_navigation",
"Comp_JAZZTEL_distinct_days_with_navigation",
"Comp_MOVISTAR_max_count",
"Comp_MOVISTAR_min_days_since_navigation",
"Comp_MASMOVIL_max_count",
"Comp_MASMOVIL_max_days_since_navigation",
"Comp_MASMOVIL_min_days_since_navigation",
"Comp_MASMOVIL_distinct_days_with_navigation",
"Comp_YOIGO_max_count",
"Comp_YOIGO_max_days_since_navigation",
"Comp_YOIGO_min_days_since_navigation",
"Comp_YOIGO_distinct_days_with_navigation",
"Comp_VODAFONE_max_count",
"Comp_VODAFONE_min_days_since_navigation",
"Comp_VODAFONE_distinct_days_with_navigation",
"Comp_LOWI_sum_count",
"Comp_LOWI_max_count",
"Comp_LOWI_max_days_since_navigation",
"Comp_LOWI_min_days_since_navigation",
"Comp_LOWI_distinct_days_with_navigation",
"Comp_O2_max_count",
"Comp_O2_max_days_since_navigation",
"Comp_O2_min_days_since_navigation",
"Comp_O2_distinct_days_with_navigation",
"Comp_unknown_max_days_since_navigation",
"Comp_unknown_min_days_since_navigation",
"Comp_sum_count_comps",
"Comp_max_count_comps",
"Comp_sum_distinct_days_with_navigation_vdf",
"Comp_norm_sum_distinct_days_with_navigation_vdf",
"Tickets_num_tickets_opened",
"Tickets_max_time_opened_tipo_tramitacion",
"Tickets_max_time_opened_tipo_reclamacion",
"Tickets_max_time_opened_tipo_averia",
"Tickets_max_time_opened_tipo_incidencia",
"Tickets_min_time_opened_tipo_tramitacion",
"Tickets_min_time_opened_tipo_reclamacion",
"Tickets_min_time_opened_tipo_averia",
"Tickets_min_time_opened_tipo_incidencia",
"Tickets_std_time_opened_tipo_tramitacion",
"Tickets_std_time_opened_tipo_reclamacion",
"Tickets_std_time_opened_tipo_averia",
"Tickets_std_time_opened_tipo_incidencia",
"Tickets_mean_time_opened_tipo_tramitacion",
"Tickets_mean_time_opened_tipo_reclamacion",
"Tickets_mean_time_opened_tipo_averia",
"Tickets_mean_time_opened_tipo_incidencia",
"Tickets_mean_time_opened",
"Tickets_max_time_opened",
"Tickets_min_time_opened",
"Tickets_std_time_opened",
"Tickets_num_tickets_closed",
"Tickets_max_time_closed_tipo_tramitacion",
"Tickets_max_time_closed_tipo_reclamacion",
"Tickets_max_time_closed_tipo_averia",
"Tickets_max_time_closed_tipo_incidencia",
"Tickets_min_time_closed_tipo_tramitacion",
"Tickets_min_time_closed_tipo_reclamacion",
"Tickets_min_time_closed_tipo_averia",
"Tickets_min_time_closed_tipo_incidencia",
"Tickets_std_time_closed_tipo_tramitacion",
"Tickets_std_time_closed_tipo_reclamacion",
"Tickets_std_time_closed_tipo_averia",
"Tickets_std_time_closed_tipo_incidencia",
"Tickets_mean_time_closed_tipo_tramitacion",
"Tickets_mean_time_closed_tipo_reclamacion",
"Tickets_mean_time_closed_tipo_averia",
"Tickets_mean_time_closed_tipo_incidencia",
"Tickets_num_tickets_tipo_tramitacion",
"Tickets_num_tickets_tipo_reclamacion",
"Tickets_num_tickets_tipo_averia",
"Cust_L2_days_since_fecha_migracion",
"Cust_Agg_fbb_services_nc",
"Cust_Agg_mobile_services_nc",
"Cust_Agg_tv_services_nc",
"Cust_Agg_fbb_services_nif",
"Cust_Agg_bam_services_nc",
"Cust_Agg_max_dias_desde_fx_football_tv",
"Cust_Agg_max_dias_desde_fx_motor_tv",
"Cust_Agg_max_dias_desde_fx_pvr_tv",
"Cust_Agg_max_dias_desde_fx_zapper_tv",
"Cust_Agg_max_dias_desde_fx_trybuy_tv",
"Cust_Agg_mobile_services_nif",
"Cust_Agg_bam_services_nif",
"Cust_Agg_L2_bam_fx_first_days_since_nc",
"Cust_Agg_L2_fbb_fx_first_days_since_nc",
"Cust_Agg_L2_fixed_fx_first_days_since_nc",
"Cust_Agg_L2_tv_fx_first_days_since_nc",
"GNV_Voice_L2_total_mou_w",
"GNV_Voice_L2_total_mou_we",
"GNV_Voice_L2_max_mou_WE",
"GNV_Voice_L2_hour_max_mou_WE",
"GNV_Data_L2_total_data_volume_W",
"GNV_Data_L2_data_per_connection_W",
"GNV_Data_L2_total_data_volume_WE",
"GNV_Data_L2_data_per_connection_WE",
"GNV_Data_L2_total_connections_W",
"GNV_Data_L2_max_data_volume_W",
"GNV_Data_L2_hour_max_data_volume_WE",
"GNV_Type_Voice_TARIFICACION_ADICIONAL_PRESTADOR_MOU",
"GNV_Type_Voice_INTERNACIONALES_ANGOLA_MOU",
"GNV_Type_Voice_INTERNACIONALES_BANGLADESH_MOU",
"GNV_Type_Voice_INTERNACIONALES_BELICE_MOU",
"GNV_Type_Voice_INTERNACIONALES_BIELORRUSIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_BOTSWANA_MOU",
"GNV_Type_Voice_INTERNACIONALES_BRASIL_MOU",
"GNV_Type_Voice_INTERNACIONALES_BURKINA_FASO_MOU",
"GNV_Type_Voice_INTERNACIONALES_CHAD_MOU",
"GNV_Type_Voice_INTERNACIONALES_CHINA_MOU",
"GNV_Type_Voice_INTERNACIONALES_CHIPRE_MOU",
"GNV_Type_Voice_INTERNACIONALES_COREA_REP_MOU",
"GNV_Type_Voice_INTERNACIONALES_COSTA_DE_MARFIL_MOU",
"GNV_Type_Voice_INTERNACIONALES_DOMINICANA_REP_MOU",
"GNV_Type_Voice_INTERNACIONALES_ECUADOR_MOU",
"GNV_Type_Voice_INTERNACIONALES_EGIPTO_MOU",
"GNV_Type_Voice_INTERNACIONALES_ESLOVENIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_FINLANDIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GAMBIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GHANA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUADALUPE_MOU",
"GNV_Type_Voice_INTERNACIONALES_HUNGRIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_IRAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_COOK_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISRAEL_MOU",
"GNV_Type_Voice_INTERNACIONALES_JAPON_MOU",
"GNV_Type_Voice_INTERNACIONALES_KENYA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MADAGASCAR_MOU",
"GNV_Type_Voice_INTERNACIONALES_MARTINICA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MAURITANIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MOLDAVIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MOZAMBIQUE_MOU",
"GNV_Type_Voice_INTERNACIONALES_NIGERIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_NUEVA_CALEDONIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_REUNION_MOU",
"GNV_Type_Voice_INTERNACIONALES_SINGAPUR_MOU",
"GNV_Type_Voice_INTERNACIONALES_SUDAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_MOZAMBIQUE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SUDAFRICA_MOU",
"GNV_Type_Voice_INTERNACIONALES_TAILANDIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_VENEZUELA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ZAIRE_MOU",
"GNV_Type_Voice_MOVILES_OTHER_MOU",
"GNV_Type_Voice_MOVILES_DIA_MOU",
"GNV_Type_Voice_MOVILES_ORBITEL_MOU",
"GNV_Type_Voice_MOVILES_TUENTI_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ASESOR_COMERCIAL_EMPRESAS_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_BOMBEROS_PROV_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_EMERGENCIA_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ESPECIAL_ATENCION_AL_CLIENTE_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_TONOS_DE_ESPERA_MOU",
"Bill_N1_InvoiceCharges",
"Bill_N1_num_ids_fict",
"Bill_N1_Tax_Amount",
"Bill_N2_InvoiceCharges",
"Bill_N1_Debt_Amount",
"Bill_N2_Tax_Amount",
"Bill_N3_InvoiceCharges",
"Bill_N2_Debt_Amount",
"Bill_N3_Tax_Amount",
"Bill_N3_Debt_Amount",
"Bill_N5_InvoiceCharges",
"Bill_N4_Tax_Amount",
"Bill_N5_Amount_To_Pay",
"Bill_N4_Debt_Amount",
"Order_L2_N9_StartDate_days_since",
"Order_L2_N1_StartDate_days_since",
"Order_L2_N10_StartDate_days_since",
"Order_L2_nb_orders",
"Penal_L2_CUST_PENDING_N1_penal_amount",
"Penal_L2_CUST_PENDING_N2_penal_amount",
"Penal_L2_CUST_PENDING_N3_penal_amount",
"Penal_L2_CUST_PENDING_N4_penal_amount",
"Penal_L2_CUST_PENDING_N4_penal_amount_total",
"Penal_L2_CUST_PENDING_N5_penal_amount",
"Penal_L2_CUST_PENDING_N1_end_date_days_until",
"Penal_L2_SRV_PENDING_N1_end_date_days_until",
"device_month_imei_changes",
"device_tenure_days_from_n1",
"device_tenure_days_n2",
"Spinners_nif_port_number",
"Spinners_nif_distinct_msisdn",
"Spinners_orange_ACAN",
"Spinners_yoigo_ACAN",
"Spinners_nif_port_freq_per_msisdn",
"netscout_ns_apps_accuweather_data_mb",
"netscout_ns_apps_accuweather_days",
"netscout_ns_apps_adult_data_mb",
"netscout_ns_apps_adult_days",
"netscout_ns_apps_adultweb_data_mb",
"netscout_ns_apps_adultweb_days",
"netscout_ns_apps_alcohol_data_mb",
"netscout_ns_apps_alibaba_data_mb",
"netscout_ns_apps_alibaba_total_effective_dl_mb",
"netscout_ns_apps_alibaba_days",
"netscout_ns_apps_alipay_data_mb",
"netscout_ns_apps_amazon_data_mb",
"netscout_ns_apps_amazon_days",
"netscout_ns_apps_apple_data_mb",
"netscout_ns_apps_arts_data_mb",
"netscout_ns_apps_arts_days",
"netscout_ns_apps_astrology_data_mb",
"netscout_ns_apps_astrology_days",
"netscout_ns_apps_audio_data_mb",
"netscout_ns_apps_audio_days",
"netscout_ns_apps_auto_data_mb",
"netscout_ns_apps_badoo_data_mb",
"netscout_ns_apps_badoo_days",
"netscout_ns_apps_baidu_data_mb",
"netscout_ns_apps_baidu_days",
"netscout_ns_apps_bbc_data_mb",
"netscout_ns_apps_bbc_days",
"netscout_ns_apps_booking_data_mb",
"netscout_ns_apps_booking_days",
"netscout_ns_apps_books_data_mb",
"netscout_ns_apps_business_data_mb",
"netscout_ns_apps_business_days",
"netscout_ns_apps_chats_data_mb",
"netscout_ns_apps_classified_data_mb",
"netscout_ns_apps_dating_data_mb",
"netscout_ns_apps_dating_days",
"netscout_ns_apps_dropbox_data_mb",
"netscout_ns_apps_dropbox_days",
"netscout_ns_apps_ebay_data_mb",
"netscout_ns_apps_ebay_days",
"netscout_ns_apps_education_data_mb",
"netscout_ns_apps_facebookmessages_data_mb",
"netscout_ns_apps_facebook_days",
"netscout_ns_apps_facebookmessages_days",
"netscout_ns_apps_facebook_video_data_mb",
"netscout_ns_apps_facebook_video_total_effective_dl_mb",
"netscout_ns_apps_facebook_data_mb",
"netscout_ns_apps_family_data_mb",
"netscout_ns_apps_family_days",
"netscout_ns_apps_fashion_data_mb",
"netscout_ns_apps_finance_data_mb",
"netscout_ns_apps_finance_days",
"netscout_ns_apps_food_data_mb",
"netscout_ns_apps_food_days",
"netscout_ns_apps_foursquare_data_mb",
"netscout_ns_apps_foursquare_days",
"netscout_ns_apps_gambling_data_mb",
"netscout_ns_apps_gambling_days",
"netscout_ns_apps_github_data_mb",
"netscout_ns_apps_github_days",
"netscout_ns_apps_gmail_days",
"netscout_ns_apps_googledrive_data_mb",
"netscout_ns_apps_googledrive_days",
"netscout_ns_apps_googleearth_data_mb",
"netscout_ns_apps_googleearth_days",
"netscout_ns_apps_googlemaps_data_mb",
"netscout_ns_apps_googlemaps_days",
"netscout_ns_apps_googleplay_data_mb",
"netscout_ns_apps_groupon_data_mb",
"netscout_ns_apps_groupon_days",
"netscout_ns_apps_hacking_data_mb",
"netscout_ns_apps_home_data_mb",
"netscout_ns_apps_home_days",
"netscout_ns_apps_instagram_data_mb",
"netscout_ns_apps_instagram_total_effective_dl_mb",
"netscout_ns_apps_instagram_days",
"netscout_ns_apps_itunes_data_mb",
"netscout_ns_apps_jobs_data_mb",
"netscout_ns_apps_jobs_days",
"netscout_ns_apps_kids_data_mb",
"netscout_ns_apps_kids_days",
"netscout_ns_apps_legal_data_mb",
"netscout_ns_apps_legal_total_effective_ul_mb",
"netscout_ns_apps_line_data_mb",
"netscout_ns_apps_linkedin_data_mb",
"netscout_ns_apps_linkedin_days",
"netscout_ns_apps_medical_data_mb",
"netscout_ns_apps_music_data_mb",
"netscout_ns_apps_music_days",
"netscout_ns_apps_netflix_data_mb",
"netscout_ns_apps_netflix_days",
"netscout_ns_apps_netflix_total_effective_ul_mb",
"netscout_ns_apps_netflixvideo_data_mb",
"netscout_ns_apps_news_data_mb",
"netscout_ns_apps_news_days",
"netscout_ns_apps_paypal_data_mb",
"netscout_ns_apps_paypal_days",
"netscout_ns_apps_pets_data_mb",
"netscout_ns_apps_pinterest_data_mb",
"netscout_ns_apps_pinterest_days",
"netscout_ns_apps_politics_data_mb",
"netscout_ns_apps_pregnancy_data_mb",
"netscout_ns_apps_pregnancy_days",
"netscout_ns_apps_qq_data_mb",
"netscout_ns_apps_qq_days",
"netscout_ns_apps_reddit_data_mb",
"netscout_ns_apps_reddit_days",
"netscout_ns_apps_samsung_data_mb",
"netscout_ns_apps_samsung_days",
"netscout_ns_apps_science_data_mb",
"netscout_ns_apps_science_days",
"netscout_ns_apps_shopping_data_mb",
"netscout_ns_apps_skype_data_mb",
"netscout_ns_apps_skype_days",
"netscout_ns_apps_snapchat_data_mb",
"netscout_ns_apps_snapchat_days",
"netscout_ns_apps_socialnetwork_data_mb",
"netscout_ns_apps_socialnetwork_total_effective_dl_mb",
"netscout_ns_apps_sports_data_mb",
"netscout_ns_apps_sports_days",
"netscout_ns_apps_spotify_data_mb",
"netscout_ns_apps_spotify_days",
"netscout_ns_apps_steam_data_mb",
"netscout_ns_apps_taobao_data_mb",
"netscout_ns_apps_taobao_days",
"netscout_ns_apps_technology_data_mb",
"netscout_ns_apps_travel_data_mb",
"netscout_ns_apps_twitch_data_mb",
"netscout_ns_apps_twitch_days",
"netscout_ns_apps_twitter_data_mb",
"netscout_ns_apps_twitter_days",
"netscout_ns_apps_video_data_mb",
"netscout_ns_apps_video_total_effective_dl_mb",
"netscout_ns_apps_video_days",
"netscout_ns_apps_videostreaming_data_mb",
"netscout_ns_apps_videostreaming_days",
"netscout_ns_apps_vimeo_data_mb",
"netscout_ns_apps_vimeo_days",
"netscout_ns_apps_violence_data_mb",
"netscout_ns_apps_violence_days",
"netscout_ns_apps_webgames_data_mb",
"netscout_ns_apps_webgames_days",
"netscout_ns_apps_webmobile_data_mb",
"netscout_ns_apps_webmobile_days",
"netscout_ns_apps_web_jazztel_http_data_mb",
"netscout_ns_apps_web_jazztel_http_timestamps",
"netscout_ns_apps_web_lowi_http_data_mb",
"netscout_ns_apps_web_lowi_http_days",
"netscout_ns_apps_web_lowi_https_data_mb",
"netscout_ns_apps_web_masmovil_http_data_mb",
"netscout_ns_apps_web_masmovil_http_days",
"netscout_ns_apps_web_masmovil_https_data_mb",
"netscout_ns_apps_web_masmovil_http_timestamps",
"netscout_ns_apps_web_movistar_http_data_mb",
"netscout_ns_apps_web_movistar_https_data_mb",
"netscout_ns_apps_web_o2_http_data_mb",
"netscout_ns_apps_web_o2_http_days",
"netscout_ns_apps_web_o2_http_timestamps",
"netscout_ns_apps_web_o2_https_data_mb",
"netscout_ns_apps_web_orange_http_data_mb",
"netscout_ns_apps_web_orange_http_total_effective_dl_mb",
"netscout_ns_apps_web_orange_https_data_mb",
"netscout_ns_apps_web_pepephone_http_data_mb",
"netscout_ns_apps_web_pepephone_http_days",
"netscout_ns_apps_web_pepephone_https_data_mb",
"netscout_ns_apps_web_vodafone_http_data_mb",
"netscout_ns_apps_web_vodafone_http_days",
"netscout_ns_apps_web_vodafone_https_data_mb",
"netscout_ns_apps_web_vodafone_https_days",
"netscout_ns_apps_web_yoigo_http_data_mb",
"netscout_ns_apps_web_yoigo_http_days",
"netscout_ns_apps_web_yoigo_https_data_mb",
"netscout_ns_apps_web_yoigo_http_timestamps",
"netscout_ns_apps_sports_total_effective_dl_mb",
"netscout_ns_apps_web_yomvi_http_data_mb",
"netscout_ns_apps_web_yomvi_https_data_mb",
"netscout_ns_apps_web_yomvi_https_days",
"netscout_ns_apps_wechat_data_mb",
"netscout_ns_apps_whatsapp_data_mb",
"netscout_ns_apps_whatsapp_media_message_data_mb",
"netscout_ns_apps_whatsapp_media_message_days",
"netscout_ns_apps_whatsapp_voice_calling_data_mb",
"netscout_ns_apps_whatsapp_voice_calling_days",
"netscout_ns_apps_wikipedia_data_mb",
"netscout_ns_apps_wikipedia_total_effective_ul_mb",
"netscout_ns_apps_yandex_data_mb",
"netscout_ns_apps_youtube_data_mb",
"netscout_ns_apps_zynga_data_mb",
"tgs_days_since_f_inicio_dto",
"Reimbursement_days_since",
"netscout_ns_apps_web_pepephone_https_total_effective_dl_mb",
"Comp_PEPEPHONE_max_days_since_navigation",
"netscout_ns_apps_web_jazztel_https_days",
"netscout_ns_apps_web_movistar_https_total_effective_ul_mb",
"netscout_ns_apps_web_movistar_https_timestamps",
"Comp_ORANGE_max_days_since_navigation",
"netscout_ns_apps_web_yoigo_https_total_effective_dl_mb",
"Comp_MOVISTAR_max_days_since_navigation",
"netscout_ns_apps_web_o2_https_total_effective_dl_mb",
"Comp_VODAFONE_sum_count",
"Comp_num_distinct_comps",
"Comp_sum_distinct_days_with_navigation_comps",
"Comp_min_days_since_navigation_comps",
"Comp_max_days_since_navigation_comps",
"Cust_Agg_bam_mobile_services_nc",
"Cust_Agg_bam_mobile_services_nif",
"Cust_Agg_total_football_price_nc",
"Cust_Agg_total_football_price_nif",
"Cust_Agg_total_price_football",
"Cust_Agg_total_price_trybuy",
"Cust_Agg_total_price_trybuy_autom",
"Cust_Agg_num_tariff_unknown",
"Cust_Agg_L2_bam_mobile_fx_first_days_since_nc",
"Cust_Agg_L2_bam_mobile_fx_first_days_since_nif",
"GNV_Type_Voice_INTERNACIONALES_AFGANISTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_AFGANISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ALASKA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ALASKA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ANGUILLA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ANGUILLA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ANTIGUA_Y_BARBUDA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ANTIGUA_Y_BARBUDA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ANTILLAS_HOLANDESAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_ANTILLAS_HOLANDESAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ARMENIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ARMENIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ARUBA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ARUBA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_AZERBAIYAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_AZERBAIYAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BAHAMAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_BAHAMAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BAHREIN_MOU",
"GNV_Type_Voice_INTERNACIONALES_BAHREIN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BARBADOS_MOU",
"GNV_Type_Voice_INTERNACIONALES_BARBADOS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BENIN_MOU",
"GNV_Type_Voice_INTERNACIONALES_BENIN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BERMUDAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_BERMUDAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BHUTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_BHUTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BIRMANIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_BIRMANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOSTAR_MOU",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_MOSTAR_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_SRPSKE_MOU",
"GNV_Type_Voice_INTERNACIONALES_BOSNIA_SRPSKE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_BRUNEI_MOU",
"GNV_Type_Voice_INTERNACIONALES_BRUNEI_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CABO_VERDE_MOU",
"GNV_Type_Voice_INTERNACIONALES_CABO_VERDE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CAMBOYA_MOU",
"GNV_Type_Voice_INTERNACIONALES_CAMBOYA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CAMERUN_MOU",
"GNV_Type_Voice_INTERNACIONALES_CAMERUN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_CONGO_MOU",
"GNV_Type_Voice_INTERNACIONALES_CONGO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ECUADOR__PORTA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ECUADOR__PORTA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_MOVIL_MOU",
"GNV_Type_Voice_INTERNACIONALES_EMIRATOS_ARABES_UNIDOS_MOVIL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ETIOPIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ETIOPIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_FILIPINAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_FILIPINAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_FIYI_MOU",
"GNV_Type_Voice_INTERNACIONALES_FIYI_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GABON_MOU",
"GNV_Type_Voice_INTERNACIONALES_GABON_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUAM_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUAM_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUATEMALA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUATEMALA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUAYANA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUAYANA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUAYANA_FRANCESA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUAYANA_FRANCESA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUINEA_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUINEA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_GUINEABISSAU_MOU",
"GNV_Type_Voice_INTERNACIONALES_GUINEABISSAU_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_HAITI_MOU",
"GNV_Type_Voice_INTERNACIONALES_HAITI_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_HAWAI_MOU",
"GNV_Type_Voice_INTERNACIONALES_HAWAI_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_AMERICANAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_AMERICANAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_BRITANICAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_I_VIRGENES_BRITANICAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_INDONESIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_INDONESIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_IRAQ_MOU",
"GNV_Type_Voice_INTERNACIONALES_IRAQ_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLA_NIUE_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLA_NIUE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_FEROE_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_FEROE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_MALVINAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_MALVINAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_MARSHALL_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_MARSHALL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_SALOMON_MOU",
"GNV_Type_Voice_INTERNACIONALES_ISLAS_SALOMON_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_JAMAICA_MOU",
"GNV_Type_Voice_INTERNACIONALES_JAMAICA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_JORDANIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_JORDANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_KAZAJASTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_KAZAJASTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_KIRGUIZISTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_KIRGUIZISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_KOSOVO_MOU",
"GNV_Type_Voice_INTERNACIONALES_KOSOVO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LIBANO_MOU",
"GNV_Type_Voice_INTERNACIONALES_LIBANO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LAOS_MOU",
"GNV_Type_Voice_INTERNACIONALES_LAOS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LESOTHO_MOU",
"GNV_Type_Voice_INTERNACIONALES_LESOTHO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LIBERIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_LIBERIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LIBIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_LIBIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_LIECHTENSTEIN_MOU",
"GNV_Type_Voice_INTERNACIONALES_LIECHTENSTEIN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MONACO_MOU",
"GNV_Type_Voice_INTERNACIONALES_MONACO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MACAO_MOU",
"GNV_Type_Voice_INTERNACIONALES_MACAO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MACEDONIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MACEDONIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MALASIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MALASIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MALDIVAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_MALDIVAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MAURICIO_MOU",
"GNV_Type_Voice_INTERNACIONALES_MAURICIO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MAYOTTE_MOU",
"GNV_Type_Voice_INTERNACIONALES_MAYOTTE_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MONGOLIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_MONGOLIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_MONTENEGRO_MOU",
"GNV_Type_Voice_INTERNACIONALES_MONTENEGRO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_NAMIBIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_NAMIBIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_NEPAL_MOU",
"GNV_Type_Voice_INTERNACIONALES_NEPAL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_NIGER_MOU",
"GNV_Type_Voice_INTERNACIONALES_NIGER_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_OMAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_OMAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PALAU_MOU",
"GNV_Type_Voice_INTERNACIONALES_PALAU_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PALESTINA_MOU",
"GNV_Type_Voice_INTERNACIONALES_PALESTINA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_PAPUA_NUEVA_GUINEA_MOU",
"GNV_Type_Voice_INTERNACIONALES_PAPUA_NUEVA_GUINEA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_POLINESIA_FRANCESA_MOU",
"GNV_Type_Voice_INTERNACIONALES_POLINESIA_FRANCESA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_REPUBLICA_CENTROAFRICANA_MOU",
"GNV_Type_Voice_INTERNACIONALES_REPUBLICA_CENTROAFRICANA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_RUANDA_MOU",
"GNV_Type_Voice_INTERNACIONALES_RUANDA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SAN_MARINO_MOU",
"GNV_Type_Voice_INTERNACIONALES_SAN_MARINO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SAN_MARTIN_MOU",
"GNV_Type_Voice_INTERNACIONALES_SAN_MARTIN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SAN_PEDRO_Y_MIQUELON_MOU",
"GNV_Type_Voice_INTERNACIONALES_SAN_PEDRO_Y_MIQUELON_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SAN_VICENTE_Y_GRANADINAS_MOU",
"GNV_Type_Voice_INTERNACIONALES_SAN_VICENTE_Y_GRANADINAS_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SANTO_TOME_Y_PRINCIPE_MOVIL_MOU",
"GNV_Type_Voice_INTERNACIONALES_SANTO_TOME_Y_PRINCIPE_MOVIL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SERBIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_SERBIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SEYCHELLES_MOU",
"GNV_Type_Voice_INTERNACIONALES_SEYCHELLES_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SIERRA_LEONA_MOU",
"GNV_Type_Voice_INTERNACIONALES_SIERRA_LEONA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SIRIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_SIRIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SOMALIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_SOMALIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_STA_LUCIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_STA_LUCIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SURINAM_MOU",
"GNV_Type_Voice_INTERNACIONALES_SURINAM_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_SWAZILANDIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_SWAZILANDIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TADJIKISTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_TADJIKISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TAIWAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_TAIWAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TANZANIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_TANZANIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TIMOR_ORIENTAL_MOU",
"GNV_Type_Voice_INTERNACIONALES_TIMOR_ORIENTAL_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TOGO_MOU",
"GNV_Type_Voice_INTERNACIONALES_TOGO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TRINIDAD_Y_TOBAGO_MOU",
"GNV_Type_Voice_INTERNACIONALES_TRINIDAD_Y_TOBAGO_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_TURKMENISTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_TURKMENISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_UGANDA_MOU",
"GNV_Type_Voice_INTERNACIONALES_UGANDA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_UZBEKISTAN_MOU",
"GNV_Type_Voice_INTERNACIONALES_UZBEKISTAN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_VIETNAM_MOU",
"GNV_Type_Voice_INTERNACIONALES_VIETNAM_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_YEMEN_MOU",
"GNV_Type_Voice_INTERNACIONALES_YEMEN_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ZAMBIA_MOU",
"GNV_Type_Voice_INTERNACIONALES_ZAMBIA_Num_Of_Calls",
"GNV_Type_Voice_INTERNACIONALES_ZIMBABWE_MOU",
"GNV_Type_Voice_INTERNACIONALES_ZIMBABWE_Num_Of_Calls",
"GNV_Type_Voice_MOVILES_BARABLU_MOU",
"GNV_Type_Voice_MOVILES_BARABLU_Num_Of_Calls",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_MOU",
"GNV_Type_Voice_SERVICIOS_GRATUITOS_ASISTENCIA_TECNICA_Num_Of_Calls",
"GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_MOU",
"GNV_Type_Voice_OTROS_DESTINOS_INFORMACION_AVYS_TELECOM_Num_Of_Calls",
"Camp_NIFs_Retention_Voice_EMA_Universal_0",
"Camp_NIFs_Retention_Voice_EMA_Universal_1",
"Camp_NIFs_Retention_Voice_TEL_Target_0",
"Camp_NIFs_Retention_Voice_TEL_Target_1",
"Camp_NIFs_Retention_Voice_TEL_Control_0",
"Camp_NIFs_Retention_Voice_TEL_Control_1",
"Camp_NIFs_Retention_Voice_TEL_Universal_0",
"Camp_NIFs_Retention_Voice_TEL_Universal_1",
"Camp_NIFs_Retention_Voice_SMS_Universal_0",
"Camp_NIFs_Retention_Voice_SMS_Universal_1",
"Camp_NIFs_Retention_Voice_MMS_Target_0",
"Camp_NIFs_Retention_Voice_MMS_Target_1",
"Camp_NIFs_Retention_Voice_MMS_Control_0",
"Camp_NIFs_Retention_Voice_MMS_Control_1",
"Camp_NIFs_Retention_Voice_MMS_Universal_0",
"Camp_NIFs_Retention_Voice_MMS_Universal_1",
"Camp_NIFs_Up_Cross_Sell_EMA_Universal_0",
"Camp_NIFs_Up_Cross_Sell_EMA_Universal_1",
"Camp_NIFs_Up_Cross_Sell_SAT_Target_0",
"Camp_NIFs_Up_Cross_Sell_SAT_Target_1",
"Camp_NIFs_Up_Cross_Sell_SAT_Universal_0",
"Camp_NIFs_Up_Cross_Sell_SAT_Universal_1",
"Camp_NIFs_Up_Cross_Sell_SMS_Target_0",
"Camp_NIFs_Up_Cross_Sell_SMS_Universal_1",
"Camp_NIFs_Up_Cross_Sell_MMS_Universal_0",
"Camp_NIFs_Up_Cross_Sell_MMS_Universal_1",
"Camp_L2_nif_total_camps_Control",
"Camp_L2_nif_pcg_redem_Control",
"Camp_L2_nif_total_camps_Universal",
"Camp_L2_nif_pcg_redem_Universal",
"Camp_L2_nif_total_camps_Retention_Voice",
"Camp_L2_nif_pcg_redem_Retention_Voice",
"Camp_L2_nif_pcg_redem_Up_Cross_Sell",
"Camp_L2_nif_total_camps_EMA",
"Camp_L2_nif_pcg_redem_EMA",
"Camp_L2_nif_pcg_redem_TEL",
"Camp_L2_nif_pcg_redem_SAT",
"Camp_L2_nif_total_camps_SMS",
"Camp_L2_nif_pcg_redem_MMS",
"Camp_SRV_Retention_Voice_EMA_Universal_0",
"Camp_SRV_Retention_Voice_EMA_Universal_1",
"Camp_SRV_Retention_Voice_SMS_Target_0",
"Camp_SRV_Retention_Voice_MMS_Target_0",
"Camp_SRV_Retention_Voice_MMS_Target_1",
"Camp_SRV_Retention_Voice_MMS_Control_0",
"Camp_SRV_Retention_Voice_MMS_Control_1",
"Camp_SRV_Retention_Voice_MMS_Universal_0",
"Camp_SRV_Retention_Voice_MMS_Universal_1",
"Camp_SRV_Up_Cross_Sell_EMA_Universal_1",
"Camp_SRV_Up_Cross_Sell_SAT_Target_0",
"Camp_SRV_Up_Cross_Sell_SAT_Target_1",
"Camp_SRV_Up_Cross_Sell_SAT_Control_0",
"Camp_SRV_Up_Cross_Sell_SAT_Control_1",
"Camp_SRV_Up_Cross_Sell_SAT_Universal_0",
"Camp_SRV_Up_Cross_Sell_SAT_Universal_1",
"Camp_SRV_Up_Cross_Sell_SMS_Target_0",
"Camp_SRV_Up_Cross_Sell_MMS_Universal_0",
"Camp_SRV_Up_Cross_Sell_MMS_Universal_1",
"Camp_L2_srv_pcg_redem_Control",
"Camp_L2_srv_total_camps_Universal",
"Camp_L2_srv_pcg_redem_Universal",
"Camp_L2_srv_pcg_redem_Retention_Voice",
"Camp_L2_srv_pcg_redem_Up_Cross_Sell",
"Camp_L2_srv_total_camps_EMA",
"Camp_L2_srv_pcg_redem_EMA",
"Camp_L2_srv_pcg_redem_TEL",
"Camp_L2_srv_pcg_redem_SAT",
"Camp_L2_srv_total_camps_SMS",
"Camp_L2_srv_pcg_redem_MMS",
"Order_Agg_Activa_Promo_CAN_orders",
"Order_Agg_Descon_Prepago_CAN_orders",
"Order_Agg_Descon_Prepago_COM_orders",
"Order_Agg_Descon_Prepago_PEN_orders",
"Order_Agg_Factura_Canal_Presencial_CAN_orders",
"Order_Agg_Factura_Terminal_CAN_orders",
"Order_Agg_Factura_Terminal_PEN_orders",
"Order_Agg_Fideliza_cliente_PEN_orders",
"Order_Agg_Instalacion_SecureNet_CAN_orders",
"Order_Agg_Instalacion_SecureNet_COM_orders",
"Order_Agg_Instalacion_SecureNet_PEN_orders",
"PER_PREFS_LORTAD",
"Spinners_movistar_PCAN",
"Spinners_movistar_AACE",
"Spinners_movistar_AENV",
"Spinners_simyo_PCAN",
"Spinners_simyo_AACE",
"Spinners_simyo_AENV",
"Spinners_orange_PCAN",
"Spinners_orange_AACE",
"Spinners_jazztel_PCAN",
"Spinners_jazztel_AACE",
"Spinners_jazztel_AENV",
"Spinners_yoigo_AACE",
"Spinners_masmovil_PCAN",
"Spinners_masmovil_AACE",
"Spinners_masmovil_AENV",
"Spinners_pepephone_PCAN",
"Spinners_pepephone_AACE",
"Spinners_pepephone_AENV",
"Spinners_reuskal_ACON",
"Spinners_reuskal_ASOL",
"Spinners_reuskal_PCAN",
"Spinners_reuskal_ACAN",
"Spinners_reuskal_AACE",
"Spinners_reuskal_AENV",
"Spinners_reuskal_APOR",
"Spinners_reuskal_AREC",
"Spinners_unknown_PCAN",
"Spinners_unknown_AACE",
"Spinners_otros_AACE",
"Spinners_otros_AENV",
"Spinners_total_reuskal",
"netscout_ns_apps_web_jazztel_data_mb",
"netscout_ns_apps_web_jazztel_total_effective_dl_mb",
"netscout_ns_apps_web_jazztel_total_effective_ul_mb",
"netscout_ns_apps_web_jazztel_days",
"netscout_ns_apps_web_jazztel_timestamps",
"netscout_ns_apps_web_lowi_data_mb",
"netscout_ns_apps_web_lowi_total_effective_dl_mb",
"netscout_ns_apps_web_lowi_total_effective_ul_mb",
"netscout_ns_apps_web_lowi_days",
"netscout_ns_apps_web_lowi_timestamps",
"netscout_ns_apps_web_marcacom_http_data_mb",
"netscout_ns_apps_web_marcacom_http_total_effective_dl_mb",
"netscout_ns_apps_web_marcacom_http_total_effective_ul_mb",
"netscout_ns_apps_web_marcacom_http_days",
"netscout_ns_apps_web_marcacom_http_timestamps",
"netscout_ns_apps_web_marcacom_https_data_mb",
"netscout_ns_apps_web_marcacom_https_total_effective_dl_mb",
"netscout_ns_apps_web_marcacom_https_total_effective_ul_mb",
"netscout_ns_apps_web_marcacom_https_days",
"netscout_ns_apps_web_marcacom_https_timestamps",
"netscout_ns_apps_web_masmovil_data_mb",
"netscout_ns_apps_web_masmovil_total_effective_dl_mb",
"netscout_ns_apps_web_masmovil_total_effective_ul_mb",
"netscout_ns_apps_web_masmovil_days",
"netscout_ns_apps_web_masmovil_timestamps",
"netscout_ns_apps_web_movistar_data_mb",
"netscout_ns_apps_web_movistar_total_effective_dl_mb",
"netscout_ns_apps_web_movistar_total_effective_ul_mb",
"netscout_ns_apps_web_movistar_days",
"netscout_ns_apps_web_movistar_timestamps",
"netscout_ns_apps_web_o2_data_mb",
"netscout_ns_apps_web_o2_total_effective_dl_mb",
"netscout_ns_apps_web_o2_total_effective_ul_mb",
"netscout_ns_apps_web_o2_days",
"netscout_ns_apps_web_o2_timestamps",
"netscout_ns_apps_web_orange_data_mb",
"netscout_ns_apps_web_orange_total_effective_dl_mb",
"netscout_ns_apps_web_orange_total_effective_ul_mb",
"netscout_ns_apps_web_orange_days",
"netscout_ns_apps_web_orange_timestamps",
"netscout_ns_apps_web_pepephone_data_mb",
"netscout_ns_apps_web_pepephone_total_effective_dl_mb",
"netscout_ns_apps_web_pepephone_total_effective_ul_mb",
"netscout_ns_apps_web_pepephone_days",
"netscout_ns_apps_web_pepephone_timestamps",
"netscout_ns_apps_web_vodafone_data_mb",
"netscout_ns_apps_web_vodafone_total_effective_dl_mb",
"netscout_ns_apps_web_vodafone_total_effective_ul_mb",
"netscout_ns_apps_web_vodafone_days",
"netscout_ns_apps_web_vodafone_timestamps",
"netscout_ns_apps_web_yoigo_data_mb",
"netscout_ns_apps_web_yoigo_total_effective_dl_mb",
"netscout_ns_apps_web_yoigo_total_effective_ul_mb",
"netscout_ns_apps_web_yoigo_days",
"netscout_ns_apps_web_yoigo_timestamps",
"tgs_sum_ind_under_use",
"tgs_sum_ind_over_use",
"tgs_days_since_f_inicio_bi_exp",
"tgs_days_until_f_fin_bi_exp",
"Pbms_srv_num_reclamaciones_ini_w2",
"Pbms_srv_num_reclamaciones_prev_w2",
"Pbms_srv_num_reclamaciones_w2vsw2",
"Pbms_srv_num_reclamaciones_ini_w4",
"Pbms_srv_num_reclamaciones_prev_w4",
"Pbms_srv_num_reclamaciones_w4vsw4",
"Pbms_srv_num_reclamaciones_ini_w8",
"Pbms_srv_num_reclamaciones_prev_w8",
"Pbms_srv_num_reclamaciones_w8vsw8",
"Pbms_srv_num_averias_ini_w2",
"Pbms_srv_num_averias_prev_w2",
"Pbms_srv_num_averias_w2vsw2",
"Pbms_srv_num_averias_ini_w4",
"Pbms_srv_num_averias_prev_w4",
"Pbms_srv_num_averias_w4vsw4",
"Pbms_srv_num_averias_ini_w8",
"Pbms_srv_num_averias_prev_w8",
"Pbms_srv_num_averias_w8vsw8",
"Pbms_srv_num_soporte_tecnico_ini_w2",
"Pbms_srv_num_soporte_tecnico_prev_w2",
"Pbms_srv_num_soporte_tecnico_w2vsw2",
"Pbms_srv_num_soporte_tecnico_ini_w4",
"Pbms_srv_num_soporte_tecnico_prev_w4",
"Pbms_srv_num_soporte_tecnico_w4vsw4",
"Pbms_srv_num_soporte_tecnico_ini_w8",
"Pbms_srv_num_soporte_tecnico_prev_w8",
"Pbms_srv_num_soporte_tecnico_w8vsw8",
"Pbms_srv_ind_reclamaciones",
"Pbms_srv_ind_averias",
"Pbms_srv_ind_soporte",
"NUM_RECLAMACIONES_NIF_ini_w2",
"NUM_RECLAMACIONES_NIF_prev_w2",
"NUM_RECLAMACIONES_NIF_w2vsw2",
"NUM_RECLAMACIONES_NIF_ini_w4",
"NUM_RECLAMACIONES_NIF_prev_w4",
"NUM_RECLAMACIONES_NIF_w4vsw4",
"NUM_RECLAMACIONES_NIF_ini_w8",
"NUM_RECLAMACIONES_NIF_prev_w8",
"NUM_RECLAMACIONES_NIF_w8vsw8",
"NUM_AVERIAS_NIF_ini_w2",
"NUM_AVERIAS_NIF_prev_w2",
"NUM_AVERIAS_NIF_w2vsw2",
"NUM_AVERIAS_NIF_ini_w4",
"NUM_AVERIAS_NIF_prev_w4",
"NUM_AVERIAS_NIF_w4vsw4",
"NUM_AVERIAS_NIF_ini_w8",
"NUM_AVERIAS_NIF_prev_w8",
"NUM_AVERIAS_NIF_w8vsw8",
"NUM_SOPORTE_TECNICO_NIF_ini_w2",
"NUM_SOPORTE_TECNICO_NIF_prev_w2",
"NUM_SOPORTE_TECNICO_NIF_w2vsw2",
"NUM_SOPORTE_TECNICO_NIF_ini_w4",
"NUM_SOPORTE_TECNICO_NIF_prev_w4",
"NUM_SOPORTE_TECNICO_NIF_w4vsw4",
"NUM_SOPORTE_TECNICO_NIF_ini_w8",
"NUM_SOPORTE_TECNICO_NIF_prev_w8",
"NUM_SOPORTE_TECNICO_NIF_w8vsw8",
"Reimbursement_num_n8",
"Reimbursement_num_n6",
"Reimbursement_num_n4",
"Reimbursement_num_n5",
"Reimbursement_num_n2",
"Reimbursement_num_n3",
"Reimbursement_num_month_2",
"Reimbursement_num_n7",
"Reimbursement_num_n1",
"Reimbursement_num_month_1",
"Comp_unknown_sum_count",
"Comp_unknown_max_count",
"Comp_unknown_distinct_days_with_navigation",
"Tickets_num_tickets_tipo_incidencia_opened",
"Tickets_num_tickets_tipo_incidencia_closed",
"Tickets_weeks_averias",
"Tickets_weeks_facturacion",
"Tickets_weeks_reclamacion",
"Tickets_weeks_incidencias",
"Tickets_num_tickets_tipo_incidencia"]


    return corr_vars