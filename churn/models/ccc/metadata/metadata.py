from itertools import chain

def flatten(listOfLists):
    return list(chain.from_iterable(listOfLists))

def getIdFeats():
    return(list(["msisdn",
      "rgu",
      "num_cliente",
      "campo1",
      "campo2",
      "campo3"]))

############# GNV #############
def getCatFeatsGnv():
    return([])

def getGnvHours(): 
    return(list(range(0,24)))

def getGnvUsageTypes():
    return(list(["Chat_Zero",
    "Maps_Pass",
    "VideoHD_Pass",
    "Video_Pass",
    "MasMegas",
    "Music_Pass",
    "Social_Pass",
    "RegularData"]))

def getGnvPeriods(): 
    return(list(["W", "WE"]))

def getInitialGnvColumns():
    gnv_hours = getGnvHours()
    gnv_periods = getGnvPeriods()
    gnv_types = getGnvUsageTypes()
    gnv_data_consumption_init =[]
    gnv_data_connections_init = []
    gnv_call_mou_init = []
    gnv_call_num_init = []
    for h,p,t in [(h,p,t) for h in gnv_hours for p in gnv_periods for t in gnv_types]:
        gnv_data_consumption_init.append("GNV_hour_" + str(h) + "_" + p + "_" + t + "_Data_Volume_MB")
        gnv_data_connections_init.append("GNV_hour_" + str(h) + "_" + p + "_" + t + "_Num_Of_Connections")
    for h2,p2 in [(h2,p2) for h2 in gnv_hours for p2 in gnv_periods]:                   
        gnv_call_mou_init.append( "GNV_hour_" + str(h) + "_" + p + "_MOU")
        gnv_call_num_init.append("GNV_hour_" + str(h) + "_" + p + "_Num_Of_Calls")
    return(gnv_data_connections_init+gnv_data_consumption_init+gnv_call_mou_init+gnv_call_num_init)


def getNewGnvColumns():
    gnv_hours = getGnvHours()
    gnv_hourly_aggs = []
    for h in gnv_hours:
        gnv_hourly_aggs.append(["total_data_volume_w_" + str(h),
        "total_connections_w_" + str(h),
        "data_per_connection_w_" + str(h),
        "total_data_volume_we_" + str(h),
        "total_connections_we_" + str(h),
        "data_per_connection_we_" + str(h),
        "mou_per_call_w_" + str(h),
        "mou_per_call_we_" + str(h)])
    gnv_hourly_aggs = flatten(gnv_hourly_aggs)
#    return(gnv_hourly_aggs)
    gnv_daily_aggs = ["total_data_volume_w",
          "total_data_volume_we",
          "total_data_volume",
          "total_connections_w",
          "total_connections_we",
          "total_connections",
          "data_per_connection_w",
          "data_per_connection_we",
          "data_per_connection",
          "total_mou_w",
          "total_mou_we"]
    gnv_max_aggs = ["max_data_volume_we",
          "hour_max_data_volume_we",
          "max_connections_we",
          "hour_max_connections_we",
          "max_data_per_connection_we",
          "hour_max_data_per_connection_we",

          "max_data_volume_w",
          "hour_max_data_volume_w",
          "max_connections_w",
          "hour_max_connections_w",
          "max_data_per_connection_w",
          "hour_max_data_per_connection_w",

          "max_mou_w",
          "hour_max_mou_w",
          "max_num_calls_w",
          "hour_max_num_calls_w",
          "max_mou_per_call_w",
          "hour_max_mou_per_call_w",

          "max_mou_we",
          "hour_max_mou_we",
          "max_num_calls_we",
          "hour_max_num_calls_we",
          "max_mou_per_call_we",
          "hour_max_mou_per_call_we"]
    return(gnv_hourly_aggs+gnv_daily_aggs+gnv_max_aggs)


def getProcessedGnvColumns(): 
    return(getInitialGnvColumns() + getNewGnvColumns())

############# CAMPAIGNS #############

def getCatFeatsCamp(): 
    return([])

def getCampLevels(): 
    return(list(["NIFs", "SRV"]))

def getCampTypes(): 
    return(list([
    #"Retention_HH", No target 1
    "Retention_Voice",
    #"Legal_Informativa", No target 1
    #"Delight", No target 1
    # "Ignite", No target 1
    "Up_Cross_Sell"
    # TODO: "Up_Cross_Sell_HH", "Up_Cross_Sell" to be renamed to avoid confusion
    # "Welcome", No target 1
    # "Terceros", No SRV
    ]))

def getCampChannels(): 
    return(list(["EMA",
    "TEL",
    "SLS",
    "MLT",
    "SAT",
    # "TER",
    "NOT",
    "SMS",
    "MMS"]))

def getCampGroups():
    return(list(["Target", "Control", "Universal"]))

def getCampRed():
    return(list(["0", "1"]))

def getCampNifColumns():
    return(list(["Camp_NIFs_Delight_EMA_Target_0",
    "Camp_NIFs_Delight_SMS_Control_0",
    "Camp_NIFs_Delight_SMS_Target_0",
    "Camp_NIFs_Delight_TEL_Target_0",
    "Camp_NIFs_Delight_TEL_Universal_0",
    "Camp_NIFs_Ignite_EMA_Target_0",
    "Camp_NIFs_Ignite_SMS_Control_0",
    "Camp_NIFs_Ignite_SMS_Target_0",
    "Camp_NIFs_Ignite_TEL_Universal_0",
    "Camp_NIFs_Legal_Informativa_EMA_Target_0",
    #"Camp_NIFs_Legal_Informativa_fac_Target_0", TODO: fac?
    "Camp_NIFs_Legal_Informativa_SLS_Target_0",
    "Camp_NIFs_Legal_Informativa_SMS_Target_0",
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
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_0",
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
    "Camp_NIFs_Up_Cross_Sell_SMS_Target_0",
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
    "Camp_NIFs_Welcome_TEL_Universal_0"]))
    #"Camp_NIFs_unknown_SAT_Target_0"

def getCampSrvColumns():
    return(list(["Camp_SRV_Delight_EMA_Target_0",
    "Camp_SRV_Delight_MLT_Universal_0",
    "Camp_SRV_Delight_NOT_Target_0",
    "Camp_SRV_Delight_NOT_Universal_0",
    "Camp_SRV_Delight_SMS_Control_0",
    "Camp_SRV_Delight_SMS_Target_0",
    "Camp_SRV_Delight_SMS_Universal_0",
    "Camp_SRV_Delight_TEL_Universal_0",
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
    "Camp_SRV_Retention_Voice_SMS_Target_0",
    "Camp_SRV_Retention_Voice_SMS_Target_1",
    "Camp_SRV_Retention_Voice_SMS_Universal_0",
    "Camp_SRV_Retention_Voice_SMS_Universal_1",
    "Camp_SRV_Retention_Voice_TEL_Control_0",
    "Camp_SRV_Retention_Voice_TEL_Control_1",
    "Camp_SRV_Retention_Voice_TEL_Target_0",
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
    #"Camp_SRV_Up_Cross_Sell_rcs_Control_0", TODO: rcs?
    #"Camp_SRV_Up_Cross_Sell_rcs_Target_0", TODO: rcs?
    "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
    "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
    "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
    "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
    "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
    "Camp_SRV_Up_Cross_Sell_SMS_Target_0",
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
    "Camp_SRV_Welcome_SMS_Target_0"]))

def getInitialCampColumns():
    return(getCampNifColumns() + getCampSrvColumns())

def getNewCampColumns():
    camp_groups_aggs = []
    camp_channels_aggs = []
    camp_types_aggs = []    
    campGroups = getCampGroups()
    campChannels = getCampChannels()
    campTypes = getCampTypes()
    for g in campGroups:
        camp_groups_aggs.append(["total_camps_" + g, "total_redem_" + g, "pcg_redem_" + g])
    for ch in campChannels:
        camp_channels_aggs.append(["total_camps_" + ch, "total_redem_" + ch, "pcg_redem_" + ch])
    for t in campTypes:
        camp_types_aggs.append(["total_camps_" + t, "total_redem_" + t, "pcg_redem_" + t])

    return(flatten(camp_groups_aggs) + flatten(camp_channels_aggs) + flatten(camp_types_aggs))

def getProcessedCampColumns(): 
    return(getInitialCampColumns() + getNewCampColumns())

############# BILLING #############

def getCatFeatsBilling():
    return([])

def getBillingMonths():
    return(list(range(1,6)))

def getInitialBillingColumns(): 
    billing_cols = []
    billing_months = getBillingMonths()
    for n in billing_months:
        billing_cols.append(list(["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount", "Bill_N" + str(n) + "_Debt_Amount"]))
    return(flatten(billing_cols))

def getNewBillingColumns():
    billing_months = getBillingMonths()
    new_billing_cols = []
    for m in billing_months:
        new_billing_cols.append("bill_n" + str(m) + "_net")
    return(new_billing_cols)

def getProcessedBillingColumns(): 
    return(getInitialBillingColumns()+  getNewBillingColumns())


############# CRM #############

def getInitialCrmColumns():
    return(list(["codigo_postal",
    "metodo_pago",
    "cta_correo",
    "factura_electronica",
    "superoferta",
    "fecha_migracion",
    "tipo_documento",
    "nacionalidad",
    "x_antiguedad_cuenta",
    "x_datos_navegacion",
    "x_datos_trafico",
    "x_cesion_datos",
    "x_user_facebook",
    "x_user_twitter",
    "marriage2hgbst_elm",
    "gender2hgbst_elm",
    "flg_robinson",
    "x_formato_factura",
    "x_idioma_factura",
    "bam_services",
    "bam_fx_first",
    "bam-movil_services",
    "bam-movil_fx_first",
    "fbb_services",
    "fbb_fx_first",
    "fixed_services",
    "fixed_fx_first",
    "movil_services",
    "movil_fx_first",
    "prepaid_services",
    "prepaid_fx_first",
    "tv_services",
    "tv_fx_first",
    "fx_srv_basic",
    "tipo_sim",
    "desc_tariff",
    "tariff",
    "fx_tariff",
    "price_tariff",
    "voice_tariff",
    "fx_voice_tariff",
    "data",
    "fx_data",
    "dto_lev1",
    "fx_dto_lev1",
    "price_dto_lev1",
    "dto_lev2",
    "fx_dto_lev2",
    "price_dto_lev2",
    "data_additional",
    "fx_data_additional",
    "roam_zona_2",
    "fx_roam_zona_2",
    "sim_vf"]))

def getCatFeatsCrm(): 
    return(list([
    "metodo_pago",
    "cta_correo",
    "factura_electronica",
    "superoferta",
    "cliente_migrado",
    "tipo_documento",
    "nacionalidad",
    "x_antiguedad_cuenta",
    "x_datos_navegacion",
    "x_datos_trafico",
    "x_cesion_datos",
    "marriage2hgbst_elm",
    "gender2hgbst_elm",
    "flg_robinson",
    "x_formato_factura",
    "x_idioma_factura",
    "tipo_sim",
    "segunda_linea",
    "desc_tariff",
    "tariff",
    "voice_tariff",
    "data",
    "dto_lev1",
    "dto_lev2",
    "data_additional",
    "roam_zona_2",
    "sim_vf"]))

def getNewCrmColumns(): 
    return(list(["dias_desde_fecha_migracion",
    "cliente_migrado",
    "dias_desde_bam_fx_first",
    "dias_desde_bam-movil_fx_first",
    "dias_desde_fbb_fx_first",
    "dias_desde_fixed_fx_first",
    "dias_desde_movil_fx_first",
    "dias_desde_prepaid_fx_first",
    "dias_desde_tv_fx_first",
    "dias_desde_fx_srv_basic",
    "segunda_linea",
    "dias_desde_fx_tariff",
    "dias_desde_fx_voice_tariff",
    "dias_desde_fx_data",
    "dias_desde_fx_dto_lev1",
    "dias_desde_fx_dto_lev2",
    "dias_desde_fx_data_additional",
    "dias_desde_fx_roam_zona_2",
    "total_num_services",
    "real_price"]))

def Diff(li1, li2): 
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2] 
    return li_dif 
    
def getProcessedCrmColumns():
    return list(set(getInitialCrmColumns()) - set(getNewCrmColumns()) - set(list([
    "fecha_migracion",
    "bam_fx_first",
    "bam-movil_fx_first",
    "fbb_fx_first",
    "fixed_fx_first",
    "movil_fx_first",
    "prepaid_fx_first",
    "tv_fx_first",
    "fx_srv_basic",
    "fx_tariff",
    "fx_voice_tariff",
    "fx_data",
    "fx_dto_lev1",
    "fx_dto_lev2",
    "fx_data_additional",
    "fx_roam_zona_2"])))

############# NUM CLIENT #############
def getCatFeatsNumcliente():
    return([])

def getInitialNumclienteColumns():
    return([])

def getNewNumclienteColumns(): 
    return(list(["num_movil", "num_bam", "num_fixed", "num_prepaid", "num_tv", "num_fbb", "num_futbol", "total_futbol_price", "total_penal_cust_pending_n1_penal_amount",
    "total_penal_cust_pending_n2_penal_amount",
    "total_penal_cust_pending_n3_penal_amount",
    "total_penal_cust_pending_n4_penal_amount",
    "total_penal_cust_pending_n5_penal_amount",

    "total_penal_srv_pending_n1_penal_amount",
    "total_penal_srv_pending_n2_penal_amount",
    "total_penal_srv_pending_n3_penal_amount",
    "total_penal_srv_pending_n4_penal_amount",
    "total_penal_srv_pending_n5_penal_amount",

    "total_max_dias_hasta_penal_cust_pending_end_date",
    "total_max_dias_hasta_penal_srv_pending_end_date"]))

def getProcessedNumclienteColumns(): 
    return(getInitialNumclienteColumns() + (getNewNumclienteColumns()))

def getRawMobileColumns(): 
    return list(set(getIdFeats()+getInitialCrmColumns()+getInitialGnvColumns()+getInitialCampColumns()+getInitialBillingColumns()+getInitialNumclienteColumns()))

def getProcessedColumns(): 
    return(getIdFeats()+getProcessedCrmColumns()+getProcessedGnvColumns()+getProcessedCampColumns()+getProcessedBillingColumns()+getProcessedNumclienteColumns())

def getFeatSubset(feats2rem):
    removed_feats = getFeats2RemFromList(feats2rem)
    return list(set(getProcessedColumns()) - set(removed_feats))

def getCatFeats():
    return(getCatFeatsCrm() + getCatFeatsGnv() + getCatFeatsCamp() + getCatFeatsBilling() + getCatFeatsNumcliente())

def getNoInputFeats():
    return(list([
    "x_user_facebook",
    "x_user_twitter",
    "codigo_postal"]))

def getFeats2RemFromList(feats2rem): 
    
    def getFeats2Rem(f): 
        if (f == "id"):
            return(getIdFeats())
        elif (f == "gnv_raw"): 
            return(getInitialGnvColumns())
        elif (f == "gnv_new"): 
            return(getNewGnvColumns())
        elif (f == "camp_raw"):
            return(getInitialCampColumns())
        elif (f == "camp_new"):
            return(getNewCampColumns())
        elif (f == "camp_all"):
            return(getInitialCampColumns()+getNewCampColumns())
        elif (f == "billing_raw"):
            return(getInitialBillingColumns())
        elif (f == "billing_new"):
            return(getNewBillingColumns())
        elif (f == "numcliente_raw"):
            return(getInitialNumclienteColumns())
        elif (f == "numcliente_new"):
            return(getNewNumclienteColumns())
    
    if not feats2rem:
        return([])
    else: 
        return(getFeats2Rem(feats2rem))
