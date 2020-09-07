import itertools

def getIdFeats():

    ids=["msisdn", "num_cliente", "campo1", "msisdn_d", "nif_cliente", "rgu"]

    return list(set(ids))

def getIdFeats_tr():

    ids=["num_cliente_tr", "campo1_tr", "nif_cliente_tr", "rgu_tr"]

    return list(set(ids))


# CRM

def getInitialCrmColumns():
    initcols = ["metodo_pago",\
    "cta_correo",\
    "factura_electronica",\
    "superoferta",\
    "cliente_migrado",\
    "tipo_documento",\
    "x_publicidad_email",\
    "nacionalidad",\
    "x_antiguedad_cuenta",\
    "x_datos_navegacion",\
    "x_datos_trafico",\
    "x_cesion_datos",\
    "x_user_facebook",\
    "x_user_twitter",\
    "marriage2hgbst_elm",\
    "gender2hgbst_elm",\
    "flg_robinson",\
    "x_formato_factura",\
    "x_idioma_factura",\
    "bam_services",\
    "dias_desde_bam_fx_first",\
    "bam-movil_services",\
    "dias_desde_bam-movil_fx_first",\
    "fbb_services",\
    "dias_desde_fbb_fx_first",\
    "fixed_services",\
    "dias_desde_fixed_fx_first",\
    "movil_services",\
    "dias_desde_movil_fx_first",\
    "prepaid_services",\
    "dias_desde_prepaid_fx_first",\
    "tv_services",\
    "dias_desde_tv_fx_first",\
    "dias_desde_fx_fbb_upgrade"]
    
    return list(set(initcols))

def getCatFeatsCrm():
    catcols=["metodo_pago",\
    "cta_correo",\
    "factura_electronica",\
    "superoferta",\
    "cliente_migrado",\
    "tipo_documento",\
    "x_publicidad_email",\
    "nacionalidad",\
    "x_antiguedad_cuenta",\
    "x_datos_navegacion",\
    "x_datos_trafico",\
    "x_cesion_datos",\
    "x_user_facebook",\
    "x_user_twitter",\
    "marriage2hgbst_elm",\
    "gender2hgbst_elm",\
    "flg_robinson",\
    "x_formato_factura",\
    "x_idioma_factura",\
    "fbb_upgrade"]

    return list(set(catcols))
        
def getNewCrmColumns():
    newcols=["max_dias_desde_fx_football_tv",\
    "football_services",\
    "total_price_football",\
    "max_dias_desde_fx_motor_tv",\
    "motor_services",\
    "total_price_motor",\
    "max_dias_desde_fx_pvr_tv",\
    "pvr_services",\
    "total_price_pvr",\
    "max_dias_desde_fx_zapper_tv",\
    "zapper_services",\
    "total_price_zapper",\
    "max_dias_desde_fx_trybuy_tv",\
    "trybuy_services",\
    "total_price_trybuy",\
    "max_dias_desde_fx_trybuy_autom_tv",\
    "trybuy_autom_services",\
    "total_price_trybuy_autom",\
    "hz_services",\
    "min_dias_desde_fx_srv_basic",\
    "max_dias_desde_fx_srv_basic",\
    "mean_dias_desde_fx_srv_basic",\
    "total_price_srv_basic",\
    "mean_price_srv_basic",\
    "max_price_srv_basic",\
    "min_price_srv_basic",\
    "min_dias_desde_fx_dto_lev1",\
    "min_dias_desde_fx_dto_lev2",\
    "total_price_dto_lev1",\
    "total_price_dto_lev2",\
    "num_2lins",\
    "num_tariff_unknown",\
    "num_tariff_xs",\
    "num_tariff_redm",\
    "num_tariff_redl",\
    "num_tariff_smart",\
    "num_tariff_minim",\
    "num_tariff_plana200min",\
    "num_tariff_planaminilim",\
    "num_tariff_maslineasmini",\
    "num_tariff_megayuser",\
    "num_tariff_otros",\
    "total_price_tariff",\
    "max_price_tariff",\
    "min_price_tariff",\
    "total_tv_total_charges",\
    "total_penal_cust_pending_n1_penal_amount",\
    "total_penal_cust_pending_n2_penal_amount",\
    "total_penal_cust_pending_n3_penal_amount",\
    "total_penal_cust_pending_n4_penal_amount",\
    "total_penal_cust_pending_n5_penal_amount",\
    "total_penal_srv_pending_n1_penal_amount",\
    "total_penal_srv_pending_n2_penal_amount",\
    "total_penal_srv_pending_n3_penal_amount",\
    "total_penal_srv_pending_n4_penal_amount",\
    "total_penal_srv_pending_n5_penal_amount",\
    "total_max_dias_hasta_penal_cust_pending_end_date",\
    "total_min_dias_hasta_penal_cust_pending_end_date",\
    "total_max_dias_hasta_penal_srv_pending_end_date",\
    "total_min_dias_hasta_penal_srv_pending_end_date"]

    return list(set(newcols))

def getCrmFeats():
    return list(set(getNewCrmColumns() + getInitialCrmColumns() + getCatFeatsCrm()))

# Billing

def getInitialBillingColumns():
    billing_columns = reduce((lambda x, y: x + y), [["Bill_N" + str(n) + "_InvoiceCharges", "Bill_N" + str(n) + "_Amount_To_Pay", "Bill_N" + str(n) + "_Tax_Amount", "Bill_N" + str(n) + "_Debt_Amount"] for n in range(1,6)])
    return list(set(billing_columns))

def getNewBillingColumns():
    billing_columns = ["bill_n1_net",\
    "bill_n2_net",\
    "bill_n3_net",\
    "bill_n4_net",\
    "bill_n5_net",\
    "inc_bill_n1_n2_net",\
    "inc_bill_n1_n3_net",\
    "inc_bill_n1_n4_net",\
    "inc_bill_n1_n5_net",\
    "inc_Bill_N1_N2_Amount_To_Pay",\
    "inc_Bill_N1_N3_Amount_To_Pay",\
    "inc_Bill_N1_N4_Amount_To_Pay",\
    "inc_Bill_N1_N5_Amount_To_Pay"]
    return list(set(billing_columns))

def getCatFeatsBilling():
    return []

def getBillingFeats():
    return getNewBillingColumns() + getInitialBillingColumns() + getCatFeatsBilling()

# Mobile SOPO

def getInitialMobSopoColumns():
    return []

def getNewMobSopoColumns():

    destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown", "otros"]

    estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

    destino_estado = list(itertools.product(destinos, estados))

    values = [(str(x[0] + "_" + str(x[1]))) for x in destino_estado]

    mobsopo_columns = ["nif_port_number",\
    "nif_min_days_since_port",\
    "nif_max_days_since_port",\
    "nif_avg_days_since_port",\
    "nif_var_days_since_port",\
    "nif_distinct_msisdn",\
    "nif_port_freq_per_day",\
    "nif_port_freq_per_msisdn",\
    "total_acan",\
    "total_apor",\
    "total_arec",\
    "total_movistar",\
    "total_simyo",\
    "total_orange",\
    "total_jazztel",\
    "total_yoigo",\
    "total_masmovil",\
    "total_pepephone",\
    "total_reuskal",\
    "total_unknown",\
    "total_otros",\
    "num_distinct_operators"] + values

    return list(set(mobsopo_columns))

def getCatFeatsMobSopo():
    return []

def getMobSopoFeats():
    return list(set(getNewMobSopoColumns() + getInitialMobSopoColumns() + getCatFeatsMobSopo()))

# Orders

def getInitialOrdersColumns():
    return []

def getNewOrdersColumns():
    return ["days_since_last_order", "days_since_first_order"]

def getCatFeatsOrders():
    return []

def getOrdersFeats():
    return list(set(getNewOrdersColumns() + getInitialOrdersColumns() + getCatFeatsOrders()))

# Other functions

def getNoInputFeats():
    return []
