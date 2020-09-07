def getFeatGroups(spark, df, feat_list, num_feats = 20, mode = 0):
    from Metadata import getIdFeats
    gnv = [name_ for name_ in df.columns if name_.lower().startswith('gnv')]
    inter =  [name_ for name_ in df.columns if name_.lower().startswith(('ccc', 'orders'))] + [name_ for name_ in df.columns if name_.lower().startswith('pbms_')] + ['others_ind_pbma_srv']
    bill = [name_ for name_ in df.columns if ('dto' in name_.lower()) & (not name_.lower().startswith('price_'))& (not name_.lower().startswith('dias_desde')) & (not name_.lower().startswith('tgs')) & (not name_.lower().startswith('incremental'))] + [name_ for name_ in df.columns if name_.lower().startswith(('bill','price_'))] + ['real_price', 'total_futbol_price', 'desc_tariff','tariff','fx_tariff','voice_tariff','fx_voice_tariff']
    data = [name_ for name_ in df.columns if name_.lower().startswith(('total_data_volume_w','data_per_connection_'))] + ['max_connections_w','max_connections_we','max_num_calls_w','max_num_calls_we', 'total_data_volume', 'data','data_per_connection', 'fx_data','data_additional','fx_data_additional', 'roam_zona_2','fx_roam_zona_2']    
    conn = [name_ for name_ in df.columns if name_.lower().startswith('total_connections')]
    mou =  [name_ for name_ in df.columns if name_.lower().startswith('mou')] +  [name_ for name_ in df.columns if ('_mou_'in name_) & (not name_.lower().startswith('incremental')) & (not name_.lower().startswith('hour_max'))]
    max_data = [name_ for name_ in df.columns if name_.lower().startswith(('max_data','hour_max'))]
    netapps =  [name_ for name_ in df.columns if name_.lower().startswith('netapps')]
    navigation =  [name_ for name_ in df.columns if name_.lower().startswith('navigation')]
    incremental_pen =  [name_ for name_ in df.columns if (name_.lower().startswith('incremental_')) & ('penal' in name_)]
    incremental_eng =  [name_ for name_ in df.columns if (name_.lower().startswith('incremental_')) & (('_services' in name_)or('delta_num' in name_)) ]
    incremental_bill =  [name_ for name_ in df.columns if (name_.lower().startswith('incremental_')) & (('bill' in name_) or ('price' in name_)or ('tariff' in name_))]
    incremental_use =  [name_ for name_ in df.columns if (name_.lower().startswith('incremental_')) & (('data' in name_)or ('calls' in name_) or ('connection' in name_)or ('_mou' in name_))]
    campaigns =  [name_ for name_ in df.columns if name_.lower().startswith('campaigns_')]
    tgs = [name_ for name_ in df.columns if name_.lower().startswith('tgs')]
    additional_bill = [name_ for name_ in df.columns if (name_.lower().startswith('additional_')) & (('bill' in name_)or('price' in name_))] + ['additional_total_tv_total_charges']
    additional_eng = [name_ for name_ in df.columns if (name_.lower().startswith('additional_')) & (('num_' in name_)or('_services' in name_)or('_tv' in name_)and('charges' not in name_) ) ]
    days =  [name_ for name_ in df.columns if name_.lower().startswith('dias_desde')]
    pen = [name_ for name_ in df.columns if name_.lower().startswith(('total_penal_','total_max_dias_hasta_penal'))]
    base = getIdFeats() + ['nif_cliente','codigo_postal','num_cliente_customer', 'ref_date','fx_srv_basic', 'sim_vf', 'srv_basic']
    port =  [name_ for name_ in df.columns if (name_.lower().startswith('nif_')) & (name_ not in ['nif_cliente'])]
    spin_2 = [name_ for name_ in df.columns if name_.lower()[-4:] in ['acon','acan', 'apor','asol', 'pcan', 'aace', 'aenv', 'arec']]  + ['total_movistar','total_simyo','total_orange','total_jazztel','total_yoigo','total_masmovil','total_pepephone','total_reuskal','total_unknown','total_otros', 'num_distinct_operators']
    spin_3 = [name_ for name_ in df.columns if name_.lower() in ['yoigo','orange', 'movistar','simyo', 'jazztel', 'masmovil', 'pepephone', 'reuskal_']]
    device = [name_ for name_ in df.columns if name_.lower().startswith('device_')]
    eng_2 = [name_ for name_ in df.columns if ((name_.lower().endswith(('_services','_first')))) & (not name_.lower().startswith(('incremental', 'dias_desde'))) & (not name_.lower().startswith('additional'))] + ['cliente_migrado','metodo_pago','cta_correo','tipo_sim', 'segunda_linea','factura_electronica','superoferta','fecha_migracion','tipo_documento','nacionalidad','x_antiguedad_cuenta','x_datos_navegacion','x_datos_trafico','x_cesion_datos','x_user_facebook','x_user_twitter','marriage2hgbst_elm','gender2hgbst_elm','flg_robinson','x_formato_factura','x_idioma_factura', 'num_movil','num_bam','num_fixed','num_prepaid','num_tv','num_fbb','num_futbol']

    use = gnv + data + conn + mou + max_data + netapps + incremental_use
    billing = bill + incremental_bill + additional_bill
    interactions = inter + campaigns
    spinners =  navigation + spin_2 + port + days
    engagement = eng_2 + device + incremental_eng + additional_eng
    penalization = tgs + pen + incremental_pen
    
    penal_var = []
    use_var = []
    spin_var = []
    bill_var = []
    eng_var = []
    inter_var = []
    
    use_f = 0
    spin_f = 0
    eng_f = 0
    bill_f = 0
    pen_f = 0
    inter_f = 0

    c = 1
    th = 4
    if num_feats == 20:
        th = 4   
    for name in feat_list:
        if (use_f & spin_f & eng_f & bill_f & inter_f & pen_f) & (c >= num_feats):
            break
        if name in use:
            if (len(use_var) < th - 2):
                use_var.append(name)
                print(name)
                use_f = 1
                c += 1
        elif name in penalization:
            if len(penal_var) < th + 2:
                penal_var.append(name)
                print(name) 
                pen_f = 1
                c += 1
        elif name in spinners:
            if len(spin_var) < th -1:
                spin_var.append(name)
                print(name)
                spin_f = 1
                c += 1
        elif name in billing:
            if len(bill_var) < th :
                bill_var.append(name)
                print(name)
                bill_f = 1
                c += 1
        elif name in engagement:
            if len(eng_var) < th - 1:
                eng_var.append(name) 
                print(name)
                eng_f = 1
                c += 1
        elif name in interactions:
            if len(inter_var) < th:
                inter_var.append(name)
                print(name)
                inter_f = 1
                c += 1
        else:
            print'Feature not categorized {}'.format(name)

    print('List of Use features')
    print(use_var)
    print('***********')
    print('List of Billing features')
    print(bill_var)
    print('***********')
    print('List of Interaction features')
    print(inter_var)
    print('***********')
    print('List of Spinner features')
    print(spin_var)
    print('***********')
    print('List of Engagement features')
    print(eng_var)
    print('***********')
    print('List of Penalization features')
    print(penal_var)
    print('***********')
    
    if mode == 1:
        return use_var, bill_var, inter_var, spin_var, eng_var, penal_var
    else:
        return use_var + bill_var + inter_var + spin_var + eng_var + penal_var