def getFeatGroups_fbb(spark, numerical_feats, num_feats = 20, mode = 0):
    
    apps = [name_ for name_ in numerical_feats if name_.startswith('netapps')]
    tgs = [name_ for name_ in numerical_feats if name_.startswith('tgs')]
    order = [name_ for name_ in numerical_feats if name_.startswith('orders')]
    pbms = [name_ for name_ in numerical_feats if name_.startswith('pbms')]# + ['others_ind_pbma_srv']
    device = [name_ for name_ in numerical_feats if name_.startswith('device')]
    ccc = [name_ for name_ in numerical_feats if name_.startswith('ccc')]
    campaigns = [name_ for name_ in numerical_feats if name_.startswith('campaigns')]
    incremental_bill = [name_ for name_ in numerical_feats if name_.startswith('incremental') and ('bill' in name_ or 'price' in name_ or 'tariff' in name_)]
    incremental_penal = [name_ for name_ in numerical_feats if name_.startswith('incremental') and 'penal' in name_]
    pen = [name_ for name_ in numerical_feats if 'penal' in name_]
    price = [name_ for name_ in numerical_feats if 'price' in name_]
    incremental_data = [name_ for name_ in numerical_feats if name_.startswith('incremental') and ('total_data' in name_ or 'max_mou' in name_ or 'max_connections' in name_ or 'data_per' in name_ or 'total_connections' in name_ or 'max_data' in name_ or 'max_num' in name_ or 'total_mou' in name_)]
    incremental_services = [name_ for name_ in numerical_feats if name_.startswith('incremental') and ('services' in name_ or 'delta_num_' in name_)]
    additional_bill = [name_ for name_ in numerical_feats if name_.startswith('additional') and ('bill' in name_ or 'price' in name_)] + ['additional_total_tv_total_charges']
    additional_tariff = [name_ for name_ in numerical_feats if name_.startswith('additional') and ('num_tariff' in name_ )]
    additional_days_since = [name_ for name_ in numerical_feats if name_.startswith('additional') and ('dias_desde' in name_ )]
    additional_services = [name_ for name_ in numerical_feats if name_.startswith('additional') and ('services' in name_ )] + ['additional_num_2lins']
    navigation = [name_ for name_ in numerical_feats if name_.startswith('navigation')]
    bill = [name_ for name_ in numerical_feats if name_.lower().startswith('bill') or 'bill' in name_.lower()]
    spin_2 = [name_ for name_ in numerical_feats if name_.lower()[-4:] in ['acon','acan', 'apor','asol', 'pcan', 'aace', 'aenv', 'arec']]  + ['total_movistar','total_simyo','total_orange','total_jazztel','total_yoigo','total_masmovil','total_pepephone','total_reuskal','total_unknown','total_otros', 'num_distinct_operators']
    services = [name_ for name_ in numerical_feats if 'services' in name_]
    spin_ = [name_ for name_ in numerical_feats if 'port' in name_]

    penalization = tgs + incremental_penal + pen
    interactions = order + campaigns + ccc + pbms
    use = apps + incremental_data
    billing = incremental_bill + additional_bill + bill
    engagement = incremental_services + additional_services + additional_tariff + device + services + price
    spinners = navigation + additional_days_since + spin_2 + spin_
    
    #if mode == 0:
        #return use, billing, interactions, spinners, engagement, penalization
        #TODO: Adapt for the EA
    #else:
    
    use_f = 0
    spin_f = 0
    eng_f = 0
    bill_f = 0
    pen_f = 0
    inter_f = 0

    penal_var = []
    use_var = []
    spin_var = []
    bill_var = []
    serv_var = []
    inter_var = []

    c = 0
    th = 4
    for name in numerical_feats:
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
            if len(serv_var) < th - 1:
                serv_var.append(name) 
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
    print(serv_var)
    print('***********')
    print('List of Penalization features')
    print(penal_var)
    print('***********')
    
    return use_var, bill_var, inter_var, spin_var, serv_var, penal_var