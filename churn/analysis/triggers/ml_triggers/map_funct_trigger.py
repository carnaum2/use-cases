def getFeatGroups_trigger(spark, df, feat_list, num_feats = 16, mode = 0):
    
    feats = df.columns
    
    feats_ord = [name_ for name_ in feats if (('ord' in name_) or ('nb' in name_)) & ('SLA' not in name_)] + [name_ for name_ in feats if 'SLA' in name_ or 'mean_sla' in name_]
    feats_reimb = [name_ for name_ in feats if name_.lower().startswith('reimbursement')]
    feats_bill = [name_ for name_ in feats if 'bill' in name_.lower()]
    #feats_sla = [name_ for name_ in feats if 'SLA' in name_ or 'mean_sla' in name_]
    feats_tickets_serv = ['NUM_TICKETS_TIPO_INC','NUM_TICKETS_TIPO_AV','NUM_TICKETS_TIPO_INF']
    feats_tickets_fact = ['NUM_TICKETS_TIPO_FACT', 'NUM_TICKETS_TIPO_REC']
    
    reimb_f = 0
    ord_f = 0
    bill_f = 0

    feats_tickets_serv_var = feats_tickets_serv 
    feats_tickets_fact_var = feats_tickets_fact
    reimb_var = ['Reimbursement_num']
    orders_var = []
    bill_var = []

    c = 6
    th = 4
    if num_feats == 16:
        th = 4   
    for name in feat_list[:150]:
        print(c)
        if df.select(name).distinct().count() > 5:
            if (reimb_f & ord_f & bill_f) & (c >= num_feats):
                break
            if name in feats_reimb:
                if (len(reimb_var) < th ):
                    reimb_var.append(name)
                    print(name)
                    reimb_f = 1
                    c += 1
            elif name in feats_ord:
                if len(orders_var) < th :
                    orders_var.append(name)
                    print(name)
                    c += 1
                    ord_f = 1
            elif name in feats_bill:
                if len(bill_var) < th + 1 :
                    bill_var.append(name)
                    print(name)
                    bill_f = 1
                    c += 1
            else:
                print'Feature not categorized {}'.format(name)
        else:
            print'Feature {} does not have enough values'.format(name)
            continue

    print('List of Reimbursement features')
    print(reimb_var)
    print('***********')
    print('List of Billing features')
    print(bill_var)
    print('***********')
    print('List of Order features')
    print(orders_var)
    print('***********')
    print('List of Ticket services features')
    print(feats_tickets_serv_var)
    print('***********')
    print('List of Ticket billing features')
    print(feats_tickets_fact_var)
    print('***********')

    
    if mode == 1:
        return reimb_var, bill_var, orders_var, feats_tickets_serv_var, feats_tickets_fact_var
    else:
        return reimb_var + bill_var + orders_var + feats_tickets_serv_var + feats_tickets_fact_var
    
def getFeatGroups_trigger_cris(spark, df, mode = 0):
    
    feats = list(set(df.columns) - set(['label', 'nif_cliente', 'blindaje', 'segment_nif']))
    
    feats_bill = [name_ for name_ in feats if 'bill' in name_.lower()]
    feats_ccc = [name_ for name_ in feats if ('calls' in name_.lower()) or ('invoices' in name_.lower())]
    feats_rgu = [name_ for name_ in feats if 'rgu' in name_.lower()]
    feats_orders = [name_ for name_ in feats if 'order' in name_.lower()] + ['nb_running_last30_gt5']
    feats_tick = [name_ for name_ in feats if 'tickets' in name_.lower()]
    feats_reimb = [name_ for name_ in feats if name_.lower().startswith('reimbursement')]
    
    bill_var = feats_bill
    rgu_var = feats_rgu
    orders_var = feats_orders
    ccc_var = feats_ccc
    tickets_var = feats_tick
    reimb_var = feats_reimb

    print('List of Reimbursement features')
    print(reimb_var)
    print('***********')
    print('List of Billing features')
    print(bill_var)
    print('***********')
    print('List of Order features')
    print(orders_var)
    print('***********')
    print('List of Ticket features')
    print(tickets_var)
    print('***********')
    print('List of CCC features')
    print(ccc_var)
    print('***********')
    print('List of RGU features')
    print(rgu_var)
    print('***********')

    
    if mode == 1:
        return reimb_var, bill_var, orders_var, tickets_var, ccc_var, rgu_var
    else:
        return reimb_var + bill_var + orders_var + tickets_var + ccc_var + rgu_var



    for name_ in var_to_imp:
        print'Obtaining imputation value for feature ' + name_
        val = table_meta.filter(table_meta['feature'] == name_).select('imp_value').rdd.map(lambda x: x[0]).collect()[0]
        print(val)
        null_vals.append(float(val))