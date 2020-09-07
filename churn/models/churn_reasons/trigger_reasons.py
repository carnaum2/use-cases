# -*- coding: utf-8 -*-
from datetime import datetime as dt
import numpy as np

def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print(pathname)
    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):

        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)

        #from churn.utils.constants import CHURN_DELIVERIES_DIR
        #root_dir = CHURN_DELIVERIES_DIR
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))


    return root_dir

def getStats(spark, df, features, key = 'label'):
    #Function to obtain mean and std of a column
    from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
    df_nc = df.filter(df[key] == 0)
    df_c = df.filter(df[key] == 1)
    stats_no = []
    stats_c = [] 
    i = 0
    for sel_ in features:
        stats_nc = df_nc.select(_mean(col(sel_)).alias('mean'),_stddev(col(sel_)).alias('std')).collect()
        mean_ = stats_nc[0]['mean']
        std = stats_nc[0]['std']
        stats_no.append((mean_,std))
        stats = df_c.select(_mean(col(sel_)).alias('mean'),_stddev(col(sel_)).alias('std')).collect()
        mean_ = stats[0]['mean']
        std = stats[0]['std']
        stats_c.append((mean_,std))        
        i = i+1
    return stats_no, stats_c

def getMaxValGroup(spark, df, selected_cols, group):
    #Function to get max val in a group of columns
    for col_name in selected_cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))
    name_val = 'max_val_' + group
    df = df.withColumn(name_val, sort_array(array([col(x) for x in selected_cols]), asc=False)[0])
    return(df)

def getTopNVals(spark, df, filtered_cols, n_feats):
    #Function to get the scores of the categories ordered
    for col_name in filtered_cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        df = df.withColumn(name_val, sort_array(array([col(x) for x in filtered_cols]), asc=False)[i])
    return(df)

def getTopNFeatsK2(spark, df, n_feats, filtered_cols):
    #Function to create the reasons ranking
    from pyspark.sql.types import DoubleType, StringType
    df2 = df   
    modify_values_udf = udf(modify_values2, StringType())
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        name_feat = 'top{}_feat'.format(i)
        df =  df\
        .withColumn(name_feat, modify_values_udf(array(df2.columns[-len(filtered_cols)-n_feats:-n_feats]), name_val, array(filtered_cols)))
 
    for i in range (0,n_feats):
        name_col = 'top{}_reason'.format(i)
        name_feat = 'top{}_feat'.format(i)
        name_val = 'top{}_val'.format(i)
        df =  df\
        .withColumn(name_col, when(df[name_val] > 0, df[name_feat]).otherwise('-'))           
    return(df)

def modify_values2(r, max_col, filtered_cols): 
    l = []
    for i in range(len(filtered_cols)):
        if r[i]== max_col:
            l.append(reasons[i])
    return l

def getDfSample(spark, df, key, n_muestras):
    #Function to subsample a df
    from pyspark.sql.functions import col
    from pyspark.sql.functions import rand
    import numpy as np
    schema = df.schema
    class_1 = df.filter(col(key) == 1.0)
    class_2 = df.filter(col(key) == 0.0)
    

    sampled_2 = class_2.take(int(n_muestras))
    sampled_1 = class_1.take(int(n_muestras))
        
    sampled_2 = spark.createDataFrame(sampled_2,schema)
    sampled_1 = spark.createDataFrame(sampled_1,schema)
        
    sample = sampled_2.union(sampled_1)
    
    balanced_f = sample.orderBy(rand())
    
    return balanced_f

def get_trigger_minicar(spark, closing_day):
    
    path = '/data/udf/vf_es/churn/triggers/car_test_' + closing_day
    if pathExist(path):

        print "[Info Main Trigger Loader] " + time.ctime() + " File " + str(path) + " already exists. Reading it."
        df_car = spark.read.parquet(path)
        return df_car

    else:

        print'Prediction date: {}'.format(closing_day)    
        print'Loading tickets source for closing_day: {}'.format(closing_day)
        tickets = get_tickets_car(spark, closing_day)   
        print("############ Loading customer base ############")  
        from churn.datapreparation.general.customer_base_utils import  get_customer_base_segment
        df_base_msisdn = get_customer_base_segment(spark, date_=closing_day)
        print'Size of customer base: {}'.format(df_base_msisdn.count())
        print'Number of NIFs in customer base: {}'.format(df_base_msisdn.select('NIF_CLIENTE').distinct().count())
        print("############ Filtering Car ############")
        df_tar = get_filtered_car(spark, closing_day, df_base_msisdn)
        print'Size of filtered customer base: {}'.format(df_tar.count())
        print("############ Loading billing car ############")
        billingdf_f = get_billing_car(spark, closing_day)
        billing_tar = df_tar.select('NIF_CLIENTE').join(billingdf_f.drop_duplicates(subset=['NIF_CLIENTE']), ['NIF_CLIENTE'], 'left')

        from pyspark.sql.functions import countDistinct, count, sum
        from pyspark.sql import Row, DataFrame, Column, Window
        window = Window.partitionBy("NIF_CLIENTE")
        df = tickets.where(((col('TIPO_TICKET') != 'Activación/Desactivación') & (col('TIPO_TICKET') != 'Baja') & (col('TIPO_TICKET') != 'Portabilidad') & (col('TIPO_TICKET') != 'Problemas con portabilidad')) | (col('TIPO_TICKET').isNull()))
        
        df_tickets = df.withColumn('tmp_fact', when(((col('X_TIPO_OPERACION') == 'Tramitacion')), 1.0).otherwise(0.0))\
                .withColumn('NUM_TICKETS_TIPO_FACT', sum('tmp_fact').over(window))\
        .withColumn('tmp_rec', when((col('X_TIPO_OPERACION') == 'Reclamacion'), 1.0).otherwise(0.0))\
        .withColumn('NUM_TICKETS_TIPO_REC', sum('tmp_rec').over(window))\
        .withColumn('tmp_inf', when((((col('X_TIPO_OPERACION') == 'Informacion'))), 1.0).otherwise(0.0))\
        .withColumn('NUM_TICKETS_TIPO_INF', sum('tmp_inf').over(window))\
        .withColumn('tmp_av', when((col('X_TIPO_OPERACION') == 'Averia'), 1.0).otherwise(0.0))\
                .withColumn('NUM_TICKETS_TIPO_AV', sum('tmp_av').over(window))\
        .withColumn('tmp_inc', when((col('X_TIPO_OPERACION') == 'Incidencia'), 1.0).otherwise(0.0))\
                .withColumn('NUM_TICKETS_TIPO_INC', sum('tmp_inc').over(window))\
           
        
        print("############ Loading Reimbursements base ############")   
        reimb_hdfs = '/data/attributes/vf_es/trigger_analysis/reimbursements/year={}/month={}/day={}'.format(int(closing_day[:4]),int(closing_day[4:6]),int(closing_day[6:8]))
        reimb_tr = spark.read.load(reimb_hdfs).drop_duplicates(subset=['NIF_CLIENTE'])

        billing_reimb = billing_tar.join(reimb_tr,['NIF_CLIENTE'], 'left' ).fillna(0)
        df_tickets_sel = df_tickets.select('NIF_CLIENTE', 'NUM_TICKETS_TIPO_FACT','NUM_TICKETS_TIPO_REC', 'NUM_TICKETS_TIPO_AV', 'NUM_TICKETS_TIPO_INC', 'NUM_TICKETS_TIPO_INF').drop_duplicates(subset=['NIF_CLIENTE'])
        billing_reimb_tickets = billing_reimb.join(df_tickets_sel, ['NIF_CLIENTE'], 'left').fillna(-1)
        print("############ Loading orders base ############")
        orders_hdfs = '/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/year={}/month={}/day={}'.format(int(closing_day[:4]),int(closing_day[4:6]),int(closing_day[6:8]))
        orders_tr = spark.read.load(orders_hdfs)

        df_car = billing_reimb_tickets.join(orders_tr, ['NIF_CLIENTE'], 'left').fillna(0)

        print'Size of final car: {}'.format(df_car.count())
        print'Number of different NIFs in final car: {}'.format(df_car.select('NIF_CLIENTE').distinct().count())
        df_car.repartition(200).write.save(path, format='parquet', mode='overwrite')
        return df_car

#Variables to analyze and existing variable categories 
global relevant_var, reasons

#Save table roots

TABLE_TRIGGER = '/data/udf/vf_es/churn/churn_reasons/trigger_reasons_results'

if __name__ == "__main__":

    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-s', '--starting_d', metavar = '<starting_d>', type= int, help= 'training day', required = True)
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= int, help= 'closing day', required = True)
    
    args = parser.parse_args()
    set_paths_and_logger()

    import pykhaos.utils.pyspark_configuration as pyspark_config
    from pyspark.sql.functions import *
    from map_funct_trigger import getFeatGroups_trigger
    from utils_fbb_churn import *


    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
    
    import datetime    
    executed_at = str(datetime.datetime.now())
        
    #Number of feats to analyze
    n_feats = 16 #config['number_feats']
    #Number of feats that compose the ranking
    top_feats = 5
    key = "label"
    training_day = str(args.starting_d)
    closing_day = str(args.closing_d)
    #Save directory
    save_dir = TABLE_TRIGGER
    #Train and Test Dataframes
    print'############ Loading Train dataframe for training day {} and test for closing day ############'.format(training_day, closing_day)
    path_train = '/data/udf/vf_es/churn/triggers/car_tr_' + training_day
    Train_ = spark.read.parquet(path_train)
    print'Loaded Trainig Dataframe'
    num_t = Train_.count()
    print'Size of Training Dataframe: ' + str(num_t)
    #if num_t > 150000:
        #Train_ = getDfSample(spark, Train_, key, 75000)
        
        
    df_car = get_trigger_minicar(spark, closing_day)
    print'Loaded Test Dataframe'
    print("############ Finished loading Datasets ############")
    
    #Feats to analyze and importance
    path_feats = '/data/udf/vf_es/churn/triggers/feat_imp_' + training_day
    
    feats = spark.read.load(path_feats)
        
    feat_list = feats.orderBy(feats['feat_importance'],ascending= False).select('feature').rdd.map(lambda x: str(x[0])).collect()
    print(feat_list)
    #Feature mapper        
    reimb_var, bill_var, orders_var, feats_tickets_serv_var, feats_tickets_fact_var = getFeatGroups_trigger(spark, df_car, feat_list, 16, 1)
        
    relevant_var = reimb_var + bill_var + orders_var + feats_tickets_serv_var + feats_tickets_fact_var
    print("############ Finished loading Most Important Features ############")
    print 'Number of features to analyze: {}'.format(len(relevant_var))        

    #Score columns for each category
    score_bill = [x + '_score' for x in bill_var]
    score_orders = [x + '_score' for x in orders_var]
    score_reimb = [x + '_score' for x in reimb_var]
    score_tickets_serv = [x + '_score' for x in feats_tickets_serv_var]
    score_tickets_fact = [x + '_score' for x in feats_tickets_fact_var]   
    
    #Score columns for each variable and ranking columns
    score_cols = [name_ + '_score' for name_ in relevant_var]
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    val_cols = ['top'+ str(i) + '_val' for i in range(0,top_feats)]
    
    #Score columns for each category
    score_cols_2 = ['max_val_bill','max_val_reimb', 'max_val_ord', 'max_val_tickets', 'max_val_ticketsS']
    #Reasons (categories)
    reasons = ['Billing','Reimbursements', 'Orders and SLA', 'Billing Tickets', 'Service']
    n_groups = len(reasons)
    
    #Initial probabilities per class (should be 0.5)
    label = key
    n_0s = Train_.filter(col(label) == 0.0).count()
    n_1s = Train_.filter(col(label) == 1.0).count()
    p_0s = float(n_0s)/(n_0s + n_1s)
    p_1s = float(n_1s)/(n_0s + n_1s)
    
    #Stadistics for each variable (required for kernel functions)
    stats_no, stats_c = getStats(spark, Train_, relevant_var, key )
    
    import pyspark.mllib.stat.KernelDensity as Kernel 
    
    #Cast to float (required for kernel functions)
    for col_name in relevant_var:
        df_car = df_car.withColumn(col_name, col(col_name).cast('float'))
        Train_ = Train_.withColumn(col_name, col(col_name).cast('float'))    
    
    #Churn and no churn subsets
    df_churners = Train_.filter(Train_[label] == 1)
    df_no_churners = Train_.filter(Train_[label] == 0)
    
    i = 0
    
    #Df to save results
    df_aux = df_car.select( ['NIF_CLIENTE'] + relevant_var)

    for name_ in relevant_var:
        #Kernel predictors for both sets
        kd_c = Kernel()
        kd_nc = Kernel()
        print('Predicting feature: ')
        print(name_)
        #Training df
        rdd_c = df_churners.select(name_).rdd.map(lambda x: x[0])
        kd_c.setSample(rdd_c)
        #thumb bandwidth estimator
        h_c = stats_c[i][1]*(4/3/n_1s)**(1/5)
        kd_c.setBandwidth(h_c)
        #Column distinct values
        sel = df_car.select(name_).distinct()
        vals = [row[name_] for row in sel.collect()]
        #Predict p1 stage
        pred = kd_c.estimate(np.array(vals))      
        pred2 = map(float, pred)
        save = zip(vals, pred2)
        #Save prediction scores
        rdd1 = spark.sparkContext.parallelize(save)
        rdd2 = rdd1.map(lambda x: [i for i in x])
        df_new_1s = rdd2.toDF([name_, name_+'_p1'])
        #Training df
        rdd_c = df_no_churners.select(name_).rdd.map(lambda x: x[0])
        kd_nc.setSample(rdd_c)
         #thumb bandwidth estimator
        h_nc = stats_no[i][1]*(4/3/n_0s)**(1/5)
        kd_nc.setBandwidth(h_nc)   
        #Predict p0 stage        
        pred = kd_nc.estimate(np.array(vals))
        pred2 = map(float, pred)
        save = zip(vals,pred2)
        #Save prediction scores
        rdd1 = spark.sparkContext.parallelize(save)
        rdd2 = rdd1.map(lambda x: [i for i in x])
        df_new_0s = rdd2.toDF([name_,name_+'_p0'])
        #Join p0s and p1s
        df_join = df_new_1s.join(df_new_0s, [name_], 'inner')
        #
        df_aux = df_aux.join(df_join, [name_], 'inner') 
        i = i + 1
    
    j = 0
    #Calculate bayesian scores
    for name_ in relevant_var:
        df_aux = df_aux.withColumn(name_ + '_score', (p_1s * df_aux[name_+'_p1'])/(p_1s * df_aux[name_+'_p1'] + p_0s * df_aux[name_+'_p0']))
        j = j+1
        
    #Get max score for each category
    df_aux = getMaxValGroup(spark, df_aux, score_bill, 'bill')
    df_aux = getMaxValGroup(spark, df_aux, score_reimb, 'reimb')    
    df_aux = getMaxValGroup(spark, df_aux, score_orders, 'ord') 
    df_aux = getMaxValGroup(spark, df_aux, score_tickets_fact, 'tickets')
    df_aux = getMaxValGroup(spark, df_aux, score_tickets_serv, 'ticketsS')
    
    #Get ordered Scores
    df_ord = getTopNVals(spark, df_aux, score_cols_2, n_groups)
    
    #Get ordered reasons    
    df_final = getTopNFeatsK2(spark, df_ord, n_groups, filtered_cols = score_cols_2)
    
    from pyspark.sql.functions import log2
    #Entropy of the first reason
    df_final = df_final.withColumn('Incertidumbre', when(df_final['top0_val'] < 1, -df_final['top0_val']*log2(df_final['top0_val'])-(1-df_final['top0_val'])*log2(1-df_final['top0_val'])).otherwise(0))
    
    #Save results
    save_df = df_final.select(['NIF_CLIENTE'] +  name_cols + ['Incertidumbre'])\
    .withColumn('executed_at', lit(executed_at)).withColumn('top5_reason', lit('[Customer]')).withColumn('model', lit('asaezco')).withColumn('closing_day', lit(str(args.closing_d))) 
    save_df.repartition(300).write.save(save_dir, format='parquet', mode='append')
    
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")