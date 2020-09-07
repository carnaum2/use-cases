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
    from map_funct_trigger import getFeatGroups_trigger_cris
    from utils_fbb_churn import *


    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
    
    import datetime    
    executed_at = str(datetime.datetime.now())
        
    #Number of feats to analyze
    n_feats = 30 #config['number_feats']
    #Number of feats that compose the ranking
    top_feats = 6
    key = "label"
    training_day = str(args.starting_d)
    closing_day = str(args.closing_d)
    #Save directory
    save_dir = TABLE_TRIGGER
    #Train and Test Dataframes
    print'############ Loading Train dataframe for training day {} and test for closing day ############'.format(training_day, closing_day)
    path_train = '/data/udf/vf_es/churn/triggers/car_exploration_model1/year={}/month={}/day={}'.format(int(training_day[:4]),int(training_day[4:6]),int(training_day[6:8]))
    
    Train_ = spark.read.parquet(path_train)
    
    print'Loaded Trainig Dataframe'
    num_t = Train_.count()
    print'Size of Training Dataframe: ' + str(num_t)
    if num_t > 150000:
        Train_ = getDfSample(spark, Train_, key, 75000)


    path_test = '/data/udf/vf_es/churn/triggers/car_exploration_unlabeled_model1/year={}/month={}/day={}'.format(int(closing_day[:4]),int(closing_day[4:6]),int(closing_day[6:8]))    
    df_car = spark.read.load(path_test)
    print'Loaded Test Dataframe'
    print("############ Finished loading Datasets ############")
    
    #Feature mapper        
    reimb_var, bill_var, orders_var, tickets_var, ccc_var, rgu_var = getFeatGroups_trigger_cris(spark, df_car, 1)
        
    relevant_var = reimb_var + bill_var + orders_var + tickets_var + ccc_var + rgu_var
    print("############ Finished loading Most Important Features ############")
    print 'Number of features to analyze: {}'.format(len(relevant_var))        

    #Score columns for each category
    score_bill = [x + '_score' for x in bill_var]
    score_orders = [x + '_score' for x in orders_var]
    score_reimb = [x + '_score' for x in reimb_var]
    score_tickets_fact = [x + '_score' for x in tickets_var]
    score_ccc = [x + '_score' for x in ccc_var]
    score_rgu = [x + '_score' for x in rgu_var]
    
    
    #Score columns for each variable and ranking columns
    score_cols = [name_ + '_score' for name_ in relevant_var]
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    val_cols = ['top'+ str(i) + '_val' for i in range(0,top_feats)]
    
    #Score columns for each category
    score_cols_2 = ['max_val_bill','max_val_reimb', 'max_val_ord', 'max_val_tickets', 'max_val_ticketsS', 'max_val_rgu']
    #Reasons (categories)
    reasons = ['Billing','Reimbursements', 'Orders and SLA', 'Billing Tickets', 'Service', 'Customer']
    
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
    df_aux = getMaxValGroup(spark, df_aux, score_orders, 'ord')
    df_aux = getMaxValGroup(spark, df_aux, score_reimb, 'reimb')
    df_aux = getMaxValGroup(spark, df_aux, score_tickets_fact, 'tickets')
    df_aux = getMaxValGroup(spark, df_aux, score_ccc, 'ticketsS')
    df_aux = getMaxValGroup(spark, df_aux, score_rgu, 'rgu')
    
    #Get ordered Scores
    df_ord = getTopNVals(spark, df_aux, score_cols_2, n_groups)
    
    #Get ordered reasons    
    df_final = getTopNFeatsK2(spark, df_ord, n_groups, filtered_cols = score_cols_2)
    
    from pyspark.sql.functions import log2
    #Entropy of the first reason
    df_final = df_final.withColumn('Incertidumbre', when(df_final['top0_val'] < 1, -df_final['top0_val']*log2(df_final['top0_val'])-(1-df_final['top0_val'])*log2(1-df_final['top0_val'])).otherwise(0))
    
    #Save results
    save_df = df_final.select(['NIF_CLIENTE'] +  name_cols  + ['Incertidumbre'])\
    .withColumn('executed_at', lit(executed_at)).withColumn('model', lit('csanc109')).withColumn('closing_day', lit(str(args.closing_d))) 
    save_df.repartition(300).write.save(save_dir, format='parquet', mode='append')
    
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")