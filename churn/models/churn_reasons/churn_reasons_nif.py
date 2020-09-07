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

TABLE_FULL = '/data/udf/vf_es/churn/churn_reasons/churn_reasons_results'
TABLE_SAVE = '/data/udf/vf_es/churn/churn_reasons/churn_reasons_NIF'

if __name__ == "__main__":

    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= int, help= 'closing day', default = 20190331)
    parser.add_argument('-f', '--filename', metavar = '<filename>', type= str, help= 'filename', default = 'kernel_20.yaml')
    
    args = parser.parse_args()
    set_paths_and_logger()

    import pykhaos.utils.pyspark_configuration as pyspark_config
    from pyspark.sql.functions import *
    from map_funct import getFeatGroups

    #Configuration file
    import yaml
    config = yaml.load(open(args.filename))
    
    top_feats = config['top_feats']
    key = "label"
    closing_day = str(args.closing_d)

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
    
    #Score columns for each variable and ranking columns
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    val_cols = ['top'+ str(i) + '_val' for i in range(0,top_feats)]
    
    #Score columns for each category
    score_cols_2 = ['max_val_pen','max_val_serv','max_val_bill','max_val_spin', 'max_val_use', 'max_val_inter']
    #Reasons (categories)
    reasons = ['Penalization','Engagement','Billing','Spinner', 'Use', 'Interactions']
    n_groups = len(reasons)
    save_dir = TABLE_SAVE

    #Table Loading  
    df_mob = spark.read.load('/data/udf/vf_es/churn/churn_reasons/churn_reasons_NIF_MOB').where(col('closing_day')==str(args.closing_d))
    df_fbb = spark.read.load('/data/udf/vf_es/churn/churn_reasons/churn_reasons_NIF_FBB').where(col('closing_day')==str(args.closing_d))
    
    #df_fbb = spark.read.load('/data/udf/vf_es/churn/churn_reasons/churn_reasons_NIF').where((col('closing_day')==str(args.closing_d))&(col('service') == 'fbb'))
    #df_mob = spark.read.load('/data/udf/vf_es/churn/churn_reasons/churn_reasons_NIF').where((col('closing_day')==str(args.closing_d))&(col('service')== 'mobile'))
    
    from pyspark.sql.functions import mean, countDistinct
    aggr_mob = df_mob.groupby('NIF_CLIENTE').agg(mean('max_val_pen').alias('max_val_pen_mob'),mean('max_val_serv').alias('max_val_serv_mob'),mean('max_val_bill').alias('max_val_bill_mob'),mean('max_val_spin').alias('max_val_spin_mob'),mean('max_val_use').alias('max_val_use_mob'),mean('max_val_inter').alias('max_val_inter_mob'), countDistinct('msisdn').alias('NUM_MSISDN_MOB'))
    
    aggr_fbb = df_fbb.groupby('NIF_CLIENTE').agg(mean('max_val_pen').alias('max_val_pen_fbb'),mean('max_val_serv').alias('max_val_serv_fbb'),mean('max_val_bill').alias('max_val_bill_fbb'),mean('max_val_spin').alias('max_val_spin_fbb'),mean('max_val_use').alias('max_val_use_fbb'),mean('max_val_inter').alias('max_val_inter_fbb'), countDistinct('msisdn').alias('NUM_MSISDN_FBB'))
    
    table_full = aggr_fbb.join(aggr_mob, ['NIF_CLIENTE'], 'full')
    
    fill_cols = ['max_val_pen_fbb','max_val_serv_fbb','max_val_bill_fbb','max_val_spin_fbb','max_val_use_fbb','max_val_inter_fbb','NUM_MSISDN_FBB','max_val_pen_mob','max_val_serv_mob','max_val_bill_mob','max_val_spin_mob','max_val_use_mob','max_val_inter_mob','NUM_MSISDN_MOB']
    
    for name_ in fill_cols:
        table_full = table_full.na.fill({name_ : 0.0})
        
    names = ['pen', 'serv', 'bill', 'use', 'inter', 'spin']
    for name_ in names:
        table_full = table_full.withColumn('max_val_' + name_, (col('max_val_' + name_ + '_fbb')*col('NUM_MSISDN_FBB') + col('max_val_' + name_ + '_mob')*col('NUM_MSISDN_MOB'))/((col('NUM_MSISDN_MOB') + col('NUM_MSISDN_FBB'))))
    
    #Get ordered Scores
    df_ord = getTopNVals(spark, table_full, score_cols_2, n_groups)
    
    #Get ordered reasons    
    df_final = getTopNFeatsK2(spark, df_ord, n_groups, filtered_cols = score_cols_2)
    
    from pyspark.sql.functions import log2
    #Entropy of the first reason
    df_final = df_final.withColumn('Incertidumbre', when(df_final['top0_val'] < 1, -df_final['top0_val']*log2(df_final['top0_val'])-(1-df_final['top0_val'])*log2(1-df_final['top0_val'])).otherwise(0))
    
    
    import datetime    
    executed_at = str(datetime.datetime.now())
    
    #Save results
    #+ score_cols_2 + score_cols
    save_df = df_final.select(['nif_cliente'] +  name_cols +['Incertidumbre'])\
    .withColumn('executed_at', lit(executed_at)).withColumn('closing_day', lit(str(args.closing_d))) 
    save_df.repartition(300).write.save(save_dir, format='parquet', mode='append')
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")