import sys
sys.path.append("/var/SP/data/home/asaezco/src/devel/pykahos")
#sys.path.append("/var/SP/data/home/asaezco/src/devel/use-cases")
from datetime import datetime as dt
import imp
import os
import numpy as np
#from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull,substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
#from churn.config_manager.config_mgr import Config
#from churn.datapreparation.engine.churn_data_loader import get_labeled_or_unlabeled_car
#from churn.utils.general_functions import init_spark
import pykhaos.utils.pyspark_configuration as pyspark_config
from pyspark.sql.functions import *
from map_funct import getFeatGroups

def getStats(spark, df, features, key = 'label'):
    from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
    df_nc = df.filter(df[key] == 0)
    df_c = df.filter(df[key] == 1)
    #df_nc.cache()
    #df_c.cache()   
    stats_no = []
    stats_c = [] 
    i = 0
    for sel_ in features:
        print(sel_)
        stats_nc = df_nc.select(_mean(col(sel_)).alias('mean'),_stddev(col(sel_)).alias('std')).collect()
        mean_ = stats_nc[0]['mean']
        std = stats_nc[0]['std']
        stats_no.append((mean_,std))
        print(std)
        stats = df_c.select(_mean(col(sel_)).alias('mean'),_stddev(col(sel_)).alias('std')).collect()
        mean_ = stats[0]['mean']
        std = stats[0]['std']
        stats_c.append((mean_,std))        
        print(std)
        i = i+1
    #df_nc.unpersist()
    #df_c.unpersist()
    return stats_no, stats_c

def getMaxValGroup(spark, df, selected_cols, group):
    for col_name in selected_cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))
    name_val = 'max_val_' + group
    df = df.withColumn(name_val, sort_array(array([col(x) for x in selected_cols]), asc=False)[0])
    return(df)

def getTopNVals(spark, df, filtered_cols, n_feats):
    for col_name in filtered_cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        df = df.withColumn(name_val, sort_array(array([col(x) for x in filtered_cols]), asc=False)[i])
    return(df)

def getTopNFeatsK2(spark, df, n_feats, filtered_cols):
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

global relevant_var, reasons

if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= int, help= 'closing day', default = 20190331)
    parser.add_argument('-p', '--pred_name', metavar = '<pred_name>', type= str, help= 'pred_name', default = 'None')
    parser.add_argument('-f', '--filename', metavar = '<filename>', type= str, help= 'filename', default = 'kernel_20.yaml')
    #parser.add_argument('-c', '--config_f', metavar = '<config_f>', type= str, help= 'configuration file', default = "model_under_eval.yaml")
    parser.add_argument('-s', '--segment', metavar = '<segment>', type= str, help= 'segment under analysis', default = "mob")
    
    args = parser.parse_args()
    
    import yaml
    config = yaml.load(open(args.filename))

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
   
    #config_obj = Config(config['config_f'], internal_config_filename=os.path.join(imp.find_module('churn')[1], "config_manager", "config", "internal_config_churn.yaml"))
    #print("############ Config Object Created ############")
    
    n_feats = config['number_feats']
    top_feats = config['top_feats']
    key = "label"
    now = str(args.closing_d) #dt.now().strftime("%Y%m%d")
    if args.segment == 'mob':
        save_dir = 'tests_es.asaezco_kernel_churn_reasons_mob_' + now
    elif args.segment == 'mobandfbb':
        save_dir = 'tests_es.asaezco_kernel_churn_reasons_mobandfbb_' + now
    closing_day = args.closing_d
    
    if args.pred_name != 'None' and config['test_df'] != 'None':
        print("############ Loading Dataset Specified with Arguments ############")
        Train_ = spark.read.parquet('/data/udf/vf_es/churn/models/' + args.pred_name + '/training_set')
        
        num_t = Train_.count()
        print('Size of Training Dataframe:')
        print(num_t)
        if num_t > 70000:
            Train_ = getDfSample(spark, Train_, key, 35000)

        car_f = spark.read.load(config['test_df']+ str(args.closing_d))
        
        if args.segment == 'mob':
            df_car_1 = car_f.where((col("num_movil") >= 1) & (col("num_fixed") == 0) & (col("num_tv") == 0) & (col("num_bam") == 0) & (col("num_fbb") == 0))
        elif args.segment == 'mobandfbb':
            df_car_1 = car_f.where((col("num_movil") > 0) & (col("num_fbb") > 0))
        
        c_day = str(args.closing_d)
        name_extra = '/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'.format(int(c_day[:4]),int(c_day[4:6]),int(c_day[6:]))                          
        extra_feats = spark.read.load(name_extra)
        df_car = df_car_1.join(extra_feats, ['msisdn'],'inner')
        print("############ Finished loading Datasets ############")
        
        feats = spark.read.load('/data/udf/vf_es/churn/models/' + args.pred_name + '/feat_importance')
        
        feat_list = feats.orderBy(feats['importance'],ascending= False).select('feature').rdd.map(lambda x: str(x[0])).collect()
        
        if args.segment == 'mob':
            n_feats = 20
        elif args.segment == 'mobandfbb':
            n_feats = 16
            
        use_var, bill_var, inter_var, spinners_var, serv_var,  penal_var = getFeatGroups(spark, df_car, feat_list[:100], n_feats, 1)
        
        relevant_var = spinners_var + bill_var + penal_var + serv_var + use_var  + inter_var
        print("############ Finished loading Most Important Features ############")
        print 'Number of features to analyze: {}'.format(len(relevant_var))        
    else:
        print('Error Loading Input Datasets')
        import sys
        sys.exit(0)
    
    score_bill = [x + '_score' for x in bill_var]
    score_spinners = [x + '_score' for x in spinners_var]
    score_penal = [x + '_score' for x in penal_var]
    score_serv = [x + '_score' for x in serv_var]
    score_use = [x + '_score' for x in use_var]
    score_inter = [x + '_score' for x in inter_var]
    
    score_cols = [name_ + '_score' for name_ in relevant_var]
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    val_cols = ['top'+ str(i) + '_val' for i in range(0,top_feats)]

    score_cols_2 = ['max_val_pen','max_val_serv','max_val_bill','max_val_spin', 'max_val_use', 'max_val_inter']
    reasons = ['Penalization','Engagement','Billing','Spinner', 'Use', 'Interactions']
    n_groups = len(reasons)    

      
    if config['nulls_df'] != 'None':    
        table_meta_name = config['nulls_df']
    else:
        print("############ Loading Churn Metadata Table ############")
        table_meta_name = 'hdfs://nameservice1/user/hive/warehouse/tests_es.db/jvmm_metadata'        
    table_meta = spark.read.option("delimiter", "|").option("header", True).csv(table_meta_name)
    print("############ Finished Loading Metadata Table ############")
        
    null_vals = []
    for name_ in relevant_var:
        print(name_)
        val = table_meta.filter(table_meta['feature']== name_).select('imputation').rdd.map(lambda x: x[0]).collect()[0]
        print(val)
        null_vals.append(float(val))
        
    label = key
    n_0s = Train_.filter(col(label) == 0.0).count()
    n_1s = Train_.filter(col(label) == 1.0).count()
    p_0s = float(n_0s)/(n_0s + n_1s)
    p_1s = float(n_1s)/(n_0s + n_1s)
    
    stats_no, stats_c = getStats(spark, Train_, relevant_var, key )
    
    import pyspark.mllib.stat.KernelDensity as Kernel   

    for col_name in relevant_var:
        df_car = df_car.withColumn(col_name, col(col_name).cast('float'))
        Train_ = Train_.withColumn(col_name, col(col_name).cast('float'))    

    df_churners = Train_.filter(Train_[label] == 1)
    df_no_churners = Train_.filter(Train_[label] == 0)
    
    #df_churners.cache()
    #df_no_churners.cache()

    i = 0

    df_aux = df_car.select('msisdn')

    for name_ in relevant_var:

        kd_c = Kernel()
        kd_nc = Kernel()
        print('Predicting feature: ')
        print(name_)

        rdd_c = df_churners.select(name_).rdd.map(lambda x: x[0])
        kd_c.setSample(rdd_c)
        h_c = stats_c[i][1]*(4/3/n_1s)**(1/5)
        kd_c.setBandwidth(h_c)   
        sel = df_car.select(['msisdn',name_])

        lists = [(row['msisdn'],row[name_]) for row in sel.collect()]
        msisdns, var = zip(*lists)
        pred = kd_c.estimate(np.array(var))

        pred2 = map(float, pred)
        save = zip(msisdns,pred2)
        
        rdd1 = spark.sparkContext.parallelize(save)
        rdd2 = rdd1.map(lambda x: [i for i in x])
        df_new = rdd2.toDF(['msisdn',name_+'_p1'])

        df_aux = df_aux.join(df_new, ['msisdn'], 'inner') 
        
        rdd_c = df_no_churners.select(name_).rdd.map(lambda x: x[0])
        kd_nc.setSample(rdd_c)
        h_nc = stats_no[i][1]*(4/3/n_0s)**(1/5)
        kd_nc.setBandwidth(h_nc)   
        
        pred = kd_nc.estimate(np.array(var))
        pred2 = map(float, pred)
        save = zip(msisdns,pred2)

        rdd1 = spark.sparkContext.parallelize(save)
        rdd2 = rdd1.map(lambda x: [i for i in x])
        df_new = rdd2.toDF(['msisdn',name_+'_p0'])

        df_aux = df_aux.join(df_new, ['msisdn'], 'inner')
        
        i = i + 1
        
    df_aux = df_aux.join(df_car, ['msisdn'], 'inner')
    
    #df_churners.unpersist()
    #df_no_churners.unpersist()
    
    j = 0   
    for name_ in relevant_var:
        df_aux = df_aux.withColumn(name_ + '_score', when(df_aux[name_]!= null_vals[j],(p_1s * df_aux[name_+'_p1'])/(p_1s * df_aux[name_+'_p1'] + p_0s * df_aux[name_+'_p0'])).otherwise(0))
        j = j+1
        
    df_aux = getMaxValGroup(spark, df_aux, score_penal, 'pen')
    df_aux = getMaxValGroup(spark, df_aux, score_serv, 'serv')
    df_aux = getMaxValGroup(spark, df_aux, score_bill, 'bill')
    df_aux = getMaxValGroup(spark, df_aux, score_spinners, 'spin')
    df_aux = getMaxValGroup(spark, df_aux, score_use, 'use')
    df_aux = getMaxValGroup(spark, df_aux, score_inter, 'inter')
    
    df_ord = getTopNVals(spark, df_aux, score_cols_2, n_groups)
        
    df_final = getTopNFeatsK2(spark, df_ord, n_groups, filtered_cols = score_cols_2)
    
    from pyspark.sql.functions import log2

    df_final = df_final.withColumn('Incertidumbre', when(df_final['top0_val'] < 1, -df_final['top0_val']*log2(df_final['top0_val'])-(1-df_final['top0_val'])*log2(1-df_final['top0_val'])).otherwise(0))
    
    df_final.select(['msisdn'] +  name_cols + score_cols_2 + score_cols +['Incertidumbre']).write.format('parquet').mode('overwrite').saveAsTable(save_dir)
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")
