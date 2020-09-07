import sys
sys.path.append("/var/SP/data/home/asaezco/src/devel2/pykahos")
from datetime import datetime as dt
import imp
import os
import numpy as np
from pyspark.sql.types import DoubleType, StringType, IntegerType
import pykhaos.utils.pyspark_configuration as pyspark_config
from pyspark.sql.functions import *
from map_funct import getFeatGroups

def getStats(spark, df, features, key = 'label'):
    #Function to obtain mean and std of a column
    from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
    df_nc = df.filter(df[key] == 0)
    df_c = df.filter(df[key] == 1)
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
TABLE_ONLYMOB = 'tests_es.churn_team_kernel_churn_reasons_onlymob_'
TABLE_MOBILEANDFBB = 'tests_es.churn_team_kernel_churn_reasons_mobileandfbb_'
TABLE_OTHERS = 'tests_es.churn_team_kernel_churn_reasons_others_'

if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= int, help= 'closing day', default = 20190331)
    parser.add_argument('-p', '--pred_name', metavar = '<pred_name>', type= str, help= 'pred_name', default = 'None')
    parser.add_argument('-f', '--filename', metavar = '<filename>', type= str, help= 'filename', default = 'kernel_20.yaml')
    parser.add_argument('-s', '--segment', metavar = '<segment>', type= str, help= 'segment under analysis', default = "onlymob")
    
    args = parser.parse_args()
    
    import datetime    
    executed_at = str(datetime.datetime.now())
    
    model_level = 'service'
    
    model_name = 'churn_explanaible_analytics_' + args.segment
    
    training_closing_date = str(args.closing_d)
    
    target = 'Extracted from the portablities table'
    
    model_path = '/data/attributes/vf_es/model_outputs/' + model_name + '/model/'
    
    metrics_path = '/data/attributes/vf_es/model_outputs/' + model_name + '/metrics/'
    
    metrics_train = 'None'
    
    metrics_test = 'None'  
    
    algorithm = 'Bayesian Classifier based on Pyspark Kernel Density Functions'
    author_login = 'asaezco'
    
    extra_info = 'Top 6 reasons for customer churn risk'
    
    scores_extra_info_headers = 'None'
    
    time = executed_at[11:13] + executed_at[14:16] + executed_at[17:19]
    
    year = int(executed_at[:4])
    
    month = int(executed_at[5:7])
    
    day = int(executed_at[8:10])    
    #Configuration file
    import yaml
    config = yaml.load(open(args.filename))

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
    #Number of feats to analyze
    n_feats = config['number_feats']
    #Number of feats that compose the ranking
    top_feats = config['top_feats']
    key = "label"
    closing_day = str(args.closing_d)
    #Save directory
    if args.segment == 'onlymob':
        save_dir = TABLE_ONLYMOB + closing_day
    elif args.segment == 'mobileandfbb':
        save_dir = TABLE_MOBILEANDFBB + closing_day
    elif args.segment == 'others':
        save_dir = TABLE_OTHERS + closing_day       
    #Train and Test Dataframes
    if args.pred_name != 'None' and config['test_df'] != 'None':
        print("############ Loading Dataset Specified with Arguments ############")
        Train_ = spark.read.parquet('/data/udf/vf_es/churn/models/' + args.pred_name + '/training_set')
        
        num_t = Train_.count()
        print('Size of Training Dataframe:')
        print(num_t)
        if num_t > 150000:
            Train_ = getDfSample(spark, Train_, key, 75000)

        car_f = spark.read.load(config['test_df']+ str(args.closing_d))
        
        if args.segment == 'onlymob':
            df_car_1 = car_f.where((col("num_movil") >= 1) & (col("num_fixed") == 0) & (col("num_tv") == 0) & (col("num_bam") == 0) & (col("num_fbb") == 0))
        elif args.segment == 'mobileandfbb':
            df_car_1 = car_f.where((col("num_movil") > 0) & (col("num_fbb") > 0))
        elif args.segment == 'others':
            df_car_1 = car_f.where((col("num_movil") > 0) & (col("num_fbb") == 0) & ((col("num_fixed") > 0) | (col("num_tv") > 0) | (col("num_bam") > 0)))      
            
        c_day = str(args.closing_d)
        name_extra = '/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'.format(int(c_day[:4]),int(c_day[4:6]),int(c_day[6:]))                          
        extra_feats = spark.read.load(name_extra)
        df_car = df_car_1.join(extra_feats, ['msisdn'],'inner')
        print("############ Finished loading Datasets ############")
    #Feats to analyze and importance
        feats = spark.read.load('/data/udf/vf_es/churn/models/' + args.pred_name + '/feat_importance')
        
        feat_list = feats.orderBy(feats['importance'],ascending= False).select('feature').rdd.map(lambda x: str(x[0])).collect()
    #Feature mapper        
        use_var, bill_var, inter_var, spinners_var, serv_var,  penal_var = getFeatGroups(spark, df_car, feat_list[:100], n_feats, 1)
        
        relevant_var = spinners_var + bill_var + penal_var + serv_var + use_var  + inter_var
        varimp = relevant_var
        print("############ Finished loading Most Important Features ############")
        print 'Number of features to analyze: {}'.format(len(relevant_var))        
    else:
        print('Error Loading Input Datasets')
        import sys
        sys.exit(0)
    #Score columns for each category
    score_bill = [x + '_score' for x in bill_var]
    score_spinners = [x + '_score' for x in spinners_var]
    score_penal = [x + '_score' for x in penal_var]
    score_serv = [x + '_score' for x in serv_var]
    score_use = [x + '_score' for x in use_var]
    score_inter = [x + '_score' for x in inter_var]
    
    #Score columns for each variable and ranking columns
    score_cols = [name_ + '_score' for name_ in relevant_var]
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    val_cols = ['top'+ str(i) + '_val' for i in range(0,top_feats)]
    
    #Score columns for each category
    score_cols_2 = ['max_val_pen','max_val_serv','max_val_bill','max_val_spin', 'max_val_use', 'max_val_inter']
    #Reasons (categories)
    reasons = ['Penalization','Engagement','Billing','Spinner', 'Use', 'Interactions']
    n_groups = len(reasons)    

    #Null values  
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
    df_aux = df_car.select( ['msisdn'] + relevant_var)
    
    model_executed_at = str(datetime.datetime.now())   
    predict_closing_date = str(args.closing_d)    

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
        df_aux = df_aux.withColumn(name_ + '_score', when(df_aux[name_]!= null_vals[j],(p_1s * df_aux[name_+'_p1'])/(p_1s * df_aux[name_+'_p1'] + p_0s * df_aux[name_+'_p0'])).otherwise(0))
        j = j+1
    #Get max score for each category    
    df_aux = getMaxValGroup(spark, df_aux, score_penal, 'pen')
    df_aux = getMaxValGroup(spark, df_aux, score_serv, 'serv')
    df_aux = getMaxValGroup(spark, df_aux, score_bill, 'bill')
    df_aux = getMaxValGroup(spark, df_aux, score_spinners, 'spin')
    df_aux = getMaxValGroup(spark, df_aux, score_use, 'use')
    df_aux = getMaxValGroup(spark, df_aux, score_inter, 'inter')
    
    #Get ordered Scores
    df_ord = getTopNVals(spark, df_aux, score_cols_2, n_groups)
    
    #Get ordered reasons    
    df_final = getTopNFeatsK2(spark, df_ord, n_groups, filtered_cols = score_cols_2)
    
    from pyspark.sql.functions import log2
    #Entropy of the first reason
    df_final = df_final.withColumn('Incertidumbre', when(df_final['top0_val'] < 1, -df_final['top0_val']*log2(df_final['top0_val'])-(1-df_final['top0_val'])*log2(1-df_final['top0_val'])).otherwise(0))
    #Save results
    df_final.select(['msisdn'] +  name_cols + score_cols_2 + score_cols +['Incertidumbre']).write.format('parquet').mode('overwrite').saveAsTable(save_dir)
    df_final_ids = df_final.join(df_car.select('msisdn','num_cliente','nif_cliente'),['msisdn'], how='left')
    
    df_model_outputs_scores = df_final_ids.select(['msisdn', 'num_cliente', 'nif_cliente'] + val_cols + name_cols + ['Incertidumbre']).withColumn('executed_at', lit(executed_at))\
.withColumn('model_executed_at', lit(model_executed_at)).withColumn('predict_closing_date', lit(predict_closing_date))\
.withColumn('client_id', col('num_cliente')).withColumn('nif', col('nif_cliente'))\
.withColumn('model_output',  concat_ws(';',*val_cols)).withColumn('scoring', df_final_ids['Incertidumbre']).withColumn('prediction',  concat_ws(';',*name_cols))\
.withColumn('extra_info', lit(extra_info)).withColumn('scores_extra_info_headers', lit(scores_extra_info_headers)).withColumn('time', lit(time)).withColumn('model_name',lit(model_name)).withColumn('year', lit(year)).withColumn('month', lit(month))\
.withColumn('day', lit(day))

    for c in val_cols + name_cols + ['Incertidumbre']:
        df_model_outputs_scores = df_model_outputs_scores.drop(col(c))
    
    schema = StructType([StructField('model_name', StringType(), True), StructField('executed_at', StringType(), True), StructField('model_level', StringType(), True), StructField('training_closing_date', StringType(), True), StructField('target', StringType(), True), StructField('model_path', StringType(), True), StructField('metrics_path', StringType(), True), StructField('metrics_train', StringType(), True), StructField('metrics_test', StringType(), True), StructField('varimp', StringType(), True), StructField('algorithm', StringType(), True), StructField('author_login', StringType(), True), StructField('extra_info', StringType(), True), StructField('scores_extra_info_headers', StringType(), True), StructField('year', IntegerType(), True), StructField('month', IntegerType(), True), StructField('day', IntegerType(), True), StructField('time', IntegerType(), True)])
    
    modelParam = [{'model_name': model_name, 'executed_at': executed_at,
                   'model_level': model_level,
                   'training_closing_date': training_closing_date,
                   'target': 'port',
                   'model_path': model_path,
                   'metrics_path': metrics_path,
                   'metrics_train': 'None',
                   'metrics_test': 'None',
                   'varimp': varimp,
                   'algorithm': algorithm,
                   'author_login': author_login,
                   'extra_info': extra_info,
                   'scores_extra_info_headers': scores_extra_info_headers,
                   'year': year,
                   'month': month,
                   'day': day,
                   'time': time}]
    
    
    dfModelParameters = spark.createDataFrame(modelParam, schema)

    dfModelParameters = dfModelParameters.coalesce(1)
    dfModelParameters.write.mode("append").format("parquet").save( "/data/attributes/vf_es/model_outputs/model_parameters/model_name={}/year={}/month={}/day={}".format(model_name, year, month, day)) 
    
    save_dir = "/data/attributes/vf_es/model_outputs/model_scores/model_name={}/year={}/month={}/day={}".format(model_name, year, month, day)
    
    df_model_outputs_scores.write.format('parquet').mode('append').saveAsTable(save_dir)    
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")