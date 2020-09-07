from pyspark.sql.functions import *
from  pyspark.sql.functions import abs

def getDeviationdf(spark, df, features, stats, key = 'label'):

    media = stats.filter(stats['summary']=='mean')
    std = stats.filter(stats['summary']=='stddev')
    min_ = stats.filter(stats['summary']=='min')
    max_ = stats.filter(stats['summary']=='max')

    df_dev = df.select(['msisdn',key] + features)
    
    for name_ in features:
        media_n = float(media.select(name_).collect()[0][0])
        std_n = float(std.select(name_).collect()[0][0])
        df_dev = df_dev.withColumn(name_ + "_dev", (df_dev[name_] - media_n)/std_n)
    dev_columns = [name_ + '_dev' for name_ in features]
    
    return dev_columns, df_dev

def getChurnArq(spark, features, stats, stats_churn, key = 'label'):
    
    arq = []
    
    media = stats.filter(stats['summary']=='mean')
    std = stats.filter(stats['summary']=='stddev')
    min_ = stats.filter(stats['summary']=='min')
    max_ = stats.filter(stats['summary']=='max')

    media_churn = stats_churn.filter(stats_churn['summary']=='mean')
    std_churn = stats_churn.filter(stats_churn['summary']=='stddev')
    min_churn = stats_churn.filter(stats_churn['summary']=='min')
    max_churn = stats_churn.filter(stats_churn['summary']=='max')
    
    for name_ in features:
        media_c = float(media_churn.select(name_).collect()[0][0])
        media_n = float(media.select(name_).collect()[0][0])
        std_n = float(std.select(name_).collect()[0][0])
        arq.append((name_ + '_dev',(media_c-media_n)/std_n,(media_c-media_n > 0)))
    return(arq)


def getFilteredDevdf(spark, df, arq):
    for (name_,desv,dir_) in arq:
        if dir_:
            df = df.withColumn(name_ + "_fil", when(df[name_] > desv/2, abs(df[name_])).otherwise(0))
        else:
            df = df.withColumn(name_ + "_fil", when(df[name_] < desv/2, abs(df[name_])).otherwise(0))
    return(df)

def modify_values(r, max_col):
    l = []
    for i in range(len(filtered_cols)):
        if r[i]== max_col:
            l.append(filtered_cols[i])
    return l

def modify_values2(r, max_col, filtered_cols):
    l = []
    for i in range(len(filtered_cols)):
        if r[i]== max_col:
            l.append(filtered_cols[i])
    return l

def getTopNVals(spark, df, filtered_cols, n_feats):
    for col_name in filtered_cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        df = df.withColumn(name_val, sort_array(array([col(x) for x in filtered_cols]), asc=False)[i])
    return(df)

def getTopNFeatsK(spark, df, n_feats, filtered_cols):
    from pyspark.sql.types import DoubleType, StringType
    df2 = df   
    modify_values_udf = udf(modify_values2, StringType())
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        name_feat = 'top{}_feat'.format(i)
        df =  df\
        .withColumn(name_feat, modify_values_udf(array(df2.columns[-len(filtered_cols)-n_feats:-n_feats]),name_val, array(filtered_cols)))       
    return(df)

def getTopNFeats(spark, df, n_feats, filtered_cols):
    from pyspark.sql.types import DoubleType, StringType
    df2 = df   
    modify_values_udf = udf(modify_values, StringType())
    for i in range (0,n_feats):
        name_val = 'top{}_val'.format(i)
        name_feat = 'top{}_feat'.format(i)
        df =  df\
        .withColumn(name_feat, modify_values_udf(array(df2.columns[-len(filtered_cols)-n_feats:-n_feats]),name_val))

    for i in range (0,n_feats):
        name_col = 'top{}_feat_2'.format(i)
        name_feat = 'top{}_feat'.format(i)
        name_val = 'top{}_val'.format(i)
        df =  df\
        .withColumn(name_col, when(df[name_val] > 0, df[name_feat]).otherwise('-'))        
    return(df)

def getDeviationdf_nulls(spark, df, features, stats_nc, vals, key = 'label'):

    df_dev = df.select(['msisdn',key] + features)
    i = 0
    for name_ in features:
        df_dev = df_dev.withColumn(name_ + "_dev", 
                                   when(df_dev[name_] != vals[i], ((df_dev[name_] - stats_nc[i][0])/stats_nc[i][1])).otherwise(0))
        i = i+1
    dev_columns = [name_ + '_dev' for name_ in features]
    
    return dev_columns, df_dev

def getStats(spark, df, features, vals, key = 'label'):
    from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
    df_nc = df.filter(df[key] == 0)
    df_c = df.filter(df[key] == 1)
    df_nc.cache()
    df_c.cache()   
    stats_no = []
    stats_c = [] 
    i = 0
    for sel_ in features:
        print(sel_)
        stats_nc = df_nc.filter(df_nc[sel_] != vals[i])\
        .select(_mean(col(sel_)).alias('mean'),_stddev(col(sel_)).alias('std')).collect()
        mean_ = stats_nc[0]['mean']
        std = stats_nc[0]['std']
        stats_no.append((mean_,std))
        print(mean_)
        stats = df_c.filter(df_c[sel_] != vals[i])\
        .select(_mean(col(sel_)).alias('mean')).collect()
        mean_ = stats[0]['mean']
        std = stats_nc[0]['std']
        stats_c.append((mean_,std))        
        print(mean_)
        i = i+1
    df_nc.unpersist()
    df_c.unpersist()
    return stats_no, stats_c

def getChurnArq_nulls(spark, features, stats_nc, stats_churn):
    
    arq = []   
    i = 0
    
    for name_ in features:
        arq.append((name_ + '_dev',(stats_churn[i][0]-stats_nc[i][0])/stats_nc[i][1],(stats_churn[i][0]-stats_nc[i][0] > 0)))
        i = i+1
    return(arq)