from datetime import datetime as dt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

TABLE_FULL = '/data/udf/vf_es/churn/churn_reasons/churn_reasons_table'

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


if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= int, help= 'closing day', default = 20190307)
    parser.add_argument('-p', '--pred_name', metavar = '<pred_name>', type= str, help= 'pred_name', default = 'None')
    
    args = parser.parse_args()

    set_paths_and_logger()
    import pykhaos.utils.pyspark_configuration as pyspark_config

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="main_distributions", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")  
    
    key = "label"
    closing_day = args.closing_d
    save_dir = TABLE_FULL
    top_feats = 6
    name_cols = ['top'+ str(i) + '_reason' for i in range(0,top_feats)]
    
    table_name_1 = 'hdfs://nameservice1/user/hive/warehouse/tests_es.db/churn_team_kernel_churn_reasons_mobileandfbb_' + str(closing_day)
    table_name_2 = 'hdfs://nameservice1/user/hive/warehouse/tests_es.db/churn_team_kernel_churn_reasons_onlymob_' + str(closing_day)
    table_name_3 = 'hdfs://nameservice1/user/hive/warehouse/tests_es.db/churn_team_kernel_churn_reasons_others_' + str(closing_day)
    
    #table_name_1 = '/data/udf/vf_es/churn/churn_reasons_mobileandfbb_' + str(closing_day)
    #table_name_2 = '/data/udf/vf_es/churn/churn_reasons_onlymob_' + str(closing_day)
    #table_name_3 = '/data/udf/vf_es/churn/churn_reasons_others_' + str(closing_day)
    
    df_mob = spark.read.parquet(table_name_2)
    df_mobandfbb = spark.read.parquet(table_name_1)
    df_others = spark.read.parquet(table_name_3)
    
    df_mob_sel = df_mob.select(['msisdn','Incertidumbre'] + name_cols)
    df_mobandfbb_sel = df_mobandfbb.select(['msisdn','Incertidumbre'] + name_cols)
    df_others_sel = df_others.select(['msisdn','Incertidumbre'] + name_cols)
    
    import datetime    
    executed_at = str(datetime.datetime.now())
    import re
    gg = re.match(".*_tr(.*)_tt.*", args.pred_name)
    
    if gg:
        pred_name = gg.group(1)
    else:
        import sys
        sys.exit('Wrong pred_name')
        
    from pyspark.sql.functions import lit
    full = df_mobandfbb_sel.union(df_mob_sel).union(df_others_sel).withColumn('executed_at', lit(executed_at)).withColumn('segment',lit('full')).withColumn('training_closing_date', lit(pred_name))      
    print("############ Finished Union ############")
    
    full.repartition(200).write.save(save_dir, format='parquet', mode='append')
    #full.write.format('parquet').mode('overwrite').saveAsTable(save_dir)
    
    print("Table saved as")
    print(save_dir)
    
    for seg in [(df_mob_sel, 'mobile'),(df_mobandfbb_sel, 'mobile and fbb'), (df_others, 'others')]:
        
        print'Segment {}'.format(seg[1])
        df = seg[0]
    
        total = df.count()
        spin = df.filter(df['top0_reason'] == '[Spinner]').count()
        use = df.filter(df['top0_reason'] == '[Use]').count()
        penal = df.filter(df['top0_reason'] == '[Penalization]').count()
        billing = df.filter(df['top0_reason'] == '[Billing]').count()
        services = df.filter(df['top0_reason'] == '[Engagement]').count()
        interact = df.filter(df['top0_reason'] == '[Interactions]').count()

        spinners = float(100*spin)/total
        use_p = float(100*use)/total
        penalization = float(100*penal)/total
        billing_p = float(100*billing)/total
        engagement = float(100*services)/total
        interactions_ = float(100*interact)/total

        print 'Churn explained by Use: {}%'.format(use_p)
        print 'Churn explained by Billing: {}%'.format(billing_p)
        print 'Churn explained by Engagement: {}%'.format(engagement)
        print 'Churn explained by Penalization: {}%'.format(penalization)
        print 'Churn explained by Spinners: {}%'.format(spinners)
        print 'Churn explained by Interactions: {}%'.format(interactions_)
        
        labels = ['Penalization', 'Spinner','Use', 'Billing', 'Engagement', 'Interactions']
        arr = [penalization, spinners, use_p, billing_p, engagement, interactions_]
        
        tuples = zip(labels, arr)       
        sorted_ = sorted(tuples, key=lambda tup: tup[1], reverse=True)
        
        labels, arr = zip(*sorted_)
        
        index = np.arange(len(labels))
        plt.bar(index, arr)
        plt.xlabel('Reason', fontsize=10)
        plt.ylabel('Explained %', fontsize=10)
        plt.xticks(index, labels, fontsize=8, rotation=30)
        plt.title('First explanation for churn on segment: {}'.format(seg[1]))
        plt.savefig('/var/SP/data/home/asaezco/src/devel/Graphs/' + seg[1] + '_imp_' + str(closing_day) + '.png')
        plt.figure()
        
        
    print'Total number of customers: {}'.format(full.count())
    print("############ Finished Process ############")