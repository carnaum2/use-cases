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

#Loadd table roots

TABLE_TRIGGER = '/data/udf/vf_es/churn/churn_reasons/trigger_reasons_results'

if __name__ == "__main__":

    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
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
    
    closing_day = str(args.closing_d)
    
    df_reasons = spark.read.load(TABLE_TRIGGER)
    df_fil_reasons = df_reasons.where(col('closing_day')== closing_day)
        
    df_alv_reasons = df_fil_reasons.where(col('model') == 'asaezco')
    df_cris_reasons = df_fil_reasons.where(col('model') == 'csanc109')   
    print'Loaded Reasons Dataframes'
    
    path_combined = '/data/udf/vf_es/churn/triggers/model_combined_50k/year={}/month={}/day={}'.format(int(closing_day[:4]),int(closing_day[4:6]),int(closing_day[6:8]))
    df_car = spark.read.load(path_combined)    
    print("############ Finished loading Datasets ############")
    
    selcols = ['NIF_CLIENTE',
 'top0_reason',
 'top1_reason',
 'top2_reason',
 'top3_reason',
<<<<<<< HEAD
 'top4_reason','top5_reason', 'Incertidumbre']
=======
 'top4_reason', 'Incertidumbre']
>>>>>>> master
    
    df_full_alv = df_car.where(col('model')== 'asaezco').join(df_alv_reasons.select(selcols) ,['NIF_CLIENTE'], 'inner')
    df_full_cris = df_car.where(col('model')== 'csanc109').join(df_cris_reasons.select(selcols) ,['NIF_CLIENTE'], 'inner')
    
    df_total = df_full_cris.union(df_full_alv)

    for ii in range(0, 5):
        df_total = df_total.withColumn("top{}_reason".format(ii),
                                                       regexp_extract(col("top{}_reason".format(ii)), "^\[(.*)\]$", 1))
    
    df_save = df_total.select('NIF_CLIENTE', 'top0_reason','top1_reason','top2_reason','top3_reason','top4_reason', 'Incertidumbre')
    
    #Save results
    save_dir = '/data/udf/vf_es/churn/triggers/trigger_reasons/year={}/month={}/day={}'.format(int(closing_day[:4]),int(closing_day[4:6]),int(closing_day[6:8]))
    df_save.repartition(300).write.save(save_dir, format='parquet', mode='append')
    
    
    print("Table saved as")
    print(save_dir)
    print("############ Finished Process ############")