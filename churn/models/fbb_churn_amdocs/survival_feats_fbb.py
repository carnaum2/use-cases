import sys
import imp
from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
from pyspark.sql.functions import (udf,
                                    col,
                                    decode,
                                    when,
                                    lit,
                                    lower,
                                    concat,
                                    translate,
                                    count,
                                    sum as sql_sum,
                                    max as sql_max,
                                    min as sql_min,
                                    avg as sql_avg,
                                    greatest,
                                    least,
                                    isnull,
                                    isnan,
                                    struct,
                                    substring,
                                    size,
                                    length,
                                    year,
                                    month,
                                    dayofmonth,
                                    unix_timestamp,
                                    date_format,
                                    from_unixtime,
                                    datediff,
                                    to_date,
                                    desc,
                                    asc,
                                    countDistinct,
                                    row_number)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, LongType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from datetime import datetime
from itertools import chain
from survival_funct import *
import numpy as np
from functools import reduce
from utils_general import *
from utils_model import *
from metadata_fbb_churn import *
from feature_selection_utils import *
import subprocess

def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    return udf(to_array_, ArrayType(DoubleType()))(col)

def set_paths():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and "dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    USE_CASES = "/var/SP/data/home/asaezco/src/devel2/use-cases"#dirname(os.path.abspath(imp.find_module('churn')[1]))

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))

if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-y', '--cycles', metavar = '<cycles>', type= int, help= 'number of cycles', default = 4)
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= str, help= 'closing day', required = True)
    parser.add_argument('-c', '--config_f', metavar = '<config_f>', type= str, help= 'configuration file', default = "model_under_eval.yaml")
    parser.add_argument('-s', '--training_day', metavar='<TRAINING_DAY>', type=str, required=True, help='Training day YYYYMMDD. Date of the CAR taken to train the model.')
    
    args = parser.parse_args()
    
    set_paths()
    
    from churn.datapreparation.general.data_loader import get_port_requests_table
    from pykhaos.utils.date_functions import *
    from utils_fbb_churn import *

    import pykhaos.utils.pyspark_configuration as pyspark_config
    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles
    from churn.config_manager.config_mgr import Config
    
    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="survival_analysis", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
   
    config_obj = Config(args.config_f, internal_config_filename=os.path.join(imp.find_module('churn')[1], "config_manager", "config", "internal_config_churn.yaml"))
    print("############ Config Object Created ############")
    
    ######################## TRAINING DF ########################
    
    selcols = getIdFeats() + getCrmFeats() + getBillingFeats() + getMobSopoFeats() + getOrdersFeats()
    now = datetime.now()
    date_name = str(now.year) + str(now.month).rjust(2, '0') + str(now.day).rjust(2, '0')
    origin = '/user/hive/warehouse/tests_es.db/jvmm_amdocs_ids_'
    
    
    trcycle_ini = args.training_day
    horizon = args.cycles #4
    # Cycle used for CAR and Extra Feats in the test set
    ttcycle_ini = args.closing_d#'20181231'  # Test data
    
    inittrdf_ini = getFbbChurnLabeledCarCycles(spark, origin, trcycle_ini, selcols, horizon)

    dfExtraFeat = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                         .format(int(trcycle_ini[0:4]), int(trcycle_ini[4:6]), int(trcycle_ini[6:8])))

        # Taking only the clients with a fbb service
    dfExtraFeatfbb = dfExtraFeat.join(inittrdf_ini, ["num_cliente"], "leftsemi")

    dfExtraFeatfbb = dfExtraFeatfbb.cache()
    print "[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats: ", dfExtraFeatfbb.count()

    # Taking the Extra Features of interest and adding their values for num_client when necessary
    dfExtraFeatSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb)

    print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeatSel.columns:
            dfillNa.pop(kkey, None)

    inittrdf = inittrdf_ini.join(dfExtraFeatSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
    print("############ Finished loading Training Dataset ############")
    
    
    ttdf_ini = getFbbChurnLabeledCarCycles(spark, origin, ttcycle_ini, selcols,horizon)

    print "[Info Main FbbChurn] " + time.ctime() + " Saving ttdf_ini to HDFS "

    dfExtraFeat_tt = spark.read.parquet('/data/udf/vf_es/churn/extra_feats_mod/extra_feats/year={}/month={}/day={}'
                                            .format(int(ttcycle_ini[0:4]), int(ttcycle_ini[4:6]), int(ttcycle_ini[6:8])))

    dfExtraFeatfbb_tt = dfExtraFeat_tt.join(ttdf_ini.select('num_cliente'), on='num_cliente', how='leftsemi')
    print(dfExtraFeatfbb_tt.select('num_cliente').distinct().count(), ttdf_ini.select('num_cliente').distinct().count())

    dfExtraFeatfbb_tt = dfExtraFeatfbb_tt.cache()
    print("[Info Main FbbChurn] " + time.ctime() + " Count of the ExtraFeats ", dfExtraFeatfbb_tt.count())

    dfExtraFeat_ttSel, selColumnas = addExtraFeatsEvol(dfExtraFeatfbb_tt)

    print "[Info Main FbbChurn] " + time.ctime() + " Calculating the total value of the extra feats for each number client in tt"

    dfillNa = fillNa(spark)
    for kkey in dfillNa.keys():
        if kkey not in dfExtraFeat_ttSel.columns:
             dfillNa.pop(kkey, None)

    ttdf = ttdf_ini.join(dfExtraFeat_ttSel, ["msisdn", "num_cliente", 'rgu'], how="left").na.fill(dfillNa)
    print "[Info Main FbbChurn] " + time.ctime() + " Number of clients after joining the Extra Feats to the test set " + str(ttdf.count())
    print("############ Finished loading Test Dataset ############")
    
    closing_day_f = str(args.closing_d)
    key = "censor"
    label = 'label'
    from datetime import datetime as dt
    now = dt.now().strftime("%Y%m%d")

    quantileProbabilities = [0.5, 0.75, 0.8, 0.9, 0.95, 0.99]
    
    cycles_horizon = args.cycles
    closing_day = str(args.closing_d)
    discarded_cycles = 0
    start_date = args.training_day
    start_date = move_date_n_cycles(start_date, n=discarded_cycles) if discarded_cycles > 0 else move_date_n_days(start_date, n=1)
    end_date = move_date_n_cycles(start_date, n=cycles_horizon)
    print('End Date')
    print(end_date)
    df_sol_por_discard = get_port_requests_table(spark, config_obj, start_date, end_date, closing_day, select_cols=['msisdn_a','portout_date', 'label'])
    
    df_ids = getIds(spark, closing_day_f)
    
    portas = df_sol_por_discard.select('msisdn_a','portout_date').withColumnRenamed('msisdn_a','msisdn')
    
    labeled = inittrdf.join(portas, ['msisdn'], how="left")
    
    labeled2 = labeled.withColumn('endDate', labeled['portout_date'].cast(DateType()))
    
    start_ = 'fx_fbb_fx_first'
    
    labeled3 = labeled2.join( df_ids.select('msisdn', start_ +'_nif', start_ +'_nc'), ['msisdn'], 'inner')
    
    from pyspark.sql.functions import least, col
    labeled4 = labeled3.withColumn('startTime', least(labeled3[start_ +'_nif'],labeled3[start_ +'_nc']))
    prepared = labeled4.withColumn('startDate', labeled4['startTime'].cast(DateType()))
    
    from pyspark.sql import functions as F
    timeFmt = "yyyy-MM-dd"
    timeDiff = (F.unix_timestamp('endDate', format=timeFmt) - F.unix_timestamp('startDate', format=timeFmt))
    final_df = prepared.withColumn("Duration", timeDiff)

    final_censor = final_df.withColumnRenamed('label', 'censor')
    timeDiff2 = (F.unix_timestamp('closing_day', format=timeFmt) - F.unix_timestamp('startTime', format=timeFmt))
    import datetime as datetime
    closing_date=datetime.date(int(closing_day_f[:4]),int(closing_day_f[4:6]),int(closing_day_f[6:8]))
    
    final = final_censor.withColumn('closing_day', lit(closing_date))
    
    sec_2_day = 3600*24
    
    final_2 = final.withColumn('label', when(final[key] == 1, final_censor['Duration']/(sec_2_day)).otherwise(timeDiff2/(sec_2_day)))
    
    final_d = final_2
    (Train_, Test_) = final_d.randomSplit([0.8, 0.2])

    # Getting only the numeric variables
    allFeats = inittrdf.columns
    catCols = [item[0] for item in inittrdf.dtypes if item[1].startswith('string')]
    numerical_feats = list(set(allFeats) - set(list(
        set().union(getIdFeats(), getIdFeats_tr(), getNoInputFeats(), catCols, [c + "_enc" for c in getCatFeatsCrm()],
                    ["label"]))))
    
    trdf = balance_df2(Train_.where(col(label).isNotNull()).filter(Train_[label] > 0), 'censor')
    print("############ Balanced Dataset ############")
    
    noninf_feats = getNonInfFeats(trdf, numerical_feats)
    featCols = list(set(numerical_feats) - set(noninf_feats))
    
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler, PCA
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import AFTSurvivalRegression
    
    aft = AFTSurvivalRegression(quantilesCol="quantiles", maxIter = 100000, fitIntercept=True, tol=1e-10, aggregationDepth=12)
    print("############ Training Stage ############")  
    assembler = VectorAssembler(inputCols=featCols, outputCol="features")
    
    pipeline = Pipeline(stages=[assembler, aft])
    
    model = pipeline.fit(trdf)
    print("############ Assembling Stage ############")
    print("############ Prediction Stage ############")
    
    trans = model.transform(trdf)
    trans_test = model.transform(Test_)
    predicted = trans.select('msisdn','censor','label','features', 'prediction')
    trans_t = model.transform(ttdf)
    complete = trans_t.select('msisdn','censor','label','features', 'prediction')
    predicted_t = trans_test.select('msisdn','censor','label','features', 'prediction')
    

    '''
    survival = aft.fit(selected)
    predicted = survival.transform(selected)
    predicted_t = survival.transform(selected_test)
    complete = survival.transform(selected_t)
    '''
    
    from pyspark.ml.evaluation import RegressionEvaluator,BinaryClassificationEvaluator
    evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction_2', labelCol='label_2', metricName='areaUnderROC')
    
    predicted_car_f = complete
    save_cols = []
    
    for n in np.arange(1,7, 0.5):
        
            m = int(n*12)
            name_ = 'over_' + str(m) + '_months' + suf
            th_2 = 365*(n)
            prepared_car_f = prepared_car_f.withColumn(name_, when(prepared_car_f['prediction'] > th_2,0.0).otherwise(1.0))
            save_cols.append(name_)

    prepared_car_f = prepared_car_f.withColumn('closing_day', lit(closing_date_f)).withColumn('Expected_lifetime', prepared_car_f['prediction'] - timeDiff2/(3600*24)).na.fill({'Expected_lifetime': -1.0})
    
    
    df_final_ = prepared_car_f.withColumn("transformed", to_array(col("quantiles")))
    
    df_final = df_final_
    
    i = 0
    for n in quantileProbabilities:
        df_final = df_final.withColumn('Lifetime_quantile_' + str(int(100*n)), col('transformed')[i])
        i = i + 1
        save_cols.append('Lifetime_quantile_' + str(int(100*n)))
    
    save_cols.append('Expected_lifetime')
    save_cols.append('prediction')    
    
    save_dir_2 = 'tests_es.churn_team_survival_fbb_extra_' + str(args.closing_d)
    df_final.select(['msisdn']+save_cols).write.format('parquet').mode('overwrite').saveAsTable(save_dir_2)

    print("############ Saved Survival extra feats as: ############")
    print(save_dir_2)    
    
    for n in range(1,7):
        th_2 = 365*(n+0.5)
        prepared = predicted.withColumn('prediction_2', when(predicted['prediction'] > th_2,0.0).otherwise(1.0)).withColumn('label_2', when(predicted['label'] > th_2,0.0).otherwise(1.0))
        auc = evaluator.evaluate(prepared)
        print("Number of Years: ")
        print(n+0.5)
        print("AUC (Train): ")
        print(auc)
    
    prepared_t = predicted_t
    
    for n in range(1,7):
        th_2 = 365*(n)
        name_ = 'over_' + str(n) + '_years'
        evaluator = BinaryClassificationEvaluator(rawPredictionCol=name_, labelCol='label_2', metricName='areaUnderROC')
        prepared_t = prepared_t.withColumn(name_, when(prepared_t['prediction_f'] > th_2,0.0).otherwise(1.0)).withColumn('label_2', when(prepared_t['label'] > th_2,0.0).otherwise(1.0))
        auc = evaluator.evaluate(prepared_t)
        print("Number of Years: ")
        print(n)
        print("AUC (Test): ")
        print(auc)
    
    print("############ Finished Process ############")