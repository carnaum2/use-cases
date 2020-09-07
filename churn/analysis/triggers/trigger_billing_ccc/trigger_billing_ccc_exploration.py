
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import sys
import time
import math
import re
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
                                   row_number,
                                   regexp_replace,
                                   upper,
                                   trim,
                                   array,
                                   create_map,
                                   randn)
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType

def getOrderedRelevantFeats(model, featCols, pca, classAlg):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances
        return getImportantFeaturesFromVector(featCols, impFeats)

def getImportantFeaturesFromVector(featCols, impFeats):
    feat_and_imp=zip(featCols, impFeats.toArray())
    return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)

def get_deciles(dt, column, nile = 40):
    print "[Info get_deciles] Computing deciles"
    discretizer = QuantileDiscretizer(numBuckets=nile, inputCol=column, outputCol="decile")
    dtdecile = discretizer.fit(dt).transform(dt).withColumn("decile", col("decile") + lit(1.0))
    return dtdecile

def get_lift(dt, score_col, label_col, nile = 40, refprevalence = None):

    print "[Info get_lift] Computing lift"

    if(refprevalence == None):
        refprevalence = dt.select(label_col).rdd.map(lambda r: r[label_col]).mean()

    print "[Info get_lift] Computing lift - Ref Prevalence for class 1: " + str(refprevalence)

    dtdecile = get_deciles(dt, score_col, nile)

    result = dtdecile\
    .groupBy("decile")\
    .agg(sql_avg(label_col).alias("prevalence"))\
    .withColumn("refprevalence", lit(refprevalence))\
    .withColumn("lift", col("prevalence")/col("refprevalence"))\
    .withColumn("decile", col("decile").cast("double"))\
    .select("decile", "lift")\
    .rdd\
    .map(lambda r: (r["decile"], r["lift"]))\
    .collect()

    result_ord = sorted(result, key=lambda tup: tup[0], reverse=True)

    return result_ord

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
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

if __name__ == "__main__":

    #sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    #sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    set_paths()

    # Reading from mini_ids. The following imports would be required for the automated process (computing features on the fly)
    #from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
    #from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_attributes_df

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    tr_date_ = sys.argv[1]

    tt_date_ = sys.argv[2]

    model_ = sys.argv[3]

    # Parsing year, month, day

    tr_year_ = tr_date_[0:4]

    tr_month_ = str(int(tr_date_[4:6]))

    tr_day_ = str(int(tr_date_[6:8]))

    tt_year_ = tt_date_[0:4]

    tt_month_ = str(int(tt_date_[4:6]))

    tt_day_ = str(int(tt_date_[6:8]))

    ##########################################################################################
    # The model is built on the segment of customers with calls related to billing to the call centre during the last month.
    # Thus, additional feats are added to this segment by using left_outer + na.fill
    ##########################################################################################

    ##########################################################################################
    # 2. Loading tr data
    ##########################################################################################

    tr_ccc_all = spark\
        .read\
        .parquet('/data/attributes/vf_es/trigger_analysis/ccc/year=' + tr_year_ + '/month=' + tr_month_ + '/day=' + tr_day_)\

    bill_cols = [c for c in tr_ccc_all.columns if 'billing' in c.lower()]

    tr_base = spark\
        .read\
        .parquet('/data/attributes/vf_es/trigger_analysis/customer_master/year=' + tr_year_ + '/month=' + tr_month_ + '/day=' + tr_day_)\
        .filter(col('segment_nif')!='Pure_prepaid')\
        .withColumn('blindaje', lit('none'))\
        .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') >= 0) & (col('tgs_days_until_fecha_fin_dto') <= 60), 'soft').otherwise(col('blindaje')) )\
        .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') > 60),'hard').otherwise(col('blindaje')))\
        .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label')

    tr_churn_ref = tr_base.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tr_volume_ref = tr_base.count()

    tr_ccc_base = tr_ccc_all.join(tr_base, ['nif_cliente'], 'inner')

    tr_input_df = tr_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0) & (col('BILLING_POSTPAID_w4') > 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label'])

    tr_segment_churn_ref = tr_input_df.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tr_segment_volume = tr_input_df.count()

    print '[Info Tr Set] Volume of the base for ' + tr_date_ + ': ' + str(tr_volume_ref) + ' - Churn rate in the base for ' + tr_date_ + ': ' + str(tr_churn_ref) + ' - Volume of the segment for ' + tr_date_ + ': ' + str(tr_segment_volume) + ' - Churn rate in the segment for ' + tr_date_ + ': ' + str(tr_segment_churn_ref)

    # Adding reimbursement features

    tr_reimb_df = spark.read.parquet('/data/attributes/vf_es/trigger_analysis/reimbursements/year=' + tr_year_ + '/month=' + tr_month_ + '/day=' + tr_day_)

    reimb_map = {'Reimbursement_adjustment_net': 0.0,\
                 'Reimbursement_adjustment_debt': 0.0,\
                 'Reimbursement_num': 0.0,\
                 'Reimbursement_num_n8': 0.0,\
                 'Reimbursement_num_n6': 0.0,\
                 'Reimbursement_days_since': 1000.0,\
                 'Reimbursement_num_n4': 0.0,\
                 'Reimbursement_num_n5': 0.0,\
                 'Reimbursement_num_n2': 0.0,\
                 'Reimbursement_num_n3': 0.0,\
                 'Reimbursement_days_2_solve': -1.0,\
                 'Reimbursement_num_month_2': 0.0,\
                 'Reimbursement_num_n7': 0.0,\
                 'Reimbursement_num_n1': 0.0,\
                 'Reimbursement_num_month_1': 0.0}

    tr_input_df = tr_input_df.join(tr_reimb_df, ['nif_cliente'], 'left_outer').na.fill(reimb_map)

    ##########################################################################################
    # 3. Loading tt data
    ##########################################################################################

    tt_ccc_all = spark \
        .read \
        .parquet(
        '/data/attributes/vf_es/trigger_analysis/ccc/year=' + tt_year_ + '/month=' + tt_month_ + '/day=' + tt_day_) \


    tt_base = spark \
        .read \
        .parquet(
        '/data/attributes/vf_es/trigger_analysis/customer_master/year=' + tt_year_ + '/month=' + tt_month_ + '/day=' + tt_day_) \
        .filter(col('segment_nif') != 'Pure_prepaid') \
        .withColumn('blindaje', lit('none')) \
        .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') >= 0) & (col('tgs_days_until_fecha_fin_dto') <= 60), 'soft').otherwise(col('blindaje'))) \
        .withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') > 60), 'hard').otherwise(col('blindaje'))) \
        .select('nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label')

    tt_churn_ref = tt_base.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tt_volume_ref = tt_base.count()

    tt_ccc_base = tt_ccc_all.join(tt_base, ['nif_cliente'], 'inner')

    tt_input_df = tt_ccc_base.filter((col('CHURN_CANCELLATIONS_w8') == 0) & (col('BILLING_POSTPAID_w4') > 0)).select(bill_cols + ['nif_cliente', 'blindaje', 'nb_rgus', 'nb_tv_services_nif', 'label'])

    tt_segment_churn_ref = tt_input_df.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']

    tt_segment_volume = tt_input_df.count()

    print '[Info Tt Set] Volume of the base for ' + tt_date_ + ': ' + str(tt_volume_ref) + ' - Churn rate in the base for ' + tt_date_ + ': ' + str(tt_churn_ref) + ' - Volume of the segment for ' + tt_date_ + ': ' + str(tt_segment_volume) + ' - Churn rate in the segment for ' + tt_date_ + ': ' + str(tt_segment_churn_ref)

    # Adding reimbursement features

    tt_reimb_df = spark.read.parquet('/data/attributes/vf_es/trigger_analysis/reimbursements/year=' + tt_year_ + '/month=' + tt_month_ + '/day=' + tt_day_)

    tt_input_df = tt_input_df.join(tt_reimb_df, ['nif_cliente'], 'left_outer').na.fill(reimb_map)

    ##########################################################################################
    # 4. Modeling
    ##########################################################################################

    categorical_columns = ['blindaje']
    stages = []

    for categorical_col in categorical_columns:
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    numeric_columns = bill_cols + ['nb_rgus', 'nb_tv_services_nif'] + reimb_map.keys()
    assembler_inputs = [c + "_class_vec" for c in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    def get_model(x):
        return {
            'rf': RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol="label", seed=1234, maxBins=32, minInstancesPerNode=20, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'gbt': GBTClassifier(featuresCol='features', labelCol='label', maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
        }[x]

    dt = get_model(model_)
    stages += [dt]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_input_df)

    ##########################################################################################
    # 5. Evaluation
    ##########################################################################################

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    predictions = pipeline_model\
        .transform(tt_input_df)\
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType()))\
        .withColumn('model_score', col('model_score') + lit(0.000001)*randn())

    evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='label', metricName='areaUnderROC')

    auc = evaluator.evaluate(predictions)

    print 'AUC = ' + str(auc)

    # Approx., each segment with 2500 samples

    num_nile = int(math.floor(tt_segment_volume/2500))

    lift = get_lift(predictions, 'model_score', 'label', num_nile, tt_churn_ref)

    for d, l in lift:
        print 'Lift curve - ' + str(d) + ": " + str(l)


    '''
    print(dt_model.toDebugString)

    explanation = dt_model.toDebugString.splitlines()
    for i in range(0, len(assembler_inputs)):
        for ii_ll in range(0, len(explanation)):
            explanation[ii_ll] = explanation[ii_ll].replace("feature {}".format(i), "'{}'".format(assembler_inputs[i]))
            #         explanation[ii_ll] = re.sub("Predict: 0.0", "Predict: NO_CHURN", explanation[ii_ll])
            explanation[ii_ll] = re.sub("Predict: 1.0", "Predict: ***********CHURN", explanation[ii_ll])
    explanation
    '''





