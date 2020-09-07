
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
                                   count as sql_count,
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


# Test de Carmen 
def getOrderedRelevantFeats(model, featCols, pca="f"):
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

def filter_population(spark, df, filter_ = 'none'):

    def get_filter(x):
        return {
            'only_comps_nocancel': (col('sum_count_vdf')==0) & (col('sum_count_comps')>0) & (col('CHURN_CANCELLATIONS_w8')==0),
            'only_comps': (col('sum_count_vdf') == 0) & (col('sum_count_comps') > 0),
            'comps': (col('sum_count_comps') > 0),
            'only_vdf': (col('sum_count_vdf') > 0) & (col('sum_count_comps') == 0),
            'only_vdf_nocancel': (col('sum_count_vdf') > 0) & (col('sum_count_comps') == 0) & (col('CHURN_CANCELLATIONS_w8') == 0),
            'none': (col('sum_count_vdf') >= 0)
        }[x]


    condition_ = get_filter(filter_)

    filter_df = df.filter(condition_)

    print "[Info filter_population] Population filter: " + str(filter_) + " - Size of the output DF: " + str(filter_df.count())

    return filter_df

def evaluate_navcomp_model_msisdn(spark, tr_date_, tr_set, tt_date_, tt_set, filter_, model_, metadata_sources):

    ##########################################################################################
    # 2. Loading tr data
    # ##########################################################################################

    tr_set = filter_population(spark, tr_set, filter_)

    tr_set.groupBy('label').agg(count('*').alias('num_samples')).show()

    print "[Info evaluate_navcomp_model] After filter - Label count on tr set showed above"

    ##########################################################################################
    # 3. Loading tt data
    # ##########################################################################################

    tt_set = filter_population(spark, tt_set, filter_)

    tt_set.groupBy('label').agg(count('*').alias('num_samples')).show()

    print "[Info evaluate_navcomp_model] After filter - Label count on tt set showed above"

    ##########################################################################################
    # 4. Modeling
    ##########################################################################################

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    metadata = get_metadata(spark, sources=metadata_sources)

    categorical_columns = metadata.filter(col('type') == 'categorical').rdd.map(lambda x: x['feature']).collect()
    stages = []

    for categorical_col in categorical_columns:
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index', handleInvalid='keep')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    numeric_columns = metadata.filter(col('type') == 'numeric').rdd.map(lambda x: x['feature']).collect()
    assembler_inputs = [c + "_class_vec" for c in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    for cfeat in categorical_columns:
        print "[Info evaluate_navcomp_model] Categorical column: " + cfeat

    for nfeat in numeric_columns:
        print "[Info evaluate_navcomp_model] Numeric column: " + nfeat

    def get_model(x):
        return {
            'rf': RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol="label", seed=1234,
                                         maxBins=32, minInstancesPerNode=100, impurity='gini',
                                         featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'gbt': GBTClassifier(featuresCol='features', labelCol='label', maxDepth=5, maxBins=32,
                                 minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100,
                                 stepSize=0.1, seed=None, subsamplingRate=0.7),
        }[x]

    dt = get_model(model_)
    stages += [dt]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_set)

    feat_importance_list = getOrderedRelevantFeats(pipeline_model, assembler_inputs, pca="f")

    for fimp in feat_importance_list:
        print "[Info evaluate_navcomp_model] Feat imp - " + fimp[0] + ": " + str(fimp[1])

    ###########################################################################################
    # 5. Evaluation
    ###########################################################################################

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    tr_predictions = pipeline_model \
        .transform(tr_set) \
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType())) \
        .withColumn('model_score', col('model_score') + lit(0.00001) * randn())

    predictions = pipeline_model \
        .transform(tt_set) \
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType())) \
        .withColumn('model_score', col('model_score') + lit(0.00001) * randn())

    path_to_save = "/data/udf/vf_es/churn/triggers/nav_comp_tests_all_labels/"
    start_time = time.time()
    time_part = int(time.time())
    print("Started saving - {} for closing_day={} and no filter by label - time_part = {}".format(path_to_save, tt_date_, time_part))
    predictions = predictions.withColumn("day", lit(int(tt_date_[6:])))
    predictions = predictions.withColumn("month", lit(int(tt_date_[4:6])))
    predictions = predictions.withColumn("year", lit(int(tt_date_[:4])))
    predictions = predictions.withColumn("time", lit(time_part))

    #(predictions.where(col("label")==1).repartition(100).write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))
    (predictions.repartition(100).write.partitionBy('year', 'month', 'day', 'time').mode("append").format("parquet").save(path_to_save))

    print("Elapsed time saving {} minutes".format((time.time()-start_time)/60.0))



    evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='label',
                                              metricName='areaUnderROC')

    tr_auc = evaluator.evaluate(tr_predictions)

    auc = evaluator.evaluate(predictions)

    result_print = "[Info evaluate_navcomp_model] Navcomp model - Filter = " + filter_ +  " - tr_date = " + str(tr_date_) + " - tt_date = " + str(tt_date_) + " - AUC(tr) = " + str(tr_auc) + " - AUC(tt) = " + str(auc)

    print result_print

    # Approx., each segment with 5000 samples

    from churn_nrt.src.projects_utils.models.modeler import get_cumulative_churn_rate_fix_step
    cum_churn_rate = get_cumulative_churn_rate_fix_step(spark, predictions, ord_col ='model_score', label_col ='label')

    # Lift curve wrt the base

    from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests, get_customer_base

    base_df = get_customer_base(spark, tt_date_).filter(col('rgu') == 'mobile').select('msisdn')

    from pykhaos.utils.date_functions import move_date_n_days

    end_port = move_date_n_days(tt_date_, 15)

    base_df = base_df\
        .join(get_mobile_portout_requests(spark, tt_date_, end_port).select('msisdn', 'label_mob'), ['msisdn'], 'left').na.fill(0.0)

    print '[Info evaluate_navcomp_model] Labeled base for ' + str(tt_date_) + " - Size: " + str(base_df.count()) + " - Num distinct msisdn: " + str(base_df.select('msisdn').distinct().count())

    tt_churn_ref = base_df.select(sql_avg('label_mob').alias('churn_ref')).rdd.first()['churn_ref']

    print '[Info evaluate_navcomp_model] Churn of the base for ' + str(tt_date_) + ": " + str(tt_churn_ref)

    print '[Info evaluate_navcomp_model] Churn rate'
    cum_churn_rate.orderBy(desc('bucket')).show(50, False)

    return auc

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

    start_time_process = time.time()


    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    ##########################################################################################

    tr_date_ = sys.argv[1]

    tt_date_ = sys.argv[2]

    #filter_ = sys.argv[3]

    model_ = sys.argv[3]

    from churn.analysis.triggers.navcomp.metadata import METADATA_STANDARD_MODULES
    metadata_sources = sys.argv[4].split(",") if len(sys.argv)>4 else METADATA_STANDARD_MODULES

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    from churn.analysis.triggers.navcomp.metadata import get_metadata
    get_metadata(spark, sources=metadata_sources).show()

    # Parsing year, month, day

    tt_year_ = tt_date_[0:4]

    tt_month_ = str(int(tt_date_[4:6]))

    tt_day_ = str(int(tt_date_[6:8]))

    save_ = True
    verbose = True

    ##########################################################################################
    # 2. Loading tr data
    # ##########################################################################################

    from churn.analysis.triggers.navcomp.navcomp_utils import get_labeled_set_msisdn

    from churn.analysis.triggers.base_utils.base_utils import get_active_filter, get_disconnection_process_filter, get_churn_call_filter

    # Loading the initial set of customers who navigate through competitors websites

    initial_tr_set_df = get_labeled_set_msisdn(spark, tr_date_, sources=metadata_sources, save_=save_, verbose=verbose)

    # Modeling filters

    tr_active_filter = get_active_filter(spark, tr_date_, 90)

    tr_disconnection_filter = get_disconnection_process_filter(spark, tr_date_, 90)

    tr_churn_call_filter = get_churn_call_filter(spark, tr_date_, 90, 'msisdn')

    tr_set = initial_tr_set_df \
        .join(tr_active_filter, ['msisdn'], 'inner') \
        .join(tr_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tr_churn_call_filter, ['msisdn'], 'inner')

    tr_set.groupBy('label').agg(count('*').alias('nb_services')).show()

    print '[Info navcomp_model] Training set before model filter showed above'

    #from churn.analysis.triggers.navcomp.navcomp_utils import get_labeled_set_msisdn
    #from churn.analysis.triggers.base_utils.base_utils import get_active_filter

    #tr_set = get_labeled_set_msisdn(spark, tr_date_)
    #tr_active_base = get_active_filter(spark, tr_date_, 90)
    #tr_set = tr_set.join(tr_active_base, ['msisdn'], 'inner')


    ##########################################################################################
    # 3. Loading tt data
    # ##########################################################################################

    initial_tt_set_df = get_labeled_set_msisdn(spark, tt_date_, sources=metadata_sources, save_=save_, verbose=verbose)

    # Modeling filters

    tt_active_filter = get_active_filter(spark, tt_date_, 90)

    tt_disconnection_filter = get_disconnection_process_filter(spark, tt_date_, 90)

    tt_churn_call_filter = get_churn_call_filter(spark, tt_date_, 90, 'msisdn')

    tt_set = initial_tt_set_df \
        .join(tt_active_filter, ['msisdn'], 'inner') \
        .join(tt_disconnection_filter, ['nif_cliente'], 'inner') \
        .join(tt_churn_call_filter, ['msisdn'], 'inner')

    tt_set.groupBy('label').agg(count('*').alias('nb_services')).show()

    print '[Info navcomp_model] Test set before model filter showed above'

    #tt_set = get_labeled_set_msisdn(spark, tt_date_)
    #tt_active_base = get_active_filter(spark, tt_date_, 90)
    #tt_set = tt_set.join(tt_active_base, ['msisdn'], 'inner')

    #filters = ['only_comps_nocancel', 'only_comps', 'only_vdf', 'only_vdf_nocancel', 'none']
    filters = ['comps']

    for filter_ in filters:
        #tmp = evaluate_navcomp_model(spark, tr_date_, tr_set, tt_date_, tt_set, filter_, model_)
        tmp = evaluate_navcomp_model_msisdn(spark, tr_date_, tr_set, tt_date_, tt_set, filter_, model_, metadata_sources)
        del tmp


    elapsed_time = time.time() - start_time_process
    print("Elapsed time - complete process {} minutes ({} hours)".format(int(elapsed_time/60.0), elapsed_time/3600.0))
