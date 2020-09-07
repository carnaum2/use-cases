from pyspark.sql.functions import count as sql_count, col, udf, lit, randn, max as sql_max, min as sql_min, desc, lit, avg as sql_avg, sum as sql_sum
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import IsotonicRegression
import numpy as np
from pyspark.ml.feature import QuantileDiscretizer
import time

from pyspark.sql import Window
from functools import reduce

def balance_df(df=None, label=None):
    """
    Balance classes of the given DataFrame

    :param df: A DataFrame to balance classes in label column
    :type df: :class:`pyspark.sql.DataFrame`
    :param label: The label column to balance
    :type label: str

    :return: The given DataFrame balanced
    :rtype: :class:`pyspark.sql.DataFrame`
    """

    counts_dict = df.groupBy(label).agg(sql_count('*')).rdd.collectAsMap()

    minor_cat_size = min(counts_dict.values())

    # Calculate the rate to apply to classes
    fractions_dict = {}
    for l in list(counts_dict.keys()):
        fractions_dict[l] = float(minor_cat_size) / counts_dict[l]

    balanced = reduce((lambda x, y: x.union(y)), [df.filter(col(label) == k).sample(False, fractions_dict[k]) for k in list(counts_dict.keys())])

    repartdf = balanced.repartition(400)

    print("[Info balance_df2] Dataframe has been balanced - Total number of rows is " + str(repartdf.count()))

    # balanced = df.sampleBy(label, fractions=fractions_dict, seed=1234)

    return repartdf

def getOrderedRelevantFeats(model, featCols, pca):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances

        feat_and_imp = list(zip(featCols, impFeats.toArray()))
        return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)

def get_split(df_tar, train_split_ratio=0.7):


    [df_unbaltr, df_tt] = df_tar.randomSplit([train_split_ratio, 1-train_split_ratio], 1234)

    #print "[Info FbbChurn] " + time.ctime() + " Total number of training samples is : " + str(unbaltrdf.count())

    df_unbaltr.describe('label').show()

    print(" Stat description of the target variable printed above")

    # 1.2. Balanced df for training

    df_unbaltr.groupBy('label').agg(sql_count('*')).show()

    print(" Count on label column for unbalanced tr set showed above")

    df_tr = balance_df(df_unbaltr, 'label')

    df_tr.groupBy('label').agg(sql_count('*')).show()

    print(" Count on label column for balanced tr set showed above")

    return df_unbaltr, df_tr, df_tt



def encoding_categoricals(categorical_columns, stages_=None):

    stages = [] if stages_ == None else stages_

    for categorical_col in categorical_columns:
        print(("StringIndexer+OneHotEncoderEstimator for variable {}".format(categorical_col)))
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    new_categorical_inputs = [c + "_class_vec" for c in categorical_columns]

    return stages, new_categorical_inputs


def get_model(model_name, label_col="label", featuresCol="features"):
    return {
        'rf': RandomForestClassifier(featuresCol=featuresCol, numTrees=500, maxDepth=10, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=20, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
        'gbt': GBTClassifier(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
    }[model_name]


def smart_fit(model_name, df_tr, feats=None, non_info_cols=None, categorical_cols=None, label_col="label"):
    '''
    :param model_name: rf or gbt to train the model
    :param df_tr: training dataframe
    :param feats: list of columns to used for training. If not specified, then all columns in df_tr are used, except label_col
    :param non_info_cols:
    :param categorical_cols. If categorical_cols=None, it detects them automatically, extract the categoricals as the columns with type=string
                             If categorical_cols=[] it ignores the categoricals
                             if categoricals_cols is present, encode this list of cols
    :return:
    '''

    if not non_info_cols:
        non_info_cols = []

    if not feats:
        feats = list(set(df_tr.columns) - {label_col})

    if categorical_cols is None:
        print("[modeler] smart_fit | detecting automatically categorical columns")
        categorical_cols = [col_ for col_, type_ in df_tr.dtypes if type_=="string" and col_ not in non_info_cols]
        print(("[modeler] smart_fit | Detected {} categoricals: {}".format(len(categorical_cols), ",".join(categorical_cols))))

    elif len(categorical_cols) == 0:
        print("[modeler] smart_fit | Discarding categoricals since user input categorical_cols=[]")
        detected_categorical_cols = [col_ for col_, type_ in df_tr.dtypes if type_=="string" and col_ not in non_info_cols]
        # Remove categorical from feats
        feats = list(set(df_tr.columns) - {label_col} - set(detected_categorical_cols))

    stages = []
    new_categorical_inputs = []

    if categorical_cols:
        print(("[modeler] smart_fit | about to encode categoricals: {}".format(len(categorical_cols), ",".join(categorical_cols))))
        stages, new_categorical_inputs = encoding_categoricals(categorical_cols, stages_=[])

    numeric_columns = list(set(feats) - set(categorical_cols) - set(non_info_cols) - {label_col})
    assembler_inputs = new_categorical_inputs + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]
    print((len(assembler_inputs)))
    dt = get_model(model_name, label_col=label_col, featuresCol="features")
    stages += [dt]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(df_tr)

    return pipeline_model, assembler_inputs



def fit(df_tr, feat_cols, maxDepth=25, maxBins=32, minInstancesPerNode=50,
        impurity="gini", featureSubsetStrategy="sqrt",
        subsamplingRate=0.7, numTrees=250,seed=1234):
    '''
    Use model.stages[-1].extractParamMap() (where model is the returned model) to extract the parameters used in the RF
    :param df_tr:
    :param feat_cols:
    :param featureSubsetStrategy The number of features to consider for splits at each tree node. Supported options: auto, all, onethird, sqrt,  log2,  (0.0-1.0],  [1-n].): sqrt
    :param minInfoGain Minimum information gain for a split to be considered at a tree node.): 0.0
    :param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.): 256
    :param maxDepth depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.): 25
    :param minInstancesPerNode Minimum number of instances each child must have after split.  If a split causes the left or right child to have fewer than minInstancesPerNode,  the split will be discarded as invalid. Should be >= 1.): 50
    :param maxBins Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature.): 32
    :param labelCol label column name): label
    :param featuresCol features column name): features
    :param predictionCol prediction column name): prediction
    :param numTrees Number of trees to train (>= 1)): 50
    :param impurity Criterion used for information gain calculation (case-insensitive). Supported options: entropy,  gini): gini
    :param probabilityCol Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences,  not precise probabilities): probability
    :param rawPredictionCol raw prediction (a.k.a. confidence) column name): rawPrediction
    :param seed random seed): 1234
    :param subsamplingRate Fraction of the training data used for learning each decision tree,  in range (0,  1].): 0.7
    :param cacheNodeIds If false,  the algorithm will pass trees to executors to match instances with nodes. If true,  the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees.): False
    :param checkpointInterval set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext): 10}
    :return:
    '''
    #print("Feat cols = {}".format(",".join(feat_cols)))

    assembler = VectorAssembler(inputCols = feat_cols, outputCol = "features")

    classifier = RandomForestClassifier(featuresCol="features",
                                        labelCol="label",
                                        maxDepth=maxDepth,
                                        maxBins=maxBins,
                                        minInstancesPerNode=minInstancesPerNode,
                                        impurity=impurity,
                                        featureSubsetStrategy=featureSubsetStrategy,
                                        subsamplingRate=subsamplingRate,
                                        numTrees=numTrees,
                                        seed = seed)

    pipeline = Pipeline(stages= [assembler, classifier])

    model = pipeline.fit(df_tr)

    return model


def get_feats_imp(model, feat_cols, top=None):
    print("[modeler] get_feats_imp - Started")

    feat_importance = getOrderedRelevantFeats(model, feat_cols, 'f')

    ii = 1
    for fimp in feat_importance:
        print("[{:>3}] {} : {}".format(ii, fimp[0], fimp[1]))
        if not top or (top != None and ii < top):
            ii = ii + 1
        else:
            break


    return feat_importance


def get_score(model, df, calib_model=None, add_randn=True, score_col="model_score"):
    '''

    :param model: model trained
    :param df: training dataframe
    :param add_randn: True if a random number should be added to the score
    :param calib_model: calibrated model if any
    :param score_col: output col in case calib_model=None. If calib_model!=None, then output col is calib_model.getPredictionCol()
    :return: a dataframe with the same columns as df and additionally score_col.  if calib_mode !=0, calib_model[2].getPredictionCol() column is added
    '''

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    # Train evaluation
    print(("[modeler] get_score | Setting score in column '{}'".format(score_col)))
    df_preds = model.transform(df).withColumn(score_col, getScore(col("probability")).cast(DoubleType()))
    print(("[modeler] get_score | after transform columns '{}'".format(",".join(df_preds.columns))))

    if calib_model:
        print("[modeler] get_score | calibrating scores...")
        print((calib_model[2].extractParamMap()))
        print((calib_model[2].getFeaturesCol()))
        print((calib_model[2].getPredictionCol()))
        print(("[modeler] get_score | Applying calib_model. features_col = '{}' || prediction_col = '{}'".format(calib_model[2].getFeaturesCol(),
                                                                                          calib_model[2].getPredictionCol())))
        score_col = calib_model[2].getPredictionCol() # IsotonicRegression
        df_preds  = calib_model[0].transform(df_preds) # IsotonicRegressionModel
        #After calibrating, the calibrated score is stored in column calib_model.getPredictionCol()
        print(("[modeler] get_score | Calibrated score is stored in column {}".format(score_col)))
        print(("[modeler] get_score | after calib columns {}".format(",".join(df_preds.columns))))
    else:
        print("[modeler] get_score | Scores wont be calibrated")

    if add_randn:
        print("[modeler] get_score | Adding random number to scores")
        df_preds = df_preds.withColumn(score_col, col(score_col) + lit(0.00001)*randn())

    return df_preds


def get_metrics(df_preds, title="", do_churn_rate_fix_step=True, score_col="model_score", label_col="label"):
    '''

    :param df_preds:
    :param title: string just for showing results
    :param nb_deciles:
    :param score_col
    :param label_col:
    :param refprevalence:
    :return:
    '''

    print(("[modeler] get_metriics do_churn_rate_fix_step={}, score_col={}, label_col={}".format(do_churn_rate_fix_step, score_col, label_col)))

    preds_and_labels = df_preds.select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))

    my_metrics = BinaryClassificationMetrics(preds_and_labels)

    auc = my_metrics.areaUnderROC

    print(("[modeler] get_metrics METRICS FOR {}".format(title)))
    print(("[modeler] get_metrics \t AUC = {}".format(auc)))

    if do_churn_rate_fix_step:

        cum_churn_rate = get_cumulative_churn_rate_fix_step(df_preds, ord_col=score_col, label_col=label_col)

        print('[modeler] get_metrics  Churn rate')
        cum_churn_rate.orderBy(desc('bucket')).show(50, False)

        return auc, cum_churn_rate

    return auc, None



def get_calibration_function2(spark, model, valdf, labelcol, numpoints = 10):
    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    valpredsdf = model.transform(valdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType())).select(['model_score', labelcol])
    minprob = valpredsdf.select(sql_min('model_score').alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max('model_score').alias('max')).rdd.collect()[0]['max']
    numlabels = float(valpredsdf.filter(col(labelcol) == 1.0).count())
    print("[modeler] get_calibration_function2 | Training - Num samples in the target class " + str(numlabels))
    delta = float(maxprob - minprob)/float(numpoints)
    ths = list([(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))])
    samplepoints = [(float(i), float(valpredsdf.filter((col('model_score') <= i) & (col(labelcol) == 1.0)).count())/numlabels) for i in ths]
    # samplepoints = [(float(i), valpredsdf.filter(col('model_score') <= i &).select(sql_avg(labelcol).alias('rate')).rdd.collect()[0]['rate']) for i in ths]
    for pair in samplepoints:
        print("[modeler] get_calibration_function2 | Training " + str(pair[0]) + ", " + str(pair[1]))
    sampledf = spark.createDataFrame(samplepoints, ['model_score', 'target_prob'])
    sampledf.show()
    print("[modeler] get_calibration_function2 | Samples to implement the calibration model showed above")
    ir = IsotonicRegression(featuresCol='model_score', labelCol='target_prob', predictionCol='calib_model_score')
    irmodel = ir.fit(sampledf)
    return (irmodel, samplepoints, ir)


def get_lift(dt, score_col, label_col, nile = 40, refprevalence=None):

    '''
    AVOID USING THIS FUNCTION. It cannot be guaranteed that segments are equally sized. Use get_cumulative_churn_rate_fix_step
    :param dt:
    :param score_col:
    :param label_col:
    :param nile:
    :param refprevalence:
    :return:
    '''
    print("[Info get_lift] Computing lift")

    if(refprevalence == None):
        refprevalence = dt.select(label_col).rdd.map(lambda r: r[label_col]).mean()

    print("[Info get_lift] Computing lift - Ref Prevalence for class 1: " + str(refprevalence))

    dtdecile = get_deciles(dt, score_col, nile)
    result = dtdecile\
        .groupBy("decile")\
        .agg(*[sql_avg(label_col).alias("prevalence"), sql_count("*").alias("decile_count")])\
        .withColumn("refprevalence", lit(refprevalence))\
        .withColumn("lift", col("prevalence")/col("refprevalence"))\
        .withColumn("decile", col("decile").cast("double"))\
        .sort(desc("decile"))


    print("DECILE COUNT")
    print((result.select("decile",  "decile_count", "prevalence", "refprevalence", "lift").show()))

    result2 = (result.select("decile", "lift")
    .rdd\
    .map(lambda r: (r["decile"], r["lift"]))\
    .collect())

    result_ord = sorted(result2, key=lambda tup: tup[0], reverse=True)

    return result_ord


def get_cumulative_lift(spark, df_tt_preds, tt_churn_ref, top_list=None):

        if not top_list:
            top_list = [50000, 40000, 30000, 20000, 15000, 12000, 10000, 5000, 3000, 2000, 1000]


        myschema = df_tt_preds.schema
        for ii in top_list:
            start_time_ii = time.time()
            df_tt_preds = df_tt_preds.sort(desc("model_score"))
            df_top = spark.createDataFrame(df_tt_preds.head(ii),  schema=myschema)
            churn_rate_top =  df_top.select(sql_avg('label').alias('churn_rate')).rdd.first()['churn_rate']
            print(("[modeler] get_cumulative_lift | TOP{} - LIFT={} churn_rate={} churn_rate_ref={} [elapsed = {} minutes]".format(ii, churn_rate_top/tt_churn_ref,
                                                                                                  churn_rate_top*100.0,
                                                                                                  tt_churn_ref*100.0,
                                                                                                  (time.time()-start_time_ii)/60.0)))

def get_cumulative_churn_rate_fix_step(df, step_ = 5000, ord_col ="scoring", label_col ="label", verbose = True):

    '''

    :param spark: spark session
    :param df: dataframe that includes both the target (label) and the numeric column (ord_col) used to sort the customers/services
    :param ord_col: column of the dataframe that must be considered when ordering the customers/services
    :param label_col: target indicating the condition (churn or no churn) of the customer/service (it assumed to be a double with two possible values 1.0 or 0.0)
    :param verbose: print info or not
    :return: dataframe including cum_volume and cum_lift
    '''

    from pyspark.ml.feature import QuantileDiscretizer

    import math

    n_samples = df.count()

    if verbose:
        print("[modeler] get_cumulative_lift_fix_step | Size of the input df: " + str(n_samples))

    tmp_num_buckets = int(math.floor(float(n_samples)/float(step_)))

    num_buckets = tmp_num_buckets if tmp_num_buckets <= 100 else 100

    if verbose:
        print("[modeler] get_cumulative_lift_fix_step |  Number of buckets for a resolution of " + str(step_) + ": " + str(tmp_num_buckets) + " - Final number of buckets: " + str(num_buckets))

    qds = QuantileDiscretizer(numBuckets=num_buckets, inputCol=ord_col, outputCol="bucket", relativeError=0.0)

    bucketed_df = qds.fit(df).transform(df)

    # Num churners per bucket


    bucketed_df = bucketed_df\
        .groupBy('bucket')\
        .agg(sql_count("*").alias("volume"), sql_sum(label_col).alias('num_churners'))\
        .withColumn('all_segment', lit('all'))

    windowval = (Window.partitionBy('all_segment').orderBy(desc('bucket')).rangeBetween(Window.unboundedPreceding, 0))

    result_df = bucketed_df \
        .withColumn('cum_num_churners', sql_sum('num_churners').over(windowval)) \
        .withColumn('cum_volume', sql_sum('volume').over(windowval)) \
        .withColumn('churn_rate', col('num_churners') / col('volume')) \
        .withColumn('cum_churn_rate', col('cum_num_churners') / col('cum_volume'))

    if verbose:
        result_df.orderBy(desc('bucket')).show(50, False)

    return result_df

def get_deciles(dt, column, nile = 40, verbose=True):
    if verbose: print("Computing deciles")
    discretizer = QuantileDiscretizer(numBuckets=nile, inputCol=column, outputCol="decile", relativeError=0)
    bucketizer = discretizer.fit(dt)
    dtdecile = bucketizer.transform(dt).withColumn("decile", col("decile") + lit(1.0))
    print((bucketizer.getSplits()))

    return dtdecile