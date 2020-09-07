
from pyspark.sql.functions import count as sql_count, col, udf, lit, randn, max as sql_max, min as sql_min, desc, lit, avg as sql_avg, sum as sql_sum, substring, from_unixtime, unix_timestamp, asc
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import IsotonicRegression, GBTRegressor, DecisionTreeRegressor, LinearRegression, RandomForestRegressor, GeneralizedLinearRegression
import numpy as np
from pyspark.ml.feature import QuantileDiscretizer
import time
import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
from scipy.stats import entropy

def balance_df(df, label='label'):
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
    for l in counts_dict.keys():
        fractions_dict[l] = float(minor_cat_size) / counts_dict[l]

    balanced = reduce((lambda x, y: x.union(y)), [df.filter(col(label) == k).sample(False, fractions_dict[k]) for k in list(counts_dict.keys())])

    repartdf = balanced.repartition(200)

    print("[modeler] balance_df | Dataframe has been balanced:")
    repartdf.groupBy(label).agg(sql_count('*').alias("count")).show()


    print "[modeler] balance_df | Dataframe has been balanced - Total number of rows is " + str(repartdf.count())

    # balanced = df.sampleBy(label, fractions=fractions_dict, seed=1234)

    return repartdf

def getOrderedRelevantFeats(model, featCols, pca):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        try:
            impFeats = model.stages[-1].featureImportances

            feat_and_imp = zip(featCols, impFeats.toArray())
            return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)
        except:
            print("[ERROR] Check that model has featureImportances attribute!")
            return None

def get_split(df_tar, train_split_ratio=0.7):


    [df_unbaltr, df_tt] = df_tar.randomSplit([train_split_ratio, 1-train_split_ratio], 1234)

    #print "[Info FbbChurn] " + time.ctime() + " Total number of training samples is : " + str(unbaltrdf.count())

    df_unbaltr.describe('label').show()

    print " Stat description of the target variable printed above"

    # 1.2. Balanced df for training

    df_unbaltr.groupBy('label').agg(sql_count('*')).show()

    print " Count on label column for unbalanced tr set showed above"

    df_tr = balance_df(df_unbaltr, 'label')

    df_tr.groupBy('label').agg(sql_count('*')).show()

    print " Count on label column for balanced tr set showed above"

    return df_unbaltr, df_tr, df_tt



def encoding_categoricals(categorical_columns, stages_=None, handle_invalid="error"):
    '''

    :param categorical_columns:
    :param stages_:
    :param handle_invalid:
            'error': throws an exception (which is the default)
            'skip': skips the rows containing the unseen labels entirely (removes the rows on the output!)
            'keep': puts unseen labels in a special additional bucket, at index numLabels
    :return:
    '''

    stages = [] if stages_ == None else stages_

    for categorical_col in categorical_columns:
        print("StringIndexer+OneHotEncoderEstimator for variable {}".format(categorical_col))
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index', handleInvalid=handle_invalid) # options are "keep", "error" or "skip"
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    new_categorical_inputs = [c + "_class_vec" for c in categorical_columns]

    return stages, new_categorical_inputs


def get_model(model_name, label_col="label", featuresCol="features"):
    return {
        'rf': RandomForestClassifier(featuresCol=featuresCol, numTrees=800, maxDepth=10, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=100, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
        'gbt': GBTClassifier(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
        'gbt_reg' : GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=100)
    }[model_name]


def isClassificationProblem(model_name):
    '''
    Returns True if model_name is a reference to a Classifier model.
    :param model_name: 'rf' or 'gbt' to train the model or an object with the model to be used
    :return:
    '''

    print("[modeler] Called isClassifier with {}".format(model_name if isinstance(model_name, str) else model_name.uid))
    dt = get_model(model_name, label_col="label", featuresCol="features") if isinstance(model_name, str) else model_name

    if isinstance(dt, RandomForestClassifier) or isinstance(dt, GBTClassifier):
        return True
    elif isinstance(dt, GBTRegressor) or isinstance(dt, RandomForestRegressor) or isinstance(dt, DecisionTreeRegressor) or isinstance(dt, LinearRegression) or isinstance(dt, GeneralizedLinearRegression):
        return False
    else:
        print("[ERROR] {} is not considered in 'isClassificationProblem' function in modeler.py file. Please, add it an re-run your process.".format(model_name if isinstance(model_name, str) else model_name.uid))
        sys.exit()

def smart_fit(model_name, df_tr, feats=None, non_info_cols=None, categorical_cols=None, label_col="label", balance_tr_df=False,
              handle_invalid="error"):
    '''
    :param model_name: rf or gbt to train the model or an object with the model to be used
    :param df_tr: training dataframe
    :param feats: list of columns to used for training. If not specified, then all columns in df_tr are used, except label_col
    :param non_info_cols:
    :param categorical_cols. If categorical_cols=None, it detects them automatically, extract the categoricals as the columns with type=string
                             If categorical_cols=[] it ignores the categoricals
                             if categoricals_cols is present, encode this list of cols
    :param handle_invalid:
            'error': throws an exception (which is the default)
            'skip': skips the rows containing the unseen labels entirely (removes the rows on the output!)
            'keep': puts unseen labels in a special additional bucket, at index numLabels
    :return:
    '''

    if not non_info_cols:
        non_info_cols = []

    if not feats:
        print("[modeler] smart_fit | Detecting automatically features since no feats were selected")
        feats = list(set(df_tr.columns) - {label_col})

    if categorical_cols is None:
        print("[modeler] smart_fit | detecting automatically categorical columns")
        categorical_cols = [col_ for col_, type_ in df_tr.dtypes if type_=="string" and col_ not in non_info_cols]
        print("[modeler] smart_fit | Detected {} categoricals: {}".format(len(categorical_cols), ",".join(categorical_cols)))

    elif len(categorical_cols) == 0:
        print("[modeler] smart_fit | Discarding categoricals since user input categorical_cols=[]")
        detected_categorical_cols = [col_ for col_, type_ in df_tr.select(feats).dtypes if type_=="string" and col_ not in non_info_cols]
        # Remove categorical from feats
        feats = list(set(feats) - {label_col} - set(detected_categorical_cols))

    stages = []
    new_categorical_inputs = []

    if categorical_cols:
        print("[modeler] smart_fit | about to encode categoricals: {}".format(len(categorical_cols), ",".join(categorical_cols)))
        stages, new_categorical_inputs = encoding_categoricals(categorical_cols, stages_=[], handle_invalid=handle_invalid)

    numeric_columns = list(set(feats) - set(categorical_cols) - set(non_info_cols) - {label_col})
    assembler_inputs = new_categorical_inputs + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]
    print(len(assembler_inputs))
    # model_name could be a string or an object
    dt = get_model(model_name, label_col=label_col, featuresCol="features") if isinstance(model_name, str) else model_name
    stages += [dt]
    pipeline = Pipeline(stages=stages)
    if balance_tr_df:
        pipeline_model = pipeline.fit(balance_df(df_tr))
    else:
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

    if not feat_importance: return None

    ii = 1
    for fimp in feat_importance:
        print "[{:>3}] {} : {}".format(ii, fimp[0], fimp[1])
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
    print("[modeler] get_score | Setting score in column '{}'".format(score_col))
    df_preds = model.transform(df).withColumn(score_col, getScore(col("probability")).cast(DoubleType()))

    if calib_model:
        print("[modeler] get_score | calibrating scores...")
        print(calib_model[2].extractParamMap())
        print(calib_model[2].getFeaturesCol())
        print(calib_model[2].getPredictionCol())
        print("[modeler] get_score | Applying calib_model. features_col = '{}' || prediction_col = '{}'".format(calib_model[2].getFeaturesCol(),
                                                                                          calib_model[2].getPredictionCol()))
        score_col = calib_model[2].getPredictionCol() # IsotonicRegression
        df_preds  = calib_model[0].transform(df_preds) # IsotonicRegressionModel
        #After calibrating, the calibrated score is stored in column calib_model.getPredictionCol()
        print("[modeler] get_score | Calibrated score is stored in column {}".format(score_col))
        print("[modeler] get_score | after calib columns {}".format(",".join(df_preds.columns)))
    else:
        print("[modeler] get_score | Scores wont be calibrated")

    if add_randn:
        print("[modeler] get_score | Adding random number to scores")
        df_preds = df_preds.withColumn(score_col, col(score_col) + lit(0.00001)*randn())

    return df_preds

def get_auc(df_preds, score_col="model_score", label_col="label"):


    print("[modeler] get_auc | label distribution")
    df_preds.groupBy(label_col).agg(sql_count("*").alias("count")).show()

    # Checking the direction of the feature/score: higher score means 1?

    avg_df = df_preds.groupBy(label_col).agg(sql_avg(score_col).alias('feat_avg'))

    avg0 = avg_df.filter(col(label_col)==0).first()['feat_avg']
    avg1 = avg_df.filter(col(label_col) == 1).first()['feat_avg']

    df_preds = df_preds.withColumn(score_col, -1.0*col(score_col)) if(avg0 > avg1) else df_preds

    # From df to RDD

    preds_and_labels = df_preds.select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))

    my_metrics = BinaryClassificationMetrics(preds_and_labels)

    auc = my_metrics.areaUnderROC

    return auc

def get_metrics(spark, df_preds, title="", do_churn_rate_fix_step=True, score_col="model_score", label_col="label"):
    '''
    Metrics for a classification problem
    :param df_preds:
    :param title: string just for showing results
    :param score_col
    :param label_col:
    :return:
    '''

    print("[modeler] get_metrics do_churn_rate_fix_step={}, score_col={}, label_col={}".format(do_churn_rate_fix_step, score_col, label_col))

    #preds_and_labels = df_preds.select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))

    #my_metrics = BinaryClassificationMetrics(preds_and_labels)
    auc = get_auc(df_preds, score_col=score_col, label_col=label_col)

    print("[modeler] get_metrics METRICS FOR {}".format(title))
    print("[modeler] get_metrics \t AUC = {}".format(auc))

    lift_in_top_churn = get_lift_in_top_churn(df_preds, ord_col=score_col, label_col=label_col, verbose=True)
    print("[modeler] get_metrics lift_in_top_churn = {}".format(lift_in_top_churn))

    captured_churn = get_captured_churn(spark, df_preds, top=0.2, ord_col=score_col, label_col=label_col)
    print("[modeler] captured churn in top 20 % = {}".format(100.0 * captured_churn))


    if do_churn_rate_fix_step:

        cum_churn_rate = get_cumulative_churn_rate_fix_step(spark, df_preds, ord_col=score_col, label_col=label_col)

        print '[modeler] get_metrics  Churn rate'
        cum_churn_rate.orderBy(desc('bucket')).show(50, False)

        return auc, cum_churn_rate, lift_in_top_churn

    return auc, None, lift_in_top_churn


def get_multiclass_metrics(spark, df_preds, score_col="model_score", label_col="label", additional_metrics=True):
    '''
    Metrics for a classification problem
    :param df_preds:
    :param title: string just for showing results
    :param score_col
    :param label_col:
    :return:
    '''

    print("[modeler] get_multiclass_metrics | score_col={}, label_col={}".format(score_col, label_col))


    preds_and_labels =  df_preds.repartition(200).select([score_col, label_col]).cache()

    print("[modeler] get_multiclass_metrics | preds_and_labels.count()={}".format(preds_and_labels.count()))


    preds_and_labels = preds_and_labels.rdd.map(lambda r: (r[score_col], float(r[label_col])))

    metrics = MulticlassMetrics(preds_and_labels)
    print("confusion_matrix")
    print(metrics.confusionMatrix().toArray())

    metrics_dict = {}

    metrics_dict["confusion_matrix"] = metrics.confusionMatrix().toArray()

    # Overall statistics
    precision = metrics.precision()
    recall = metrics.recall()
    f1Score = metrics.fMeasure()

    metrics_dict["precision"] = precision
    metrics_dict["recall"] = recall
    metrics_dict["f1_score"] = f1Score

    # Statistics by class
    labels_list = df_preds.select(label_col).distinct().rdd.map(lambda x: x[0]).collect()
    for label in sorted(labels_list):
        metrics_dict["precision_class{}".format(label)] = metrics.precision(label)
        metrics_dict["recall_class{}".format(label)] = metrics.recall(label)
        metrics_dict["f1_measure_class{}".format(label)] = metrics.fMeasure(float(label), beta=1.0)

    # Weighted stats
    metrics_dict["weighted_recall"] = metrics.weightedRecall
    metrics_dict["weighted_precision"] = metrics.weightedPrecision
    metrics_dict["weighted_f1_score"] = metrics.weightedFMeasure()
    metrics_dict["weighted_f0.5_score"] = metrics.weightedFMeasure(beta=0.5)
    metrics_dict["weighted_false_positive_rate"] = metrics.weightedFalsePositiveRate

    if additional_metrics:
        add_metrics_dict = compute_metrics_by_entropy(df_preds, score_col="model_score", label_col="label")
        metrics_dict.update(add_metrics_dict)

    print("[modeler] get_multiclass_metrics | Metrics Summary:")
    import pprint
    pprint.pprint(metrics_dict)

    return metrics_dict


def compute_metrics_by_entropy(df_preds, score_col="model_score", label_col="label", prob_col="probability", q_probs_list=None, pred_col="prediction"):
    '''

    :param spark:
    :param df_preds:
    :param score_col:
    :param label_col:
    :param q_probs_list:
    :return:
    '''
    start_time = time.time()

    print(df_preds.columns)

    if not q_probs_list:
        q_probs_list = [0.05, 0.10, 0.20, 0.25, 0.3, 0.35, 0.4, 0.5, 0.55, 0.6, 0.75, 0.8, 0.85, 0.90, 0.95, 0.99, 1.00]

    print("[modeler] compute_metrics_by_entropy | score_col={}, label_col={}, prob_col={} pred_col={}".format(score_col, label_col, prob_col, pred_col))

    num_classes = len(df_preds.select(prob_col).first()[0]) # num classes in classification problem

    print("[modeler] compute_metrics_by_entropy | {} problem".format("binary" if num_classes==2 else "multiclass"))

    def entropy_(base, v):
        '''
        Calculate the entropy of a distribution for given probability values. 'v' is a Vector
        The entropy is calculated as S = -sum(pk * log(pk), axis=axis).
        :param base The logarithmic base to use.
        '''
        return float(entropy(v.tolist(), qk=None, base=base))

    from functools import partial
    entropy_udf = udf(partial(entropy_, num_classes), FloatType())

    df_preds = df_preds.withColumn("entropy", entropy_udf(col(prob_col)))

    metrics_dict ={}
    thresh_list = []
    weighted_f1_score_list = []
    volumes_list = []
    volumes_by_class_list = [] # only for multiclass
    precision_by_class_list=[] # only for multiclass
    recall_list = []
    accuracy_list = []
    precision_list=[]

    labels_list = df_preds.select(label_col).distinct().rdd.map(lambda x: x[0]).collect()
    print("[modeler] compute_metrics_by_entropy | labels: {}".format(labels_list))

    df_preds = df_preds.cache()
    print("[modeler] compute_metrics_by_entropy | df_preds has {} rows".format(df_preds.count()))
    for qq in q_probs_list:
        thresh = 1
        if num_classes > 2:
            if qq == 1:
                preds_and_labels = df_preds.select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))
                thresh = 1
            else:
                thresh = df_preds.approxQuantile("entropy", [qq], 0.000001)[0]
                preds_and_labels = df_preds.where(col("entropy") <= thresh).select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))
            metrics = MulticlassMetrics(preds_and_labels)
            metrics_dict["confusion_matrix_q{}".format(qq)] = metrics.confusionMatrix().toArray()
            weighted_f1_score_list.append(metrics.weightedFMeasure())
            accuracy_list.append(metrics.accuracy)
            for lab in labels_list:
                try:
                    precision_by_class_list.append([metrics.precision(lab)])
                except Exception as e:
                    print(e)
                    print("[ERROR] Label {} not found".format(lab))

            volumes_by_class_list.append([df_preds.where(col("entropy") <= thresh).where(col(pred_col)==lab).count() for lab in labels_list])

        else:

            if qq == 1:
                df_ = df_preds
            else:
                thresh = df_preds.approxQuantile("entropy", [qq], 0.000001)[0]
                df_ = df_preds.where(col("entropy") <= thresh)

            TP = df_.where( (col("label")==1) & (col(pred_col)==1) ).count()
            FP = df_.where( (col("label")==0) & (col(pred_col)==1)).count()
            FN = df_.where( (col("label")==1) & (col(pred_col)==0)).count()
            TN = df_.where( (col("label")==0) & (col(pred_col)==0)).count()
            accuracy = 1.0 * (TP + TN) / df_.count()
            recall = 1.0 * TP / (TP+FN)
            precision = 1.0 * TP / (TP + FP)
            F1 = ((2.0 * precision * recall) / (precision + recall)) if (precision+recall) > 0 else 0
            weighted_f1_score_list.append(F1)
            recall_list.append(recall)
            precision_list.append(precision)
            accuracy_list.append(accuracy)
            metrics_dict["confusion_matrix_q{}".format(qq)] = [[TN,FP],[FN, TP]]


        thresh_list.append(thresh)
        volumes_list.append(df_preds.where(col("entropy") <= thresh).count())

    metrics_dict["entropy_thresh_list"] = thresh_list
    metrics_dict["entropy_volumes_list"] = volumes_list
    metrics_dict["entropy_probs_list"] = q_probs_list
    metrics_dict["entropy_accuracy_list"] = accuracy_list
    metrics_dict["entropy_precision_list"] = precision_list
    metrics_dict["labels_list"] = labels_list

    if num_classes > 2:
        metrics_dict["entropy_weighted_f1_score_list"] = weighted_f1_score_list
        metrics_dict["entropy_precision_by_class_list"] = precision_by_class_list
        metrics_dict["entropy_volumes_by_class_list"] = volumes_by_class_list
    else:
        metrics_dict["entropy_f1_score_list"] = weighted_f1_score_list
        metrics_dict["entropy_recall_list"] = recall_list

    print("[modeler] compute_metrics_by_entropy | Elapsed time: {} minutes".format( (time.time() - start_time)/60))

    return metrics_dict

def get_metrics_regression(spark, df_preds, title="", score_col="model_score", label_col="label"):
    '''
    Metrics for a regression problem
    :param df_preds:
    :param title: string just for showing results
    :param nb_deciles:
    :param score_col
    :param label_col:
    :param refprevalence:
    :return:
    '''

    print("[modeler] get_metrics score_col={}, label_col={}".format(score_col, label_col))



    gbt_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=score_col, metricName="rmse")
    rmse = gbt_evaluator.evaluate(df_preds.select([score_col, label_col]))
    print("[modeler] get_metrics_regression | Root Mean Squared Error (RMSE) on {} data = {}".format(title,rmse))

    gbt_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=score_col, metricName="r2")
    r2 = gbt_evaluator.evaluate(df_preds.select([score_col, label_col]))
    print("[modeler] get_metrics_regression | R2 on {} data = {}".format(title, r2))

    return rmse, r2


def get_calibration_function2(spark, model, valdf, labelcol, numpoints = 10):
    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    valpredsdf = model.transform(valdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType())).select(['model_score', labelcol])
    minprob = valpredsdf.select(sql_min('model_score').alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max('model_score').alias('max')).rdd.collect()[0]['max']
    numlabels = float(valpredsdf.filter(col(labelcol) == 1.0).count())
    print "[modeler] get_calibration_function2 | Training - Num samples in the target class " + str(numlabels)
    delta = float(maxprob - minprob)/float(numpoints)
    ths = list([(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))])
    samplepoints = [(float(i), float(valpredsdf.filter((col('model_score') <= i) & (col(labelcol) == 1.0)).count())/numlabels) for i in ths]
    # samplepoints = [(float(i), valpredsdf.filter(col('model_score') <= i &).select(sql_avg(labelcol).alias('rate')).rdd.collect()[0]['rate']) for i in ths]
    for pair in samplepoints:
        print "[modeler] get_calibration_function2 | Training " + str(pair[0]) + ", " + str(pair[1])
    sampledf = spark.createDataFrame(samplepoints, ['model_score', 'target_prob'])
    sampledf.show()
    print "[modeler] get_calibration_function2 | Samples to implement the calibration model showed above"
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
    print "[Info get_lift] Computing lift"

    if(refprevalence == None):
        refprevalence = dt.select(label_col).rdd.map(lambda r: r[label_col]).mean()

    print "[Info get_lift] Computing lift - Ref Prevalence for class 1: " + str(refprevalence)

    dtdecile = get_deciles(dt, score_col, nile)
    result = dtdecile\
        .groupBy("decile")\
        .agg(*[sql_avg(label_col).alias("prevalence"), sql_count("*").alias("decile_count")])\
        .withColumn("refprevalence", lit(refprevalence))\
        .withColumn("lift", col("prevalence")/col("refprevalence"))\
        .withColumn("decile", col("decile").cast("double"))\
        .sort(desc("decile"))


    print("DECILE COUNT")
    print(result.select("decile",  "decile_count", "prevalence", "refprevalence", "lift").show())

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
            print("[modeler] get_cumulative_lift | TOP{} - LIFT={} churn_rate={} churn_rate_ref={} [elapsed = {} minutes]".format(ii, churn_rate_top/tt_churn_ref,
                                                                                                  churn_rate_top*100.0,
                                                                                                  tt_churn_ref*100.0,
                                                                                                  (time.time()-start_time_ii)/60.0))


def get_f1_score_curve_fix_step(spark, df, step_ = 5000, ord_col ="scoring", label_col ="label", verbose = True, noise = 0):

    # TODO: extend to f-beta, with beta as an input argument set to 1 by default
    # beta = the weight given to recall beta times the weight given to precision
    # f-beta = 1/((1/(beta+1))*(1/precision) + (beta/(beta+1))*(1/recall)) = (1 + beta)*precision*recall/(beta*precision + recall)

    lift_df = get_cumulative_churn_rate_fix_step(spark, df, step_, ord_col, label_col, verbose, noise)

    # F1 score vs volume

    window = Window.partitionBy('all')

    f1_df = lift_df\
        .withColumn('all', lit('all'))\
        .withColumn('total_num_churners', sql_sum('num_churners').over(window))\
        .withColumn('precision', col('cum_churn_rate'))\
        .withColumn('recall', col('cum_num_churners').cast('double')/col('total_num_churners'))\
        .withColumn('f1_score', 2*col('precision')*col('recall')/(col('precision') + col('recall')))\
        .select('f1_score', 'cum_volume')

    # Optimum threshold: volume and threshold on the score for which the F1 score is the maximum

    opt_vol = f1_df \
        .withColumn('tmp', lit('all')) \
        .withColumn('max_f1', sql_max('f1_score').over(Window.partitionBy('tmp'))) \
        .filter(col('f1_score') == col('max_f1')).first()['cum_volume']

    total_vol = df.count()

    opt_th = df.approxQuantile(ord_col, [1.0 - float(opt_vol) / float(total_vol)], 0)[0]

    # Churners captured by using the optimum volume

    total_churn = df.filter(col(label_col)==1).count()

    cap_churn = df.filter((col(ord_col) > opt_th) & (col(label_col)==1)).count()

    if (verbose):
        print '[Info] Volume: ' + str(opt_vol) + ' out of ' + str(total_vol) + ' (' + str(float(opt_vol)/float(total_vol))  + ') - Churn: ' + str(cap_churn) + ' out of ' + str(total_churn) + ' (' + str(float(cap_churn)/float(total_churn)) + ') - Optimum threshold: ' + str(opt_th)

    return (f1_df, opt_vol, opt_th)

def get_lift_in_top_churn(df, ord_col ="scoring", label_col ="label", verbose = True):

    df = df.select(ord_col, label_col)

    df.cache()

    churn_rate = df.select(sql_avg(label_col).alias('churn_rate')).first()['churn_rate']

    th = df.approxQuantile(ord_col, [1.0 - churn_rate], 0)[0]

    churn_rate_in_top_churn = df.filter(col(ord_col) > th).select(sql_avg(label_col).alias('churn_rate')).first()['churn_rate']

    if(verbose):
        group_df = df.groupBy(label_col).agg(sql_count('*').alias('num_samples'))

        print '[Info] Num samples 0: ' + str(group_df.filter(col(label_col)==0).first()['num_samples']) + ' - Num samples 1: ' + str(group_df.filter(col(label_col)==1).first()['num_samples'])

        print '[Info] Churn rate in top ' + str(df.filter(col(ord_col) > th).count()) + ': ' + str(churn_rate_in_top_churn)

    return churn_rate_in_top_churn

def get_cumulative_churn_rate_fix_step(spark, df, step_ = 5000, ord_col ="scoring", label_col ="label", verbose = True, noise = 0):

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
        print "[modeler] get_cumulative_lift_fix_step | Size of the input df: " + str(n_samples)

    tmp_num_buckets = int(math.floor(float(n_samples)/float(step_))) if step_ >= 1 else int(1.0/float(step_))

    #if step_ < 1 --> proportion of samples in each bucket: gives the user the way to specify the number of points

    num_buckets = tmp_num_buckets if tmp_num_buckets <= 5000 else 5000

    if verbose:
        print "[modeler] get_cumulative_lift_fix_step |  Number of buckets for a resolution of " + str(step_) + ": " + str(tmp_num_buckets) + " - Final number of buckets: " + str(num_buckets)

    if num_buckets > 1:
        df = df.withColumn(ord_col, col(ord_col) + lit(noise) * randn())
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
            .withColumn('cum_churn_rate', col('cum_num_churners') / col('cum_volume'))\
            .withColumn('total_volume', sql_sum('volume').over(Window.partitionBy('all_segment')))\
            .withColumn('total_churners', sql_sum('num_churners').over(Window.partitionBy('all_segment')))\
            .withColumn('cum_captured_churn', col('cum_num_churners')/col('total_churners'))\
            .withColumn('cum_volume_for_capture', col('cum_volume')/col('total_volume'))\
            .drop('all_segment')

    else:

        print("[modeler] get_cumulative_lift_fix_step | Only one bucket for a step of {} and num_samples {}. Consider reducing the step size.".format(step_, n_samples))
        import pandas as pd

        num_churners = df.where(col(label_col)==1).count()


        df_pandas = pd.DataFrame({"bucket" : 1, "volume" : n_samples, "all_segment" : "all", "cum_num_churners" : num_churners,
                                  "cum_volume" : n_samples, "churn_rate" : 1.0 * num_churners /n_samples,
                                  "cum_churn_rate" :  1.0 * num_churners /n_samples,
                                  "cum_captured_churn" : 1.0, "cum_volume_for_capture" : 1.0}, index=[0])

        cols_ = list(df_pandas.columns)

        result_df = spark.createDataFrame(pd.DataFrame(df_pandas)).select(*cols_)



    if verbose:
        result_df.orderBy(desc('bucket')).show(50, False)

    return result_df

def get_deciles(dt, column, nile = 40, verbose=True):
    if verbose: print "Computing deciles"
    discretizer = QuantileDiscretizer(numBuckets=nile, inputCol=column, outputCol="decile", relativeError=0)
    bucketizer = discretizer.fit(dt)
    dtdecile = bucketizer.transform(dt).withColumn("decile", col("decile") + lit(1.0))
    print(bucketizer.getSplits())

    return dtdecile

def get_model_combination(spark, df1, df2, num_buckets = 300, ord_col =  "model_score", verbose = True):
    # From df1, the signature (ordered structure of (segment, bucket) elements) is obtained
    mob1_hard = df1.filter(col('segment')=='bound=hard')
    mob1_soft = df1.filter(col('segment')=='bound=soft')
    mob1_none = df1.filter(col('segment')=='bound=none')
    lift_mob1_hard_comb = get_cumulative_churn_rate_fix_step(spark, mob1_hard, step_ = 1.0/num_buckets, ord_col =ord_col, label_col ="label", verbose = False, noise = 0.000001).withColumn('segment', lit('bound=hard')).select('bucket', 'cum_churn_rate', 'segment')
    lift_mob1_soft_comb = get_cumulative_churn_rate_fix_step(spark, mob1_soft, step_ = 1.0/num_buckets, ord_col =ord_col, label_col ="label", verbose = False, noise = 0.000001).withColumn('segment', lit('bound=soft')).select('bucket', 'cum_churn_rate', 'segment')
    lift_mob1_none_comb = get_cumulative_churn_rate_fix_step(spark, mob1_none, step_ = 1.0/num_buckets, ord_col =ord_col, label_col ="label", verbose = False, noise = 0.000001).withColumn('segment', lit('bound=none')).select('bucket', 'cum_churn_rate', 'segment')
    lift_mob1_comb = lift_mob1_none_comb.union(lift_mob1_soft_comb).union(lift_mob1_hard_comb)
    signature = lift_mob1_comb.toPandas()
    signature = signature.sort_values(by='cum_churn_rate', ascending=True)
    signature['position'] = range(1, len(signature) + 1)
    signature_df = spark.createDataFrame(signature)
    if(verbose):
        signature_df.show()
    # Combining: the set to be ordered (df2) is prepared by computing buckets
    # IMPORTANT: an equal number of buckets in both the signature and the structure (bucketed) df2 is required
    mob2_hard = df2.filter(col('segment')=='bound=hard')
    mob2_soft = df2.filter(col('segment')=='bound=soft')
    mob2_none = df2.filter(col('segment')=='bound=none')
    from pyspark.ml.feature import QuantileDiscretizer
    qds = QuantileDiscretizer(numBuckets=num_buckets, inputCol=ord_col, outputCol="bucket", relativeError=0.0)
    mob2_hard = mob2_hard.withColumn(ord_col, col(ord_col) + lit(0.000001) * randn())
    mob2_soft = mob2_soft.withColumn(ord_col, col(ord_col) + lit(0.000001) * randn())
    mob2_none = mob2_none.withColumn(ord_col, col(ord_col) + lit(0.000001) * randn())
    bucketed_hard = qds.fit(mob2_hard).transform(mob2_hard)
    bucketed_soft = qds.fit(mob2_soft).transform(mob2_soft)
    bucketed_none = qds.fit(mob2_none).transform(mob2_none)
    bucketed_df = bucketed_hard.union(bucketed_soft).union(bucketed_none)
    # Ordering df2 according to the obtained signature and generating an artificial score (ord_score) that is
    # mapped onto the [0, 1] interval
    ord_df = bucketed_df\
    .join(signature_df.select('bucket', 'segment', 'position'), ['bucket', 'segment'], 'inner')\
    .withColumn('ord_score', col('position') + col(ord_col))\
    .withColumn('all', lit('all'))\
    .withColumn('max_ord_score', sql_max('ord_score').over(Window.partitionBy('all')))\
    .withColumn('min_ord_score', sql_min('ord_score').over(Window.partitionBy('all')))\
    .withColumn('norm_ord_score', (col('ord_score') - col('min_ord_score'))/(col('max_ord_score') - col('min_ord_score')))
    if(verbose):
        ord_df.show()
    return ord_df

def get_captured_churn(spark, df, top=0.2, ord_col="scoring", label_col="label"):
    '''

    REturn the captured churn for the top "top"

    :param spark:
    :param df:
    :param top: number between 0 and 1. E.g. top=0.2 returns the captured churn for the top 20%
    :param ord_col: column with the order of the dataframe, e.g. the score
    :param label_col: column name with the label column
    :return:
    '''
    cum_churn_rate = get_cumulative_churn_rate_fix_step(spark, df, ord_col=ord_col, label_col=label_col, step_=top)

    num_buckets = int(1.0 / float(top))

    return cum_churn_rate.where(col("bucket") == (num_buckets - 1)).rdd.first()["cum_captured_churn"]

def get_projected_churn_rate(churn, cycles):

    '''
    The function computes the projection of the churn observed in a given period onto a number of cycles or periods
    :param churn: churn rate observed in a period
    :param cycles: number of periods to consider for the projection
    :return: churn rate in the period given by cycles*period
    '''


    # projected.churn <- 1 - ((1 - observed.churn)^cycles - observed.churn*(1 - observed.churn)^cycles)
    projected_churn = 1 - (1 - churn)**(cycles + 1)

    return projected_churn

def get_benefit_analysis(spark, df, step_= 5000, ord_col="scoring", label_col="label", verbose=True, noise=0, discount=[15], period=1, redemp_rate=0.25, bound_factor=False):

    '''

    :param spark:
    :param df: df containing predictions and label
    :param benefit_col: column of the cost_df containing the variable representing the benefit
    :return:
    '''

    # Optimum threshold: volume and threshold on the score for which the benefit is the maximum

    cost_df = get_cost_curve(spark, df, step_= step_, ord_col=ord_col, label_col=label_col, verbose=verbose, noise=noise, discount=discount, period=period, redemp_rate=redemp_rate, bound_factor=bound_factor)

    benefit_cols = ['benefit_wrt_baseline_' + str(d) for d in discount]

    cost_df.select(['cum_volume'] + benefit_cols).orderBy(asc('cum_volume')).show(100, False)

    def get_opt_vol_for_benefit(c_df, d):

        benefit_col = 'benefit_wrt_baseline_' + str(d)
        perfect_benefit_col = 'perfect_benefit_' + str(d)
        abs_benefit_col = 'benefit_' + str(d)

        r = c_df\
            .withColumn('tmp', lit('all')) \
            .withColumn('max_benefit', sql_max(benefit_col).over(Window.partitionBy('tmp'))) \
            .filter(col(benefit_col) == col('max_benefit')).first()

        return (r['cum_volume'], r['max_benefit'], r[perfect_benefit_col], r[abs_benefit_col])

    opt_vol = [((d,) + get_opt_vol_for_benefit(cost_df, d)) for d in discount]

    total_vol = df.count()

    opt_th = df.approxQuantile(ord_col, [1.0 - float(v[1])/float(total_vol) for v in opt_vol], 0)

    # Adding the optimum threshold to the series

    opt_vol = zip(opt_vol, opt_th)

    opt_vol = [e[0] + (e[1],) for e in opt_vol]

    result_df = spark\
        .createDataFrame(opt_vol, ['discount', 'opt_volume', 'max_benefit_wrt_baseline', 'perfect_benefit', 'max_abs_benefit', 'opt_threshold'])\
        .withColumn('opt_volume_pcg', col('opt_volume')/total_vol)\
        .withColumn('norm_max_benefit_wrt_baseline', col('max_benefit_wrt_baseline')/col('perfect_benefit'))

    return result_df

def get_roc_curve(spark, df, step_=5000, ord_col="scoring", label_col="label", verbose=True, noise=0):

    # Adding some residual noise to ensure the number of buckets
    df = df.withColumn(ord_col, col(ord_col) + lit(noise) * randn())

    roc_vol = get_cumulative_churn_rate_fix_step(spark, df, step_, ord_col, label_col, verbose, noise=0) \
        .select('cum_num_churners', 'cum_volume', 'total_churners', 'total_volume') \
        .withColumnRenamed('cum_volume', 'predicted_positive') \
        .withColumnRenamed('cum_num_churners', 'true_positive') \
        .withColumnRenamed('total_churners', 'total_positive') \
        .withColumn('total_negative', col('total_volume') - col('total_positive')) \
        .withColumn('false_positive', col('predicted_positive') - col('true_positive')) \
        .withColumn('false_negative', col('total_positive') - col('true_positive')) \
        .withColumn('true_negative', col('total_negative') - col('false_positive'))\
        .withColumn('tpr', col('true_positive')/col('total_positive'))\
        .withColumn('fpr', col('false_positive')/col('total_negative'))\
        .withColumn('pcg_volume', 1.0 - col('predicted_positive')/col('total_volume'))\
        .select('tpr', 'fpr', 'pcg_volume')\
        .rdd\
        .map(lambda r: (r['pcg_volume'], r['tpr'], r['fpr'])).collect()

    # Sorting in ascending order

    roc_vol.sort(key=lambda tup: tup[0])

    pcgs = [e[0] for e in roc_vol]

    ths = df.approxQuantile(ord_col, pcgs, 0)

    roc_vol = zip(roc_vol, ths)

    roc_vol = [e[0] + (e[1],) for e in roc_vol]

    roc_curve = [(e[3], e[1], e[2]) for e in roc_vol]

    return roc_curve

def get_optimum_sens_spec_threshold(spark, df, step_=5000, ord_col="scoring", label_col="label", verbose=True, noise=0):

    '''

    :param spark:
    :param df:
    :param step_:
    :param ord_col:
    :param label_col:
    :param verbose:
    :param noise:
    :return: opt_point: a tuple including (optimum threshold for the balance between sens & spec, tpr, fpr, minimum distance from the ROC curve to the left top corner with tpr=1 and fpr = 0)
    '''

    roc_curve = get_roc_curve(spark, df, step_= step_, ord_col=ord_col, label_col=label_col, verbose=verbose, noise=noise)

    def euclidean_distance(p1, p2):

        return sum((p - q) ** 2 for p, q in zip(p1, p2)) ** .5

    roc_curve_d = [e + (euclidean_distance((e[1], e[2]), (1.0, 0.0)), ) for e in roc_curve]

    # opt_point includes (threshold, tpr, fpr, distance to 1,1)

    roc_curve_d.sort(key=lambda tup: tup[3])

    opt_point = roc_curve_d[0]

    return opt_point






def get_cost_curve(spark, df, step_=5000, ord_col="scoring", label_col="label", verbose=True, noise=0, discount=[15], period=1, redemp_rate=0.25, bound_factor=True):

    '''
    Financial performance/benefit of an action = (1 - d)*TP(u) + d*TN(u) - d*FP(u) - (1 - d)*FN(u), where u is the decision threshold
    Baseline or reference: no action is carried out for any customer;
    equivalently, all the customers as labeled as 'no churn': TP(noact)=FP(noact)=0, FN(noact) = total number of positive cases, TN(noact) = total number of negative cases
    What is gained from the campaign? = Benefit(u) - Benefit(noact)
    :param spark:
    :param df:
    :param step_:
    :param ord_col:
    :param label_col:
    :param verbose:
    :param noise:
    :param discount:
    :param period:
    :param redemp_rate:
    :param bound_factor:
    :return:
    '''

    churn_rate = df.select(sql_avg(label_col).alias('churn')).first()['churn']

    # period is expressed in months: it is the horizon or period during which the churn event is observed for the computation of the label

    num_cycles_projection = int(12.0 / float(period))

    annual_churn_rate = get_projected_churn_rate(churn_rate, num_cycles_projection) if (bound_factor) else 0

    if (verbose):
        print "[Info] Churn rate observed during " + str(period) + " months: " + str(churn_rate)
        print "[Info] Annual projected churn rate: " + str(annual_churn_rate)


    cost_df = get_cumulative_churn_rate_fix_step(spark, df, step_, ord_col, label_col, verbose, noise)\
        .select('cum_num_churners', 'cum_volume', 'total_churners', 'total_volume')\
        .withColumnRenamed('cum_volume', 'predicted_positive')\
        .withColumnRenamed('cum_num_churners', 'true_positive') \
        .withColumnRenamed('total_churners', 'total_positive') \
        .withColumn('total_negative', col('total_volume') - col('total_positive'))\
        .withColumn('false_positive', col('predicted_positive') - col('true_positive'))\
        .withColumn('false_negative', col('total_positive') - col('true_positive'))\
        .withColumn('true_negative', col('total_negative') - col('false_positive'))

    # Benefit for each discount (retention cost)

    for d in discount:
        cost_df = cost_df\
            .withColumn('baseline_revenue_' + str(d), redemp_rate*float(d)*0.01*col('total_negative'))\
            .withColumn('baseline_cost_' + str(d), redemp_rate*(1 - float(d)*0.01)*col('total_positive'))\
            .withColumn('baseline_benefit_' + str(d), col('baseline_revenue_' + str(d)) - col('baseline_cost_' + str(d)))\
            .withColumn('revenue_' + str(d), (1 - float(d)*0.01)*redemp_rate*col('true_positive') + float(d)*0.01*redemp_rate*col('true_negative') + (1 - float(d)*0.01)*redemp_rate*annual_churn_rate * redemp_rate * col('false_positive'))\
            .withColumn('cost_' + str(d), (1 - annual_churn_rate) * float(d)*0.01*redemp_rate * col('false_positive') + (1 - float(d)*0.01)*redemp_rate*col('false_negative'))\
            .withColumn('benefit_' + str(d), col('revenue_' + str(d)) - col('cost_' + str(d)))\
            .withColumn('benefit_wrt_baseline_' + str(d), col('benefit_' + str(d)) - col('baseline_benefit_' + str(d)))\
            .withColumn('perfect_benefit_' + str(d), 2*(1 - float(d)*0.01)*redemp_rate*col('total_positive'))

    cost_df = cost_df\
        .withColumnRenamed('predicted_positive', 'cum_volume')\
        .withColumnRenamed('true_positive', 'cum_num_churners') \
        .withColumnRenamed('total_positive', 'total_churners') \

    return cost_df

def get_last_mob_port_out_date(spark):

    return spark.read.table("raw_es.portabilitiesinout_sopo_solicitud_portabilidad")\
        .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))\
        .select(sql_max("sopo_ds_fecha_solicitud").alias('max_date')).first()['max_date']

def get_last_fix_port_out_date(spark):

    return spark.read.table("raw_es.portabilitiesinout_portafijo")\
        .withColumn("FECHA_INSERCION_SGP", substring(col("FECHA_INSERCION_SGP"), 0, 10))\
        .select(sql_max('FECHA_INSERCION_SGP').alias('max_date')).first()['max_date']


def get_churn_vanish_curve(spark, df, init_date, level, step = 5.0, n_points = 12):

    date_incs_ = [step*e for e in list(range(1, (n_points + 1)))]

    import datetime as dt

    last_mob = int(dt.datetime.strptime(get_last_mob_port_out_date(spark), '%Y-%m-%d').strftime('%Y%m%d'))

    last_fix = int(dt.datetime.strptime(get_last_fix_port_out_date(spark), '%Y-%m-%d').strftime('%Y%m%d'))

    last_port = min([last_mob, last_fix])

    from churn_nrt.src.utils.date_functions import move_date_n_days

    final_date_incs_ = [d for d in date_incs_ if int(move_date_n_days(init_date, d)) < last_port]

    from churn_nrt.src.data.sopos_dxs import MobPort, Target

    if((level=='msisdn') | (level=='mobile')):

        id = 'msisdn'

        port_df = MobPort(spark, churn_window=final_date_incs_[-1])\
            .get_module(init_date, save=False, save_others=False, force_gen=True)\
            .withColumnRenamed('label_mob', 'label')\
            .select('msisdn', 'label', 'portout_date_mob')\
            .withColumnRenamed('portout_date_mob', 'portout_date')

    elif((level=='nif') | (level=='nif_cliente')):

        id = 'nif_cliente'

        port_df = Target(spark, churn_window=final_date_incs_[-1]) \
            .get_module(init_date, save=False, save_others=False, force_gen=True) \
            .select('nif_cliente', 'label', 'portout_date')

    def filter_ports_between_dates(p_df, di, de):

        return p_df\
            .filter((from_unixtime(unix_timestamp(col("portout_date"), "yyyy-MM-dd")) >= from_unixtime(unix_timestamp(lit(di), "yyyyMMdd"))) & (from_unixtime(unix_timestamp(col("portout_date"), "yyyy-MM-dd")) <= from_unixtime(unix_timestamp(lit(de), "yyyyMMdd"))) )\
            .drop('portout_date')

    churn_curve = [(d, df.join(filter_ports_between_dates(port_df, init_date, move_date_n_days(init_date, d)), [id], 'left').na.fill({'label': 0.0}).select(sql_avg('label').alias('churn')).first()['churn']) for d in final_date_incs_]

    churn_curve_df = spark.createDataFrame(churn_curve, ['days_since', 'churn'])

    return churn_curve_df







