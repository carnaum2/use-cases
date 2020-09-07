

from pyspark.sql.functions import count as sql_count, col, udf, lit, randn
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier


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
    for l in counts_dict.keys():
        fractions_dict[l] = float(minor_cat_size) / counts_dict[l]

    balanced = reduce((lambda x, y: x.union(y)), [df.filter(col(label) == k).sample(False, fractions_dict[k]) for k in list(counts_dict.keys())])

    repartdf = balanced.repartition(400)

    print "[Info balance_df2] Dataframe has been balanced - Total number of rows is " + str(repartdf.count())

    # balanced = df.sampleBy(label, fractions=fractions_dict, seed=1234)

    return repartdf

def getOrderedRelevantFeats(model, featCols, pca):
    if (pca.lower()=="t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances

        feat_and_imp = zip(featCols, impFeats.toArray())
        return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)

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



def encoding_categoricals(categorical_columns, stages_=None):

    stages = [] if stages_ == None else stages_

    for categorical_col in categorical_columns:
        print("StringIndexer+OneHotEncoderEstimator for variable {}".format(categorical_col))
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index')
        encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
        stages += [string_indexer, encoder]

    new_categorical_inputs = [c + "_class_vec" for c in categorical_columns]

    return stages, new_categorical_inputs


def get_model(model_name):
    return {
        'rf': RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol="label", seed=1234, maxBins=32, minInstancesPerNode=20, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
        'gbt': GBTClassifier(featuresCol='features', labelCol='label', maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, lossType='logistic', maxIter=100, stepSize=0.1, seed=None, subsamplingRate=0.7),
    }[model_name]


def smart_fit(model_name, df_tr):
    '''

    :param model_name:
    :return:
    '''

    from churn.analysis.poc_segments.Metadata import FEATS, NON_INFO_COLS


    categorical_columns = [col_ for col_, type_ in df_tr.dtypes if type_=="string" and col_ not in NON_INFO_COLS]
    print("Detected {} categoricals: {}".format(len(categorical_columns), ",".join(categorical_columns)))

    stages, new_categorical_inputs = encoding_categoricals(categorical_columns, stages_=[])

    numeric_columns = list(set(FEATS) - set(categorical_columns) - set(NON_INFO_COLS))
    assembler_inputs = new_categorical_inputs + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    dt = get_model(model_name)
    stages += [dt]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(df_tr)

    return pipeline_model



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
    feat_importance = getOrderedRelevantFeats(model, feat_cols, 'f')

    ii = 1
    for fimp in feat_importance:
        print "[{:>3}] {} : {}".format(ii, fimp[0], fimp[1])
        if not top or (top != None and ii < top):
            ii = ii + 1
        else:
            break


    return feat_importance


def get_score(model, df, add_randn=True):
    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    # Train evaluation
    df_preds = model.transform(df).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))

    if add_randn:
        df_preds = df_preds.withColumn('model_score', col('model_score') + lit(0.000001)*randn())

    return df_preds


def get_metrics(df_preds, title="", nb_deciles=10, score_col="model_score", label_col="label", refprevalence=None):

    preds_and_labels = df_preds.select([score_col, label_col]).rdd.map(lambda r: (r[score_col], float(r[label_col])))

    my_metrics = BinaryClassificationMetrics(preds_and_labels)

    print("METRICS FOR {}".format(title))
    print("\t AUC = {}".format(my_metrics.areaUnderROC))

    if nb_deciles:
        from pykhaos.modeling.model_performance import get_lift
        lift = get_lift(df_preds, score_col, label_col, nb_deciles, refprevalence)

        padding_res = len(str(len(lift)))

        for d, l in lift:
            print "\t[{:>{padding_res}}] : {}".format(int(d),l, padding_res=padding_res)

        return lift

    return None


