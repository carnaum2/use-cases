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
                        rand,
                        countDistinct,
                        variance)
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder, QuantileDiscretizer, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import IsotonicRegression
import math
import operator
import numpy as np

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

def get_lift(dt, score_col, label_col, nile = 40):
    print "[Info get_lift] Computing lift"
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

def getCalibrationFunction(spark, model, valdf, labelcol, numpoints = 10):
    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    valpredsdf = model.transform(valdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType())).select(['model_score', labelcol])
    minprob = valpredsdf.select(sql_min('model_score').alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max('model_score').alias('max')).rdd.collect()[0]['max']
    delta = float(maxprob - minprob)/float(numpoints)
    splits = list([-float("inf")] + [(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))] + [float("inf")])
    midpoints = {float(i): ((minprob + 0.5*delta) + i*delta) for i in list(np.arange(0,numpoints,step=1))}
    # model_score_discrete: from 0 to numpoints - 1
    bucketizer_prob = Bucketizer(splits=splits, inputCol='model_score', outputCol= 'model_score_discrete')
    # dictionary --> bucket: percentage of samples with label=1
    freqprob = bucketizer_prob.transform(valpredsdf).groupBy('model_score_discrete').agg(sql_avg(labelcol)).rdd.collectAsMap()
    for k in np.arange(0, numpoints):
        if not(k in freqprob):
            freqprob[k] = 0
    # samplepoints --> examples of the type: model_output (bin mid), proportion of target 1
    for (k, v) in freqprob.items():
        print "[Info Calibration] freqprob: key = " + str(k) + " - value = " + str(v)
    for (k, v) in midpoints.items():
        print "[Info Calibration] midpoints: key = " + str(k) + " - value = " + str(v)
    samplepoints = [(float(midpoints[i]), float(freqprob[i])) for i in list(freqprob.keys())]
    for pair in samplepoints:
        print "[Info Calibration] " + str(pair[0]) + ", " + str(pair[1])
    sampledf = spark.createDataFrame(samplepoints, ['model_score', 'target_prob'])
    sampledf.show()
    ir = IsotonicRegression(featuresCol='model_score', labelCol='target_prob', predictionCol='calib_model_score')
    irmodel = ir.fit(sampledf)
    return (irmodel, samplepoints)

def get_calibration_function2(spark, model, valdf, labelcol, numpoints = 10):
    import numpy as np
    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    valpredsdf = model.transform(valdf).withColumn("model_score", getScore(col("probability")).cast(DoubleType())).select(['model_score', labelcol])
    minprob = valpredsdf.select(sql_min('model_score').alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max('model_score').alias('max')).rdd.collect()[0]['max']
    numlabels = float(valpredsdf.filter(col(labelcol) == 1.0).count())
    print "[Info Calibration] Training - Num samples in the target class " + str(numlabels)
    delta = float(maxprob - minprob)/float(numpoints)
    ths = list([(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))])
    samplepoints = [(float(i), float(valpredsdf.filter((col('model_score') <= i) & (col(labelcol) == 1.0)).count())/numlabels) for i in ths]
    # samplepoints = [(float(i), valpredsdf.filter(col('model_score') <= i &).select(sql_avg(labelcol).alias('rate')).rdd.collect()[0]['rate']) for i in ths]
    for pair in samplepoints:
        print "[Info Calibration] Training " + str(pair[0]) + ", " + str(pair[1])
    sampledf = spark.createDataFrame(samplepoints, ['model_score', 'target_prob'])
    sampledf.show()
    print "[Info Calibration] Samples to implement the calibration model showed above"
    ir = IsotonicRegression(featuresCol='model_score', labelCol='target_prob', predictionCol='calib_model_score')
    irmodel = ir.fit(sampledf)
    return (irmodel, samplepoints)

def get_calibration_function_vec(spark, model, valdf, labelcol, numpoints = 10, num = 0):
    import numpy as np
    score_name = "model_score_" + str(num)
    score_calib_name ='calib_model_score_' + str(num)
    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    valpredsdf = model.transform(valdf).withColumn(score_name, getScore(col("probability")).cast(DoubleType())).select([score_name, labelcol])
    minprob = valpredsdf.select(sql_min(score_name).alias('min')).rdd.collect()[0]['min']
    maxprob = valpredsdf.select(sql_max(score_name).alias('max')).rdd.collect()[0]['max']
    numlabels = float(valpredsdf.filter(col(labelcol) == 1.0).count())
    print "[Info Calibration] Training - Num samples in the target class " + str(numlabels)
    delta = float(maxprob - minprob)/float(numpoints)
    ths = list([(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))])
    samplepoints = [(float(i), float(valpredsdf.filter((col(score_name) <= i) & (col(labelcol) == 1.0)).count())/numlabels) for i in ths]
    # samplepoints = [(float(i), valpredsdf.filter(col('model_score') <= i &).select(sql_avg(labelcol).alias('rate')).rdd.collect()[0]['rate']) for i in ths]
    for pair in samplepoints:
        print "[Info Calibration] Training " + str(pair[0]) + ", " + str(pair[1])
    sampledf = spark.createDataFrame(samplepoints, [score_name, 'target_prob'])
    sampledf.show()
    print "[Info Calibration] Samples to implement the calibration model showed above"
    ir = IsotonicRegression(featuresCol=score_name, labelCol='target_prob', predictionCol=score_calib_name)
    irmodel = ir.fit(sampledf)
    return (irmodel, samplepoints)

def validate_calibration2(df, samplepoints, labelcol):
    print "[Info Calibration] Starting the validation process for the calibration"
    ths = list([pair[0] for pair in samplepoints])
    numlabels = float(df.filter(col(labelcol) == 1.0).count())
    caliboutput = [(float(i), float(df.filter((col('calib_model_score') <= i) & (col(labelcol) == 1.0)).count())/numlabels) for i in ths]
    for pair in caliboutput:
        print "[Info Calibration] Validation " + str(pair[0]) + ", " + str(pair[1])
    return None

def validateCalibration(df, samplepoints, labelcol, numpoints = 10):
    minprob = df.select(sql_min('calib_model_score').alias('min')).rdd.collect()[0]['min']
    maxprob = df.select(sql_max('calib_model_score').alias('max')).rdd.collect()[0]['max']
    delta = float(maxprob - minprob)/float(numpoints)
    splits = list([-float("inf")] + [(minprob + i*delta) for i in list(np.arange(1,numpoints,step=1))] + [float("inf")])
    midpoints = {float(i): ((minprob + 0.5*delta) + i*delta) for i in list(np.arange(0,numpoints,step=1))}
    # model_score_discrete: from 0 to numpoints - 1
    bucketizer_prob = Bucketizer(splits=splits, inputCol='calib_model_score', outputCol= 'calib_model_score_discrete')
    # dictionary --> bucket: percentage of samples with label=1
    freqprob = bucketizer_prob.transform(df).groupBy('calib_model_score_discrete').agg(sql_avg(labelcol)).rdd.collectAsMap()
    for k in np.arange(0, numpoints):
        if not(k in freqprob):
            freqprob[k] = 0
    # samplepoints --> examples of the type: model_output (bin mid), proportion of target 1
    for (k, v) in freqprob.items():
        print "[Info Calibration] freqprob validation: key = " + str(k) + " - value = " + str(v)
    for (k, v) in midpoints.items():
        print "[Info Calibration] midpoints validation: key = " + str(k) + " - value = " + str(v)
    samplepoints = [(float(midpoints[i]), float(freqprob[i])) for i in list(freqprob.keys())]
    for pair in samplepoints:
        print "[Info Calibration] samplepints validation " + str(pair[0]) + ", " + str(pair[1])
    #sampledf = spark.createDataFrame(samplepoints, ['calib_model_score', 'target_prob'])
    #sampledf.show()
    return None