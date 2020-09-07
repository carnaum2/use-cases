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

def getNonInfFeats(df, feats):
    return getConstantVariables(df, feats)

def getNumberOfBins(df, feat, n, maxfeat, minfeat):
    # FreedmanDiaconis rule
    quartiles = df.approxQuantile(feat, [0.25, 0.75], 0.0)
    iqr = quartiles[1] - quartiles[0]
    b = 2*iqr/(n**(1.0/3.0))
    nbins = math.floor((maxfeat - minfeat)/b) if ((b > 0) & (maxfeat > minfeat)) else 0
    splits = list([-float("inf")] + [minfeat + i*b for i in list(np.arange(1,nbins,step=1))] + [float("inf")]) if (nbins > 0) else [0]
    # print "b: " + str(b) + " - iqr: " + str(iqr) + " - nbins: " + str(nbins) + " - len(splits): " + str(len(splits))
    return splits

def getNumberOfBins2(df, feat):
    # FreedmanDiaconis rule
    quartiles = df.approxQuantile(feat, [0.05, 0.95], 0.001)
    iqr = quartiles[1] - quartiles[0]
    #b = 2*iqr/(n**(1.0/3.0))
    nbins = 25.0
    b = iqr/nbins
    #print "[Info NumberOfBins] numberofbins = " + str(nbins) + " - binsize = " + str(b) + " - iqr[0] = " + str(iqr[0]) + " - iqr[1] = " + str(iqr[1])
    #nbins = math.floor((maxfeat - minfeat)/b) if ((b > 0) & (maxfeat > minfeat)) else 0
    splits = list([-float("inf")] + [(quartiles[0] + i*b) for i in list(np.arange(1,int(nbins),step=1))] + [float("inf")]) if (iqr > 0) else [0]
    # print "b: " + str(b) + " - iqr: " + str(iqr) + " - nbins: " + str(nbins) + " - len(splits): " + str(len(splits))
    return splits

def getFeatEntropy(df, feat, n, maxfeat, minfeat):
    tmpdf = df.select(col(feat).cast(DoubleType())).withColumn('unif', lit(minfeat) + (lit(maxfeat) - lit(minfeat))*rand())
    #splits = getNumberOfBins(tmpdf, feat, n, maxfeat, minfeat)
    splits = getNumberOfBins2(tmpdf, feat)
    print "[Info Entropy] computing entropy: feat = " + str(feat) + " - numberofbins = " + str((len(splits) - 1))
    relent = 0.0
    if(len(splits) > 1):
        bucketizer_feat = Bucketizer(splits=splits, inputCol=feat, outputCol=feat + '_discrete')
        # Transform original data into its bucket index
        freqfeat = bucketizer_feat.transform(tmpdf).groupBy(feat + '_discrete').agg(count('*')).rdd.collectAsMap()
        #qdsfeat = QuantileDiscretizer(numBuckets = nbins, inputCol = feat, outputCol = feat + '_discrete', relativeError = 0.0, handleInvalid = "error")
        #freqfeat = qdsfeat.fit(tmpdf).transform(tmpdf).groupBy(feat + '_discrete').agg(sql_avg(feat + '_discrete')).rdd.collectAsMap()
        relfreqfeat = [float(v)/float(n) for (k,v) in freqfeat.iteritems()]
        featent = getEntropy(relfreqfeat)
        bucketizer_unif = Bucketizer(splits=splits, inputCol='unif', outputCol='unif_discrete')
        # Transform original data into its bucket index
        frequnif = bucketizer_unif.transform(tmpdf).groupBy('unif_discrete').agg(count('*')).rdd.collectAsMap()
        #qdsunif = QuantileDiscretizer(numBuckets = nbins, inputCol = 'unif', outputCol = 'unif_discrete', relativeError = 0.0, handleInvalid = "error")
        #frequnif = qdsunif.fit(tmpdf).transform(tmpdf).groupBy('unif_discrete').agg(sql_avg('unif_discrete')).rdd.collectAsMap()
        relfrequnif = [float(v)/float(n) for (k,v) in frequnif.iteritems()]
        unifent = getEntropy(relfrequnif)
        relent = 0.0 if (unifent==0) else featent/unifent
        # tmpdf.show()
        # print "Entropy feat: " + str(featent) + " - Entropy unif: " + str(unifent)
        # Ideally, in the interval [0, 1]
        # 0: no entropy, no inf
        # 1: maximum entropy (as the uniform distrib)
    return relent

def getFeatEntropy2(df, feat):
    tmpdf = df.select(col(feat).cast(DoubleType()))
    #splits = getNumberOfBins(tmpdf, feat, n, maxfeat, minfeat)
    splits = getNumberOfBins2(tmpdf, feat)
    print "[Info Entropy] computing entropy: feat = " + str(feat) + " - numberofbins = " + str((len(splits) - 1))
    relent = 0.0
    nbins = len(splits)
    if(nbins > 1):
        # Entropy of the feat (the problem relies on the estimation of the pdf, i.e., histogram)
        sample = tmpdf.rdd.map(lambda r: r[feat])
        # n_samples = sample.count()
        freqfeat = sample.histogram(splits)[1]
        n_samples = sum(freqfeat)
        relfreqfeat = [float(c)/float(n_samples) for c in freqfeat]
        featent = getEntropy(relfreqfeat)
        # Entropy of the uniform variable (taken as reference)
        relfrequnif = [1.0/float(nbins)]*int(nbins)
        unifent = getEntropy(relfrequnif)
        relent = 0.0 if (unifent==0) else featent/unifent

    print "[Info Entropy] computing entropy: feat = " + str(feat) + " - numberofbins = " + str((len(splits) - 1)) + " - relent = " + str(relent)   
    return relent

def getEntropy(pdf):
    return sum([-x*math.log(x) if x > 0 else 0.0 for x in pdf])

def getFeatEntropyFromDf(df, feats):
    df.cache()
    # n = df.count()
    #feat_stats = {f: (df.select(sql_max(f)), df.select(sql_min(f))) for f in feats}
    #feat_ent = {f: getFeatEntropy(df.select(f), f, n, df.select(sql_max(f).alias('max')).rdd.collect()[0]['max'], df.select(sql_min(f).alias('min')).rdd.collect()[0]['min']) for f in feats}
    feat_ent = {f: getFeatEntropy2(df.select(f), f) for f in feats}
    return sorted(feat_ent.items(), key=operator.itemgetter(1), reverse=True)

def getConstantVariables(df, feats):

    # Create a function with two arguments, listDiv and n (number of divisions):
    def divLista(listDiv, n):
        # For item i in a range that is a length of listDiv,
        for i in range(0, len(listDiv), n):
            # Create an index range for listDiv of n items:
            yield listDiv[i:i + n]

    numerical_feats_group = list(divLista(feats, 200))

    cte_vars_total = []
    for nn, feats in enumerate(numerical_feats_group):
        print "[Info FbbChurn] " + str(nn + 1) + "/" + str(len(numerical_feats_group)) + " Selecting the features that add information to the model"

        vars_list = [variance(f).alias(f) for f in feats]
        f_vars = df.select(vars_list).rdd.collect()[0].asDict()
        cte_vars = [f for f in list(f_vars.keys()) if f_vars[f] == 0]

        cte_vars_total.extend(cte_vars)
        #print(len(cte_vars_total))
    return cte_vars_total

    # return [f for f in feats if df.select(countDistinct(f).alias('numdist')).rdd.collect()[0]['numdist']==1]
   # vars_list = [variance(f).alias(f) for f in feats]
   # f_vars = df.select(vars_list).rdd.collect()[0].asDict()
   # cte_vars = [f for f in list(f_vars.keys()) if f_vars[f]==0]
   # return cte_vars
