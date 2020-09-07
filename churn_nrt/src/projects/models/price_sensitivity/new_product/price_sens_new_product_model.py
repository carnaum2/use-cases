#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from common.src.main.python.utils.hdfs_generic import *
import sys, os
import time
from pyspark.sql.functions import (
                                    col, concat, lpad, max as sql_max,
                                    when,
                                    lit)


import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


def __get_last_date(spark):

    last_date = spark\
    .read\
    .parquet('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/')\
    .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
    .select(sql_max(col('mydate')).alias('last_date'))\
    .rdd\
    .first()['last_date']

    return last_date


if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - algorithm: algorithm for training
    #      - mode_ : evaluation or prediction
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    from churn_nrt.src.projects.models.price_sensitivity.new_product.metadata import METADATA_STANDARD_MODULES, get_metadata
    from churn_nrt.src.utils.constants import MODE_PRODUCTION, MODE_EVALUATION
    from churn_nrt.src.projects.models.price_sensitivity.new_product.model_classes import BASE_COMP, BASE_EVAL
    from churn_nrt.src.projects.models.price_sensitivity.new_product.constants import OWNER_LOGIN, MODEL_OUTPUT_NAME_GLOBAL_POTENTIAL_CLASSIF, MODEL_OUTPUT_NAME_HIGH_POTENTIAL, EXTRA_INFO_COLS, INSERT_TOP_K
    from churn_nrt.src.projects.models.price_sensitivity.new_product.model_classes import NewProductModel, BASE_COMP, BASE_EVAL
    from churn_nrt.src.projects_utils.models.modeler import get_metrics_regression

    import argparse

    parser = argparse.ArgumentParser(
        description="Run price sensitivity model - new product",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in test')

    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf",
                        help='model to be used for training de model')

    parser.add_argument('--sources', metavar='<YYYYMMDD>', type=str, required=False, default=METADATA_STANDARD_MODULES,
                        help='list of sources to be used for building the training dataset')

    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--new_product', metavar='<new product>', type=str, required=False, default=None, help='product')

    parser.add_argument('--horizon_comp', metavar='<horizon in cycles>', type=int, required=False, default=6, help='number of days for horizon')
    parser.add_argument('--horizon_check', metavar='<horizon in cycles>', type=int, required=False, default=24, help='number of days for horizon')

    parser.add_argument('--filter_correlated_feats', metavar='<filter_correlated_feats>', type=int, required=False, default=0, help='Filter correlated feats')
    parser.add_argument('--day_to_insert', metavar='<day_to_insert>', type=int, required=False, default=4, help='day to insert in model outputs')
    parser.add_argument('--base', metavar='eval/comp', type=str, required=False, default="eval", help='eval: test set build as train set; dist: the whole base is returned')
    parser.add_argument('--label_type', metavar='muchos', type=float, required=True, default=1, help='1: label is diff of bills; 2: label is rel inc of bills')
    parser.add_argument('--balance_tr_df', metavar='<balance_tr_df>', type=int, required=False, default=0, help='balance tr df')
    parser.add_argument('--do_calibrate_scores', metavar='<do_calibrate_scores>', type=int, required=False, default=0, help='calibrate scores or not')
    parser.add_argument('--purpose', metavar='<modeling vs data_generation>', type=str, required=False, default="modeling", help='modeling vs data_generation')

    #parser.add_argument('--version', metavar='<version>', type=int, required=False, default=3, help='version of tr set')



    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    model_ = args.model
    horizon_comp = args.horizon_comp
    horizon_check = args.horizon_check
    new_product = args.new_product
    base = args.base
    label_type = args.label_type
    balance_tr_df = True #if args.balance_tr_df == 1 else False
    do_calibrate_scores = args.do_calibrate_scores
    purpose = args.purpose

    if base not in [BASE_COMP, BASE_EVAL]:
        print("Unknown value for base parameter: {}. Valid values are {} and {}".format(base, BASE_EVAL, BASE_COMP))
        import sys
        sys.exit(1)

    #version = args.version

    metadata_sources = args.sources.split(",") if args.sources and isinstance(args.sources, str) else METADATA_STANDARD_MODULES
    mode_ = args.mode if args.mode else "evaluation"
    print(type(args.filter_correlated_feats), args.filter_correlated_feats)
    day_to_insert = args.day_to_insert
    filter_correlated_feats = True if args.filter_correlated_feats == 1 else False

    if mode_ == MODE_PRODUCTION:

        if metadata_sources[0] == "pricesens_newprod11" and label_type in [7.05, 7.10, 8.05, 8.10]:
            MODEL_OUTPUT_NAME = MODEL_OUTPUT_NAME_HIGH_POTENTIAL
        elif metadata_sources[0] == "pricesens_newprod10" and label_type in [3,4]:
            MODEL_OUTPUT_NAME = MODEL_OUTPUT_NAME_GLOBAL_POTENTIAL_CLASSIF
        else:
            print("Metadata sources {} with label_type {} is not supported for production mode".format(metadata_sources[0], label_type))
            import sys
            sys.exit(1)

        print("MODEL_OUTPUT_NAME={}".format(MODEL_OUTPUT_NAME))

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}' new_product={} horizon_comp={} horizon_check={} filter_correlated_feats={} "
          "day_to_insert={} do_calibrate_scores={} purpose={}".format(tr_date_,
                                                          tt_date_,
                                                          model_,
                                                          ",".join(metadata_sources),
                                                          new_product,
                                                          horizon_comp,
                                                          horizon_check,
                                                          filter_correlated_feats,
                                                          day_to_insert, do_calibrate_scores, purpose
                                                          ))


    if mode_ == MODE_EVALUATION and base == BASE_COMP:
        print("We cannot evaluate using a complete base. Program will exit now")
        import sys
        sys.exit(1)


    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("price_sens_new_product")

    # sc, spark, sql_context = get_spark_session("price_sens_new_product")
    sc.setLogLevel('WARN')

    print("Starting check of input params")

    from churn_nrt.src.data_utils.Metadata import Metadata
    metadata_obj =  Metadata(spark, get_metadata, ["num_cliente"], metadata_sources)

    if not tt_date_:
        if mode_==MODE_PRODUCTION:
            print("Not introduced a test date. Computing automatically...")
            tt_date_ = str(__get_last_date(spark))
            print("Computed tt_date_={}".format(tt_date_))
        else:
            print("Mode {} does not support empty --tt_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass


    if not tr_date_:
        if mode_ == MODE_PRODUCTION:
            print("Not introduced a training date. Computing automatically")
            from churn_nrt.src.utils.date_functions import move_date_n_cycles

            tr_date_ = move_date_n_cycles(tt_date_, n=-4) # avoid overlapping between train and test
            print("Computed training date from test_date ({}) --> {}".format(tt_date_, tr_date_))
        else:
            print("Mode {} does not support empty --tr_date. Program will stop here!".format(mode_))
            sys.exit()
    else:
        # TODO check something?
        pass

    print("ARGS tr_date='{}' tt_date='{}' model='{}' metadata_set='{}'".format(tr_date_, tt_date_, model_, ",".join(metadata_sources)))

    #################################################


    def get_model(model_name, label_col="label", featuresCol="features"):
        from pyspark.ml.regression import GBTRegressor, RandomForestRegressor, GeneralizedLinearRegression
        from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

        return {
            'gbt_reg': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=100),
            'gbt_reg2': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=3, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=50),
            'gbt_reg3': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=2, maxBins=32, minInstancesPerNode=15, minInfoGain=0.0, maxIter=25),
            'gbt_reg4': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=2, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=25),
            'gbt_reg5': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=10, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=200),
            'gbt_reg6': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=10, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=100),
            "rf_reg" : RandomForestRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=5, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, featureSubsetStrategy="auto"),
            "rf_reg2": RandomForestRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=3, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, impurity="variance", subsamplingRate=1.0, seed=None, numTrees=20, featureSubsetStrategy="auto"),
            "glr_reg" : GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=10, regParam=0.3),
            "glr_reg2": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=0.3),
            "glr_reg3": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=0.9),
            "glr_reg4": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=5),
            "glr_reg5": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=0.9),
            "glr_reg6": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=10),
            "glr_reg7": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=25),
            "glr_reg8": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=50),
            "glr_reg9": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gaussian", link="identity", maxIter=1000, regParam=100),
            "glr_reg10": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=10),
            "glr_tw1": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="tweedie", maxIter=1000, regParam=10, variancePower=1),
            "glr_tw2": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="tweedie", maxIter=1000, regParam=10, variancePower=1.5),
            "glr_tw3": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="tweedie", maxIter=100, variancePower=1),
            "glr_tw4": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="tweedie", maxIter=100, regParam=5, variancePower=1),
            "glr_reg11": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=10),
            "glr_reg12": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=500, regParam=10),
            "glr_reg13": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=500, regParam=1),
            "glr_reg14": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=500, regParam=100),
            "glr_reg15": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=100, regParam=0.1),
            "glr_tw5": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="tweedie", maxIter=100, regParam=0.5, variancePower=1),
            "glr_reg16": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="identity", maxIter=100, regParam=0.1),
            "glr_reg17": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="identity", maxIter=100, regParam=1),
            "glr_reg18": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="identity", maxIter=100, regParam=10),
            "glr_reg19": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="identity", maxIter=100, regParam=100),
            "glr_reg20": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=100, regParam=0.01),
            "glr_reg21": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", link="log", maxIter=100, regParam=0.5),
            "glr_reg22": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=25),
            "glr_reg23": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=100),
            "glr_reg24": GeneralizedLinearRegression(featuresCol=featuresCol, labelCol=label_col, family="gamma", maxIter=1000, regParam=5),
            "rf_reg3": RandomForestRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=15, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, impurity="variance", subsamplingRate=0.7, seed=None, numTrees=30, featureSubsetStrategy="auto"),
            "rf_reg4": RandomForestRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=25, maxBins=64, minInstancesPerNode=5, minInfoGain=0.0, impurity="variance", subsamplingRate=0.7, seed=None, numTrees=50, featureSubsetStrategy="auto"),
            'gbt_reg7': GBTRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=6, maxBins=32, minInstancesPerNode=10, minInfoGain=0.0, maxIter=100),
            "rf_reg5": RandomForestRegressor(featuresCol=featuresCol, labelCol=label_col, maxDepth=25, maxBins=64, minInstancesPerNode=5, minInfoGain=0.0, impurity="variance", subsamplingRate=0.7, seed=None, numTrees=150, featureSubsetStrategy="auto"),
            'rf_class': RandomForestClassifier(featuresCol=featuresCol, numTrees=800, maxDepth=10, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=100, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'rf_class2': RandomForestClassifier(featuresCol=featuresCol, numTrees=1800, maxDepth=15, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=100, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'rf_class3': RandomForestClassifier(featuresCol=featuresCol, numTrees=1800, maxDepth=15, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=10, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'rf_class4': RandomForestClassifier(featuresCol=featuresCol, numTrees=1000, maxDepth=15, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=50, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7),
            'rf_class5': RandomForestClassifier(featuresCol=featuresCol, numTrees=1800, maxDepth=15, labelCol=label_col, seed=1234, maxBins=32, minInstancesPerNode=25, impurity='gini', featureSubsetStrategy='sqrt', subsamplingRate=0.7)

        }[model_name]

    model_obj = get_model(model_, featuresCol="features", label_col="label")

    # Check consistency between model chosen and label type
    from churn_nrt.src.projects_utils.models.modeler import isClassificationProblem

    if isClassificationProblem(model_obj) and not (label_type in [3,4,5, 6] or int(label_type)==7 or int(label_type)==8):
        print("label_type {} cannot be used in a classification problem with model {}".format(label_type, model_))
        import sys
        sys.exit(1)
    if not isClassificationProblem(model_obj) and not label_type in [1,2]:
        print("label_type {} cannot be used in a regression problem with model {}".format(label_type, model_))
        import sys
        sys.exit(1)



    if purpose == "data_generation":

        print("__CSANC109__Purpose_DATA_GENERATION")
        from churn_nrt.src.utils.date_functions import move_date_n_cycles
        from churn_nrt.src.projects.models.price_sensitivity.new_product.model_classes import NewProductData
        from churn_nrt.src.projects.models.price_sensitivity.new_product.metadata import get_version

        closing_day = tr_date_
        version = get_version(metadata_obj.METADATA_SOURCES)
        while closing_day <= tt_date_:
            start_time_iter = time.time()
            print("__CSANC109__NewProductData__{}".format(closing_day))

            try:
                df_tr_newprod = NewProductData(spark, new_product, horizon_comp, horizon_check, version=version, base=base).get_module(closing_day,
                                                                                                                           force_gen=False,
                                                                                                                           save_others=False, save=True if version != 101 else False,
                                                                                                                           sources=metadata_obj.METADATA_SOURCES)
                df_tr_newprod = df_tr_newprod.cache()
                print("__CSANC109__NewProductData__{}__{}".format(closing_day, df_tr_newprod.count()))
                print("__CSANC109__NewProductData__{}_Ended_{} minutes".format(closing_day, (time.time() - start_time_iter)/60.))


            except Exception as e:
                print("__CSANC109 Except")
                print(e)
                print("__CSANC109 Except_end")
            except:
                print("__CSANC109 Except")

            closing_day = move_date_n_cycles(closing_day, n=+1)



        sys.exit(0)

    elif purpose == "pattern":
        print("__CSANC109__PURPOSE_PATTERN")
        from churn_nrt.src.projects_utils.models.ModelTemplate import SET_NAME_TEST, SET_NAME_TRAIN
        model_ps_newproduct = NewProductModel(spark, tr_date_, mode_, model_obj, OWNER_LOGIN, metadata_obj, new_product, horizon_comp,
                                            horizon_check, base=base, label_type=label_type, force_gen=False)
        feats_cols =  metadata_obj.get_cols('all', filter_correlated_feats=filter_correlated_feats)
        df_train = model_ps_newproduct.get_set(tr_date_, labeled=True, feats_cols=feats_cols, set_name=SET_NAME_TRAIN)


        import datetime as dt
        timestamp_ = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        pref_path = "" if label_type == 1 else "lab{}".format(label_type)

        if mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
            path_to_save = "/user/csanc109/projects/churn/data/price_sens_new_product_{}_{}_{}/df_train_{}_{}".format("pattern", horizon_comp, horizon_check, tr_date_, timestamp_)
            print("About to save test df_train - {}".format(path_to_save))
            df_train.write.mode('overwrite').save(path_to_save)

        print("__CSANC109__MODEL_run_training")
        df_tr_preds = model_ps_newproduct.run_training(df_train, do_calibrate_scores=0, filter_correlated_feats=filter_correlated_feats, balance_tr_df=False, handle_invalid="error")

        pref_path = "" if label_type == 1 else "lab{}".format(label_type)
        if mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
            path_to_save = "/user/csanc109/projects/churn/data/price_sens_new_product_{}_{}_{}/df_train_preds_{}_{}".format("pattern", horizon_comp, horizon_check, tr_date_, timestamp_)
            print("About to save test df_tr_preds - {}".format(path_to_save))
            df_tr_preds.write.mode('overwrite').save(path_to_save)

        from churn_nrt.src.projects_utils.models.modeler import smart_fit, get_metrics, get_feats_imp
        imp_vars = get_feats_imp(model_ps_newproduct.PIPELINE_MODEL, model_ps_newproduct.ASSEMBLER_INPUTS)
        feats_importance_names = [col_name for col_name, importance in imp_vars]

        from churn_nrt.src.projects_utils.models.modeler_profiling import get_model_pattern
        pattern = get_model_pattern(spark, df_tr_preds, feats_importance_names, method="dt", score_col="model_score", top_ratio=0.10, threshold=5)
        print("__CSANC109__PATTERN")
        print(pattern)
        import sys
        sys.exit()



    model_ps_newproduct = NewProductModel(spark, tr_date_, mode_, model_obj, OWNER_LOGIN, metadata_obj, new_product, horizon_comp,
                                        horizon_check, base=base, label_type=label_type, force_gen=False)

    df_tr_preds, dict_tt_preds, df_val_preds = model_ps_newproduct.run(tt_date=[tt_date_], do_calibrate_scores=do_calibrate_scores, filter_correlated_feats=filter_correlated_feats,
                                                            balance_tr_df=balance_tr_df, handle_invalid="skip", model_name_storage="csanc109_testing_")
    df_tt_preds=dict_tt_preds[tt_date_]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # CORRECT PREDICTIONS < 0
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    from churn_nrt.src.projects.models.price_sensitivity.new_product.metadata import get_version

    version = get_version(metadata_sources)
    if mode_ == MODE_EVALUATION and label_type in [2] and version < 10:
        df_tt_preds = df_tt_preds.withColumn("scoring", when(col("scoring") < 0, 0).otherwise(col("scoring")))
        rmse_test_fixed, r2_test_fixed = get_metrics_regression(spark, df_tt_preds, title="TEST-fixed", score_col="scoring", label_col="label")
        model_ps_newproduct.MODEL_METRICS.create_metrics_regression("test_fixed_{}".format(tt_date_), df_tt_preds, rmse_test_fixed, "scoring", r2=r2_test_fixed)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Save df for further analysis (optional)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import time

    start_time_pricesens = time.time()
    import datetime as dt

    timestamp_ = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    pref_path = "" if label_type == 1 else "lab{}".format(label_type)

    if base == BASE_EVAL and mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
        path_to_save = "/user/csanc109/projects/churn/data/price_sens_new_product_{}_{}_{}/df_{}_{}_{}".format(new_product, horizon_comp, horizon_check, pref_path, tt_date_, timestamp_)
        print("About to save test df - {}".format(path_to_save))
        df_tt_preds.write.mode('overwrite').save(path_to_save)

    elif mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
        path_to_save = "/user/csanc109/projects/churn/data/price_sens_new_product_{}_{}_{}_dist/df_{}_{}_{}".format(new_product, horizon_comp, horizon_check, pref_path, tt_date_, timestamp_)
        print("About to save test df - {}".format(path_to_save))
        df_tt_preds.select("NUM_CLIENTE", "scoring").write.mode('overwrite').save(path_to_save)

        print("Ended saving test df - {} (elapsed time {} minutes)".format(path_to_save, (time.time() - start_time_pricesens) / 60.0))

    if df_val_preds is not None:
        print("df_val_preds is not None")
        if base == BASE_EVAL and mode_ != MODE_PRODUCTION and os.getenv('USER') == "csanc109":
            path_to_save = "/user/csanc109/projects/churn/data/price_sens_new_product_{}_{}_{}/df_val_{}_{}_{}".format(new_product, horizon_comp, horizon_check, pref_path, tt_date_, timestamp_)
            print("About to save test df_val - {}".format(path_to_save))
            df_val_preds.write.mode('overwrite').save(path_to_save)



    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # show summary
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    print("----MODEL PRICE SENS _ NEW PRODUCT SUMMARY----")
    model_ps_newproduct.print_summary() # print a summary (optional)
    print("----END MODEL PRICE SENS _ NEW PRODUCT SUMMARY----")





    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # In production mode, insert into model_outputs
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    if mode_ == MODE_PRODUCTION:

        perc_risk = 0.2
        if metadata_sources[0] == "pricesens_newprod11":
            perc_risk = 0.2
            pass
        elif metadata_sources[0] == "pricesens_newprod10":
            perc_risk = 0.0
            pass

        from churn_nrt.src.data_utils.model_outputs_manager import add_decile
        #df_tt_preds = add_decile(df_tt_preds, score="scoring", perc=perc_risk)

        model_ps_newproduct.insert_model_outputs(df_tt_preds, MODEL_OUTPUT_NAME, test_date=tt_date_,
                                        insert_top_k=INSERT_TOP_K,
                                        extra_info_cols=EXTRA_INFO_COLS,
                                        day_to_insert=day_to_insert)





    print("Process finished - Elapsed time: {} minutes ({} hours)".format( (time.time()-start_time_total)/60.0, (time.time()-start_time_total)/3600.0))