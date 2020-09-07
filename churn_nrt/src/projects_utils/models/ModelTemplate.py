import time
import datetime as dt
import pandas as pd
import sys

from pyspark.sql.functions import lit, col, when, coalesce, length, regexp_replace, split, concat_ws, avg as sql_avg, desc


from churn_nrt.src.projects_utils.models.Metrics import ModelMetrics
from churn_nrt.src.utils.exceptions import Exceptions
from churn_nrt.src.projects_utils.models.modeler import smart_fit, get_metrics, get_feats_imp, isClassificationProblem, get_metrics_regression,  get_score, get_multiclass_metrics
from churn_nrt.src.data_utils.model_outputs_manager import insert_to_model_scores, insert_to_model_parameters, ensure_types_model_scores_columns, MODEL_OUTPUTS_NULL_TAG, \
    build_extra_info_model_parameters

from churn_nrt.src.data_utils.model_outputs_manager import create_model_output_dfs, build_metrics_string
from churn_nrt.src.utils.constants import HDFS_PIPELINE_MODELS

SET_NAME_TRAIN = "train"
SET_NAME_TEST = "test"

TRAIN_METRICS_LABEL = "train"
TEST_METRICS_LABEL = "test"



class ModelTemplate:

    METADATA = None
    MODEL_OUTPUTS_NAME = None
    TR_DATE = None
    #TT_DATE = None
    MODE = None
    MODEL = None # could be a name (rf, gbt) or an object with the model object to be used
    SPARK = None
    OWNER_LOGIN = None
    LABEL_COL = "label"
    MODEL_METRICS = None

    ASSEMBLER_INPUTS = None
    PIPELINE_MODEL = None
    CALIB_MODEL = None

    FORCE_GEN = False

    def __init__(self, spark, tr_date, mode_, model_, owner_login, metadata_obj, force_gen=False):
        '''
        :param spark: spark context name
        :param tr_date: yyyymmdd for training the model
        :param mode_: prediction or evaluation
        :param algorithm: rf or gbt or a model object to be used to train
        :param owner_login: insert here the username of the owner of this model
        :param feats_cols: list of columns to be used as feats. Set to none to use all the columns that
        :param categorical_cols:
        :param non_info_cols:
        '''

        self.SPARK = spark
        self.METADATA = metadata_obj

        self.TR_DATE = tr_date
        #self.TT_DATE = tt_date
        self.MODE = mode_
        self.MODEL = model_
        self.OWNER_LOGIN = owner_login
        #self.EXTRA_INFO_COLS = []
        #self.NON_INFO_COLS = non_info_cols # FIXME: autodetect using metadata
        self.MODEL_METRICS = ModelMetrics()
        self.ASSEMBLER_INPUTS = None

        self.FORCE_GEN = force_gen

        print("[ModelTemplate] Initializing model...")
        print(repr(self))

    def set_label_col(self, label_col):
        print("[ModelTemplate] Setting label_col to '{}'".format(label_col))
        self.LABEL_COL = label_col

    def __repr__(self):
        return "[ModelTemplate] \n " + \
               "train_date={}\nmodel={}\n".format(self.TR_DATE, self.MODEL) + \
               "mode={}\nowner={}\n".format(self.MODE, self.OWNER_LOGIN) + \
               "label_col={}\n".format(self.LABEL_COL) + \
               "metadata_function={}\nsources={}\n".format(self.METADATA.get_metadata_function_abspath(), ",".join(self.METADATA.METADATA_SOURCES)) + \
               "Model Metrics:\n{}".format(self.MODEL_METRICS)

    def print_summary(self):
        print(self)

    def get_set(self, closing_day, labeled, feats_cols=None, set_name=None, *args, **kwargs):
        raise Exceptions.NOT_IMPLEMENTED

    # def get_filter_set(self, closing_day, labeled, feats_cols=None, *args, **kwargs):
    #     df_set = self.get_set(closing_day, labeled, feats_cols, *args, **kwargs)
    #     #print("[ModelTemplate] Before filter by segment {}".format(df_set.count()))
    #     #df_filt_set = self.filter_by_segment(df_set)
    #     #print("[ModelTemplate] After filter by segment {}".format(df_filt_set.count()))
    #     return df_set


    def get_performance_estimation(self, df_set, weights=None, seed=1234):

        print("[ModelTemplate] get_performance_estimation | Start ...")

        if not weights:
            weights = [0.5, 0.25, 0.25]

        [tr_input_df, df_val, df_test] = df_set.randomSplit(weights, seed)

        categorical_columns =  self.METADATA.get_cols(type_='categorical')

        feats_cols = self.METADATA.get_cols(type_='all')


        start_time_modeling = time.time()
        pipeline_model, assembler_inputs = smart_fit(self.MODEL, tr_input_df,
                                                     feats=feats_cols,  # use all columns in tr_input_df
                                                     non_info_cols=self.METADATA.get_non_info_cols(),
                                                     categorical_cols=categorical_columns,
                                                     label_col=self.LABEL_COL)
        # self.ASSEMBLER_INPUTS = assembler_inputs
        # self.PIPELINE_MODEL = pipeline_model
        feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

        print("[ModelTemplate] get_performance_estimation | Finished smart_fit |  elapsed = {} minutes]".format((time.time() - start_time_modeling) / 60.0))

        from churn_nrt.src.projects_utils.models.modeler import get_calibration_function2
        calib_model = get_calibration_function2(self.SPARK, pipeline_model, df_val, self.LABEL_COL, 10)
        predicted_tt_calib = calib_model[0].transform(df_test)
        auc_test, lift_test, lift_in_top_churn = get_metrics(self.SPARK, predicted_tt_calib, title=TEST_METRICS_LABEL, do_churn_rate_fix_step=False, score_col="calib_model_score", label_col=self.LABEL_COL)

        self.MODEL_METRICS.create_metrics("val", predicted_tt_calib, auc_test, "calib_model_score")


        return auc_test, lift_test, feat_importance_list




    def run(self, tt_date, do_calibrate_scores=0, filter_correlated_feats=False, balance_tr_df=False, handle_invalid="skip", model_name_storage=None):
        '''
        :param tt_date may be a string with a test date or a list of string to run the model for several dates
        :param do_calibrate_scores:
            0: no calibration
            1: standard calibration with IsotonicRegressor algorithm
            2: model predicts on val_df and returns the scores to allow model combination based on buckets
            'keep': puts unseen labels in a special additional bucket, at index numLabels
        :param handle_invalid:
            'error': throws an exception (which is the default)
            'skip': skips the rows containing the unseen labels entirely (removes the rows on the output!)
            'keep': puts unseen labels in a special additional bucket, at index numLabels
        :param model_name_storage If it is not None, then this name is used as preffix for storing the pipeline in hdfs

        :return:
        '''

        ##########################################################################################
        # 1. Check input data and init vars
        ##########################################################################################

        check_args(do_calibrate_scores)

        if isinstance(tt_date, str):
            tt_date = [tt_date]

        print("[ModelTemplate] run | run model for dates: {})".format(",".join(tt_date)))

        print("[ModelTemplate] run | {} do_calibrate={} filter_correlated_feats={} model_name_storage={}".format(repr(self), do_calibrate_scores,
                                                                                                                 filter_correlated_feats,
                                                                                                                 model_name_storage))

        start_time_total = time.time()

        print("[ModelTemplate] run | Columns for training {}".format("does not include correlated feats" if filter_correlated_feats else "include all columns in metadata"))
        #categorical_columns = self.METADATA.get_cols('categorical', filter_correlated_feats=filter_correlated_feats)
        feats_cols =  self.METADATA.get_cols('all', filter_correlated_feats=filter_correlated_feats)


        doing_classification = isClassificationProblem(self.MODEL)

        ##########################################################################################
        # 2. Loading tr data
        ##########################################################################################
        df_tr_all = self.get_set(self.TR_DATE, True, feats_cols, SET_NAME_TRAIN).cache()  # labeled

        if do_calibrate_scores == 1:
            [tr_input_df, df_val] = df_tr_all.randomSplit([0.8, 0.2], 1234)
            print("[ModelTemplate] run | Count training={} validation={}".format(tr_input_df.count(), df_val.count()))
        elif do_calibrate_scores == 2:
            [tr_input_df, df_val] = df_tr_all.randomSplit([0.7, 0.3], 1234)
            print("[ModelTemplate] run | Count training={} validation={}".format(tr_input_df.count(), df_val.count()))
        else:
            print("[ModelTemplate] run | Selected do not calibrate scores")
            tr_input_df = df_tr_all
            df_val = None

        start_time_modeling = time.time()

        # binary_classification = None (not apply) if we are doing regression
        # binary_classification = True if we are doing binary classification
        # binary_classification = False if we are doing multiclass classification
        binary_classification = None if not doing_classification else tr_input_df.select(self.LABEL_COL).distinct().count()==2



        df_tr_preds = self.run_training(tr_input_df, df_val=df_val, do_calibrate_scores=do_calibrate_scores,
                                        filter_correlated_feats=filter_correlated_feats, balance_tr_df=balance_tr_df, handle_invalid=handle_invalid, model_name_storage=model_name_storage)

        print("[ModelTemplate] run | Finished training model |  elapsed = {} minutes | total elapsed = {} minutes]".format((time.time() - start_time_modeling) / 60.0,
                                                                                                                (time.time() - start_time_total) / 60.0))




        df_val_preds = None
        if do_calibrate_scores == 2 and isClassificationProblem(self.MODEL):
            df_val_preds = get_score(self.PIPELINE_MODEL, df_val, self.CALIB_MODEL, score_col="model_score")


        tt_dfs = {}
        for test_date in tt_date:
            df_tt_preds = self.run_predict(test_date, do_calibrate_scores=do_calibrate_scores, filter_correlated_feats=filter_correlated_feats, binary_classification=binary_classification)
            tt_dfs[test_date] = df_tt_preds

        print("[ModelTemplate] Finished | total elapsed = {} hours]".format((time.time() - start_time_total) / 3600.0))

        return df_tr_preds, tt_dfs, df_val_preds


    def run_training(self, tr_input_df,  df_val=None, do_calibrate_scores=0, filter_correlated_feats=False, balance_tr_df=False,handle_invalid="error", model_name_storage=None):
        '''

        :param tr_input_df: dataset to train the model
        :param filter_correlated_feats:
        :param balance_tr_df:
        :param do_calibrate_scores:
            0: no calibration
            1: standard calibration with IsotonicRegressor algorithm
            2: model predicts on val_df and returns the scores to allow model combination based on buckets
            'keep': puts unseen labels in a special additional bucket, at index numLabels
        :param handle_invalid:
            'error': throws an exception (which is the default)
            'skip': skips the rows containing the unseen labels entirely (removes the rows on the output!)
            'keep': puts unseen labels in a special additional bucket, at index numLabels
        :param model_name_storage If it is not None, then this name is used as preffix for storing the pipeline in hdfs

        :return:
        '''


        doing_classification = isClassificationProblem(self.MODEL)

        # binary_classification = None (not apply) if we are doing regression
        # binary_classification = True if we are doing binary classification
        # binary_classification = False if we are doing multiclass classification
        binary_classification = None if not doing_classification else tr_input_df.select(self.LABEL_COL).distinct().count()==2

        if doing_classification and not binary_classification and do_calibrate_scores != 0:
            print("[ModelTemplate] run | Ooops! Multiclass classification does not support calibration. Please, set do_calibrate_scores=0 and relaunch run method")
            sys.exit(1)


        print("[ModelTemplate] run | Columns for training {}".format("does not include correlated feats" if filter_correlated_feats else "include all columns in metadata"))
        categorical_columns = self.METADATA.get_cols('categorical', filter_correlated_feats=filter_correlated_feats)
        feats_cols =  self.METADATA.get_cols('all', filter_correlated_feats=filter_correlated_feats)


        ##########################################################################################
        # 3. Modeling
        ##########################################################################################

        print("[ModelTemplate] run | About to call smart_fit with label_col={}".format(self.LABEL_COL))

        if balance_tr_df:
            from churn_nrt.src.projects_utils.models.modeler import balance_df
            df_tr = balance_df(tr_input_df)
        else:
            df_tr = tr_input_df

        pipeline_model, assembler_inputs = smart_fit(self.MODEL, df_tr,
                                                     feats=feats_cols,
                                                     non_info_cols=self.METADATA.get_non_info_cols(),
                                                     categorical_cols=categorical_columns,
                                                     label_col=self.LABEL_COL,
                                                     #balance_tr_df=balance_tr_df,
                                                     handle_invalid=handle_invalid)

        self.ASSEMBLER_INPUTS = assembler_inputs
        self.PIPELINE_MODEL = pipeline_model

        if model_name_storage:
            try:
                start_time_model = time.time()
                timestamp_ = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                model_path = HDFS_PIPELINE_MODELS+model_name_storage+"_"+timestamp_
                print("[ModelTemplate] run | pipeline_model will be saved in path {}".format(model_path))
                self.PIPELINE_MODEL.save(model_path)
                print("[ModelTemplate] run | Saved pipeline_model in path {} - Elapsed time: {} minutes".format(model_path, (time.time() - start_time_model)/60.0))

            except:
                print("[ERROR] [ModelTemplate] run | Problems saving the pipeline model")
        else:
            print("[ModelTemplate] run | pipeline_model will not be saved")

        # Print importance feats
        _ = get_feats_imp(pipeline_model, assembler_inputs)



        ##########################################################################################
        # 4. Calibration
        ##########################################################################################
        if do_calibrate_scores == 1 and isClassificationProblem(self.MODEL):
            print("[ModelTemplate] run | Selected calibrate scores. Computing calib model")
            from churn_nrt.src.projects_utils.models.modeler import get_calibration_function2
            #FIXME rename function
            self.CALIB_MODEL = get_calibration_function2(self.SPARK, self.PIPELINE_MODEL, df_val, self.LABEL_COL, 10)



        ##########################################################################################
        # 6. Evaluation
        ##########################################################################################

        start_time_evaluation = time.time()


        if doing_classification:


            if binary_classification:

                df_tr_preds = get_score(self.PIPELINE_MODEL, df_tr, self.CALIB_MODEL, score_col="model_score")

                auc_train, _, lift_in_top_churn_train = get_metrics(self.SPARK, df_tr_preds.select("calib_model_score" if self.CALIB_MODEL else "model_score", self.LABEL_COL), title=TRAIN_METRICS_LABEL, do_churn_rate_fix_step=False,
                                           score_col="calib_model_score" if self.CALIB_MODEL else "model_score", label_col=self.LABEL_COL)

                print("[ModelTemplate] Elapsed time evaluating train {} minutes (auc_train={} lift_in_top_churn_train={})".format((time.time() - start_time_evaluation) / 60.0, auc_train, lift_in_top_churn_train))
                self.MODEL_METRICS.create_metrics("train", df_tr_preds.select("calib_model_score" if self.CALIB_MODEL else "model_score", self.LABEL_COL), auc_train, "calib_model_score" if self.CALIB_MODEL else "model_score", lift_in_top_churn=lift_in_top_churn_train)
            else:
                df_tr_preds = self.PIPELINE_MODEL.transform(df_tr).withColumn("model_score", col("prediction"))
                train_metrics_dict = get_multiclass_metrics(self.SPARK, df_tr_preds.select("model_score", self.LABEL_COL, "probability"), score_col="model_score", label_col=self.LABEL_COL)
                self.MODEL_METRICS.create_metrics_multiclass(TRAIN_METRICS_LABEL, metrics_dict=train_metrics_dict)

        else:

            df_tr_preds = self.PIPELINE_MODEL.transform(df_tr)

            rmse_train, r2_train = get_metrics_regression(self.SPARK, df_tr_preds, title=TRAIN_METRICS_LABEL,
                                              score_col="prediction",
                                              label_col=self.LABEL_COL)

            self.MODEL_METRICS.create_metrics_regression(TRAIN_METRICS_LABEL, df_tr_preds, rmse_train, "prediction", r2=r2_train)

            print("[ModelTemplate] Elapsed time evaluating train {} minutes (rmse_train={} r2_train={})".format((time.time() - start_time_evaluation) / 60.0,
                                                                                                                rmse_train, r2_train))
        return df_tr_preds

    def run_predict(self, test_date, df_tt_test=None, do_calibrate_scores=0, filter_correlated_feats=False, metrics_label=None, binary_classification=True):
        '''

        :param test_date: date for get test dataframe
        :param df_tt_test: dataframe for test. If None, then df_tt_test is got from get_set function
        :param do_calibrate_scores:
        :param filter_correlated_feats:
        :param balance_tr_df:
        :param handle_invalid:
        :param metrics_label label to insert the metrics in the metrics dict
        :return:
        '''

        check_args(do_calibrate_scores)

        feats_cols = self.METADATA.get_cols('all', filter_correlated_feats=filter_correlated_feats)
        doing_classification = isClassificationProblem(self.MODEL)

        if not metrics_label:
            metrics_label = TEST_METRICS_LABEL + "_{}".format(test_date) if not self.CALIB_MODEL else "test_calib_{}".format(test_date)

        print("[ModelTemplate] run | Running model for test_date {} - metrics_label={}".format(test_date,metrics_label))

        ##########################################################################################
        # 5. Loading tt data
        ##########################################################################################


        labeled = True if self.MODE == "evaluation" else False
        if df_tt_test is None:
            print("[ModelTemplate] run | Calling get_set with date {}".format(test_date))
            tt_input_df = self.get_set(test_date, labeled, feats_cols, SET_NAME_TEST)
        else:
            print("[ModelTemplate] run | Using parameter df_tt_test as test set")
            tt_input_df = df_tt_test
        print("[ModelTemplate] Count test={}".format(tt_input_df.count()))

        # binary_classification = None (not apply) if we are doing regression
        # binary_classification = True if we are doing binary classification
        # binary_classification = False if we are doing multiclass classification
        if not doing_classification:
            binary_classification = None
        elif self.MODE == "evaluation":
            binary_classification = tt_input_df.select(self.LABEL_COL).distinct().count()==2
        else:
            binary_classification = binary_classification #FIXME how to know if the classification is binary or not? Check this!


        # -----
        auc_test = auc_train = lift_in_top_churn_test = -1
        df_tt_preds = None

        if self.MODE == "evaluation":

            start_time_evaluation = time.time()

            if doing_classification:
                if not do_calibrate_scores == 1:

                    if binary_classification:
                        df_tt_preds = get_score(self.PIPELINE_MODEL, tt_input_df, calib_model=self.CALIB_MODEL, score_col="model_score")

                        auc_test, _, lift_in_top_churn_test = get_metrics(self.SPARK, df_tt_preds.select("model_score", self.LABEL_COL), title=metrics_label, do_churn_rate_fix_step=True, score_col="model_score",
                                                                          label_col=self.LABEL_COL)
                    else:

                        df_tt_preds = self.PIPELINE_MODEL.transform(tt_input_df).withColumn("model_score", col("prediction"))
                        test_metrics_dict = get_multiclass_metrics(self.SPARK, df_tt_preds.select("model_score", self.LABEL_COL, "probability"), score_col="model_score", label_col=self.LABEL_COL)
                        self.MODEL_METRICS.create_metrics_multiclass("test_set", metrics_dict=test_metrics_dict)

                else:
                    #TODO Implement do_calibrates_scores=1
                    pass


            else:
                df_tt_preds = self.PIPELINE_MODEL.transform(tt_input_df)
                rmse_test, r2_test = get_metrics_regression(self.SPARK, df_tt_preds, title=metrics_label, score_col="prediction", label_col=self.LABEL_COL)

                print("[ModelTemplate] Elapsed time evaluating test for date {} -- {} minutes (rmse_test={}, r2_test={})".format(test_date, (time.time() - start_time_evaluation) / 60.0, rmse_test,
                                                                                                                                 r2_test))
                self.MODEL_METRICS.create_metrics_regression("test_{}".format(test_date), df_tt_preds, rmse_test, "prediction", r2=r2_test)

        else:  # mode prediction --> df_tt_preds is not labeled

            if doing_classification:
                if do_calibrate_scores != 1:
                    auc_test = -1.0
                    lift_in_top_churn_test = -1
                    if binary_classification:
                        df_tt_preds = get_score(self.PIPELINE_MODEL, tt_input_df, calib_model=None, score_col="model_score")
                    else:
                        print("[ModelTemplate] - copying prediction col into model_score col")
                        df_tt_preds = self.PIPELINE_MODEL.transform(tt_input_df).withColumn("model_score", col("prediction"))
                    print("[ModelTemplate]  - calibration={} tr_date={} - tt_date={} - AUC(tr)={} - AUC(tt)={}".format(do_calibrate_scores, self.TR_DATE, test_date, auc_train, auc_test))

                    print("CSANC109__DEBUG")
                    df_tt_preds.select("prediction", "model_score").show(10)
            else:
                df_tt_preds = self.PIPELINE_MODEL.transform(tt_input_df)
                self.MODEL_METRICS.create_metrics_regression("test_{}".format(test_date), df_tt_preds, -1, "prediction", r2=-1)

        if doing_classification:
            score_col = "model_score"
            if do_calibrate_scores == 1:  # "calib_model_score" in df_tt_preds.columns:
                print("[ModelTemplate] Renaming 'calib_model_score' by 'scoring'")
                df_tt_preds = df_tt_preds.withColumnRenamed("calib_model_score", "scoring")
            else:
                print("[ModelTemplate] Renaming '{}' by 'scoring'".format(score_col))
                df_tt_preds = df_tt_preds.withColumnRenamed("model_score", "scoring")

            #if binary_classification:
            self.MODEL_METRICS.create_metrics("test_{}".format(test_date), df_tt_preds, auc_test, "scoring", lift_in_top_churn=lift_in_top_churn_test)

        else:
            df_tt_preds = df_tt_preds.withColumnRenamed("prediction", "scoring")  # for model_outputs

        print("CSANC109__DEBUG")
        df_tt_preds.select("prediction", "scoring").show(10)

        return df_tt_preds

    def insert_model_outputs(self, df_tt_preds, model_outputs_name, test_date, insert_top_k=None, extra_info_cols=None, day_to_insert=0, test_metrics_label=None):

        '''
        :param df_tt_preds - asummes score column is named "scoring"
        :param model_outputs_name: model_name in model outputs  '/data/attributes/vf_es/model_outputs/model_parameters/model_name=<model_outputs_name>'
        :param test_date date used for testing the model
        :param insert_top_k:
                    None --> insert the complete dataframe
                    k --> with k>0, insert  the top k of the scored dataframe
        :param extra_info_cols: list of columns that are present in df_tt_preds, and that have to be concat in extra_info field of model_outputs
        :param day_to_insert: 0 (int) for inserting in the partition corresponding to the execution date
                              -1 (int) for inserting in the partition corresponding to the test  date
                              1-7 : 1 (int) for inserting on next Monday, ...., 7 for inserting on next Sunday
                              YYYYMMDD (int or str) for inserting on this partition
        :param test_metrics_label key in METRICS_DICT to extract metrics and insert them in model_outputs
        :return:
        '''

        test_metrics_label = TEST_METRICS_LABEL + "_{}".format(test_date) if not test_metrics_label else None

        ##########################################################################################
        # 7. Model outputs
        ##########################################################################################


        if insert_top_k and insert_top_k <= 0:
            print("[ModelTemplate] check_args - insert_top_k must be None (insert the complete df) or k>0 to insert the top-k")
            sys.exit()

        metrics_train_str = build_metrics_string(self.MODEL_METRICS.get_metrics_dict(TRAIN_METRICS_LABEL))
        metrics_test_str = build_metrics_string(self.MODEL_METRICS.get_metrics_dict(test_metrics_label))
        print(metrics_train_str)
        print(metrics_test_str)
        extra_info_field = build_extra_info_model_parameters(mode=self.MODE, input_dim=len(self.ASSEMBLER_INPUTS), extra_field=None)

        if "nif_cliente" in df_tt_preds.columns:
            print("[ModelTemplate] Renaming 'nif_cliente' by 'nif'")
            df_tt_preds = df_tt_preds.withColumnRenamed("nif_cliente", "nif")

        if "num_cliente" in df_tt_preds.columns:
            print("[ModelTemplate] Renaming 'num_cliente' by 'client_id'")
            df_tt_preds = df_tt_preds.withColumnRenamed("num_cliente", "client_id")

        feat_importance_list = get_feats_imp(self.PIPELINE_MODEL, self.ASSEMBLER_INPUTS)

        feats_importance_names = [col_name for col_name, importance in feat_importance_list]

        #score_col = self.MODEL_METRICS.get_metrics("test").get_score_col()
        score_col  = "scoring"
        # if score_col != "scoring":
            # print("[ModelTemplate] insert_model_outputs | Renaming '{}' by 'scoring'".format(score_col))
            # df_tt_preds = df_tt_preds.withColumnRenamed(score_col, "scoring")

        df_parameters, df_model_scores = create_model_output_dfs(self.SPARK, self.OWNER_LOGIN, self.MODEL if isinstance(self.MODEL, str) else self.MODEL.uid, test_date, model_outputs_name, df_tt_preds,
                                                                      training_closing_date=self.TR_DATE + "to" + self.TR_DATE,
                                                                      metrics_train=metrics_train_str,
                                                                      metrics_test=metrics_test_str,
                                                                      varimp=";".join(feats_importance_names[:min(len(feats_importance_names), 30)]),
                                                                      extra_info_cols = extra_info_cols,
                                                                      extra_info_field=extra_info_field,
                                                                      day_to_insert=day_to_insert)

        df_parameters.show()
        df_model_scores.show()


        if insert_top_k > 0:
            # keep the top-k
            print("[ModelTemplate] Run | Keeping only the top-{} of scored dataframe".format(insert_top_k))
            myschema = df_model_scores.schema
            df_model_scores = df_model_scores.sort(desc(score_col))
            df_model_scores = self.SPARK.createDataFrame(df_model_scores.head(insert_top_k), schema=myschema)

        start_time_model_outputs = time.time()

        insert_to_model_scores(df_model_scores)
        insert_to_model_parameters(df_parameters)

        print("[ModelTemplate] Finished inserting in model outputs |  elapsed = {} minutes".format((time.time() - start_time_model_outputs) / 60.0))

        return df_model_scores, df_parameters


def check_args(do_calibrate_scores):
    if type(do_calibrate_scores) != int:
        print("[ModelTemplate] run | check_args :: do_calibrate_scores must be integer: [0-No calib, 1-Standard calib, 2-Bucket calib]")
        sys.exit(1)
    elif do_calibrate_scores == 1:
        print("[ModelTemplate] check_args :: do_calibrate_scores = 1 is a valid value, but it requires to review ModelTemplate since it has been a long time without using it")
        sys.exit(1)