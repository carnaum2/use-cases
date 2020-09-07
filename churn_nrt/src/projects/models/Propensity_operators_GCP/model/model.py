# General imports
import argparse
from pyspark.sql import SparkSession
from google.cloud import storage
import os
import pandas as pd


# Global:
APP_NAME = "PropensityOperator"
SOURCE = "gcp_new"
BUCKET = "vf-es-ca-nonlive-dev"
CONFIG_PATH = "gs://vf-es-ca-nonlive-dev/models/propensity_operators/configs.yml"
MODEL_PATH = "gs://vf-es-ca-nonlive-dev/models/propensity_operators/my_propensity_operators.zip"
MODEL_NAME = "my_propensity_operators.zip"


# Loading Spark :
spark = (
    SparkSession.builder.appName(APP_NAME)
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.sql.session.timeZone", "CET")
        .enableHiveSupport()
        .getOrCreate()
)
# Adding logger:
logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)


def load_zip(file_path, file_name):
    # Bucket configs
    client = storage.Client()
    
    bucket=file_path.split("/")[2]
    split=bucket+'/'
    file_path=file_path.partition(split)[2] #get path without bucket
    
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(file_path)

    # Check if file exists:
    print("{} exists: ".format(file_path), blob.exists())
    blob.download_to_filename(file_name)
    
    
    
# Loading libraries:
load_zip(MODEL_PATH, MODEL_NAME)
spark.sparkContext.addPyFile(MODEL_NAME)
logger.info('{} module loaded '.format(MODEL_NAME))


# Imports from churn_nrt

from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import get_split
from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import smart_fit
from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import get_score
from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import get_metrics
from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import get_feats_imp
from my_propensity_operators.churn_nrt.src.projects_utils.models.modeler import get_calibration_function2

# Imports from my model
from my_propensity_operators.data.get_dataset import get_ids

# Imports from utils:
from my_propensity_operators.utils.loaders import load_yml_from_gcp, upload_file_to_gcp


logger.info('Internal libraries loaded')



if __name__ == "__main__":



    parser = argparse.ArgumentParser(
        description="Model that predicts the propensity to the operator specified. The prediction scores for each operator will be saved, in order to join all of them in predictions.py",
        epilog="Please report bugs and issues to Carmen, carmen.arnau1@vodafone.com",
    )

    parser.add_argument(
        "-train_date",
        "--train-date",
        metavar="<train_date>",
        type=str,
        help="YearMonthDay (YYYYMMDD) of the training date to process",
    )
    parser.add_argument(
        "-test_date",
        "--test-date",
        metavar="<test_date>",
        type=str,
        help="YearMonthDay (YYYYMMDD) of the testing date to process",
    )
    parser.add_argument(
        "-mode",
        "--mode",
        metavar="<mode>",
        type=str, help="Mode to run the model",
    )

    parser.add_argument(
        "-operator",
        "--operator",
        metavar="<operator>",
        type=str,
        help="Operator to run the model",
    )


    # Arguments parsing:
    try:
        args = parser.parse_args()
        train_date = args.train_date
        test_date = args.test_date
        mode = args.mode
        operator = args.operator
    except:
        train_date = str("20190914")
        test_date = str("20190914")
        mode = str("production")
        operator = "masmovil"




    # Info:
    logger.info("Getting dataset for {}".format(operator))
    logger.info("Train date: {}".format(train_date))
    logger.info("Test date: {}".format(test_date))
    logger.info("Mode : {}".format(mode))


    # Loading configs:
    configs = load_yml_from_gcp(CONFIG_PATH)
    confs = configs[SOURCE]

    logger.info("Configs dict : {}".format(confs))

    # Path to IDS table:

    path_ids_table = confs["ids_table"]

    logger.info("ids path: {}".format(path_ids_table))

    # Loading train and test data:
    train_final = get_ids(operator,
                          train_date,
                          spark,
                          path_ids_table,
                          "False",
                          mode,
                          confs,
                          logger)

    test_final = get_ids(operator,
                         test_date,
                         spark,
                         path_ids_table,
                         "True",
                         mode,
                         confs,
                         logger)

    logger.info("Fitting model for the operator given...")
    logger.info("Splitting and balancing data for the model...")
    train_unbal, train, validation = get_split(train_final)

    logger.info("Assembling training data and fitting  model...")
    feats = [var for var in train.columns if var not in ["msisdn", "label"]]

    model, assembler = smart_fit(
        "rf",
        train,
        feats=feats,
        non_info_cols="msisdn",
        categorical_cols=None,
        label_col="label",
    )

    logger.info("Getting predictions and scores...")

    pred_test = get_score(
        model, test_final, calib_model=None, add_randn=False, score_col="model_score"
    )
    pred_validation = get_score(
        model, validation, calib_model=None, add_randn=False, score_col="model_score"
    )
    pred_train = get_score(
        model, train, calib_model=None, add_randn=False, score_col="model_score"
    )

    logger.info("Getting AUC...")

    dict_auc = {}

    auc_train, cum_churn_rate_train = get_metrics(
        pred_train,
        title="",
        do_churn_rate_fix_step=False,
        score_col="model_score",
        label_col="label",
    )

    logger.info("AUC train : {}".format(auc_train))
    auc_validation, cum_churn_rate_validation = get_metrics(
        pred_validation,
        title="",
        do_churn_rate_fix_step=False,
        score_col="model_score",
        label_col="label",
    )

    dict_auc['train'] = auc_train
    dict_auc['val'] = auc_validation

    logger.info("AUC validation: {}".format(auc_validation))

    if mode == "evaluation":
        auc_test, cum_churn_rate_test = get_metrics(
            pred_test,
            title="",
            do_churn_rate_fix_step=False,
            score_col="model_score",
            label_col="label",
        )
        dict_auc['test'] = auc_test

        logger.info("AUC test: {}".format(auc_test))

    # Saving AUC results
    df_auc = pd.DataFrame.from_dict(dict_auc, 'index')
    df_auc.rename(columns={0: 'AUC'}, inplace=True)

    auc_local = 'auc_{}.csv'.format(operator)
    df_auc.to_csv(auc_local)

    path_auc = os.path.join(confs["metrics_path"],
                            "AUC",
                            operator,
                            mode,
                            test_date + ".csv")

    upload_file_to_gcp(auc_local, path_auc)


    feat_imp = get_feats_imp(model, feats, top=None)

    logger.info("Feature importance for the model...")

    logger.info(feat_imp)

    logger.info("Getting calibration function for the model...")

    # We calibrate the scores so that we can compare scores from different models

    calib = get_calibration_function2(spark, model, validation, "label", numpoints=10)

    logger.info("Calibrating scores of test predictions..")

    pred_test_calib = calib[0].transform(pred_test)

    if mode == "evaluation":

        pred_test_calib = (
            pred_test_calib.withColumnRenamed("msisdn", "msisdn_" + operator)
                .withColumnRenamed("calib_model_score", "calib_score_" + operator)
                .withColumnRenamed("label", operator + "_label")
                .select(
                "msisdn_" + operator, "calib_score_" + operator, operator + "_label"
            )
        )

    else:

        pred_test_calib = (
            pred_test_calib.withColumnRenamed("msisdn", "msisdn_" + operator)
                .withColumnRenamed("calib_model_score", "calib_score_" + operator)
                .select("msisdn_" + operator, "calib_score_" + operator)
        )

    path_to_save_pred = os.path.join(confs["predictions_path"],
                                     operator,
                                     mode,
                                     test_date)


    logger.info("Saving predictions for {} to {}..".format(operator, path_to_save_pred))

    pred_test_calib.repartition(300).write.save(
        path_to_save_pred,
        format="parquet",
        mode="overwrite",
    )

    logger.info("Predictions saved!...")
