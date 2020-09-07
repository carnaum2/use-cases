#General imports
import os
import datetime as dt
import argparse

from pyspark.sql import SparkSession
from google.cloud import storage
import pyspark.sql.functions as psf
from pyspark.sql.functions import (lit, from_unixtime, unix_timestamp,regexp_replace,
                                   split,concat_ws, array,
                                   col, array_contains,collect_list, when)
import pandas as pd


#Global
APP_NAME = 'Propensity Predictions'
BUCKET = "vf-es-ca-nonlive-dev"
CONFIG_PATH = "gs://vf-es-ca-nonlive-dev/models/propensity_operators/configs.yml"
MODEL_PATH = "gs://vf-es-ca-nonlive-dev/models/propensity_operators/my_propensity_operators.zip"
MODEL_NAME = "my_propensity_operators.zip"
SOURCE = 'gcp_new'


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

from my_propensity_operators.churn_nrt.src.data.customer_base import CustomerBase
from my_propensity_operators.churn_nrt.src.utils.date_functions import get_next_dow
from my_propensity_operators.utils.loaders import load_yml_from_gcp, upload_file_to_gcp


# Imports from my model (ADD)

# Imports from utils (ADD)

logger.info('Internal libraries loaded')


if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description="Join test predictions for each operator (generated in model.py) and get the final prediction. The final predictions dataframe will be saved",
        epilog="Please report bugs and issues to Carmen, carmen.arnau1@vodafone.com",
    )

    parser.add_argument(
        "-test_date",
        "--test-date",
        metavar="<test_date>",
        type=str,
        help="YearMonthDay (YYYYMMDD) of the testing date to process",
    )
    parser.add_argument(
        "-mode", "--mode", metavar="<mode>", type=str, help="Mode to run the model",
    )



    # Arguments parsing:
    args = parser.parse_args()
    test_date = args.test_date.strip()
    mode = args.mode.strip()

    # Info:
    logger.info("Test date:-{}-".format(test_date))
    logger.info("Mode : -{}-".format(mode))

    # Loading configs:
    configs = load_yml_from_gcp(CONFIG_PATH)
    confs = configs[SOURCE]

    logger.info("Configs dict : {}".format(confs))




    logger.info("Loading calibrated predictions generated in model.py...")

    path_preds_folder = os.path.join(confs["predictions_path"],
                                     "{}",
                                     mode,
                                     test_date)

    logger.info("Path to predictions : {}".format(path_preds_folder))

    path_pred_masmovil = path_preds_folder.format("masmovil")
    path_pred_movistar = path_preds_folder.format("movistar")
    path_pred_orange = path_preds_folder.format("orange")
    path_pred_others = path_preds_folder.format("others")


    pred_masmovil_test_calib = spark.read.load(path_pred_masmovil)
    pred_movistar_test_calib = spark.read.load(path_pred_movistar)
    pred_orange_test_calib = spark.read.load(path_pred_orange)
    pred_others_test_calib = spark.read.load(path_pred_others)


    logger.info('Joining prediction dataframes...')

    predicciones_union = pred_masmovil_test_calib.join(pred_movistar_test_calib,
                                                       on=(pred_masmovil_test_calib['msisdn_masmovil'] ==
                                                           pred_movistar_test_calib['msisdn_movistar']), how='inner')

    predicciones_union = predicciones_union.join(pred_orange_test_calib,
                                                 on=(predicciones_union['msisdn_masmovil'] == pred_orange_test_calib[
                                                     'msisdn_orange']), how='inner')

    predicciones_union = predicciones_union.join(pred_others_test_calib,
                                                 on=(predicciones_union['msisdn_masmovil'] == pred_others_test_calib[
                                                     'msisdn_others']), how='inner')

    if mode == 'evaluation':

        logger.info('Since mode is evaluation, we add real label to predictions dataframe...')

        predicciones_union = predicciones_union.withColumn('real_label',
                                                           when(predicciones_union['masmovil_label'] == 1, 1).otherwise(
                                                               when(predicciones_union['movistar_label'] == 1,
                                                                    2).otherwise(
                                                                   when(predicciones_union['orange_label'] == 1,
                                                                        3).otherwise(
                                                                       when(predicciones_union['others_label'] == 1,
                                                                            4)))))

        predicciones_union = predicciones_union.select('msisdn_masmovil', 'calib_score_masmovil',
                                                       'calib_score_movistar',
                                                       'calib_score_orange', 'calib_score_others',
                                                       'real_label').withColumnRenamed('msisdn_masmovil', 'msisdn')


    else:  # If we are in production mode we do not have the real labels

        predicciones_union = predicciones_union.select('msisdn_masmovil', 'calib_score_masmovil',
                                                       'calib_score_movistar', 'calib_score_orange',
                                                       'calib_score_others').withColumnRenamed('msisdn_masmovil', 'msisdn')


    logger.info('Grouping predictions by NIF...')

    base_test = CustomerBase(spark, confs).get_module(test_date).filter(col('rgu') == 'mobile').select('nif_cliente', 'msisdn')

    predicciones_union_nif = predicciones_union.join(base_test, on='msisdn', how='inner')

    logger.info('Getting maximum scores for each NIF...')

    predicciones_union_grouped = predicciones_union_nif.groupby('nif_cliente').agg({'calib_score_masmovil': 'max',
                                                                                    'calib_score_movistar': 'max',
                                                                                    'calib_score_orange': 'max',
                                                                                    'calib_score_others': 'max'})

    if mode == 'evaluation':

        logger.info('Since mode is evaluation, we add real label: list of labels for each NIF')

        list_label = predicciones_union_nif.groupBy('nif_cliente').agg(collect_list("real_label"))

        predicciones_union_grouped = predicciones_union_grouped.join(list_label, on='nif_cliente', how='inner')

        predicciones_union_grouped = predicciones_union_grouped.withColumnRenamed('max(calib_score_others)',
                                                                                  'calib_score_others').withColumnRenamed('max(calib_score_masmovil)',
                                                                                    'calib_score_masmovil').withColumnRenamed('max(calib_score_orange)',
                                                                                        'calib_score_orange').withColumnRenamed('max(calib_score_movistar)',
                                                                                              'calib_score_movistar').withColumnRenamed('collect_list(real_label)', 'real_label')

    else:

        logger.info('Since mode is production, we no not add real labels')

        predicciones_union_grouped = predicciones_union_grouped.withColumnRenamed('max(calib_score_others)',
                                                                                  'calib_score_others').withColumnRenamed('max(calib_score_masmovil)',
                                                                                    'calib_score_masmovil').withColumnRenamed('max(calib_score_orange)',
                                                                                   'calib_score_orange').withColumnRenamed('max(calib_score_movistar)','calib_score_movistar')


    logger.info('Normalizing calibrated scores for each operator...')

    predicciones_union_norm_masmovil = predicciones_union_grouped.withColumn('calib_score_masmovil',
                                                                             col('calib_score_masmovil') /
                                                                             sum(col(column) for column in
                                                                                 predicciones_union_grouped.columns[
                                                                                 1:5])).select('nif_cliente',
                                                                                               'calib_score_masmovil')

    predicciones_union_norm_movistar = predicciones_union_grouped.withColumn('calib_score_movistar',
                                                                             col('calib_score_movistar') /
                                                                             sum(col(column) for column in
                                                                                 predicciones_union_grouped.columns[
                                                                                 1:5])).select('nif_cliente',
                                                                                               'calib_score_movistar')

    predicciones_union_norm_orange = predicciones_union_grouped.withColumn('calib_score_orange',
                                                                           col('calib_score_orange') /
                                                                           sum(col(column) for column in
                                                                               predicciones_union_grouped.columns[
                                                                               1:5])).select('nif_cliente',
                                                                                             'calib_score_orange')

    if mode == 'evaluation':

        predicciones_union_norm_others = predicciones_union_grouped.withColumn('calib_score_others',
                                                                               col('calib_score_others') /
                                                                               sum(col(column) for column in
                                                                                   predicciones_union_grouped.columns[
                                                                                   1:5])).select('nif_cliente',
                                                                                                 'calib_score_others',
                                                                                                 'real_label')

    else:

        predicciones_union_norm_others = predicciones_union_grouped.withColumn('calib_score_others',
                                                                               col('calib_score_others') /
                                                                               sum(col(column) for column in
                                                                                   predicciones_union_grouped.columns[
                                                                                   1:5])).select('nif_cliente',
                                                                                                 'calib_score_others')

    predicciones_union_norm = predicciones_union_norm_masmovil.join(predicciones_union_norm_movistar, on='nif_cliente',
                                                                    how='inner')

    predicciones_union_norm = predicciones_union_norm.join(predicciones_union_norm_orange, on='nif_cliente',
                                                           how='inner')

    predicciones_union_norm = predicciones_union_norm.join(predicciones_union_norm_others, on='nif_cliente', how='inner')


    logger.info('Getting predicted label: operator whose score is higher...')

    cond = "psf.when" + ".when".join(
        ["(psf.col('" + c + "') == psf.col('prob_max'), psf.lit('" + c + "'))" for c in
         predicciones_union_norm.columns[1:5]])

    predicciones_final = predicciones_union_norm.withColumn("prob_max",psf.greatest(
                                                                *predicciones_union_norm.columns[1:5])).withColumn("predicted_label", eval(cond))

    predicciones_final = predicciones_final.withColumn("predicted_label",
                                                       when(col("predicted_label") == 'calib_score_masmovil',
                                                            1).otherwise(
                                                           when(col("predicted_label") == 'calib_score_movistar',
                                                                2).otherwise(
                                                               when(col("predicted_label") == 'calib_score_orange',
                                                                    3).otherwise(
                                                                   when(col("predicted_label") == 'calib_score_others',
                                                                        4)))))

    final_predictions = predicciones_final.orderBy('prob_max', ascending=False)

    path_results = os.path.join(confs["predictions_path"],
                                  "final",
                                  mode,
                                  test_date)


    if mode == 'evaluation':

        logger.info('Since mode is evaluation, we calculate the accuracy of the model: if the predicted label is contained in the list of real labels for each NIF, we consider the prediction correct')

        final_predictions_accuracy = final_predictions.withColumn('real_label_contain_1',
                                                                  when(array_contains(col('real_label'), 1),
                                                                       1).otherwise(0)).withColumn('real_label_contain_2',
                                                                           when(array_contains(col('real_label'), 2),
                                                                                1).otherwise(0)).withColumn('real_label_contain_3',
                                                                           when(array_contains(col('real_label'), 3),
                                                                                1).otherwise(0)).withColumn('real_label_contain_4',
                                                                           when(array_contains(col('real_label'), 4),
                                                                                1).otherwise(0))

        final_predictions_accuracy = final_predictions_accuracy.withColumn('acierto',
                                                                           when((((col('predicted_label') == 1) & (col(
                                                                               'real_label_contain_1') == 1))
                                                                                 | ((col('predicted_label') == 2) & (
                                                                                           col('real_label_contain_2') == 1))
                                                                                 | ((col('predicted_label') == 3) & (
                                                                                           col('real_label_contain_3') == 1))
                                                                                 | ((col('predicted_label') == 4) & (
                                                                                           col('real_label_contain_4') == 1))),1).otherwise(0))

        logger.info('Accuracy of the model (proportion of correct predictions)')

        final_predictions_accuracy = final_predictions_accuracy.orderBy('prob_max', ascending=False)

        total = final_predictions_accuracy.count()

        num_correct = final_predictions_accuracy.filter(col('acierto') == 1).count()

        ac_total = float(num_correct) / float(total)

        logger.info('Accuracy of the model on top 30000')

        num_correct_top = final_predictions_accuracy.limit(30000).filter(col('acierto') == 1).count()

        ac_30000 = float(num_correct_top) / float(30000)

        logger.info('Accuracy of the model on top 10000')

        num_correct_top = final_predictions_accuracy.limit(10000).filter(col('acierto') == 1).count()

        ac_10000 = float(num_correct_top) / float(10000)

        logger.info('Saving accuracy')

        dict_accuracy = {}

        dict_accuracy['total'] = ac_total
        dict_accuracy['Top 30.000'] = ac_30000
        dict_accuracy['Top 10.000'] = ac_10000

        # Saving accuracy results
        df_accuracy = pd.DataFrame.from_dict(dict_accuracy, 'index')
        df_accuracy.rename(columns={0: 'Accuracy'}, inplace=True)

        accuracy_local = 'accuracy.csv'
        df_accuracy.to_csv(accuracy_local)

        path_accuracy = os.path.join(confs["metrics_path"], 'accuracy', test_date + ".csv")
        upload_file_to_gcp(accuracy_local, path_accuracy)


        

        logger.info('Saving final predictions dataframe')

        final_predictions_accuracy.repartition(300).write.save(path_results,
                                                               format='parquet',
                                                               mode='overwrite')

        logger.info('Final predictions dataframe saved for evaluation')

    else:

        logger.info('Since mode is production, we put the predictions dataframe in model_output format')

        model_output_cols = ["model_name",
                             "executed_at",
                             "model_executed_at",
                             "predict_closing_date",
                             "msisdn",
                             "client_id",
                             "nif",
                             "model_output",
                             "scoring",
                             "prediction",
                             "extra_info",
                             "year",
                             "month",
                             "day",
                             "time"]

        final_predictions = final_predictions.withColumn('predicted_label', when(col("predicted_label") == 1,
                                                                                 'masmovil').otherwise(
            when(col("predicted_label") == 2,
                 'movistar').otherwise(when(col("predicted_label") == 3,
                                            'orange').otherwise(when(col("predicted_label") == 4, 'others')))))

        final_predictions = final_predictions.withColumn('all_probs',
                                                         array(final_predictions['calib_score_masmovil'],
                                                               final_predictions['calib_score_movistar'],
                                                               final_predictions['calib_score_orange'],
                                                               final_predictions['calib_score_others']))

        final_predictions = final_predictions.withColumn('all_probs', concat_ws(',', final_predictions.all_probs))


        executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

        partition_date = str(get_next_dow(3))  # get day of next wednesday
        partition_year = int(partition_date[0:4])
        partition_month = int(partition_date[5:7])
        partition_day = int(partition_date[8:10])

        df_model_scores = (final_predictions
                           .withColumn("model_name", lit("churn_competitor_version2").cast("string"))
                           .withColumn("executed_at",
                                       from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast(
                                           "string"))
                           .withColumn("model_executed_at", col("executed_at").cast("string"))
                           .withColumn("client_id", lit(""))
                           .withColumn("msisdn", lit(""))
                           .withColumn("nif", col("nif_cliente").cast("string"))
                           .withColumn("scoring", col("prob_max"))
                           .withColumn("model_output", col('all_probs'))
                           .withColumn("prediction", col('predicted_label').cast("string"))
                           .withColumn("extra_info", lit("").cast("string"))
                           .withColumn("predict_closing_date", lit(test_date))
                           .withColumn("year", lit(partition_year).cast("integer"))
                           .withColumn("month", lit(partition_month).cast("integer"))
                           .withColumn("day", lit(partition_day).cast("integer"))
                           .withColumn("time",regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                           .select(*model_output_cols))






        df_model_scores \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .save(path_results)

        logger.info('Final predictions dataframe saved in model output format')
