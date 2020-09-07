from pyspark.sql.functions import col, length
from pyspark.sql.types import FloatType
import os
from churn.utils.constants import YAML_DATA_PREPARATION, YAML_CLOSING_DAY
#from churn.utils.constants import YAML_CAMPAIGN, YAML_CAMPAIGN_DATE, YAML_DATA_PREPARATION, YAML_CLOSING_DAY
from churn.utils.general_functions import amdocs_table_reader
from pykhaos.utils.constants import WHOAMI
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import (length, col, when)

from churn.utils.constants import *


PROJECT_NAME = "churn_ccc"



DIR_DELIVERY = CHURN_LISTS_DIR
DIR_DOWNLOAD = CHURN_TMP_DIR


def make_predictions(spark, predict_model_name, predict_model_path, closing_day, h2o_port=54222):  # input_data[YAML_PREDICT][YAML_DO_PREDICT]
    '''
    Compute predictions dataframe. Unlabeled dataset is obtained from loading
        '/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c<closing_day>_n60_comercial_msisdn_<tipo>'
    stored previously in the data preparation processes with unlabeled flag set to true
    :param spark:
    :param predict_model_name: model identifier
    :param predict_model_path: model filename is: os.path.join(predict_model_path, <predict_model_name>.pkl')
    :param closing_day: closing day for locate unlabeled dataframe
    :return: a dataframe with columns 'msisdn', "SCORE", "reason", "type"
    '''

    from pykhaos.modeling.h2o.h2o_functions import restart_cluster_loop, shutdown_cluster
    restart_cluster_loop(port=h2o_port, max_mem_size="10G", enable_assertions=False)  #


    from pykhaos.modeling.model import Model

    modeler_predict = Model.load(predict_model_name, predict_model_path) # load the model
    from churn.models.ccc.data.ccc_data import CCC_Data

    summary_info = {}
    df_list = None
    for tipo in ["mobileandfbb", "onlymob"]:
        file_for_predict = '/data/udf/vf_es/churn/ccc_model/comercial_unlabeled/df_c{}_n60_comercial_msisdn_{}'.format(
            closing_day, tipo)
        predict_ccc_data_obj = CCC_Data(filename=file_for_predict, pkey_cols=["msisdn"], input_data=None) # generate obj
        modeler_predict.predict(predict_ccc_data_obj) # predict

        df_predicciones = predict_ccc_data_obj.data()[['msisdn', u'label', 'PREDICTIONS', 'SCORE', 'p1']]
        df_predicciones["label"] = df_predicciones["label"].asnumeric()
        df_predicciones["reason"] = (df_predicciones["label"] != -1).ifelse(df_predicciones["label"],
                                                                            df_predicciones["PREDICTIONS"])
        df_predicciones["reason"] = (df_predicciones["reason"] == 0).ifelse("NO_COMERCIAL", "COMERCIAL")
        df_predicciones["type"] = tipo
        df_predicciones = df_predicciones[['msisdn', "SCORE", "reason", "type"]]
        if df_list is not None:
            df_list = df_list.rbind(df_predicciones)
        else:
            df_list = df_predicciones
        summary_info[tipo] = {"length": len(df_predicciones)}

    df_scored_hidden = spark.createDataFrame(df_list.as_data_frame())
    #df_scored_hidden = df_scored_hidden.withColumnRenamed("msisdn", "msisdn_a")
    summary_info["all"] = {"length": len(df_list)}

    import pprint
    pprint.pprint(summary_info)

    print(df_scored_hidden.columns)

    shutdown_cluster()

    return df_scored_hidden



def merging_process(df_lever_predict_hidden, df_scores_incidencias, df_churn_reasons):

    df_scores_incidencias = df_scores_incidencias.withColumn("comb_score", col("comb_score").cast(FloatType()))

    if df_lever_predict_hidden is not None:
        df_complete = df_scores_incidencias.join(df_lever_predict_hidden, on=["msisdn"], how="left").withColumnRenamed("reason", "palanca")
        df_complete = df_complete.fillna("COMERCIAL", subset=["palanca"])
        # fcarren - asked to set palanca=NO_COMERCIAL when the customer has a service problem
        df_complete = df_complete.withColumn("palanca", when(col("IND_PBMA_SRV")==1, "NO_COMERCIAL").otherwise(col("palanca")))
    else:
        df_complete = df_scores_incidencias

    if df_churn_reasons is not None:
        df_complete = df_complete.join(df_churn_reasons, on=["msisdn"], how="left")

    df_complete = df_complete.orderBy('comb_score', ascending=False)

    # msisdn --> anonymized
    # msisdn_d --> deanonymized

    return df_complete


def insert_to_delivery_table(df_complete, name_table_prepared, columns_to_keep=None):
    df_complete = df_complete.withColumnRenamed("msisdn", "msisdn_a")
    df_complete = df_complete.withColumnRenamed("msisdn_d", "msisdn")

    if columns_to_keep:
        df_complete = df_complete.select(columns_to_keep + ["msisdn_a"])
        print("Merging_process will write: {}".format("|".join(columns_to_keep)))
    else:
        print("Merging_process does not receive columns to write. All columns will be written")

    # df_complete = (df_complete.where(col('msisdn').isNotNull())
    #                .where(length(col('msisdn')) == 9)
    #                .where(col('comb_score').isNotNull()))

    df_complete = df_complete.drop_duplicates(subset=['msisdn'])

    print("Merged completed!")


    (df_complete.where(col('comb_score').isNotNull()).where(col('msisdn').isNotNull())
     .write
     .format('parquet')
     .mode('overwrite')
     .saveAsTable(name_table_prepared))

    print("Created table {}".format(name_table_prepared))


def prepare_delivery(spark, project_name, closing_day, name_table_prepared,
                     name_file_delivery,dir_download, dir_delivery, columns=None):
    '''
        dir_delivery - directory to store generated file (output)

    '''

    print("Preparing delivery for project='{}' closing_day='{}'".format(project_name, closing_day))
    print("Loading info from table '{}'".format(name_table_prepared))
    print("Results will be written on following filename '{}'".format(name_file_delivery))

    from pykhaos.delivery.prepare_delivery import DeliveryManager
    dm = DeliveryManager(spark, project_name, closing_day, None, None)
    dm.deliver_table(name_table_prepared,
                     name_file_delivery,
                     dir_download,
                     dir_delivery,
                     None, # local_dir_deliverables
                     sep="|",
                     columns=columns,
                     order_by="comb_score",
                     file_extension="txt",
                     allow_multiple_partitions=False)