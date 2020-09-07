
import os
import sys
from pyspark.sql.functions import col, regexp_extract, lit
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

MODEL_SCORES_HDFS_PATH = "/data/attributes/vf_es/model_outputs/model_scores/"
MODEL_PARAMS_HDFS_PATH = "/data/attributes/vf_es/model_outputs/model_parameters/"


MODEL_NAME_PARTITION_CHURN_PREDS = "model_name=churn_preds_{}"
MODEL_NAME_PARTITION_MODEL_NAME = "model_name={}"

CHURN_PREDS_SEGMENTS = ["onlymob", "mobileandfbb"]#, "others"]


def get_complete_scores_segment(spark, segment):
    '''
    Return the join between scores and parameters table for the segment "segment", where model_name "model_name=churn_preds_{segment}"
    :param spark:
    :param segment: Eg. mobileandfbb, onlymob, others
    :return:
    '''
    df_scores = spark.read.load(os.path.join(MODEL_SCORES_HDFS_PATH, MODEL_NAME_PARTITION_CHURN_PREDS.format(segment)))
    df_params = spark.read.load(os.path.join(MODEL_PARAMS_HDFS_PATH, MODEL_NAME_PARTITION_CHURN_PREDS.format(segment))).drop("year", "month", "day")

    df_complete = df_scores.join(df_params, on=["executed_at"], how="left")
    return df_complete

def get_complete_scores_model_name(spark, model_name):
    '''
    Return the join between scores and parameters table for the model_name specified
    :param spark:
    :param model_name: Eg. churn_preds_mobileandfbb, delivery_churn
    :return:
    '''
    df_scores = spark.read.load(os.path.join(MODEL_SCORES_HDFS_PATH, MODEL_NAME_PARTITION_MODEL_NAME.format(model_name)))
    df_params = spark.read.load(os.path.join(MODEL_PARAMS_HDFS_PATH, MODEL_NAME_PARTITION_MODEL_NAME.format(model_name)))

    df_complete = df_scores.join(df_params, on=["executed_at", "year", "month", "day"], how="left")
    return df_complete

def get_complete_scores_model_name_closing_day(spark, model_name, closing_day):
    '''
    Return the join between scores and parameters table for the model_name specified and the closing_day (predict_closing_date)
    :param spark:
    :param model_name: Eg. churn_preds_mobileandfbb, delivery_churn
    :return:
    '''
    return get_complete_scores_model_name(spark, model_name).where(col("predict_closing_date")==closing_day)

def get_scores_segment(spark, predict_closing_date, segment):
    df_scores = spark.read.load(os.path.join(MODEL_SCORES_HDFS_PATH, MODEL_NAME_PARTITION_CHURN_PREDS.format(segment)))\
           .where(col('predict_closing_date') == predict_closing_date)
    return df_scores

def get_complete_scores_closingday(spark, predict_closing_date):
    df = get_complete_scores(spark).where(col("predict_closing_date") == predict_closing_date)
    return df


def get_complete_scores(spark):
    '''
    Return the join for scores&parameters for all the segments in CHURN_PREDS_SEGMENTS
    :param spark:
    :return:
    '''
    df_list = []
    for segment in CHURN_PREDS_SEGMENTS:
        df_ = get_complete_scores_segment(spark, segment=segment)
        df_list.append(df_)

    from pykhaos.utils.pyspark_utils import union_all
    return union_all(df_list)


def get_scores_(spark, predict_closing_date):

    df_list = []
    for segment in CHURN_PREDS_SEGMENTS:
        df_ = get_scores_segment(spark, predict_closing_date, segment=segment)
        df_list.append(df_)

    from pykhaos.utils.pyspark_utils import union_all
    return union_all(df_list)






def ensure_types_model_scores_columns(df, check_ids=True):
    '''
    Run this function with a dataframe BEFORE inserting in model_scores. It ensures the columns and types
    :param df:
    :param check_ids: for PEGA, only one id should be filled.
    :return:
    '''

    id_cols = []
    ALL_ID_COLS = ["msisdn", "nif", "client_id"]

    df = df.withColumn("executed_at", col("executed_at").cast("string"))
    df = df.withColumn("model_executed_at", col("model_executed_at").cast("string"))
    df = df.withColumn("predict_closing_date", col("predict_closing_date").cast("string"))

    if check_ids:
        if "msisdn" in df.columns:
            df = df.withColumn("msisdn", col("msisdn").cast("string"))
            id_cols.append("msisdn")
        elif "client_id" in df.columns:
            df = df.withColumn("client_id", col("client_id").cast("string"))
            id_cols.append("client_id")
        elif "nif" in df.columns:
            df = df.withColumn("nif", col("nif").cast("string"))
            id_cols.append("nif")

        if not id_cols:
            if logger: logger.error("At least one identifier should be specified (msisdn/nif/client_id)")
            sys.exit()

    # Create the columns with missing identifiers
    for col_ in  ALL_ID_COLS:
        if not col_ in df.columns:
            # Avoid this problem "Caused by: java.lang.RuntimeException: Unsupported data type NullType."
            df = df.withColumn(col_, lit(""))
            print("Added None to non existing col {}".format(col_))


    df = df.withColumn("model_output", col("model_output").cast("string"))
    df = df.withColumn("scoring", col("scoring").cast("float"))
    df = df.withColumn("prediction", col("prediction").cast("string"))
    df = df.withColumn("extra_info", col("extra_info").cast("string"))
    df = df.withColumn("model_name", col("model_name").cast("string"))
    df = df.withColumn("time", col("time").cast("int"))
    df = df.withColumn("year", col("year").cast("int"))
    df = df.withColumn("month", col("month").cast("int"))
    df = df.withColumn("day", col("day").cast("int"))

    try:
        df.show()
    except UnicodeEncodeError as e:
        print(e)

    except Exception as e:
        print(e)

    df = df.select(* (["executed_at", "model_executed_at", "predict_closing_date", "model_output", "scoring", "prediction", "extra_info", "time", "model_name", "year", "month", "day"] + ALL_ID_COLS))

    return df

def insert_to_model_scores(df, save_path=None):
    '''
    Given a dataframe, insert it in model_scores. Note that dataframe must have model_name, year, month and day columns
    :param df:
    :param save_path:
    :return:
    '''

    if not save_path:
        save_path = MODEL_SCORES_HDFS_PATH
        if logger: logger.info("Set save_path to '{}'".format(save_path))

    if not save_path:
        if logger: logger.error("Saving path must not be null in insert_model_scores_df")
        sys.exit()
    elif df is None:
        if logger: logger.error("Saving dataframe must not be null in insert_model_scores_df")
        sys.exit()
    elif "model_name" not in df.columns:
        if logger: logger.error("column 'model_name' must be in dataframe")
        sys.exit()
    elif "year" not in df.columns:
        if logger: logger.error("column 'year' must be in dataframe")
        sys.exit()
    elif "month" not in df.columns:
        if logger: logger.error("column 'month' must be in dataframe")
        sys.exit()
    elif "day" not in df.columns:
        if logger: logger.error("column 'day' must be in dataframe")
        sys.exit()

    df = ensure_types_model_scores_columns(df)

    (df.coalesce(1)
      .write
      .partitionBy('model_name', 'year', 'month', 'day')
      .mode("append")
      .format("parquet")
      .save(save_path))


    if logger: logger.info("Dataframe successfully save into '{}'".format(save_path))



def insert_to_model_parameters(df, save_path=None):
    '''
    Given a dataframe, insert it in model_parameters. Note that dataframe must have model_name, year, month and day columns
    :param df:
    :param save_path:
    :return:
    '''
    if not save_path:
        save_path = MODEL_PARAMS_HDFS_PATH
        if logger: logger.info("Set save_path to '{}'".format(save_path))


    if not save_path:
        if logger: logger.error("Saving path must not be null in insert_model_parameters_df")
        sys.exit()
    elif df is None:
        if logger: logger.error("Saving dataframe must not be null in insert_model_parameters_df")
        sys.exit()
    elif "model_name" not in df.columns:
        if logger: logger.error("column 'model_name' must be in dataframe")
        sys.exit()
    elif "year" not in df.columns:
        if logger: logger.error("column 'year' must be in dataframe")
        sys.exit()
    elif "month" not in df.columns:
        if logger: logger.error("column 'month' must be in dataframe")
        sys.exit()
    elif "day" not in df.columns:
        if logger: logger.error("column 'day' must be in dataframe")
        sys.exit()

    (df.coalesce(1)
      .write
      .partitionBy('model_name', 'year', 'month', 'day')
      .mode("append")
      .format("parquet")
      .save(save_path))


    if logger: logger.info("Dataframe successfully save into '{}'".format(save_path))

