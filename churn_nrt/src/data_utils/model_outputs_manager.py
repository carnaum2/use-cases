
import os
import sys
from pyspark.sql.functions import col, desc, lit, concat, when, coalesce, regexp_replace, split, concat_ws, length, asc, lpad, max as sql_max, regexp_extract

MODEL_SCORES_HDFS_PATH = "/data/attributes/vf_es/model_outputs/model_scores/"
MODEL_PARAMS_HDFS_PATH = "/data/attributes/vf_es/model_outputs/model_parameters/"


MODEL_NAME_PARTITION_CHURN_PREDS = "model_name=churn_preds_{}"
MODEL_NAME_PARTITION_MODEL_NAME = "model_name={}"

CHURN_PREDS_SEGMENTS = ["onlymob", "mobileandfbb"]#, "others"]

MODEL_OUTPUTS_NULL_TAG = "null"

import datetime as dt
import pandas as pd


def __build_str_from_dict(mydict):
    '''

    :param mydict:
    :return:
    '''
    return ";".join(["{}:{}".format(k,v) for k, v in mydict.items()])


def build_metrics_string(mydict):
        '''
        Build the string to be inserted in metrics_train/metrics_test in model parameters partition
        :param auc:
        :param df_model_scores:
        :param score_col: name of the column that contains the scoring info
        :return:
        '''

        return __build_str_from_dict(mydict) if mydict else ""

def build_extra_info_model_parameters(mode, input_dim, extra_field=None):
    '''
    Build field extra_info of model_parameters
    :param mode:
    :param input_dim:
    :param extra_field: an string to be appended or a dict to be appended as key1=value1;key2=value2;...
    :return:
    '''

    extra_info_field = u'mode={};input_dim={}'.format(mode,input_dim)
    print("build_extra_info_model_parameters", extra_field, type(extra_field))

    if extra_field and isinstance(extra_info_field, str):
        extra_info_field += ";" + extra_field if not extra_field.startswith(";") else extra_field
    elif extra_field and isinstance(extra_info_field, dict):
        extra_info_field += __build_str_from_dict(extra_field)

    return extra_info_field


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

def ensure_types_model_scores_columns(df):
    '''
    Run this function with a dataframe BEFORE inserting in model_scores. It ensures the columns and types
    :param df:
    :return:
    '''

    id_cols = []
    ALL_ID_COLS = ["msisdn", "nif", "client_id"]

    df = df.withColumn("executed_at", col("executed_at").cast("string"))
    df = df.withColumn("model_executed_at", col("model_executed_at").cast("string"))
    df = df.withColumn("predict_closing_date", col("predict_closing_date").cast("string"))
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
        print("At least one identifier (msisdn/nif/cliente_id) should be specified (msisdn/nif/client_id). Program will exit here!")
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
        print("Set save_path to '{}'".format(save_path))

    if not save_path:
        print("Saving path must not be null in insert_model_scores_df")
        sys.exit()
    elif df is None:
        print("Saving dataframe must not be null in insert_model_scores_df")
        sys.exit()
    elif "model_name" not in df.columns:
        print("column 'model_name' must be in dataframe")
        sys.exit()
    elif "year" not in df.columns:
        print("column 'year' must be in dataframe")
        sys.exit()
    elif "month" not in df.columns:
        print("column 'month' must be in dataframe")
        sys.exit()
    elif "day" not in df.columns:
        print("column 'day' must be in dataframe")
        sys.exit()

    df = ensure_types_model_scores_columns(df)


    df = df.cache()
    print("About to save {} rows in model_scores".format(df.count()))
    #df.show()
    df = df.repartition(200)

    (df.write
      .partitionBy('model_name', 'year', 'month', 'day')
      .mode("append")
      .format("parquet")
      .save(save_path))


    print("Dataframe successfully save into '{}'".format(save_path))



def insert_to_model_parameters(df, save_path=None):
    '''
    Given a dataframe, insert it in model_parameters. Note that dataframe must have model_name, year, month and day columns
    :param df:
    :param save_path:
    :return:
    '''
    if not save_path:
        save_path = MODEL_PARAMS_HDFS_PATH
        print("Set save_path to '{}'".format(save_path))


    if not save_path:
        print("Saving path must not be null in insert_model_parameters_df")
        sys.exit()
    elif df is None:
        print("Saving dataframe must not be null in insert_model_parameters_df")
        sys.exit()
    elif "model_name" not in df.columns:
        print("column 'model_name' must be in dataframe")
        sys.exit()
    elif "year" not in df.columns:
        print("column 'year' must be in dataframe")
        sys.exit()
    elif "month" not in df.columns:
        print("column 'month' must be in dataframe")
        sys.exit()
    elif "day" not in df.columns:
        print("column 'day' must be in dataframe")
        sys.exit()

    (df.write
      .partitionBy('model_name', 'year', 'month', 'day')
      .mode("append")
      .format("parquet")
      .save(save_path))


    print("Dataframe successfully save into '{}'".format(save_path))



def create_model_output_dfs(spark, owner, algorithm, closing_day, model_outputs_name, df_model_scores, training_closing_date=None,
                                metrics_train="", metrics_test="", varimp="", extra_info_cols=None, extra_info_field="",
                            day_to_insert=0):
        '''
        WARN: It assumes score column is named "scoring".
        :param closing_day:
        :param df_model_scores:
        :param training_closing_date:
        :param metrics_train:
        :param metrics_test:
        :param varimp:
        :param extra_info_cols list of co                                                                                                                                                                          lumns to be concat in extra info field, separated by ;
        :param extra_info_field: info to store in extra_info column of model_parameters
        :param day_to_insert: 0 (int) for inserting in the partition corresponding to the execution date
                              -1 (int) for inserting in the partition corresponding to the test  date
                              1-7 : 1 (int) for inserting on next Monday, ...., 7 for inserting on next Sunday
                              YYYYMMDD (int or str) for inserting on this partition
        :return:
        '''

        print("[ModelTemplate] create_model_output_dfs | Start...")


        executed_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        if day_to_insert >= -1 and day_to_insert <= 7:
            if day_to_insert == 0: # partition is extracted from current datetime
                print("Inserting in current execution time... {}".format(executed_at))
                return_feed_execution = executed_at.split(" ")[0].replace("-","")
            elif day_to_insert == -1:
                return_feed_execution = closing_day
            else:
                from churn_nrt.src.utils.date_functions import get_next_dow
                return_feed_execution = get_next_dow(weekday=day_to_insert).strftime("%Y%m%d")
        else:
            print("User wants to insert on partition {}".format(day_to_insert))
            return_feed_execution = str(day_to_insert)

        print("return_feed_execution", return_feed_execution)
        day_partition = int(return_feed_execution[6:])
        month_partition = int(return_feed_execution[4:6])
        year_partition = int(return_feed_execution[:4])

        print("[ModelTemplate] create_model_output_dfs | (day_to_insert={}) Going to insert with partition {} {} {}".format(day_to_insert,
                                                                                                                            year_partition,
                                                                                                                            month_partition,
                                                                                                                            day_partition))

        #MODEL PARAMETERS

        df_pandas = pd.DataFrame({"model_name": [model_outputs_name],
                                  "executed_at": [executed_at],
                                  "model_level": ["nif"],
                                  "training_closing_date": [training_closing_date if training_closing_date else closing_day],
                                  "target": ["port"], "model_path": [""], "metrics_path": [""],
                                  "metrics_train": [metrics_train if metrics_train else ""],
                                  "metrics_test":  [metrics_test if metrics_test else ""],
                                  "varimp": [varimp],
                                  "algorithm": [algorithm],
                                  "author_login": [owner],
                                  "extra_info": [extra_info_field],
                                  "scores_extra_info_headers": [";".join(extra_info_cols) if extra_info_cols else ""],
                                  "year": [year_partition],
                                  "month": [month_partition],
                                  "day": [day_partition],
                                  "time": [int(executed_at.split(" ")[1].replace(":", ""))]})

        df_parameters = (spark.createDataFrame(df_pandas).withColumn("day", col("day").cast("integer"))
                                                        .withColumn("month", col("month").cast("integer"))
                                                        .withColumn("year", col("year").cast("integer"))
                                                        .withColumn("time", col("time").cast("integer")))

        #MODEL SCORES

        # set to null the columns that go to extra_info field and have no value
        for col_ in extra_info_cols:
            df_model_scores = df_model_scores.withColumn(col_, when(coalesce(length(col(col_)), lit(0)) == 0, MODEL_OUTPUTS_NULL_TAG).otherwise(col(col_)))

        df_model_scores = (
            df_model_scores.withColumn("extra_info", concat_ws(";", *[col(col_name) for col_name in extra_info_cols]))
                .withColumn("prediction", lit("0"))# .withColumn("prediction", when(col("comb_decile") == 0, "0").otherwise("1"))
                .drop(*extra_info_cols)# .withColumnRenamed("comb_score", "scoring")
                .withColumn("model_name", lit(model_outputs_name))
                .withColumn("executed_at", lit(executed_at))
                .withColumn("model_executed_at", lit(executed_at))
                .withColumn("year", lit(year_partition).cast("integer"))
                .withColumn("month",lit(month_partition).cast("integer"))
                .withColumn("day", lit(day_partition).cast("integer"))
                .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                .withColumn("predict_closing_date", lit(closing_day))
                .withColumn("model_output", lit(None))
        )

        df_model_scores = ensure_types_model_scores_columns(df_model_scores)

        return df_parameters, df_model_scores


def add_decile(df_scores, score="scoring", perc=0.2, n=10, n_norisk=0, noise = 0):
    '''
            :param df_scores:
            :param score: column with the scores
            :param perc: percentage of custmers to mark with risk. If perc>1, it indicates the number of customers to be marked with the risk flag
            :param n: number of percentiles
            :param n_norisk: number of percentiles for customers marked as no risk
            :return: df_scores with additional decile and risk flag columns
    '''

    TMP_DECIL = "tmp_decil"

    # This cast is inherited from the previous version of the function, but, apparently, it could be avoided

    from pyspark.sql.types import FloatType

    df_scores = df_scores.withColumn(score, col(score).cast(FloatType()))

    # Adding noise: this prevents misleading results for the risk column when a number of entries have the same score
    # The function approxQuantile cannot find a separation between entries with the same score and the
    # percentage of samples at risk, specified by perc, would not be obtained in the output DF

    from pyspark.sql.functions import randn

    df_scores = df_scores.withColumn(score, col(score) + lit(noise) * randn())

    df_scores = df_scores.cache()

    format_perc = perc if (perc <= 1) else float(perc) / float(df_scores.count())

    qq = df_scores.approxQuantile(score, [1 - format_perc], 0.000001)[0]

    df_scores = df_scores.withColumn("risk", when(col(score) >= qq, 1).otherwise(0))

    from pyspark.sql.window import Window

    window = Window.partitionBy('risk').orderBy(asc(score))

    from pyspark.sql.functions import ntile

    df_scores = df_scores \
        .withColumn(TMP_DECIL, ntile(n).over(window)) \
        .withColumn(TMP_DECIL, when(col('risk') == 0, 0).otherwise(col(TMP_DECIL))) \
        .withColumn("decil", concat(lit("decil="), col(TMP_DECIL))) \
        .withColumn("flag_propension", concat(lit("flag_propension="), col("risk"))) \
        .drop(TMP_DECIL)

    if (n_norisk > 0):
        TMP_PERCENTILE = "tmp_percentile"
        df_scores = df_scores \
            .withColumn(TMP_PERCENTILE, ntile(n_norisk).over(window)) \
            .withColumn(TMP_PERCENTILE, when(col('risk') == 1, 0).otherwise(col(TMP_PERCENTILE))) \
            .withColumn("percentile_norisk", concat(lit("percentile_norisk="), col(TMP_PERCENTILE))) \
            .drop(TMP_PERCENTILE)

    return df_scores


def get_last_dates(spark, model_name):
    '''
    Given a model name returns the last delivery, training and test dates stored in model_scores
    Firstly, it gets the latest delivery date and for this entry, returns the training and test dates
    :param spark:
    :param model_name: model name used in model_scores
    :return:
    '''
    deliv_last_date = (spark.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name={}".format(model_name))
                                 .withColumn("deliv_partition", concat(col("year"), lpad(col("month"), 2, '0'), lpad(col("day"), 2, '0')))
                                 .select(sql_max(col('deliv_partition')).alias('last_date')).rdd.first()['last_date'])

    df_scores = (spark.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name={}".format(model_name))
                 .withColumn("deliv_partition", concat(col("year"), lpad(col("month"), 2, '0'), lpad(col("day"), 2, '0')))
                 .where(col("deliv_partition") == deliv_last_date).select("executed_at", "predict_closing_date", "deliv_partition").limit(1))

    df_params = (spark.read.load("/data/attributes/vf_es/model_outputs/model_parameters/model_name={}".format(model_name))
                           .withColumn("deliv_partition", concat(col("year"), lpad(col("month"), 2, '0'), lpad(col("day"), 2, '0')))
                           .where(col("deliv_partition") == deliv_last_date).select("executed_at", "training_closing_date"))

    df_scores = df_scores.join(df_params, on=["executed_at"], how="outer")
    df_scores = df_scores.select("executed_at", "training_closing_date", "predict_closing_date", "deliv_partition")
    df_scores = (df_scores.withColumn("training_closing_date", regexp_extract(col('training_closing_date'), '(\w+)to(\w+)', 1))
                          .withColumnRenamed("training_closing_date", "tr_date")
                          .withColumnRenamed("predict_closing_date", "tt_date"))

    tr_date = df_scores.select("tr_date").rdd.first()[0]
    tt_date = df_scores.select("tt_date").rdd.first()[0]

    return deliv_last_date, tr_date, tt_date