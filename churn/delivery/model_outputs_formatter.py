
import datetime as dt

from churn.delivery.delivery_constants import MODEL_NAME_DELIV_CHURN, EXTRA_INFO_COLS, MODEL_OUTPUTS_NULL_TAG

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

from pyspark.sql.functions import col, when, coalesce, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract
from churn.datapreparation.general.model_outputs_manager import ensure_types_model_scores_columns




def create_model_output_dataframes(spark, closing_day, df_model_scores, training_closing_date=None, historic=False):

    print("create_model_output_dataframes", historic)
    # if df_model_scores is None:
    #     from churn.delivery.delivery_constants import NAME_TABLE_DEANONYMIZED
    #     table_name = NAME_TABLE_DEANONYMIZED.format(closing_day)
    #     if logger: logger.info("Reading from table {}".format(table_name))
    #
    #     df_model_scores = spark.read.table(table_name).drop(
    #         "msisdn").withColumnRenamed("msisdn_a", "msisdn")  # .withColumn("num_cliente", lit("X")).withColumn("nif_cliente", lit("Y"))
    # else:
    #     if logger: logger.info("Getting info from argument df")
    #
    # If historic = True, it inserts in the partition that corresponds to the closing_day
    # If historic = False, it inserts in the partition that corresponds to the next Friday since today

    executed_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S ")

    # VALID SOLUTION. TO BE USED WHEN THIS PROCESS IS RUN IN JENKINS
    # day_partition = int(executed_at[8:10])
    # month_partition = int(executed_at[5:7])
    # year_partition = int(executed_at[:4])

    # TEMPORARY SOLUTION FOR RETURN FEED. PARTITION IS SET TO THE DATE OF NEXT FRIDAY
    from pykhaos.utils.date_functions import get_next_dow
    if historic:
        return_feed_execution = closing_day
    else:
        return_feed_execution = get_next_dow(weekday=5).strftime("%Y%m%d")

    day_partition = int(return_feed_execution[6:])
    month_partition = int(return_feed_execution[4:6])
    year_partition = int(return_feed_execution[:4])


    print("Going to insert with partition {} {} {} since historic={}".format(year_partition, month_partition, day_partition, historic))

    '''
    MODEL PARAMETERS
    '''

    import pandas as pd
    df_pandas = pd.DataFrame({
        "model_name": [MODEL_NAME_DELIV_CHURN],
        "executed_at": [executed_at],
        "model_level": ["msisdn"],
        "training_closing_date": [training_closing_date if training_closing_date else closing_day],
        "target": [""],
        "model_path": [""],
        "metrics_path": [""],
        "metrics_train": [""],
        "metrics_test": [""],
        "varimp": [""],
        "algorithm": [""],
        "author_login": ["csanc109;asaezco;bgmerin1;jmarcoso"],
        "extra_info": [""],
        "scores_extra_info_headers": [";".join(EXTRA_INFO_COLS)],
        "year":  [year_partition],
        "month": [month_partition],
        "day":   [day_partition],
        "time": [int(executed_at.split(" ")[1].replace(":", ""))]
    })

    df_parameters = spark.createDataFrame(df_pandas).withColumn("day", col("day").cast("integer"))\
                                                    .withColumn("month", col("month").cast("integer"))\
                                                    .withColumn("year", col("year").cast("integer"))\
                                                    .withColumn("time", col("time").cast("integer"))

    '''
    MODEL SCORES
    '''

    # set to null the columns that go to extra_info field and have no value
    for col_ in EXTRA_INFO_COLS:
        df_model_scores = df_model_scores.withColumn(col_,
                                                     when(coalesce(length(col(col_)), lit(0)) == 0, MODEL_OUTPUTS_NULL_TAG).otherwise(
                                                         col(col_)))

    df_model_scores = (
            df_model_scores.withColumn("extra_info", concat_ws(";", *[col(col_name) for col_name in EXTRA_INFO_COLS]))
            .withColumn("prediction", when(col("comb_decile") == 0, "0").otherwise("1"))
            .drop(*EXTRA_INFO_COLS)
            .withColumnRenamed("comb_score", "scoring")
            .withColumn("model_name", lit(MODEL_NAME_DELIV_CHURN))
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


def insert_to_model_outputs(spark, closing_day, df_model_scores, training_closing_date=None, historic=False):

    df_param, df_scores = create_model_output_dataframes(spark, closing_day, df_model_scores, training_closing_date, historic)

    from churn.datapreparation.general.model_outputs_manager import insert_to_model_scores, insert_to_model_parameters
    insert_to_model_scores(df_scores)
    insert_to_model_parameters(df_param)

    if logger: logger.info("Inserted to model outputs")


def build_pred_name(spark, closing_day, segment, horizon):
    '''
    Build the pred_name where the model info was stored from closing_day, segment and horizon
    pred_name is something like: 'prediction_tr20190314to20190314_tt20190514_horizon8_on20190522_225408'
    Pred_name allows us to build the path where the info is located:
    /data/udf/vf_es/churn/models/prediction_tr20190314to20190314_tt20190514_horizon8_on20190522_225408
    :param spark:
    :param closing_day:
    :param segment:
    :param horizon:
    :return:
    '''
    params_set = get_training_dates(spark, closing_day=closing_day, segment=segment, horizon=horizon)
    timestamp = params_set[0]["executed_at"].replace(" ","_").replace(":","").replace("-","")

    if segment != "fbb":
        pred_name =  "prediction_tr{}_tt{}_horizon{}_on{}".format(params_set[0]['training_closing_date'], closing_day, horizon, timestamp)
    else:
        pred_name = "prediction_fbb_tr{}_tt{}_horizon{}_on{}".format(params_set[0]['training_closing_date'], closing_day, horizon, timestamp)

    return pred_name


def get_training_dates(spark, closing_day, segment, horizon):
    '''
    Return a list of dict, where every dict has executed_at and training_closing_date for this closing_day and segment
    Example:
            [{'executed_at': u'2019-05-08 10:06:22',
              'training_closing_date': u'20190307to20190307'},
             {'executed_at': u'2019-05-07 16:24:34',
              'training_closing_date': u'20190228to20190228'}]
    Note: the list is ordered by training_closing_date,executed_at
    :param spark:
    :param closing_day:
    :param segment:
    :return:
    '''
    sql_query = "select msisdn, scoring, model_executed_at from parquet.`/data/attributes/vf_es/model_outputs/model_scores/model_name={}` where predict_closing_date = '{}'".format(
        "churn_preds_" + segment, closing_day)

    df = spark.sql(sql_query)

    exe_list = df.select("model_executed_at").distinct().rdd.map(lambda xx: xx[0]).collect()

    sql_query = "select * from parquet.`/data/attributes/vf_es/model_outputs/model_parameters/model_name={}` where executed_at in ('{}') sort by training_closing_date desc, executed_at desc".format(
        "churn_preds_" + segment,
        "','".join(exe_list),
    )

    df_param = spark.sql(sql_query)

    df_param = df_param.withColumn("horizon", regexp_extract(col("extra_info"), "horizon=([0-9]*)(;|$)", 1))

    if df_param.where(col("horizon").isNotNull()).take(1):
        existing_models = df_param.where(col("horizon") == horizon)
    else:
        print("horizon value not found as expected")
        existing_models = df_param  # do not filter by horizon

    existing_models = existing_models.select("training_closing_date", "executed_at").rdd.collect()

    models = []
    for mm in existing_models:
        models.append(mm.asDict())

    return models