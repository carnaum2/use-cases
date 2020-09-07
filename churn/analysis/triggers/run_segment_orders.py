from pyspark.mllib.evaluation import BinaryClassificationMetrics
from churn.datapreparation.general.sla_data_loader import do_merge
import pandas as pd

from pyspark.sql.functions import lower, upper, trim, collect_set, collect_list, asc, desc, when, col, lit, udf, size, \
    array, upper, coalesce, length, max as sql_max, count as sql_count, min as sql_min, collect_list, desc, asc, \
    sum as sql_sum,  collect_set, col, lit, collect_list, desc, asc, mean as sql_mean, datediff, concat_ws, regexp_replace, split

from pyspark.sql.types import FloatType
import numpy as np
import datetime as dt
import time

LABEL_COL = "label"
MODEL_TRIGGERS_ORDERS = "triggers_orders"



def get_metrics(df_preds, label="", nb_deciles=10):
    preds_and_labels = df_preds.select(['model_score', 'label']).rdd.map(
        lambda r: (r['model_score'], float(r['label'])))

    my_metrics = BinaryClassificationMetrics(preds_and_labels)

    print("METRICS FOR {}".format(label))
    print("\t AUC = {}".format(my_metrics.areaUnderROC))
    if nb_deciles:
        lift = get_lift(df_preds, 'model_score', 'label', nb_deciles)

        for d, l in lift:
            print "\t" + str(d) + ": " + str(l)


def plot_hist(data):
    import numpy as np
    import matplotlib.pyplot as mplt
    import matplotlib.ticker as mtick

    binSides, binCounts = data

    N = len(binCounts)
    ind = np.arange(N)
    width = 1

    fig, ax = mplt.subplots()
    rects1 = ax.bar(ind + 0.5, binCounts, width, color='b')

    ax.set_ylabel('Frequencies')
    ax.set_title('Histogram')
    ax.set_xticks(np.arange(N + 1))
    ax.set_xticklabels(binSides)
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))

    mplt.show()



def get_ccc_attrib_saving_path(closing_day, suffix):

    return "/data/attributes/vf_es/trigger_analysis/ccc_attributes{}/year={}/month={}/day={}".format(suffix,
                                                                                                              int(closing_day[:4]),
                                                                                                              int(closing_day[4:6]),
                                                                                                              int(closing_day[6:]))



def save_ccc_attributes(df, closing_day, suffix):

    path_to_save = "/data/attributes/vf_es/trigger_analysis/ccc_attributes{}/".format(suffix)

    df = df.withColumn("day", lit(int(closing_day[6:])))
    df = df.withColumn("month", lit(int(closing_day[4:6])))
    df = df.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {}".format(path_to_save))
    (df.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved {} for closing_day {}".format(path_to_save, closing_day))




def get_car(spark, closing_day, labeled_mini_ids, rule_nb=0):
    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    # from pykhaos.utils.hdfs_functions import check_hdfs_exists
    #
    # if labeled_mini_ids:
    #     path_to_minids = "/data/attributes/vf_es/trigger_analysis/mini_ids/year={}/month={}/day={}".format(
    #         int(closing_day[:4]),
    #         int(closing_day[4:6]),
    #         int(closing_day[6:]))
    # else:
    #     path_to_minids = "/data/attributes/vf_es/trigger_analysis/mini_ids_unlabeled/year={}/month={}/day={}".format(
    #         int(closing_day[:4]),
    #         int(closing_day[4:6]),
    #         int(closing_day[6:]))
    #
    # if check_hdfs_exists(path_to_minids):
    #     print("Found already joined minids - '{}'".format(path_to_minids))
    #     df_tar_all = spark.read.parquet(path_to_minids)
    #
    # else:
    #print("Not found {}".format(path_to_minids))

    if True: # TEMPORARY
        from churn.analysis.triggers.customer_master import get_segment_msisdn_anyday


        df_base_msisdn = get_segment_msisdn_anyday(spark, closing_day)

        from pykhaos.utils.date_functions import move_date_n_cycles
        from churn.datapreparation.general.ccc_utils import get_nif_ccc_period_attributes
        from churn.analysis.triggers.customer_master import get_customer_saving_path, get_customer_master_module

        import time

        closing_day_w8 = move_date_n_cycles(closing_day, n=-8)

        ccc_attrib_path = get_ccc_attrib_saving_path(closing_day, suffix="_w8")

        if check_hdfs_exists(ccc_attrib_path):
            print("Found already a ccc attrib saving path - '{}'".format(ccc_attrib_path))
            df_ccc_w8 = spark.read.parquet(ccc_attrib_path)
        else:
            start_time_ccc = time.time()
            df_ccc_w8 = get_nif_ccc_period_attributes(spark, closing_day_w8, closing_day, df_base_msisdn, suffix="_w8")
            print("Elapsed time computing ccc atributes {}".format((time.time() - start_time_ccc) / 60.0))

            save_ccc_attributes(df_ccc_w8, closing_day, "_w8")

        df_ccc_w8 = df_ccc_w8.select("nif_cliente", "num_calls_w8", "CHURN_CANCELLATIONS_w8")

        saving_path=get_customer_saving_path(not labeled_mini_ids)+"year={}/month={}/day={}".format(int(closing_day[:4]),
                                                                                         int(closing_day[4:6]),
                                                                                         int(closing_day[6:]))
        print("Looking customer master in {}".format(saving_path))

        if check_hdfs_exists(saving_path):
            print("Found already a customer saving path - '{}'".format(saving_path))
            df_base = spark.read.parquet(saving_path)
        else:
            print("Not found {}. Generating...".format(saving_path))
            df_base = get_customer_master_module(spark, closing_day, unlabeled=(not labeled_mini_ids), save=True)
            print("Generated {} for closing_dat {}".format(saving_path, closing_day))

        df_tar_all = df_base.join(df_ccc_w8, on=["nif_cliente"], how="left")

        #from churn.datapreparation.general.sla_data_loader import get_orders_module
        #df_orders_sla = get_orders_module(spark, closing_day, exclude_clasif_list=None, days_range=[30], deadlines_range=[5,10])
        df_orders_sla = get_mini_orders_module(spark, closing_day)
        df_tar_all = df_tar_all.join(df_orders_sla, on=["nif_cliente"], how="left")


    df_tar_all = df_tar_all.fillna(0, subset=["label", "num_calls_w8", "CHURN_CANCELLATIONS_w8", "nb_rgus_cycles_2", "nb_rgus"])
    df_tar_all = df_tar_all.fillna(-1, subset=["tgs_days_until_fecha_fin_dto", "tgs_has_discount"])
    df_tar_all = df_tar_all.fillna("unknown", subset=["tgs_target_accionamiento", 'segment_nif'])
    df_tar_all = df_tar_all.withColumn("num_calls_w2", lit(0))  # Trick. Rule does not need this

    from churn.analysis.triggers.run_groupby_analysis import get_nifs_superoferta, filter_car
    df_tar_all = df_tar_all.join(get_nifs_superoferta(spark, closing_day), on=["nif_cliente"], how="left")


    # la cartera de referencia es sin prepago y sin las llamadas por churn cancellation
    df_tar_all = df_tar_all.where(col("segment_nif") != "Pure_prepaid")
    df_tar_all = filter_car(df_tar_all, segment="all")



    # . . . . . . . . . . . . . . . . . . . . . . . . .
    # WRITE HERE THE RULE YOU WANT TO APPLY
    # . . . . . . . . . . . . . . . . . . . . . . . . .

    if rule_nb == 0:
        print('Using rule: (col("num_calls_w2") >= 0) & (col("diff_rgus_n_n2") < 0) & (col("nb_started_orders_last30") > 0)')
        df_tar = df_tar_all.where(((col("num_calls_w2") >= 0) & (col("diff_rgus_n_n2") < 0) & (col("nb_started_orders_last30") > 0)))
    elif rule_nb == 1:
        print('Using rule: ((col("num_calls_w2") >= 0) & (col("nb_started_orders_last30") > 0))')
        df_tar = df_tar_all.where(((col("num_calls_w2") >= 0) & (col("nb_started_orders_last30") > 0)))
    else:
        print("Unknown rule number {}".format(rule_nb))
        import sys
        sys.exit()

    df_tar.select(col("segment_nif")).groupby("segment_nif").agg(sql_count("*")).show()

    df_tar = df_tar.cache()
    print("df_tar.count", df_tar.count())

    if not "label" in df_tar.columns:
        df_tar = df_tar.withColumn("label", lit(0))

    df_tar.select(col(LABEL_COL)).groupby(LABEL_COL).agg(sql_count("*")).show()


    if labeled_mini_ids:
        refprevalence = df_tar.select(LABEL_COL).rdd.map(lambda r: r[LABEL_COL]).mean()  # tasa de churn del SEGMENTO_2 (SEGMENT1 after la regla del trigger)
        refprevalence_all = df_tar_all.select(LABEL_COL).rdd.map(lambda r: r[LABEL_COL]).mean()  # SEGMENTO 1: quitando prepago y quitando llamantes por churn
        print(refprevalence, refprevalence_all)
    else:
        refprevalence = refprevalence_all = None


    return df_tar, refprevalence, refprevalence_all



def get_mini_orders_module(spark, closing_day):

    path_to_file = "/data/attributes/vf_es/trigger_analysis/orders_sla_csanc109/year={}/month={}/day={}".format(int(closing_day[:4]),
                                                                                                                                    int(closing_day[4:6]),
                                                                                                                                    int(closing_day[6:]))
    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    if check_hdfs_exists(path_to_file):
        print("Using file {}".format(path_to_file))
        df_orders_bytype = spark.read.load(path_to_file)
    else:
        print("build mini orders module")
        days_range = [30]
        deadlines_range = [5, 10]

        df_order = do_merge(spark, closing_day, None, days_range, deadlines_range)
        df_orders_bytype = (df_order.groupby('nif_cliente').pivot('x_clasificacion').agg(*([sql_count(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("orders_last{}".format(dd)) for dd in days_range] +
                                                                                           [sql_sum(col("flag_last{}_gt{}".format(dd, ss))).alias("nb_running_last{}_gt{}".format(dd, ss)) for dd in days_range for ss in deadlines_range] +
                                                                                           [sql_sum(col("flag_last{}_lte{}".format(dd, ss))).alias("nb_running_last{}_lte{}".format(dd, ss)) for dd in days_range for ss in deadlines_range])))


        df_agg = (df_order.groupby("NIF_CLIENTE").agg(*([sql_count(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)),
                                                                        col("days_since_start")).otherwise(None)).alias("nb_started_orders_last{}".format(dd)) for dd in days_range] +
                                                        [sql_max(when(((col("days_since_start") < dd) & (col("days_since_start") != -1)), col("days_since_start")).otherwise(None)).alias("first_order_last{}".format(dd)) for dd in days_range]
                                                        )))

        df_orders_bytype = df_orders_bytype.join(df_agg, on=["nif_cliente"], how="inner")


    nb_started_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("orders_last30")]
    nb_last30_gt5_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("last30_gt5")]
    nb_last30_gt10_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("last30_gt10")]

    df_orders_bytype = df_orders_bytype.select(*(nb_started_cols + nb_last30_gt5_cols + nb_last30_gt10_cols + ["nif_cliente"]))

    return df_orders_bytype


def get_orders_summary(spark, closing_day, df_orders_bytype, refprevalence_all, labeled_mini_ids, show_df=False, rule_nb=0):


    # join with the car of the rule, to keep only cohort NIFs
    #df_orders_bytype = df_orders_bytype.join(df_tar.select("nif_cliente", "label").distinct(), on=["nif_cliente"], how="right")
    df_orders_bytype = df_orders_bytype.withColumn("label", when(col("label") == 0, "NO_CHURN").otherwise("CHURN"))

    if show_df:
        df_orders_bytype.select('x_clasificacion').distinct().show()

        # print("df_orders_segment.count", df_orders_segment.count())
        print("nifs distintos en ordenes", df_orders_bytype.select("nif_cliente").distinct().count())

    nb_started_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("orders_last30") and not col_.startswith("nb_started") and not col_.startswith("nb_completed")]
    #days_start_first = [col_ for col_ in df_orders_bytype.columns if col_.endswith("days_first")]
    #days_start_last = [col_ for col_ in df_orders_bytype.columns if col_.endswith("days_last")]
    nb_last30_gt5_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("last30_gt5")]
    nb_last30_gt10_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("last30_gt10")]


    df_orders_bytype = df_orders_bytype.fillna(0, subset=nb_started_cols)
    df_orders_bytype = df_orders_bytype.fillna(0, subset=nb_last30_gt5_cols)
    df_orders_bytype = df_orders_bytype.fillna(0, subset=nb_last30_gt10_cols)



    df_orders_bytype_summary = (df_orders_bytype.groupby("label")
                                .agg(*([sql_count(when((col(col_) > 0), col(col_)).otherwise(None)).alias("NIFS_started_{}".format(col_)) for col_ in nb_started_cols] +
                                       [sql_mean(col(col_)).alias("avg_{}".format(col_)) for col_ in nb_started_cols] +
                                       # [sql_mean(col(col_)).alias("avg_{}".format(col_)) for col_ in days_start_first] +
                                       # [sql_mean(col(col_)).alias("avg_{}".format(col_)) for col_ in days_start_last] +
                                       [sql_count(when((col(col_) > 0), col(col_)).otherwise(None)).alias("NIFS_running_{}".format(col_)) for col_ in nb_last30_gt5_cols] +
                                       [sql_mean(col(col_)).alias("avg_{}".format(col_)) for col_ in nb_last30_gt5_cols] +
                                       [sql_count(when((col(col_) > 0), col(col_)).otherwise(None)).alias("NIFS_running_{}".format(col_)) for col_ in nb_last30_gt10_cols] +
                                       [sql_mean(col(col_)).alias("avg_{}".format(col_)) for col_ in nb_last30_gt10_cols]
                                       ))
                                )

    df_churn_within_type = df_orders_bytype_summary

    total_nifs = df_orders_bytype.count()

    NIFs_Cols = [col_ for col_ in df_churn_within_type.columns if col_.startswith("NIFS_started")]
    NIFs_running_last30_gt5_Cols = [col_ for col_ in df_churn_within_type.columns if col_.startswith("NIFS_running") and col_.endswith("last30_gt5")]
    NIFs_running_last30_gt10_Cols = [col_ for col_ in df_churn_within_type.columns if col_.startswith("NIFS_running") and col_.endswith("last30_gt10")]


    # show summary table for every set of cols
    sets_cols =  { "nb_orders_by_type" : NIFs_Cols,
                   "nb_running_orders_last30_gt5" : NIFs_running_last30_gt5_Cols,
                   "nb_running_orders_last30_gt10" : NIFs_running_last30_gt10_Cols}


    result_pd = {}
    for set_name, set_col in sets_cols.items():

        print(set_name)
        print(set_col)

        df_churn_within_type_pd = df_churn_within_type.select(*(["label"] + set_col)).toPandas()

        df_churn_within_type_pd.set_index("label", inplace=True)

        df_churn_within_type_pd.loc[:, 'Total'] = df_churn_within_type_pd.sum(numeric_only=True, axis=1)
        df_churn_within_type_pd.loc['Total'] = df_churn_within_type_pd.sum(numeric_only=True, axis=0)
        if labeled_mini_ids:
            df_churn_within_type_pd.loc['Churn_Rate_by_Type'] = df_churn_within_type_pd.loc['CHURN'] / \
                                                                df_churn_within_type_pd.loc['Total']
            if refprevalence_all:
                df_churn_within_type_pd.loc['lift'] = df_churn_within_type_pd.loc['Churn_Rate_by_Type'] / refprevalence_all

            df_churn_within_type_pd.loc['churners_rate'] = df_churn_within_type_pd.loc['CHURN'] / total_nifs

        print(set_name)
        print(df_churn_within_type_pd)

        df_churn_within_type_pd.to_csv("/var/SP/data/home/csanc109/data/triggers/df_{}_{}_rule{}_{}.csv".format(closing_day, set_name, rule_nb, time.time()), sep="|")

        result_pd[set_name] = [df_churn_within_type_pd , set_col]

    ### AVG DAYS FIRST

    if labeled_mini_ids and show_df:
        avg_cols_first = [col_ for col_ in df_orders_bytype_summary.columns if col_.startswith("avg_") and col_.endswith("first")]
        df_orders_bytype_summary.select(["label"] + avg_cols_first).toPandas()

    ### AVG DAYS LAST
    if labeled_mini_ids and show_df:
        avg_cols_last = [col_ for col_ in df_orders_bytype_summary.columns if col_.startswith("avg_") and col_.endswith("last")]
        df_orders_bytype_summary.select(["label"] + avg_cols_last).toPandas()

    return result_pd, df_orders_bytype



def extract_lift_dict(df_churn_within_type_pd, NIFs_Cols):
    print(NIFs_Cols)
    lift_dicts = (pd.DataFrame(df_churn_within_type_pd[NIFs_Cols].loc['lift'])).to_dict()["lift"]
    lift_dicts = {k.replace("NIFS_started_nb_completed_", "").replace("NIFS_started_", "").replace("_orders_last30", ""): v for k, v in lift_dicts.items()}
    # {'aumento': 7.154840007184473,
    #  'cambio': 8.452344900326093,
    #  'devolucion': 7.702031547167948}
    import pprint
    pprint.pprint(lift_dicts)
    return lift_dicts


def get_lift_dict(spark, closing_day, rule_nb=0, show_df=False):

    df_tar, refprevalence, refprevalence_all = get_car(spark, closing_day, labeled_mini_ids=True, rule_nb=rule_nb)

    result_dict, df_orders_bytype = get_orders_summary(spark, closing_day, df_tar, refprevalence_all, labeled_mini_ids=True, show_df=show_df, rule_nb=rule_nb)

    df_churn_within_type_pd =  result_dict["nb_orders_by_type"][0]
    NIFs_Cols =  result_dict["nb_orders_by_type"][1]
    lift_dicts_train = extract_lift_dict(df_churn_within_type_pd, NIFs_Cols)
    return lift_dicts_train


def assign_lifts(df_orders_bytype, lift_dicts_train, show_df=False):


    print("assign_lifts")
    print(df_orders_bytype.columns)


    nb_started_cols = [col_ for col_ in df_orders_bytype.columns if col_.endswith("orders_last30") and (not col_.startswith("nb_started")
                                                                                                   and not col_.startswith("nb_completed"))]

    print("assign_lifts", nb_started_cols)

    ### List Generation

    df_orders_bytype_lifts = df_orders_bytype.select(*(nb_started_cols + ["nif_cliente"]))
    for col_ in nb_started_cols:
        # col_='aumento_orders_last30'
        order_type = col_.replace("_orders_last30", "")
        df_orders_bytype_lifts = df_orders_bytype_lifts.withColumn("lift_{}".format(order_type), when(col(col_) > 0,  lit(lift_dicts_train[ order_type])).otherwise(0))

    lift_cols = [col_ for col_ in df_orders_bytype_lifts.columns if col_.startswith("lift_")]

    if show_df:
        df_orders_bytype_lifts.select(*(["nif_cliente"] + lift_cols)).show()

    max_udf = udf(lambda milista: float(np.max(milista)), FloatType())

    df_orders_bytype_lifts = df_orders_bytype_lifts.withColumn("array_lift", array([col_ for col_ in df_orders_bytype_lifts.columns if col_.startswith("lift_")]))
    df_orders_bytype_lifts = df_orders_bytype_lifts.withColumn("lift_max", when(size(col("array_lift")) > 0, max_udf(col("array_lift"))).otherwise(-1))

    if show_df:
        df_orders_bytype_lifts.select(*(["nif_cliente", "lift_max"] + lift_cols)).show()

    df_orders_bytype_lifts = df_orders_bytype_lifts.withColumnRenamed("nif_cliente", "nif").withColumnRenamed("lift_max", "scoring").select(*(["nif", "scoring"] + lift_cols))

    return df_orders_bytype_lifts, lift_cols


def make_lift_prediction(spark, closing_day_predict, lift_dicts_train):

    df_tar, refprevalence, refprevalence_all = get_car(spark, closing_day_predict, labeled_mini_ids=False)

    _, df_orders_bytype = get_orders_summary(spark, closing_day_predict, df_tar,
                                                                              refprevalence_all, labeled_mini_ids=False, show_df=False)

    df_orders_bytype_lifts, lift_cols = assign_lifts(df_orders_bytype, lift_dicts_train)

    return df_orders_bytype_lifts, lift_cols


def create_model_output_dataframes(spark, closing_day, df_model_scores, model_params_dict, extra_info_cols=None):
    from churn.delivery.delivery_constants import MODEL_OUTPUTS_NULL_TAG
    from churn.datapreparation.general.model_outputs_manager import ensure_types_model_scores_columns

    '''

    :param spark:
    :param closing_day:
    :param df_model_scores:
    :param model_params_dict:
        e.g. model_params_dict = {"model_level": ["nif"],
                    "training_closing_date": [closing_day],
                    "target": ["(col(num_calls_w2)>=0) &  (col(diff_rgus_n_n2)<0) & (col(nb_started_orders_last30)>0);no_prepaid"],
                    "model_path": ["/data/attributes/vf_es/trigger_analysis/mini_ids/year=2019/month=4/day=14"],# car used to find the rule
                    "metrics_path": [""],
                    "metrics_train": ["vol=17112;churn_rate=18.4;lift=7.6"], # volume after filters
                    "metrics_test": [""],
                    "varimp": ["num_calls_w2;diff_rgus_n_n2;nb_started_orders_last30"],
                    "algorithm": ["manual"],
                    "author_login": ["ds_team"],
                    "extra_info": [""],
                    }
    :param extra_info_cols:
    :return:
    '''
    executed_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S ")

    # TEMPORARY SOLUTION FOR RETURN FEED. PARTITION IS SET TO THE DATE OF NEXT FRIDAY
    from pykhaos.utils.date_functions import get_next_dow
    return_feed_execution = get_next_dow(weekday=4, from_date=dt.datetime.strptime(predict_closing_day, "%Y%m%d")).strftime("%Y%m%d")

    day_partition = int(return_feed_execution[6:])
    month_partition = int(return_feed_execution[4:6])
    year_partition = int(return_feed_execution[:4])

    print("dataframes of model outputs set with values: year={} month={} day={}".format(year_partition, month_partition, day_partition))

    '''
    MODEL PARAMETERS
    '''

    model_params_dict.update({
        "model_name": [MODEL_TRIGGERS_ORDERS],
        "executed_at": [executed_at],
        "year": [year_partition],
        "month": [month_partition],
        "day": [day_partition],
        "time": [int(executed_at.split(" ")[1].replace(":", ""))],
        "scores_extra_info_headers": [";".join(extra_info_cols)]
    })

    import pandas as pd
    df_pandas = pd.DataFrame(model_params_dict)

    df_parameters = spark.createDataFrame(df_pandas).withColumn("day", col("day").cast("integer")) \
        .withColumn("month", col("month").cast("integer")) \
        .withColumn("year", col("year").cast("integer")) \
        .withColumn("time", col("time").cast("integer"))

    '''
    MODEL SCORES
    '''
    if extra_info_cols:
        for col_ in extra_info_cols:
            df_model_scores = df_model_scores.withColumn(col_, when(coalesce(length(col(col_)), lit(0)) == 0,
                                                                    MODEL_OUTPUTS_NULL_TAG).otherwise(col(col_)))
        df_model_scores = (df_model_scores.withColumn("extra_info", concat_ws(";", *[col(col_name) for col_name in
                                                                                     extra_info_cols])).drop(
            *extra_info_cols))

    df_model_scores = (df_model_scores
                       .withColumn("prediction", lit("0"))
                       .withColumnRenamed("comb_score", "scoring")
                       .withColumn("model_name", lit(MODEL_TRIGGERS_ORDERS))
                       .withColumn("executed_at", lit(executed_at))
                       .withColumn("model_executed_at", lit(executed_at))
                       .withColumn("year", lit(year_partition).cast("integer"))
                       .withColumn("month", lit(month_partition).cast("integer"))
                       .withColumn("day", lit(day_partition).cast("integer"))
                       .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .withColumn("predict_closing_date", lit(closing_day))
                       .withColumn("model_output", lit(None))
                       )

    df_model_scores = ensure_types_model_scores_columns(df_model_scores)

    df_model_scores = df_model_scores.sort(desc("scoring"))

    return df_parameters, df_model_scores


#
# spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 churn/analysis/triggers/run_segment_orders.py -c 20190414  -p 20190521 --save 2>&1 | tee /var/SP/data/home/csanc109/logging/triggers_bytype_`date '+%Y%m%d_%H%M%S'`.log
# spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 churn/analysis/triggers/run_segment_orders.py -c 20190414 | tee /var/SP/data/home/csanc109/logging/triggers_bytype_`date '+%Y%m%d_%H%M%S'`.log


if __name__ == "__main__":


    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists
    from pykhaos.modeling.model_performance import get_lift

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD to compute lifts by type')
    parser.add_argument('-p', '--predict_closing_day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Predict closing day to generate trigger')
    parser.add_argument('-s', '--save', action='store_true', help='save to model outputs')
    parser.add_argument('--rule_nb', metavar='0', type=int, required=False,
                        help='by default rule nb is 0')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    predict_closing_day = args.predict_closing_day.split(" ")[0] if args.predict_closing_day else None
    save_trigger = args.save
    rule_nb = args.rule_nb


    import sys
    if "," in closing_day:
        print("Incorrect format for closing_day {}".format(closing_day))
        sys.exit()


    if predict_closing_day and "," in predict_closing_day:
        print("Incorrect format for predict_closing_day {}".format(predict_closing_day))
        sys.exit()

    if save_trigger and not predict_closing_day:
        print("Unable to save trigger without a predict_closing_day")



    print(closing_day)
    print(predict_closing_day)
    print(save_trigger)
    print(rule_nb, type(rule_nb))




    # +--------------+
    # | segment_nif |
    # +--------------+
    # | unknown |
    # | Pure_prepaid |
    # | Standalone_FBB |
    # | Other |
    # | Convergent |
    # | Mobile_only |
    # +--------------+


    from churn.utils.general_functions import init_spark
    spark = init_spark("run_segment_orders")

    if predict_closing_day == "auto":
        from churn.datapreparation.general.customer_base_utils import get_last_date
        predict_closing_day = get_last_date(spark)
        print("predict_closing_day computed automatically {}".format(predict_closing_day))

    print(closing_day)
    print(predict_closing_day)
    print(save_trigger)
    print(rule_nb, type(rule_nb))


    # Compute lift for a labeled closing_day
    lift_dicts_train = get_lift_dict(spark, closing_day, rule_nb)

    if predict_closing_day:

        df_orders_bytype_lifts, lift_cols = make_lift_prediction(spark, predict_closing_day, lift_dicts_train)

        from churn.datapreparation.general.model_outputs_manager import ensure_types_model_scores_columns
        from churn.delivery.delivery_constants import MODEL_OUTPUTS_NULL_TAG


        rule_desc = "(col(num_calls_w2)>=0) &  (col(diff_rgus_n_n2)<0) & (col(nb_started_orders_last30)>0);no_prepaid" if rule_nb==0 else "(col(num_calls_w2)>=0) & (col(nb_started_orders_last30)>0);no_prepaid"

        model_params_dict = {"model_level": ["nif"],
                             "training_closing_date": [closing_day],
                             "target": [rule_desc
                                 ],
                             "model_path": [
                                 "/data/attributes/vf_es/trigger_analysis/mini_ids/year=2019/month=4/day=14"],
                             # car used to find the rule
                             "metrics_path": [""],
                             "metrics_train": ["vol=17112;churn_rate=18.4;lift=7.6"],  # volume after filters
                             "metrics_test": [""],
                             "varimp": ["num_calls_w2;diff_rgus_n_n2;nb_started_orders_last30"],
                             "algorithm": ["manual"],
                             "author_login": ["ds_team"],
                             "extra_info": [""],
                             }

        df_parameters, df_model_scores = create_model_output_dataframes(spark, predict_closing_day,
                                                                        df_orders_bytype_lifts, model_params_dict,
                                                                        extra_info_cols=lift_cols)

        print(df_model_scores.count())
        df_parameters.show()

        print("df_model_scores={}".format(df_model_scores.count()))

        if save_trigger:

            print("Starting to save to model outputs")

            from churn.datapreparation.general.model_outputs_manager import insert_to_model_scores, insert_to_model_parameters
            insert_to_model_scores(df_model_scores)
            insert_to_model_parameters(df_parameters)
            print("Inserted to model outputs")
        else:
            print("Not saving results....")
    else:
        print("Not specified a predict_closing_day")