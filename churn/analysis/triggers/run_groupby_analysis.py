from pyspark.sql.functions import greatest, lower, upper, trim, randn
from pyspark.sql.functions import collect_set, col, lpad, lit, collect_list, desc, asc, mean as sql_mean, sum as sql_sum, datediff, count as sql_count, substring
#from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
from pyspark.sql.functions import array, regexp_extract, datediff, to_date, from_unixtime, unix_timestamp, desc, when, col, lit, udf, size, \
    array, isnan, upper, coalesce, length, lower, concat, create_map, sum as sql_sum, greatest, max as sql_max, sort_array
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql.functions import datediff
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import collect_set, concat, size, coalesce, col, lpad, struct, count as sql_count, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
from pyspark.sql.functions import array, regexp_extract
from itertools import chain
from pyspark.ml.feature import QuantileDiscretizer


LABEL_COL = "label"

def get_nifs_superoferta(spark, closing_day):
    from churn.datapreparation.general.customer_base_utils import get_customers

    df_services = get_customers(spark, date_=closing_day)

    df_services.select("SUPEROFERTA").distinct().show()

    df_services = df_services.withColumn("is_superoferta", when(col("SUPEROFERTA") == "ON19", 1).otherwise(0))
    df_nifs_superoferta = (df_services.select("nif_cliente", "is_superoferta").groupBy("nif_cliente").agg(
        sql_max('is_superoferta').alias("has_superoferta")))

    return df_nifs_superoferta


def get_discrete_var(df_tar, var, cc):

    df_tar2 = df_tar

    #     var = "nb_running_last30_gt5"
    #     cc = ["nb_running_last30_gt5_disc", 5]

    splits_dict = {}
    discretizer = QuantileDiscretizer(numBuckets=cc[1], inputCol=var, outputCol=cc[0], relativeError=0)
    bucketizer = discretizer.fit(df_tar2)
    df_tar2 = bucketizer.transform(df_tar2)
    splits_dict[cc[0]] = bucketizer.getSplits()
    print(bucketizer.getSplits())
    num_buckets = len(bucketizer.getSplits()) - 1

    df_tar2 = df_tar2.withColumn("prueba", col(cc[0]))
    mysplit = splits_dict[cc[0]]
    print(mysplit)
    labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
    for num, label in labels_dict.items():
        df_tar2 = df_tar2.withColumn("prueba", when(col("prueba") == num, labels_dict[num]).otherwise(col("prueba")))

    df_tar2.select(var, "prueba").distinct().show()



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


def filter_car(df_tar, segment="all"):

    if not segment:
        segment = "all"

    df_tar = df_tar.filter(col('CHURN_CANCELLATIONS_w8') == 0)
    print("Filtering superoferta ON19")
    df_tar = df_tar.where(col("has_superoferta") == 0)


    if "tgs_days_until_fecha_fin_dto" in df_tar.columns:
        df_tar = df_tar.withColumn("tgs_days_until_fecha_fin_dto", when(col("tgs_days_until_fecha_fin_dto")<0, -1).otherwise(col("tgs_days_until_fecha_fin_dto")))
        df_tar = df_tar.withColumn("blindaje", when(col("tgs_days_until_fecha_fin_dto")>0, 1).otherwise(0))
        df_tar = df_tar.withColumn("grado_blindaje", when( ((col("tgs_days_until_fecha_fin_dto")<=0) | (col("tgs_days_until_fecha_fin_dto").isNull())) , "NO_TIENE")
                                                    .when( ((col("tgs_days_until_fecha_fin_dto")>0) &  (col("tgs_days_until_fecha_fin_dto")<=60)), "SOFT")
                                                    .otherwise("HARD"))


    # if "nb_started_orders_last30" in df_tar.columns:
    #     print("Filtering clients without orders")
    #     df_tar = df_tar.where(col("nb_started_orders_last30")>0)

    if "inc_bill_n1_n5_net" in df_tar.columns:
        df_tar = df_tar.withColumn("inc_billing_noinc_rgus",  when( ((col("inc_bill_n1_n5_net")>0) & (col("diff_rgus_n_n2")<=0)),1).otherwise(0))
        df_tar = df_tar.withColumn("inc_billing_noinc_rgus_blindaje",  when( ((col("inc_bill_n1_n5_net")>0) & (col("diff_rgus_n_n2")<=0) & (col("blindaje")==1)),1).otherwise(0))

    if "Bill_N1_Amount_To_Pay" in df_tar.columns:
        df_tar = df_tar.withColumn("mean_price_rgu",
                                   when(col("nb_rgus") != 0, col("Bill_N1_Amount_To_Pay") / col("nb_rgus")).otherwise(-1))

    if segment == "all":
        return df_tar

    if "," in segment:
        segment = segment.split(",")
    else:
        segment = [segment]

    print("filtering by segment {}".format(segment))
    df_tar = df_tar.where(col("segment_nif").isin(segment))


    return df_tar


def run_analysis(vars_analysis, df_tar, refprevalence, refprevalence_all):

    # var1:5,var2:3,var3:4
    vars_analysis = vars_analysis.split(",")

    print(vars_analysis)

    var_dict = {vv.split(":")[0]: [vv.split(":")[0] + "_disc", int(vv.split(":")[1])] for vv in vars_analysis}


    import pprint
    pprint.pprint(var_dict)

    for var, cc in var_dict.items(): # cc = ["name of the discretized variable", num_buckets]
        if var == cc[0]:
            print("discrete variable must be different to actual column")
            sys.exit()
        if cc[0] and cc[0] in df_tar.columns:
            print("dropping {}".format(cc[0]))
            df_tar = df_tar.drop(cc[0])

    splits_dict = {}
    for var, cc in var_dict.items():
        if not var: continue
        if var == "nb_started_orders_last30":
            df_tar = df_tar.withColumn("nb_started_orders_last30_disc", when(col("nb_started_orders_last30") == 0, "orders=0").otherwise("orders>0"))
        elif var == "nb_running_last30_gt5":
            df_tar = df_tar.withColumn("nb_running_last30_gt5_disc", when(col("nb_running_last30_gt5") == 0, "nb_running_last30_gt5=0").otherwise("nb_running_last30_gt5>0"))
        elif var == "nb_running_last30_gt10":
            df_tar = df_tar.withColumn("nb_running_last30_gt10_disc", when(col("nb_running_last30_gt10") == 0, "nb_running_last30_gt10=0").otherwise("nb_running_last30_gt10>0"))
        elif var == "nb_running_last30_gt15":
            df_tar = df_tar.withColumn("nb_running_last30_gt15_disc", when(col("nb_running_last30_gt15") == 0, "nb_running_last30_gt15=0").otherwise("nb_running_last30_gt15>0"))
        elif var == "nb_running_last30_gt20":
            df_tar = df_tar.withColumn("nb_running_last30_gt20_disc", when(col("nb_running_last30_gt20") == 0, "nb_running_last30_gt20=0").otherwise("nb_running_last30_gt20>0"))
        elif var == "nb_running_last30_gt25":
            df_tar = df_tar.withColumn("nb_running_last30_gt25_disc", when(col("nb_running_last30_gt25") == 0, "nb_running_last30_gt25=0").otherwise("nb_running_last30_gt25>0"))






        elif var == "inc_billing_noinc_rgus":
            df_tar = df_tar.withColumn("inc_billing_noinc_rgus_disc", when(col("inc_billing_noinc_rgus") == 0, "inc_billing_noinc_rgus=0").when(col("inc_billing_noinc_rgus") == 1, "inc_billing_noinc_rgus==1"))
        elif var == "inc_billing_noinc_rgus":
            df_tar = df_tar.withColumn("inc_billing_noinc_rgus_disc", when(col("inc_billing_noinc_rgus") == 0, "inc_billing_noinc_rgus=0").when(col("inc_billing_noinc_rgus") == 1, "inc_billing_noinc_rgus==1"))
        #elif var == "num_calls_w2":
        #    df_tar = df_tar.withColumn("num_calls_w2_disc", when(col("num_calls_w2") == 0, "num_calls_w2=0").when(col("num_calls_w2") == 1, "num_calls_w2==1").otherwise("num_calls_w2>1"))
        elif var == "num_calls_w4":
            df_tar = df_tar.withColumn("num_calls_w4_disc", when(col("num_calls_w4") == 0, "num_calls_w4=0").when(col("num_calls_w4") == 1, "num_calls_w4==1").otherwise("num_calls_w4>1"))
        elif var == "grado_blindaje":
            df_tar = df_tar.withColumn("grado_blindaje_disc", col("grado_blindaje"))
        elif var == "segment_nif":
            df_tar = df_tar.withColumn("segment_nif_disc", col("segment_nif"))
        else:
            for ii in [0, 1]:
                print(var, cc[0])
                discretizer = QuantileDiscretizer(numBuckets=cc[1], inputCol=var, outputCol=cc[0], relativeError=0)
                bucketizer = discretizer.fit(df_tar)
                df_tar = bucketizer.transform(df_tar)
                splits_dict[cc[0]] = bucketizer.getSplits()
                print(bucketizer.getSplits())
                num_buckets = len(bucketizer.getSplits()) - 1
                print("Intento {} ** Bucketizer of variable {} returned {} buckets - requested {}".format(ii, var, num_buckets, cc[1]))
                break
                # if num_buckets < cc[1]:
                #     print("Requested {} buckets. Returned {} buckets".format(cc[1], num_buckets))
                #     df_tar = df_tar.drop(cc[0])
                #     #df_tar = df_tar.withColumn(var, col(var) + lit(0.000001) * randn())
                # else:
                #     break

    # refprevalence = df_tar.select(LABEL_COL).rdd.map(lambda r: r[label_col]).mean()
    # refprevalence_all = df_tar_all.select(label_col).rdd.map(lambda r: r[label_col]).mean()

    import pprint
    pprint.pprint(splits_dict)


    print("churn with filter", refprevalence)
    print("churn all NIFs", refprevalence_all)

    # cc = ["num_calls_w2_disc", None]
    #
    # df_tar2  = df_tar
    # df_tar2 = df_tar2.withColumn("prueba", col(cc[0]))
    # mysplit = splits_dict[cc[0]]
    # print(mysplit)
    # labels_dict = {ii:"[{},{})".format(mysplit[ii],mysplit[ii+1]) for ii in range(0, len(mysplit)-1)}
    # for num,label in labels_dict.items():
    #     df_tar2 = df_tar2.withColumn("prueba", when(col("prueba")==num, labels_dict[num]).otherwise(col("prueba")))
    #
    # df_tar2.where(col("num_calls_w2")==0).select("num_calls_w2", "prueba").distinct().show()

    ##############
    # A L L       V A R I A B L E S
    #############


    print("#" * 30)
    print("           A L L     V A R I A B L E S               ")
    print("#" * 30)

    myvars = [cc[0] for vv, cc in var_dict.items() if cc[0]]

    df_GG = df_tar.select(*myvars + ["label"]).groupBy(*myvars).agg(sql_sum("label").alias("num_churners"),
                                                                    sql_count("label").alias("num_nif")).withColumn(
        "churn_rate", col("num_churners") / col("num_nif"))

    for var, cc in var_dict.items():
        if not var or not cc[0] in splits_dict.keys(): continue
        mysplit = splits_dict[cc[0]]
        labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
        for num, label in labels_dict.items():
            df_GG = df_GG.withColumn(cc[0], when(col(cc[0]) == num, labels_dict[num]).otherwise(col(cc[0])))

    df_GG = df_GG.withColumn("LIFT", col("churn_rate") / lit(refprevalence))
    df_GG = df_GG.withColumn("LIFT_ALL", col("churn_rate") / lit(refprevalence_all))
    df_GG = df_GG.sort(desc("LIFT"))
    df_GG.show(20, truncate=False)

    print(" ")
    print(" ")
    print(" ")
    print(" ")


    ##############
    # 1    V A R I A B L E
    #############


    print("#" * 30)
    print("           1      V A R I A B L E                ")
    print("#" * 30)

    for i in range(0, len(myvars)):

        df_GG_simple = df_tar.select(*[myvars[i]] + ["label"]).groupBy(myvars[i]).agg(
            sql_sum("label").alias("num_churners"), sql_count("label").alias("num_nif")).withColumn("churn_rate",
                                                                                                       col(
                                                                                                           "num_churners") / col(
                                                                                                           "num_nif"))

        for var, cc in var_dict.items():
            if not var or not cc[0] in splits_dict.keys() or not cc[0] in df_GG_simple.columns: continue
            mysplit = splits_dict[cc[0]]
            labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
            for num, label in labels_dict.items():
                df_GG_simple = df_GG_simple.withColumn(cc[0],
                                                       when(col(cc[0]) == num, labels_dict[num]).otherwise(col(cc[0])))

        df_GG_simple = df_GG_simple.withColumn("LIFT", col("churn_rate") / lit(refprevalence))
        df_GG_simple = df_GG_simple.withColumn("LIFT_ALL", col("churn_rate") / lit(refprevalence_all))
        df_GG_simple = df_GG_simple.sort(desc("LIFT"))
        df_GG_simple.show(10, truncate=False)
        print(" .  .  .  .  .  .  .  .  .  ")


    print(" ")
    print(" ")
    print(" ")
    print(" ")

    ##############
    # 2     V A R I A B L E S
    #############

    if len(vars_analysis)>=2:

        print("#" * 30)
        print("           2      V A R I A B L E S             ")
        print("#" * 30)

        for i in range(0, len(myvars)):
            for j in range(0, len(myvars)):
                if i == j or i > j: continue

                df_GG_simple_2 = df_tar.select(*[myvars[i], myvars[j]] + ["label"]).groupBy(myvars[i], myvars[j]).agg(
                    sql_sum("label").alias("num_churners"), sql_count("label").alias("num_nif")).withColumn("churn_rate",
                                                                                                               col(
                                                                                                                   "num_churners") / col(
                                                                                                                   "num_nif"))

                for var, cc in var_dict.items():
                    if not var or not cc[0] in splits_dict.keys() or not cc[0] in df_GG_simple_2.columns: continue
                    mysplit = splits_dict[cc[0]]
                    labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
                    for num, label in labels_dict.items():
                        df_GG_simple_2 = df_GG_simple_2.withColumn(cc[0],
                                                                   when(col(cc[0]) == num, labels_dict[num]).otherwise(
                                                                       col(cc[0])))

                df_GG_simple_2 = df_GG_simple_2.withColumn("LIFT", col("churn_rate") / lit(refprevalence))
                df_GG_simple_2 = df_GG_simple_2.withColumn("LIFT_ALL", col("churn_rate") / lit(refprevalence_all))
                df_GG_simple_2 = df_GG_simple_2.sort(desc("LIFT"))
                df_GG_simple_2.show(10, truncate=False)
                print(" .  .  .  .  .  .  .  .  .  ")

        print(" ")
        print(" ")
        print(" ")
        print(" ")

    ##############
    # 3     V A R I A B L E S
    #############


    if len(vars_analysis)>=3:


        print("#" * 30)
        print("           3      V A R I A B L E S              ")
        print("#" * 30)

        for i in range(0, len(myvars)):
            for j in range(0, len(myvars)):
                for k in range(0, len(myvars)):
                    if i == j or i == k or j == k or i < j or j < k: continue

                    df_GG_simple_3 = df_tar.select(*[myvars[i], myvars[j], myvars[k]] + ["label"]).groupBy(myvars[i], myvars[j],
                                                                                                           myvars[k]).agg(
                        sql_sum("label").alias("num_churners"), sql_count("label").alias("num_nif")).withColumn("churn_rate",
                                                                                                                   col(
                                                                                                                       "num_churners") / col(
                                                                                                                       "num_nif"))
                    for var, cc in var_dict.items():
                        if not var or not cc[0] in splits_dict.keys() or not cc[0] in df_GG_simple_3.columns: continue
                        mysplit = splits_dict[cc[0]]
                        labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in range(0, len(mysplit) - 1)}
                        for num, label in labels_dict.items():
                            df_GG_simple_3 = df_GG_simple_3.withColumn(cc[0],
                                                                       when(col(cc[0]) == num, labels_dict[num]).otherwise(
                                                                           col(cc[0])))

                    df_GG_simple_3 = df_GG_simple_3.withColumn("LIFT", col("churn_rate") / lit(refprevalence))
                    df_GG_simple_3 = df_GG_simple_3.withColumn("LIFT_ALL", col("churn_rate") / lit(refprevalence_all))
                    df_GG_simple_3 = df_GG_simple_3.sort(desc("LIFT"))
                    df_GG_simple_3.show(10, truncate=False)
                    print(" .  .  .  .  .  .  .  .  .  ")

        print(" ")
        print(" ")
        print(" ")
        print(" ")


    ##############
    # 4     V A R I A B L E S
    #############

    if len(vars_analysis)>=4:


        print("#" * 30)
        print("           4      V A R I A B L E S              ")
        print("#" * 30)



        for i in range(0, len(myvars)):
            for j in range(0, len(myvars)):
                for k in range(0, len(myvars)):
                    for z in range(0, len(myvars)):
                        if i == j or i == k or j == k or k == z or i == z or i < j or j < k or k < z: continue

                        df_GG_simple_4 = df_tar.select(*[myvars[i], myvars[j], myvars[k], myvars[z]] + ["label"]).groupBy(
                            myvars[i], myvars[j], myvars[k], myvars[z]).agg(sql_sum("label").alias("num_churners"),
                                                                            sql_count("label").alias(
                                                                                "num_nif")).withColumn("churn_rate", col(
                            "num_churners") / col("num_nif"))
                        for var, cc in var_dict.items():
                            if not var or not cc[0] in splits_dict.keys() or not cc[0] in df_GG_simple_4.columns: continue
                            mysplit = splits_dict[cc[0]]
                            labels_dict = {ii: "[{},{})".format(mysplit[ii], mysplit[ii + 1]) for ii in
                                           range(0, len(mysplit) - 1)}
                            for num, label in labels_dict.items():
                                df_GG_simple_4 = df_GG_simple_4.withColumn(cc[0], when(col(cc[0]) == num,
                                                                                       labels_dict[num]).otherwise(col(cc[0])))

                        df_GG_simple_4 = df_GG_simple_4.withColumn("LIFT", col("churn_rate") / lit(refprevalence))
                        df_GG_simple_4 = df_GG_simple_4.withColumn("LIFT_ALL", col("churn_rate") / lit(refprevalence_all))
                        df_GG_simple_4 = df_GG_simple_4.sort(desc("LIFT"))
                        df_GG_simple_4.show(10, truncate=False)
                        print(" .  .  .  .  .  .  .  .  .  ")

    return df_tar

def get_car(spark, closing_day, segment=None, labeled_mini_ids=True):

    from pykhaos.utils.hdfs_functions import check_hdfs_exists


    if labeled_mini_ids:
        path_to_minids = "/data/attributes/vf_es/trigger_analysis/mini_ids/year={}/month={}/day={}".format(
        int(closing_day[:4]),
        int(closing_day[4:6]),
        int(closing_day[6:]))
    else:
        path_to_minids = "/data/attributes/vf_es/trigger_analysis/mini_ids_unlabeled/year={}/month={}/day={}".format(
        int(closing_day[:4]),
        int(closing_day[4:6]),
        int(closing_day[6:]))

    if check_hdfs_exists(path_to_minids):
        print("Found already joined minids - '{}'".format(path_to_minids))
        df_tar_all = spark.read.parquet(path_to_minids)
    else:

        print("Not found {}".format(path_to_minids))
        import sys
        sys.exit()
        # from churn.analysis.triggers.get_ids_for_analysis import get_mini_ids, get_sel_cols
        # sources = ['customer_master', 'ccc', 'orders_sla', 'averias', 'reclamaciones', 'soporte_tecnico', 'billing', 'reimbursements']
        # df_tar_all = get_mini_ids(spark, sources, date_=closing_day)

    df_tar_all = df_tar_all.join(get_nifs_superoferta(spark, closing_day), on=["nif_cliente"], how="left")
    df_tar_all = df_tar_all.where(col("segment_nif") != "Pure_prepaid")

    # la cartera de referencia es sin prepago y sin las llamadas por churn cancellation
    df_tar = filter_car(df_tar_all, segment)

    # from churn.datapreparation.general.sla_data_loader import get_additional_metrics
    # df_additional, _ = get_additional_metrics(spark, closing_day)
    # df_tar = df_tar.join(df_additional, on=["nif_cliente"], how="left")
    # df_tar = df_tar.fillna(0, subset=df_additional.columns)

    refprevalence = df_tar.select(LABEL_COL).rdd.map(lambda r: r[LABEL_COL]).mean()
    refprevalence_all = df_tar_all.select(LABEL_COL).rdd.map(lambda r: r[LABEL_COL]).mean()

    return df_tar, refprevalence, refprevalence_all


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
                        help='Closing day YYYYMMDD (same used for the car generation)')
    parser.add_argument('-v', '--vars_analysis', metavar='var1:5,var2:3,var3:1', type=str, required=True,
                        help='List of var:num_buckets')
    parser.add_argument('-s', '--segment', metavar='segment1,segment2', type=str, required=False,default="all",
                        help='List of var:num_buckets')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day = args.closing_day.split(" ")[0]
    vars_analysis = args.vars_analysis
    segment = args.segment

    if "," in closing_day:
        closing_day_list = closing_day.split(",")
    else:
        closing_day_list = [closing_day]


    print(closing_day_list)
    print(vars_analysis)
    print(segment)

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
    spark = init_spark("customer_master_mini_ids")

    from churn.analysis.triggers.get_ids_for_analysis import get_sel_cols

    numerical_feats = get_sel_cols().values()

    noninf_feats = ['NIF_CLIENTE']


    COLS = numerical_feats + noninf_feats + [LABEL_COL]

    feat_cols = numerical_feats  # notebook is not still prepared for categoricals.

    df_tar, refprevalence, refprevalence_all = get_car(spark, closing_day)

    run_analysis(vars_analysis, df_tar, refprevalence, refprevalence_all)