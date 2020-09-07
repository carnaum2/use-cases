#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql.types import StringType, IntegerType
from common.src.main.python.utils.hdfs_generic import *
from pyspark.sql.functions import (col,
                                    when,
                                    lit,
                                    lower,
                                    array,
                                    concat,
                                    translate,
                                    count,
                                    sum as sql_sum,
                                    max as sql_max,
                                    min as sql_min,
                                    avg as sql_avg,
                                    lpad,
                                    greatest,
                                    least,
                                    isnull,
                                    isnan,
                                    struct,
                                    substring,
                                    size,
                                    length,
                                    udf,
                                    year,
                                    month,
                                    dayofmonth,
                                    unix_timestamp,
                                    date_format,
                                    from_unixtime,
                                    datediff,
                                    to_date,
                                    desc,
                                    asc,
                                    countDistinct,
                                    row_number,
                                    regexp_replace,
                                    upper,
                                    trim)


# Number of days to compute aggregates of navcomp attributes. Applies to get_navcomp_attributes and get_volume attributes
# FIXME set to -14 when incremental attributes are added
NDAYS_LENGTH_PERIOD = -15

def get_navcomp_attributes1(spark, starting_date, process_date, level="msisdn", suffix=""):

    from functools import reduce
    import itertools
    from pykhaos.utils.date_functions import get_diff_days

    if ("nif" in level.lower()):
        level = "nif_cliente"

    print(
                "[Info get_navcomp_attributes] starting_date: " + starting_date + " - ending_date: " + process_date + " - level: " + level)

    window_length = get_diff_days(starting_date, process_date)

    print("[Info get_navcomp_attributes] Starting the computation of the feats")

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2",
                   "unknown"]

    count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["sum_count", "max_count"]))]

    days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["max_days_since_navigation",
                                                                                  "min_days_since_navigation",
                                                                                  "distinct_days_with_navigation"]))]

    null_imp_dict = dict(
        [(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [
            (x, 10000) for x in days_feats if ("min_days" in x)])

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    basedf = get_customer_base(spark, process_date).select('msisdn', 'nif_cliente')

    repart_navigdf = (
        spark
            .read
            .parquet("/data/attributes/vf_es/return_feed/data_navigation/")
            .withColumn("formatted_event_date", from_unixtime(unix_timestamp(col("event_date"), "yyyy-MM-dd")))
            .filter((col("formatted_event_date") >= from_unixtime(unix_timestamp(lit(starting_date), "yyyyMMdd"))) & (
                    col("formatted_event_date") <= from_unixtime(unix_timestamp(lit(process_date), "yyyyMMdd"))))
            .withColumn("days_since_navigation", datediff(from_unixtime(unix_timestamp(lit(process_date), "yyyyMMdd")),
                                                          col("formatted_event_date")).cast("double"))
            .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9))
            .select("msisdn", "application_name", "count", "formatted_event_date", "days_since_navigation")
            .withColumn("competitor", lit("unknown"))
            .withColumn("competitor",
                        when(col("application_name").contains("PEPEPHONE"), "PEPEPHONE").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("ORANGE"), "ORANGE").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("JAZZTEL"), "JAZZTEL").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("MOVISTAR"), "MOVISTAR").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("MASMOVIL"), "MASMOVIL").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("YOIGO"), "YOIGO").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("VODAFONE"), "VODAFONE").otherwise(col("competitor")))
            .withColumn("competitor",
                        when(col("application_name").contains("LOWI"), "LOWI").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("O2"), "O2").otherwise(col("competitor")))
            .join(basedf, ['msisdn'], 'inner')
            .repartition(400)
    )

    print("[Info get_navcomp_attributes] After joining the base - Volume of repart_navigdf: " + str(
        repart_navigdf.count()) + " - Distinct msisdn: " + str(
        repart_navigdf.select('msisdn').distinct().count()) + " - Distinct nif: " + str(
        repart_navigdf.select('nif_cliente').distinct().count()))

    repart_navigdf = (
        repart_navigdf
            .groupBy(level)
            .pivot("competitor", competitors)
            .agg(sql_sum("count").alias("sum_count"),
                 sql_max("count").alias("max_count"),
                 sql_max("days_since_navigation").alias("max_days_since_navigation"),
                 sql_min("days_since_navigation").alias("min_days_since_navigation"),
                 countDistinct("formatted_event_date").alias("distinct_days_with_navigation"))
            .na
            .fill(null_imp_dict)
            .withColumn("sum_count_vdf", reduce(lambda a, b: a + b, [col(x) for x in count_feats if
                                                                     (("sum_count" in x) & ("VODAFONE" in x))]))
            .withColumn("sum_count_comps", reduce(lambda a, b: a + b, [col(x) for x in count_feats if
                                                                       (("sum_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("num_distinct_comps", reduce(lambda a, b: a + b,
                                                     [when(col(x) > 0, 1.0).otherwise(0.0) for x in count_feats if
                                                      (("sum_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("max_count_comps",
                        greatest(*[x for x in count_feats if (("max_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("sum_distinct_days_with_navigation_vdf", reduce(lambda a, b: a + b,
                                                                        [col(x) for x in days_feats if ((
                                                                                                                    "distinct_days_with_navigation" in x) & (
                                                                                                                    "VODAFONE" in x))]))
            .withColumn("norm_sum_distinct_days_with_navigation_vdf",
                        col("sum_distinct_days_with_navigation_vdf").cast('double') / lit(window_length).cast('double'))
            .withColumn("sum_distinct_days_with_navigation_comps", reduce(lambda a, b: a + b,
                                                                          [col(x) for x in days_feats if ((
                                                                                                                      "distinct_days_with_navigation" in x) & (
                                                                                                                      "VODAFONE" not in x))]))
            .withColumn("norm_sum_distinct_days_with_navigation_comps",
                        col("sum_distinct_days_with_navigation_comps").cast('double') / lit(window_length).cast(
                            'double'))
            .withColumn("min_days_since_navigation_comps",
                        least(*[x for x in days_feats if (("min_days_since_navigation" in x) & ("VODAFONE" not in x))]))
            .withColumn("norm_min_days_since_navigation_comps",
                        col("min_days_since_navigation_comps").cast('double') / lit(window_length).cast('double'))
            .withColumn("max_days_since_navigation_comps", greatest(
            *[x for x in days_feats if (("max_days_since_navigation" in x) & ("VODAFONE" not in x))]))
            .withColumn("norm_max_days_since_navigation_comps",
                        col("max_days_since_navigation_comps").cast('double') / lit(window_length).cast('double'))
    )

    cols = list(set(repart_navigdf.columns) - {level})

    for c in cols:
        repart_navigdf = repart_navigdf.withColumnRenamed(c, c + suffix)


    print("[Info get_navcomp_attributes] After pivoting - Volume of repart_navigdf: " + str(repart_navigdf.count()) + " - Distinct " + level + ": " + str(repart_navigdf.select(level).distinct().count()))

    print("[Infoget_navcomp_attributes] Competitors web extraction completed")

    # val final_cols = List("msisdn") ++ Metadata.getProcessedNavigationColumns()

    # agg_navigdf.select(final_cols.head, final_cols.tail:_*)

    return repart_navigdf

def get_navcomp_attributes(spark, starting_date, process_date, level="msisdn", suffix="", orig_path=True):

    from functools import reduce
    import itertools
    from pykhaos.utils.date_functions import get_diff_days

    if("nif" in level.lower()):
        level = "nif_cliente"

    print("[Info get_navcomp_attributes] starting_date: " + starting_date + " - ending_date: " + process_date + " - level: " + level)

    window_length = get_diff_days(starting_date, process_date)

    print("[Info get_navcomp_attributes] Starting the computation of the feats")

    competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown"]

    count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["sum_count", "max_count"]))]

    days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(competitors, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]

    null_imp_dict = dict([(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if(("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if("min_days" in x)])

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base

    basedf = get_customer_base(spark, process_date).select('msisdn', 'nif_cliente')

    path_to_read = "/data/attributes/vf_es/return_feed/data_navigation/" if orig_path==True else "/data/udf/vf_es/netscout/dailyMSISDNApplicationName/"
    apps_names = [u'WEB_O2_HTTPS',
                 u'WEB_MOVISTAR_HTTPS',
                 u'WEB_VODAFONE_HTTPS',
                 u'WEB_YOIGO_HTTP',
                 u'WEB_LOWI_HTTPS',
                 u'WEB_ORANGE_HTTP',
                 u'WEB_JAZZTEL_HTTP',
                 u'WEB_MASMOVIL_HTTP',
                 u'WEB_VODAFONE_HTTP',
                 u'WEB_MOVISTAR_HTTP',
                 u'WEB_ORANGE_HTTPS',
                 u'WEB_PEPEPHONE_HTTPS',
                 u'WEB_PEPEPHONE_HTTP',
                 u'WEB_JAZZTEL_HTTPS',
                 u'WEB_LOWI_HTTP',
                 u'WEB_MASMOVIL_HTTPS',
                 u'WEB_YOIGO_HTTPS',
                 u'WEB_O2_HTTP']

    print("Reading from {}".format(path_to_read))

    repart_navigdf = (spark.read.parquet(path_to_read))

    if not orig_path:
        print("Adding event_date")
        repart_navigdf = (repart_navigdf.where((col("SUM_userplane_upload_bytes_count") + col("SUM_userplane_download_bytes_count")) > 524288)
                                        .where(col("application_name").isin(apps_names))
                                        .withColumn("event_date",  concat(col('year'), lit("-"), lpad(col('month'), 2, '0'), lit("-"), lpad(col('day'), 2, '0'))))

    repart_navigdf = (repart_navigdf.withColumn("formatted_event_date", from_unixtime(unix_timestamp(col("event_date"), "yyyy-MM-dd")))
            .filter((col("formatted_event_date") >= from_unixtime(unix_timestamp(lit(starting_date), "yyyyMMdd"))) & (col("formatted_event_date") <= from_unixtime(unix_timestamp(lit(process_date), "yyyyMMdd"))))
            .withColumn("days_since_navigation", datediff(from_unixtime(unix_timestamp(lit(process_date), "yyyyMMdd")), col("formatted_event_date")).cast("double"))
            .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9))
            .select("msisdn", "application_name", "count", "formatted_event_date", "days_since_navigation")
      )


    repart_navigdf = (repart_navigdf
            .withColumn("competitor", lit("unknown"))
            .withColumn("competitor", when(col("application_name").contains("PEPEPHONE"), "PEPEPHONE").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("ORANGE"), "ORANGE").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("JAZZTEL"), "JAZZTEL").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("MOVISTAR"), "MOVISTAR").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("MASMOVIL"), "MASMOVIL").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("YOIGO"), "YOIGO").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("VODAFONE"), "VODAFONE").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("LOWI"), "LOWI").otherwise(col("competitor")))
            .withColumn("competitor", when(col("application_name").contains("O2"), "O2").otherwise(col("competitor")))
            .join(basedf, ['msisdn'], 'inner')
            .repartition(400)
    )

    def get_most_consulted_operator(consulted_list, days_since_1st_navigation_list):
        if not consulted_list: return "None"
        combined_list = [(a, b, c) for a, b, c in zip(consulted_list, days_since_1st_navigation_list, competitors)]
        # sort by consulted list and then by days since 1st navigation
        sorted_list = sorted(combined_list, key=lambda x: (x[0], x[1]), reverse=True)
        return sorted_list[0][2] if sorted_list else "None"

    get_most_consulted_operator_udf = udf(lambda x, y: get_most_consulted_operator(x, y), StringType())

    #print("[Info get_navcomp_attributes] After joining the base - Volume of repart_navigdf: " + str(repart_navigdf.count()) + " - Distinct msisdn: " + str(repart_navigdf.select('msisdn').distinct().count()) + " - Distinct nif: " + str(repart_navigdf.select('nif_cliente').distinct().count()))

    repart_navigdf = (
        repart_navigdf
            .groupBy(level)
            .pivot("competitor", competitors)
            .agg(sql_sum("count").alias("sum_count"),
                 sql_max("count").alias("max_count"),
                 sql_max("days_since_navigation").alias("max_days_since_navigation"),
                 sql_min("days_since_navigation").alias("min_days_since_navigation"),
                 countDistinct("formatted_event_date").alias("distinct_days_with_navigation"))
            .na
            .fill(null_imp_dict)
            .withColumn("sum_count_vdf", reduce(lambda a, b: a + b, [col(x) for x in count_feats if(("sum_count" in x) & ("VODAFONE" in x))]))
            .withColumn("sum_count_comps", reduce(lambda a, b: a + b, [col(x) for x in count_feats if (("sum_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("num_distinct_comps", reduce(lambda a, b: a + b, [when(col(x) > 0, 1.0).otherwise(0.0) for x in count_feats if(("sum_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("max_count_comps", greatest(*[x for x in count_feats if (("max_count" in x) & ("VODAFONE" not in x))]))
            .withColumn("sum_distinct_days_with_navigation_vdf", reduce(lambda a, b: a + b, [col(x) for x in days_feats if (("distinct_days_with_navigation" in x) & ("VODAFONE" in x))]))
            .withColumn("norm_sum_distinct_days_with_navigation_vdf", col("sum_distinct_days_with_navigation_vdf").cast('double')/lit(window_length).cast('double'))
            .withColumn("sum_distinct_days_with_navigation_comps", reduce(lambda a, b: a + b, [col(x) for x in days_feats if (("distinct_days_with_navigation" in x) & ("VODAFONE" not in x))]))
            .withColumn("norm_sum_distinct_days_with_navigation_comps", col("sum_distinct_days_with_navigation_comps").cast('double') / lit(window_length).cast('double'))
            .withColumn("min_days_since_navigation_comps", least(*[x for x in days_feats if (("min_days_since_navigation" in x) & ("VODAFONE" not in x))]))
            .withColumn("norm_min_days_since_navigation_comps", col("min_days_since_navigation_comps").cast('double') / lit(window_length).cast('double'))
            .withColumn("max_days_since_navigation_comps", greatest(*[x for x in days_feats if (("max_days_since_navigation" in x) & ("VODAFONE" not in x))]))
            .withColumn("norm_max_days_since_navigation_comps", col("max_days_since_navigation_comps").cast('double') / lit(window_length).cast('double'))
            .withColumn('consulted_list', array(*[col_ + "_sum_count" for col_ in competitors]))
            .withColumn('days_since_1st_navigation_list',  array(*[col_ + "_max_days_since_navigation" for col_ in competitors]))
            .withColumn('most_consulted_operator', get_most_consulted_operator_udf(col('consulted_list'), col("days_since_1st_navigation_list")))
            .drop('consulted_list', 'days_since_1st_navigation_list'))

    #print("[Info get_navcomp_attributes] After pivoting - Volume of repart_navigdf: " + str(repart_navigdf.count()) + " - Distinct " + level + ": " + str(repart_navigdf.select(level).distinct().count()))

    print("[Infoget_navcomp_attributes] Competitors web extraction completed")

    #val final_cols = List("msisdn") ++ Metadata.getProcessedNavigationColumns()

    #agg_navigdf.select(final_cols.head, final_cols.tail:_*)

    return repart_navigdf

def get_incremental_attributes(spark, prevdf, initdf, level, cols, suffix, name):

    initdf = initdf.join(prevdf, [level], 'inner')

    for c in cols:
        initdf = initdf.withColumn("inc_" + c, col(c) - col(c + suffix))

    selcols = ["inc_" + c for c in cols]
    selcols.extend([level])

    resultdf = initdf.select(selcols)

    inc_cols = ["inc_" + c for c in cols]

    for c in inc_cols:
        resultdf = resultdf.withColumnRenamed(c, c + name)

    return resultdf

def get_navcomp_attributes_all(spark, end_date, level="msisdn"):

    from pykhaos.utils.date_functions import move_date_n_cycles

    # Generating dates

    w2 = move_date_n_cycles(end_date, -2)

    w4 = move_date_n_cycles(end_date, -4)

    # Aggregated attributes

    agg_w2_navcomp_attributes = get_navcomp_attributes1(spark, w2, end_date, level, "_w2")

    agg_w4_navcomp_attributes = get_navcomp_attributes1(spark, w4, end_date, level, "_w4")

    from churn.analysis.triggers.navcomp.metadata import get_metadata

    navcomp_metadata = get_metadata(spark, sources=['navcomp'])

    navcomp_map_tmp = navcomp_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    navcomp_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in navcomp_map_tmp])

    # Adding w2 attributes

    navcomp_map_w2 = navcomp_map

    for k in navcomp_map_w2.keys():
        navcomp_map_w2[k + '_w2'] = navcomp_map_w2.pop(k)

    navcomp_attributes = agg_w4_navcomp_attributes.join(agg_w2_navcomp_attributes, [level], 'left').na.fill(navcomp_map_w2)

    print "[Info get_navcomp_attributes_all] navcomp_attributes -> size = " + str(navcomp_attributes.count()) + " - nb_distinct_" + level + " = " + str(navcomp_attributes.select(level).distinct().count())

    # Incremental attributes

    # Last 2 cycles vs previous 2 cycles

    last2w_navcomp_attributes =  get_navcomp_attributes1(spark, w2, end_date, level)

    previous2w_navcomp_attributes = get_navcomp_attributes1(spark, w4, w2, level, "_prevw2")

    #inc_cols = [x[0] for x in navcomp_map_tmp if x[2] == 'numeric']

    inc_cols = list(set(last2w_navcomp_attributes.columns) - {level})

    inc_w2_navcomp_attributes = get_incremental_attributes(spark, previous2w_navcomp_attributes, last2w_navcomp_attributes, level, inc_cols, "_prevw2", "_w2vsw2")

    inc_w2_navcomp_attributes.show()

    inc_w2_navcomp_attributes.printSchema()

    print "[Info get_navcomp_attributes_all] inc_w2_navcomp_attributes -> size = " + str(inc_w2_navcomp_attributes.count()) + " - nb_distinct_" + level + " = " + str(inc_w2_navcomp_attributes.select(level).distinct().count())

    navcomp_attributes = navcomp_attributes.join(inc_w2_navcomp_attributes, [level], 'left').na.fill(0)

    print "[Info get_navcomp_attributes_all] navcomp_attributes -> size = " + str(navcomp_attributes.count()) + " - nb_distinct_" + level + " = " + str(navcomp_attributes.select(level).distinct().count())

    return navcomp_attributes


# copied into churn_nrt.... customer_base
def _get_customer_master_feats(customer_tr_df):

    class UDFclass:

        @staticmethod
        def compute_rgus_attrib(current_list, rgu_name):
            import collections
            rgus_dict = collections.Counter(current_list) if current_list is not None else {}
            if rgu_name not in rgus_dict:
                return 0
            else:
                return rgus_dict[rgu_name]

    compute_rgus_attrib_udf = udf(lambda x, y: UDFclass.compute_rgus_attrib(x, y), IntegerType())

    customer_tr_df = customer_tr_df.withColumn("nb_rgus_mobile", compute_rgus_attrib_udf("rgus_list", lit("mobile")))
    customer_tr_df = customer_tr_df.withColumn("nb_rgus_tv", compute_rgus_attrib_udf("rgus_list", lit("tv")))
    customer_tr_df = customer_tr_df.withColumn("nb_rgus_fixed", compute_rgus_attrib_udf("rgus_list", lit("fixed")))
    customer_tr_df = customer_tr_df.withColumn("nb_rgus_fbb", compute_rgus_attrib_udf("rgus_list", lit("fbb")))
    customer_tr_df = customer_tr_df.withColumn("nb_rgus_bam", compute_rgus_attrib_udf("rgus_list", lit("bam")))

    return customer_tr_df


def get_customer_base_navcomp(spark, date_, verbose=False):
    '''
    Build the customer base for the navcomp model. This module is build as a join of customer_base and customer_master modules
    :param spark:
    :param date_:
    :return:
    '''
    from churn.analysis.triggers.navcomp.metadata import get_metadata

    from churn.analysis.triggers.base_utils.base_utils import get_customer_base
    add_columns_customer = ["birth_date", "fecha_naci", 'CLASE_CLI_COD_CLASE_CLIENTE', 'X_CLIENTE_PRUEBA', "TIPO_DOCUMENTO"]
    # add_columns = ["TARIFF", 'COD_ESTADO_GENERAL', "srv_basic"]

    base_df = get_customer_base(spark, date_, add_columns_customer=add_columns_customer).filter(col('rgu') == 'mobile').select('msisdn', 'nif_cliente',
                                                                                                                               'num_cliente', 'birth_date',
                                                                                                                               'fecha_naci')
    base_df = base_df.drop_duplicates(["msisdn"])

    base_df = base_df.drop_duplicates(['msisdn', 'nif_cliente', 'num_cliente'])
    base_df = (base_df.withColumn("birth_date", when((col("birth_date").isNull() | (col("birth_date") == 1753)), col("fecha_naci")).otherwise(col("birth_date")).cast(IntegerType()))
               .withColumn("age", when(col("birth_date") != 1753, lit(int(date_[:4])) - col("birth_date")).otherwise(-1)))
    base_df = base_df.withColumn("age_disc", when(col("age").isNull(), "other")
                                 .when((col("age") >   0) & (col("age") < 20), "<20")
                                 .when((col("age") >= 20) & (col("age") < 25), "[20-25)")
                                 .when((col("age") >= 25) & (col("age") < 30),"[25-30)")
                                 .when((col("age") >= 30) & (col("age") < 35), "[30-35)")
                                 .when((col("age") >= 35) & (col("age") < 40), "[35-40)")
                                 .when((col("age") >= 40) & (col("age") < 45), "[40-45)")
                                 .when((col("age") >= 45) & (col("age") < 50), "[45-50)")
                                 .when((col("age") >= 50) & (col("age") < 55), "[50-55)")
                                 .when((col("age") >= 55) & (col("age") < 60), "[55-60)")
                                 .when((col("age") >= 60) & (col("age") < 65), "[60-65)")
                                 .when(col("age") >= 65, ">=65").otherwise("other")).drop("birth_date")
    # base_df.columns at this point: ['msisdn', 'nif_cliente', 'num_cliente', 'fecha_naci', 'age', 'age_disc']

    from churn.analysis.triggers.orders.customer_master import get_customer_master

    customer_metadata = get_metadata(spark, sources=['customer'])

    customer_feats = customer_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['nif_cliente']

    customer_tr_df = (get_customer_master(spark, date_, unlabeled=True).filter((col('segment_nif').isin("Other", "Convergent", "Mobile_only")) & (col("segment_nif").isNotNull())).drop_duplicates(["nif_cliente"]))

    customer_tr_df = (
        _get_customer_master_feats(customer_tr_df).select('nif_cliente', 'segment_nif', 'nb_rgus', 'rgus_list', 'tgs_days_until_fecha_fin_dto',
                                                          'nb_rgus_mobile', 'nb_rgus_tv', 'nb_rgus_fixed',
                                                          'nb_rgus_fbb', 'nb_rgus_bam'))

    customer_map_tmp = customer_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

    customer_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in customer_map_tmp])

    base_df = base_df.join(customer_tr_df, ['nif_cliente'], 'inner').na.fill(customer_map)

    base_df = base_df.select(*(customer_feats + ["msisdn"])).na.fill(customer_map)


    base_df = base_df.cache()


    print '[Info navcomp_model] Tr set - The volume of base_df is ' + str(base_df.count()) + ' - The number of distinct NIFs is ' + str(base_df.select('nif_cliente').distinct().count())

    return base_df

def get_navcomp_car_msisdn(spark, date_, sources=None, save_ = True, verbose = True):
    '''

    :param spark:
    :param date_:
    :param save_:
    :param verbose:
    :return:
    '''

    # FIXME add level as input parameter

    from churn.analysis.triggers.navcomp.metadata import METADATA_STANDARD_MODULES

    sources = METADATA_STANDARD_MODULES if not sources else sources

    print("CSANC109 Called get_navcomp_car_msisdn with date={} save={} and verbose={}".format(date_, save_, verbose))
    print("SOURCES", sources)

    year_ = date_[0:4]

    month_ = str(int(date_[4:6]))

    day_ = str(int(date_[6:8]))

    from churn.analysis.triggers.navcomp.metadata import get_metadata
    from pykhaos.utils.date_functions import move_date_n_days

    starting_date = move_date_n_days(date_, n=NDAYS_LENGTH_PERIOD)

    navcomp_tr_df = get_navcomp_attributes(spark, starting_date, date_, 'msisdn')
    navcomp_tr_df = navcomp_tr_df.repartition(400)
    navcomp_tr_df.cache()

    if(verbose):
        print '[Info navcomp_model] Tr set - The volume of navcomp_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())


    base_df = get_customer_base_navcomp(spark, date_, verbose)

    navcomp_tr_df = navcomp_tr_df.join(base_df, ['msisdn'], 'inner')

    if(verbose):
        print '[Info navcomp_model] Tr set - The volume of navcomp_tr_df after adding IDs from the base is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    if "ccc" in sources:
        # Adding CCC feats

        from churn.analysis.triggers.ccc_utils.ccc_utils import get_ccc_attributes

        ccc_metadata = get_metadata(spark, sources=['ccc'])

        ccc_feats = ccc_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['msisdn']

        ccc_map_tmp = ccc_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        ccc_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in ccc_map_tmp])

        ccc_tr_df = get_ccc_attributes(spark, date_, base_df.select("msisdn")).select(ccc_feats)
        ccc_tr_df = ccc_tr_df.cache()
        if(verbose):
            print '[Info navcomp_model] Tr set - The volume of ccc_tr_df is ' + str(ccc_tr_df.count()) + ' - The number of distinct MSISDNs in ccc_tr_df is ' + str(ccc_tr_df.select('msisdn').distinct().count())

        navcomp_tr_df = navcomp_tr_df.join(ccc_tr_df, ['msisdn'], 'left').na.fill(ccc_map)

        if(verbose):
           print '[Info navcomp_model] Tr set - Volume after adding ccc_tr_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    else:
        print("Skipped ccc attributes")

    if "spinners" in sources:
        # Adding spinners

        from churn.analysis.triggers.spinners_utils.spinners_utils import get_spinners_attributes_nif

        spinners_metadata = get_metadata(spark, sources=['spinners'])

        spinners_feats = spinners_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['nif_cliente']

        spinners_map_tmp = spinners_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()

        spinners_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in spinners_map_tmp])

        spinners_df = get_spinners_attributes_nif(spark, date_).select(spinners_feats)

        navcomp_tr_df = navcomp_tr_df.join(spinners_df, ['nif_cliente'], 'left').na.fill(spinners_map)

    else:
        print("Skipped spinners attributes")

    if(verbose):
        print '[Info navcomp_model] Tr set - Volume after adding spinners_df is ' + str(navcomp_tr_df.count()) + ' - The number of distinct MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())

    if "scores" in sources:
        print("CSANC109 about to add latest scores stored in model_outputs")
        df_scores = get_latest_scores(spark, date_)
        mean_score = df_scores.select(sql_avg("scoring").alias('mean_score')).rdd.first()['mean_score']

        #FIXME add in metadata that scoring has to be imputed with mean
        navcomp_tr_df = navcomp_tr_df.join(df_scores.select("msisdn", "scoring"), ['msisdn'], 'left').withColumnRenamed("scoring", "latest_score")
        print("Imputing null values of latest_score column with mean = {}".format(mean_score))
        navcomp_tr_df = navcomp_tr_df.fillna({"latest_score" : mean_score})
        navcomp_tr_df = navcomp_tr_df.cache()
    else:
        print("Skipped adding scores attributes")

    if "volumen" in sources:
        print("Adding volumen attributes")
        volumen_metadata = get_metadata(spark, sources=['volumen'])
        volumen_feats = volumen_metadata.select('feature').rdd.map(lambda x: x['feature']).collect() + ['msisdn']

        volumen_map_tmp = volumen_metadata.select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
        volumen_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else (x[0], float(x[1])) for x in volumen_map_tmp])
        df_get_volumen = get_volumen_attributes(spark, date_, starting_date).select(volumen_feats)
        navcomp_tr_df = navcomp_tr_df.join(df_get_volumen, ['msisdn'], 'left').na.fill(volumen_map)
        print("Added volumen attributes")
    else:
        print("Skipped volumen source")

    navcomp_tr_df = navcomp_tr_df.cache()
    print("Car generated volume={} save_={}".format(navcomp_tr_df.count(), save_))


    if save_:

        navcomp_set_path = '/data/udf/vf_es/churn/triggers/navcomp_msisdn_data/'
        print("Starting to saved in {} with partition year={}/month={}/day={}".format(navcomp_set_path, year_, month_, day_))

        navcomp_tr_df.repartition(200)\
            .withColumn('year', lit(year_)) \
            .withColumn('month', lit(month_)) \
            .withColumn('day', lit(day_)) \
            .write \
            .partitionBy('year', 'month', 'day')\
            .mode("append")\
            .format("parquet")\
            .save(navcomp_set_path)

        print("Saved in {} with partition year={}/month={}/day={}".format(navcomp_set_path, year_, month_, day_))

    return navcomp_tr_df


def get_unlabeled_set_msisdn(spark, date_, sources=None, save_ = False, verbose = False):
    '''
    Return an unlabeled car by msisdn
    :param spark:
    :param date_:
    :param sources:
    :param save_:
    :param verbose:
    :return:
    '''
    print("CSANC109 Called get_unlabeled_set_msisdn with date={} save={} and verbose={}".format(date_, save_, verbose))

    # FIXME add level as input parameter

    year_ = date_[0:4]

    month_ = str(int(date_[4:6]))

    day_ = str(int(date_[6:8]))

    navcomp_set_path = '/data/udf/vf_es/churn/triggers/navcomp_msisdn_data/year=' + str(year_) + '/month=' + str(int(month_)) + '/day=' + str(int(day_))

    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    data_exists = check_hdfs_exists(navcomp_set_path)
    print("path {} exists={}".format(navcomp_set_path, data_exists))
    navcomp_tr_df = (spark.read.parquet(navcomp_set_path)) if data_exists==True else get_navcomp_car_msisdn(spark, date_, sources, save_, verbose)

    print('Filtering segments different to ["Other", "Convergent", "Mobile_only"]')
    navcomp_tr_df = navcomp_tr_df.where(col('segment_nif').isin("Other", "Convergent", "Mobile_only"))

    return navcomp_tr_df


def get_labeled_set_msisdn(spark, date_, sources='all', save_ = False, verbose = False):

    print("CSANC109 Called get_labeled_set_msisdn with date={} save={} and verbose={}".format(date_, save_, verbose))


    # FIXME add level as input parameter

    navcomp_tr_df = get_unlabeled_set_msisdn(spark, date_, sources, save_ = save_, verbose = verbose)

    # Labeling

    from churn.analysis.triggers.base_utils.base_utils import get_mobile_portout_requests

    from pykhaos.utils.date_functions import move_date_n_days

    end_port = move_date_n_days(date_, 15)

    target = get_mobile_portout_requests(spark, date_, end_port).withColumnRenamed('label_mob', 'label').select('msisdn', 'label')

    navcomp_tr_df = navcomp_tr_df.join(target, ['msisdn'], 'left').na.fill({'label': 0.0})

    if verbose:
        print '[Info get_labeled_set_msisdn] Total volume of the labeled set for ' + str(date_) + ' is ' + str(navcomp_tr_df.count()) + ' - Number of MSISDNs is ' + str(navcomp_tr_df.select('msisdn').distinct().count())
    #print '[Info navcomp_model] Percentage of churners is ' + str(navcomp_tr_df.select(sql_avg('label').alias('churn')).rdd.map(lambda x: x['churn']).collect()[0])

    navcomp_tr_df.groupBy('label').agg(count('*').alias('num_services')).show()

    print '[Info get_labeled_set_msisdn] Churn proportion above'

    return navcomp_tr_df




def get_latest_scores(spark, closing_day):

    df_scores_all = spark.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/").where(col("predict_closing_date")<closing_day)

    df_dates = df_scores_all.select("year", "month", "day").distinct()
    df_dates = df_dates.withColumn("part_date",  concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0')))
    dates = df_dates.where(col("part_date") <= closing_day).sort(desc("part_date")).limit(1).rdd

    print(dates.collect())

    print(dates.take(1))
    if dates.take(1):
        year_ = dates.map(lambda x: x["year"]).collect()[0]
        month_ = dates.map(lambda x: x["month"]).collect()[0]
        day_ = dates.map(lambda x: x["day"]).collect()[0]

        scores_path = "/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/year={}/month={}/day={}".format(year_, month_, day_)
        print("Reading scores from path '{}'".format(scores_path))
        df_scores = spark.read.load(scores_path)
        return df_scores
    else:
        return None


def get_volumen_attributes(spark,closing_day, starting_day):

    '''
    Function to obtain new attributes related with the volumen of navigation
    in the webs of competitors.
    - nDays: number of days before the closing day used to gather navigation data
    - nDaysPorta: number of days after the closing day to label the churners in the navigation data
    '''

    from pykhaos.utils.date_functions import move_date_n_days, months_range
    from churn_nrt.src.utils.pyspark_utils import union_all

    from datetime import datetime
    import numpy as np
    from pyspark.sql.types import TimestampType
    from pyspark.sql import Row, DataFrame, Column, Window

    print("Getting volume attributes for closing_day {} and starting_day {}".format(closing_day, starting_day))
    # starting_day=move_date_n_days(closing_day,nDays)

    ## Reading the navigation data
    ################################

    yyyy_mm_list = months_range(starting_day[:6], closing_day[:6])
    df_list = []

    for yyyy_mm in yyyy_mm_list:
        print("Reading /data/attributes/vf_es/return_feed/data_navigation/year={}/month={}".format(yyyy_mm[:4], int(yyyy_mm[4:6])))
        navcomp1 = (spark.read.load("/data/attributes/vf_es/return_feed/data_navigation/year={}/month={}".format(yyyy_mm[:4],int(yyyy_mm[4:6]))))

        navcomp1 = navcomp1.withColumn('month', lit(str(yyyy_mm[4:6])))
        navcomp1 = navcomp1.withColumn('year', lit(str(yyyy_mm[:4])))

        df_list.append(navcomp1)

    navcomp = union_all(df_list)

    navcomp = navcomp.withColumn("event_date_date", navcomp["event_date"].cast(TimestampType()))

    ## Calculating new attributes
    ##############################

    volumenes = navcomp.select('subscriber_msisdn').drop_duplicates()
    diff_days = (datetime.strptime(closing_day, '%Y%m%d') - datetime.strptime(starting_day, '%Y%m%d')).days

    dtClosing_day = datetime.strptime(closing_day, '%Y%m%d')
    fecha_tmp = dtClosing_day

    for dday in np.arange(diff_days / 7) + 1:

        if dday == 2:
            selWeek = 'prev'
        else:
            selWeek = dday

        fecha = move_date_n_days(closing_day, -dday * 7, str_fmt="%Y%m%d")
        fecha_Int = datetime.strptime(fecha, '%Y%m%d')

        print(fecha_Int, fecha_tmp)

        navcompSel = navcomp.filter((col('event_date_date') >= fecha_Int) & (col('event_date_date') < fecha_tmp))

        competidores = ['PEPEPHONE', 'JAZZTEL', 'MOVISTAR', 'MASMOVIL', 'YOIGO', 'LOWI', 'O2', 'ORANGE']
        operadores = ['PEPEPHONE', 'JAZZTEL', 'MOVISTAR', 'MASMOVIL', 'YOIGO', 'VODAFONE', 'LOWI', 'O2', 'ORANGE']


        window = Window.partitionBy("subscriber_msisdn")

        for comp in operadores:

            dftemp = (navcompSel.filter(col('application_name').like("%" + comp + "%")).withColumn(
                'total_upload_' + comp + '_w' + str(selWeek), sql_sum('SUM_userplane_upload_bytes_count').over(window))
                      .withColumn('min_upload_' + comp + '_w' + str(selWeek),
                                  sql_min('SUM_userplane_upload_bytes_count').over(window))
                      .withColumn('max_upload_' + comp + '_w' + str(selWeek),
                                  sql_max('SUM_userplane_upload_bytes_count').over(window))
                      .withColumn('avg_upload_' + comp + '_w' + str(selWeek),
                                  sql_avg('SUM_userplane_upload_bytes_count').over(window))
                      .withColumn('total_download_' + comp + '_w' + str(selWeek),
                                  sql_sum('SUM_userplane_download_bytes_count').over(window))
                      .withColumn('min_download_' + comp + '_w' + str(selWeek),
                                  sql_min('SUM_userplane_download_bytes_count').over(window))
                      .withColumn('max_download_' + comp + '_w' + str(selWeek),
                                  sql_max('SUM_userplane_download_bytes_count').over(window))
                      .withColumn('avg_download_' + comp + '_w' + str(selWeek),
                                  sql_avg('SUM_userplane_download_bytes_count').over(window))

                      )

            colSel = ['subscriber_msisdn', 'total_upload_' + comp + '_w' + str(selWeek),
                      'min_upload_' + comp + '_w' + str(selWeek), 'max_upload_' + comp + '_w' + str(selWeek),
                      'avg_upload_' + comp + '_w' + str(selWeek), 'total_download_' + comp + '_w' + str(selWeek),
                      'min_download_' + comp + '_w' + str(selWeek),
                      'max_download_' + comp + '_w' + str(selWeek), 'avg_download_' + comp + '_w' + str(selWeek)]
            volumenes = volumenes.join(dftemp.select(colSel), 'subscriber_msisdn', 'left').drop_duplicates()



            if fecha_tmp != dtClosing_day:
                navcompTotal = navcomp.filter((col('event_date_date') >= fecha_Int) & (col('event_date_date') < dtClosing_day))
                dftemp_up = (navcompTotal.filter(col('application_name').like("%" + comp + "%")).withColumn(
                    'total_upload_' + comp + '_w' + str(dday), sql_sum('SUM_userplane_upload_bytes_count').over(window))
                             .withColumn('min_upload_' + comp + '_w' + str(dday),
                                         sql_min('SUM_userplane_upload_bytes_count').over(window))
                             .withColumn('max_upload_' + comp + '_w' + str(dday),
                                         sql_max('SUM_userplane_upload_bytes_count').over(window))
                             .withColumn('avg_upload_' + comp + '_w' + str(dday),
                                         sql_avg('SUM_userplane_upload_bytes_count').over(window))
                             .withColumn('total_download_' + comp + '_w' + str(dday),
                                         sql_sum('SUM_userplane_download_bytes_count').over(window))
                             .withColumn('min_download_' + comp + '_w' + str(dday),
                                         sql_min('SUM_userplane_download_bytes_count').over(window))
                             .withColumn('max_download_' + comp + '_w' + str(dday),
                                         sql_max('SUM_userplane_download_bytes_count').over(window))
                             .withColumn('avg_download_' + comp + '_w' + str(dday),
                                         sql_avg('SUM_userplane_download_bytes_count').over(window))
                             )

                colSel = ['subscriber_msisdn', 'total_upload_' + comp + '_w' + str(dday),
                          'min_upload_' + comp + '_w' + str(dday), 'max_upload_' + comp + '_w' + str(dday),
                          'avg_upload_' + comp + '_w' + str(dday), 'total_download_' + comp + '_w' + str(dday),
                          'min_download_' + comp + '_w' + str(dday), 'max_download_' + comp + '_w' + str(dday),
                          'avg_download_' + comp + '_w' + str(dday)]

                volumenes = volumenes.join(dftemp_up.select(colSel), 'subscriber_msisdn', 'left').drop_duplicates()
            volumenes = volumenes.cache()
            print("hola2", volumenes.count())

        #print(volumenes.count())

        fecha_tmp = fecha_Int

    volumenes = volumenes.fillna(0)

    for week in ['1', '2', 'prev']:
        maxDownComp = ['total_download_' + comp + '_w' + str(week) for comp in competidores]
        maxUpComp = ['total_upload_' + comp + '_w' + str(week) for comp in competidores]

        volumenes = (volumenes.withColumn('totalDownloadCompetidores_w' + str(week),sum(volumenes[ccol] for ccol in maxDownComp))
                              .withColumn('totalUploadCompetidores_w' + str(week), sum(volumenes[ccol] for ccol in maxUpComp))
                     )


    # Calculating incremental values between weeks
    #################################################
    volumenesInc=volumenes

    for comp in operadores:

        volumenesInc = (volumenesInc
                        .withColumn('total_upload_' + comp + '_w1w2', col('total_upload_' + comp + '_w1') - col('total_upload_' + comp + '_wprev'))
                        .withColumn('total_download_' + comp + '_w1w2', col('total_download_' + comp + '_w1') - col('total_download_' + comp + '_wprev'))
                        .drop('total_upload_' + comp + '_wprev').drop('total_download_' + comp + '_wprev')
                        .drop('min_upload_' + comp + '_wprev').drop('min_download_' + comp + '_wprev')
                        .drop('max_upload_' + comp + '_wprev').drop('max_download_' + comp + '_wprev')
                        .drop('avg_upload_' + comp + '_wprev').drop('avg_download_' + comp + '_wprev')
                        )

    volumenesInc=(volumenesInc.withColumn('totalDownloadCompetidores_w1w2', col('totalDownloadCompetidores_w1') - col('totalDownloadCompetidores_wprev'))
                              .withColumn('totalUploadCompetidores_w1w2', col('totalUploadCompetidores_w1') - col('totalUploadCompetidores_wprev')))
    volumenesInc = (volumenesInc.drop('totalUploadCompetidores_wprev').drop('totalDownloadCompetidores_wprev'))

    volumenesOp = (volumenesInc.withColumn('totalUploadOperadores_w1', sum(volumenesInc['total_upload_' + ccol + '_w1'] for ccol in operadores[:1]))
                   .withColumn('totalUploadOperadores_w2',sum(volumenesInc['total_upload_' + ccol + '_w2'] for ccol in operadores[:1]))
                   .withColumn('totalDownloadOperadores_w1', sum(volumenesInc['total_download_' + ccol + '_w1'] for ccol in operadores[:1]))
                   .withColumn('totalDownloadOperadores_w2',sum(volumenesInc['total_download_' + ccol + '_w2'] for ccol in operadores[:1]))
                   )

    ## Calculating ratios between variables
    #########################################

    for comp in operadores:
        volumenesOp = (volumenesOp.withColumn('Ratio_upload_' + comp + '_w1',col('total_upload_' + comp + '_w1') / col('totalUploadOperadores_w1'))
                                  .withColumn('Ratio_upload_' + comp + '_w2',col('total_upload_' + comp + '_w2') / col('totalUploadOperadores_w2'))
                       )

        #volumenesOp = volumenesOp.cache()

        volumenesOp = (volumenesOp.withColumn('Ratio_download_' + comp + '_w1',col('total_download_' + comp + '_w1') / col('totalUploadOperadores_w1'))
                       .withColumn('Ratio_download_' + comp + '_w2',col('total_download_' + comp + '_w2') / col('totalUploadOperadores_w2'))
                       )

    volumenesOp = volumenesOp.cache()
    print("hola3", volumenes.count())

    volumenesOp = volumenesOp.fillna(0)

    volumenesOp = volumenesOp.withColumn("msisdn", col("subscriber_msisdn").substr(3, 9))

    return volumenesOp


