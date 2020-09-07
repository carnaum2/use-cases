
import sys

from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from pyspark.sql.types import StringType, IntegerType

from pyspark.sql.functions import (udf,
                                   col,
                                   decode,
                                   when,
                                   lit,
                                   lower,
                                   lpad,
                                   concat,
                                   translate,
                                   count,
                                   sum as sql_sum,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   count as sql_count,
                                   greatest,
                                   least,
                                   isnull,
                                   isnan,
                                   struct,
                                   substring,
                                   size,
                                   length,
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
                                   trim,
                                   array,
                                   create_map,
                                   randn)

import itertools
import pandas as pd
import datetime as dt

OPERATORS = ["JAZZTEL", "LOWI", "MASMOVIL", "MOVISTAR", "O2", "ORANGE", "PEPEPHONE", "VODAFONE", "YOIGO", "VIRGINTELCO"]
COMPETITORS = OPERATORS + ["unknown"]


def get_last_date(spark):
    navcomp_last_date = spark\
    .read\
    .parquet("/data/udf/vf_es/netscout/dailyMSISDNApplicationName")\
    .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
    .select(sql_max(col('mydate')).alias('last_date'))\
    .rdd\
    .first()['last_date']

    return int(navcomp_last_date)



class NavCompData(DataTemplate):

    WINDOW_LENGTH = 30

    def __init__(self, spark, window_length):
        self.WINDOW_LENGTH = window_length
        DataTemplate.__init__(self, spark,  "navcomp/{}".format(self.WINDOW_LENGTH))


    def check_valid_params(self, closing_day, **kwargs):

        if self.WINDOW_LENGTH < 0:
            print("[ERROR] window_length must be a positive number. Functions are called with '-window_length'. Input: {}".format(self.WINDOW_LENGTH))
            import sys
            sys.exit()

        if kwargs.get('window_length', None) != None:
            print("[ERROR] window_length must be specified only in the __init__ function, do not pass it in the get_module call")
            import sys
            sys.exit()
        import datetime as dt

        today_str = dt.datetime.today().strftime("%Y%m%d")
        if closing_day > today_str:
            print("ERROR: closing_day [{}] + window_length [{}] must be lower than today [{}] - 3 days".format(closing_day, self.WINDOW_LENGTH, today_str))
            print("ERROR: Program will exit here!")
            import sys
            sys.exit()
        print("[NavCompData] check_valid_params | Params ok")
        return True


    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):
        '''

        :param closing_day:
        :param args:
        :return:
        '''

        orig_path = False # --> True: to extract the info from processed table

        from functools import reduce
        import itertools
        from churn_nrt.src.utils.date_functions import move_date_n_days

        level = "msisdn"
        if ("nif" in level.lower()):
             level = "nif_cliente"

        starting_date = move_date_n_days(closing_day, n=-abs(self.WINDOW_LENGTH))


        print("[NavCompData] build_module | starting_date: " + starting_date + " - ending_date: " + closing_day + " - level: " + level)

        print("[NavCompData] build_module |  Starting the computation of the feats")


        count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["sum_count", "max_count"]))]

        days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]

        null_imp_dict = dict([(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if ("min_days" in x)])

        #from churn.analysis.triggers.base_utils.base_utils import get_customer_base
        # from churn_nrt.src.data.customer_base import CustomerBase
        # basedf = CustomerBase(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen).select('msisdn', 'nif_cliente')
        # from churn_nrt.src.data_utils.base_filters import keep_active_services
        # basedf = keep_active_services(basedf)


        # autentico crudo: /data/raw/vf_es/netscout/SUBSCRIBERUSERPLANE/1.2/parquet/  <--- muy grande
        # La tabla /data/udf/vf_es/netscout/dailyMSISDNApplicationName/ es un agregado diario que genera IT
        # Tercer paso: agregado para CVM: /data/attributes/vf_es/return_feed/data_navigation/
        path_to_read = "/data/attributes/vf_es/return_feed/data_navigation/" if orig_path == True else "/data/udf/vf_es/netscout/dailyMSISDNApplicationName/"
        apps_names = ["WEB_" + op + "_" + p for op in OPERATORS for p in ["HTTP", "HTTPS"]]

        print("[NavCompData] build_module | Reading from {}".format(path_to_read))

        repart_navigdf = (self.SPARK.read.parquet(path_to_read))

        if not orig_path: # https://jira.sp.vodafone.com/browse/BDET-2081
            print("Adding event_date")
            repart_navigdf = (repart_navigdf.where(col("subscriber_msisdn").isNotNull())
                              .where((col("SUM_userplane_upload_bytes_count") + col("SUM_userplane_download_bytes_count")) > 524288)
                              .where(col("application_name").isin(apps_names))
                    .withColumn("event_date", concat(col('year'), lit("-"), lpad(col('month'), 2, '0'), lit("-"), lpad(col('day'), 2, '0'))))\
                   .groupBy("subscriber_msisdn", "application_name", "event_date").agg(
                        sql_sum("count").alias("count"),
                        sql_sum(col("SUM_userplane_upload_bytes_count")).alias("SUM_userplane_upload_bytes_count"),
                        sql_sum(col("SUM_userplane_download_bytes_count")).alias("SUM_userplane_download_bytes_count"))


        repart_navigdf = (repart_navigdf.withColumn("formatted_event_date", from_unixtime(unix_timestamp(col("event_date"), "yyyy-MM-dd")))
                          .filter((col("formatted_event_date") >= from_unixtime(unix_timestamp(lit(starting_date), "yyyyMMdd"))) & (
                        col("formatted_event_date") <= from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd"))))
                          .withColumn("days_since_navigation", datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), col("formatted_event_date")).cast("double"))
                          .withColumn("msisdn", col("subscriber_msisdn").substr(3, 9)).select("msisdn", "application_name", "count", "formatted_event_date", "days_since_navigation"))

        repart_navigdf = (repart_navigdf.withColumn("competitor", lit("unknown"))
                                        .withColumn("competitor", when(col("application_name").contains("PEPEPHONE"), "PEPEPHONE").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("ORANGE"), "ORANGE").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("JAZZTEL"), "JAZZTEL").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("MOVISTAR"), "MOVISTAR").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("MASMOVIL"), "MASMOVIL").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("YOIGO"), "YOIGO").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("VODAFONE"), "VODAFONE").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("LOWI"), "LOWI").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("VIRGINTELCO"), "VIRGINTELCO").otherwise(col("competitor")))
                                        .withColumn("competitor", when(col("application_name").contains("O2"), "O2").otherwise(col("competitor"))))#.join(basedf, ['msisdn'], 'inner').repartition(400))

        def get_most_consulted_operator(consulted_list, days_since_last_navigation_list):
            '''
            :param consulted_list: sum count of each competitor
            :param days_since_last_navigation_list: days since last navigation for each competitor
            :return: a single competitor (rules specified by CVM and implemented in this function; see email on 20200716)
            '''
            if not consulted_list: return "None"
            combined_list = [(a, b, c) for a, b, c in zip(consulted_list, days_since_last_navigation_list, COMPETITORS)]
            combined_list = [e for e in combined_list if (('VODAFONE' not in e[2]) & ('LOWI' not in e[2]))]
            combined_list = [(e[0], -1 * e[1], e[2]) for e in combined_list]
            # sort by consulted list and then by days since 1st navigation
            sorted_list = sorted(combined_list, key=lambda x: (x[0], x[1]), reverse=True)
            return sorted_list[0][2] if sorted_list else "None"

        get_most_consulted_operator_udf = udf(lambda x, y: get_most_consulted_operator(x, y), StringType())

        # print("[Info get_navcomp_attributes] After joining the base - Volume of repart_navigdf: " + str(repart_navigdf.count()) + " - Distinct msisdn: " + str(repart_navigdf.select('msisdn').distinct().count()) + " - Distinct nif: " + str(repart_navigdf.select('nif_cliente').distinct().count()))

        repart_navigdf = (repart_navigdf.groupBy(level).pivot("competitor", COMPETITORS).agg(sql_sum("count").alias("sum_count"), sql_max("count").alias("max_count"),
                                                                                             sql_max("days_since_navigation").alias("max_days_since_navigation"),
                                                                                             sql_min("days_since_navigation").alias("min_days_since_navigation"),
                                                                                             countDistinct("formatted_event_date").alias("distinct_days_with_navigation")).na.fill(null_imp_dict)
                          .withColumn("sum_count_vdf", reduce(lambda a, b: a + b, [col(x) for x in count_feats if (("sum_count" in x) & (("VODAFONE" in x) | ("LOWI" in x)))]))
                          .withColumn("sum_count_comps", reduce(lambda a, b: a + b, [col(x) for x in count_feats if (("sum_count" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("num_distinct_comps", reduce(lambda a, b: a + b, [when(col(x) > 0, 1.0).otherwise(0.0) for x in count_feats if (("sum_count" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("max_count_comps", greatest(*[x for x in count_feats if (("max_count" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("sum_distinct_days_with_navigation_vdf", reduce(lambda a, b: a + b, [col(x) for x in days_feats if (("distinct_days_with_navigation" in x) & (("VODAFONE" in x) | ("LOWI" in x)))]))
                          .withColumn("norm_sum_distinct_days_with_navigation_vdf", col("sum_distinct_days_with_navigation_vdf").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                          .withColumn("sum_distinct_days_with_navigation_comps", reduce(lambda a, b: a + b, [col(x) for x in days_feats if (("distinct_days_with_navigation" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("norm_sum_distinct_days_with_navigation_comps", col("sum_distinct_days_with_navigation_comps").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                          .withColumn("min_days_since_navigation_comps", least(*[x for x in days_feats if (("min_days_since_navigation" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("norm_min_days_since_navigation_comps",  col("min_days_since_navigation_comps").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                          .withColumn("max_days_since_navigation_comps", greatest(*[x for x in days_feats if (("max_days_since_navigation" in x) & ("VODAFONE" not in x) & ("LOWI" not in x))]))
                          .withColumn("norm_max_days_since_navigation_comps", col("max_days_since_navigation_comps").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                          .withColumn('consulted_list', array(*[col_ + "_sum_count" for col_ in COMPETITORS]))
                          .withColumn('days_since_last_navigation_list', array(*[col_ + "_min_days_since_navigation" for col_ in COMPETITORS]))
                          .withColumn('most_consulted_operator', get_most_consulted_operator_udf(col('consulted_list'), col("days_since_last_navigation_list")))
                          .drop('consulted_list', 'days_since_last_navigation_list'))

        print("[NavCompData] build_module |  Competitors web extraction completed - count = {}".format(repart_navigdf.count()))

        return repart_navigdf


    def get_metadata(self):

        print("[NavCompData] get_metadata | Building metadata dataframe")

        #competitors = ["PEPEPHONE", "ORANGE", "JAZZTEL", "MOVISTAR", "MASMOVIL", "YOIGO", "VODAFONE", "LOWI", "O2", "unknown", "VIRGIN"]

        count_feats = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["sum_count", "max_count"]))]

        days_feats = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["max_days_since_navigation", "min_days_since_navigation", "distinct_days_with_navigation"]))]

        add_feats = ["sum_count_vdf", "sum_count_comps", "num_distinct_comps", "max_count_comps", "sum_distinct_days_with_navigation_vdf", "norm_sum_distinct_days_with_navigation_vdf", \
                     "sum_distinct_days_with_navigation_comps", "norm_sum_distinct_days_with_navigation_comps", "min_days_since_navigation_comps", "norm_min_days_since_navigation_comps", \
                     "max_days_since_navigation_comps", "norm_max_days_since_navigation_comps"]

        null_imp_dict = dict(
            [(x, 0) for x in count_feats] + [(x, 0) for x in days_feats if (("distinct_days" in x) | ("max_days" in x))] + [(x, 10000) for x in days_feats if ("min_days" in x)] + [(x, 0) for x in
                                                                                                                                                                                    add_feats])

        feats = null_imp_dict.keys()

        na_vals = [str(x) for x in null_imp_dict.values()]

        # TODO add in production most_consulted operator
        cat_feats = ["most_consulted_operator"]
        cat_na_vals = ["None"]

        data = {'feature': feats+cat_feats, 'imp_value': na_vals+cat_na_vals}


        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('navcomp')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type'))) \
            .withColumn('level', lit('nif'))

        return metadata_df


class NavCompAdvData(DataTemplate):
    '''
    This module is a combination of single NavCompData plus incrementals
    '''


    def __init__(self, spark):
        DataTemplate.__init__(self, spark,  "navcomp_adv")


    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):

        from churn_nrt.src.data.customer_base import CustomerBase
        #df_cust = CustomerBase(self.SPARK).get_module(closing_day).drop_duplicates(["msisdn"])

        dxx_diff_cols = []

        df_navcomp_15 = NavCompData(self.SPARK, 15).get_module(closing_day,save=save_others, save_others=save_others, force_gen=force_gen)
        cols_list = df_navcomp_15.columns
        new_suffixed_cols = [col_ + "_last15" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_15.columns]
        df_navcomp_15 = df_navcomp_15.toDF(*new_suffixed_cols)

        df_navcomp_30 = NavCompData(self.SPARK, 30).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last30" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_30.columns]
        df_navcomp_30 = df_navcomp_30.toDF(*new_suffixed_cols)

        df_navcomp_7 = NavCompData(self.SPARK, 7).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last7" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_7.columns]
        df_navcomp_7 = df_navcomp_7.toDF(*new_suffixed_cols)

        df_navcomp_60 = NavCompData(self.SPARK, 60).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last60" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_60.columns]
        df_navcomp_60 = df_navcomp_60.toDF(*new_suffixed_cols)

        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_dd30 = move_date_n_days(closing_day, n=-30)

        df_navcomp_dd30 = NavCompData(self.SPARK, 30).get_module(closing_day_dd30, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_dd30" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_dd30.columns]
        df_navcomp_dd30 = df_navcomp_dd30.toDF(*new_suffixed_cols)

        #df_navcomp_all = df_cust.join(df_navcomp_7, on=["msisdn"], how="left")
        df_navcomp_all = df_navcomp_7.join(df_navcomp_15, on=["msisdn"], how="outer")
        df_navcomp_all = df_navcomp_all.join(df_navcomp_30, on=["msisdn"], how="outer")
        df_navcomp_all = df_navcomp_all.join(df_navcomp_60, on=["msisdn"], how="outer")


        df_navcomp_all = df_navcomp_all.join(df_navcomp_dd30, on=["msisdn"], how="left")

        dxx_diff_cols += [(col(col_ + "_last30") - col(col_ + "_dd30")).alias("inc_" + col_ + "_d30") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += [(2 * col(col_ + "_last15") - col(col_ + "_last30")).alias("inc_" + col_ + "_d15") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += [(2 * col(col_ + "_last7") - col(col_ + "_last15")).alias("inc_" + col_ + "_d7") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]

        df_navcomp_all = df_navcomp_all.select(*(df_navcomp_all.columns + dxx_diff_cols))


        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_navcomp_all = apply_metadata(self.get_metadata(), df_navcomp_all)
        df_navcomp_all = df_navcomp_all.repartition(200)
        return df_navcomp_all

    def get_metadata(self):

        dfs_metadata = []

        for dd in [7,15,30,60]:
            df_metadata = NavCompData(self.SPARK, dd).get_metadata()
            #new_suffixed_cols = [col_ + "_last{}".format(dd) if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_metadata.columns]
            df_metadata = df_metadata.withColumn("feature", when(col("feature").isin("msisdn", "nif_cliente"), col("feature")).otherwise(concat(col("feature"), lit("_last{}".format(dd)))))
            dfs_metadata.append(df_metadata)
            if dd == 30:
                df_metadata = NavCompData(self.SPARK, dd).get_metadata()
                df_metadata = df_metadata.withColumn("feature", when(col("feature").isin("msisdn", "nif_cliente"), col("feature")).otherwise(concat(col("feature"), lit("_dd{}".format(dd)))))
                dfs_metadata.append(df_metadata)

        df_metadata = NavCompData(self.SPARK, 7).get_metadata() # 7 for instance
        cols_list = df_metadata.rdd.map(lambda x: x['feature']).collect()

        # increment columns
        dxx_diff_cols = ["inc_" + col_ + "_d30" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += ["inc_" + col_ + "_d15" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += ["inc_" + col_ + "_d7" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]

        data = {'feature': dxx_diff_cols, 'imp_value': 0}


        df_increment = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('navcomp_adv')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('msisdn'))

        dfs_metadata.append(df_increment)

        from churn_nrt.src.utils.pyspark_utils import union_all

        df_metadata = union_all(dfs_metadata)
        df_metadata = df_metadata.withColumn('source', lit('navcomp_adv'))
        df_metadata = df_metadata.drop_duplicates(["feature"])


        return df_metadata


