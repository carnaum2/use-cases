



import sys

from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, when, coalesce, lit, length, datediff
from churn_nrt.src.utils.date_functions import move_date_n_days
from churn_nrt.src.utils.hdfs_functions import get_partitions_path_range
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

import itertools
from functools import partial
from pyspark.sql.functions import (greatest, least)

GROUPS = ["VIRTUALES", "MOVISTAR", "YOIGO", "MASMOVIL", "O2", "JAZZTEL", "ORANGE", "VIRGIN", "PEPEPHONE"]

COMPETITORS = ["LEBARA MOVIL", "AMENA", "MOVISTAR", "REPUBLICA MOVIL", "BLAU", "LYCA MOBIL", "ORTEL MOBILE", "SIMYO", "GT MOBILE", "YOIGO", "O2", "HAPPY MOVIL", "R", "DIGI", "MASMOVIL",
               "CARREFOUR MOVIL", "DIGLmobil", "ORBITEL MOBILE", "EUSKALTEL", "JAZZTEL", "ORANGE", "TALKOUT", "VIRGIN",  "PEPEPHONE"]

PREFFIX_COLS = "callscomp_"

PATH_CDR_RAW = "/data/raw/vf_es/mediated_cdr/NAVAJO/1.0/parquet/"

def _process_one_day(spark, path_, closing_day):
    df_master_comp = spark.read.load("/data/raw/vf_es/lookup/COMPETITOR_TELEPHONE/1.0/parquet")
    df_master_comp = df_master_comp.withColumn("COMPETIDOR", when(col("COMPETIDOR").rlike("MASM.VIL"), "MASMOVIL").otherwise(col("COMPETIDOR")))
    df_master_comp = df_master_comp.withColumn("GROUP", when(col("GROUP").rlike("MASM.VIL"), "MASMOVIL").otherwise(col("GROUP")))

    df_cdr = spark.read.load(path_)
    df_cdr = df_cdr.join(df_master_comp, on=(df_master_comp["TELEPHONE_NUMBER"] == df_cdr["NRSECUN"]), how="inner").where(col("CDSENLLA") == "OL")

    from churn_nrt.src.data.customer_base import CustomerBase
    df_cb = CustomerBase(spark).get_module(closing_day)

    df_cdr = df_cdr.join(df_cb.select("msisdn"), on=(df_cdr["NRPRIMA"] == df_cb["msisdn"]), how="inner")

    df_gr = (df_cdr.groupBy("msisdn", "GROUP", "COMPETIDOR", "DTINILLA").agg(sql_count("*").alias("num_calls"), sql_sum("TMDURLLA").alias("sum_duration"),
        sql_avg("TMDURLLA").alias("avg_duration")).withColumn("days_since_call",
                                                              datediff(from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")), from_unixtime(unix_timestamp(col("DTINILLA"), "yyyyMMdd"))).cast(
                                                                  "double")))

    return df_gr


def get_last_date(spark):
    closing_day = dt.datetime.today().strftime("%Y%m%d")
    starting_day = move_date_n_days(closing_day, n=-30)

    for _ in [0, 1, 2]:

        cdr_paths = get_partitions_path_range(PATH_CDR_RAW, starting_day, closing_day)[::-1]
        #print(starting_day, closing_day)

        for path_ in cdr_paths:
            try:
                callscomp_last_date = (spark.read.parquet(path_).where(col("CDSENLLA") == "OL").select(sql_max(col('DTINILLA')).alias('last_date')).rdd.first()['last_date'])
                return int(callscomp_last_date)
            except:
                continue

        closing_day = move_date_n_days(starting_day, n=-1)
        starting_day = move_date_n_days(starting_day, n=-30)

    return None


class CallsCompData(DataTemplate):

    WINDOW_LENGTH = None

    def __init__(self, spark, window_length):

        self.WINDOW_LENGTH = window_length
        DataTemplate.__init__(self, spark,  "callscomp/{}".format(self.WINDOW_LENGTH))


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
        print("[CallsCompData] check_valid_params | Params ok")
        return True

    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):
        '''

        :param closing_day:
        :param args:
        :return:
        '''



        level = "msisdn"

        GROUPS = ["VIRTUALES", "MOVISTAR", "YOIGO", "MASMOVIL", "O2", "JAZZTEL", "ORANGE", "VIRGIN", "PEPEPHONE"]

        COMPETITORS = ["LEBARA MOVIL", "AMENA", "MOVISTAR", "REPUBLICA MOVIL", "BLAU", "LYCA MOBIL", "ORTEL MOBILE", "SIMYO", "GT MOBILE", "YOIGO", "O2", "HAPPY MOVIL", "R", "DIGI", "MASMOVIL",
                       "CARREFOUR MOVIL", "DIGLmobil", "ORBITEL MOBILE", "EUSKALTEL", "JAZZTEL", "ORANGE", "TALKOUT", "VIRGIN", "PEPEPHONE"]


        starting_day = move_date_n_days(closing_day, n=-abs(self.WINDOW_LENGTH))


        cdr_paths = get_partitions_path_range(PATH_CDR_RAW, starting_day, closing_day)

        parts_ = [_process_one_day(self.SPARK, path_, closing_day) for path_ in cdr_paths]

        df_cdr_grs = reduce(lambda a, b: a.union(b), parts_)


        count_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["num_calls"]))]
        distinct_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["distinct_days_with_calls"]))]
        min_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["min_days_since_call"]))]
        max_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["max_days_since_call"]))]


        count_feats_gr= [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["num_calls_group"]))]
        distinct_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["distinct_days_with_calls_group"]))]
        min_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["min_days_since_call_group"]))]
        max_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["max_days_since_call_group"]))]



        df_cdr_grs_comp = (df_cdr_grs.groupBy(level).pivot("COMPETIDOR", COMPETITORS).agg(sql_count("*").alias("num_calls"),
                                                                                             sql_max("num_calls").alias("max_calls_day"),
                                                                                             sql_max("days_since_call").alias("max_days_since_call"),
                                                                                             sql_min("days_since_call").alias("min_days_since_call"),
                                                                                             countDistinct("DTINILLA").alias("distinct_days_with_calls")))
        df_cdr_grs_groups = (df_cdr_grs.groupBy(level).pivot("GROUP", GROUPS).agg(sql_count("*").alias("num_calls_group"),
                                                                                             sql_max("num_calls").alias("max_calls_day_group"),
                                                                                             sql_max("days_since_call").alias("max_days_since_call_group"),
                                                                                             sql_min("days_since_call").alias("min_days_since_call_group"),
                                                                                             countDistinct("DTINILLA").alias("distinct_days_with_calls_group")))


        def get_most_consulted_operator(ops_list, consulted_list, days_since_1st_navigation_list):
            if not consulted_list: return "None"
            combined_list = [(a, b, c) for a, b, c in zip(consulted_list, days_since_1st_navigation_list, ops_list)]
            # sort by consulted list and then by days since 1st navigation
            sorted_list = sorted(combined_list, key=lambda x: (x[0], x[1]), reverse=True)
            return sorted_list[0][2] if sorted_list else "None"


        get_most_consulted_comp_udf = udf(partial(get_most_consulted_operator, COMPETITORS), StringType())
        get_most_consulted_group_udf = udf(partial(get_most_consulted_operator, GROUPS), StringType())

        df_cdr_all = (df_cdr_grs_comp.join(df_cdr_grs_groups.drop("num_cliente"), on=["msisdn"], how="outer")
                  .withColumn("num_distinct_groups", reduce(lambda a, b: a + b, [when(col(x) > 0, 1.0).otherwise(0.0) for x in count_feats_gr]))
                  .withColumn("num_distinct_comps",  reduce(lambda a, b: a + b, [when(col(x) > 0, 1.0).otherwise(0.0) for x in count_feats_comp]))
                  .withColumn("max_count_comps", greatest(*[x for x in count_feats_comp])).withColumn("max_count_group", greatest(*[x for x in count_feats_gr]))
                  .withColumn("min_count_comps", least(*[x for x in count_feats_comp])).withColumn("min_count_group", least(*[x for x in count_feats_gr]))
                  .withColumn("sum_distinct_days_with_calls", reduce(lambda a, b: a + b, [col(x) for x in distinct_days_feats_comp]))
                  .withColumn("norm_sum_distinct_days_with_calls", col("sum_distinct_days_with_calls").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                  .withColumn('days_since_1st_call_comp_list', array(*[col_ + "_max_days_since_call" for col_ in COMPETITORS]))
                  .withColumn('days_since_1st_call_group_list', array(*[col_ + "_max_days_since_call_group" for col_ in GROUPS]))
                  .withColumn('comp_called_list', array(*count_feats_comp)).withColumn('group_called_list', array(*count_feats_gr))
                  .withColumn('most_consulted_comp', get_most_consulted_comp_udf(col("comp_called_list"), col("days_since_1st_call_comp_list")))  # .drop('consulted_list', 'days_since_1st_navigation_list'))
                  .withColumn('most_consulted_group', get_most_consulted_group_udf(col('group_called_list'), col("days_since_1st_call_group_list")))  # .drop('consulted_list', 'days_since_1st_navigation_list'))
                  .withColumn("min_days_since_call_group", least(*[x for x in min_days_feats_gr]))
                  .withColumn("norm_min_days_since_call_group", col("min_days_since_call_group").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                  .withColumn("max_days_since_call_group", least(*[x for x in max_days_feats_gr]))
                  .withColumn("norm_max_days_since_call_group", col("max_days_since_call_group").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                  .withColumn("min_days_since_call_comps", least(*[x for x in min_days_feats_comp]))
                  .withColumn("norm_min_days_since_call_comps", col("min_days_since_call_comps").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                  .withColumn("max_days_since_call_comps", least(*[x for x in max_days_feats_comp]))
                  .withColumn("norm_max_days_since_call_comps", col("max_days_since_call_comps").cast('double') / lit(self.WINDOW_LENGTH).cast('double'))
                  ).drop('comp_called_list', 'group_called_list', 'days_since_1st_call_comp_list', 'days_since_1st_call_group_list')

        new_suffixed_cols = [col_.replace(" ","_") for col_ in df_cdr_all.columns]
        df_cdr_all = df_cdr_all.toDF(*new_suffixed_cols)

        module_cols = self.get_metadata().rdd.map(lambda x: x['feature']).collect() + ["msisdn"]
        if len(module_cols) > len(df_cdr_all.columns):
            # some columns were not generated. be sure the df has all the columns in the metadata
            df_cdr_all = df_cdr_all.select(*([col(col_) if col_ in df_cdr_all.columns else (lit(None).cast(IntegerType())).alias(col_) for col_ in module_cols]))

        new_cols =[PREFFIX_COLS+col_ if col_ not in ["msisdn"] else col_ for col_ in df_cdr_all.columns]
        df_cdr_all = df_cdr_all.toDF(*new_cols)

        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_cdr_all = apply_metadata(self.get_metadata(), df_cdr_all)

        return df_cdr_all#.drop("num_cliente")


    def get_metadata(self):
        print("[CallsCompData] get_metadata | Building metadata dataframe")

        import itertools
        #from functools import partial
        #from pyspark.sql.functions import (greatest, least)

        count_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["num_calls"]))]
        distinct_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["distinct_days_with_calls"]))]
        #min_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["min_days_since_call"]))]
        max_days_feats_comp = [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["max_days_since_call"]))]

        count_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["num_calls_group"]))]
        distinct_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["distinct_days_with_calls_group"]))]
        #min_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["min_days_since_call_group"]))]
        max_days_feats_gr = [p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["max_days_since_call_group"]))]



        zero_add_feats = ["num_distinct_groups", "num_distinct_comps", "max_count_comps", "max_count_group", "min_count_comps", "min_count_group", "sum_distinct_days_with_calls",
                          "norm_sum_distinct_days_with_calls", "max_days_since_call_group", "norm_max_days_since_call_group", "max_days_since_call_comps", "norm_max_days_since_call_comps"] + [
                             p[0] + "_" + p[1] for p in list(itertools.product(GROUPS, ["num_calls_group", "max_calls_day_group", "max_days_since_call_group", "distinct_days_with_calls_group"]))] + [
                             p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["num_calls", "max_calls_day", "max_days_since_call", "distinct_days_with_calls"]))]

        th_feats = ["min_days_since_call_comps", "norm_min_days_since_call_comps", "min_days_since_call_group", "norm_min_days_since_call_group"] + [p[0] + "_" + p[1] for p in list(
            itertools.product(GROUPS, ["min_days_since_call_group"]))] + [p[0] + "_" + p[1] for p in list(itertools.product(COMPETITORS, ["min_days_since_call"]))]

        null_imp_dict = dict(
            [(x, 0) for x in count_feats_comp + count_feats_gr] + [(x, 0) for x in distinct_days_feats_comp + distinct_days_feats_gr] + [(x, 10000) for x in max_days_feats_comp + max_days_feats_gr] + [
                (x, 0) for x in zero_add_feats] + [(x, 10000) for x in th_feats])

        feats = null_imp_dict.keys()

        na_vals = [str(x) for x in null_imp_dict.values()]

        cat_feats = [PREFFIX_COLS + "most_consulted_comp", PREFFIX_COLS+"most_consulted_group"]
        #cat_feats = ["most_consulted_comp", "most_consulted_group"]

        cat_na_vals = ["None", "None"]
        feats = [PREFFIX_COLS+col_ for col_ in feats]

        data = {'feature': feats + cat_feats, 'imp_value': na_vals + cat_na_vals}

        import pandas as pd

        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(data)).withColumn('source', lit('navcomp')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type'))) \
            .withColumn('level', lit('nif'))

        metadata_df = metadata_df.withColumn('feature', regexp_replace('feature', ' ', '_'))

        return metadata_df



class CallsCompAdvData(DataTemplate):
    '''
    This module is a combination of single NavCompData plus incrementals
    '''


    def __init__(self, spark):
        DataTemplate.__init__(self, spark,  "callscomp_adv")


    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):


        print("[CallsCompAdvData] build_module | Asking for module for closing_day={} save_others={} force_gen={}".format(closing_day, save_others, force_gen))
        from churn_nrt.src.data.customer_base import CustomerBase

        dxx_diff_cols = []

        df_callscomp_15 = CallsCompData(self.SPARK, 15).get_module(closing_day,save=save_others, save_others=save_others, force_gen=force_gen)
        cols_list = df_callscomp_15.columns
        new_suffixed_cols = [col_ + "_last15" if col_ not in ["msisdn"] else col_ for col_ in df_callscomp_15.columns]
        df_callscomp_15 = df_callscomp_15.toDF(*new_suffixed_cols)

        df_callscomp_30 = CallsCompData(self.SPARK, 30).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last30" if col_ not in ["msisdn"] else col_ for col_ in df_callscomp_30.columns]
        df_callscomp_30 = df_callscomp_30.toDF(*new_suffixed_cols)

        df_callscomp_7 = CallsCompData(self.SPARK, 7).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last7" if col_ not in ["msisdn"] else col_ for col_ in df_callscomp_7.columns]
        df_callscomp_7 = df_callscomp_7.toDF(*new_suffixed_cols)

        df_callscomp_60 = CallsCompData(self.SPARK, 60).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_last60" if col_ not in ["msisdn"] else col_ for col_ in df_callscomp_60.columns]
        df_callscomp_60 = df_callscomp_60.toDF(*new_suffixed_cols)

        from churn_nrt.src.utils.date_functions import move_date_n_days
        closing_day_dd30 = move_date_n_days(closing_day, n=-30)

        df_navcomp_dd30 = CallsCompData(self.SPARK, 30).get_module(closing_day_dd30, save=save_others, save_others=save_others, force_gen=force_gen)
        new_suffixed_cols = [col_ + "_dd30" if col_ not in ["msisdn"] else col_ for col_ in df_navcomp_dd30.columns]
        df_navcomp_dd30 = df_navcomp_dd30.toDF(*new_suffixed_cols)

        #df_navcomp_all = df_cust.join(df_navcomp_7, on=["msisdn"], how="left")
        df_callscomp_all = df_callscomp_7.join(df_callscomp_15, on=["msisdn"], how="outer")
        df_callscomp_all = df_callscomp_all.join(df_callscomp_30, on=["msisdn"], how="outer")
        df_callscomp_all = df_callscomp_all.join(df_callscomp_60, on=["msisdn"], how="outer")
        df_callscomp_all = df_callscomp_all.join(df_navcomp_dd30, on=["msisdn"], how="left")


        dxx_diff_cols += [(col(col_ + "_last30") - col(col_ + "_dd30")).alias("inc_" + col_ + "_d30") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += [(2 * col(col_ + "_last15") - col(col_ + "_last30")).alias("inc_" + col_ + "_d15") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += [(2 * col(col_ + "_last7") - col(col_ + "_last15")).alias("inc_" + col_ + "_d7") for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]

        df_callscomp_all = df_callscomp_all.select(*(df_callscomp_all.columns + dxx_diff_cols))


        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_callscomp_all = apply_metadata(self.get_metadata(), df_callscomp_all)
        df_callscomp_all = df_callscomp_all.repartition(200)
        return df_callscomp_all

    def get_metadata(self):

        dfs_metadata = []

        for dd in [7,15,30,60]:
            df_metadata = CallsCompData(self.SPARK, dd).get_metadata()
            #new_suffixed_cols = [col_ + "_last{}".format(dd) if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_metadata.columns]
            df_metadata = df_metadata.withColumn("feature", when(col("feature").isin("msisdn", "nif_cliente"), col("feature")).otherwise(concat(col("feature"), lit("_last{}".format(dd)))))
            dfs_metadata.append(df_metadata)
            if dd == 30:
                df_metadata = CallsCompData(self.SPARK, dd).get_metadata()
                df_metadata = df_metadata.withColumn("feature", when(col("feature").isin("msisdn", "nif_cliente"), col("feature")).otherwise(concat(col("feature"), lit("_dd{}".format(dd)))))
                dfs_metadata.append(df_metadata)

        df_metadata = CallsCompData(self.SPARK, 7).get_metadata() # 7 for instance
        cols_list = df_metadata.rdd.map(lambda x: x['feature']).collect()

        # increment columns
        dxx_diff_cols = ["inc_" + col_ + "_d30" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += ["inc_" + col_ + "_d15" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        dxx_diff_cols += ["inc_" + col_ + "_d7" for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]

        data = {'feature': dxx_diff_cols, 'imp_value': 0}


        df_increment = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('callscomp_adv')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('msisdn'))

        dfs_metadata.append(df_increment)

        from churn_nrt.src.utils.pyspark_utils import union_all

        df_metadata = union_all(dfs_metadata)
        df_metadata = df_metadata.withColumn('source', lit('callscomp_adv'))
        df_metadata = df_metadata.drop_duplicates(["feature"])


        return df_metadata
