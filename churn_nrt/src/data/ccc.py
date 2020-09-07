# -*- coding: utf-8 -*-


import time
import os
import sys
import re
import time
import itertools
import pandas as pd

from pyspark.sql.functions import (udf, col, decode, when, lit, lower,
                                   translate, count, max, avg, sum as sql_sum,
                                   isnull,
                                   unix_timestamp,
                                   from_unixtime, upper)

from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
from churn_nrt.src.utils.date_functions import move_date_n_cycles
from churn_nrt.src.utils.constants import HDFS_CHURN_NRT, PARTITION_DATE



from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.utils.constants import LEVEL_MSISDN, LEVEL_NIF


def get_bucket_info(spark):

    start_time = time.time()

    bucket =  (spark
    .read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .load("/data/udf/vf_es/ref_tables/amdocs_ids/Agrup_Buckets_unific.txt")
    .withColumn('INT_Tipo', upper(col('INT_Tipo')))
    .withColumn('INT_Subtipo', upper(col('INT_Subtipo')))
    .withColumn('INT_Razon', upper(col('INT_Razon')))
    .withColumn('INT_Resultado', upper(col('INT_Resultado')))
    .withColumn("bucket", upper(col("bucket")))
    .withColumn("sub_bucket", upper(col("sub_bucket")))
    .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")))
    .withColumn('bucket', when(col('bucket').like('%MI VODAFONE%'), 'MI_VODAFONE')#.otherwise(col('bucket')))
    .when(col('bucket').like('%BILLING - POSTPAID%'), 'BILLING_POSTPAID')#.otherwise(col('bucket')))
    .when(col('bucket').like('%DEVICE DELIVERY/REPAIR%'), 'DEVICE_DELIVERY_REPAIR')#.otherwise(col('bucket')))
    .when(col('bucket').like('%DSL/FIBER INCIDENCES AND SUPPORT%'), 'DSL_FIBER_INCIDENCES_AND_SUPPORT')#.otherwise(col('bucket')))
    .when(col('bucket').like('%INTERNET_EN_EL_MOVIL%'), 'INTERNET_EN_EL_MOVIL')#.otherwise(col('bucket')))
    .when(col('bucket').like('%DEVICE UPGRADE%'), 'DEVICE_UPGRADE')#.otherwise(col('bucket')))
    .when(col('bucket').like('%PRODUCT AND SERVICE MANAGEMENT%'), 'PRODUCT_AND_SERVICE_MANAGEMENT')#.otherwise(col('bucket')))
    .when(col('bucket').like('%INFOSATIS%'), 'OTHER_CUSTOMER_INFOSATIS_START_AVERIA')#.otherwise(col('bucket')))
    .when(col('bucket').like('%CHURN/CANCELLATIONS%'), 'CHURN_CANCELLATIONS')#.otherwise(col('bucket')))
    .when(col('bucket').like('%VOICE AND MOBILE DATA INCIDENCES AND SUPPORT%'), 'VOICE_AND_MOBILE_DATA_INCIDENCES_AND_SUPPORT')#.otherwise(col('bucket')))
    .when(col('bucket').like('%PREPAID BALANCE%'), 'PREPAID_BALANCE')#.otherwise(col('bucket')))
    .when(col('bucket').like('%COLLECTIONS%'), 'COLLECTIONS')#.otherwise(col('bucket')))
    .when(col('bucket').like('%QUICK CLOSING%'), 'QUICK_CLOSING')#.otherwise(col('bucket')))
    .when(col('bucket').like('%TARIFF MANAGEMENT%'), 'TARIFF_MANAGEMENT')#.otherwise(col('bucket')))
    .when(col('bucket').like('%NEW ADDS PROCESS%'), 'NEW_ADDS_PROCESS')#.otherwise(col('bucket')))
    .when(col('bucket').like('%OTHER CUSTOMER INFORMATION MANAGEMENT%'), 'OTHER_CUSTOMER_INFORMATION_MANAGEMENT').otherwise(col('bucket')))
               ).cache()

    print('[CCC] get_bucket_info | Num entries in bucket: {}'.format(bucket.count()))

    print("[CCC] get_bucket_info | Elapsed time {} secs ".format(time.time() - start_time))

    return bucket




def _convert_cycles_to_suffix(cycles):
    return "_w{}".format(cycles)


def _get_ccc_period_attributes(spark, start_date, end_date, base, suffix=""):
    # Bucket

    bucket = get_bucket_info(spark)

    bucket_list = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()

    interactions_ono = spark.table('raw_es.callcentrecalls_interactionono') \
        .withColumn('formatted_date', from_unixtime(unix_timestamp(col("FX_CREATE_DATE"), "yyyy-MM-dd"))  ) \
        .filter((col('formatted_date') >= from_unixtime(unix_timestamp(lit(start_date), "yyyyMMdd"))) & (col('formatted_date') <= from_unixtime(unix_timestamp(lit(end_date), "yyyyMMdd")))  )

    interactions_ono = interactions_ono.filter("DS_DIRECTION IN ('De entrada', 'Entrante')"  ) # DS_DIRECTION IN ('De entrada', 'Entrante')
    interactions_ono = interactions_ono.filter(col("CO_TYPE").rlike('(?i)^Llamada Telefono$|^Telefonica$|$\.\.\.^'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^BO'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^B\.O'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Emis'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Gestion B\.O\.$|(?i)^Gestion casos$|(?i)^Gestion Casos Resueltos$|' \
                                                                         '(?i)^Gestion Casos Resueltos$|(?i)^Gestion documental$|(?i)^Gestion documental fax$|' \
                                                                         '(?i)^2ª Codificación$|(?i)^BAJ_BO Televenta$|(?i)^BAJ_B\.O\. Top 3000$|(?i)^BBOO$' \
                                                                         '(?i)^BO Fraude$|(?i)^B.O Gestion$|(?i)^B.O Portabilidad$|(?i)^BO Scoring$|' \
                                                                         '(?i)^Bo Scoring Permanencia$|(?i)^Consulta ficha$|(?i)^Callme back$|' \
                                                                         '(?i)^Consultar ficha$|(?i)^Backoffice Reclamaciones$|(?i)^BACKOFFICE$|(?i)^BackOffice Retención$|(?i)^NBA$|'
                                                                         '(?i)^Ofrecimiento comercial$|(?i)^No Ofrecimiento$|(?i)^Porta Salientes Emp Info$|(?i)^Porta Salientes Emp Movil$|' \
                                                                         '(?i)^Porta Salientes Emp Fijo$|(?i)^Callmeback$|(?i)^Caso Improcedente$|(?i)^Caso Resuelto$|(?i)^Caso Mal  Enviado BO 123$|(?i)^Gestion BO$'))

    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^CIERRE RAPID'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^SMS FollowUP Always Solv'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^BackOffice'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Ilocalizable'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Detractor'))

    interactions_ono = interactions_ono \
        .withColumnRenamed('DS_REASON_1', 'INT_Tipo') \
        .withColumnRenamed('DS_REASON_2', 'INT_Subtipo') \
        .withColumnRenamed('DS_REASON_3', 'INT_Razon') \
        .withColumnRenamed('DS_RESULT',   'INT_Resultado') \
        .withColumnRenamed('DS_DIRECTION', 'DIRECTION') \
        .withColumnRenamed('CO_TYPE', 'TYPE_TD') \
        .withColumn('INT_Tipo', upper(col('INT_Tipo'))) \
        .withColumn('INT_Subtipo', upper(col('INT_Subtipo'))) \
        .withColumn('INT_Razon', upper(col('INT_Razon'))) \
        .withColumn('INT_Resultado', upper(col('INT_Resultado'))) \
        .join(bucket.drop('Stack').distinct(), ['INT_Tipo', 'INT_Subtipo', 'INT_Razon', 'INT_Resultado'], 'left_outer') \
        .withColumnRenamed('DS_X_PHONE_CONSULTATION', 'msisdn') \
        .withColumnRenamed('DS_X_GROUP_WORK', 'grupo_trabajo') \
        .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")) & (~isnull(col('msisdn'))) & (~col('msisdn').isin("", " "))) \
        .select('msisdn', 'bucket')

        # bucket_list = interactions_ono.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()

    print '[CCC] _get_ccc_period_attributes | Period: ' + str(start_date) + ' - ' + str(end_date) + ' - Number of entries before the join with the base: ' + str(interactions_ono.count())

    interactions_ono = interactions_ono \
        .groupBy('msisdn') \
        .pivot('bucket', bucket_list) \
        .agg(count("*")) \
        .na.fill(0) \
        .withColumn('num_calls', sum([col(x) for x in bucket_list]))

    print '[CCC] _get_ccc_period_attributes | Period: ' + str(start_date) + ' - ' + str(end_date) + ' - Number of entries after the join with the base: ' + str(interactions_ono.count())

    bucket_list.extend(['num_calls'])

    for c in bucket_list:
        interactions_ono = interactions_ono.withColumnRenamed(c, c + suffix)

    base = base.select('msisdn').distinct()

    interactions_ono = base.join(interactions_ono, ['msisdn'], 'left_outer').na.fill(0)

    return interactions_ono


def get_incremental_attributes(prevdf, initdf, cols, suffix, name):

    initdf = initdf.join(prevdf, ['msisdn'], 'inner')

    for c in cols:
        initdf = initdf.withColumn("inc_" + c, col(c) - col(c + suffix))

    selcols = ["inc_" + c for c in cols]
    selcols.extend(['msisdn'])

    resultdf = initdf.select(selcols)

    inc_cols = ["inc_" + c for c in cols]

    for c in inc_cols:
        resultdf = resultdf.withColumnRenamed(c, c + name)

    return resultdf

def _get_ccc_attributes(spark, end_date, base, n_periods=None, period="cycles"):
    '''

    :param spark:
    :param end_date:
    :param base:
    :param n_periods: a number of n_periods (days/cycles) to compute attributes. It should be a positive number
    :period "cycles" if n_periods is the number of cycles to move backward;  "days" if n_periods is the number of days to move backward
    :return:
    '''
    print("[CCC] _get_ccc_attributes | Called get_ccc_attributes with end_date={} n_periods={} period={}".format(end_date, n_periods, period))

    from churn_nrt.src.utils.date_functions import move_date_n_cycles, move_date_n_days

    if not n_periods:

        # Generating dates

        end_date_w2 = move_date_n_cycles(end_date, -2)
        end_date_w4 = move_date_n_cycles(end_date, -4)
        end_date_w8 = move_date_n_cycles(end_date, -8)

        print("[CCC] _get_ccc_attributes | Computing ccc attributes using the default dates: w2 ({}), w4 ({}) and w8 ({})".format(end_date_w2, end_date_w4, end_date_w8))


        # Aggregated attributes
        # FIXME: base should not be an input argument for _get_ccc_period_attributes; cache and count added to force the computation of the DF and prevent Resolved attribute(s) error
        agg_w2_ccc_attributes = _get_ccc_period_attributes(spark, end_date_w2, end_date, base, "_w2")

        agg_w2_ccc_attributes = agg_w2_ccc_attributes.repartition(200).cache()
        print "[Info] Size of agg_w2_ccc_attributes: " + str(agg_w2_ccc_attributes.count())

        agg_w4_ccc_attributes = _get_ccc_period_attributes(spark, end_date_w4, end_date, base, "_w4")

        agg_w4_ccc_attributes = agg_w4_ccc_attributes.repartition(200).cache()
        print "[Info] Size of agg_w4_ccc_attributes: " + str(agg_w4_ccc_attributes.count())

        agg_w8_ccc_attributes = _get_ccc_period_attributes(spark, end_date_w8, end_date, base, "_w8")

        agg_w8_ccc_attributes = agg_w8_ccc_attributes.cache().repartition(200)
        print "[Info] Size of agg_w8_ccc_attributes: " + str(agg_w8_ccc_attributes.count())

        print("DEBUG w2", agg_w2_ccc_attributes.columns)
        print("DEBUG w4", agg_w4_ccc_attributes.columns)
        print("DEBUG w8", agg_w8_ccc_attributes.columns)

        # agg_w4_ccc_attributes.alias("df_w4")
        # agg_w2_ccc_attributes.alias("df_w2")
        # agg_w8_ccc_attributes.alias("df_w8")


        ccc_attributes = agg_w4_ccc_attributes.join(agg_w2_ccc_attributes, on=["msisdn"], how='inner')

        print("DEBUG join", ccc_attributes.columns)
        ccc_attributes = agg_w8_ccc_attributes.join(ccc_attributes, ['msisdn'], 'inner')

        print "[CCC] _get_ccc_attributes | Size of agg_w2_ccc_attributes: " + str(agg_w2_ccc_attributes.count()) + " - Size of agg_w4_ccc_attributes: " + str(
            agg_w4_ccc_attributes.count()) + " - Size of agg_w8_ccc_attributes: " + str(agg_w8_ccc_attributes.count()) + " - Size of ccc_attributes: " + str(ccc_attributes.count())

    else:

        if n_periods < 0:
            print("[CCC] _get_ccc_attributes | ERROR n_periods must be a positive number. Functions are called with '-n_periods'. Input: {}".format(n_periods))
            import sys
            sys.exit()

        end_date_w = move_date_n_cycles(end_date, -abs(n_periods)) if period=="cycles" else move_date_n_days(end_date, -abs(n_periods))
        print("[CCC] _get_ccc_attributes | Computing ccc attributes using dates {}-{}".format(end_date_w, end_date))
        return _get_ccc_period_attributes(spark, end_date_w, end_date, base, "")

    # Incremental attributes
    # Columns
    # Bucket
    # bucket = get_bucket_info(spark)

    # inc_cols = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect()
    # inc_cols.extend(['num_calls'])

    # Last 2 cycles vs previous 2 cycles

    #Remove "_w2" suffix
    new_cols_nosuffix = [re.sub(r"_w2$", "", col_) for col_ in agg_w2_ccc_attributes.columns if col_ != "msisdn"]

    # w2 - [w4-w2]
    w2vsw2 = [(2.0 * col(col_ + "_w2") - col(col_ + "_w4")).alias("inc_" + col_ + "_w2vsw2") for col_ in new_cols_nosuffix]

    # Last 1 month vs previous 1 month
    w4vsw4 = [(2.0 * col(col_ + "_w4") - col(col_ + "_w8")).alias("inc_" + col_ + "_w4vsw4") for col_ in new_cols_nosuffix]

    ccc_attributes = ccc_attributes.select(*(ccc_attributes.columns + w2vsw2 + w4vsw4))

    return ccc_attributes



def get_nif_aggregates(spark, df, date_, save_others, force_gen=False):

    allcols = df.columns

    featcols = [c for c in allcols if c != 'msisdn']

    # Getting the mapper msisdn-NIF

    mapper = CustomerBase(spark).get_module(date_, save=False, save_others=save_others, force_gen=force_gen, add_tgs=False).select('msisdn', 'NIF_CLIENTE') \
        .dropDuplicates()

    # Adding the NIF and computing the aggregates

    aggs = [sql_sum(col(c)).alias(c) for c in featcols]

    df = df \
        .join(mapper, ['msisdn'], 'inner') \
        .groupBy('NIF_CLIENTE') \
        .agg(*aggs)

    return df


def check_args(level):
    if level not in [LEVEL_MSISDN, LEVEL_NIF]:
        print("[CCC] check_args | Unknown level {}. Parameter 'level' must be one of '{}', '{}'".format(level, LEVEL_MSISDN, LEVEL_NIF))
        print("[CCC] check_args | Program will exit here!")
        sys.exit()


class CCC(DataTemplate):
    '''
    Compute attributes for [date_-90, date_]
        ccc_df = CCC(spark, level=level).get_module(date_, save=False, save_others=False, n_periods=90, period="days")
        The result is not stored even if save=True since the module is generated for non-default parameters

    '''

    LEVEL = "msisdn"

    def __init__(self, spark, level = LEVEL_MSISDN):
        check_args(level)
        self.LEVEL = level

        DataTemplate.__init__(self, spark, "ccc/{}".format(self.LEVEL))

    def is_default_module(self, *args, **kwargs):
        # if None --> then it is a default module
        check =  (kwargs.get('n_periods', None) == None) and (kwargs.get('filter_function', None) == None)
        if not check:
            print("[CCC] is_default_module | Module {} cannot be saved since parameters in get_module are not the defaults ones!".format(self.MODULE_NAME))
        return check


    def build_module(self, closing_day, save_others, force_gen=False, n_periods=None, period="cycles", filter_function=None, **kwargs):
        '''
        Return the ccc attributes aggregated over the number of cycles in 'cycles' parameter
        :param spark:
        :param closing_day:
        :param n_periods Compute the module for cycles other than w2, w4 and w8. If specified, the module is not saved
                Set n_periods=None to run the module with default attributes
        :param filter_function function to use to filter the base. example:
                    from churn_nrt.src.data_utils.base_filters import keep_active_services
                    df = CCC(spark, level="nif").get_module("20191014", filter_function=keep_active_services)
        :return:
        '''

        print("[CCC] build_module | Computing CCC attributes for closing_day={} and level={}".format(closing_day, self.LEVEL))
        print(self)
        base = (CustomerBase(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen, add_tgs=False))

        if filter_function:
            print("[CCC] build_module | Filtering base using function {}... ".format(filter_function.__name__))
            base = filter_function(base)


        base = (base.select('msisdn').distinct())

        # FIXME: cache and count on base to prevent Resolved attribute(s) error

        base = base.cache()

        print "[Info] Size of base: " + str(base.count())

        ccc_attributes = _get_ccc_attributes(self.SPARK, closing_day, base, n_periods=n_periods, period=period)

        if self.LEVEL==LEVEL_NIF:
            ccc_attributes = get_nif_aggregates(self.SPARK, ccc_attributes, closing_day, save_others, force_gen=force_gen)

        return ccc_attributes


    def get_metadata(self):

        bucket = get_bucket_info(self.SPARK)
        bucket_list = bucket.select('bucket').distinct().rdd.map(lambda r: r['bucket']).collect() + ['num_calls']
        suffixes = ['w2', 'w4', 'w8']
        agg_feats = [p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, suffixes))]

        incs = ['w2vsw2', 'w4vsw4']
        inc_feats = ["inc_" + p[0] + "_" + p[1] for p in list(itertools.product(bucket_list, incs))]

        feats = agg_feats + inc_feats
        cat_feats = []
        na_vals = [str(x) for x in len(feats) * [0]]
        data = {'feature': feats, 'imp_value': na_vals}

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
            .withColumn('source', lit('ccc'))
            .withColumn('type', lit('numeric'))
            .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))
            .withColumn('level', lit('nif')))

        return metadata_df
