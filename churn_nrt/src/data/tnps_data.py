#!/usr/bin/env python
# -*- coding: utf-8 -*-

from churn_nrt.src.data_utils.DataTemplate import DataTemplate

from common.src.main.python.utils.hdfs_generic import *

import numpy
import re
import subprocess
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, collect_list, collect_set, concat, concat_ws, greatest, least, lit, lpad, size, struct, trim, udf, when
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import FloatType, IntegerType, StringType


def get_tnps_service_df(spark, closing_day, starting_day):
    from pyspark.sql.functions import array_contains, col, collect_list, collect_set, concat, concat_ws, greatest, least, lit, lpad, size, struct, trim, udf, when
    from pyspark.sql.functions import year, month, dayofmonth
    from pyspark.sql.types import FloatType, IntegerType, StringType
    print('Getting TNPS Information...')
    print 'Loading TNPS ...'

    tnps = spark.table('raw_es.tnps')

    tnps = tnps.withColumn('year', year('FechaLLamYDILO')).withColumn('month', month('FechaLLamYDILO')).withColumn(
        'day', dayofmonth('FechaLLamYDILO'))
    tnps = tnps.cache()
    print tnps.count()
    tnps = tnps.withColumn('partitioned_month', 100 * tnps.year + tnps.month)

    tnps = tnps.where((concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
                      & (concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) >= starting_day))
    tnps = tnps.distinct()
    tnps = tnps.withColumnRenamed('SERIAL_NUMBER', 'msisdn')

    nums_pregunta_recomendaria = ['4155.0', '4161.0', '4167.0', '4173.0',
                                  '4179.0', '4185.0', '4191.0', '4197.0', '5001.0', '5018.0', '5190.0', '5774.0',
                                  '5775.0', '5776.0', '5805.0', '5818.0',
                                  '5821.0', '5825.0', '5835.0', '5847.0', '5860.0', '5894.0', '5910.0', '5974.0',
                                  '6025.0', '6034.0', '6064.0', '6066.0',
                                  '6128.0', '6191.0', '6260.0', '6286.0', '6295.0', '6303.0', '6308.0', '6319.0',
                                  '6473.0', '6595.0']

    tnps_nps = tnps.filter(tnps['Num_Pregunta'].isin(nums_pregunta_recomendaria))
    vdns = [x.VDN for x in tnps_nps.select('VDN').distinct().collect()]

    tnps_nps = tnps_nps.filter('Respuesta != "ERROR"').withColumn('Respuesta_Num',
                                                                  when(tnps_nps.Respuesta.like('CERO'), lit(0))
                                                                  .when(tnps_nps.Respuesta.like('UNO'), lit(1))
                                                                  .when(tnps_nps.Respuesta.like('DOS'), lit(2))
                                                                  .when(tnps_nps.Respuesta.like('TRES'), lit(3))
                                                                  .when(tnps_nps.Respuesta.like('CUATRO'), lit(4))
                                                                  .when(tnps_nps.Respuesta.like('CINCO'), lit(5))
                                                                  .when(tnps_nps.Respuesta.like('SEIS'), lit(6))
                                                                  .when(tnps_nps.Respuesta.like('SIETE'), lit(7))
                                                                  .when(tnps_nps.Respuesta.like('OCHO'), lit(8))
                                                                  .when(tnps_nps.Respuesta.like('NUEVE'), lit(9))
                                                                  .when(tnps_nps.Respuesta.like('DIEZ'), lit(10)))

    # tnps_nps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

    print 'Pivoting TNPS on VDN ...'

    tnps_pivoted = tnps_nps.groupby('msisdn', 'partitioned_month', 'year', 'month').pivot('VDN', values=vdns).min(
        'Respuesta_Num')

    ymd_year = int(closing_day[-8:-4])
    ymd_month = int(closing_day[-4:-2])
    ymd_day = int(closing_day[-2:])
    tnps_pivoted = tnps_pivoted.drop('year', 'month', 'day', 'partitioned_month') \
        .withColumn('year', lit(ymd_year)) \
        .withColumn('month', lit(ymd_month)) \
        .withColumn('day', lit(ymd_day)) \
        .withColumn('partitioned_month', lit(100 * ymd_year + ymd_month))

    print 'Appending VDN_ prefix to columns ...'
    for c in tnps_pivoted.columns:
        if c not in ['msisdn', 'partitioned_month', 'year', 'month', 'day']:
            tnps_pivoted = tnps_pivoted.withColumnRenamed(c, 'VDN_' + c)

    # Calculate min, mean, std, and max of all TNPS_VDN_*
    tnps_cols = [x for x in tnps_pivoted.columns if x.startswith('VDN_')]
    tnps_pivoted = tnps_pivoted.cache()
    print'Size of tnps table: ' + str(tnps_pivoted.count())

    tnps_pivoted = tnps_pivoted.withColumn('list_VDN', concat_ws(',', *tnps_cols))

    tnps_pivoted = tnps_pivoted.withColumn('min_VDN', least(*tnps_cols))  # .fillna(-1)

    mean_vdn = udf(lambda row: float(numpy.mean(filter(lambda x: x is not None, row))) if len(
        filter(lambda x: x is not None, row)) > 0 else None, FloatType())
    tnps_pivoted = tnps_pivoted.withColumn('mean_VDN', mean_vdn(struct([col(x) for x in tnps_cols])))

    std_vdn = udf(lambda row: float(numpy.std(filter(lambda x: x is not None, row))) if len(
        filter(lambda x: x is not None, row)) > 0 else None, FloatType())
    tnps_pivoted = tnps_pivoted.withColumn('std_VDN', std_vdn(struct([col(x) for x in tnps_cols])))

    tnps_pivoted = tnps_pivoted.withColumn('max_VDN', greatest(*tnps_cols))  # .fillna(-1)
    tnps_pivoted = tnps_pivoted.cache()

    #tnps_pivoted.printSchema()

    tnps_pivoted = tnps_pivoted.withColumn('TNPS01', tnps_pivoted['min_VDN'])
    tnps_pivoted = tnps_pivoted.withColumn('TNPS4', when(tnps_pivoted['TNPS01'].isin(9, 10), 'PROMOTER') \
                                           .when(tnps_pivoted['TNPS01'].isin(7, 8), 'NEUTRAL') \
                                           .when(tnps_pivoted['TNPS01'].isin(4, 5, 6), 'SOFT DETRACTOR') \
                                           .when(tnps_pivoted['TNPS01'].isin(0, 1, 2, 3), 'HARD DETRACTOR'))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS', when(tnps_pivoted['TNPS4'].isin('HARD DETRACTOR', 'SOFT DETRACTOR'),
                                                        'DETRACTOR') \
                                           .otherwise(tnps_pivoted['TNPS4']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS3PRONEU',
                                           when(tnps_pivoted['TNPS4'].isin('NEUTRAL', 'PROMOTER'), 'NON DETRACTOR') \
                                           .otherwise(tnps_pivoted['TNPS4']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS3NEUSDET',
                                           when(tnps_pivoted['TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL'), 'INNER') \
                                           .otherwise(tnps_pivoted['TNPS4']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2HDET',
                                           when(tnps_pivoted['TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL', 'PROMOTER'),
                                                'NON HARD DETRACTOR') \
                                           .otherwise(tnps_pivoted['TNPS4']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2SDET',
                                           when(tnps_pivoted['TNPS4'].isin('HARD DETRACTOR', 'NEUTRAL', 'PROMOTER'),
                                                'NON SOFT DETRACTOR') \
                                           .otherwise(tnps_pivoted['TNPS4']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2DET',
                                           when(tnps_pivoted['TNPS'].isin('NEUTRAL', 'PROMOTER'), 'NON DETRACTOR') \
                                           .otherwise(tnps_pivoted['TNPS']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2NEU',
                                           when(tnps_pivoted['TNPS'].isin('DETRACTOR', 'PROMOTER'), 'NON NEUTRAL') \
                                           .otherwise(tnps_pivoted['TNPS']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2PRO',
                                           when(tnps_pivoted['TNPS'].isin('DETRACTOR', 'NEUTRAL'), 'NON PROMOTER') \
                                           .otherwise(tnps_pivoted['TNPS']))

    tnps_pivoted = tnps_pivoted.withColumn('TNPS2INOUT',
                                           when(tnps_pivoted['TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL'), 'INNER') \
                                           .when(tnps_pivoted['TNPS4'].isin('HARD DETRACTOR', 'PROMOTER'), 'OUTER'))

    tnps_by_msisdn = tnps_pivoted.repartition(200)

    tnps_oldColumns = tnps_by_msisdn.columns
    tnps_newColumns = ['tnps_' + c if c not in ['msisdn', 'partitioned_month', 'year', 'month', 'day'] else c for c in
                       tnps_by_msisdn.columns]
    tnps_by_msisdn = reduce(lambda df, idx: df.withColumnRenamed(tnps_oldColumns[idx], tnps_newColumns[idx]),
                            xrange(len(tnps_oldColumns)), tnps_by_msisdn)

    return tnps_by_msisdn


class TNPS(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "tnps")


    def build_module(self, closing_day, force_gen=False, window_cycles=-4, **kwargs):

        print("Building {} module for closing_day {}".format(self.MODULE_NAME, closing_day))
        from churn_nrt.src.data.tnps_data import get_tnps_service_df
        from churn_nrt.src.utils.date_functions import move_date_n_days
        starting_day = move_date_n_days(closing_day, n=-30)
        print'Starting_day: ' + starting_day
        print'closing_day: ' + closing_day
        df_tnps = get_tnps_service_df(self.SPARK, closing_day, starting_day)


        from churn_nrt.src.utils.date_functions import move_date_n_days

        inc_feats = ['tnps_min_VDN', 'tnps_mean_VDN', 'tnps_max_VDN', 'tnps_std_VDN']
        sel_feats = ['tnps_TNPS2DET', 'tnps_TNPS2HDET', 'tnps_min_VDN', 'tnps_mean_VDN', 'tnps_max_VDN', 'tnps_std_VDN',
                     'msisdn']
        not_inc_cols = ['tnps_TNPS2DET', 'tnps_TNPS2HDET', 'msisdn']

        closing_day_30 = move_date_n_days(closing_day, n=-30)
        closing_day_60 = move_date_n_days(closing_day, n=-60)
        closing_day_14 = move_date_n_days(closing_day, n=-14)

        df_tnps_w4 = df_tnps.select(sel_feats)
        df_tnps_w4w8 = get_tnps_service_df(self.SPARK, closing_day_30, closing_day_60).select(sel_feats)

        df_tnps_w2 = get_tnps_service_df(self.SPARK, closing_day, closing_day_14).select(sel_feats)
        df_tnps_w2w4 = get_tnps_service_df(self.SPARK, closing_day_14, closing_day_30).select(sel_feats)

        from churn_nrt.src.utils.pyspark_utils import rename_columns_sufix

        df_tnps_w4 = df_tnps_w4.withColumn('isDetractor_w4',when(col('tnps_TNPS2DET') == "DETRACTOR",1).otherwise(0))
        #.drop('tnps_TNPS2DET').drop('tnps_TNPS2HDET') #rename_columns_sufix(df_tnps_w4, "w4", sep="_", nocols=not_inc_cols)

        df_tnps_w2 = rename_columns_sufix(df_tnps_w2, "w2", sep="_", nocols=not_inc_cols).withColumn('isDetractor_w2', when(col('tnps_TNPS2DET') == "DETRACTOR",1).otherwise(0))\
        .drop('tnps_TNPS2DET').drop('tnps_TNPS2HDET')

        df_tnps_w2w4 = rename_columns_sufix(df_tnps_w2w4, "w2w4", sep="_", nocols=not_inc_cols).withColumn('isDetractor_w2w4', when(col('tnps_TNPS2DET') == "DETRACTOR", 1).otherwise(0))\
        .drop('tnps_TNPS2DET').drop('tnps_TNPS2HDET')

        df_tnps_w4w8 = rename_columns_sufix(df_tnps_w4w8, "w4w8", sep="_", nocols=not_inc_cols).withColumn('isDetractor_w4w8', when(col('tnps_TNPS2DET') == "DETRACTOR", 1).otherwise(0))\
        .drop('tnps_TNPS2DET').drop('tnps_TNPS2HDET')

        df_inc = df_tnps_w4.join(df_tnps_w4w8, ['msisdn'], 'left').join(df_tnps_w2, ['msisdn'], 'left').join(df_tnps_w2w4, ['msisdn'], 'left')

        df_inc = df_inc.cache()
        print'Size of incremental tnps df: ' + str(df_inc.count())

        for feat in inc_feats:
            df_inc = df_inc.withColumn('inc_' + feat + '_w2w2', col(feat + '_w2') - col(feat + '_w2w4')).withColumn(
                'inc_' + feat + '_w4w4', col(feat) - col(feat + '_w4w8'))

        df_inc = df_inc.withColumn('flag_detractor_w2w2', col('isDetractor_w2') - col('isDetractor_w2w4')).withColumn(
            'flag_detractor_w4w4', col('isDetractor_w4') - col('isDetractor_w4w8'))

        df_inc = df_inc.cache()
        print'Size of final incremental tnps df: ' + str(df_inc.count())

        return df_inc


    def get_metadata(self):


        feats_ori_ = ['tnps_' + f + '_VDN' for f in ['min', 'max', 'std', 'mean']]
        feats_cat = ['tnps_TNPS2HDET']

        feats_num_ = [f + suf for f in feats_ori_ for suf in ['','_w2','_w2w4','_w4w8']] + ['flag_detractor_w2w2', 'flag_detractor_w4w4']
        feats_inc = ['inc_' + f + suf for f in feats_ori_ for suf in ['_w2w2', '_w4w4']]

        feats_num = feats_num_ + feats_inc

        null_imp_dict_num = dict([(x, 0.0) for x in feats_num])
        feats = null_imp_dict_num.keys()
        na_vals = null_imp_dict_num.values()
        na_vals = [str(x) for x in na_vals]

        null_imp_dict_cat = dict([(x, 'unknown') for x in feats_cat])
        feats_c = null_imp_dict_cat.keys()
        na_vals_cat = null_imp_dict_cat.values()
        na_vals_cat = [str(x) for x in na_vals_cat]

        data = {'feature': feats, 'imp_value': na_vals}
        data_cat = {'feature': feats_c, 'imp_value': na_vals_cat}

        import pandas as pd

        metadata1_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('tnps')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('msisdn'))


        metadata2_df = self.SPARK.createDataFrame(pd.DataFrame(data_cat)) \
            .withColumn('source', lit('tnps')) \
            .withColumn('type', lit('categorical')) \
            .withColumn('level', lit('msisdn'))

        return metadata1_df.union(metadata2_df)




