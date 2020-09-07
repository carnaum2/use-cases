#!/usr/bin/env python
# -*- coding: utf-8 -*-

from churn_nrt.src.data_utils.DataTemplate import DataTemplate

import os
import sys
import time
import math
import re
from pyspark.sql.functions import (udf,
                                   col,
                                   decode,
                                   when,
                                   lit,
                                   lower,
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
from pyspark.ml.feature import QuantileDiscretizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import DoubleType


def get_module_tickets(spark, starting_day, closing_day):
    ga_tickets_ = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(
        col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')

    print'Starting day: ' + starting_day
    print'Closing day: ' + closing_day
    print("############ Loading ticket sources ############")

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    ga_tickets_ = ga_tickets_.withColumn('CREATION_TIME',
                                         to_timestamp(ga_tickets_.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
    close = str(closing_day[0:4]) + '-' + str(closing_day[4:6]) + '-' + str(closing_day[6:8]) + ' 00:00:00'
    start = str(starting_day[0:4]) + '-' + str(starting_day[4:6]) + '-' + str(starting_day[6:8]) + ' 00:00:00'
    dates = (start, close)
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
    ga_tickets = ga_tickets_.where((ga_tickets_.CREATION_TIME < date_to) & (ga_tickets_.CREATION_TIME > date_from))
    ga_tickets_detalle = ga_tickets_detalle.drop_duplicates(subset=['OBJID', 'FECHA_1ER_CIERRE'])
    tickets_ = ga_tickets.withColumn('ticket_date',
                                     concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')) \
        .withColumn('DURACION', regexp_replace('DURACION', ',', '.')) \
        .join(ga_tickets_detalle, ['OBJID'], 'left_outer')
    clientes = clientes.withColumnRenamed('OBJID', 'CLIENTES_ID')

    tickets = tickets_.join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.CLIENTES_ID, 'inner') \
        .filter(col('NIF_CLIENTE').isNotNull())

    print'Number of tickets during period: ' + str(tickets.count())
    from pyspark.sql import Window
    window = Window.partitionBy("NIF_CLIENTE")
    df = tickets

    codigos = ['Tramitacion', 'Reclamacion', 'Averia', 'Incidencia']

    from pyspark.sql.functions import countDistinct
    df_tickets = df.groupby('nif_cliente').agg(
        *[countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias('num_tickets_tipo_' + TIPO.lower())
          for TIPO in codigos])

    base_cols = [col_ for col_ in df_tickets.columns if col_.startswith('num_tickets_tipo_')]
    save_cols = base_cols
    return df_tickets.select(['NIF_CLIENTE'] + base_cols).drop_duplicates(["nif_cliente"])

def _get_tickets_car(spark, closing_day):
    from churn_nrt.src.utils.date_functions import move_date_n_days
    closing_day_w = move_date_n_days(closing_day, n=-7)
    closing_day_2w = move_date_n_days(closing_day, n=-15)
    closing_day_4w = move_date_n_days(closing_day, n=-30)
    closing_day_8w = move_date_n_days(closing_day, n=-60)

    tickets_w = get_module_tickets(spark, closing_day_w, closing_day)
    tickets_w2 = get_module_tickets(spark, closing_day_2w, closing_day)
    tickets_w4 = get_module_tickets(spark, closing_day_4w, closing_day)
    tickets_w8 = get_module_tickets(spark, closing_day_8w, closing_day)
    tickets_w4w2 = get_module_tickets(spark, closing_day_4w, closing_day_2w)
    tickets_w8w4 = get_module_tickets(spark, closing_day_8w, closing_day_4w)

    base_cols = [col_ for col_ in tickets_w8.columns if col_.startswith('num_tickets_tipo_')]

    for col_ in base_cols:
        tickets_w = tickets_w.withColumnRenamed(col_, col_ + '_w')
        tickets_w8 = tickets_w8.withColumnRenamed(col_, col_ + '_w8')
        tickets_w4 = tickets_w4.withColumnRenamed(col_, col_ + '_w4')
        tickets_w2 = tickets_w2.withColumnRenamed(col_, col_ + '_w2')
        tickets_w4w2 = tickets_w4w2.withColumnRenamed(col_, col_ + '_w4w2')
        tickets_w8w4 = tickets_w8w4.withColumnRenamed(col_, col_ + '_w8w4')

    df_tickets = tickets_w8.join(tickets_w4, ['NIF_CLIENTE'], 'left').join(tickets_w2, ['NIF_CLIENTE'], 'left') \
        .join(tickets_w4w2, ['NIF_CLIENTE'], 'left').join(tickets_w8w4, ['NIF_CLIENTE'], 'left').join(tickets_w, ['NIF_CLIENTE'], 'left').fillna(0)

    for col_ in base_cols:
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w2w2', col(col_ + '_w2') - col(col_ + '_w4w2'))
        df_tickets = df_tickets.withColumn('INC_' + col_ + '_w4w4', col(col_ + '_w4') - col(col_ + '_w8w4'))

    return df_tickets.fillna(0)

def _get_tickets_timing_car(spark, starting_day, closing_day):
    ga_tickets_ = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(
        col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')

    print'Starting day: ' + starting_day
    print'Closing day: ' + closing_day
    print("############ Loading ticket sources ############")

    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad

    ga_tickets_ = ga_tickets_.withColumn('CREATION_TIME',
                                         to_timestamp(ga_tickets_.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
    close = str(closing_day[0:4]) + '-' + str(closing_day[4:6]) + '-' + str(closing_day[6:8]) + ' 00:00:00'
    start = str(starting_day[0:4]) + '-' + str(starting_day[4:6]) + '-' + str(starting_day[6:8]) + ' 00:00:00'
    dates = (start, close)
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

    ga_tickets = ga_tickets_.where((ga_tickets_.CREATION_TIME < date_to) & (ga_tickets_.CREATION_TIME > date_from))
    ga_tickets_detalle = ga_tickets_detalle.drop_duplicates(subset=['OBJID', 'FECHA_1ER_CIERRE'])
    tickets_ = ga_tickets.withColumn('ticket_date',
                                     concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0'))) \
        .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss')) \
        .withColumn('DURACION', regexp_replace('DURACION', ',', '.')) \
        .join(ga_tickets_detalle, ['OBJID'], 'left_outer')
    clientes = clientes.withColumnRenamed('OBJID', 'CLIENTES_ID')

    tickets = tickets_.join(clientes, ga_tickets.CASE_REPORTER2YESTE == clientes.CLIENTES_ID, 'inner') \
        .filter(col('NIF_CLIENTE').isNotNull())
    print'Size of tickets df: '+str(tickets.count())


    tickets = tickets.where((col('X_TIPO_OPERACION')== 'Tramitacion')|(col('X_TIPO_OPERACION')== 'Reclamacion')|(col('X_TIPO_OPERACION')== 'Averia')|(col('X_TIPO_OPERACION')== 'Incidencia'))
    print'Size of filtered tickets df: ' + str(tickets.count())
    print'Creating timing ticket features'
    from pyspark.sql.functions import unix_timestamp
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    timeDiff = (unix_timestamp('FECHA_1ER_CIERRE', format=timeFmt)
                - unix_timestamp('CREATION_TIME', format=timeFmt))
    timeDiff_closing = (unix_timestamp('closing_date', format=timeFmt)
                        - unix_timestamp('CREATION_TIME', format=timeFmt))
    tickets = tickets.withColumn("Ticket_Time", timeDiff)
    tickets = tickets.withColumn('closing_date', lit(date_to)).withColumn("Ticket_Time_closing", timeDiff_closing)

    tickets = tickets.withColumn("Ticket_Time_hours", col("Ticket_Time") / 3600)
    tickets = tickets.withColumn("Ticket_Time_opened_hours", col("Ticket_Time_closing") / 3600)

    codigos = ['Tramitacion', 'Reclamacion', 'Averia', 'Incidencia']
    print'Creating ticket count (per type and state) features'
    df_opened = tickets.where(col('FECHA_1ER_CIERRE').isNull())
    df_opened_count = df_opened \
        .groupby('nif_cliente').agg(*([countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias(
        'num_tickets_tipo_' + TIPO.lower() + '_opened') for TIPO in codigos] + [
                                          countDistinct(col("OBJID")).alias('num_tickets_opened')]))

    df_closed = tickets.where(~col('FECHA_1ER_CIERRE').isNull())
    df_closed_count = df_closed \
        .groupby('nif_cliente').agg(*([countDistinct(when(col("X_TIPO_OPERACION") == TIPO, col("OBJID"))).alias(
        'num_tickets_tipo_' + TIPO.lower() + '_closed') for TIPO in codigos] + [
                                          countDistinct(col("OBJID")).alias('num_tickets_closed')]))

    from pyspark.sql.functions import max as sql_max, min as sql_min, mean as sql_avg, stddev

    tickets_closed = df_closed
    df_stats_cl = tickets_closed.groupby('nif_cliente').agg(
        *([sql_max(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('max_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_min(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('min_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              stddev(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('std_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_hours"))) \
          .alias('mean_time_closed_tipo_' + TIPO.lower()) for TIPO in codigos])).fillna(0)

    tickets_opened = df_opened
    df_stats_op = tickets_opened.groupby('nif_cliente').agg(
        *([sql_max(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('max_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_min(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('min_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              stddev(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('std_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(when(col("X_TIPO_OPERACION") == TIPO, col("Ticket_Time_opened_hours"))) \
          .alias('mean_time_opened_tipo_' + TIPO.lower()) for TIPO in codigos] + [
              sql_avg(col("Ticket_Time_opened_hours")) \
          .alias('mean_time_opened')] + [sql_max(col("Ticket_Time_opened_hours")).alias('max_time_opened')] + [
              sql_min(col("Ticket_Time_opened_hours")) \
          .alias('min_time_opened')] + [stddev(col("Ticket_Time_opened_hours")).alias('std_time_opened')])).fillna(0)

    print'Creating week repetition features'
    from pyspark.sql.types import DoubleType, StringType, IntegerType, TimestampType
    from churn_nrt.src.utils.date_functions import move_date_n_days
    vec_dates = []

    aux = closing_day
    for i in range(1, 9):
        date_ = move_date_n_days(closing_day, n=-7 * i)
        close = str(aux[0:4]) + '-' + str(aux[4:6]) + '-' + str(aux[6:8]) + ' 00:00:00'
        start = str(date_[0:4]) + '-' + str(date_[4:6]) + '-' + str(date_[6:8]) + ' 00:00:00'
        dates = (start, close)
        date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
        vec_dates.append([date_from, date_to, i, i + 1])
        aux = date_

    df_reit = tickets \
        .groupby('nif_cliente').agg(*[countDistinct(
        when((col("X_TIPO_OPERACION") == TIPO) & (col("CREATION_TIME") > x[0]) & (col("CREATION_TIME") < x[1]),
             col("OBJID"))).alias('num_tickets_' + TIPO.lower() + '_' + str(x[2] - 1) + '_' + str(x[3] - 1)) for TIPO in
                                      codigos for x in vec_dates])

    aux_cols = df_reit.columns[1:]

    for name in aux_cols:
        df_reit = df_reit.withColumn(name + '_', when(col(name) > 0, 1.0).otherwise(0.0))

    av_cols = [name + '_' for name in aux_cols if 'averia' in name]
    tram_cols = [name + '_' for name in aux_cols if 'tramitacion' in name]
    rec_cols = [name + '_' for name in aux_cols if 'reclamacion' in name]
    inc_cols = [name + '_' for name in aux_cols if 'incidencia' in name]


    from operator import add
    from functools import reduce

    df_reit = df_reit.withColumn('weeks_averias', reduce(add, [col(x) for x in av_cols]))
    df_reit = df_reit.withColumn('weeks_facturacion', reduce(add, [col(x) for x in tram_cols]))
    df_reit = df_reit.withColumn('weeks_reclamacion', reduce(add, [col(x) for x in rec_cols]))
    df_reit = df_reit.withColumn('weeks_incidencias', reduce(add, [col(x) for x in inc_cols]))

    save_cols = [name for name in df_reit.columns if 'weeks' in name] + ['NIF_CLIENTE']

    df_reit = df_reit.select(save_cols)

    codigos_iso = [('X220 No llega señal', 'X220_no_signal'), ('M5 Problema RF', 'M5_problema_RF'),
                   ('M3 Fallo CM', 'M3_Fallo_CM'), ('T6 No hay tono', 'T6_no_hay_tono'),
                   ('Incomunicacion', 'Incomunicacion'),
                   ('P05 Deco defectuoso', 'P05_deco_defectuoso'), ('Portabilidad', 'Portabilidad'),
                   ('T7 Conexiones', 'T7_conexiones')]

    print'Creating isolated customer features'
    from pyspark.sql.functions import count as sql_count
    df_iso = tickets.groupby('nif_cliente').agg(
        *[sql_count(when(col("X_CODIGO_APERTURA") == x[0], col("X_CODIGO_APERTURA"))).alias('count_' + x[1]) for x in
          codigos_iso])

    df1 = df_opened_count.join(df_stats_op, ['NIF_CLIENTE'], 'full_outer')
    df2 = df_closed_count.join(df_stats_cl, ['NIF_CLIENTE'], 'full_outer')

    df_final = df1.join(df2, ['NIF_CLIENTE'], 'full_outer').join(df_reit, ['NIF_CLIENTE'], 'full_outer') \
        .join(df_iso, ['NIF_CLIENTE'], 'full_outer')

    return df_final



class Tickets(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "tickets")


    def build_module(self, closing_day, force_gen=False, window_cycles=-4, **kwargs):

        print("Building {} module for closing_day {}".format(self.MODULE_NAME, closing_day))

        tickets = _get_tickets_car(self.SPARK, closing_day)

        print'Number of distinct NIFs in tickets (states): ' + str(tickets.select('nif_cliente').distinct().count())

        from churn_nrt.src.utils.date_functions import move_date_n_days

        starting_day = move_date_n_days(closing_day, n=-60)

        print'Creating additional tickets feats...'
        tickets_timing = _get_tickets_timing_car(self.SPARK, starting_day, closing_day)


        print'Number of distinct NIFs in tickets (timing): ' + str(tickets_timing.select('nif_cliente').distinct().count())

        df_tickets = tickets.join(tickets_timing, ['NIF_CLIENTE'], 'left_outer')

        return df_tickets


    def get_metadata(self):

        dict_ = {'num_tickets_tipo_tramitacion_opened': 0.0, 'num_tickets_tipo_reclamacion_opened': 0.0, 'num_tickets_tipo_averia_opened': 0.0, 'num_tickets_tipo_incidencia_opened': 0.0, \
                        'num_tickets_opened': 0.0, 'max_time_opened_tipo_tramitacion': -1.0, 'max_time_opened_tipo_reclamacion': -1.0, 'max_time_opened_tipo_averia': -1.0, \
                        'max_time_opened_tipo_incidencia': -1.0, 'min_time_opened_tipo_tramitacion': -1.0, 'min_time_opened_tipo_reclamacion': -1.0, 'min_time_opened_tipo_averia': -1.0, \
                        'min_time_opened_tipo_incidencia': -1.0, 'std_time_opened_tipo_tramitacion': -1.0, 'std_time_opened_tipo_reclamacion': -1.0, 'std_time_opened_tipo_averia': -1.0, \
                        'std_time_opened_tipo_incidencia': -1.0, 'mean_time_opened_tipo_tramitacion': -1.0, 'mean_time_opened_tipo_reclamacion': -1.0, 'mean_time_opened_tipo_averia': -1.0, \
                        'mean_time_opened_tipo_incidencia': -1.0, 'mean_time_opened': -1.0, 'max_time_opened': -1.0, 'min_time_opened': -1.0, 'std_time_opened': -1.0, \
                        'num_tickets_tipo_tramitacion_closed': 0.0, 'num_tickets_tipo_reclamacion_closed': 0.0, 'num_tickets_tipo_averia_closed': 0.0, 'num_tickets_tipo_incidencia_closed': 0.0, \
                        'num_tickets_closed': 0.0, 'max_time_closed_tipo_tramitacion': -1.0, 'max_time_closed_tipo_reclamacion': -1.0, 'max_time_closed_tipo_averia': -1.0, \
                        'max_time_closed_tipo_incidencia': -1.0, 'min_time_closed_tipo_tramitacion': -1.0, 'min_time_closed_tipo_reclamacion': -1.0, 'min_time_closed_tipo_averia': -1.0, \
                        'min_time_closed_tipo_incidencia': -1.0, 'std_time_closed_tipo_tramitacion': -1.0, 'std_time_closed_tipo_reclamacion': -1.0, 'std_time_closed_tipo_averia': -1.0, \
                        'std_time_closed_tipo_incidencia': -1.0, 'mean_time_closed_tipo_tramitacion': -1.0, 'mean_time_closed_tipo_reclamacion': -1.0, 'mean_time_closed_tipo_averia': -1.0, \
                        'mean_time_closed_tipo_incidencia': -1.0, 'weeks_averias': 0.0, 'weeks_facturacion': 0.0, 'weeks_reclamacion': 0.0, 'weeks_incidencias': 0.0, 'count_X220_no_signal': 0.0, \
                        'count_M5_problema_RF': 0.0, 'count_M3_Fallo_CM': 0.0, 'count_T6_no_hay_tono': 0.0, 'count_Incomunicacion': 0.0, 'count_P05_deco_defectuoso': 0.0, 'count_Portabilidad': 0.0, \
                        'count_T7_conexiones': 0.0}

        feats = dict_.keys()

        na_vals = dict_.values()

        na_vals = [str(x) for x in na_vals]

        data = {'feature': feats, 'imp_value': na_vals}

        import pandas as pd

        metadata1_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('tickets_additional')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('nif'))

        types = ['tramitacion', 'reclamacion', 'averia', 'incidencia']
        weeks = ['_w', '_w2', '_w4', '_w8', '_w4w2', '_w8w4']
        weeks_inc = ['_w2w2', '_w4w4']

        tick_feats = ['num_tickets_tipo_' + typ + w for typ in types for w in weeks] + ['INC_num_tickets_tipo_' + typ + w for typ in types for w in weeks_inc]
        null_imp_dict = dict([(x, 0.0) for x in tick_feats])

        feats = null_imp_dict.keys()

        na_vals = null_imp_dict.values()

        na_vals = [str(x) for x in na_vals]

        data = {'feature': feats, 'imp_value': na_vals}


        metadata2_df = self.SPARK.createDataFrame(pd.DataFrame(data)) \
            .withColumn('source', lit('tickets')) \
            .withColumn('type', lit('numeric')) \
            .withColumn('level', lit('nif'))

        return metadata1_df.union(metadata2_df)




        return metadata_df



