import sys
from datetime import datetime as dt
import imp
import os
import numpy as np
from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull,substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType


def set_paths_and_logger():
    '''
    :return:
    '''

    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print(pathname)
    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):

        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)

        #from churn.utils.constants import CHURN_DELIVERIES_DIR
        #root_dir = CHURN_DELIVERIES_DIR
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

    mypath = os.path.join(root_dir, "amdocs_informational_dataset")
    if mypath not in sys.path:
        sys.path.insert(0, mypath)
        print("Added '{}' to path".format(mypath))


    return root_dir

def get_next_dow(weekday, from_date=None):
    '''
    weekday: weekday is 1 for monday; 2 for tuesday; ...; 7 for sunday.
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=5) [get next friday] and the function returns datetime.date(2019, 6, 14) [14-June-2019]
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=2) [get next tuesday] and the function returns datetime.date(2019, 6, 11) [11-June-2019, Today]

    Note: weekday=0 is the same as weekday=7
    Note: this function runs with isoweekday (monday is 1 not 0)

    from_date: if from_date != None, instead of using today uses this day.

    '''

    from_date = from_date if from_date else dt.date.today()
    import datetime as dt2
    return from_date + dt2.timedelta( (weekday-from_date.isoweekday()) % 7 )

def create_model_output_dataframes(spark, predict_closing_day, df_model_scores, model_params_dict, extra_info_cols=None):
    from churn.delivery.delivery_constants import MODEL_OUTPUTS_NULL_TAG
    from churn.datapreparation.general.model_outputs_manager import ensure_types_model_scores_columns
    MODEL_TRIGGERS_TICKETS =  "triggers_tickets"
    executed_at = dt.now().strftime("%Y-%m-%d %H:%M:%S ")

    # TEMPORARY SOLUTION FOR RETURN FEED. PARTITION IS SET TO THE DATE OF NEXT WEDNESDAY (after delivery of 6-aug)
    #from pykhaos.utils.date_functions import get_next_dow
    return_feed_execution = get_next_dow(weekday=1, from_date=dt.strptime(predict_closing_day, "%Y%m%d")).strftime("%Y%m%d")

    day_partition = int(return_feed_execution[6:])
    month_partition = int(return_feed_execution[4:6])
    year_partition = int(return_feed_execution[:4])

    print("dataframes of model outputs set with values: year={} month={} day={}".format(year_partition, month_partition, day_partition))

    '''
    MODEL PARAMETERS
    '''

    model_params_dict.update({
        "model_name": [MODEL_TRIGGERS_TICKETS],
        "executed_at": [executed_at],
        "year": [year_partition],
        "month": [month_partition],
        "day": [day_partition],
        "time": [int(executed_at.split(" ")[1].replace(":", ""))],
        "scores_extra_info_headers": [" "]
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

    df_model_scores = (df_model_scores.withColumn("extra_info", lit(""))
                       .withColumn("prediction", lit("0"))
                       .withColumn("model_name", lit(MODEL_TRIGGERS_TICKETS))
                       .withColumn("executed_at", lit(executed_at))
                       .withColumn("model_executed_at", lit(executed_at))
                       .withColumn("year", lit(year_partition).cast("integer"))
                       .withColumn("month", lit(month_partition).cast("integer"))
                       .withColumn("day", lit(day_partition).cast("integer"))
                       .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .withColumn("predict_closing_date", lit(closing_day))
                       .withColumn("model_output", lit(None))
                       )
    try:
        df_model_scores.show(20)
    except UnicodeEncodeError as e:
        print(e)

    except Exception as e:
        print(e)

    df_model_scores = ensure_types_model_scores_columns(df_model_scores)

    df_model_scores = df_model_scores.sort(desc("scoring"))

    return df_parameters, df_model_scores


if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser(description = 'List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar = '<closing_d>', type= str, help= 'closing day', default = 'None')
    parser.add_argument('-n', '--n_tickets', metavar = '<n_tickets>', type= int, help= 'tickets threshold', default = 55)
    
    args = parser.parse_args()

    set_paths_and_logger()

    import pykhaos.utils.pyspark_configuration as pyspark_config
    from pyspark.sql.functions import *

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="ticket_triggers", log_level="OFF", min_n_executors = 1, max_n_executors = 15, n_cores = 4, executor_memory = "32g", driver_memory="32g")
    print("############ Process Started ############")
   
    ga_tickets = spark.read.parquet('/data/raw/vf_es/callcentrecalls/TICKETSOW/1.2/parquet')
    ga_tickets_detalle = spark.read.table('raw_es.callcentrecalls_ticketdetailow')
    ga_franquicia = spark.read.table('raw_es.callcentrecalls_ticketfranchiseow')
    ga_close_case = spark.read.table('raw_es.callcentrecalls_ticketclosecaseow')
    clientes = spark.read.table('raw_es.customerprofilecar_customerow').select('OBJID', 'NIF_CLIENTE').filter(col('NIF_CLIENTE').isNotNull()).filter(col('NIF_CLIENTE') != '').filter(col('NIF_CLIENTE') != '7')
    ga_tipo_tickets = spark.read.parquet('/data/raw/vf_es/cvm/GATYPETICKETS/1.0/parquet')
    print("############ Loaded ticket sources ############")
    
    if args.closing_d != 'None':
        closing_day = str(args.closing_d)
    else:
        from churn.datapreparation.general.customer_base_utils import get_last_date
        closing_day = get_last_date(spark)
    
    print'Prediction date: {}'.format(closing_day)
    
    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles
    starting_day = move_date_n_cycles(closing_day, n=-4)
    
    from pyspark.sql.functions import year, month, dayofmonth, regexp_replace, to_timestamp, when, concat, lpad
    tickets = (ga_tickets
                    .where( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                           &(concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
                    .withColumn('CREATION_TIME', to_timestamp(ga_tickets.CREATION_TIME, 'dd/MM/yyyy HH.mm.ss'))
                    .withColumn('DURACION', regexp_replace('DURACION', ',', '.'))
                    .join(ga_tickets_detalle, ga_tickets.OBJID==ga_tickets_detalle.OBJID, 'left_outer')
                    .join(ga_franquicia, ga_tickets_detalle.OBJID==ga_franquicia.ID_FRANQUICIA, 'left_outer')
                    .join(ga_close_case, ga_tickets.OBJID==ga_close_case.OBJID, 'left_outer')
                    .join(clientes, ga_tickets.CASE_REPORTER2YESTE==clientes.OBJID, 'left_outer')
                    .join(ga_tipo_tickets, ga_tickets.ID_TIPO_TICKET==ga_tipo_tickets.ID_TIPO_TICKET, 'left_outer')
                    .filter(col('NIF_CLIENTE').isNotNull()))
    
    tickets.groupby('X_TIPO_OPERACION').count().sort('count', ascending=False).show(truncate=False)
    
    print'Total number of tickets between {} and {}: {}'.format(starting_day, closing_day, tickets.count())
    
    print'Total number of different customers with tickets between {} and {}: {}'.format(starting_day, closing_day, tickets.select('NIF_CLIENTE').distinct().count())
    
    print("############ Loading customer base ############")
    
    from churn.datapreparation.general.customer_base_utils import  get_customer_base_segment
    df_base_msisdn = get_customer_base_segment(spark, date_=closing_day)
    
    print'Total number of services: {}'.format(df_base_msisdn.count())
    print'Total number of different NIFs: {}'.format(df_base_msisdn.select('NIF_CLIENTE').distinct().count())
    
    from churn.analysis.triggers.orders.run_segment_orders import get_ccc_attrs_w8
    
    df_ccc = get_ccc_attrs_w8(spark, closing_day, df_base_msisdn)    
    df_tar = df_ccc.filter(col('CHURN_CANCELLATIONS_w8') == 0)
    
    print'Total number of customers with recent churn cancellations: {}'.format(df_ccc.count()-df_tar.count())
    
    df_fact = tickets.where(col('X_TIPO_OPERACION') == 'Tramitacion')
    df_rec = tickets.where(col('X_TIPO_OPERACION') == 'Reclamacion')
    averias = tickets.where(col('X_TIPO_OPERACION') == 'Averia')
    df_inc = tickets.where(col('X_TIPO_OPERACION') == 'Incidencia')
    df_pet = tickets.where(col('X_TIPO_OPERACION') == 'Peticion')
    
    print'Number of billing tickets: {}'.format(df_fact.count())
    #df_fact.groupby('TIPO_TICKET').count().sort('count', ascending=False).show(truncate=False)
    
    from pyspark.sql.functions import countDistinct
    df_fact_nif = df_fact.select('NIF_CLIENTE', 'ID_NUMBER').groupby('NIF_CLIENTE').agg(countDistinct('ID_NUMBER').alias('NUM_AVERIAS_NIF'))
    
    print'Number of NIFs with billing tickets: {}'.format(df_fact_nif.count())
    
    df_fact_labeled = df_tar.join(df_fact, ['NIF_CLIENTE'], 'inner')
    df_fact_nulls = df_fact_labeled.where(col('TIPO_TICKET').isNull())    
    print'Number of target NIFs with billing tickets: {}'.format(df_fact_nulls.select('NIF_CLIENTE').distinct().count())
    
    from pyspark.sql.functions import countDistinct, count, sum
    from pyspark.sql import Row, DataFrame, Column, Window
    window = Window.partitionBy("NIF_CLIENTE")
    
    df = df_fact_nulls
    df = df.select('NIF_CLIENTE', 'TIPO_TICKET')\
        .withColumn('tmp', when((col('TIPO_TICKET').isNull()), 1.0).otherwise(0.0))\
        .withColumn('scoring', sum('tmp').over(window))
    
    trigger_customers = df.where(col('scoring') > args.n_tickets).drop('tmp','TIPO_TICKET').drop_duplicates(subset=['NIF_CLIENTE']).sort(desc("scoring")).withColumnRenamed('NIF_CLIENTE','nif')
    
    print'Number of customers with the trigger condition: {}'.format(trigger_customers.count())
    
    #hdfs_root_dir = 
    #hdfs_partition_path = 'year=' + str(int(closing_day[:4])) + '/month=' + str(int(closing_day[4:6])) + '/day=' + str(int(closing_day[6:8]))
    
    model_params_dict = {"model_level": ["nif"],
                             "training_closing_date": ['20190607'],
                             "target": ['Total number of billing tickets'],
                             "model_path": [" "],
                             # car used to find the rule
                             "metrics_path": [""],
                             "metrics_train": ["vol={};churn_rate={};lift={}".format(16300, 6.70, 3.98)],  # volume after filters
                             "metrics_test": [""],
                             "varimp": ["Num_tickets_tramitacion"],
                             "algorithm": ["manual"],
                             "author_login": ["ds_team"],
                             "extra_info": [""],
                             }

    df_parameters, df_model_scores = create_model_output_dataframes(spark, closing_day,
                                                                        trigger_customers, model_params_dict,
                                                                        None)

    #df_parameters.show()
    print("df_model_scores={}".format(df_model_scores.count()))
    
    print("Starting to save to model outputs")

    from churn.datapreparation.general.model_outputs_manager import insert_to_model_scores, insert_to_model_parameters
    insert_to_model_scores(df_model_scores)
    print("Inserted to model outputs scores")
    insert_to_model_parameters(df_parameters)
    print("Inserted to model outputs parameters")
    
    
    vec = [5,25,55,60,75,100]
    vols = []
    ratio = []
    for i in vec:
        num_t = df.where(col('scoring') > i).select('NIF_CLIENTE').distinct().count()
        print'Total volume of customers with more than {} billing tickets: {}'.format(i,num_t)
    
    print("############ Finished Process ############")