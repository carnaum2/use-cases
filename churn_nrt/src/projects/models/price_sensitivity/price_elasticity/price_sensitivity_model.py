#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
import sys
import time
# from pyspark.sql.window import Window
# from pyspark.sql.functions import (
#                                    col,
#                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
#                                    avg as sql_avg,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
#                                    )
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# import datetime as dt
from pyspark.sql.functions import col, count, when, lit, length, concat_ws, regexp_replace, year, month, dayofmonth, split, regexp_extract, coalesce

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re
    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset/')
    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))


def get_target_nifs_ids(spark, date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_ccc=True, n_cycles=12, horizon=4, gap_window=4, verbose=False):

    from churn_nrt.src.utils.date_functions import move_date_n_cycles
    date_prev = move_date_n_cycles(date_, -n_cycles)

    horizon_sup = (horizon+gap_window) * 7
    horizon_inf = gap_window*7

    PATH_IDS = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/'

    from churn_nrt.src.utils.constants import PARTITION_DATE

    partition_date = PARTITION_DATE.format(date_[:4], str(int(date_[4:6])), str(int(date_[6:8])))
    partition_date_prev = PARTITION_DATE.format(date_prev[:4], str(int(date_prev[4:6])), str(int(date_prev[6:8])))

    path_ids = PATH_IDS + partition_date
    path_ids_prev = PATH_IDS + partition_date_prev

    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists
    print(path_ids)
    if check_hdfs_exists(path_ids):
        df_ids_ori = spark.read.load(path_ids)
        df_ids = df_ids_ori.select('nif_cliente', 'tgs_days_until_fecha_fin_dto').distinct()
    else:
        import sys
        print('IDS is not generated for date {}'.format(date_))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)
    print(path_ids_prev)
    if check_hdfs_exists(path_ids_prev):
        pass
    else:
        import sys
        print('IDS is not generated for date {}'.format(date_prev))
        print('IDS must be created first, in order to run the model. Exiting process')
        sys.exit(-1)
    if verbose:
        print('Number of distinct nif_cliente-tgs_dto: {}'.format(df_ids.count()))
        print('Number of distinct nif_cliente {}'.format(df_ids.select('nif_cliente').distinct().count()))

    df_ids = df_ids.where((col('tgs_days_until_fecha_fin_dto') < horizon_sup) & (col('tgs_days_until_fecha_fin_dto') > horizon_inf))
    if verbose:
        print('Size target customers df: {}'.format(df_ids.count()))

    if filter_recent:
        from churn_nrt.src.data_utils.ids_filters import get_non_recent_customers_filter
        df_ids_rec = get_non_recent_customers_filter(spark, date_, n_cycles, level='nif', verbose=verbose)
        df_ids = df_ids.join(df_ids_rec.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after early churn filter: {}'.format(df_ids.count()))

    if filter_disc:
        from churn_nrt.src.data_utils.ids_filters import get_disconnection_process_filter
        df_ids_disc = get_disconnection_process_filter(spark, date_, n_cycles, verbose)
        df_ids = df_ids.join(df_ids_disc.select('nif_cliente'), ['nif_cliente'], 'inner')
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after disconnection filter: {}'.format(df_ids.count()))


    if filter_ord:
        df_ids = df_ids.join(df_ids_ori.select('nif_cliente', "Ord_sla_has_forbidden_orders_last90"), ['nif_cliente'], 'inner').where(~(col("Ord_sla_has_forbidden_orders_last90") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after forbidden orders filter: {}'.format(df_ids.count()))

    if filter_ccc:
        df_ids = df_ids.join(df_ids_ori.select('nif_cliente', "CCC_num_calls_w4"), ['nif_cliente'], 'inner').where(~(col("CCC_num_calls_w4") > 0))
        df_ids = df_ids.cache()
        if verbose:
            print('Size of df after forbidden orders filter: {}'.format(df_ids.count()))

    df_ids_final = df_ids.select('nif_cliente').drop_duplicates(['nif_cliente'])
    df_ids_final = df_ids_final.cache()
    print('Number of NIFs on the final IDS target: {}'.format(df_ids_final.select('nif_cliente').distinct().count()))

    return df_ids_final

def get_ccc_label(spark, date_, horizon=8, gap_window = 2, gap_label = 4, verbose=False):
    from pyspark.sql.functions import upper, isnull, col, unix_timestamp, from_unixtime, countDistinct
    bucket = spark.read.format("csv").option("header", "true").option("delimiter", ";") \
        .load("/data/udf/vf_es/ref_tables/amdocs_ids/Agrup_Buckets_unific.txt").withColumn('INT_Tipo',upper(col('INT_Tipo'))) \
        .withColumn('INT_Subtipo', upper(col('INT_Subtipo'))) \
        .withColumn('INT_Razon', upper(col('INT_Razon'))) \
        .withColumn('INT_Resultado', upper(col('INT_Resultado'))) \
        .withColumn("bucket", upper(col("bucket"))) \
        .withColumn("sub_bucket", upper(col("sub_bucket"))) \
        .filter((~isnull(col('bucket'))) & (~col('bucket').isin("", " ")))

    select_trip = bucket.where( \
        (col('INT_Razon').contains('PERMANENCIA')) | \
        (col('INT_Razon').contains('PLAN PRECIOS')) | \
        (col('INT_Razon').contains('DESCUENTOS')) | \
        (col('INT_Razon').contains('DTO')) | \
        (col('INT_Razon').contains('PROMO')) | \
        (col('INT_Razon').contains('TOTAL A PAGAR')) | \
        (col('INT_Razon').contains('IMPORTE')) | \
        (col('INT_Subtipo').contains('DEUDAS Y PAGOS')) | \
        (col('INT_Subtipo').contains('DUDAS FACTURACION')) | \
        (col('INT_Subtipo').contains('ERROR FACTURA')) | (col('INT_Subtipo').contains('ERROR IMPORTE')) | \
        (col('INT_Subtipo').contains('EXPLICAR FACTURA')) | (col('INT_Subtipo').contains('TARIFICA')) | \
        ((col('Bucket').contains('CHURN')) | (col('Sub_Bucket').contains('CHURN'))))
    if verbose:
        select_trip = select_trip.cache()
        print("Number of selected triplets: {}".format(select_trip.count()))

    from churn_nrt.src.utils.date_functions import move_date_n_cycles

    date_inf = date_
    date_sup = move_date_n_cycles(date_, horizon + gap_window + gap_label)

    interactions_ono = spark.table('raw_es.callcentrecalls_interactionono') \
        .withColumn('formatted_date', from_unixtime(unix_timestamp(col("FX_CREATE_DATE"), "yyyy-MM-dd"))) \
        .filter((col('formatted_date') >= from_unixtime(unix_timestamp(lit(date_inf), "yyyyMMdd"))) & (
                col('formatted_date') <= from_unixtime(unix_timestamp(lit(date_sup), "yyyyMMdd"))))

    interactions_ono = interactions_ono.filter(
        "DS_DIRECTION IN ('De entrada', 'Entrante')")  # DS_DIRECTION IN ('De entrada', 'Entrante')
    interactions_ono = interactions_ono.filter(col("CO_TYPE").rlike('(?i)^Llamada Telefono$|^Telefonica$|$\.\.\.^'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^BO'))
    interactions_ono = interactions_ono.filter(~col("DS_X_GROUP_WORK").rlike('(?i)^B\.O'))
    interactions_ono = interactions_ono.filter(~col("DS_REASON_1").rlike('(?i)^Emis'))
    interactions_ono = interactions_ono.filter(
        ~col("DS_REASON_1").rlike('(?i)^Gestion B\.O\.$|(?i)^Gestion casos$|(?i)^Gestion Casos Resueltos$|' \
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
        .withColumnRenamed('DS_RESULT', 'INT_Resultado') \
        .withColumnRenamed('DS_DIRECTION', 'DIRECTION') \
        .withColumnRenamed('CO_TYPE', 'TYPE_TD') \
        .withColumn('INT_Tipo', upper(col('INT_Tipo'))) \
        .withColumn('INT_Subtipo', upper(col('INT_Subtipo'))) \
        .withColumn('INT_Razon', upper(col('INT_Razon'))) \
        .withColumn('INT_Resultado', upper(col('INT_Resultado')))

    interactions_ono_msisdn = interactions_ono.withColumnRenamed('DS_X_PHONE_CONSULTATION', 'msisdn')
    interactions_ono_target = interactions_ono_msisdn.join(select_trip.select('INT_Tipo', 'INT_Subtipo', 'INT_Razon').distinct(), ['INT_Tipo', 'INT_Subtipo', 'INT_Razon'],'inner')

    if verbose:
        interactions_ono_target = interactions_ono_target.cache()
        print("Number of msisdn with price CCC: {}".format(interactions_ono_target.count()))

    from churn_nrt.src.data.customers_data import Customer
    from churn_nrt.src.data.services_data import Service

    df_serv = Service(spark).get_module(date_, save=False, save_others=False, force_gen=False)
    df_cust = Customer(spark).get_module(date_, save=False, save_others=False, force_gen=False)

    df_services = df_serv.join(df_cust.select('NUM_CLIENTE', 'NIF_CLIENTE'), ['NUM_CLIENTE'], 'inner') \
        .withColumnRenamed('NUM_CLIENTE', 'num_cliente') \
        .withColumnRenamed('NIF_CLIENTE', 'nif_cliente').select(["msisdn", "num_cliente", "nif_cliente"])

    interactions_ono_target_nif = interactions_ono_target.join(df_services.select("msisdn", "nif_cliente"), ['msisdn'], 'inner')

    label_calls_agg = interactions_ono_target_nif.groupBy('nif_cliente').agg(countDistinct('CL_OBJID').alias('label_calls'))

    if verbose:
        label_calls_agg = label_calls_agg.cache()
        print("Number of NIFs with price CCC: {}".format(label_calls_agg.count()))

    return label_calls_agg

def get_price_target(spark, date_, horizon=4, gap_window = 4, gap_label = 4, verbose=False):

    ########## Labels at NIF level ##########
    from churn_nrt.src.data.sopos_dxs import Target
    target_nifs_train = Target(spark, churn_window=(horizon+gap_window+gap_label)*7, level='nif').get_module(date_, save=False, save_others=False, force_gen=True)

    ############### CCC ###############
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ccc_label = get_ccc_label(spark, date_, horizon, gap_window , gap_label, verbose).withColumn('label_ccc', when(col('label_calls')>0,1.0).otherwise(0.0))

    label_join = target_nifs_train.join(ccc_label,['nif_cliente'], 'left').fillna({'label_ccc':0.0})
    label_join = label_join.withColumn('label_price', when(((col('label') + col('label_ccc'))>0), 1.0).otherwise(0.0))
    return label_join

def get_next_day_of_the_week(day_of_the_week):

    import datetime as dt

    idx = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}
    n = idx[day_of_the_week.lower()]

    # import datetime
    d = dt.date.today()

    while d.weekday() != n:
        d += dt.timedelta(1)

    year_= str(d.year)
    month_= str(d.month).rjust(2, '0')
    day_= str(d.day).rjust(2, '0')

    print 'Next ' + day_of_the_week + ' is day ' + str(day_) + ' of month ' + str(month_) + ' of year ' + str(year_)

    return year_+month_+day_

if __name__ == "__main__":

    set_paths()
    from constants import CORRELATED_FEATS

    #from churn_nrt.src.projects.models.price_sensitivity.constants import CORRELATED_FEATS
    #from churn_nrt.src.projects.models.price_sensitivity.price_sensitivity_model import get_ccc_label
    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("price_sensitivity")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - mode_ : evaluation or prediction
    #      - model :  algorithm for training
    #      - horizon : cycles horizon to predict sensitivity
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(description="Run price_sensitivity model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Alvaro <alvaro.saez@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in test')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='model to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--horizon', metavar='8', type=str, required=False, help='tbc')
    parser.add_argument('--verbose', metavar=0, type=int, required=False, help='tbc')
    parser.add_argument('--gap_w', metavar='2', type=str, required=False, help='Number of cycles for gap window')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    model_ = args.model
    horizon = int(args.horizon)
    verbose = False
    gap_w = int(args.gap_w)
    gap_label = 4

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))
    print("ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))

    ######################## Target NIFs ########################
    df_train_nifs = get_target_nifs_ids(spark, tr_date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_ccc=True, n_cycles=12, horizon=horizon, gap_window=gap_w, verbose=False)
    #df_test_nifs = get_target_nifs_ids(spark, tt_date_, filter_recent=True, filter_disc=True, filter_ord=True,n_cycles=12, horizon=horizon, gap_window=gap_w, verbose=False)

    ######################## IDS features ########################
    from churn_nrt.src.data_utils.ids_utils import get_ids_nif
    df_train_ids = get_ids_nif(spark, tr_date_)
    df_test_ids = get_ids_nif(spark, tt_date_)

    ######################## Target Train ########################
    target_train = get_price_target(spark, tr_date_, horizon=horizon, gap_window = gap_w, gap_label= gap_label,  verbose=verbose)
    df_train_labeled = df_train_nifs.join(target_train.drop('label').drop('label_calls').drop('label_ccc'), ['nif_cliente'], 'inner').join(df_train_ids, ['nif_cliente'], 'inner')
    df_train_labeled = df_train_labeled.cache()
    print('Size of the labeled training df: {}'.format(df_train_labeled.count()))
    df_train_labeled.groupBy('label_price').agg(count('*')).show()
    #df_train_labeled.groupby('label', 'label_price', 'label_ccc').agg(count('*')).show(10)

    ######################## Target Test ########################
    if mode_ == 'evaluation':
        df_test_nifs = get_target_nifs_ids(spark, tt_date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_ccc=True, n_cycles=12, horizon=horizon, gap_window=gap_w, verbose=False)
        target_test = get_price_target(spark, tt_date_, horizon=horizon, gap_window = gap_w, gap_label= gap_label, verbose=verbose)
        df_test_labeled = df_test_nifs.join(target_test, ['nif_cliente'], 'inner').join(df_test_ids,['nif_cliente'], 'inner')
        df_test_labeled = df_test_labeled.cache()
        print('Size of the labeled test df: {}'.format(df_test_labeled.count()))
        df_test_labeled.groupBy('label_price').agg(count('*')).show()
    else:
        from churn_nrt.src.data_utils.ids_utils import get_filtered_ids
        df_test_nifs = get_filtered_ids(spark, tr_date_, filter_recent=True, filter_disc=True, filter_ord=True, filter_cc =True, n_cycles=12, verbose=False)
        df_test_pred = df_test_nifs.join(df_test_ids, ['nif_cliente'],'inner')

    ######################## Feature selection ########################
    from churn_nrt.src.data_utils.ids_utils import get_no_input_feats, get_noninf_features
    non_inf_features = get_noninf_features() + get_no_input_feats()


    from churn_nrt.src.data_utils.ids_utils import get_ids_nif_feats, get_ids_msisdn_feats, get_ids_nc_feats, get_ids
    nif_feats = get_ids_nif_feats(df_train_ids.dtypes)
    df_ = get_ids(spark, tr_date_)
    msisdn_feats_ = get_ids_msisdn_feats(df_.dtypes, True)
    msisdn_feats = [f + '_agg_mean' for f in msisdn_feats_]
    nc_feats = get_ids_nc_feats(df_train_labeled.dtypes, numeric=True)

    feats = nif_feats + msisdn_feats + nc_feats

    numeric_feats = [f[0] for f in df_train_labeled.select(feats).dtypes if f[1] in ["double", "int", "float", "long", "bigint"]]


    gnv_roam_columns = [c for c in numeric_feats if ('GNV_Roam' in c)]
    address_columns = [c for c in numeric_feats if ('address_' in c)]
    numeric_columns = list(set(numeric_feats) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))

    #feats = [f for f in numeric_columns if f not in CORRELATED_FEATS]
    feats = list(set(numeric_columns))
    print "[Info] Metadata has been read"

    ######################## Modeling ########################
    from churn_nrt.src.projects_utils.models.modeler import get_metrics, get_feats_imp, get_score
    # 3.3. Building the stages of the pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline

    stages = []

    assembler_inputs = feats
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    print("[Info] Starting fit....")
    from churn_nrt.src.projects_utils.models.modeler import get_model

    mymodel = get_model(model_, "label_price")
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(df_train_labeled.fillna(0.0))
    ######################## Feature Importance ########################
    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    ######################## Train Evaluation ########################
    df_tr_preds = get_score(pipeline_model, df_train_labeled.fillna(0), calib_model=None, score_col="model_score")
    auc_train, _, _ = get_metrics(spark, df_tr_preds.fillna(0.0), title="TRAIN", do_churn_rate_fix_step=True, score_col="model_score", label_col="label_price")
    print('AUC for TRAINING set: {}'.format(auc_train))
    path_to_save = '/data/attributes/vf_es/price_sensitivity_train_scores/year={}/month={}/day={}'.format(tr_date_[:4],tr_date_[4:6], tr_date_[6:8])
    df_tr_preds.select('nif_cliente', 'model_score', 'label_price').write.mode("append").format("parquet").save(path_to_save)
    print('Saved df in: ')
    print(path_to_save)

    if mode_ == 'evaluation':
    ######################## Test Evaluation ########################
        df_tt_preds = get_score(pipeline_model, df_test_labeled.fillna(0), calib_model=None, score_col="model_score")
        auc_test, _, _ = get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST", do_churn_rate_fix_step=True, score_col="model_score", label_col="label_price")
        print('AUC for TEST set: {}'.format(auc_test))

    else:
    ######################## Test Predictions ########################
        df_tt_preds = get_score(pipeline_model, df_test_pred.fillna(0), calib_model=None, score_col="model_score")
        print'Generated predictions for test set.'

        partition_date = "year={}/month={}/day={}".format(str(int(tt_date_[0:4])), str(int(tt_date_[4:6])),
                                                      str(int(tt_date_[6:8])))

        model_output_cols = ["model_name", \
                         "executed_at", \
                         "model_executed_at", \
                         "predict_closing_date", \
                         "msisdn", \
                         "client_id", \
                         "nif", \
                         "model_output", \
                         "scoring", \
                         "prediction", \
                         "extra_info", \
                         "year", \
                         "month", \
                         "day", \
                         "time"]

        partition_date = get_next_day_of_the_week('friday')

        partition_year = int(partition_date[0:4])
        partition_month = int(partition_date[4:6])
        partition_day = int(partition_date[6:8])

        import datetime as dt

        df_mo = df_tt_preds.select('nif_cliente', 'model_score')

        executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        from pyspark.sql.functions import split, from_unixtime, unix_timestamp

        model_name = "price_sensitivity"
        df_model_scores = df_mo \
            .withColumn("model_name", lit(model_name).cast("string")) \
            .withColumn("executed_at", from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string")) \
            .withColumn("model_executed_at", col("executed_at").cast("string")) \
            .withColumn("client_id", lit("")) \
            .withColumn("msisdn", lit("")) \
            .withColumnRenamed("nif_cliente", "nif") \
            .withColumn("scoring", col("model_score").cast("float")) \
            .withColumn("model_output", lit("").cast("string")) \
            .withColumn("prediction", lit("").cast("string")) \
            .withColumn("extra_info", lit("")) \
            .withColumn("predict_closing_date", lit(tt_date_)) \
            .withColumn("year", lit(partition_year).cast("integer")) \
            .withColumn("month", lit(partition_month).cast("integer")) \
            .withColumn("day", lit(partition_day).cast("integer")) \
            .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer")) \
            .select(*model_output_cols)

        import pandas as pd

        df_pandas = pd.DataFrame({
            "model_name": [model_name],
            "executed_at": [executed_at],
            "model_level": ["nif"],
            "training_closing_date": [tr_date_],
            "target": [""],
            "model_path": [""],
            "metrics_path": [""],
            "metrics_train": [""],
            "metrics_test": [""],
            "varimp": ["-"],
            "algorithm": [model_],
            "author_login": ["asaezco"],
            "extra_info": [""],
            "scores_extra_info_headers": ["None"],
            "year": [partition_year],
            "month": [partition_month],
            "day": [partition_day],
            "time": [int(executed_at.split("_")[1])]})

        df_model_parameters = spark \
            .createDataFrame(df_pandas) \
            .withColumn("day", col("day").cast("integer")) \
            .withColumn("month", col("month").cast("integer")) \
            .withColumn("year", col("year").cast("integer")) \
            .withColumn("time", col("time").cast("integer"))

        df_model_scores.coalesce(400) \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save("/data/attributes/vf_es/model_outputs/model_scores/")

        df_model_parameters \
            .coalesce(1) \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save("/data/attributes/vf_es/model_outputs/model_parameters/")

        print('########## Inserted to MO ##########')

    print('########## Finished process ##########')







