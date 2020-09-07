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



import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


def set_paths():
    import os, re, sys
    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset_copia/')

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


if __name__ == "__main__":

    set_paths()

    from churn_nrt.src.projects.models.revamp.model_classes import RevampMobile
    from churn_nrt.src.data_utils.Metadata import Metadata
    from churn_nrt.src.projects.models.revamp.metadata import get_metadata

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("revamp_mobile")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(description="Run price_sensitivity model --tr YYYYMMDD --tt YYYYMMDD [--model rf]", epilog='Please report bugs and issues to Alvaro <alvaro.saez@vodafone.com>')
    parser.add_argument('--tr_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in training')
    parser.add_argument('--tt_date', metavar='<YYYYMMDD>', type=str, required=True, help='Date to be used in test')
    parser.add_argument('--model', metavar='rf,xgboost', type=str, required=False, default="rf", help='algorithm to be used for training de model')
    parser.add_argument('--mode', metavar='<evaluation,production>', type=str, required=False, help='tbc')
    parser.add_argument('--horizon', metavar='<4,8>', type=str, required=False, default=8, help='tbc')
    parser.add_argument('--verbose', metavar=0, type=int, required=False, help='tbc')
    parser.add_argument('--segment', metavar='<none,hard, soft>', type=str, required=True, help='selected bound segment')
    parser.add_argument('--operator', metavar='<operator or None>', type=str, required=False,  default="none", help='selected operator: orange, masmovil, movistar, others')
    parser.add_argument('--insert_day', metavar='<insert_day or None>', type=str, required=False, default="5", help='Day of the week where results will be stored')

    args = parser.parse_args()
    print(args)

    tr_date_ = args.tr_date
    tt_date_ = args.tt_date
    mode_ = args.mode
    model_ = args.model
    horizon = int(args.horizon)
    verbose = False
    segment = args.segment
    operator = None if args.operator == "none" else args.operator
    insert_day = int(args.insert_day)

    print("INPUT ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))
    print("ARGS tr_date='{}' tt_date='{}' model='{}'".format(tr_date_, tt_date_, model_))


    metadata_obj = Metadata(spark, get_metadata, ["msisdn"], ["ids"])

    model_revamp = RevampMobile(spark, tr_date_, mode_, model_, 'asaezco',metadata_obj, segment, horizon, operator)

    df_tr_preds, dict_tt_preds, df_val_preds = model_revamp.run(tt_date=[tt_date_], do_calibrate_scores=2, filter_correlated_feats=True)
    df_tt_preds = dict_tt_preds[tt_date_]

    path_to_save = '/data/attributes/vf_es/revamp/validation_mob/segment={}/year={}/month={}/day={}'.format(segment,int(tr_date_[:4]),int(tr_date_[4:6]),int(tr_date_[6:8]))
    if operator:
        path_to_save = '/data/attributes/vf_es/revamp/validation_mob/operator={}/segment={}/year={}/month={}/day={}'.format(operator,segment,int(tr_date_[:4]),int(tr_date_[4:6]),int(tr_date_[6:8]))

    df_val_preds.select('msisdn', 'label', 'model_score').write.mode("overwrite").format("parquet").save(path_to_save)

    if mode_ == 'evaluation':
        from churn_nrt.src.projects_utils.models.modeler import get_metrics

        auc_test, _, _ = get_metrics(spark, df_tt_preds.fillna(0.0), title="TEST", do_churn_rate_fix_step=True, score_col="scoring", label_col="label")
        print('AUC for TEST set: {}'.format(auc_test))

    if mode_ == 'production':
        model_output_n = "revamp_mobile_" + segment
        if operator:
            model_output_n = model_output_n + "_" + operator
        model_revamp.insert_model_outputs(df_tt_preds.drop('nif_cliente', 'num_cliente'), model_output_n, test_date=tt_date_, insert_top_k=None, extra_info_cols=[], day_to_insert=insert_day)  # insert the complete df







