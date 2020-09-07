# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, isnull,
                                   substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.functions import *
from utils_trigger import get_trigger_minicar3, get_billing_car, getIds, get_tickets_car, get_filtered_car, get_next_dow
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
import matplotlib

matplotlib.use('Agg')


def convert_to_date(dd_str):
    import datetime as dt
    if dd_str in [None, ""] or dd_str != dd_str: return None

    dd_obj = dt.datetime.strptime(dd_str.replace("-", "").replace("/", ""), "%Y%m%d")
    if dd_obj < dt.datetime.strptime("19000101", "%Y%m%d"):
        return None

    return dd_obj.strftime("%Y-%m-%d %H:%M:%S") if dd_str and dd_str == dd_str else dd_str


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

        # from churn.utils.constants import CHURN_DELIVERIES_DIR
        # root_dir = CHURN_DELIVERIES_DIR
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


if __name__ == "__main__":

    no_inputs = ['msisdn', 'seg_pospaid_nif', 'nif_cliente']

    import argparse

    parser = argparse.ArgumentParser(description='List of Configurable Parameters')
    parser.add_argument('-d', '--closing_d', metavar='<closing_d>', type=str, help='closing day', required=True)
    parser.add_argument('-s', '--starting_d', metavar='<starting_d>', type=str, help='starting day', required=True)
    args = parser.parse_args()

    set_paths_and_logger()

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *
    import pykhaos.utils.pyspark_configuration as pyspark_config

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="ticket_triggers", log_level="OFF",
                                                              min_n_executors=1, max_n_executors=15, n_cores=4,
                                                              executor_memory="32g", driver_memory="32g")
    print("############ Process Started ##############")

    starting_day = args.starting_d
    closing_day = args.closing_d

    # Imports

    from churn.datapreparation.general.customer_base_utils import get_customer_base_segment

    from churn.analysis.triggers.orders.run_segment_orders import get_ccc_attrs_w8

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    # Training set

    path = '/data/udf/vf_es/churn/triggers/car_labeled_filtered/year={}/month={}/day={}'.format(int(starting_day[:4]),
                                                                                       int(starting_day[4:6]),
                                                                                       int(starting_day[6:8]))

    if pathExist(path):
        print'Labeled starting_day car already exists. Reading it: '
        df_car_tr_labeled = spark.read.load(path)
        print'Loaded labeled starting_day car from: ' + path
    else:
        df_car = get_trigger_minicar3(spark, starting_day)

        #df_base_msisdn = get_customer_base_segment(spark, date_=starting_day)

        #df_ccc = get_ccc_attrs_w8(spark, starting_day, df_base_msisdn)

        #df_tar = df_ccc.filter(col('CHURN_CANCELLATIONS_w8') == 0)

        #df_car = df_car_.join(df_tar.select('CHURN_CANCELLATIONS_w8', 'NIF_CLIENTE'), ['NIF_CLIENTE'], 'inner')

        df_customers = get_churn_target_nif(spark, starting_day)

        df_customers = df_customers.select('NIF_CLIENTE', 'label').distinct()

        df_car_tr_labeled = df_car.join(df_customers, ['NIF_CLIENTE'], 'inner')

        df_car_tr_labeled.repartition(200).write.save(path, format='parquet', mode='append')

    df_car_tr_labeled.groupBy('label').agg(count('*').alias('nb')).show()

    print "[Info trigger_ml] Label count on (unbalanced) tr set showed above"

    # Test set

    path = '/data/udf/vf_es/churn/triggers/car_labeled_filtered/year={}/month={}/day={}'.format(int(closing_day[:4]),
                                                                                       int(closing_day[4:6]),
                                                                                       int(closing_day[6:8]))
    if pathExist(path):
        print'Labeled starting_day car already exists. Reading it: '
        df_car_tt_labeled = spark.read.load(path)
        print'Loaded labeled starting_day car from: ' + path
    else:
        df_car_tt = get_trigger_minicar3(spark, closing_day)

        #df_base_msisdn_tt = get_customer_base_segment(spark, date_=closing_day)

        #df_ccc_tt = get_ccc_attrs_w8(spark, closing_day, df_base_msisdn_tt)

        #df_tar_tt = df_ccc_tt.filter(col('CHURN_CANCELLATIONS_w8') == 0)

        #df_car_tt = df_car_tt_.join(df_tar_tt.select('CHURN_CANCELLATIONS_w8', 'NIF_CLIENTE'), ['NIF_CLIENTE'], 'inner')

        df_customers_tt = get_churn_target_nif(spark, closing_day)

        df_customers_tt = df_customers_tt.select('NIF_CLIENTE', 'label').distinct()

        base_churn_rate_tt = 100.0 * df_customers_tt.where(col('label') > 0).count() / df_customers_tt.count()

        df_car_tt_labeled = df_car_tt.join(df_customers_tt, ['NIF_CLIENTE'], 'inner')

    df_car_tt_labeled.groupBy('label').agg(count('*').alias('nb')).show()

    # hdfs_path = '/data/attributes/vf_es/trigger_analysis/customer_master/year={}/month={}/day={}'.format(int(starting_day[:4]),int(starting_day[4:6]),int(starting_day[6:8]))

    # Modeling

    feats = list(set(df_car_tr_labeled.columns) - set(
        ['NIF_CLIENTE', 'nif_cliente', 'CHURN_CANCELLATIONS_w8', 'diff_rgus_n_n2', 'tgs_target_accionamiento',
         'tgs_has_discount', 'tgs_days_until_fecha_fin_dto', 'segment_nif', 'seg_pospaid_nif', 'nb_rgus_cycles_2',
         'MSISDN', 'msisdn', 'NUM_CLIENTE', 'num_cliente', 'label']))

    from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
    from pyspark.mllib.evaluation import BinaryClassificationMetrics

    assembler = VectorAssembler(inputCols=feats, outputCol="features")

    classifier = RandomForestClassifier(featuresCol="features", \
                                        labelCol="label", \
                                        maxDepth=10, \
                                        maxBins=32, \
                                        minInstancesPerNode=150, \
                                        impurity="entropy", \
                                        featureSubsetStrategy="sqrt", \
                                        subsamplingRate=0.85, minInfoGain=0.001, \
                                        numTrees=800, \
                                        seed=1234)

    pipeline = Pipeline(stages=[assembler, classifier])

    [unbaltrdf, valdf] = df_car_tr_labeled.randomSplit([0.8, 0.2], seed=1234)

    from churn.models.fbb_churn_amdocs.utils_general import *
    from churn.models.fbb_churn_amdocs.utils_model import *

    path = '/data/udf/vf_es/churn/triggers/car_tr_filtered/year={}/month={}/day={}'.format(int(starting_day[:4]),
                                                                                  int(starting_day[4:6]),
                                                                                  int(starting_day[6:8]))
    path_val = '/data/udf/vf_es/churn/triggers/valdf_filtered/year={}/month={}/day={}'.format(int(starting_day[:4]),
                                                                                     int(starting_day[4:6]),
                                                                                     int(starting_day[6:8]))

    if pathExist(path) & pathExist(path_val):
        print'Training and validation dfs already exist. Reading them: '
        trdf = spark.read.load(path)
        print'Loaded tr df from: ' + path
        valdf = spark.read.load(path_val)
        print'Loaded val df from: ' + path_val
    else:
        trdf = balance_df2(unbaltrdf, 'label')
        trdf.repartition(200).write.save(path, format='parquet', mode='append')
        valdf.repartition(200).write.save(path_val, format='parquet', mode='append')

    trdf = trdf.repartition(200)

    trdf.groupBy('label').agg(count('*').alias('nb')).show()

    print "[Info trigger_ml] Label count on (balanced) tr set showed above"

    model = pipeline.fit(trdf)

    calibmodel = get_calibration_function2(spark, model, valdf, 'label', 10)

    feat_importance_ = getOrderedRelevantFeats(model, feats, 'f', 'rf')

    feat_importance = [(el[0], float(el[1])) for el in feat_importance_]

    df_imp = spark.createDataFrame(feat_importance, ['feature', 'feat_importance'])
    path = '/data/udf/vf_es/churn/triggers/feat_imp/year={}/month={}/day={}'.format(int(starting_day[:4]),
                                                                                    int(starting_day[4:6]),
                                                                                    int(starting_day[6:8]))

    df_imp.repartition(200).write.save(path, format='parquet', mode='overwrite')
    print'Feat Imp saved as ' + path

    for fimp in feat_importance:
        print "[Info Main FbbChurn] Imp feat " + str(fimp[0]) + ": " + str(fimp[1])

    getScore = udf(lambda prob: float(prob[1]), DoubleType())
    predicted_tt = model.transform(df_car_tt_labeled)
    predicted_tt = predicted_tt.withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    predicted_tt_calib = calibmodel[0].transform(predicted_tt)

    predicted_tr = model.transform(trdf)
    predicted_tr = predicted_tr.withColumn("model_score", getScore(col("probability")).cast(DoubleType()))
    predicted_tr_calib = calibmodel[0].transform(predicted_tr)

    trPredictionAndLabels = predicted_tr_calib.select(['calib_model_score', 'label']).rdd.map(
        lambda r: (r['calib_model_score'], r['label']))
    trmetrics = BinaryClassificationMetrics(trPredictionAndLabels)
    print(" Area under ROC(tr) = " + str(trmetrics.areaUnderROC))

    ordered_tt = predicted_tt_calib.select('NIF_CLIENTE', 'calib_model_score').orderBy(desc('calib_model_score'))

    selected = ordered_tt.head(50000)
    schema = ordered_tt.schema
    selected_df = spark.createDataFrame(selected, schema=schema)
    n_save = 50000
    path = '/data/udf/vf_es/churn/triggers/filtered_model2_50k_filtered/year={}/month={}/day={}'.format(int(closing_day[:4]),
                                                                                      int(closing_day[4:6]),
                                                                                      int(closing_day[6:8]))

    selected_df.limit(n_save).select('NIF_CLIENTE', 'calib_model_score').repartition(200).write.save(path,
                                                                                                     format='parquet',
                                                                                                     mode='append')

    print'Written top 50K risk NIFs in '
    print(path)

    predicted_tr_full = model.transform(df_car_tr_labeled).withColumn("model_score",
                                                                      getScore(col("probability")).cast(DoubleType()))
    predicted_tr_full_calib = calibmodel[0].transform(predicted_tr_full)

    print'Training lifts:'

    ordered_tt = predicted_tr_full_calib.select('NIF_CLIENTE', 'calib_model_score', 'label').orderBy(
        desc('model_score'))
    top_cust = [50000, 40000, 30000, 20000, 16000, 12000, 10000, 8000, 5000, 3000, 2000]
    vec_vol_ = []
    vec_rate_ = []
    vec_lift_ = []
    schema = ordered_tt.schema
    for i in top_cust:
        print(i)
        selected = ordered_tt.head(i)
        selected_df = spark.createDataFrame(selected, schema=schema)
        total_churners_trigger = selected_df.where(col('label') > 0).count()
        print'Churn Rate for top {}K customers: {}'.format(i, 100.0 * total_churners_trigger / i)
        churn_rate_ = 100.0 * total_churners_trigger / i
        vec_vol_.append(total_churners_trigger)
        vec_rate_.append(churn_rate_)
        vec_lift_.append(churn_rate_ / base_churn_rate_tt)
        print'Lift: {}'.format(churn_rate_ / base_churn_rate_tt)
        print'Total number of churners: {}'.format(total_churners_trigger)

print("############ Finished Process ############")


