
'''
def set_paths():
    import os
    import sys

    USECASES_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios", "use-cases")
    if USECASES_SRC not in sys.path:
        sys.path.append(USECASES_SRC)
'''

def set_paths():
    import os, re, sys

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

    from churn_nrt.src.utils.spark_session import get_spark_session
    from churn_nrt.src.projects_utils.models.modeler import get_feats_imp
    from churn_nrt.src.data.customer_base import CustomerBase

    from pyspark.sql.functions import (col,udf,sort_array, when, lit, array,array_contains,log,concat_ws,
                                       from_unixtime, unix_timestamp,
                                       regexp_replace, split, collect_list, sum
                                       )

    import datetime

    from churn_nrt.src.utils.date_functions import get_next_dow

    from pyspark.sql.types import DoubleType
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.sql.types import IntegerType
    from pyspark.sql import functions as F


    from churn_nrt.src.projects.models.propensity_operators.version2.get_dataset import get_ids_multiclass
    from churn_nrt.src.projects_utils.models.modeler import get_score, get_split, get_model


    sc, spark, sql_context = get_spark_session(app_name="afinidad_competidores_production")

    import sys

    train_date = sys.argv[1]  # arguments of the model (specified in terminal)
    test_date = sys.argv[2]
    mode = sys.argv[3]


    path = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0'

    print('[Info]: Get train and test set: ')

    train_final = get_ids_multiclass(train_date, path, mode, 'False',spark)
    test_final = get_ids_multiclass(test_date, path, mode, 'True',spark)

    var_train = [c for c in train_final.columns if c not in ['label', 'msisdn']]

    print('[Info]: Number of input variables train: ', len(var_train))

    var_test = [c for c in test_final.columns if c not in ['label', 'msisdn']]

    print('[Info]: Number of input variables test: ', len(var_test))

    print('[Info]: Drop variables test that are not in train: ')

    test_not_in_train = [c for c in var_test if c not in var_train]

    print('[Info]: Number of input test that are not in train: ', len(test_not_in_train))

    test_final = test_final.drop(*test_not_in_train)

    print('[Info]: Drop variables train that are not in test: ')

    var_test = [c for c in test_final.columns if c not in ['label', 'msisdn']]

    train_not_in_test = [c for c in var_train if c not in var_test]

    print('[Info]: Number of input train that are not in test: ', len(train_not_in_test))

    train_final = train_final.drop(*train_not_in_test)

    print('[Info]: Number of variables test adjusted: ', len(train_final.columns))
    print('[Info]: Number of variables train adjusted: ', len(test_final.columns))

    print('[Info]: Convert label to integer...')

    train_final = train_final.withColumn('label', train_final['label'].cast(IntegerType()))

    if mode == 'evaluation':

        test_final = test_final.withColumn('label', test_final['label'].cast(IntegerType()))


    print('[Info]: Splitting and balancing data for the model...')

    train_unbal, train, validation = get_split(train_final)

    feats = [var for var in train.columns if var not in ["msisdn", "label"]]

    print('[Info]: Assembling data for the model...')

    assembler = VectorAssembler(inputCols=feats, outputCol="features")
    stages = []
    stages += [assembler]

    print('[Info]: Fitting model...')

    dt = get_model('rf', label_col='label', featuresCol="features")
    stages += [dt]
    pipeline = Pipeline(stages=stages)

    model = pipeline.fit(train)

    feat_imp = get_feats_imp(model, feats, top=50)

    print("[Info]: Feature importance for the model...")
    print(feat_imp)

    print("[Info]: Getting predictions and scores...")

    pred_test = get_score(
        model, test_final, calib_model=None, add_randn=False, score_col="model_score"
    )

    print('[Info]: Getting scores for each operator...')

    getScore1 = udf(lambda prob: float(prob[1]), DoubleType())
    getScore2 = udf(lambda prob: float(prob[2]), DoubleType())
    getScore3 = udf(lambda prob: float(prob[3]), DoubleType())
    getScore4 = udf(lambda prob: float(prob[4]), DoubleType())

    print("[Info]: Getting column of scores for each class...")

    pred_test = pred_test.withColumn("masmovil", getScore1(col("probability")).cast(DoubleType())) \
        .withColumn("movistar", getScore2(col("probability")).cast(DoubleType())) \
        .withColumn("orange", getScore3(col("probability")).cast(DoubleType())) \
        .withColumn("others", getScore4(col("probability")).cast(DoubleType())).cache()

    print('[Info]: Getting prediction dataset for production...')

    print("[Info]: Grouping scores by NIF...")

    print("[Info]: Get NIF for each msisdn (customer base)")

    base_test = CustomerBase(spark).get_module(test_date).filter(col('rgu') == 'mobile').select('nif_cliente',
                                                                                                'msisdn').distinct()

    print("[Info]: Add nif to predictions dataset")

    preds_nif = pred_test.join(base_test, on='msisdn', how='inner')

    print('[Info]: Getting maximum scores for each operator and NIF...')

    preds_nif_grouped = preds_nif.groupby('nif_cliente').agg({'masmovil': 'max', 'movistar': 'max',
                                                              'orange': 'max',
                                                              'others': 'max'}) \
        .withColumnRenamed('max(masmovil)', 'masmovil') \
        .withColumnRenamed('max(movistar)', 'movistar') \
        .withColumnRenamed('max(orange)', 'orange') \
        .withColumnRenamed('max(others)', 'others')


    print("[Info]: Calibrating scores (between 0 and 1)")

    preds_nif_grouped = preds_nif_grouped.withColumn('masmovil', (col('masmovil') / (col('masmovil')+col('movistar')+col('orange')+col('others')))) \
                                                .withColumn('movistar', (col('movistar') /(col('masmovil')+col('movistar')+col('orange')+col('others')))) \
                                                .withColumn('orange', (col('orange') /(col('masmovil')+col('movistar')+col('orange')+col('others')))) \
                                                .withColumn('others', (col('others') /(col('masmovil')+col('movistar')+col('orange')+col('others'))))


    pred_nif_final = preds_nif_grouped.withColumn('prob_array', array('masmovil', 'movistar', 'orange', 'others'))

    print("[Info]: Get final prediction: operator with maximum score for each NIF")

    print("[Info]: Sort array of scores and get operator that match with the maximum score")

    pred_nif_final = pred_nif_final.withColumn('prob_sorted', sort_array(pred_nif_final['prob_array'])).cache()

    # Get column with first and second position of the array ordered (max1, max2)

    pred_nif_final = pred_nif_final.withColumn("max", pred_nif_final["prob_sorted"].getItem(3)).cache()

    cond = "when" + ".when".join(
        ["(col('" + c + "') == col('max'), lit('" + c + "'))" for c in
         pred_nif_final.columns[1:5]])

    pred_nif_final = pred_nif_final.withColumn("prediction", eval(cond))


    if mode == 'production':

        print('[Info]: Since mode is production, we put the predictions dataframe in model_output format')

        final_predictions = pred_nif_final.withColumn('prob_array', concat_ws(';', pred_nif_final.prob_array))

        final_predictions=final_predictions.select('nif_cliente','prob_array','prediction')


        model_output_cols = ["model_name",
                             "executed_at",
                             "model_executed_at",
                             "predict_closing_date",
                             "msisdn",
                             "client_id",
                             "nif",
                             "model_output",
                             "scoring",
                             "prediction",
                             "extra_info",
                             "year",
                             "month",
                             "day",
                             "time"]


        executed_at = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        partition_date = str(get_next_dow(5))  # get day of next wednesday
        partition_year = int(partition_date[0:4])
        partition_month = int(partition_date[5:7])
        partition_day = int(partition_date[8:10])

        df_model_scores = (final_predictions
                           .withColumn("model_name", lit("churn_competitor").cast("string"))
                           .withColumn("executed_at",
                                       from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast(
                                           "string"))
                           .withColumn("model_executed_at", col("executed_at").cast("string"))
                           .withColumn("client_id", lit(""))
                           .withColumn("msisdn",lit(""))
                           .withColumn("nif",col("nif_cliente").cast("string"))
                           .withColumn("scoring", lit(""))
                           .withColumn("model_output", col('prob_array'))
                           .withColumn("prediction", col('prediction').cast("string"))
                           .withColumn("extra_info", lit("").cast("string"))
                           .withColumn("predict_closing_date", lit(test_date))
                           .withColumn("year", lit(partition_year).cast("integer"))
                           .withColumn("month", lit(partition_month).cast("integer"))
                           .withColumn("day", lit(partition_day).cast("integer"))
                           .withColumn("time",
                                       regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                           .select(*model_output_cols))

        count = df_model_scores.count()
        print("[Info]: Count final predictions: ", count)

        df_model_scores \
            .write \
            .partitionBy('model_name', 'year', 'month', 'day') \
            .mode("append") \
            .format("parquet") \
            .save('/data/attributes/vf_es/model_outputs/model_scores/')

        print('[Info]: Final predictions dataframe saved in model output format')


    else:

        print('[Info]: Mode evaluation...:')

        print('[Info]: Getting multilabel prediction...')

        pred_nif_final=pred_nif_final.withColumn("max2", pred_nif_final[
            "prob_sorted"].getItem(2)).cache()

        print("[Info]: Get operators that match with max2 score...")

        cond2 = "when" + ".when".join(
            ["(col('" + c + "') == col('max2'), lit('" + c + "'))" for c in
             pred_nif_final.columns[1:5]])

        pred_nif_final = pred_nif_final.withColumn("prediction2", eval(cond2))

        pred_nif_final = pred_nif_final.withColumn('prediction_multilabel',
                                                           array('prediction', 'prediction2')).cache()

        print('[Info]: Since mode is evaluation, we add real label: list of labels for each NIF')


        list_label = preds_nif.groupBy('nif_cliente').agg(collect_list("label"))

        pred_label = pred_nif_final.join(list_label, on='nif_cliente', how='inner').withColumnRenamed('collect_list(label)', 'label')


        print("[Info]: Get certainity...")

        # Get column with first and second position of the array ordered (max1, max2)

        predicciones_final = pred_label.withColumn("certainity_1",
                                 (pred_label["prob_sorted"].getItem(3) - pred_label["prob_sorted"].getItem(2))).cache()


        predicciones_final = predicciones_final.withColumn("certainity_2", -(
                    predicciones_final["prob_array"].getItem(0) * log(predicciones_final["prob_array"].getItem(0))
                    + predicciones_final["prob_array"].getItem(1) * log(predicciones_final["prob_array"].getItem(1))
                    + predicciones_final["prob_array"].getItem(2) * log(predicciones_final["prob_array"].getItem(2))
                    + predicciones_final["prob_array"].getItem(3) * log(predicciones_final["prob_array"].getItem(3))))


        print("[Info]: Get final prediction columns...")

        pred_final = predicciones_final.select('nif_cliente', 'prob_array', 'prediction', 'label','prediction_multilabel','certainity_1','certainity_2').cache()


        print("[Info]: Saving multilabel predictions...")

        pred_final.repartition(300).write.save(
            '/data/udf/vf_es/churn/churn_competitor_model/v2/multiclase/pred_'+test_date+'_evaluation_NIF',
            format="parquet",
            mode="overwrite",
        )


        print("[Info]: Predictions saved!..")

