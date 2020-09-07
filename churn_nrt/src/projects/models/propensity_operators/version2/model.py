def set_paths():
    import os
    import sys

    USECASES_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios", "use-cases")
    if USECASES_SRC not in sys.path:
        sys.path.append(USECASES_SRC)


if __name__ == "__main__":

    set_paths()

    from churn_nrt.src.utils.spark_session import get_spark_session
    from churn_nrt.src.projects_utils.models.modeler import get_feats_imp

    from pyspark.sql.functions import (col,udf,sort_array, when, lit, array,array_contains,log,concat_ws,
                                       from_unixtime, unix_timestamp,
                                       regexp_replace, split
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


    path = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/'

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

    if mode == 'production':

        print('[Info]: Getting prediction dataset for production...')

        pred_test = pred_test.select('msisdn', 'probability', 'prediction').cache()

        getScore1 = udf(lambda prob: float(prob[1]), DoubleType())
        getScore2 = udf(lambda prob: float(prob[2]), DoubleType())
        getScore3 = udf(lambda prob: float(prob[3]), DoubleType())
        getScore4 = udf(lambda prob: float(prob[4]), DoubleType())

        print("[Info]: Getting column of scores for each class...")

        pred_test = pred_test.withColumn("1", getScore1(col("probability")).cast(DoubleType())) \
            .withColumn("2", getScore2(col("probability")).cast(DoubleType())) \
            .withColumn("3", getScore3(col("probability")).cast(DoubleType())) \
            .withColumn("4", getScore4(col("probability")).cast(DoubleType())).cache()




        pred_test = pred_test.withColumn('prob_array', array('1', '2', '3', '4')).drop('probability','1','2','3','4').cache()

        pred_test = pred_test.withColumn('prob_array', concat_ws(';', pred_test.prob_array))

        final_predictions = pred_test.withColumn('prediction', when(col("prediction") == 1,
                                                    'masmovil').otherwise(when(col("prediction") == 2,
                                                        'movistar').otherwise(when(col("prediction") == 3,
                                                            'orange').otherwise(when(col("prediction") == 4, 'others')))))


        print('[Info]: Since mode is production, we put the predictions dataframe in model_output format')

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
                           .withColumn("msisdn", col("msisdn").cast("string"))
                           .withColumn("nif",lit(""))
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


    else: #for evaluation we calculate multilabel and multiclass metrics (leven msisdn)

        print('[Info]: Getting prediction dataset for evaluation...')

        print("[Info]: Getting scores for each class...")

        pred_test = pred_test.select('msisdn', 'probability', 'prediction', 'label').cache()

        getScore1 = udf(lambda prob: float(prob[1]), DoubleType())
        getScore2 = udf(lambda prob: float(prob[2]), DoubleType())
        getScore3 = udf(lambda prob: float(prob[3]), DoubleType())
        getScore4 = udf(lambda prob: float(prob[4]), DoubleType())

        # Get column with probability of each class

        print("[Info]: Getting column of scores for each class...")

        pred_test = pred_test.withColumn("1", getScore1(col("probability")).cast(DoubleType())) \
            .withColumn("2", getScore2(col("probability")).cast(DoubleType())) \
            .withColumn("3", getScore3(col("probability")).cast(DoubleType())) \
            .withColumn("4", getScore4(col("probability")).cast(DoubleType())).cache()

        pred_test = pred_test.withColumn('prob_array', array('1', '2', '3', '4')).cache()

        print("[Info]: Sort array and get two max scores...")

        pred_test2 = pred_test.withColumn('prob_sorted', sort_array(pred_test['prob_array'])).cache()

        # Get column with first and second position of the array ordered (max1, max2)

        pred_test2 = pred_test2.withColumn("max1", pred_test2["prob_sorted"].getItem(3)).withColumn("max2", pred_test2[
            "prob_sorted"].getItem(2)).cache()

        print("[Info]: Get operators that match with max1 and max2 scores...")

        print(pred_test.columns[4:8])

        cond1 = "when" + ".when".join(
            ["(col('" + c + "') == col('max1'), lit('" + c + "'))" for c in
             pred_test2.columns[4:8]])

        print("[Info]: Condition 1...")

        print(cond1)

        predicciones_final = pred_test2.withColumn("predicted_label1", eval(cond1)).cache()

        cond2 = "when" + ".when".join(
            ["(col('" + c + "') == col('max2'), lit('" + c + "'))" for c in
             predicciones_final.columns[4:8]])

        print("[Info]: Condition 2...")

        print(cond2)

        predicciones_final = predicciones_final.withColumn("predicted_label2", eval(cond2)).cache()

        predicciones_final = predicciones_final.withColumn('prediction_multilabel',
                                                           array('predicted_label1', 'predicted_label2')).cache()


        print("[Info]: Get certainity...")

        # Get column with first and second position of the array ordered (max1, max2)

        predicciones_final = predicciones_final.withColumn("certainity_1",
                                 (predicciones_final["prob_sorted"].getItem(3) - predicciones_final["prob_sorted"].getItem(2))).cache()


        predicciones_final = predicciones_final.withColumn("certainity_2", -(
                    predicciones_final["prob_array"].getItem(0) * log(predicciones_final["prob_array"].getItem(0))
                    + predicciones_final["prob_array"].getItem(1) * log(predicciones_final["prob_array"].getItem(1))
                    + predicciones_final["prob_array"].getItem(2) * log(predicciones_final["prob_array"].getItem(2))
                    + predicciones_final["prob_array"].getItem(3) * log(predicciones_final["prob_array"].getItem(3))))


        print("[Info]: Get final prediction columns...")

        pred_final = predicciones_final.select('msisdn', 'prob_array', 'prediction', 'label','prediction_multilabel','certainity_1','certainity_2').cache()

        pred_final = pred_final.withColumn('label_multilabel', F.array('label'))\
            .withColumn('label_multilabel', col('label_multilabel').cast("array<double>"))\
            .withColumn('prediction_multilabel', col('prediction_multilabel').cast("array<double>"))


        print("[Info]: Get multilabel acierto...")

        pred_final = pred_final.withColumn('pred_contain_1',
                                           when(array_contains(col('prediction_multilabel'), 1),
                                                1).otherwise(0)).withColumn('pred_contain_2',
                                                    when(array_contains( col('prediction_multilabel'), 2),
                                                         1).otherwise(0)).withColumn(
                                                            'pred_contain_3',when(array_contains(col('prediction_multilabel'), 3),
                                                                    1).otherwise(0)).withColumn('pred_contain_4',
                                                                        when(array_contains(col('prediction_multilabel'), 4),1).otherwise(0))

        pred_final = pred_final.withColumn('acierto_multilabel',
                                           when((((col('label') == 1) & (col(
                                               'pred_contain_1') == 1))
                                                 | ((col('label') == 2) & (
                                                           col('pred_contain_2') == 1))
                                                 | ((col('label') == 3) & (
                                                           col('pred_contain_3') == 1))
                                                 | ((col('label') == 4) & (
                                                           col('pred_contain_4') == 1))), 1).otherwise(0))

        pred_final = pred_final.select('msisdn', 'prob_array', 'prediction', 'label', 'prediction_multilabel',
                                       'label_multilabel', 'certainity_1','certainity_2','acierto_multilabel')


        print("[Info]: Saving multilabel predictions...")

        pred_final.repartition(300).write.save(
            '/data/udf/vf_es/churn/churn_competitor_model/v2/multiclase/pred_'+test_date+'_evaluation',
            format="parquet",
            mode="overwrite",
        )


        print("[Info]: Predictions saved!..")

