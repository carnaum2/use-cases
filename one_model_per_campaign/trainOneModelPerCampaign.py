import datetime

# Calculate the current month and year so that campaigns from two months can be read
currentMonth = datetime.datetime.now().month
currentYear = datetime.datetime.now().year

# This literal_eval is needed since
# we have to read from a textfile
# which is formatted as python objects.
# It is totally safe.
from ast import literal_eval

# Standard Library stuff:
from functools import partial

# Numpy stuff
import numpy as np

from pyspark.sql.types import DoubleType, StringType, IntegerType, ArrayType, FloatType

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(message)s'
                    )
import sys

from time import strftime, gmtime

if __name__ == '__main__':

    sys.path.append('/var/SP/data/home/adesant3/')
    from common.src.main.python.utils.hdfs_generic import *

    # Import my own libraries
    from utils.adesant3.delivery import deliverTable
    from utils.adesant3.config import setting_bdp

    setting_bdp(max_n_executors=25,
                min_n_executors=25,
                n_cores=4,
                executor_memory="44g",
                app_name="Train One Model Per Campaign")
    sc, spark, sqlContext = run_sc()

    # spark.sparkContext.addPyFile("utils_adesant3.zip")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/utils/adesant3/dataPreparation.py")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/utils/src/main/python/general_model_trainer/general_model_trainer.py")

    from general_model_trainer import GeneralModelTrainer
    from pyspark.sql.functions import col, when, lit


    # Read IDS table
    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Leyendo IDS')
    customerAttributes = (spark.read.table('tests_es.ads_customerAggregatedAttributesLabeled'))

    logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Creating label column')
    customerAttributesWithLabel = \
        (customerAttributes
         .withColumn('redeem',
                     when((col('Label') == 'Target_Negative') | (col('Label') == 'Control_Negative'), lit(0))
                     .when((col('Label') == 'Target_Positive') | (col('Label') == 'Control_Positive'), lit(1))
                     .otherwise('Ignore')
                     )
         .drop(col('Label'))
         # Filter those cases where ignore is not considered (only interested in 0 or 1 classes)
         .where(col('redeem') != 'Ignore')
         )

    logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Training the model')
    OneModelForCampaign = (GeneralModelTrainer(customerAttributesWithLabel, 'redeem') # .sample(False, 0.1)
                           .generateFeaturesVector()
                           .train()
                           #.predict()
                           )

    OneModelForCampaign.predict(OneModelForCampaign.testData)

    print('writing predictions........')

    name_model = 'model_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dict_metrics = OneModelForCampaign.evaluate_with_sklearn_metrics(name_model)

    (sc.parallelize([dict_metrics])
     .repartition(1)
     .saveAsTextFile('hdfs:///user/adesant3/onemodelforcampaign/' + name_model + '/metrics' + name_model + '.txt'))

    print('Metrics stored in HDFS')
    print('Type hadoop dfs -cat onemodelforcampaign/' + name_model + '/metrics' + name_model + '.txt/* from 387/303 to see a metrics summary')

    # (OneModelForCampaign
    #  .get_predictions()
    #  .select(col('redeem'),
    #          # col('rawPrediction'),
    #          # col('probability'),
    #          col('prediction'),
    #          col('score')
    #          )
    #  .write
    #  .csv('20180410_predictions.csv',
    #       header=True,
    #       mode="overwrite")
    #  )

    print('writing model........')
    # (OneModelForCampaign
    #  .get_model()
    #  .write()
    #  .overwrite()
    # # Automatically calculate the name of the sparkModel
    # # Store the attributes on a file
    #  .save('hdfs:///user/adesant3/onemodelforcampaign/modelo_2.sparkModel'))

    # from sklearn import metrics
    #
    # assessment_data = (spark
    #            .read
    #            .format("csv")
    #            .option("header", "true")
    #            .load('20180410_predictions.csv')
    #            # This conversion is due to the fact that prediction is a 0.0 number
    #            .select(col('redeem').cast(IntegerType()),
    #                    col('prediction').cast(IntegerType()),
    #                    col('score').cast(DoubleType()))
    #            .toPandas()
    #            )


    # print(assessment_data.head())
    # print(assessment_data.groupby('prediction').size())
    # print(type(assessment_data['prediction'][0]))
    # print(assessment_data.groupby('redeem').size())

    # sc.parallelize([dictionary_output]).saveAsTextFile('/path/')

    #print(OneModelForCampaign.get_predictions())
    #print(OneModelForCampaign.get_predictions().toPandas())
    #print(np.array(OneModelForCampaign.get_predictions()))

    logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Fin del proceso')
