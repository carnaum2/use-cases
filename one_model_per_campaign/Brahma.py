import sys

from dataPreparation import (NIFContactHist, NIFResponseHist)
from pyspark.sql.functions import (udf, col, when, lit, collect_list)
from pyspark.sql.types import DoubleType, StringType, IntegerType, ArrayType, FloatType
from time import strftime, gmtime
import datetime



# Numpy stuff
import numpy as np


# Config logging
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(message)s'
                    )


# Import packages
sys.path.append('/var/SP/data/home/adesant3/')



sys.path.append("/var/SP/data/home/adesant3/src/use-cases/utils/src/main/python/general_model_trainer/general_model_trainer.py")
from general_model_trainer import GeneralModelTrainer




class Brahma(object):

    def __init__(self, spark, campaign_codes_list, type_campaign, time_aggregation):

        self.type_campaign = type_campaign
        self.yearmonth_training = time_aggregation.get_training_yearmonth()

        logging.info('++++++++++++++++++++++++++++++++++++++++++++++++++')
        logging.info(
            '[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Loading the data from campaign history')

        # Read Campaign Contact and Response Hist
        nif_contact_hist  = NIFContactHist(spark)
        nif_response_hist = NIFResponseHist(spark)

        print(nif_contact_hist.get_dataframe().head())
        print(nif_response_hist.get_dataframe().head())

        # Loading Contact Response NIF Level Data
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Loading Contact Resp NIF Level data')
        self.NIF_Response_hist = (nif_response_hist.get_dataframe()
                                  .where(col('CampaignCode').isin(campaign_codes_list))
                                  .where(col('YearMonth').isin(self.yearmonth_training))
                                  )

        # Loading Contact Historical NIF Level Data
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Loading Contact Hist NIF Level data')
        self.NIF_Contact_hist = (nif_contact_hist.get_dataframe()
                                 #.where(col('CampaignCode') == str(firstResponseCampaignCode))
                                 .where(col('CampaignCode').isin(campaign_codes_list))
                                 .where(col('YearMonth').isin(self.yearmonth_training))
                                 )

        # Load IDS table
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Loading IDS')
        self.customerAttributes = (spark.read.table('tests_es.amdocs_ids_srv'))




    def calculate_campaign_hist_per_nif(self):

        # Joining both
        logging.info(
            '[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Joining both contact hist and response')
        nif_with_labels = \
            (self.NIF_Contact_hist
             .join(self.NIF_Response_hist
                   .drop(col('year'))
                   .drop(col('month'))
                   .drop(col('Grupo'))
                   .drop(col('canal'))
                   .drop(col('day'))
                   .drop(col('CampaignType'))
                   .drop(col('creatividad'))
                   .drop_duplicates()
                   ,
                   ['cif_nif', 'CampaignCode', 'treatmentcode'],
                   how='left_outer'
                   )
             .withColumn('EsRespondedor',
                         when(col('responsedatetime').isNotNull(), 1)
                         .otherwise(0))
             .withColumn('Label',
                         when((col('Grupo') == 'Control') & (col('EsRespondedor') == 0), 'Control_Negative')
                         .when((col('Grupo') == 'Control') & (col('EsRespondedor') == 1), 'Control_Positive')
                         .when((col('Grupo') == 'Target') & (col('EsRespondedor') == 0), 'Target_Negative')
                         .when((col('Grupo') == 'Target') & (col('EsRespondedor') == 1), 'Target_Positive')
                         .otherwise('Ignore')
                         )
             .select(col('year'), col('month'), col('day'), col('cif_nif'), col('CampaignCode'), col('CampaignType'),
                     col('Grupo'),
                     col('creatividad'), col('canal'), col('contactdatetime'), col('Label')
                     )
             .repartition(200)
             # There is no need to order
             #.orderBy(col('year'), col('month'), col('day'), col('cif_nif'))
             )

        # Define UDF
        reduceLabelSet = udf(UDFs._create_label_set, StringType())

        self.labeled_datamart_nif_level = \
            (nif_with_labels
             .groupBy(col('year'), col('month'), col('day'), col('cif_nif'))
             # Collect all possible responses in label_set
             # Sometimes, under a same CampaignCode, there can be some contradictory responses
             .agg(collect_list('Label').alias('label_set'))
             .withColumn('CampaignCode', lit(str(self.type_campaign)))
             .withColumn('Label', reduceLabelSet(col('label_set')))
             .drop(col('label_set'))
             )

    def inject_ids_attributes_at_nif_level(self, prefix_filter_attributes = ('ccc', 'GNV')):

        # Take only the columns of interest
        cols_CustomerAttributes = ([column for column in self.customerAttributes.columns
                                    if column.startswith(prefix_filter_attributes)])
        collectList_cols_CustomerAttributes = [collect_list(col(column)) for column in cols_CustomerAttributes]

        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Collecting data at customer level')
        customerAggregatedAttributes = \
            (self.customerAttributes
             .select(['NIF_CLIENTE'] +
                     cols_CustomerAttributes
                     )
             .groupBy(col('NIF_CLIENTE'))
             .agg(*collectList_cols_CustomerAttributes)
             .join(self.labeled_datamart_nif_level,
                   self.labeled_datamart_nif_level.cif_nif == self.customerAttributes.NIF_CLIENTE,
                   how='inner'
                   )
             .repartition(200)
             )

        cols_CustomerAggregatedAttributes = [column for column in customerAggregatedAttributes.columns if
                                             any(x in column for x in list(prefix_filter_attributes))]



        # UDF
        clean_attributes_UDF = udf(UDFs._clean_attributes, (FloatType()))

        # Best practices:
        # https://medium.com/@mrpowers/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Aggregating data at customer level')
        customerAggregated = \
            (customerAggregatedAttributes
             .withColumn('redeem',
                         when((col('Label') == 'Target_Negative') | (col('Label') == 'Control_Negative'), lit(0))
                         .when((col('Label') == 'Target_Positive') | (col('Label') == 'Control_Positive'), lit(1))
                         .otherwise('Ignore')
                         )
             .drop(col('Label'))
             # Filter those cases where ignore is not considered (only interested in 0 or 1 classes)
             .where(col('redeem') != 'Ignore')
             .select(col('NIF_CLIENTE'),
                     col('redeem'),
                     *[clean_attributes_UDF(column).name(column.split('(')[1][:-1])
                       for column in cols_CustomerAggregatedAttributes])
             )

        return customerAggregated

    def train(self, train_data):

        # Training the model
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                     '] Training the model for '+self.type_campaign+' campaigns.')

        self.model = (GeneralModelTrainer(train_data, 'redeem')  # .sample(False, 0.1)
                      .generateFeaturesVector()
                      .train()
                      )

        # Name of the model in HDFS
        self.model_name = 'model_' + self.type_campaign + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def calculate_model_metrics(self, sc):
        # Saving the model to HDFS
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                     '] Calculating metrics for the '+self.type_campaign+' campaigns model.')
        self.model.predict(self.model.testData)

        dict_metrics = self.model.evaluate_with_sklearn_metrics()
        dict_metrics['name_model'] = self.model_name

        # Store the model metrics information in HDFS
        (sc.parallelize([dict_metrics])
         .repartition(1)
         .saveAsTextFile('hdfs:///user/adesant3/onemodelpercampaign/metrics/' + self.model_name + '.txt'))

        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                     '] Type hadoop dfs -cat onemodelpercampaign/metrics/' + self.model_name + '.txt/* from 387/303 to see a metrics summary')

    def save(self):
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                     '] Writing in HDFS the model for ' + self.type_campaign + ' campaigns.')
        (self.model
         .get_model()
         .write()
         .overwrite()
         .save('hdfs:///user/adesant3/onemodelpercampaign/models/'+self.model_name+'.sparkModel')
         )

    def predict(self, predict_data):
        logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                     '] Scoring customers based on the model for ' + self.type_campaign + ' campaigns.')
        self.model.predict(predict_data)

##########################
# Auxiliar functions UDFs
##########################

# @partial(udf, returnType=StringType())
class UDFs (object):
    @staticmethod
    def _create_label_set(label_set):
        # First, prioritize positive responses
        if 'Target_Positive' in label_set:
            return 'Target_Positive'
        elif 'Control_Positive' in label_set:
            return 'Control_Positive'
        # Then, negative responses
        elif 'Target_Negative' in label_set:
            return 'Target_Negative'
        elif 'Control_Negative' in label_set:
            return 'Control_Negative'
        else:
            return 'Ignore'

    @staticmethod
    def _clean_attributes(column_values):
        if len(column_values) == 0:
            return np.float(-1)
        else:
            return np.mean(column_values).tolist()

    ##########################
    # Functions UDFs
    ##########################






