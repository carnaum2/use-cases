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
                app_name="One Model Per Campaign")
    sc, spark, sqlContext = run_sc()

    from shutil import make_archive

    make_archive(
        'utils_adesant3',
        'zip',  # the archive format - or tar, bztar, gztar
        root_dir="/var/SP/data/home/adesant3/src/use-cases/utils/adesant3",  # root for archive - current working dir if None
        base_dir=None)  # start archiving from here - cwd if None too

    # spark.sparkContext.addPyFile("utils_adesant3.zip")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/utils/adesant3/dataPreparation.py")

    from dataPreparation import (NIFContactHist, NIFResponseHist)
    from pyspark.sql.functions import (udf, col, when, lit, collect_list)

    # UDFs
    # @partial(udf, returnType=IntegerType())
    def create_label_set(label_set):
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

    reduceLabelSet = udf(create_label_set, StringType())


    # Create object per contact campaign hist and their response
    logging.info('++++++++++++++++++++++++++++++++++++++++++++++++++')
    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Loading the data from campaign history')
    nif_contact_hist = NIFContactHist(spark)
    nif_response_hist = NIFResponseHist(spark)
    # The number of months to check the most common campaign
    num_month_contact_hist = 1

    firstResponseCampaignCode = nif_response_hist.get_most_common_campaign(num_month_contact_hist)

    firstResponseCampaignCode = ['AUTOMMES_PXXXT_CH_N2_AMDOCS', 'AUTOMMES_PXXXT_ALTA_MOVIL_2LIN',
                                 'AUTOMMES_PXXXC_CH_N2', '20180710_PXXXT_BTS18_XSELL_FUT', '20180410_PXXXT_AMDOCS_RBT',
                                 'AUTOMMES_PXXXT_XSELL_MOVIL_TLK', 'AUTOMMES_PXXXT_ROAMING_Z2', '20180710_PXXXT_BTS18_XSELL_TV',
                                 '20161101_PXXXT_HBO_LANDING_AMD', 'AUTOMMES_PXXXT_PROAC_SWAP_TV']

    # Taking just an example of both dataframes
    #num_samples 0
    # logging.info('Starting to create the samples, with a total of '+str(num_samples)+' per dataframe')
    # sample_NIF_Response_hist = (spark.createDataFrame(nif_response_hist.get_dataframe()
    #                                                   .where(col('CampaignCode') == str(firstResponseCampaignCode))
    #                                                   .take(num_samples)))
    #sample_NIF_Contact_hist = (spark.createDataFrame(nif_contact_hist.get_dataframe()
    #                                                 .where(col('CampaignCode') == str(firstResponseCampaignCode))
    #                                                 .take(num_samples)))

    sample_NIF_Response_hist = (nif_response_hist.get_dataframe()
                                #.where(col('CampaignCode') == str(firstResponseCampaignCode))
                                .where(col('CampaignCode').isin(firstResponseCampaignCode))
                                )

    sample_NIF_Contact_hist = (nif_contact_hist.get_dataframe()
                               .where(col('CampaignCode').isin(firstResponseCampaignCode))
                               )


    # Joining both
    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Joining both contact hist and response')
    nif_with_labels = \
        (sample_NIF_Contact_hist
         .join(sample_NIF_Response_hist
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
         .orderBy(col('year'), col('month'), col('day'), col('cif_nif'))
         )

    labeled_datamart_nif_level = \
        (nif_with_labels
         .groupBy(col('year'), col('month'), col('day'), col('cif_nif'))
         # Collect all possible responses in label_set
         # Sometimes, under a same CampaignCode, there can be some contradictory responses
         .agg(collect_list('Label').alias('label_set'))
         .withColumn('CampaignCode', lit(str(firstResponseCampaignCode)))
         .withColumn('Label', reduceLabelSet(col('label_set')))
         .drop(col('label_set'))
         )

    # Read IDS table
    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Leyendo IDS')
    customerAttributes = (spark.read.table('tests_es.amdocs_ids_srv'))

    # Take only the columns of interest
    prefix_filter_attributes = ('ccc', 'GNV')
    cols_CustomerAttributes = ([column for column in customerAttributes.columns
                                if column.startswith(prefix_filter_attributes)])
    collectList_cols_CustomerAttributes = [collect_list(col(column)) for column in cols_CustomerAttributes]


    def clean_attributes(column_values):
        if len(column_values) == 0:
            return np.float(-1)
        else:
            return np.mean(column_values).tolist()

    # Create UDF function
    clean_attributes_UDF = udf(clean_attributes, (FloatType()))

    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Cache customerAggregatedAttributes')
    customerAggregatedAttributes = \
        (customerAttributes
         .select(['NIF_CLIENTE'] +
                 cols_CustomerAttributes
                 )
         .groupBy(col('NIF_CLIENTE'))
         .agg(*collectList_cols_CustomerAttributes)
         .join(labeled_datamart_nif_level,
               labeled_datamart_nif_level.cif_nif == customerAttributes.NIF_CLIENTE,
               how='inner'
               )
         )

    cols_CustomerAggregatedAttributes = [column for column in customerAggregatedAttributes.columns if
                                         any(x in column for x in list(prefix_filter_attributes))]

    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Empezando a calcular los atributos')

    # Best practices:
    # https://medium.com/@mrpowers/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
    customerAggregated = (customerAggregatedAttributes
                          .select(col('NIF_CLIENTE'),
                                  col('Label'),
                                  *[clean_attributes_UDF(column).name(column.split('(')[1][:-1])
                                    for column in cols_CustomerAggregatedAttributes]))

    logging.info('['+str(strftime("%a, %d %b %Y %H:%M:%S", gmtime()))+'] Hora de escribir en Hive')
    (customerAggregated
     .write
     .format('parquet')
     .mode('overwrite')
     .saveAsTable('tests_es.ads_customerAggregatedAttributesLabeled')
     )

    logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) + '] Fin del proceso')
