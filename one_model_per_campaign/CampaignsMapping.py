import sys

from dataPreparation import (NIFContactHist, NIFResponseHist)
from pyspark.sql.functions import (udf, col, when, lit, collect_list, regexp_replace, max as sql_max)
from pyspark.sql.types import DoubleType, StringType, IntegerType, ArrayType, FloatType
from time import strftime, gmtime



# Numpy stuff
import numpy as np


# Config logging
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(message)s'
                    )


# Import packages
sys.path.append('/var/SP/data/home/adesant3/')


class CampaignsMapping(object):

    def __init__(self, spark):
        data_campaigns_ORI_param = \
        (spark
            .read
            .format('com.databricks.spark.csv')
            .options(header='true',
                     inferschema='true',
                     delimiter='\t')
            .load('/tmp/rbuendi1/PARAM_OCM/')
         )

        data_campaigns_param_tmp1 = (data_campaigns_ORI_param
                                     .withColumn('subtipo2', regexp_replace(col("subtipo"), "[- ]", "_"))
                                     .withColumn('subtipo',
                                                 when(col('subtipo2') == 'Up_Cross_Sell', 'Up_Cross_Sell_Voice')
                                                 .otherwise(col('subtipo2'))
                                                 )
                                     .groupBy('campaigncode', 'subtipo')
                                     .agg(sql_max(col('ARPU_PRODUCTO_PREVIO')).alias('ARPU')
                                          , sql_max(col('LTV_PRODUCTO_PREVIO')).alias('LTV'))
                                     )

        mapping_campaign_type = \
        (data_campaigns_param_tmp1
         .groupBy(col('subtipo').alias('campaign_type'))
         .agg(collect_list(col('campaigncode')).alias('campaign_list'))
         .collect()
         )

        tmp_dict_campaign_type = map(lambda row: row.asDict(), mapping_campaign_type)
        self.dict_campaign_type= {iter_campaign_type['campaign_type']:
                                      iter_campaign_type['campaign_list'] for iter_campaign_type in tmp_dict_campaign_type}

    def get_dict_campaign(self):
        return self.dict_campaign_type

    def get_campaign_type_list(self):
        return self.dict_campaign_type.keys()






