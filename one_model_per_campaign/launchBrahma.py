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
    sys.path.append('/var/SP/data/home/adesant3/src/use-cases')
    from utils.adesant3.delivery import deliverTable
    from utils.adesant3.config import setting_bdp

    setting_bdp(min_n_executors=15,
                max_n_executors=25,
                n_cores=4,
                executor_memory="50g",
                app_name="Brahma")
    sc, spark, sqlContext = run_sc()


    # spark.sparkContext.addPyFile("utils_adesant3.zip")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/utils/adesant3/dataPreparation.py")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/utils/src/main/python/general_model_trainer/general_model_trainer.py")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/one_model_per_campaign/Brahma.py")
    spark.sparkContext.addPyFile("/var/SP/data/home/adesant3/src/use-cases/one_model_per_campaign/CampaignsMapping.py")

    from dataPreparation import (NIFContactHist, NIFResponseHist)
    from Brahma import *
    from CampaignsMapping import *

    mappingCampaigns = CampaignsMapping(spark)

    # print(mappingCampaigns.get_campaign_type_list())

    dict_campaign = mappingCampaigns.get_dict_campaign()
    campaign_type_list = mappingCampaigns.get_campaign_type_list()

    type_campaign = 'Up_Cross_Sell_HH'

    campaign_codes_list = dict_campaign['Up_Cross_Sell_HH']

    model = Brahma(spark, campaign_codes_list, type_campaign, month_training = [8,9], year_training = [2018])
    model.calculate_campaign_hist_per_nif()
    training_data = model.inject_ids_attributes_at_nif_level(prefix_filter_attributes = ('ccc', 'GNV'))
    model.train(training_data.repartition(200))

    model.calculate_model_metrics(sc)
    model.save()

    logging.info('[' + str(strftime("%a, %d %b %Y %H:%M:%S", gmtime())) +
                 '] Finished the ' + type_campaign + ' campaigns model.')

