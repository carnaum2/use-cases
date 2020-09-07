#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

#from common.src.main.python.utils.hdfs_generic import *
import os
import sys
import time

def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
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

if __name__ == "__main__":

    #sys.path.append('/var/SP/data/home/jmarcoso/repositories')
    #sys.path.append('/var/SP/data/home/jmarcoso/repositories/use-cases')

    set_paths()

    # Reading from mini_ids. The following imports would be required for the automated process (computing features on the fly)
    #from churn.analysis.triggers.base_utils.base_utils import get_customer_base_segment
    #from churn.analysis.triggers.ccc_utils.ccc_utils import get_nif_ccc_attributes_df

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################
    print '[' + time.ctime() + ']', 'Process started'
    print os.environ.get('SPARK_COMMON_OPTS', '')
    print os.environ.get('PYSPARK_SUBMIT_ARGS', '')
    global sqlContext

    sc, sparkSession, sqlContext = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Modeler Test") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    ##########################################################################################
    # 1. Loading the data: delivery of triggers_ml and the corresponding label
    ##########################################################################################

    deliv_date = sys.argv[1]

    # Parsing year, month, day

    deliv_year = deliv_date[0:4]

    deliv_month = str(int(deliv_date[4:6]))

    deliv_day = str(int(deliv_date[6:8]))

    # target

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    labeldf = get_churn_target_nif(spark, deliv_date, churn_window=30).select('nif_cliente', 'label').distinct()

    # delivery

    from pyspark.sql.functions import col, randn

    delivdf = spark \
        .read \
        .parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml/year=' + deliv_year + '/month=' + deliv_month + '/day=' + deliv_day)\
        .select(['nif', 'scoring']).withColumn('scoring', col('scoring') + 0.0001 * randn())\
        .withColumnRenamed('nif', 'nif_cliente')

    # labelling

    delivdf = delivdf.distinct().join(labeldf, ['nif_cliente'], 'left').na.fill({'label': 0.0})

    # taking 50000 samples from delivdf

    myschema = delivdf.schema

    delivdf = spark.createDataFrame(delivdf.head(50000), schema=myschema)

    ##########################################################################################
    # 2. Getting the cumlift with the old function
    # ##########################################################################################

    from churn_nrt.src.projects_utils.models.modeler import get_cummulative_lift

    start = time.time()

    get_cummulative_lift(spark, delivdf.withColumnRenamed("scoring", "model_score"), 1.0, [5000*i for i in range(1,11)])

    end = time.time()

    old_fcn = (end - start)

    from churn_nrt.src.projects_utils.models.modeler import get_cumulative_lift2

    start = time.time()

    result = get_cumulative_lift2(spark, delivdf, step_ = 5000, ord_col = "scoring", label_col = "label", verbose = False)

    result.show()

    end = time.time()

    new_fcn = (end - start)

    print "[Info modeler_test] Old function: " + str(old_fcn) + " - New function: " + str(new_fcn)



