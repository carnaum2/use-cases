# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf, col, array, abs, sort_array, decode, when, lit, lower, translate, count, isnull,substring, size, length, desc)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.functions import *
from utils_trigger import get_trigger_minicar2, get_billing_car, getIds, get_tickets_car, get_filtered_car, get_next_dow
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
import matplotlib

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

        #from churn.utils.constants import CHURN_DELIVERIES_DIR
        #root_dir = CHURN_DELIVERIES_DIR
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
    import argparse

    parser = argparse.ArgumentParser(description='List of Configurable Parameters')
    parser.add_argument('-d', '--date_', metavar='<date_>', type=str, help='date', required=True)

    args = parser.parse_args()

    set_paths_and_logger()

    date_ = args.date_

    import pykhaos.utils.pyspark_configuration as pyspark_config

    sc, spark, sql_context = pyspark_config.get_spark_session(app_name="ticket_triggers", log_level="OFF",
                                                              min_n_executors=1, max_n_executors=15, n_cores=4,
                                                              executor_memory="32g", driver_memory="32g")
    print("[Info ml_triggers_test] ############ Process Started ##############")

    from churn.analysis.triggers.ml_triggers.utils_trigger import get_trigger_minicar3

    df = get_trigger_minicar3(spark, date_)

    print "[Info ml_triggers_test] Process completed - Size of df: " + str(df.count()) + " - Number of distinct NIFs: " + str(df.select('nif_cliente').distinct().count())


