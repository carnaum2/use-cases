#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.src.main.python.utils.hdfs_generic import *
import sys
import time
from pyspark.sql.functions import (col, avg as sql_avg)

import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

ALL_MODULES = ["orders_sla", "billing",  "customer_base", "navcomp/15", "customer_additional", "scores", "ccc/msisdn", "ccc/nif", "spinners", "scores"]

def set_paths():
    import os, re

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

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("data_checker")
    sc.setLogLevel('WARN')

    start_time_total = time.time()

    ##########################################################################################
    # 1. Getting input arguments:
    #      - tr_date_: training set
    #      - tt_date_: test set
    #      - algorithm: algorithm for training
    #      - mode_ : evaluation or prediction
    ##########################################################################################

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    import argparse

    parser = argparse.ArgumentParser(
        description="Run data checker ./run_data_checker.sh [module_name]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--module_name', metavar='<module name>', type=str, required=False,
                        help='name of the module')

    args = parser.parse_args()
    print(args)

    module_name_arg = args.module_name

    if not module_name_arg:
        print("Empty parameter 'module_name' -- Running for all modules")
        modules_list = ALL_MODULES
    elif "," in module_name_arg:
        modules_list = module_name_arg.split(",")
    else:
        modules_list = [module_name_arg]

    #FIXME lower all the column names
    ids = ["msisdn", "NUM_CLIENTE", "NIF_CLIENTE", "nif_cliente"]

    from churn_nrt.src.utils.pyspark_utils import count_nans

    for module_name in modules_list:

        print("- - - - - - - - module_name={} - - - - - - - - ".format(module_name))

        if module_name == "orders_sla":
            from churn_nrt.src.data.orders_sla import OrdersSLA
            df_metadata = OrdersSLA(spark).get_metadata()
        # elif module_name == "customer":
        #     from churn_nrt.src.data.customers_data import Customer
        #     df_metadata = Customer(spark).get_metadata()
        # elif module_name == "service":
        #     from churn_nrt.src.data.services_data import Service
        #     df_metadata = Service(spark).get_metadata()
        elif module_name == "scores":
            from churn_nrt.src.data.scores import Scores
            df_metadata = Scores(spark).get_metadata()
        elif module_name == "spinners":
            from churn_nrt.src.data.spinners import Spinners
            df_metadata = Spinners(spark).get_metadata()
        elif module_name == "billing":
            from churn_nrt.src.data.billing import Billing
            df_metadata = Billing(spark).get_metadata()
        elif module_name == "customer_base":
            from churn_nrt.src.data.customer_base import CustomerBase
            df_metadata = CustomerBase(spark).get_metadata()
        elif module_name.startswith("navcomp"):
            from churn_nrt.src.data.navcomp_data import NavCompData
            df_metadata = NavCompData(spark, int(module_name.split("/")[1])).get_metadata()
        elif module_name.startswith("ccc"):
            from churn_nrt.src.data.ccc import CCC
            df_metadata = CCC(spark, level=module_name.split("/")[1]).get_metadata()
        elif module_name.startswith("customer_additional"):
            from churn_nrt.src.data.customer_base import CustomerAdditional
            df_metadata = CustomerAdditional(spark, int(module_name.split("/")[1])).get_metadata()
        elif module_name.startswith("myvf"):
            from churn_nrt.src.data.myvf_data import MyVFdata
            df_metadata = MyVFdata(spark, module_name.split("/")[1]).get_metadata()
        else:
            print("[ERROR] Add the module name '{}' in data_checker.py".format(module_name))
            import sys
            sys.exit()

        metadata_cols = df_metadata.rdd.map(lambda x: x['feature']).collect()

        module_partitions = spark.read.load("/data/udf/vf_es/churn_nrt/{}/".format(module_name)).select("year", "month", "day").distinct().rdd.map(lambda x: (x['year'], x['month'], x['day'])).collect()

        for part in module_partitions:

            path_module = "/data/udf/vf_es/churn_nrt/{}/year={}/month={}/day={}".format(module_name, part[0], part[1], part[2])
            module_cols = list(set(spark.read.load(path_module).columns) - set(ids))

            A = list(set(metadata_cols) - set(module_cols))
            B = list(set(module_cols) - set(metadata_cols))
            C = count_nans(spark.read.load(path_module))

            if A or B or C:
                print("ERROR partition '{}'".format(path_module))

                if A:
                    print("\t metadata has columns that are not present in module: {}".format(",".join(A)))
                if B:
                    print("\t module has columns that are not present in metadata: {}".format(",".join(B)))
                if C:
                    print("\t module has nulls in columns: {}".format(",".join(C.keys())))


            else:
                print("OK partition '{}'".format(path_module))
