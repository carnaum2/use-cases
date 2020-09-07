#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from common.src.main.python.utils.hdfs_generic import *
import os

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

    closing_day = sys.argv[1]

    set_paths()

    sc, sparkSession, _ = run_sc()

    spark = (SparkSession \
             .builder \
             .appName("Trigger identification") \
             .master("yarn") \
             .config("spark.submit.deployMode", "client") \
             .config("spark.ui.showConsoleProgress", "true") \
             .enableHiveSupport().getOrCreate())

    from churn.analysis.triggers.ml_triggers.utils_trigger import get_trigger_minicar2

    minicar_df = get_trigger_minicar2(spark, closing_day)

    cols = minicar_df.columns

    print '[Info billing_test] Number of distinct NIFs: ' + str(minicar_df.select('nif_cliente').distinct().count())

    print '[Info billing_test] Volume: ' + str(minicar_df.count())

    print '[Info billing_test] Number of columns: ' + str(len(cols))

    for c in cols:
        print '[Info billing_test] Billing feature: ' + c

