#!/usr/bin/env python
# -*- coding: utf-8 -*-



#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=12 --conf spark.dynamicAllocation.minExecutors=12 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 24G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096  /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/social/analysis/geneva_test.py > /var/SP/data/home/csanc109/logging/geneva_data.log

if __name__ == "__main__":

    import os, re
    import sys
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

    from churn_nrt.src.utils.spark_session import get_spark_session
    sc, spark, sql_context = get_spark_session("trigger_myvf")
    sc.setLogLevel('WARN')

    import time
    start_time = time.time()
    from churn_nrt.src.data.geneva_data import GenevaData
    df_geneva = GenevaData(spark, 90).get_module("20191124", save=True, save_others=False, force_gen=False)
    print("Elapsed time {} minutes".format((time.time()-start_time)/60.0))

##### FORMA DE HACER AGREGADOS
#def get_aggregates_geneva(spark, date_) # devuelve DF con una fila por msisdn y una columna para cada agregado
#dates_ = lista de días
#lista_de_dfs = [get_aggregates_geneva(spark, date_)  for d in dates_]
#lista_de_dfs.reduce(x, y: x.union(y).groupBy(msisdn).agg(sql_sum(agregado).alias(agregado)))