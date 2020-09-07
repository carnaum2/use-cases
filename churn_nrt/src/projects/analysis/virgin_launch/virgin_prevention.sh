# !/bin/bash

LOGGING_FILE=/var/SP/data/home/$USER/logging/virgin_prevention_exp_`date '+%Y%m%d_%H%M%S'`.log

REPO_PATH=/var/SP/data/home/jmarcoso/repositories/use-cases/

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 32G --driver-memory 16G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/home/jmarcoso/repositories/use-cases/churn_nrt/src/projects/analysis/virgin_launch/virgin_prevention.py > $LOGGING_FILE 2>&1 &