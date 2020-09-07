# !/bin/bash

DELIV_DATE=$1
STORE_RESULT=$2
INSERT_DAY=$3

STORE_RESULT=${STORE_RESULT:=y}
INSERT_DAY=${INSERT_DAY:=5}


if [ -z DELIV_DATE ]
then
   echo
   echo
   echo Run this script from any directory: trigger_services_tracker.sh yyyymmdd store
   echo First argument is the date of the delivery with format yyyymmdd
   echo Second argument is the option to indicate whether the result must be stored s or not n
   echo
   exit
fi

LOGGING_FILE=/var/SP/data/home/$USER/logging/smart_routing/smart_routing_${DELIV_DATE}_`date '+%Y%m%d_%H%M%S'`.log

REPO_PATH=/var/SP/data/home/jmarcoso/repositories/use-cases/

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 32G --driver-memory 16G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/home/jmarcoso/repositories/use-cases/churn_nrt/src/projects/analysis/smart_routing/smart_routing_extractor.py --ids_date $DELIV_DATE --store $STORE_RESULT --insert_day $INSERT_DAY > $LOGGING_FILE 2>&1 &