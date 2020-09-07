# !/bin/bash

MODULE_NAME=$1

echo "MODULE " $MODULE_NAME


if [ -z $MODULE_NAME ]
then
  MODULE_NAME=""
#  MODULE_NAME="navcomp,customer,ccc,spinners,scores"
##   echo
##   echo
##   echo Run this script from any directory: run_model.sh module_name
##   echo
##   exit
fi

LOGGING_FILE=/var/SP/data/home/csanc109/logging/churn_nrt_data_checker_${MODULE_NAME/\//_}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

if [ -z $MODULE_NAME ]
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/misc/data_checker/data_checker.py 2>&1 | tee $LOGGING_FILE
else
    spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/misc/data_checker/data_checker.py --module_name $MODULE_NAME 2>&1 | tee $LOGGING_FILE
fi