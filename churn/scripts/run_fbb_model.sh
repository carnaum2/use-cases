# !/bin/bash

CLOSING_DAY=$1

if [ -z $CLOSING_DAY ]
then
   echo
   echo
   echo Run this script from any directory: ./run_fbb_model.sh yyyymmmdd 
   echo First argument is the date with format yyyymmdd closing_day
   echo
   exit
fi



echo "Running fbb delivery for closing_day" + $CLOSING_DAY

spark2-submit --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=15 --executor-cores 4 --executor-memory 60G --driver-memory 8G --conf spark.driver.maxResultSize=16G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=16G --conf spark.shuffle.service.enabled=true --conf spark.driver.extraJavaOptions=-Xss10m latest/use-cases/churn/models/fbb_churn_amdocs/fbb_churn_prod.py -p $CLOSING_DAY

