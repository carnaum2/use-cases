# !/bin/bash

CLOSING_DAY=$1

if [ -z $CLOSING_DAY ]
then
   echo
   echo
   echo Run this script from any directory: run_orders_sla.sh yyyymmmdd 
   echo First argument is the date with format yyyymmdd closing_day
   echo
   exit
fi



echo "Running orders sla for closing_day" + $CLOSING_DAY

spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 ../../churn/datapreparation/general/sla_data_loader.py -c $CLOSING_DAY


