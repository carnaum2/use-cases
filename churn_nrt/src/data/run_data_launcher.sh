# !/bin/bash

CLOSING_DAY=$1
MODULE_NAME=$2
EXTRA_INFO=$3

if [ -z $CLOSING_DAY ] || [ -z $MODULE_NAME ] #|| [ -z $EXTRA_INFO ]
then
   echo
   echo
   echo Run this script from any directory: run_orders_sla.sh yyyymmmdd module_name extra_info
   echo First argument is the date with format yyyymmdd closing_day
   echo Second argument is the module name
   echo Third argument is any extra info field required for builtin the module
   echo
   exit
fi


if [ -z $EXTRA_INFO ]
then
  ARGS="-c ${CLOSING_DAY} -m ${MODULE_NAME}"
else
  ARGS="-c ${CLOSING_DAY} -m ${MODULE_NAME} -e ${EXTRA_INFO}"
fi

echo "Running data launcher with arguments ${ARGS}"
spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 data_launcher.py $ARGS


