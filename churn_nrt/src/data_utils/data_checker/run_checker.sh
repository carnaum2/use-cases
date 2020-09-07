# !/bin/bash

SCOPE=$1
C_FROM_DATE=$2
C_TO_DATE=$3
P_FROM_DATE=$4
P_TO_DATE=$5

if [[ -z $SCOPE ]];
then
  SCOPE="all"
fi


echo "SCOPE" $SCOPE
echo "C_FROM_DATE " $C_FROM_DATE
echo "C_TO_DATE " $C_TO_DATE
echo "P_FROM_DATE " $P_FROM_DATE
echo "P_TO_DATE " $P_TO_DATE

LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_checker/checker_churn_nrt_${SCOPE}_${C_FROM_DATE}_${C_TO_DATE}_${P_FROM_DATE}_${P_TO_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE


if [[ -z $C_FROM_DATE ]] && [[ -z $C_TO_DATE ]] && [[ -z $P_FROM_DATE ]] && [[ -z $P_TO_DATE ]];
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/data_utils/data_checker/data_checker.py --scope $SCOPE > $LOGGING_FILE
elif [ "$C_FROM_DATE" = "None" ]; then
  echo "Entering partition mode"
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/data_utils/data_checker/data_checker.py --p_from_date $P_FROM_DATE --p_to_date $P_TO_DATE --scope $SCOPE > $LOGGING_FILE
else
  echo "Entering creation mode"
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/data_utils/data_checker/data_checker.py --c_from_date $FROM_DATE --c_to_date $TO_DATE --scope $SCOPE > $LOGGING_FILE
fi