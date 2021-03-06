# !/bin/bash

TR_DATE=$1
TT_DATE=$2
MODEL="rf"
MODE="production"

LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_triggers/trigger_zhilabs_prod_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

#echo "TR_DATE " $TR_DATE --> training date computed automatically in py script
#echo "TT_DATE " $TT_DATE --> test date computed automatically in py script
#echo "MODEL " $MODEL --> "gbt"
#echo "MODE " $MODE --> production

if [[ -z $TR_DATE ]] && [[ -z $TT_DATE ]];
then
  nohup spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 24G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/trigger_zhilabs/zhilabs_model.py --model $MODEL --mode $MODE  --critical critical > $LOGGING_FILE
else
  nohup spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 24G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/trigger_zhilabs/zhilabs_model.py --model $MODEL --mode $MODE  --tr_date $TR_DATE --tt_date $TT_DATE --critical critical > $LOGGING_FILE
fi