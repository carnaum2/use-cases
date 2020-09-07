# !/bin/bash

TR_DATE=$1
TT_DATE=$2
MODEL="rf"
MODE="production"
DAY_TO_INSERT=$3


if [[ -z $DAY_TO_INSERT ]];
then
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-%d) +1 day" +%Y%m%d)
fi


LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_triggers/nav_comp_production_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE
echo "To insert in " $DAY_TO_INSERT


#echo "TR_DATE " $TR_DATE --> training date computed automatically in py script
#echo "TT_DATE " $TT_DATE --> test date computed automatically in py script
#echo "MODEL " $MODEL --> "rf"
#echo "MODE " $MODE --> production

if [[ -z $TR_DATE ]] && [[ -z $TT_DATE ]];
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/navcomp/navcomp_model.py --model $MODEL --sources "navcomp,customer,ccc,spinners" --mode $MODE --day_to_insert $DAY_TO_INSERT 2>&1 | tee $LOGGING_FILE
else
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/navcomp/navcomp_model.py --model $MODEL --sources "navcomp,customer,ccc,spinners" --mode $MODE  --tr_date $TR_DATE --tt_date $TT_DATE --day_to_insert $DAY_TO_INSERT  2>&1 | tee $LOGGING_FILE
fi