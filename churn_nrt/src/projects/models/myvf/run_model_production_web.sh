# !/bin/bash

TR_DATE=$1
TT_DATE=$2
DAY_TO_INSERT=$3
PLATFORM="web"
TARGET_DAYS=15
NAVIG_DAYS=14
NAVIG_SECTIONS="permanencia"
MODEL="rf5"
FILTER_CORRELATED=1
BALANCE_TR_DF=0
MODE="production"


if [[ -z $DAY_TO_INSERT ]];
then
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-%d) +1 day" +%Y%m%d)
fi

SOURCES="navcomp,customer,spinners,myvfweb_v3"

echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "PLATFORM" $PLATFORM
echo "TARGET_DAYS" $TARGET_DAYS
echo "NAVIG_DAYS" $NAVIG_DAYS
echo "MODEL " $MODEL
echo "MODE " $MODE
echo "NAVIG_SECTIONS " $NAVIG_SECTIONS
echo "FILTER_CORRELATED" $FILTER_CORRELATED
echo "BALANCE_TR_DF" $BALANCE_TR_DF
echo "DAY_TO_INSERT" $DAY_TO_INSERT
echo "SOURCES" $SOURCES

LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_triggers/myvfweb_production_${PLATFORM}_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

#echo "TR_DATE " $TR_DATE --> training date computed automatically in py script
#echo "TT_DATE " $TT_DATE --> test date computed automatically in py script
#echo "MODEL " $MODEL --> "rf"
#echo "MODE " $MODE --> production



if [[ -z $TR_DATE ]] && [[ -z $TT_DATE ]];
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 22G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10000 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/myvf/myvf_model.py --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --navig_days $NAVIG_DAYS --platform $PLATFORM --navig_sections $NAVIG_SECTIONS --filter_correlated_feats $FILTER_CORRELATED --balance_tr_df $BALANCE_TR_DF --day_to_insert $DAY_TO_INSERT > $LOGGING_FILE
elif [[ -z $TT_DATE ]];
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 22G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10000 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/myvf/myvf_model.py --tr_date $TR_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --navig_days $NAVIG_DAYS --platform $PLATFORM --navig_sections $NAVIG_SECTIONS --filter_correlated_feats $FILTER_CORRELATED --balance_tr_df $BALANCE_TR_DF --day_to_insert $DAY_TO_INSERT > $LOGGING_FILE
else
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 22G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10000 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/myvf/myvf_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --navig_days $NAVIG_DAYS --platform $PLATFORM --navig_sections $NAVIG_SECTIONS --filter_correlated_feats $FILTER_CORRELATED --balance_tr_df $BALANCE_TR_DF --day_to_insert $DAY_TO_INSERT > $LOGGING_FILE
fi