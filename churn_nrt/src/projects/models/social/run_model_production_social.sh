# !/bin/bash

#./run_model.sh 20190808 20190823 2


current_date=$(date +%Y%m%d)

echo $current_date


if [ ${current_date: 6} -le 10 ]
then
  echo "current day < 10..."
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-01) +9 days" +%Y%m%d) # same month, day=10

elif [ ${current_date: 6} -gt 10 ] && [ ${current_date: 6} -le 25 ]
then
  echo "current day > 10 & < 25..."
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-01) +24 days" +%Y%m%d) # same month, day=25
else
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-05) +1 month" +%Y%m%d)
fi


echo "To insert in " $DAY_TO_INSERT




TR_DATE=$1
TT_DATE=$2
SEGMENT_RULE=2
SOURCES="navcomp,customer,spinners,scores,geneva"
TARGET_DAYS=30
MODEL="gbt3"
FILTER_CORRELATED=0
#BALANCE_TR_DF=0
MODE="production"



echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "TARGET_DAYS" $TARGET_DAYS
echo "SEGMENT_RULE" $SEGMENT_RULE
echo "MODEL " $MODEL
echo "MODE " $MODE
echo "FILTER_CORRELATED" $FILTER_CORRELATED
echo "SOURCES" $SOURCES
echo "DAY_TO_INSERT" $DAY_TO_INSERT



LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_triggers/social_production_${MODE}_${TR_DATE}_${TT_DATE}_${SEGMENT_RULE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE



if [[ -z $TR_DATE ]] && [[ -z $TT_DATE ]];
then
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=12 --conf spark.dynamicAllocation.minExecutors=12 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 28G --driver-memory 16G --conf spark.driver.maxResultSize=40G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/social/social_model.py --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --filter_correlated_feats $FILTER_CORRELATED --day_to_insert $DAY_TO_INSERT  > $LOGGING_FILE
else
  spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=12 --conf spark.dynamicAllocation.minExecutors=12 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 28G --driver-memory 16G --conf spark.driver.maxResultSize=40G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn_nrt/src/projects/models/social/social_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --filter_correlated_feats $FILTER_CORRELATED --day_to_insert $DAY_TO_INSERT > $LOGGING_FILE
fi


