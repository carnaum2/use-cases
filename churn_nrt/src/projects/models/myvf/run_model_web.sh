# !/bin/bash

#/var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/myvf $ ./run_model.sh 20200107 20200123 app navcomp,customer,spinners,scores,myvf_v2

TR_DATE=$1
TT_DATE=$2
PLATFORM=$3
TARGET_DAYS=15
NAVIG_DAYS=14
NAVIG_SECTIONS="permanencia"
MODEL=$5
#"gbt3"
FILTER_CORRELATED=1
SOURCES=$4
BALANCE_TR_DF=0
MODE="evaluation"


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
echo "SOURCES" $SOURCES

if [ -z $TR_DATE ] || [ -z $TT_DATE ] || [ -z $PLATFORM ] || [ -z $TARGET_DAYS ] || [ -z $NAVIG_DAYS ]
then
   echo
   echo
   echo Run this script from any directory: run_model.sh yyyymmmdd yyyymmdd model mode
   echo First argument is the date from training with format yyyymmdd
   echo Second argument is the date from test with format yyyymmdd
   echo Third argument is the platform: app or web
   echo Fourth argument is the number of days for the target
   echo Fifth argument is the number of days to look for customers that have navigated
   echo Sixth argument is the list of sections to be taken into account for navigation

  # echo Third argument is the model to used: rf or xgboost
  # echo Fourth argument is the mode: evaluation or prediction
   echo
   exit
fi

LOGGING_FILE=/var/SP/data/home/csanc109/logging/myvfweb_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/myvf/myvf_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --navig_days $NAVIG_DAYS --platform $PLATFORM --navig_sections $NAVIG_SECTIONS --filter_correlated_feats $FILTER_CORRELATED --balance_tr_df $BALANCE_TR_DF  > $LOGGING_FILE