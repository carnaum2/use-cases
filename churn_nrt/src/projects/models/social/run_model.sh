# !/bin/bash

# ./run_model.sh 20191103 20200203 2 navcomp,customer,spinners,scores,geneva

TR_DATE=$1
TT_DATE=$2
SEGMENT_RULE=$3
#SOURCES="navcomp,customer,ccc,spinners,scores,geneva"
SOURCES=$4
TARGET_DAYS=30
MODEL="gbt3"
FILTER_CORRELATED=0
#BALANCE_TR_DF=0
MODE="evaluation"


echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "TARGET_DAYS" $TARGET_DAYS
echo "SEGMENT_RULE" $SEGMENT_RULE
echo "MODEL " $MODEL
echo "MODE " $MODE
echo "FILTER_CORRELATED" $FILTER_CORRELATED
#echo "BALANCE_TR_DF" $BALANCE_TR_DF
echo "SOURCES" $SOURCES

if [ -z $TR_DATE ] || [ -z $TT_DATE ] ||  [ -z $TARGET_DAYS ]
then
   echo
   echo
   echo Run this script from any directory: run_model.sh yyyymmmdd yyyymmdd model mode
   echo First argument is the date from training with format yyyymmdd
   echo Second argument is the date from test with format yyyymmdd


  # echo Third argument is the model to used: rf or xgboost
  # echo Fourth argument is the mode: evaluation or prediction
   echo
   exit
fi

LOGGING_FILE=/var/SP/data/home/csanc109/logging/social_${MODE}_${TR_DATE}_${TT_DATE}_${SEGMENT_RULE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=12 --conf spark.dynamicAllocation.minExecutors=12 --conf spark.dynamicAllocation.maxExecutors=40 --executor-cores 4 --executor-memory 24G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 --conf 'spark.executor.extraJavaOptions=-XX:+UseCompressedOops -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC' /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/social/social_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --filter_correlated_feats $FILTER_CORRELATED  > $LOGGING_FILE