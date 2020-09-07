# !/bin/bash

TR_DATE=$1
TT_DATE=$2
SOURCES=$3
MODEL="rf"
MODE="evaluation"


echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "MODEL " $MODEL
echo "MODE " $MODE
echo "SOURCES " $SOURCES

if [ -z $TR_DATE ] || [ -z $TT_DATE ] #|| [ -z $EXTRA_INFO ]
then
   echo
   echo
   echo Run this script from any directory: run_model.sh yyyymmmdd yyyymmdd model mode
   echo First argument is the date from training with format yyyymmdd
   echo Second argument is the date from test with format yyyymmdd
 #  echo Third argument is the model to used: rf or xgboost
 #  echo Fourth argument is the mode: evaluation or prediction
   echo
   exit
fi

LOGGING_FILE=/var/SP/data/home/csanc109/logging/nav_comp_evaluation_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=4096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/navcomp/navcomp_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE 2>&1 | tee $LOGGING_FILE