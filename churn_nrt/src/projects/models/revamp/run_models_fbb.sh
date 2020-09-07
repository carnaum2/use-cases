# !/bin/bash

TR_DATE=$1
TT_DATE=$2
HORIZON=$3
MODEL=$4
MODE=$5
INSERT_DAY=$6

INSERT_DAY=${INSERT_DAY:=5}


echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "HORIZON " $HORIZON
echo "MODEL " $MODEL
echo "MODE " $MODE

if [ -z $TR_DATE ] || [ -z $TT_DATE ] #|| [ -z $EXTRA_INFO ]
then
   echo
   echo
   echo Run this script from any directory: run_model.sh yyyymmmdd yyyymmdd model mode
   echo First argument is the date from training with format yyyymmdd
   echo Second argument is the date from test with format yyyymmdd
   echo Third argument is the model to used: rf or xgboost
   echo Fourth argument is the mode: evaluation or prediction
   echo
   exit
fi

LOGGING_FILE_HARD=/var/SP/data/home/$USER/logging/revamp/revamp_fbb_hard_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log
LOGGING_FILE_SOFT=/var/SP/data/home/$USER/logging/revamp/revamp_fbb_soft_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log
LOGGING_FILE_NONE=/var/SP/data/home/$USER/logging/revamp/revamp_fbb_none_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

REPO_PATH=/var/SP/data/home/jmarcoso/repositories/use-cases/

echo "Launching fbb models "
echo "Logging file hard " $LOGGING_FILE_HARD
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/revamp/revamp_fbb.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment hard --insert_day $INSERT_DAY > $LOGGING_FILE_HARD 2>&1 &
echo "Logging file soft " $LOGGING_FILE_SOFT
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/revamp/revamp_fbb.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment soft --insert_day $INSERT_DAY > $LOGGING_FILE_SOFT 2>&1 &
echo "Logging file none " $LOGGING_FILE_NONE
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/revamp/revamp_fbb.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment none --insert_day $INSERT_DAY > $LOGGING_FILE_NONE 2>&1 &