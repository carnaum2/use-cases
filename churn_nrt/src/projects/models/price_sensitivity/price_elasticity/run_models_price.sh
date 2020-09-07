# !/bin/bash

TR_DATE=$1
TT_DATE=$2
ARPC=$3
HORIZON=$4
MODEL=$5
MODE=$6

echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "ARPC " $ARPC
echo "HORIZON " $HORIZON
echo "MODEL" $MODEL
echo "MODE" $MODE

if [ -z $TR_DATE ] || [ -z $TT_DATE ]
then
   echo
   echo
   echo Run this script from any directory: run_model.sh yyyymmmdd yyyymmdd model mode
   echo First argument is the date from training with format yyyymmdd
   echo Second argument is the date from test with format yyyymmdd
   echo Third argument is the ARPC segment to be analyzed yyyymmdd
   echo Fourth argument is the model to used: rf or xgboost
   echo Fifth argument is the mode: evaluation or production
   echo
   exit
fi

LOGGING_FILE_HARD=/var/SP/data/home/$USER/logging/price/price_hard_${TR_DATE}_${TT_DATE}_${ARPC}_`date '+%Y%m%d_%H%M%S'`.log
LOGGING_FILE_SOFT=/var/SP/data/home/$USER/logging/price/price_soft_${TR_DATE}_${TT_DATE}_${ARPC}_`date '+%Y%m%d_%H%M%S'`.log
LOGGING_FILE_NONE=/var/SP/data/home/$USER/logging/price/price_none_${TR_DATE}_${TT_DATE}_${ARPC}_`date '+%Y%m%d_%H%M%S'`.log

REPO_PATH=/var/SP/data/home/asaezco/src/devel2/use-cases/

echo "Launching price models "
echo "Logging file hard " $LOGGING_FILE_HARD
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/price_sensitivity/price_elasticity/price_model.py --tr_date $TR_DATE --tt_date $TT_DATE --price_q $ARPC --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment hard > $LOGGING_FILE_HARD 2>&1 &
echo "Logging file soft " $LOGGING_FILE_SOFT
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/price_sensitivity/price_elasticity/price_model.py --tr_date $TR_DATE --tt_date $TT_DATE --price_q $ARPC --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment soft > $LOGGING_FILE_SOFT 2>&1 &
echo "Logging file none " $LOGGING_FILE_NONE
spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 8 --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 --driver-memory 8G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 $REPO_PATH/churn_nrt/src/projects/models/price_sensitivity/price_elasticity/price_model.py --tr_date $TR_DATE --tt_date $TT_DATE --price_q $ARPC --model $MODEL --mode $MODE --horizon $HORIZON --verbose 0 --segment none > $LOGGING_FILE_NONE 2>&1 &