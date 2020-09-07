# !/bin/bash

#/var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/myvf $ ./run_model.sh 20200107 20200123 app navcomp,customer,spinners,scores,myvf_v2

TR_DATE=$1
TT_DATE=$2
TARGET_DAYS=15
SOURCES="navcomp_adv,callscomp_adv,customer,ccc,billing,spinners"
MODEL="rf"
FILTER_CORRELATED=1
BALANCE_TR_DF=1
MODE="evaluation"

if [[ -z $MODEL ]];
then
  MODEL="rf"
fi

# SOURCES "navcomp,customer,ccc,spinners,scores,myvf"

echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "PLATFORM" $PLATFORM
echo "TARGET_DAYS" $TARGET_DAYS
echo "MODEL " $MODEL
echo "MODE " $MODE
echo "FILTER_CORRELATED" $FILTER_CORRELATED
echo "BALANCE_TR_DF" $BALANCE_TR_DF
echo "SOURCES" $SOURCES



LOGGING_FILE=/var/SP/data/home/csanc109/logging/prop_virgin_${TR_DATE}_${TT_DATE}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/virgin/virgin_prop_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --sources $SOURCES --mode $MODE --target_days $TARGET_DAYS --filter_correlated_feats $FILTER_CORRELATED --balance_tr_df $BALANCE_TR_DF  > $LOGGING_FILE