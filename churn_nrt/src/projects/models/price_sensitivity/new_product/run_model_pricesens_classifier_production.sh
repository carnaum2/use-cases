# !/bin/bash

# ./run_model_pricesens_newprod.sh 20190731 20191231 6 16 mobile rf_class5 pricesens_newprod7 eval 3 data_generation


TR_DATE=$1
TT_DATE=$2
HORIZON_COMP=0
HORIZON_CHECK=20
LABEL_TYPE=$3
NEW_PROD="global"
MODEL="rf_class"
SOURCES="pricesens_newprod10"
BASE="comp"
FILTER_CORRELATED=1
MODE="production"
PURPOSE="modeling"
DAY_TO_INSERT=$4


if [[ -z $LABEL_TYPE ]];
then
  LABEL_TYPE=3
fi

if [[ -z $DAY_TO_INSERT ]];
then
  DAY_TO_INSERT=$(date -d "$(date +%Y-%m-%d) +1 day" +%Y%m%d)
fi


# SOURCES "navcomp,customer,ccc,spinners,scores,myvf"

echo "TR_DATE " $TR_DATE
echo "TT_DATE " $TT_DATE
echo "HORIZON_COMP " $HORIZON_COMP
echo "HORIZON_CHECK " $HORIZON_CHECK
echo "NEW_PROD " $NEW_PROD
echo "MODEL " $MODEL
echo "FILTER_CORRELATED" $FILTER_CORRELATED
echo "SOURCES" $SOURCES
echo "BASE" $BASE
echo "LABEL_TYPE" $LABEL_TYPE
echo "DO_CALIBRATE_SCORES" $DO_CALIBRATE_SCORES
echo "PURPOSE" $PURPOSE
echo "DAY_TO_INSERT" $DAY_TO_INSERT


LOGGING_FILE=/var/SP/data/bdpmdses/deliveries_churn/logs_triggers/price_sens_newprod_classifier_prod_${TR_DATE}_${TT_DATE}_${NEW_PROD}_${HORIZON_COMP}_${HORIZON_CHECK}_${BASE}_${MODEL}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE


spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/price_sensitivity/new_product/price_sens_new_product_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --mode $MODE --horizon_comp $HORIZON_COMP --horizon_check $HORIZON_CHECK --new_product $NEW_PROD --filter_correlated_feats $FILTER_CORRELATED --sources $SOURCES --base $BASE --label_type $LABEL_TYPE --purpose $PURPOSE  --day_to_insert $DAY_TO_INSERT > $LOGGING_FILE


