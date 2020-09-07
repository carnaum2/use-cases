# !/bin/bash


# ./run_model_pricesens_newprod.sh 20190731 20190831 6 16 mobile gbt_reg2 pricesens_newprod6


TR_DATE=$1
TT_DATE=$2
HORIZON_COMP=$3
HORIZON_CHECK=$4
NEW_PROD=$5
MODEL=$6
SOURCES=$7
BASE=$8
FILTER_CORRELATED=1
MODE="production"


if [[ -z $BASE ]];
then
  BASE="eval"
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

LOGGING_FILE=/var/SP/data/home/csanc109/logging/price_sens_newprod_${TR_DATE}_${TT_DATE}_${NEW_PROD}_${HORIZON_COMP}_${HORIZON_CHECK}_${BASE}_${MODEL}_`date '+%Y%m%d_%H%M%S'`.log

echo "Logging file " $LOGGING_FILE

spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=10240 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/price_sensitivity/new_product/price_sens_new_product_model.py --tr_date $TR_DATE --tt_date $TT_DATE --model $MODEL --mode $MODE --horizon_comp $HORIZON_COMP --horizon_check $HORIZON_CHECK --new_product $NEW_PROD --filter_correlated_feats $FILTER_CORRELATED --sources $SOURCES --base $BASE > $LOGGING_FILE