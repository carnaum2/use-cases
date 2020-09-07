# !/bin/bash
#set -x #echo on

# This script does not have any mandatory argument
# BASE_CLOSING_DAY is the day for the base in order to make predictions. Default = auto (it means it gets the last available day)
# RULE_NB is the number to identify the segment. 0 was the one used until 25-July (included). As of 30-July, rule number 2 is the default.:: 

BASE_CLOSING_DAY=$1
RULE_NB=$2
CLOSING_DAY=$3

if [ -z $CLOSING_DAY ]
then
   CLOSING_DAY=20190414 
fi

if [ -z $PREDICT_DAY ]
then
   PREDICT_DAY=auto
fi

if [ -z $RULE_NB ]
then
   RULE_NB=2
fi

SAVE="--save"
echo "training date" $CLOSING_DAY
echo "predict date" $PREDICT_DAY
echo "save" $SAVE
echo "rule" $RULE_NB

spark2-submit --conf spark.driver.port=58100 --conf spark.blockManager.port=58110 --conf spark.broadcast.port=58120 --conf spark.replClassServer.port=58130 --conf spark.ui.port=58140 --conf spark.executor.port=58150 --conf spark.fileserver.port=58160 --conf spark.port.maxRetries=1000  --queue root.BDPtenants.es.medium --conf spark.port.maxRetries=1000 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.yarn.executor.driverOverhead=2G --executor-cores 4 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=3 --executor-memory 25G --driver-memory 4G --conf spark.dynamicAllocation.maxExecutors=15 /var/SP/data/bdpmdses/deliveries_churn/latest/use-cases/churn/analysis/triggers/orders/run_segment_orders.py -c $CLOSING_DAY --rule_nb $RULE_NB -p $PREDICT_DAY $SAVE



