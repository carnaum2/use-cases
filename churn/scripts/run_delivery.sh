# !/bin/bash

CLOSING_DAY=$1
TR_INI=$2
TR_END=$3

if [ -z $CLOSING_DAY ]
then
   echo
   echo
   echo Run this script from any directory: ./run_delivery.sh yyyymmmdd
   echo First argument is the date with format yyyymmdd closing_day
   echo OPTIONAL Second argument with TR_INI
   echo OPTIONAL Third argument with TR_END
   echo
   exit
fi

if [ -z "$TR_INI" ] && [ ! -z "$TR_END" ]
then
  echo "If tr_end is specified, then tr_ini must also be specified"
  exit
fi

if [ ! -z "$TR_INI" ] && [ -z "$TR_END" ]
then
  echo "If tr_ini is specified, then tr_end must also be specified"
  exit
fi


USERNAME=`whoami`
LOGGING_DIR=/var/SP/data/home/${USERNAME}/logging/
LOGGING_FILE=${LOGGING_DIR}/delivery_${CLOSING_DAY}.log

if [ ! -d $LOGGING_DIR ]
then
     mkdir $LOGGING_DIR
     echo "Creating " $LOGGING_DIR
else
     echo "Logging directory " $LOGGING_DIR " exists"
fi

echo CLOSING_DAY $CLOSING_DAY
echo TR_INI "$TR_INI"
echo TR_END "$TR_END"
echo "USERNAME " $USERNAME
echo "LOGGING TO FILE " $LOGGING_FILE

if [ -z $TR_INI ]
then
	echo "Running delivery for closing_day " $CLOSING_DAY " Logging file "  $LOGGING_FILE
	spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=32 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 --conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 --driver-java-options="-Droot.logger=WARN,console" --jars /var/SP/data/bdpmdses/jmarcoso/apps/churnmodel/AutomChurn-assembly-0.12.jar latest/use-cases/churn/delivery/churn_delivery_master_threads.py -c ${CLOSING_DAY} --delivery 2>&1 | tee ${LOGGING_FILE}

else
  echo "Running delivery for closing_day " $CLOSING_DAY " tr_ini=" $TR_INI " tr_end=" $TR_END " Logging file "  $LOGGING_FILE
	spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=32 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 --conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 --driver-java-options="-Droot.logger=WARN,console" --jars /var/SP/data/bdpmdses/jmarcoso/apps/churnmodel/AutomChurn-assembly-0.12.jar latest/use-cases/churn/delivery/churn_delivery_master_threads.py -c ${CLOSING_DAY} --tr_ini $TR_INI --tr_end $TR_END --delivery 2>&1 | tee ${LOGGING_FILE}

fi

