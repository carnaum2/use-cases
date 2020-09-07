# !/bin/bash



for CLOSING_DAY in 20190331 20190407 20190414 20190421 20190430 20190307 20190314 20190321  
do

   echo "Running delivery for closing_day " $CLOSING_DAY
   spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=8 --conf spark.dynamicAllocation.maxExecutors=16 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 --conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 --driver-java-options="-Droot.logger=WARN,console" --jars /var/SP/data/bdpmdses/jmarcoso/apps/churnmodel/AutomChurn-assembly-0.12.jar latest/use-cases/churn/delivery/churn_delivery_master_threads.py -c ${CLOSING_DAY} --historic --delivery 2>&1 | tee /var/SP/data/home/csanc109/logging/delivery_${CLOSING_DAY}.log

done



