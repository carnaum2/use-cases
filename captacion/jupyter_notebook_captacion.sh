#!/bin/bash
pyspark2 --name "Spain CDRs Graph Processing" \
         --executor-cores 16 \
         --conf spark.driver.port=58010 \
         --conf spark.blockManager.port=58012 \
         --conf spark.broadcast.port=58014 \
         --conf spark.replClassServer.port=58021 \
         --conf spark.ui.port=58112 \
         --executor-memory 96G \
         --driver-memory 96G \
         --conf spark.driver.maxResultSize=88G \
         --conf spark.dynamicAllocation.maxExecutors=16 \
         --conf spark.sql.shuffle.partitions=360 \
         --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
         --conf spark.rpc.askTimeout=50000 \
         --conf spark.yarn.executor.memoryOverhead=10000 \
         --conf spark.network.timeout=5500 \
         --queue datascience.high \
         --jars /var/SP/data/bdpmdses/rbuendi1/captacion_julio/bda-core-complete-assembly-2.0.0.jar \
         --py-files /var/SP/data/bdpmdses/rbuendi1/captacion_julio/graphframes.zip
