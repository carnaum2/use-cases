#!/bin/bash
pyspark2 --executor-cores 16 \
         --conf spark.driver.port=58010 \
         --conf spark.blockManager.port=58012 \
         --conf spark.broadcast.port=58014 \
         --conf spark.replClassServer.port=58021 \
         --conf spark.ui.port=58112 \
         --executor-memory 32G \
         --driver-memory 64G \
         --conf spark.driver.maxResultSize=192G \
         --conf spark.dynamicAllocation.maxExecutors=256 \
         --conf spark.sql.shuffle.partitions=450 \
         --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
         --queue datascience.high \
         #--conf "spark.driver.extraClassPath=/home/jsotovi2/scala-logging-api_2.11-2.1.2.jar:/home/jsotovi2/graphframes-0.5.0-spark2.1-s_2.11.jar:/home/jsotovi2/scala-logging-slf4j_2.11-2.1.2.jar" \
         #--conf "spark.executor.extraClassPath=/home/jsotovi2/scala-logging-api_2.11-2.1.2.jar:/home/jsotovi2/graphframes-0.5.0-spark2.1-s_2.11.jar:/home/jsotovi2/scala-logging-slf4j_2.11-2.1.2.jar"
