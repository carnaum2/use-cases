#!/bin/bash

num_arguments=1

ruta_ql=/opt/src/hive/vf_ebu/car/ql
ruta_log=/opt/src/hive/vf_ebu/car/logs
log_file=PRC_EXP_PRODUCTOS

echo 'export ruta_log='$ruta_log''
echo -e "\n"
echo 'export log_file='$log_file''


if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./PRC_EXP_PRODUCTOS.sh 201610"
        echo -e "\n"
        exit -1

else

if [ -f ${ruta_log}/${log_file}.log ]; then
 rm ${ruta_log}/${log_file}.log
fi

# Root logger option
echo 'log4j.rootLogger=INFO, file' > $HOME/hive-log4j_${log_file}.properties
echo '# Direct log messages to a log file' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file=org.apache.log4j.RollingFileAppender' >> $HOME/hive-log4j_${log_file}.properties
echo "log4j.appender.file.File=${ruta_log}/${log_file}.log" >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.MaxFileSize=10MB' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.MaxBackupIndex=10' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.layout=org.apache.log4j.PatternLayout' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n' >> $HOME/hive-log4j_${log_file}.properties


start=$SECONDS

hive -hiveconf hive.log4j.file=$HOME/hive-log4j_${log_file}.properties -hiveconf MONTH0=$1 -i $ruta_ql/PRC_EXP_PRODUCTOS.hql #>> $ruta_log/PRC_EXP_PRODUCTOS.log

duration=$(($SECONDS))

echo 'Duraci�n: ' $(($duration/60)) 'min.'

rm $HOME/hive-log4j_${log_file}.properties

fi