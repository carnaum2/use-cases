#!/bin/bash

num_arguments=1

ruta_ql=/opt/src/hive/vf_postpaid/car/ql
ruta_log=/opt/src/hive/vf_postpaid/car/logs
log_file=PRC_EXP_ENTORNO

echo 'export ruta_log='$ruta_log''
echo -e "\n"
echo 'export log_file='$log_file''

if [ $# -ne $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./PRC_EXP_ENTORNO.sh 201610 "
        echo -e "\n"
        exit -1

else 

valor=$(($1%12))
if [ $valor -eq 1 ]; then 
	arg2=$(($1-89))
else arg2=$(($1-1))
fi

valor=$(($arg2%12))
if [ $valor -eq 1 ]; then 
	arg3=$(($arg2-89))
else arg3=$(($arg2-1))
fi

valor=$(($arg3%12))
if [ $valor -eq 1 ]; then 
	arg4=$(($arg3-89))
else arg4=$(($arg3-1))
fi

valor=$(($arg4%12))
if [ $valor -eq 1 ]; then 
	arg5=$(($arg4-89))
else arg5=$(($arg4-1))
fi

valor=$(($arg5%12))
if [ $valor -eq 1 ]; then 
	arg6=$(($arg5-89))
else arg6=$(($arg5-1))
fi

echo $arg2
echo $arg3
echo $arg4
echo $arg5
echo $arg6


echo 'log4j.rootLogger=INFO, file' > $HOME/hive-log4j_${log_file}.properties
echo '# Direct log messages to a log file' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file=org.apache.log4j.RollingFileAppender' >> $HOME/hive-log4j_${log_file}.properties
echo "log4j.appender.file.File=${ruta_log}/${log_file}.log" >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.MaxFileSize=10MB' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.MaxBackupIndex=10' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.layout=org.apache.log4j.PatternLayout' >> $HOME/hive-log4j_${log_file}.properties
echo 'log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n' >> $HOME/hive-log4j_${log_file}.properties

if [ -f ${ruta_log}/${log_file}.log ]; then
 rm ${ruta_log}/${log_file}.log
fi


start=$SECONDS

hive -hiveconf hive.log4j.file=$HOME/hive-log4j_${log_file}.properties -hiveconf MONTH0=$1 -hiveconf MONTH1=$arg2 -hiveconf MONTH2=$arg3 -hiveconf MONTH3=$arg4 -i $ruta_ql/PRC_EXP_ENTORNO.hql #>> $ruta_log/PRC_EXP_ENTORNO.log

duration=$(($SECONDS))

echo 'Duración: ' $(($duration/60)) 'min.'

rm $HOME/hive-log4j_${log_file}.properties

fi