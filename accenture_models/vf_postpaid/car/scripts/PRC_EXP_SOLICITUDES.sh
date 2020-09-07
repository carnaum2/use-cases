#!/bin/bash

num_arguments=1

ruta_ql=/opt/src/hive/vf_postpaid/car/ql
ruta_log=/opt/src/hive/vf_postpaid/car/logs
log_file=PRC_EXP_SOLICITUDES

echo 'export ruta_log='$ruta_log''
echo -e "\n"
echo 'export log_file='$log_file''


if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./PRC_EXP_SOLICITUDES.sh 201610"
        echo -e "\n"
        exit -1

else

year_ref=$1

year=${year_ref:0:4}

if [ ${year_ref:4:1} -eq 0 ];then
        month=${year_ref:5:1}
else
        month=${year_ref:4:2}
fi

if [ $month -lt 4 ]; then

month3=$((month + 9))
year3=$((year-1))

else

month3=$((month - 3))
year3=$year

fi

M0=$year_ref

if [ ${month3} -lt 10 ]; then
					M3=${year3}0${month3}
else
					M3=${year3}${month3}
fi

month12=$month
year12=$((year-1))

if [ ${month12} -lt 10 ]; then
					M12=${year12}0${month12}
else
					M12=${year12}${month12}
fi

echo M0=$M0
echo M3=$M3
echo M12=$M12

# Root logger option
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

hive -hiveconf hive.log4j.file=$HOME/hive-log4j_${log_file}.properties -hiveconf MONTH0=$M0 -hiveconf MONTH3=$M3 -hiveconf MONTH12=$M12 -i $ruta_ql/PRC_EXP_SOLICITUDES.hql #>> $ruta_log/PRC_EXP_SOLICITUDES.log

duration=$(($SECONDS))

echo 'Duraci�n: ' $(($duration/60)) 'min.'

rm $HOME/hive-log4j_${log_file}.properties

fi