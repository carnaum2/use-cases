#!/bin/bash

num_arguments=1

ruta_ql=/opt/src/hive/vf_postpaid/car/ql
ruta_log=/opt/src/hive/vf_postpaid/car/logs
log_file=PRC_EXP_SEGM_DATOS 

echo 'export ruta_log='$ruta_log''
echo -e "\n"
echo 'export log_file='$log_file''

if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./PRC_EXP_SEGM_DATOS.sh 201610"
        echo -e "\n"
        exit -1

else


year_ref=$1
M0=$year_ref

year=${year_ref:0:4}

# Modificacion si el anio es 0M
if [ ${year_ref:4:1} -eq 0 ];then
	month=${year_ref:5:1}
else 
	month=${year_ref:4:2}
fi
##############

if [ ${month} -lt 10 ]; then
					M0=${year}0${month}
else
					M0=${year}${month}
fi

fi

# Mes anterior
if [ $month -eq 1 ]; then
	month1=12
	year1=$((year-1))
else
	month1=$((month - 1))
	year1=$year

fi

if [ ${month1} -lt 10 ]; then
					M1=${year1}0${month1}
else
					M1=${year1}${month1}
fi

# 2 meses antes

if [ $month1 -eq 1 ]; then
	month2=12
	year2=$((year1-1))
else
	month2=$((month1 - 1))
	year2=$year1

fi

if [ ${month2} -lt 10 ]; then
					M2=${year2}0${month2}
else
					M2=${year2}${month2}
fi

# 3 meses antes

if [ $month2 -eq 1 ]; then
	month3=12
	year3=$((year2-1))
else
	month3=$((month2 - 1))
	year3=$year2
fi

if [ ${month3} -lt 10 ]; then
					M3=${year3}0${month3}
else
					M3=${year3}${month3}
fi



###############################################
echo M0=$M0
echo M1=$M1
echo M2=$M2



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

hive -hiveconf hive.log4j.file=$HOME/hive-log4j_${log_file}.properties -hiveconf MONTH0=$M0 -hiveconf MONTH1=$M1 -hiveconf MONTH2=$M2 -i $ruta_ql/PRC_EXP_SEGM_DATOS.hql #>> $ruta_log/PRC_EXP_SEGM_DATOS.log

duration=$(($SECONDS))

echo 'Duracion: ' $(($duration/60)) 'min.'

rm $HOME/hive-log4j_${log_file}.properties

