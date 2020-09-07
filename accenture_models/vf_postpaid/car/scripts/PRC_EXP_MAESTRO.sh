#!/bin/bash

num_arguments=1

ruta_ql=/opt/src/hive/vf_postpaid/car/ql
ruta_log=/opt/src/hive/vf_postpaid/car/logs
log_file=PRC_EXP_MAESTRO

echo 'export ruta_log='$ruta_log''
echo -e "\n"
echo 'export log_file='$log_file''


if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./PRC_EXP_MAESTRO.sh 201610"
        echo -e "\n"
        exit -1

else

yearmonth=$1

year=${yearmonth:0:4}
if [ ${yearmonth:4:1} -eq 0 ];then
        month=${yearmonth:5:1}
else
        month=${yearmonth:4:2}
fi


if [ $((month - 1)) -lt 1 ]; then

month1=$((month + 11))
year1=$((year-1))

else

month1=$((month - 1))
year1=$year

fi

if [ $((month - 2)) -lt 1 ]; then

month2=$((month + 10))
year2=$((year-1))

else

month2=$((month - 2))
year2=$year

fi

if [ $((month - 3)) -lt 1 ]; then

month3=$((month + 9))
year3=$((year-1))

else

month3=$((month - 3))
year3=$year

fi

if [ $((month - 4)) -lt 1 ]; then

month4=$((month + 8))
year4=$((year-1))

else

month4=$((month - 4))
year4=$year

fi

if [ $((month - 5)) -lt 1 ]; then

month5=$((month + 7))
year5=$((year-1))

else

month5=$((month - 5))
year5=$year

fi

if [ $((month - 6)) -lt 1 ]; then

month6=$((month + 6))
year6=$((year-1))

else

month6=$((month - 6))
year6=$year

fi



M0=$yearmonth

if [ ${month1} -lt 10 ]; then
					M1=${year1}0${month1}
else
					M1=${year1}${month1}
fi

if [ ${month2} -lt 10 ]; then
					M2=${year2}0${month2}
else
					M2=${year2}${month2}
fi

if [ ${month3} -lt 10 ]; then
					M3=${year3}0${month3}
else
					M3=${year3}${month3}
fi

if [ ${month4} -lt 10 ]; then
					M4=${year4}0${month4}
else
					M4=${year4}${month4}
fi

if [ ${month5} -lt 10 ]; then
					M5=${year5}0${month5}
else
					M5=${year5}${month5}
fi

if [ ${month6} -lt 10 ]; then
					M6=${year6}0${month6}
else
					M6=${year6}${month6}
fi

echo M0=$M0
echo M1=$M1
echo M2=$M2
echo M3=$M3
echo M4=$M4
echo M5=$M5
echo M6=$M6


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

####Explicativas individuales
#$ruta_scr/PRC_EXP_INDIVIDUALES.sh $M0

hive -hiveconf hive.log4j.file=$HOME/hive-log4j_${log_file}.properties -hiveconf MONTH0=$M0 -hiveconf MONTH1=$M1 -hiveconf MONTH2=$M2 -hiveconf MONTH3=$M3 -hiveconf MONTH4=$M4 -hiveconf MONTH5=$M5 -hiveconf MONTH6=$M6 -i $ruta_ql/PRC_EXP_MAESTRO.hql #>> $ruta_log/PRC_EXP_MAESTRO.log

duration=$(($SECONDS))

echo 'Duración: ' $(($duration/60)) 'min.'

rm $HOME/hive-log4j_${log_file}.properties

fi
