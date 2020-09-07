num_arguments=1

ruta_ql=/opt/src/hive/vf_postpaid/car/ql
ruta_log=/opt/src/hive/vf_postpaid/car/logs

if [ $# -ne $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./DO_PAR_QUIMERA.sh 201610 "
        echo -e "\n"
        exit -1

else

start=$SECONDS

#current YearMonth --> Target YearMonth
echo 'current YearMonth: ' $1

hive -hiveconf MONTH0=$1 -i $ruta_ql/DO_PAR_QUIMERA.hql 

duration=$(($SECONDS))
echo 'Duration: ' $(($duration/60)) 'min.'
fi

exit;