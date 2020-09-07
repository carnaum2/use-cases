# !/bin/bash

#set -x #echo on
# En el caso del cocinado mensual, la fecha tiene format YYYYMM

YYYYMMDD=$1
N387_DIR=$2

if [ -z $YYYYMMDD ]
then
   echo
   echo
   echo Run this script from any directory: ./put_tgs_hdfs.sh \<date\> [dir_387]
   echo First argument is the date \(yyyymmdd for weekly and yyyymm for monthly\)
   echo Second argument \(optional\) is the directory to use for downloading the file in 387. Default /tmp/
   echo
   echo
   exit
fi

OK_FILE=$(realpath ${N387_DIR})/PROB_SERV_${YYYYMMDD}.TXT

hdfs dfs -put $OK_FILE /data/udf/vf_es/churn/prob_srv/

echo Moved $OK_FILE to /data/udf/vf_es/churn/prob_srv/ 
