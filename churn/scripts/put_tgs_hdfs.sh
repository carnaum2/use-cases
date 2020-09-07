# !/bin/bash

#set -x #echo on


YYYYMM=$1
N387_DIR=$2

if [ -z $YYYYMM ]
then
   echo
   echo
   echo Run this script from any directory: ./put_tgs_hdfs.sh yyyymm [dir_387]
   echo First argument is the date with format yyyymm 
   echo Second argument \(optional\) is the directory to use for downloading the file in 387. Default /tmp/
   echo
   echo
   exit
fi

if [ -z $N387_DIR ]
then
    N387_DIR=/tmp/
else
    if [ -d $N387_DIR ]    
    then 
       echo $N387_DIR exists!
    else
       echo $N387_DIR directory does not exist!
       exit
    fi
fi




OK_FILE=$(realpath ${N387_DIR})/MATRIZ_PREVEN_BI_${YYYYMM}.TXT 

hdfs dfs -put $OK_FILE /data/udf/vf_es/churn/tgs/

echo Copied file $OK_FILE to hdfs dir /data/udf/vf_es/churn/tgs/

