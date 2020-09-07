# !/bin/bash
#set -x #echo on

YYYYMM=$1
MAC_DIR=$2
N387_DIR=$3

if [ -z $YYYYMM ]
then
   echo Run this script ./scp_tgs.sh yyyymm [dir_mac] [dir_387]
   echo Second argument \(optional\) is the directory to use for downloading the file in your mac. Default /Users/\<username\>/Downloads/ 
   echo Third argument \(optional\) is the directory to use for downloading the file in 387. Default /tmp/
   exit
fi

if [ -z $MAC_DIR ]
then
    MAC_DIR=/Users/$USER/Downloads/
else
    if [ -d $MAC_DIR ]    
    then 
       echo $MAC_DIR exists!
    else
       echo $MAC_DIR directory does not exist in your mac!
       exit
    fi
fi

if [ -z $N387_DIR ]
then
    N387_DIR=/tmp/
else
    if ssh $USER@milan-discovery-edge-387 '[ -d $N387_DIR ]'   
    then
       echo $N387_DIR exists on 387!
    else
       echo $N387_DIR directory does not exist on 387!
       exit
    fi
fi

echo Script will be run with the following params:
echo    date: $YYYYMM
echo    download dir: $MAC_DIR
echo    node 387 dir: $N387_DIR


OK_FILE=$(realpath ${MAC_DIR})/MATRIZ_PREVEN_BI_${YYYYMM}.TXT


scp $OK_FILE milan-discovery-edge-387:$N387_DIR

echo Now, go to node 387 and run the script ./put_tgs_hdfs.sh $YYYYMM $N387_DIR

