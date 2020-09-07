# !/bin/bash
#set -x #echo on
# En el caso del cocinado mensual, la fecha en realidad es YYYYMM
# NOTE check first if you have the command realpath in your mac. Otherwise, install it: brew install coreutils

YYYYMMDD=$1
MAC_DIR=$2
N387_DIR=$3
SCP_387=$4

if [ -z $YYYYMMDD ]
then
   echo
   echo
   echo Run this script from any directory: ./scp_prob_serv.sh \<date\> [dir_mac] [dir_387] [yes or no] 
   echo <First argument is the date \(yyyymmdd for weekly and yyyymm for monthly\)
   echo Second argument \(optional\) is the directory to use for downloading the file in your mac. Default /Users/\<username\>/Downloads/ 
   echo Third argument \(optional\) is the directory to use for downloading the file in 387. Default /tmp/
   echo Fourth argument \(optional\) determines if the downloaded file has to be moved to 387
   echo
   echo
   exit
fi

echo "date" $YYYYMMDD


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



if [ -z $SCP_387 ]
then
   SCP_387="yes"
fi

echo Script will be run with the following params:
echo    date: $YYYYMMDD
echo    download dir: $MAC_DIR
echo    node 387 dir: $N387_DIR
echo    scp to 387: $SCP_387
LL=${#YYYYMMDD}


# realpath trim the slash at the end if any
KK_FILE=$(realpath ${MAC_DIR})/kk.TXT
FILENAME=PROB_SERV_$YYYYMMDD.TXT
OK_FILE=$(realpath ${MAC_DIR})/$FILENAME

echo    temp file: $KK_FILE
echo    final file: $OK_FILE


if [ $LL -eq 8 ]
then
    scp -oHostKeyAlgorithms=+ssh-dss adstrack@esdwlbhr:/dwh_services/ads_track/bajadas/CVE_FEXP/CSG_PROB_SERV_SEMANAL_$YYYYMMDD.TXT $KK_FILE 
    cat $KK_FILE  | awk -F"|" -v YYYYMMDD=$YYYYMMDD '{if ($1 == YYYYMMDD || $1 == "FECHAEJECUCION") print $0 }' > $OK_FILE
elif [ $LL -eq 6 ]
then
    scp -oHostKeyAlgorithms=+ssh-dss adstrack@esdwlbhr:/dwh_services/ads_track/bajadas/CVE_FEXP/JLL_PROB_SERV_FINAL_LAST.TXT $KK_FILE
    cat $KK_FILE | awk -F"|" -v YYYYMMDD=$YYYYMMDD '{if ($1 == YYYYMMDD || $1 == "ANYOMES") print $0 }' > $OK_FILE
    rm $KK_FILE
fi

NB_LINES=`wc -l $OK_FILE | cut -d " " -f 2`

echo $OK_FILE contains $NB_LINES lines


if [ $NB_LINES -eq 0]
then
    echo ERROR File got from remote machine does not contain info for date $YYYYMMDD
    exit
fi 


if [ "$SCP_387" = "yes" ]
then
   scp $OK_FILE milan-discovery-edge-387:$N387_DIR
   echo Now, go to node 387 and run the script ./put_prob_serv_hdfs.sh $YYYYMMDD $N387_DIR
fi
