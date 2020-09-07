#!/bin/bash

num_arguments=1

ruta_scr=/opt/src/hive/vf_postpaid/car/scripts


if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./scr_PRC_EXP_INDIVIDUALES.sh 201610"
        echo -e "\n"
        exit -1

else

MONTH0=$1

$ruta_scr/PRC_EXP_AC_FINAL.sh $MONTH0
$ruta_scr/PRC_EXP_AC_HOGAR.sh $MONTH0
$ruta_scr/PRC_EXP_ACT_DESACT.sh $MONTH0
$ruta_scr/PRC_EXP_ARPU_CLI.sh $MONTH0
$ruta_scr/PRC_EXP_DTOS_PPALES.sh $MONTH0
$ruta_scr/PRC_EXP_ENTORNO.sh $MONTH0
$ruta_scr/PRC_EXP_KXEN_GPRS.sh $MONTH0
$ruta_scr/PRC_EXP_KXEN_GSM.sh $MONTH0
$ruta_scr/PRC_EXP_TACFAC.hql $MONTH0
$ruta_scr/PRC_EXP_LOYALTY.sh $MONTH0
$ruta_scr/PRC_EXP_MIVF.sh $MONTH0
$ruta_scr/PRC_EXP_MOSAIC.sh $MONTH0
$ruta_scr/PRC_EXP_PERMA_PROD.sh $MONTH0
$ruta_scr/PRC_EXP_TARIFICADOR.sh $MONTH0
$ruta_scr/PRC_EXP_NUEVAS_EXPLICATIVAS.sh $MONTH0
$ruta_scr/PRC_EXP_FINANCIACION.sh $MONTH0
$ruta_scr/PRC_EXP_SEGM_DATOS.sh $MONTH0
$ruta_scr/PRC_EXP_SOLICITUDES.sh $MONTH0
$ruta_scr/PRC_EXP_USAC.sh $MONTH0
$ruta_scr/PRC_SEG_ARPU_EDAD.sh $MONTH0
$ruta_scr/PRC_SEGM_TIPO_CLIENTE_LIN_PROD.sh $MONTH0
#$ruta_scr/SEG_TECNOLOGIA.sh $MONTH0




fi