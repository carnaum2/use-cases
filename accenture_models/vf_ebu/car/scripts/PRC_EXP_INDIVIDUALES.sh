#!/bin/bash

num_arguments=1

ruta_scr=/opt/src/hive/vf_ebu/car/scripts


if [ $# -lt $num_arguments ]; then
        echo -e "\n"
	echo 'Number of Arguments ' $#
	echo '1. Pair YearMonth without spaces'
		
        echo ">>>>>>>ERROR<<<<<<< Example: ./scr_PRC_EXP_INDIVIDUALES.sh 201610"
        echo -e "\n"
        exit -1

else

MONTH0=$1

$ruta_scr/PRC_EXP_CLIENTE.sh $MONTH0
$ruta_scr/PRC_EXP_PERMANENCIA.sh $MONTH0
$ruta_scr/PRC_EXP_PRODUCTOS.sh $MONTH0
$ruta_scr/PRC_EXP_USAC.sh $MONTH0
$ruta_scr/PRC_EXP_ENTORNO.sh $MONTH0
$ruta_scr/PRC_EXP_TACFAC.sh $MONTH0
$ruta_scr/PRC_EXP_ARPU.sh $MONTH0
$ruta_scr/PRC_EXP_KXEN_GSM.sh $MONTH0
$ruta_scr/PRC_EXP_KXEN_GPRS.sh $MONTH0
$ruta_scr/PRC_EXP_LINEAS_VOZ.sh $MONTH0
$ruta_scr/PRC_EXP_POTEX.sh $MONTH0
$ruta_scr/PRC_EXP_CARTERA_FIJO.sh $MONTH0
$ruta_scr/PRC_EXP_PRODUCTOS_CUENTA.sh $MONTH0
$ruta_scr/PRC_EXP_TNPS.sh $MONTH0
$ruta_scr/PRC_EXP_FINAN_CLI.sh $MONTH0
$ruta_scr/PRC_EXP_FINANCIACION.sh $MONTH0
$ruta_scr/PRC_EXP_CASOS.sh $MONTH0
#$ruta_scr/PRC_EXP_PERM_VENTA_DIR.sh $MONTH0
$ruta_scr/PRC_EXP_VELOCIDAD_ADSL.sh $MONTH0
$ruta_scr/PRC_EXP_TV.sh $MONTH0
$ruta_scr/PRC_EXP_ILIMITADA.sh $MONTH0
$ruta_scr/PRC_EXP_CONVERGENTE.sh $MONTH0
$ruta_scr/PRC_EXP_SECTOR_EMPLEADOS.sh $MONTH0
$ruta_scr/PRC_EXP_SEDES.sh $MONTH0
$ruta_scr/PRC_EXP_EMAIL.sh $MONTH0



fi