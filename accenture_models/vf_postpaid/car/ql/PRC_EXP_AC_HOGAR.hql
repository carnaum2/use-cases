USE VF_POSTPAID;


set mapred.job.name = WORK_HH_MAESTRO;
DROP TABLE WORK.WORK_HH_MAESTRO;
CREATE TABLE WORK.WORK_HH_MAESTRO AS
SELECT
B.COD_GOLDEN AS COD_GOLDEN,
A.X_NUM_IDENT AS NIF,
A.X_ID_RED AS MSISDN
FROM
(SELECT DISTINCT X_ID_RED, X_NUM_IDENT  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT X_ID_RED AS MSISDN, MAX(CASE WHEN CAST(COD_GOLDEN AS BIGINT) IS NULL THEN -99999 ELSE CAST(COD_GOLDEN AS BIGINT) END) AS COD_GOLDEN
FROM INPUT.VF_HH_VFPOST  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}' GROUP BY X_ID_RED) B
ON A.X_ID_RED = B.MSISDN;


set mapred.job.name = PAR_EXP_AC_HOGAR_LIN;
DROP TABLE VF_POSTPAID.PAR_EXP_AC_HOGAR_LIN;
CREATE TABLE VF_POSTPAID.PAR_EXP_AC_HOGAR_LIN AS
SELECT
A.NIF AS NIF,
A.MSISDN AS MSISDN,
MAX(CASE WHEN A.FLAG_HUELLA_ONO IS NULL THEN 0 ELSE A.FLAG_HUELLA_ONO END) AS  IND_HUELLA_ONO_HH,
MAX(CASE WHEN A.FLAG_HUELLA_VDF IS NULL THEN 0 ELSE A.FLAG_HUELLA_VDF END) AS  IND_HUELLA_VDF_HH,
SUM(CASE WHEN A.NUM_CLIENTES IS NULL THEN 0 ELSE A.NUM_CLIENTES END) AS  NUM_CLIENTES_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_CALC IS NULL THEN 0 ELSE A.NUM_SERVICIOS_CALC END) AS  NUM_SERVICIOS_CALC_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_CONTRATO IS NULL THEN 0 ELSE A.NUM_SERVICIOS_CONTRATO END) AS   NUM_SERV_CONTRATO_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_PARTICULARES IS NULL THEN 0 ELSE A.NUM_SERVICIOS_PARTICULARES END) AS  NUM_SERV_PARTICULARES_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_AUTONOMO IS NULL THEN 0 ELSE A.NUM_SERVICIOS_AUTONOMO END) AS  NUM_SERV_AUTONOMO_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_EMPRESA IS NULL THEN 0 ELSE A.NUM_SERVICIOS_EMPRESA END) AS  NUM_SERV_EMPRESA_HH,
SUM(CASE WHEN A.NUM_SERVICIOS_PREPAGO IS NULL THEN 0 ELSE A.NUM_SERVICIOS_PREPAGO END) AS  NUM_SERV_PREPAGO_HH,
SUM(CASE WHEN A.NUM_LINEAS_TELE2 IS NULL THEN 0 ELSE A.NUM_LINEAS_TELE2 END) AS  NUM_LINEAS_TELE2_HH,
MAX(CASE WHEN A.HOGAR_COMPARTIDO_ONO IS NULL THEN 0 ELSE A.HOGAR_COMPARTIDO_ONO END) AS  IND_HOGAR_COMPARTIDO_HH,
SUM(CASE WHEN A.VALOR_PREPAGO IS NULL THEN 0 ELSE A.VALOR_PREPAGO END) AS  NUM_VALOR_PREP_HH,
SUM(CASE WHEN A.VALOR_PREPAGO_3M IS NULL THEN 0 ELSE A.VALOR_PREPAGO_3M END) AS  NUM_VALOR_PREP_3M_HH,
SUM(CASE WHEN A.VALOR_POSPAGO IS NULL THEN 0 ELSE A.VALOR_POSPAGO END) AS  NUM_VALOR_POS_HH,
SUM(CASE WHEN A.VALOR_POSPAGO_3M IS NULL THEN 0 ELSE A.VALOR_POSPAGO_3M END) AS  NUM_VALOR_POS_3M_HH,
SUM(CASE WHEN A.VALOR_HOGAR IS NULL THEN 0 ELSE A.VALOR_HOGAR END) AS  NUM_VALOR_HOGAR_HH,
SUM(CASE WHEN A.VALOR_HOGAR_3M IS NULL THEN 0 ELSE A.VALOR_HOGAR_3M END) AS  NUM_VALOR_HOGAR_3M_HH,
MAX(CASE WHEN A.FLAGLPD IS NULL THEN 0 ELSE A.FLAGLPD END) AS  IND_LPD_HH,
MAX(CASE WHEN A.FLAGHZ IS NULL THEN 0 ELSE A.FLAGHZ END) AS  IND_HZ_HH,
MAX(CASE WHEN A.FLAGADSL IS NULL THEN 0 ELSE A.FLAGADSL END) AS  IND_ADSL_HH,
MAX(CASE WHEN A.FLAGFTTH IS NULL THEN 0 ELSE A.FLAGFTTH END) AS  IND_FTTH_HH,
SUM(CASE WHEN A.NUM_LINEAS_RED IS NULL THEN 0 ELSE A.NUM_LINEAS_RED END) AS  NUM_LINEAS_RED_HH,
MAX(CASE WHEN A.CONVERGENTE IS NULL THEN 0 ELSE A.CONVERGENTE END) AS  IND_CONVERGENTE_HH,
SUM(CASE WHEN A.CLIENTES_MENOR_25 IS NULL THEN 0 ELSE A.CLIENTES_MENOR_25 END) AS  NUM_CLIENTES_MENOR_25_HH,
SUM(CASE WHEN A.CLIENTES_25_A_35 IS NULL THEN 0 ELSE A.CLIENTES_25_A_35 END) AS  NUM_CLIENTES_25_A_35_HH,
SUM(CASE WHEN A.CLIENTES_35_A_45 IS NULL THEN 0 ELSE A.CLIENTES_35_A_45 END) AS  NUM_CLIENTES_35_A_45_HH,
SUM(CASE WHEN A.CLIENTES_45_A_55 IS NULL THEN 0 ELSE A.CLIENTES_45_A_55 END) AS  NUM_CLIENTES_45_A_55_HH,
SUM(CASE WHEN A.CLIENTE_55_A_65 IS NULL THEN 0 ELSE A.CLIENTE_55_A_65 END) AS  NUM_CLIENTE_55_A_65_HH,
SUM(CASE WHEN A.CLIENTE_MAYOR_65 IS NULL THEN 0 ELSE A.CLIENTE_MAYOR_65 END) AS  NUM_CLIENTE_MAYOR_65_HH,
SUM(CASE WHEN A.CLIENTE_EDAD_DESCONOCIDA IS NULL THEN 0 ELSE A.CLIENTE_EDAD_DESCONOCIDA END) AS  NUM_CLI_EDAD_DESCONOCIDA_HH,
MAX(CASE WHEN A.EDAD_MEDIA IS NULL THEN 0 ELSE A.EDAD_MEDIA END) AS  NUM_EDAD_MEDIA_HH,
SUM(CASE WHEN A.CLIENTES_ES IS NULL THEN 0 ELSE A.CLIENTES_ES END) AS  NUM_CLIENTES_ES_HH,
SUM(CASE WHEN A.CLIENTES_EXTRANJERO IS NULL THEN 0 ELSE A.CLIENTES_EXTRANJERO END) AS  NUM_CLI_EXTRANJERO_HH,
SUM(CASE WHEN A.CLIENTE_NACIONALIDAD_DESC IS NULL THEN 0 ELSE A.CLIENTE_NACIONALIDAD_DESC END) AS  NUM_CLI_NACIONALIDAD_DESC_HH,
SUM(CASE WHEN A.HOMBRES IS NULL THEN 0 ELSE A.HOMBRES END) AS  NUM_HOMBRES_HH,
SUM(CASE WHEN A.MUJERES IS NULL THEN 0 ELSE A.MUJERES END) AS  NUM_MUJERES_HH,
SUM(CASE WHEN A.CLIENTE_SEXO_DESCONOCIDO IS NULL THEN 0 ELSE A.CLIENTE_SEXO_DESCONOCIDO END) AS  NUM_CLIENTE_SEXO_DESC_HH,
SUM(CASE WHEN A.NUM_DISPOSITIVOS IS NULL THEN 0 ELSE A.NUM_DISPOSITIVOS END) AS  NUM_DISPOSITIVOS_HH,
SUM(CASE WHEN A.NUM_DISPOSITIVOS_MOVIL IS NULL THEN 0 ELSE A.NUM_DISPOSITIVOS_MOVIL END) AS  NUM_DISPOSITIVOS_MOVIL_HH,
SUM(CASE WHEN A.NUM_DISPOSITIVOS_OTRO IS NULL THEN 0 ELSE A.NUM_DISPOSITIVOS_OTRO END) AS  NUM_DISPOSITIVOS_OTRO_HH,
SUM(CASE WHEN A.NUM_SMARTPHONE IS NULL THEN 0 ELSE A.NUM_SMARTPHONE END) AS  NUM_SMARTPHONE_HH,
SUM(CASE WHEN A.NUM_TABLET IS NULL THEN 0 ELSE A.NUM_TABLET END) AS  NUM_TABLET_HH,
SUM(CASE WHEN A.NUM_MODEM_USB IS NULL THEN 0 ELSE A.NUM_MODEM_USB END) AS  NUM_MODEM_USB_HH,
SUM(CASE WHEN A.NUM_ANDROID IS NULL THEN 0 ELSE A.NUM_ANDROID END) AS  NUM_ANDROID_HH,
SUM(CASE WHEN A.NUM_IOS IS NULL THEN 0 ELSE A.NUM_IOS END) AS  NUM_IOS_HH,
SUM(CASE WHEN A.NUM_DISP_SOPORTAN_4G IS NULL THEN 0 ELSE A.NUM_DISP_SOPORTAN_4G END) AS  NUM_DISP_SOPORTAN_4G_HH,
SUM(CASE WHEN A.NUM_DISP_ACTIVO_4G IS NULL THEN 0 ELSE A.NUM_DISP_ACTIVO_4G END) AS  NUM_DISP_ACTIVO_4G_HH,
SUM(CASE WHEN A.MOU_CALC IS NULL THEN 0 ELSE A.MOU_CALC END) AS  NUM_MOU_CALC_HH,
MAX(CASE WHEN A.MEDIA_MOU IS NULL THEN 0 ELSE A.MEDIA_MOU END) AS  NUM_MEDIA_MOU_HH,
SUM(CASE WHEN A.DATOSTOTAL_MB_CALC IS NULL THEN 0 ELSE A.DATOSTOTAL_MB_CALC END) AS  NUM_DATOSTOT_MB_CALC_HH,
MAX(CASE WHEN A.MEDIA_DATOSTOTAL_MB IS NULL THEN 0 ELSE A.MEDIA_DATOSTOTAL_MB END) AS  NUM_MEDIA_DATOSTOT_MB_HH,
SUM(CASE WHEN A.MOU_FIXED IS NULL THEN 0 ELSE A.MOU_FIXED END) AS  NUM_MOU_FIXED,
SUM(CASE WHEN A.NUM_LLAMADAS_CALC IS NULL THEN 0 ELSE A.NUM_LLAMADAS_CALC END) AS  NUM_LLAMADAS_CALC_HH,
MAX(CASE WHEN A.MEDIA_LLAMADAS IS NULL THEN 0 ELSE A.MEDIA_LLAMADAS END) AS  NUM_MEDIA_LLAMADAS_HH,
SUM(CASE WHEN A.LLAMADAS_FIXED IS NULL THEN 0 ELSE A.LLAMADAS_FIXED END) AS  NUM_LLAMADAS_FIXED_HH,
MAX(CASE WHEN A.IND_RIESGO IS NULL THEN 0 ELSE A.IND_RIESGO END) AS  IND_RIESGO_HH,
MAX(CASE WHEN A.IND_RIESGO_PROD IS NULL THEN 0 ELSE A.IND_RIESGO_PROD END) AS  IND_RIESGO_PROD_HH,
MAX(CASE WHEN A.IND_CROSS_UPSELL IS NULL THEN 0 ELSE A.IND_CROSS_UPSELL END) AS  IND_CROSS_UPSELL_HH,
SUM(CASE WHEN A.SOLICITUDES_PORTABILIDAD_POS IS NULL THEN 0 ELSE A.SOLICITUDES_PORTABILIDAD_POS END) AS  NUM_SOLIC_PORTA_POS_HH,
SUM(CASE WHEN A.SOLICITUDES_PORTABILIDAD_PRE IS NULL THEN 0 ELSE A.SOLICITUDES_PORTABILIDAD_PRE END) AS  NUM_SOLIC_PORTA_PRE_HH,
SUM(CASE WHEN A.INTERACCIONES_INSATISF IS NULL THEN 0 ELSE A.INTERACCIONES_INSATISF END) AS  NUM_INTERAC_INSATISF_HH,
SUM(CASE WHEN A.NUM_DESACTIVACIONES_POS IS NULL THEN 0 ELSE A.NUM_DESACTIVACIONES_POS END) AS  NUM_DESACT_POS_HH,
SUM(CASE WHEN A.NUM_DESACTIVACIONES_PRE IS NULL THEN 0 ELSE A.NUM_DESACTIVACIONES_PRE END) AS  NUM_DESACT_PRE_HH,
SUM(CASE WHEN A.MIGRACIONES_PRE_POS IS NULL THEN 0 ELSE A.MIGRACIONES_PRE_POS END) AS  NUM_MIGRAS_PRE_POS_HH,
SUM(CASE WHEN A.MIGRACIONES_POS_PRE IS NULL THEN 0 ELSE A.MIGRACIONES_POS_PRE END) AS  NUM_MIGRAS_POS_PRE_HH,
SUM(CASE WHEN A.SERV_VENTANA_CANJE IS NULL THEN 0 ELSE A.SERV_VENTANA_CANJE END) AS  NUM_SERV_VENTANA_CANJE_HH,
MAX(A.TIPO_HOGAR) AS SEG_TIPO_HOGAR,
MAX(CASE WHEN A.FLAG_HUELLA_ONO IS NULL THEN 0 ELSE A.FLAG_HUELLA_ONO END + 
CASE WHEN A.FLAG_HUELLA_VDF IS NULL THEN 0 ELSE A.FLAG_HUELLA_VDF END) AS IND_HUELLA_ONO_VF_HH
FROM
(SELECT
A.NIF,
A.MSISDN,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAG_HUELLA_ONO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAG_HUELLA_ONO,',','.')) AS INT) END AS FLAG_HUELLA_ONO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAG_HUELLA_VDF,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAG_HUELLA_VDF,',','.')) AS INT) END AS FLAG_HUELLA_VDF,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_CLIENTES,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_CLIENTES,',','.')) AS INT) END AS NUM_CLIENTES,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_CALC,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_CALC,',','.')) AS INT) END AS NUM_SERVICIOS_CALC,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_CONTRATO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_CONTRATO,',','.')) AS INT) END AS NUM_SERVICIOS_CONTRATO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_PARTICULARES,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_PARTICULARES,',','.')) AS INT) END AS NUM_SERVICIOS_PARTICULARES,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_AUTONOMO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_AUTONOMO,',','.')) AS INT) END AS NUM_SERVICIOS_AUTONOMO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_EMPRESA,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_EMPRESA,',','.')) AS INT) END AS NUM_SERVICIOS_EMPRESA,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_PREPAGO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SERVICIOS_PREPAGO,',','.')) AS INT) END AS NUM_SERVICIOS_PREPAGO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LINEAS_TELE2,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LINEAS_TELE2,',','.')) AS INT) END AS NUM_LINEAS_TELE2,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.HOGAR_COMPARTIDO_ONO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.HOGAR_COMPARTIDO_ONO,',','.')) AS INT) END AS HOGAR_COMPARTIDO_ONO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_PREPAGO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_PREPAGO,',','.')) AS INT) END AS VALOR_PREPAGO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_PREPAGO_3M,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_PREPAGO_3M,',','.')) AS INT) END AS VALOR_PREPAGO_3M,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_POSPAGO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_POSPAGO,',','.')) AS INT) END AS VALOR_POSPAGO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_POSPAGO_3M,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_POSPAGO_3M,',','.')) AS INT) END AS VALOR_POSPAGO_3M,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_HOGAR,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_HOGAR,',','.')) AS INT) END AS VALOR_HOGAR,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_HOGAR_3M,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.VALOR_HOGAR_3M,',','.')) AS INT) END AS VALOR_HOGAR_3M,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAGLPD,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAGLPD,',','.')) AS INT) END AS FLAGLPD,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAGHZ,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAGHZ,',','.')) AS INT) END AS FLAGHZ,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAGADSL,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAGADSL,',','.')) AS INT) END AS FLAGADSL,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.FLAGFTTH,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.FLAGFTTH,',','.')) AS INT) END AS FLAGFTTH,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LINEAS_RED,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LINEAS_RED,',','.')) AS INT) END AS NUM_LINEAS_RED,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CONVERGENTE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CONVERGENTE,',','.')) AS INT) END AS CONVERGENTE,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_MENOR_25,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_MENOR_25,',','.')) AS INT) END AS CLIENTES_MENOR_25,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_25_A_35,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_25_A_35,',','.')) AS INT) END AS CLIENTES_25_A_35,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_35_A_45,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_35_A_45,',','.')) AS INT) END AS CLIENTES_35_A_45,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_45_A_55,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_45_A_55,',','.')) AS INT) END AS CLIENTES_45_A_55,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_55_A_65,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_55_A_65,'.',', ')) AS INT) END AS CLIENTE_55_A_65,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_MAYOR_65,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_MAYOR_65,',','.')) AS INT) END AS CLIENTE_MAYOR_65,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_EDAD_DESCONOCIDA,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_EDAD_DESCONOCIDA,',','.')) AS INT) END AS CLIENTE_EDAD_DESCONOCIDA,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.EDAD_MEDIA,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.EDAD_MEDIA,',','.')) AS INT) END AS EDAD_MEDIA,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_ES,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_ES,',','.')) AS INT) END AS CLIENTES_ES,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_EXTRANJERO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTES_EXTRANJERO,',','.')) AS INT) END AS CLIENTES_EXTRANJERO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_NACIONALIDAD_DESC,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_NACIONALIDAD_DESC,',','.')) AS INT) END AS CLIENTE_NACIONALIDAD_DESC,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.HOMBRES,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.HOMBRES,',','.')) AS INT) END AS HOMBRES,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MUJERES,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MUJERES,',','.')) AS INT) END AS MUJERES,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_SEXO_DESCONOCIDO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.CLIENTE_SEXO_DESCONOCIDO,',','.')) AS INT) END AS CLIENTE_SEXO_DESCONOCIDO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS,',','.')) AS INT) END AS NUM_DISPOSITIVOS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS_MOVIL,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS_MOVIL,',','.')) AS INT) END AS NUM_DISPOSITIVOS_MOVIL,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS_OTRO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISPOSITIVOS_OTRO,',','.')) AS INT) END AS NUM_DISPOSITIVOS_OTRO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SMARTPHONE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_SMARTPHONE,',','.')) AS INT) END AS NUM_SMARTPHONE,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_TABLET,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_TABLET,',','.')) AS INT) END AS NUM_TABLET,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_MODEM_USB,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_MODEM_USB,',','.')) AS INT) END AS NUM_MODEM_USB,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_ANDROID,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_ANDROID,',','.')) AS INT) END AS NUM_ANDROID,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_IOS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_IOS,',','.')) AS INT) END AS NUM_IOS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISP_SOPORTAN_4G,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISP_SOPORTAN_4G,',','.')) AS INT) END AS NUM_DISP_SOPORTAN_4G,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISP_ACTIVO_4G,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DISP_ACTIVO_4G,',','.')) AS INT) END AS NUM_DISP_ACTIVO_4G,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MOU_CALC,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MOU_CALC,',','.')) AS INT) END AS MOU_CALC,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_MOU,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_MOU,',','.')) AS INT) END AS MEDIA_MOU,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.DATOSTOTAL_MB_CALC,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.DATOSTOTAL_MB_CALC,',','.')) AS INT) END AS DATOSTOTAL_MB_CALC,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_DATOSTOTAL_MB,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_DATOSTOTAL_MB,',','.')) AS INT) END AS MEDIA_DATOSTOTAL_MB,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MOU_FIXED,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MOU_FIXED,',','.')) AS INT) END AS MOU_FIXED,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LLAMADAS_CALC,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_LLAMADAS_CALC,',','.')) AS INT) END AS NUM_LLAMADAS_CALC,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_LLAMADAS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MEDIA_LLAMADAS,',','.')) AS INT) END AS MEDIA_LLAMADAS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.LLAMADAS_FIXED,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.LLAMADAS_FIXED,',','.')) AS INT) END AS LLAMADAS_FIXED,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.IND_RIESGO,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.IND_RIESGO,',','.')) AS INT) END AS IND_RIESGO,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.IND_RIESGO_PROD,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.IND_RIESGO_PROD,',','.')) AS INT) END AS IND_RIESGO_PROD,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.IND_CROSS_UPSELL,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.IND_CROSS_UPSELL,',','.')) AS INT) END AS IND_CROSS_UPSELL,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.SOLICITUDES_PORTABILIDAD_POS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.SOLICITUDES_PORTABILIDAD_POS,',','.')) AS INT) END AS SOLICITUDES_PORTABILIDAD_POS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.SOLICITUDES_PORTABILIDAD_PRE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.SOLICITUDES_PORTABILIDAD_PRE,',','.')) AS INT) END AS SOLICITUDES_PORTABILIDAD_PRE,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.INTERACCIONES_INSATISF,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.INTERACCIONES_INSATISF,',','.')) AS INT) END AS INTERACCIONES_INSATISF,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DESACTIVACIONES_POS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DESACTIVACIONES_POS,',','.')) AS INT) END AS NUM_DESACTIVACIONES_POS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DESACTIVACIONES_PRE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.NUM_DESACTIVACIONES_PRE,',','.')) AS INT) END AS NUM_DESACTIVACIONES_PRE,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MIGRACIONES_PRE_POS,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MIGRACIONES_PRE_POS,',','.')) AS INT) END AS MIGRACIONES_PRE_POS,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.MIGRACIONES_POS_PRE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.MIGRACIONES_POS_PRE,',','.')) AS INT) END AS MIGRACIONES_POS_PRE,
CASE WHEN CAST(TRIM(REGEXP_REPLACE(B.SERV_VENTANA_CANJE,',','.')) AS DOUBLE) IS NOT NULL  THEN CAST(TRIM(REGEXP_REPLACE(B.SERV_VENTANA_CANJE,',','.')) AS INT) END AS SERV_VENTANA_CANJE,
B.TIPO_HOGAR
FROM
WORK.WORK_HH_MAESTRO A
LEFT JOIN
(SELECT * FROM INPUT.VF_HH_AC_HOGAR WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') B
ON A.COD_GOLDEN = B.COD_GOLDEN) A 
GROUP BY A.MSISDN, A.NIF;


set mapred.job.name = PAR_EXP_AC_HOGAR_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_AC_HOGAR_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_AC_HOGAR_CLI AS
SELECT
NIF,
MAX(IND_HUELLA_ONO_HH) AS  IND_HUELLA_ONO_HH,
MAX(IND_HUELLA_VDF_HH) AS  IND_HUELLA_VDF_HH,
MAX(NUM_CLIENTES_HH) AS  NUM_CLIENTES_HH,
SUM(NUM_SERVICIOS_CALC_HH) AS  NUM_SERVICIOS_CALC_HH,
SUM(NUM_SERV_CONTRATO_HH) AS   NUM_SERV_CONTRATO_HH,
SUM(NUM_SERV_PARTICULARES_HH) AS  NUM_SERV_PARTICULARES_HH,
SUM(NUM_SERV_AUTONOMO_HH) AS  NUM_SERV_AUTONOMO_HH,
SUM(NUM_SERV_EMPRESA_HH) AS  NUM_SERV_EMPRESA_HH,
SUM(NUM_SERV_PREPAGO_HH) AS  NUM_SERV_PREPAGO_HH,
SUM(NUM_LINEAS_TELE2_HH) AS  NUM_LINEAS_TELE2_HH,
MAX(IND_HOGAR_COMPARTIDO_HH) AS  IND_HOGAR_COMPARTIDO_HH,
SUM(NUM_VALOR_PREP_HH) AS  NUM_VALOR_PREP_HH,
SUM(NUM_VALOR_PREP_3M_HH) AS  NUM_VALOR_PREP_3M_HH,
SUM(NUM_VALOR_POS_HH) AS  NUM_VALOR_POS_HH,
SUM(NUM_VALOR_POS_3M_HH) AS  NUM_VALOR_POS_3M_HH,
SUM(NUM_VALOR_HOGAR_HH) AS  NUM_VALOR_HOGAR_HH,
SUM(NUM_VALOR_HOGAR_3M_HH) AS  NUM_VALOR_HOGAR_3M_HH,
MAX(IND_LPD_HH) AS  IND_LPD_HH,
MAX(IND_HZ_HH) AS  IND_HZ_HH,
MAX(IND_ADSL_HH) AS  IND_ADSL_HH,
MAX(IND_FTTH_HH) AS  IND_FTTH_HH,
SUM(NUM_LINEAS_RED_HH) AS  NUM_LINEAS_RED_HH,
MAX(IND_CONVERGENTE_HH) AS  IND_CONVERGENTE_HH,

MAX(NUM_CLIENTES_ES_HH) AS  NUM_CLIENTES_ES_HH,
MAX(NUM_CLI_EXTRANJERO_HH) AS  NUM_CLI_EXTRANJERO_HH,

SUM(NUM_DISPOSITIVOS_HH) AS  NUM_DISPOSITIVOS_HH,
SUM(NUM_DISPOSITIVOS_MOVIL_HH) AS  NUM_DISPOSITIVOS_MOVIL_HH,
SUM(NUM_DISPOSITIVOS_OTRO_HH) AS  NUM_DISPOSITIVOS_OTRO_HH,
SUM(NUM_SMARTPHONE_HH) AS  NUM_SMARTPHONE_HH,
SUM(NUM_TABLET_HH) AS  NUM_TABLET_HH,
SUM(NUM_MODEM_USB_HH) AS  NUM_MODEM_USB_HH,
SUM(NUM_ANDROID_HH) AS  NUM_ANDROID_HH,
SUM(NUM_IOS_HH) AS  NUM_IOS_HH,
SUM(NUM_DISP_SOPORTAN_4G_HH) AS  NUM_DISP_SOPORTAN_4G_HH,
SUM(NUM_DISP_ACTIVO_4G_HH) AS  NUM_DISP_ACTIVO_4G_HH,
SUM(NUM_MOU_CALC_HH) AS  NUM_MOU_CALC_HH,
SUM(NUM_DATOSTOT_MB_CALC_HH) AS  NUM_DATOSTOT_MB_CALC_HH,
SUM(NUM_MOU_FIXED) AS  NUM_MOU_FIXED,
SUM(NUM_LLAMADAS_CALC_HH) AS  NUM_LLAMADAS_CALC_HH,
SUM(NUM_LLAMADAS_FIXED_HH) AS  NUM_LLAMADAS_FIXED_HH,
MAX(IND_RIESGO_HH) AS  IND_RIESGO_HH,

MAX(CASE WHEN IND_RIESGO_PROD_HH IS NULL THEN 0 ELSE IND_RIESGO_PROD_HH END) AS  IND_RIESGO_PROD_HH,
MAX(CASE WHEN IND_CROSS_UPSELL_HH IS NULL THEN 0 ELSE IND_CROSS_UPSELL_HH END) AS  IND_CROSS_UPSELL_HH,
SUM(NUM_SOLIC_PORTA_POS_HH) AS  NUM_SOLIC_PORTA_POS_HH,
SUM(NUM_SOLIC_PORTA_PRE_HH) AS  NUM_SOLIC_PORTA_PRE_HH,
SUM(NUM_INTERAC_INSATISF_HH) AS  NUM_INTERAC_INSATISF_HH,
SUM(NUM_DESACT_POS_HH) AS  NUM_DESACT_POS_HH,
SUM(NUM_DESACT_PRE_HH) AS  NUM_DESACT_PRE_HH,
SUM(NUM_MIGRAS_PRE_POS_HH) AS  NUM_MIGRAS_PRE_POS_HH,
SUM(NUM_MIGRAS_POS_PRE_HH) AS  NUM_MIGRAS_POS_PRE_HH,
SUM(NUM_SERV_VENTANA_CANJE_HH) AS  NUM_SERV_VENTANA_CANJE_HH,
MAX(IND_HUELLA_ONO_VF_HH) AS IND_HUELLA_ONO_VF_HH

FROM
VF_POSTPAID.PAR_EXP_AC_HOGAR_LIN
GROUP BY NIF;

DROP TABLE WORK.WORK_HH_MAESTRO;

EXIT;

