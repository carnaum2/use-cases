USE VF_POSTPAID;
            
set mapred.job.name = WORK_EXP_SOLCI_AUX;
DROP TABLE WORK.WORK_EXP_SOLCI_AUX;
CREATE TABLE WORK.WORK_EXP_SOLCI_AUX AS
SELECT 
SOL_PORT.MSISDN,
SOL_PORT.FECHA_SOLICITUD,
SOL_PORT.PARTITIONED_MONTH AS PARTITIONED_MONTH_PORTA,
CASE WHEN SOL_PORT.PARTITIONED_MONTH < ${hiveconf:MONTH3} THEN 1 ELSE 0 END AS IND_SOLCI_PORTA_4M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='MOVISTAR') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.PARTITIONED_MONTH < ${hiveconf:MONTH3}) THEN 1 ELSE  0 END AS IND_SOLCI_PORTA_MV_4M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='AMENA') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.PARTITIONED_MONTH < ${hiveconf:MONTH3}) THEN 1 ELSE  0 END AS IND_SOLCI_PORTA_OG_4M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='YOIGO') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.PARTITIONED_MONTH < ${hiveconf:MONTH3}) THEN 1 ELSE  0 END AS IND_SOLCI_PORTA_YO_4M,
CASE WHEN (((UPPER(SOL_PORT.RECEPTOR)<>'MOVISTAR') AND (UPPER(SOL_PORT.RECEPTOR)<>'AMENA') AND (UPPER(SOL_PORT.RECEPTOR)<>'YOIGO')) OR  (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) <> 0))
 AND (SOL_PORT.PARTITIONED_MONTH < ${hiveconf:MONTH3}) THEN 1 ELSE 0 END AS IND_SOLCI_PORTA_RE_4M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='MOVISTAR') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.ESTADO_SOLICITUD='ACAN' OR SOL_PORT.ESTADO_SOLICITUD='PCAN' OR SOL_PORT.ESTADO_SOLICITUD='AREC') THEN 1 ELSE 0 END AS IND_SOLCI_CANCEL_MV_12M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='AMENA') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.ESTADO_SOLICITUD='ACAN' OR SOL_PORT.ESTADO_SOLICITUD='PCAN' OR SOL_PORT.ESTADO_SOLICITUD='AREC') THEN 1 ELSE 0 END 
AS IND_SOLCI_CANCEL_OG_12M,
CASE WHEN (UPPER(SOL_PORT.RECEPTOR)='YOIGO') AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.ESTADO_SOLICITUD='ACAN' OR SOL_PORT.ESTADO_SOLICITUD='PCAN' OR SOL_PORT.ESTADO_SOLICITUD='AREC') THEN 1 ELSE 0 END 
AS IND_SOLCI_CANCEL_YO_12M,
CASE WHEN ((UPPER(SOL_PORT.RECEPTOR)<>'MOVISTAR') AND (UPPER(SOL_PORT.RECEPTOR)<>'AMENA') AND (UPPER(SOL_PORT.RECEPTOR)<>'YOIGO')) 
AND (CAST(SOL_PORT.RECEPTORVIR AS DOUBLE) = 0)  AND (SOL_PORT.ESTADO_SOLICITUD='ACAN' OR SOL_PORT.ESTADO_SOLICITUD='PCAN' OR SOL_PORT.ESTADO_SOLICITUD='AREC') THEN 1 ELSE  0 END AS IND_SOLCI_CANCEL_RE_12M,
CASE WHEN (SOL_PORT.ESTADO_SOLICITUD='ACAN' OR SOL_PORT.ESTADO_SOLICITUD='PCAN' OR SOL_PORT.ESTADO_SOLICITUD='AREC') THEN 1 ELSE 0 END AS IND_SOLCI_CANCEL_12M
FROM INPUT.VF_SOLICITUDES_PORTABILIDAD SOL_PORT 
--LEFT JOIN(SELECT X_NUM_IDENT AS NIF, X_ID_RED AS MSISDN  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH0}) AC_FIN
--ON SOL_PORT.MSISDN=AC_FIN.MSISDN 
WHERE SOL_PORT.PARTITIONED_MONTH > ${hiveconf:MONTH12} --AND AC_FIN.MSISDN IS NULL--
AND UPPER(SUBSTR(SOL_PORT.TIPO_CLIENTE, 1, 3)) = 'PAR';

set mapred.job.name = PAR_EXP_SOLICITUDES;
DROP TABLE VF_POSTPAID.PAR_EXP_SOLICITUDES;

            CREATE TABLE VF_POSTPAID.PAR_EXP_SOLICITUDES AS
            SELECT 
                MSISDN,
                CAST('${hiveconf:MONTH0}' AS TINYINT) AS PARTITIONED_MONTH,
                MAX(IND_SOLCI_PORTA_4M) AS IND_SOLCI_PORTA_4M,
                MAX(IND_SOLCI_PORTA_MV_4M) AS IND_SOLCI_PORTA_MV_4M,
                MAX(IND_SOLCI_PORTA_OG_4M) AS IND_SOLCI_PORTA_OG_4M,
                MAX(IND_SOLCI_PORTA_YO_4M) AS IND_SOLCI_PORTA_YO_4M,
                MAX(IND_SOLCI_PORTA_RE_4M) AS IND_SOLCI_PORTA_RE_4M,
                MAX(IND_SOLCI_CANCEL_MV_12M) AS IND_SOLCI_CANCEL_MV_12M,
                MAX(IND_SOLCI_CANCEL_OG_12M) AS IND_SOLCI_CANCEL_OG_12M,
                MAX(IND_SOLCI_CANCEL_YO_12M) AS IND_SOLCI_CANCEL_YO_12M,
                MAX(IND_SOLCI_CANCEL_RE_12M) AS IND_SOLCI_CANCEL_RE_12M,
                MAX(IND_SOLCI_CANCEL_12M) AS IND_SOLCI_CANCEL_12M
            FROM WORK.WORK_EXP_SOLCI_AUX
            GROUP BY MSISDN
        ; 
        
set mapred.job.name = PAR_EXP_SOLICITUDES_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_SOLICITUDES_CLI;

            CREATE TABLE VF_POSTPAID.PAR_EXP_SOLICITUDES_CLI AS
            SELECT 
                B.NIF,
                CAST('${hiveconf:MONTH0}' AS TINYINT) AS PARTITIONED_MONTH,
                MAX(IND_SOLCI_PORTA_4M) AS IND_SOLCI_PORTA_4M,
                MAX(IND_SOLCI_PORTA_MV_4M) AS IND_SOLCI_PORTA_MV_4M,
                MAX(IND_SOLCI_PORTA_OG_4M) AS IND_SOLCI_PORTA_OG_4M,
                MAX(IND_SOLCI_PORTA_YO_4M) AS IND_SOLCI_PORTA_YO_4M,
                MAX(IND_SOLCI_PORTA_RE_4M) AS IND_SOLCI_PORTA_RE_4M,
                MAX(IND_SOLCI_CANCEL_MV_12M) AS IND_SOLCI_CANCEL_MV_12M,
                MAX(IND_SOLCI_CANCEL_OG_12M) AS IND_SOLCI_CANCEL_OG_12M,
                MAX(IND_SOLCI_CANCEL_YO_12M) AS IND_SOLCI_CANCEL_YO_12M,
                MAX(IND_SOLCI_CANCEL_RE_12M) AS IND_SOLCI_CANCEL_RE_12M,
                MAX(IND_SOLCI_CANCEL_12M) AS IND_SOLCI_CANCEL_12M
            FROM  VF_POSTPAID.PAR_EXP_SOLICITUDES A
                  LEFT JOIN
                  (SELECT X_NUM_IDENT AS NIF, X_ID_RED AS MSISDN  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = ${hiveconf:MONTH0}) B
                  ON A.MSISDN = B.MSISDN
            GROUP BY B.NIF;
			
DROP TABLE WORK.WORK_EXP_SOLCI_AUX;
        
EXIT;