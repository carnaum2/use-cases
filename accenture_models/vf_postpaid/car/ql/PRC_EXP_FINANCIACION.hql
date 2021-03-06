USE VF_POSTPAID;

set mapred.job.name = WORK_FINANCIACION_1;
DROP TABLE WORK.WORK_FINANCIACION_1;
CREATE TABLE WORK.WORK_FINANCIACION_1 AS
SELECT
A.*,
CASE WHEN A.NUM_PRECIO_TERMINAL  != 0 THEN ROUND((A.NUM_IMPORTE_CUOTAS_PAGADAS*100)/A.NUM_PRECIO_TERMINAL,2) ELSE 0 END AS NUM_PORCENTAJE_PAGADO
FROM 
(SELECT
A.*,
ROUND(A.NUM_CUOTAS_PENDIENTES+A.NUM_CUOTAS_PAGADAS,0) AS NUM_CUOTAS_TOT,
ROUND(A.NUM_IMPORTE_CUOTAS_PENDIENTES+A.NUM_IMPORTE_CUOTAS_PAGADAS,2) AS NUM_PRECIO_TERMINAL,
CASE WHEN A.NUM_CUOTAS_PENDIENTES != 0 THEN ROUND(A.NUM_IMPORTE_CUOTAS_PENDIENTES/A.NUM_CUOTAS_PENDIENTES,2) ELSE 0 END AS NUM_PRECIO_CUOTA_PEND
FROM
(SELECT 
B.MSISDN,
B.X_NUM_IDENT,
ROUND(NVL(CAST(A.IMPORTE_APLAZAMIENTO AS DOUBLE),0),2) AS NUM_PRECIO_FINANCIADO,
ROUND(NVL(CAST(A.SUM_CUOTAS_PENDIENTES AS DOUBLE), 0),0) AS NUM_CUOTAS_PENDIENTES,
ROUND(NVL(CAST(A.SUM_CUOTAS_PAGADAS AS DOUBLE), 0),0) AS NUM_CUOTAS_PAGADAS,
ROUND(NVL(CAST(REGEXP_REPLACE(A.SUM_IMPORTE_CUOTAS_PENDIENTES,',','.') AS DOUBLE),0),2) AS NUM_IMPORTE_CUOTAS_PENDIENTES,
ROUND(NVL(CAST(REGEXP_REPLACE(A.SUM_IMPORTE_CUOTAS_PAGADAS,',','.') AS DOUBLE),0),2) AS NUM_IMPORTE_CUOTAS_PAGADAS,
CASE WHEN (B.MESES_FIN_CP_TARIFA IS NULL OR B.MESES_FIN_CP_TARIFA = 9999) THEN 0 ELSE B.MESES_FIN_CP_TARIFA END AS MESES_FIN_CP_TARIFA,
CASE WHEN A.MSISDN IS NOT NULL THEN 1 ELSE 0 END AS IND_FINANCIACION,
B.IND_SIMONLY
FROM 
(SELECT * FROM INPUT.VF_POS_CVM_INFO_FINANCIACION WHERE PARTITIONED_MONTH ='${hiveconf:MONTH0}') A
RIGHT JOIN
(SELECT 
X_ID_RED AS MSISDN, X_NUM_IDENT,
ROUND(CAST(MESES_FIN_CP_TARIFA AS DOUBLE),0) AS MESES_FIN_CP_TARIFA,
CAST(FLAG_FINANCIA_SIMO AS DOUBLE) AS IND_SIMONLY
FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') B
ON UPPER(TRIM(A.MSISDN)) = UPPER(TRIM(B.MSISDN)) 
WHERE A.MSISDN IS NOT NULL OR B.IND_SIMONLY > 0 ) A
) A;


set mapred.job.name = PAR_EXP_FINANCIACION;
DROP TABLE VF_POSTPAID.PAR_EXP_FINANCIACION;
CREATE TABLE VF_POSTPAID.PAR_EXP_FINANCIACION AS
SELECT 
A.*,
CASE WHEN A.MESES_FIN_CP_TARIFA = 0 THEN 1 ELSE 0 END AS IND_SIN_PERM_TARIFA,
CASE WHEN A.MESES_FIN_CP_TARIFA >= 1 AND A.MESES_FIN_CP_TARIFA < 12 THEN 1 ELSE 0 END AS IND_PERM_TARIFA_MENOS_12,
CASE WHEN A.MESES_FIN_CP_TARIFA >= 1 AND A.MESES_FIN_CP_TARIFA < 18 THEN 1 ELSE 0 END AS IND_PERM_TARIFA_MENOS_18,
CASE WHEN A.MESES_FIN_CP_TARIFA > 12 THEN 1 ELSE 0 END AS IND_PERM_TARIFA_MAS_12,
CASE WHEN A.MESES_FIN_CP_TARIFA > 18 THEN 1 ELSE 0 END AS IND_PERM_TARIFA_MAS_18
FROM WORK.WORK_FINANCIACION_1 A;


set mapred.job.name = PAR_EXP_FINANCIACION_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_FINANCIACION_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_FINANCIACION_CLI AS
SELECT 
X_NUM_IDENT AS NIF,
SUM(NUM_PRECIO_TERMINAL) AS NUM_PRECIO_TERMINALES_FINAN,
SUM(NUM_CUOTAS_TOT) AS NUM_CUOTAS_TOT_FINAN,
SUM(NUM_PRECIO_CUOTA_PEND) AS NUM_PRECIO_CUOTAS_TOT,
MAX(IND_FINANCIACION) AS IND_FINANCIACION,
MAX(IND_SIMONLY) AS IND_SIMONLY,
MAX(IND_SIN_PERM_TARIFA) AS IND_SIN_PERM_TARIFA,
MAX(IND_PERM_TARIFA_MENOS_12) AS IND_PERM_TARIFA_MENOS_12,
MAX(IND_PERM_TARIFA_MENOS_18) AS IND_PERM_TARIFA_MENOS_18,
MAX(IND_PERM_TARIFA_MAS_12) AS IND_PERM_TARIFA_MAS_12,
MAX(IND_PERM_TARIFA_MAS_18) AS IND_PERM_TARIFA_MAS_18,
SUM(NUM_CUOTAS_PAGADAS) AS NUM_CUOTAS_PAGADAS,
SUM(NUM_CUOTAS_PENDIENTES) AS NUM_CUOTAS_PENDIENTES,
SUM(NUM_IMPORTE_CUOTAS_PENDIENTES) AS NUM_IMPORTE_POR_PAGAR,
SUM(NUM_IMPORTE_CUOTAS_PAGADAS) AS NUM_IMPORTE_PAGADO,
SUM(NUM_PRECIO_CUOTA_PEND) AS NUM_PRECIO_CUOTA_PEND,
CASE WHEN COUNT(X_NUM_IDENT)!=0 THEN ROUND(SUM(NUM_PORCENTAJE_PAGADO)/COUNT(X_NUM_IDENT),0) END AS NUM_PORCENTAJE_PAGADO 
FROM 
VF_POSTPAID.PAR_EXP_FINANCIACION
GROUP BY X_NUM_IDENT;

DROP TABLE WORK.WORK_FINANCIACION_1;

EXIT;
