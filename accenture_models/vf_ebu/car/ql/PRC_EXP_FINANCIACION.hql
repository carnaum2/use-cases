USE VF_EBU;

set mapred.job.name = WORK_FINANCIACION_HIST;
DROP TABLE WORK.WORK_FINANCIACION_HIST;
CREATE TABLE WORK.WORK_FINANCIACION_HIST AS
SELECT
A.NIF,
A.MSISDN,
A.STATUS,
A.FECHA_OPERACION,
ROUND(A.IMPORTE_APLAZAMIENTO,2) AS IMPORTE_APLAZAMIENTO,
A.FECHA_VENCIMIENTO,
A.FECHA_ABONO_CUOTA,
A.NDOC_COMPENSACION,
ROUND(A.IMPORTE_CUOTA,2)AS IMPORTE_CUOTA,
A.NPOSICION,
A.TIPO_OPERACION,
A.PARTITIONED_MONTH,
CASE WHEN A.TIPO_OPERACION LIKE 'D' THEN 1 ELSE 0 END AS IND_DEVOLUCION,
CASE WHEN A.TIPO_OPERACION LIKE 'C' THEN 1 ELSE 0 END AS IND_CANCELACION,
CASE WHEN A.TIPO_OPERACION LIKE 'J' THEN 1 ELSE 0 END AS IND_CANJE,
CASE WHEN A.TIPO_OPERACION LIKE 'A' THEN 1 ELSE 0 END AS IND_ALTA,
CASE WHEN A.STATUS LIKE 'P' THEN 1 ELSE 0 END AS IND_PENDIENTE,
CASE WHEN A.STATUS LIKE 'E' THEN 1 ELSE 0 END AS IND_ERRONEO,
CASE WHEN A.STATUS LIKE 'C' THEN 1 ELSE 0 END AS IND_COMPLETADO,
CASE WHEN A.STATUS LIKE 'F' THEN 1 ELSE 0 END AS IND_FINALIZADO
FROM
(SELECT * FROM INPUT.VF_EBU_EMP_FINANCIACION WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
WHERE CAST(A.FECHA_OPERACION AS DOUBLE) < CAST(CONCAT('${hiveconf:MONTH0}',40) AS DOUBLE);


set mapred.job.name = WORK_CARTERA_FINAN_NODUP_TOT;
DROP TABLE WORK.WORK_CARTERA_FINAN_NODUP_TOT;
CREATE TABLE WORK.WORK_CARTERA_FINAN_NODUP_TOT AS
SELECT A.*
FROM 
(SELECT * FROM WORK.WORK_FINANCIACION_HIST) A
INNER JOIN
(SELECT  
A.MSISDN FROM
(SELECT MSISDN,NPOSICION, COUNT(*) AS TOT FROM 
WORK.WORK_FINANCIACION_HIST GROUP BY MSISDN,NPOSICION) A
GROUP BY A.MSISDN
HAVING MAX(A.TOT) = 1) B
ON A.MSISDN = B.MSISDN;


set mapred.job.name = WORK_CARTERA_FINANCIACION_TOT;
DROP TABLE WORK.WORK_CARTERA_FINANCIACION_TOT;
CREATE TABLE WORK.WORK_CARTERA_FINANCIACION_TOT AS
SELECT
B.*, 
MES_ULTIMO_PAGO, 
TOTAL_CUOTAS
FROM
(SELECT 
MSISDN, 
MAX(CAST(SUBSTR(FECHA_ABONO_CUOTA,1,6) AS INT)) AS MES_ULTIMO_PAGO,
MAX(CAST(NPOSICION AS INT)) AS TOTAL_CUOTAS
FROM WORK.WORK_CARTERA_FINAN_NODUP_TOT
GROUP BY MSISDN) A
INNER JOIN
(SELECT 
A.*, 
SUBSTR(FECHA_ABONO_CUOTA,1,6) AS MES_PAGO
FROM WORK.WORK_CARTERA_FINAN_NODUP_TOT A) B
ON A.MSISDN = B.MSISDN;



set mapred.job.name = WORK_CARTERA_FINAN_CUOTAS_TOT;
DROP TABLE WORK.WORK_CARTERA_FINAN_CUOTAS_TOT;
CREATE TABLE WORK.WORK_CARTERA_FINAN_CUOTAS_TOT AS
SELECT
A.*
--      CASE
--      WHEN NUM_CUOTAS_RESTANTES >0 THEN -1
--      ELSE MONTHS_BETWEEN(TO_DATE('${hiveconf:MONTH0}'
FROM(
SELECT 
MSISDN,
--      SUM(CASE WHEN MES_PAGO IS NOT NULL AND MES_PAGO <= '${hiveconf:MONTH1}'AS NUM_CUOTAS_PAGADAS,
--      MAX(TOTAL_CUOTAS) - SUM(CASE WHEN MES_PAGO IS NOT NULL AND MES_PAGO <= '${hiveconf:MONTH1}'AS NUM_CUOTAS_RESTANTES,
--      SUM(CASE WHEN MES_PAGO IS NOT NULL AND MES_PAGO <= '${hiveconf:MONTH0}'
--      VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(IMPORTE_CUOTA,',','.')) ELSE 0 END) AS NUM_IMPORTE_PAGADO,
MAX(ROUND(IMPORTE_APLAZAMIENTO,2)) - 
SUM(CASE WHEN MES_PAGO IS NOT NULL AND LENGTH(TRIM(MES_PAGO)) = 6 THEN ROUND(IMPORTE_CUOTA,2) ELSE 0 END) AS NUM_IMPORTE_RESTANTE, 
MAX(ROUND(IMPORTE_APLAZAMIENTO,2)) AS NUM_IMPORTE_FINANCIADO
--      MAX(IMPORTE_CUOTA) AS NUM_IMPORTE_CUOTA,
--      MAX(TOTAL_CUOTAS) AS NUM_TOTAL_CUOTAS,
--      MAX(IND_CANCELACION) AS IND_CANCELACION,
--      MAX(IND_CANJE) AS IND_CANJE,
--      MAX(IND_ALTA) AS IND_ALTA,
--      MAX(IND_PENDIENTE) AS IND_PENDIENTE,
--      MAX(IND_ERRONEO) AS IND_ERRONEO,
--      MAX(IND_COMPLETADO) AS IND_COMPLETADO,
--      MAX(IND_FINALIZADO) AS IND_FINALIZADO,
--      MAX(MES_PAGO) AS MES_PAGO
FROM WORK.WORK_CARTERA_FINANCIACION_TOT
GROUP BY MSISDN) A;
      
set mapred.job.name = WORK_CART_FINAN_CUOTAS_TOT_NIF;
DROP TABLE WORK.WORK_CART_FINAN_CUOTAS_TOT_NIF;
CREATE TABLE  WORK.WORK_CART_FINAN_CUOTAS_TOT_NIF  AS
SELECT 
B.CIF_NIF,
A.*
FROM
WORK.WORK_CARTERA_FINAN_CUOTAS_TOT A
LEFT JOIN
(SELECT 
DISTINCT(A.CIF_NIF), 
B.MSISDN FROM
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH='${hiveconf:MONTH0}') A
LEFT JOIN
WORK.WORK_FINANCIACION_HIST B
ON A.CIF_NIF=B.NIF) B
ON A.MSISDN=B.MSISDN;
          
      
set mapred.job.name = EMP_EXP_FINANCIACION;
DROP TABLE VF_EBU.EMP_EXP_FINANCIACION;
CREATE TABLE VF_EBU.EMP_EXP_FINANCIACION AS
SELECT A.*
--      CASE
--      WHEN NUM_TOT_CUOTAS_RESTANTES > 0 THEN -1
--      ELSE MONTHS_BETWEEN(TO_DATE('${hiveconf:MONTH0}'
FROM 
(SELECT
CIF_NIF,
--      COUNT(CIF_NIF) AS NUM_FINANC,
--      (COUNT(CIF_NIF) - SUM(IND_FINALIZADO)) AS NUM_FINANC_ACT,
--      SUM(NUM_CUOTAS_PAGADAS) AS NUM_TOT_CUOTAS_PAGADAS,
--      SUM(NUM_CUOTAS_RESTANTES) AS NUM_TOT_CUOTAS_RESTANTES,
--      SUM(NUM_TOTAL_CUOTAS) AS NUM_TOT_CUOTAS,
--      MIN(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_FINANCIADO,',','.'))) AS NUM_FINANCIACION_MIN,
--      MAX(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_FINANCIADO,',','.'))) AS NUM_FINANCIACION_MAX,
ROUND(SUM(NUM_IMPORTE_FINANCIADO),2) AS NUM_FINANCIACION_TOTAL,
--      ROUND(AVG(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_FINANCIADO,',','.'))),2) AS NUM_FINANCIACION_MEDIA,
--      MIN(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_CUOTA,',','.'))) AS NUM_CUOTA_MIN_FINANC,
--      MAX(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_CUOTA,',','.'))) AS NUM_CUOTA_MAX_FINANC,
--      SUM(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_CUOTA,',','.'))) AS NUM_TOTAL_CUOTAS,
--      ROUND(AVG(VF_FUNC.TONUMBERIFERROR(REGEXP_REPLACE(NUM_IMPORTE_CUOTA,',','.'))),2) AS NUM_CUOTA_MEDIA,
ROUND(MIN(NUM_IMPORTE_RESTANTE),2) AS NUM_IMP_PEND_MIN,
ROUND(MAX(NUM_IMPORTE_RESTANTE),2) AS NUM_IMP_PEND_MAX,
ROUND(SUM(NUM_IMPORTE_RESTANTE),2) AS NUM_IMP_TOTAL_PEND
--      ROUND(AVG(NUM_IMPORTE_RESTANTE),2)  AS NUM_PROMEDIO_PENDIENTES, 
--      MIN(NUM_IMPORTE_PAGADO) AS NUM_PAGADO_MIN,
--      MAX(NUM_IMPORTE_PAGADO) AS NUM_PAGADO_MAX,
--SUM(NUM_IMPORTE_PAGADO) AS NUM_IMP_TOTAL_PAGADO
FROM 
WORK.WORK_CART_FINAN_CUOTAS_TOT_NIF
GROUP BY CIF_NIF ) A;

DROP TABLE WORK.WORK_FINANCIACION_HIST;
DROP TABLE WORK.WORK_CARTERA_FINAN_NODUP_TOT;
DROP TABLE WORK.WORK_CARTERA_FINANCIACION_TOT;
DROP TABLE WORK.WORK_CARTERA_FINAN_CUOTAS_TOT;
DROP TABLE WORK.WORK_CART_FINAN_CUOTAS_TOT_NIF;

EXIT;