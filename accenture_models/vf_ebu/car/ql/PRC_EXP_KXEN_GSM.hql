USE VF_EBU;


set mapred.job.name = WORK_KXEN_GSM_M0;
DROP TABLE WORK.WORK_KXEN_GSM_M0;
CREATE TABLE WORK.WORK_KXEN_GSM_M0 AS
SELECT
A.MSISDN,
SUM(MINUTOS_INTER) AS MINUTOS_INTER
FROM
(SELECT
TELEFONO as MSISDN,
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS INT) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS INT) END) AS MINUTOS_INTER
FROM 
INPUT.VF_KXEN_C_MA WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
GROUP BY A.MSISDN;
 
set mapred.job.name = WORK_KXEN_GSM_M1;
DROP TABLE WORK.WORK_KXEN_GSM_M1;
CREATE TABLE WORK.WORK_KXEN_GSM_M1 AS
SELECT
A.MSISDN,
SUM(MINUTOS_INTER) AS MINUTOS_INTER
FROM
(SELECT
TELEFONO as MSISDN,
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS INT) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS INT) END) AS MINUTOS_INTER
FROM 
INPUT.VF_KXEN_C_MA WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') A
GROUP BY MSISDN;

set mapred.job.name = WORK_KXEN_GSM_M2;
DROP TABLE WORK.WORK_KXEN_GSM_M2;
CREATE TABLE WORK.WORK_KXEN_GSM_M2 AS
SELECT
A.MSISDN,
SUM(MINUTOS_INTER) AS MINUTOS_INTER
FROM
(SELECT
TELEFONO as MSISDN,
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS INT) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS INT) END) AS MINUTOS_INTER
FROM 
INPUT.VF_KXEN_C_MA WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') A
GROUP BY MSISDN;

set mapred.job.name = WORK_KXEN_GSM_M3;
DROP TABLE WORK.WORK_KXEN_GSM_M3;
CREATE TABLE WORK.WORK_KXEN_GSM_M3 AS
SELECT
A.MSISDN,
SUM(MINUTOS_INTER) AS MINUTOS_INTER
FROM
(SELECT
TELEFONO as MSISDN,
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS INT) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS INT) END) AS MINUTOS_INTER
FROM 
INPUT.VF_KXEN_C_MA WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') A
GROUP BY MSISDN;


------------------------------------------------------------------------------------
set mapred.job.name = EMP_EXP_KXEN_GSM_LIN;
DROP TABLE VF_EBU.EMP_EXP_KXEN_GSM_LIN;
CREATE TABLE VF_EBU.EMP_EXP_KXEN_GSM_LIN AS
SELECT
M.NIF,
M.MSISDN,
'${hiveconf:MONTH0}' AS MES,
ROUND(A.MINUTOS_INTER) AS NUM_MIN_INTER_M0,
ROUND(B.MINUTOS_INTER) AS NUM_MIN_INTER_M1,
ROUND(C.MINUTOS_INTER) AS NUM_MIN_INTER_M2,
ROUND(D.MINUTOS_INTER) AS NUM_MIN_INTER_M3,
ROUND(VF_FUNC.AvgN(A.MINUTOS_INTER, B.MINUTOS_INTER, C.MINUTOS_INTER, D.MINUTOS_INTER),2) AS NUM_MIN_INTER_AVG,
ROUND(VF_FUNC.IncN(A.MINUTOS_INTER, B.MINUTOS_INTER, C.MINUTOS_INTER, D.MINUTOS_INTER),2) AS NUM_MIN_INTER_INC

from
(SELECT CIF_NIF AS NIF,
MSISDN 
FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') M
LEFT JOIN
WORK.WORK_KXEN_GSM_M0 A
ON M.MSISDN = A.MSISDN
LEFT JOIN
WORK.WORK_KXEN_GSM_M1 B
ON M.MSISDN = B.MSISDN
LEFT JOIN
WORK.WORK_KXEN_GSM_M2 C
ON M.MSISDN = C.MSISDN
LEFT JOIN
WORK.WORK_KXEN_GSM_M3 D
ON M.MSISDN = D.MSISDN;

		
------------------------------------------------------------------------------------	
set mapred.job.name = WORK_KXEN_GSM_CLI;
DROP TABLE WORK.WORK_KXEN_GSM_CLI;
CREATE TABLE WORK.WORK_KXEN_GSM_CLI AS		
SELECT
NIF,
ROUND(SUM(NUM_MIN_INTER_M0)) AS NUM_MIN_INTER_M0,
ROUND(SUM(NUM_MIN_INTER_M1)) AS NUM_MIN_INTER_M1,
ROUND(SUM(NUM_MIN_INTER_M2)) AS NUM_MIN_INTER_M2,
ROUND(SUM(NUM_MIN_INTER_M3)) AS NUM_MIN_INTER_M3
FROM 
VF_EBU.EMP_EXP_KXEN_GSM_LIN
GROUP BY NIF;


------------------------------------------------------------------------------------


set mapred.job.name = EMP_EXP_KXEN_GSM_CLI;
DROP TABLE VF_EBU.EMP_EXP_KXEN_GSM_CLI;
CREATE TABLE VF_EBU.EMP_EXP_KXEN_GSM_CLI AS	
SELECT
NIF,
'${hiveconf:MONTH0}' AS MES,
ROUND(NUM_MIN_INTER_M0) AS NUM_MIN_INTER_M0,
ROUND(VF_FUNC.AvgN(NUM_MIN_INTER_M0, NUM_MIN_INTER_M1, NUM_MIN_INTER_M2, NUM_MIN_INTER_M3),2) AS NUM_MIN_INTER_AVG,
ROUND(VF_FUNC.IncN(NUM_MIN_INTER_M0, NUM_MIN_INTER_M1, NUM_MIN_INTER_M2, NUM_MIN_INTER_M3),2) AS NUM_MIN_INTER_INC
FROM
WORK.WORK_KXEN_GSM_CLI;

DROP TABLE WORK.WORK_KXEN_GSM_M0;
DROP TABLE WORK.WORK_KXEN_GSM_M1;
DROP TABLE WORK.WORK_KXEN_GSM_M2;
DROP TABLE WORK.WORK_KXEN_GSM_M3;
DROP TABLE WORK.WORK_KXEN_GSM_CLI;

EXIT;