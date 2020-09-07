USE VF_POSTPAID;

set mapred.job.name = WORK_KXEN_GSM_M0;
DROP TABLE WORK.WORK_KXEN_GSM_M0;
CREATE TABLE WORK.WORK_KXEN_GSM_M0 AS
SELECT
A.MSISDN,
SUM(MINUTOS_INTER) AS MINUTOS_INTER
FROM
(SELECT
TELEFONO as MSISDN,
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS DOUBLE) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS DOUBLE) END) AS MINUTOS_INTER
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
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS DOUBLE) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS DOUBLE) END) AS MINUTOS_INTER
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
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS DOUBLE) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS DOUBLE) END) AS MINUTOS_INTER
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
(CASE WHEN MININTERNACIONAL_M0 IS NULL THEN 0 ELSE CAST(MININTERNACIONAL_M0  AS DOUBLE) END) + 
(CASE WHEN MINROAMINGOUTREALIZDO_M0 IS NULL THEN 0 ELSE CAST(MINROAMINGOUTREALIZDO_M0  AS DOUBLE) END) AS MINUTOS_INTER
FROM 
INPUT.VF_KXEN_C_MA WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') A
GROUP BY MSISDN;

set mapred.job.name = PAR_EXP_KXEN_GSM_LIN;
DROP TABLE VF_POSTPAID.PAR_EXP_KXEN_GSM_LIN;
CREATE TABLE VF_POSTPAID.PAR_EXP_KXEN_GSM_LIN AS
SELECT
M.NIF,
M.MSISDN,

round(A.MINUTOS_INTER, 0) AS NUM_MIN_INTER_M0,
round(B.MINUTOS_INTER, 0) AS NUM_MIN_INTER_M1,
round(C.MINUTOS_INTER, 0) AS NUM_MIN_INTER_M2,
D.MINUTOS_INTER AS NUM_MIN_INTER_M3,
VF_FUNC.AvgN(A.MINUTOS_INTER, B.MINUTOS_INTER, C.MINUTOS_INTER, D.MINUTOS_INTER) AS NUM_MIN_INTER_AVG,
VF_FUNC.IncN(A.MINUTOS_INTER, A.MINUTOS_INTER, B.MINUTOS_INTER, C.MINUTOS_INTER, D.MINUTOS_INTER) AS NUM_MIN_INTER_INC

FROM
(SELECT X_ID_RED AS MSISDN, MAX(X_NUM_IDENT) AS NIF  
FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}' GROUP BY X_ID_RED) M
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

set mapred.job.name = WORK_KXEN_GSM_CLI;
DROP TABLE WORK.WORK_KXEN_GSM_CLI;
CREATE TABLE WORK.WORK_KXEN_GSM_CLI AS
SELECT
NIF,
SUM(NUM_MIN_INTER_M0) AS NUM_MIN_INTER_M0,
SUM(NUM_MIN_INTER_M1) AS NUM_MIN_INTER_M1,
SUM(NUM_MIN_INTER_M2) AS NUM_MIN_INTER_M2,
SUM(NUM_MIN_INTER_M3) AS NUM_MIN_INTER_M3
FROM 
VF_POSTPAID.PAR_EXP_KXEN_GSM_LIN
GROUP BY NIF;

set mapred.job.name = PAR_EXP_KXEN_GSM_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_KXEN_GSM_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_KXEN_GSM_CLI AS
SELECT
NIF,
round(NUM_MIN_INTER_M0, 0) as NUM_MIN_INTER_M0,
VF_FUNC.AvgN(NUM_MIN_INTER_M0, NUM_MIN_INTER_M1, NUM_MIN_INTER_M2, NUM_MIN_INTER_M3) AS NUM_MIN_INTER_AVG,
VF_FUNC.IncN(NUM_MIN_INTER_M0, NUM_MIN_INTER_M0, NUM_MIN_INTER_M1, NUM_MIN_INTER_M2, NUM_MIN_INTER_M3) AS NUM_MIN_INTER_INC
FROM
WORK.WORK_KXEN_GSM_CLI;

DROP TABLE WORK.WORK_KXEN_GSM_M0;
DROP TABLE WORK.WORK_KXEN_GSM_M1;
DROP TABLE WORK.WORK_KXEN_GSM_M2;
DROP TABLE WORK.WORK_KXEN_GSM_M3;
DROP TABLE WORK.WORK_KXEN_GSM_CLI;

EXIT;
