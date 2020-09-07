-- PARAMETROS DEL MES0 AL MES5

USE VF_POSTPAID;  
set mapred.job.name = WORK_ACT_M0;

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

DROP TABLE WORK.WORK_ACT_M0;

CREATE TABLE WORK.WORK_ACT_M0 AS 
SELECT
NIF,
MAX(FLAGADSL) AS IND_ALTA_ADSL,
MAX(FLAGLPD) AS IND_ALTA_LPD,
MAX(FLAGFTTH) AS IND_ALTA_FTTH,
MAX(FLAGVOZ) AS IND_ALTA_VOZ
FROM
(
SELECT
A.X_NUM_IDENT AS NIF,
A.X_ID_RED AS MSISDN,
CAST(A.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(A.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(A.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(A.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH=${hiveconf:MONTH0}) A
LEFT JOIN
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH=${hiveconf:MONTH1}) B
ON A.X_ID_RED = B.X_ID_RED WHERE B.X_ID_RED IS NULL
) CC GROUP BY NIF;

set mapred.job.name = WORK_DESACT_M0;
DROP TABLE WORK.WORK_DESACT_M0;
CREATE TABLE WORK.WORK_DESACT_M0 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_BAJA_ADSL,
MAX(FLAGLPD) AS IND_BAJA_LPD,
MAX(FLAGFTTH) AS IND_BAJA_FTTH,
MAX(FLAGVOZ) AS IND_BAJA_VOZ
FROM(
SELECT
B.X_NUM_IDENT AS NIF,
B.X_ID_RED AS MSISDN,
CAST(B.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(B.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(B.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(B.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH = ${hiveconf:MONTH0}) A
RIGHT JOIN
(SELECT  X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ   FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH = ${hiveconf:MONTH1}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE A.X_ID_RED IS NULL) CC
GROUP BY NIF;


set mapred.job.name = WORK_ACT_M1;
DROP TABLE WORK.WORK_ACT_M1;
CREATE TABLE WORK.WORK_ACT_M1 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_ALTA_ADSL,
MAX(FLAGLPD) AS IND_ALTA_LPD,
MAX(FLAGFTTH) AS IND_ALTA_FTTH,
MAX(FLAGVOZ) AS IND_ALTA_VOZ
FROM(
SELECT
A.X_NUM_IDENT AS NIF,
A.X_ID_RED AS MSISDN,
CAST(A.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(A.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(A.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(A.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH1}) A
LEFT JOIN
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH2}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE B.X_ID_RED IS NULL) CC
GROUP BY NIF;

set mapred.job.name = WORK_DESACT_M1;
DROP TABLE WORK.WORK_DESACT_M1;
CREATE TABLE WORK.WORK_DESACT_M1 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_BAJA_ADSL,
MAX(FLAGLPD) AS IND_BAJA_LPD,
MAX(FLAGFTTH) AS IND_BAJA_FTTH,
MAX(FLAGVOZ) AS IND_BAJA_VOZ
FROM(
SELECT
B.X_NUM_IDENT AS NIF,
B.X_ID_RED AS MSISDN,
CAST(B.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(B.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(B.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(B.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH1}) A
RIGHT JOIN
(SELECT  X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ   FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH2}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE A.X_ID_RED IS NULL) CC
GROUP BY NIF;

set mapred.job.name = WORK_ACT_M2;
DROP TABLE WORK.WORK_ACT_M2;
CREATE TABLE WORK.WORK_ACT_M2 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_ALTA_ADSL,
MAX(FLAGLPD) AS IND_ALTA_LPD,
MAX(FLAGFTTH) AS IND_ALTA_FTTH,
MAX(FLAGVOZ) AS IND_ALTA_VOZ
FROM(
SELECT
A.X_NUM_IDENT AS NIF,
A.X_ID_RED AS MSISDN,
CAST(A.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(A.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(A.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(A.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH2}) A
LEFT JOIN
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH3}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE B.X_ID_RED IS NULL) CC
GROUP BY NIF;

set mapred.job.name = WORK_DESACT_M2;
DROP TABLE WORK.WORK_DESACT_M2;
CREATE TABLE WORK.WORK_DESACT_M2 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_BAJA_ADSL,
MAX(FLAGLPD) AS IND_BAJA_LPD,
MAX(FLAGFTTH) AS IND_BAJA_FTTH,
MAX(FLAGVOZ) AS IND_BAJA_VOZ
FROM(
SELECT
B.X_NUM_IDENT AS NIF,
B.X_ID_RED AS MSISDN,
CAST(B.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(B.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(B.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(B.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH2}) A
RIGHT JOIN
(SELECT  X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ   FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH3}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE A.X_ID_RED IS NULL) CC
GROUP BY NIF;


set mapred.job.name = WORK_ACT_M3;
DROP TABLE WORK.WORK_ACT_M3;
CREATE TABLE WORK.WORK_ACT_M3 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_ALTA_ADSL,
MAX(FLAGLPD) AS IND_ALTA_LPD,
MAX(FLAGFTTH) AS IND_ALTA_FTTH,
MAX(FLAGVOZ) AS IND_ALTA_VOZ
FROM(
SELECT
A.X_NUM_IDENT AS NIF,
A.X_ID_RED AS MSISDN,
CAST(A.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(A.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(A.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(A.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH3}) A
LEFT JOIN
(SELECT  DISTINCT X_ID_RED   FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH4}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE B.X_ID_RED IS NULL) CC
GROUP BY NIF;

set mapred.job.name = WORK_DESACT_M3;
DROP TABLE WORK.WORK_DESACT_M3;
CREATE TABLE WORK.WORK_DESACT_M3 AS        SELECT
NIF,
MAX(FLAGADSL) AS IND_BAJA_ADSL,
MAX(FLAGLPD) AS IND_BAJA_LPD,
MAX(FLAGFTTH) AS IND_BAJA_FTTH,
MAX(FLAGVOZ) AS IND_BAJA_VOZ
FROM(
SELECT
B.X_NUM_IDENT AS NIF,
B.X_ID_RED AS MSISDN,
CAST(B.FLAGADSL AS DOUBLE) as FLAGADSL,
CAST(B.FLAGLPD AS DOUBLE) as FLAGLPD,
CAST(B.FLAGFTTH AS DOUBLE) as FLAGFTTH,
CAST(B.FLAGVOZ AS DOUBLE) as FLAGVOZ
FROM
(SELECT DISTINCT X_ID_RED  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH3}) A
RIGHT JOIN
(SELECT  X_NUM_IDENT, X_ID_RED, FLAGADSL, FLAGLPD, FLAGFTTH, FLAGVOZ   FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH =  ${hiveconf:MONTH4}) B
ON A.X_ID_RED = B.X_ID_RED
WHERE A.X_ID_RED IS NULL) CC
GROUP BY NIF;


set mapred.job.name = WORK_ACT_DESACT_LIN;
DROP TABLE WORK.WORK_ACT_DESACT_LIN;
CREATE TABLE WORK.WORK_ACT_DESACT_LIN AS    SELECT
A.NIF,
A.MSISDN,${hiveconf:MONTH0} AS MES,
case when 
case when b1.ind_alta_adsl is null then 0 else b1.ind_alta_adsl end + 
case when b2.ind_alta_adsl is null then 0 else b2.ind_alta_adsl end  + 
case when b3.ind_alta_adsl is null then 0 else b3.ind_alta_adsl end  > 0 then 1 else 0 end as ind_altas_adsl_3m,

case when 
case when b1.ind_alta_lpd is null then 0 else b1.ind_alta_lpd end + 
case when b2.ind_alta_lpd is null then 0 else b2.ind_alta_lpd end + 
case when b3.ind_alta_lpd is null then 0 else b3.ind_alta_lpd end  > 0 then 1 else 0 end as ind_altas_lpd_3m,

case when 
case when b1.ind_alta_ftth is null then 0 else b1.ind_alta_ftth end + 
case when b2.ind_alta_ftth is null then 0 else b2.ind_alta_ftth end + 
case when b3.ind_alta_ftth is null then 0 else b3.ind_alta_ftth  end > 0 then 1 else 0 end as ind_altas_ftth_3m,

case when 
case when b1.ind_alta_voz is null then 0 else b1.ind_alta_voz end + 
case when b2.ind_alta_voz is null then 0 else b2.ind_alta_voz end + 
case when b3.ind_alta_voz is null then 0 else b3.ind_alta_voz end > 0 then 1 else 0 end as ind_altas_voz_3m,

case when b1.ind_alta_adsl is null then 0 else b1.ind_alta_adsl end + 
case when b2.ind_alta_adsl is null then 0 else b2.ind_alta_adsl end  + 
case when b3.ind_alta_adsl is null then 0 else b3.ind_alta_adsl end as num_altas_adsl_3m,

case when b1.ind_alta_lpd is null then 0 else b1.ind_alta_lpd end + 
case when b2.ind_alta_lpd is null then 0 else b2.ind_alta_lpd end + 
case when b3.ind_alta_lpd is null then 0 else b3.ind_alta_lpd end as num_altas_lpd_3m,

case when b1.ind_alta_ftth is null then 0 else b1.ind_alta_ftth end + 
case when b2.ind_alta_ftth is null then 0 else b2.ind_alta_ftth end + 
case when b3.ind_alta_ftth is null then 0 else b3.ind_alta_ftth end as num_altas_ftth_3m,

case when b1.ind_alta_voz is null then 0 else b1.ind_alta_voz end + 
case when b2.ind_alta_voz is null then 0 else b2.ind_alta_voz end + 
case when b3.ind_alta_voz is null then 0 else b3.ind_alta_voz end as num_altas_voz_3m,

case when 
case when c1.ind_baja_adsl is null then 0 else c1.ind_baja_adsl end + 
case when c2.ind_baja_adsl is null then 0 else c2.ind_baja_adsl end  + 
case when c3.ind_baja_adsl is null then 0 else c3.ind_baja_adsl end  > 0 then 1 else 0 end as ind_bajas_adsl_3m,

case when 
case when c1.ind_baja_lpd is null then 0 else c1.ind_baja_lpd end + 
case when c2.ind_baja_lpd is null then 0 else c2.ind_baja_lpd end + 
case when c3.ind_baja_lpd is null then 0 else c3.ind_baja_lpd end  > 0 then 1 else 0 end as ind_bajas_lpd_3m,

case when 
case when c1.ind_baja_ftth is null then 0 else c1.ind_baja_ftth end + 
case when c2.ind_baja_ftth is null then 0 else c2.ind_baja_ftth end + 
case when c3.ind_baja_ftth is null then 0 else c3.ind_baja_ftth  end > 0 then 1 else 0 end as ind_bajas_ftth_3m,

case when 
case when c1.ind_baja_voz is null then 0 else c1.ind_baja_voz end + 
case when c2.ind_baja_voz is null then 0 else c2.ind_baja_voz end + 
case when c3.ind_baja_voz is null then 0 else c3.ind_baja_voz end > 0 then 1 else 0 end as ind_bajas_voz_3m,

case when c1.ind_baja_adsl is null then 0 else c1.ind_baja_adsl end + 
case when c2.ind_baja_adsl is null then 0 else c2.ind_baja_adsl end  + 
case when c3.ind_baja_adsl is null then 0 else c3.ind_baja_adsl end as num_bajas_adsl_3m,

case when c1.ind_baja_lpd is null then 0 else c1.ind_baja_lpd end + 
case when c2.ind_baja_lpd is null then 0 else c2.ind_baja_lpd end + 
case when c3.ind_baja_lpd is null then 0 else c3.ind_baja_lpd end as num_bajas_lpd_3m,

case when c1.ind_baja_ftth is null then 0 else c1.ind_baja_ftth end + 
case when c2.ind_baja_ftth is null then 0 else c2.ind_baja_ftth end + 
case when c3.ind_baja_ftth is null then 0 else c3.ind_baja_ftth end as num_bajas_ftth_3m,

case when c1.ind_baja_voz is null then 0 else c1.ind_baja_voz end + 
case when c2.ind_baja_voz is null then 0 else c2.ind_baja_voz end + 
case when c3.ind_baja_voz is null then 0 else c3.ind_baja_voz end as num_bajas_voz_3m
FROM
(SELECT X_ID_RED AS MSISDN, MAX(X_NUM_IDENT) AS NIF  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = ${hiveconf:MONTH0}
GROUP BY X_ID_RED) A
LEFT JOIN
WORK.WORK_ACT_M0 B1
ON A.NIF = B1.NIF
LEFT JOIN
WORK.WORK_ACT_M1 B2
ON A.NIF = B2.NIF
LEFT JOIN
WORK.WORK_ACT_M2 B3
ON A.NIF = B3.NIF
LEFT JOIN
WORK.WORK_DESACT_M0 C1
ON A.NIF = C1.NIF
LEFT JOIN
WORK.WORK_DESACT_M1 C2
ON A.NIF = C2.NIF
LEFT JOIN
WORK.WORK_DESACT_M2 C3
ON A.NIF = C3.NIF
;

set mapred.job.name = PAR_EXP_ACT_DESACT_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_ACT_DESACT_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_ACT_DESACT_CLI AS     SELECT 
NIF,
${hiveconf:MONTH0} AS MES,
ROUND(MAX(IND_ALTAS_ADSL_3M)) AS IND_ALTAS_ADSL_3M,
ROUND(MAX(IND_ALTAS_LPD_3M)) AS IND_ALTAS_LPD_3M,
ROUND(MAX(IND_ALTAS_FTTH_3M)) AS IND_ALTAS_FTTH_3M,
ROUND(MAX(IND_ALTAS_VOZ_3M)) AS IND_ALTAS_VOZ_3M,
ROUND(MAX(IND_BAJAS_ADSL_3M)) AS IND_BAJAS_ADSL_3M,
ROUND(MAX(IND_BAJAS_LPD_3M)) AS IND_BAJAS_LPD_3M,
ROUND(MAX(IND_BAJAS_FTTH_3M)) AS IND_BAJAS_FTTH_3M,
ROUND(MAX(IND_BAJAS_VOZ_3M)) AS IND_BAJAS_VOZ_3M,
ROUND(SUM(NUM_ALTAS_ADSL_3M)) AS NUM_ALTAS_ADSL_3M,
ROUND(SUM(NUM_ALTAS_LPD_3M)) AS NUM_ALTAS_LPD_3M,
ROUND(SUM(NUM_ALTAS_FTTH_3M)) AS NUM_ALTAS_FTTH_3M,
ROUND(SUM(NUM_ALTAS_VOZ_3M)) AS NUM_ALTAS_VOZ_3M,
ROUND(SUM(NUM_BAJAS_ADSL_3M)) AS NUM_BAJAS_ADSL_3M,
ROUND(SUM(NUM_BAJAS_LPD_3M)) AS NUM_BAJAS_LPD_3M,
ROUND(SUM(NUM_BAJAS_FTTH_3M)) AS NUM_BAJAS_FTTH_3M,
ROUND(SUM(NUM_BAJAS_VOZ_3M)) AS NUM_BAJAS_VOZ_3M
FROM
WORK.WORK_ACT_DESACT_LIN
GROUP BY NIF; 

set mapred.job.name = PAR_EXP_ACT_DESACT_LIN;
DROP TABLE VF_POSTPAID.PAR_EXP_ACT_DESACT_LIN;
CREATE TABLE VF_POSTPAID.PAR_EXP_ACT_DESACT_LIN AS    SELECT 
A.MSISDN,
B.*        
FROM
(SELECT X_ID_RED AS MSISDN, MAX(X_NUM_IDENT) AS NIF  FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = ${hiveconf:MONTH0}
GROUP BY X_ID_RED) A
INNER JOIN
VF_POSTPAID.PAR_EXP_ACT_DESACT_CLI B
ON A.NIF = B.NIF;

DROP TABLE WORK.WORK_ACT_M0;
DROP TABLE WORK.WORK_DESACT_M0;
DROP TABLE WORK.WORK_ACT_M1;
DROP TABLE WORK.WORK_DESACT_M1;
DROP TABLE WORK.WORK_ACT_M2;
DROP TABLE WORK.WORK_DESACT_M2;
DROP TABLE WORK.WORK_ACT_M3;
DROP TABLE WORK.WORK_DESACT_M3;
DROP TABLE WORK.WORK_ACT_DESACT_LIN;

exit;


