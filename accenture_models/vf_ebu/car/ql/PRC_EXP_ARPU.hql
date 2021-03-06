USE VF_EBU;

-- WORK_ARPU_M0

set mapred.job.name = WORK_ARPU_M0;
DROP TABLE WORK.WORK_ARPU_M0;
CREATE TABLE WORK.WORK_ARPU_M0 AS 
SELECT
A.MSISDN AS MSISDN,
SUM(A.FACT_TOT) AS NUM_ARPU_TOT,
SUM(A.FACT_GSM_CON) + SUM(A.FACT_GSM_PRE) AS NUM_ARPU_VOZ,
SUM(A.FACT_GPRS) AS NUM_ARPU_NOVOZ,
SUM(A.FACT_3G) + SUM(A.FACT_INTERNET) AS NUM_ARPU_DATOS
FROM
(SELECT
A.*
FROM
(SELECT
A.TELEFONO AS MSISDN,
NVL(CAST(A.FACT_TOT AS DOUBLE),0) AS FACT_TOT,
NVL(CAST(A.FACT_GSM_PRE AS DOUBLE),0) AS FACT_GSM_PRE,
NVL(CAST(A.FACT_GSM_CON AS DOUBLE),0) AS FACT_GSM_CON,
NVL(CAST(A.FACT_3G AS DOUBLE),0) AS FACT_3G,
NVL(CAST(A.FACT_GPRS AS DOUBLE),0) AS FACT_GPRS,
NVL(CAST(A.FACT_INTERNET AS DOUBLE),0) AS FACT_INTERNET
FROM
(SELECT * FROM INPUT.VF_EBU_BDM_EM_ARPU WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT DISTINCT CIF_NIF AS NIF, MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') B
ON A.TELEFONO = B.MSISDN) A ) A
GROUP BY A.MSISDN;

      
set mapred.job.name = WORK_ARPU_M1;
DROP TABLE WORK.WORK_ARPU_M1;
CREATE TABLE WORK.WORK_ARPU_M1 AS 
SELECT
A.MSISDN AS MSISDN,
SUM(A.FACT_TOT) AS NUM_ARPU_TOT,
SUM(A.FACT_GSM_CON) + SUM(A.FACT_GSM_PRE) AS NUM_ARPU_VOZ,
SUM(A.FACT_GPRS) AS NUM_ARPU_NOVOZ,
SUM(A.FACT_3G) + SUM(A.FACT_INTERNET) AS NUM_ARPU_DATOS
FROM
(SELECT
 A.*
FROM
  (SELECT
	  A.TELEFONO AS MSISDN,
NVL(CAST(A.FACT_TOT AS DOUBLE),0) AS FACT_TOT,
NVL(CAST(A.FACT_GSM_PRE AS DOUBLE),0) AS FACT_GSM_PRE,
NVL(CAST(A.FACT_GSM_CON AS DOUBLE),0) AS FACT_GSM_CON,
NVL(CAST(A.FACT_3G AS DOUBLE),0) AS FACT_3G,
NVL(CAST(A.FACT_GPRS AS DOUBLE),0) AS FACT_GPRS,
NVL(CAST(A.FACT_INTERNET AS DOUBLE),0 ) AS FACT_INTERNET
	FROM
	  (SELECT * FROM INPUT.VF_EBU_BDM_EM_ARPU WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') A 
	  INNER JOIN
	  (SELECT DISTINCT CIF_NIF AS NIF, MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') B
	  ON A.TELEFONO = B.MSISDN) A ) A
GROUP BY A.MSISDN;
      
	  
	  
set mapred.job.name = WORK_ARPU_M2;
DROP TABLE WORK.WORK_ARPU_M2;
CREATE TABLE WORK.WORK_ARPU_M2 AS 
SELECT
  A.MSISDN,
  SUM(A.FACT_TOT) NUM_ARPU_TOT,
  SUM(A.FACT_GSM_CON) + SUM(A.FACT_GSM_PRE) AS NUM_ARPU_VOZ,
  SUM(A.FACT_GPRS) AS NUM_ARPU_NOVOZ,
  SUM(A.FACT_3G) + SUM(A.FACT_INTERNET) AS NUM_ARPU_DATOS
FROM
  (SELECT
	 A.*
  FROM
	  (SELECT
		  A.TELEFONO AS MSISDN,
NVL(CAST(A.FACT_TOT AS DOUBLE),0) AS FACT_TOT,
NVL(CAST(A.FACT_GSM_PRE AS DOUBLE),0) AS FACT_GSM_PRE,
NVL(CAST(A.FACT_GSM_CON AS DOUBLE),0) AS FACT_GSM_CON,
NVL(CAST(A.FACT_3G AS DOUBLE),0) AS FACT_3G,
NVL(CAST(A.FACT_GPRS AS DOUBLE),0) AS FACT_GPRS,
NVL(CAST(A.FACT_INTERNET AS DOUBLE),0) AS FACT_INTERNET
		FROM
		  (SELECT * FROM INPUT.VF_EBU_BDM_EM_ARPU WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') A 
		  INNER JOIN
		  (SELECT DISTINCT CIF_NIF AS NIF, MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') B
		  ON A.TELEFONO = B.MSISDN) A ) A
GROUP BY A.MSISDN;
      

set mapred.job.name = WORK_ARPU_M3;
DROP TABLE WORK.WORK_ARPU_M3;
CREATE TABLE WORK.WORK_ARPU_M3 AS 
SELECT
A.MSISDN,
SUM(A.FACT_TOT) AS NUM_ARPU_TOT,
SUM(A.FACT_GSM_CON) + SUM(A.FACT_GSM_PRE) AS NUM_ARPU_VOZ,
SUM(A.FACT_GPRS) AS NUM_ARPU_NOVOZ,
SUM(A.FACT_3G) + SUM(A.FACT_INTERNET) AS NUM_ARPU_DATOS
FROM
(SELECT
 A.*
FROM
  (SELECT
	  A.TELEFONO AS MSISDN,
NVL(CAST(A.FACT_TOT AS DOUBLE),0) AS FACT_TOT,
NVL(CAST(A.FACT_GSM_PRE AS DOUBLE),0) AS FACT_GSM_PRE,
NVL(CAST(A.FACT_GSM_CON AS DOUBLE),0) AS FACT_GSM_CON,
NVL(CAST(A.FACT_3G AS DOUBLE),0) AS FACT_3G,
NVL(CAST(A.FACT_GPRS AS DOUBLE),0) AS FACT_GPRS,
NVL(CAST(A.FACT_INTERNET AS DOUBLE),0) AS FACT_INTERNET
	FROM
	  (SELECT * FROM INPUT.VF_EBU_BDM_EM_ARPU WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') A 
	  INNER JOIN
	  (SELECT DISTINCT CIF_NIF AS NIF, MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') B
	  ON A.TELEFONO = B.MSISDN) A ) A
GROUP BY A.MSISDN;

    
set mapred.job.name = EMP_EXP_ARPU_LIN;
DROP TABLE VF_EBU.EMP_EXP_ARPU_LIN;
CREATE TABLE VF_EBU.EMP_EXP_ARPU_LIN AS
SELECT
M.NIF AS NIF,
M.MSISDN,
'${hiveconf:MONTH0}' AS MES,

ROUND(A.NUM_ARPU_TOT,2) AS NUM_ARPU_TOT_M0,
ROUND(VF_FUNC.AvgN(A.NUM_ARPU_TOT, B.NUM_ARPU_TOT, C.NUM_ARPU_TOT, D.NUM_ARPU_TOT),2) AS NUM_ARPU_TOT_AVG,
ROUND(VF_FUNC.IncN(A.NUM_ARPU_TOT, B.NUM_ARPU_TOT, C.NUM_ARPU_TOT, D.NUM_ARPU_TOT),2) AS NUM_ARPU_TOT_INC,

ROUND(A.NUM_ARPU_VOZ,2) AS NUM_ARPU_VOZ_M0,
ROUND(VF_FUNC.AvgN(A.NUM_ARPU_VOZ, B.NUM_ARPU_VOZ, C.NUM_ARPU_VOZ, D.NUM_ARPU_VOZ),2) AS NUM_ARPU_VOZ_AVG,
ROUND(VF_FUNC.IncN(A.NUM_ARPU_VOZ, B.NUM_ARPU_VOZ, C.NUM_ARPU_VOZ, D.NUM_ARPU_VOZ),2) AS NUM_ARPU_VOZ_INC,

ROUND(A.NUM_ARPU_NOVOZ,2) AS NUM_ARPU_NOVOZ_M0,
ROUND(VF_FUNC.AvgN(A.NUM_ARPU_NOVOZ, B.NUM_ARPU_NOVOZ, C.NUM_ARPU_NOVOZ, D.NUM_ARPU_NOVOZ),2) AS NUM_ARPU_NOVOZ_AVG,
ROUND(VF_FUNC.IncN(A.NUM_ARPU_NOVOZ, B.NUM_ARPU_NOVOZ, C.NUM_ARPU_NOVOZ, D.NUM_ARPU_NOVOZ),2) AS NUM_ARPU_NOVOZ_INC,

ROUND(A.NUM_ARPU_DATOS,2) AS NUM_ARPU_DATOS_M0,
ROUND(VF_FUNC.AvgN(A.NUM_ARPU_DATOS, B.NUM_ARPU_DATOS, C.NUM_ARPU_DATOS, D.NUM_ARPU_NOVOZ),2) AS NUM_ARPU_DATOS_AVG,
ROUND(VF_FUNC.IncN(A.NUM_ARPU_DATOS, B.NUM_ARPU_DATOS, C.NUM_ARPU_DATOS, D.NUM_ARPU_DATOS),2) AS NUM_ARPU_DATOS_INC,        

ROUND(B.NUM_ARPU_TOT,2) AS NUM_ARPU_TOT_M1,
ROUND(C.NUM_ARPU_TOT,2) AS NUM_ARPU_TOT_M2,
ROUND(D.NUM_ARPU_TOT,2) AS NUM_ARPU_TOT_M3,

ROUND(B.NUM_ARPU_VOZ,2) AS NUM_ARPU_VOZ_M1,
ROUND(C.NUM_ARPU_VOZ,2) AS NUM_ARPU_VOZ_M2,
ROUND(D.NUM_ARPU_VOZ,2) AS NUM_ARPU_VOZ_M3,

ROUND(B.NUM_ARPU_NOVOZ,2) AS NUM_ARPU_NOVOZ_M1,
ROUND(C.NUM_ARPU_NOVOZ,2) AS NUM_ARPU_NOVOZ_M2,
ROUND(D.NUM_ARPU_NOVOZ,2) AS NUM_ARPU_NOVOZ_M3,

ROUND(B.NUM_ARPU_DATOS,2) AS NUM_ARPU_DATOS_M1,
ROUND(C.NUM_ARPU_DATOS,2) AS NUM_ARPU_DATOS_M2,
ROUND(D.NUM_ARPU_DATOS,2) AS NUM_ARPU_DATOS_M3

FROM
(SELECT DISTINCT CIF_NIF AS NIF, MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') M
LEFT JOIN
WORK.WORK_ARPU_M0 A
ON M.MSISDN = A.MSISDN
LEFT JOIN
WORK.WORK_ARPU_M1 B
ON M.MSISDN = B.MSISDN
LEFT JOIN
WORK.WORK_ARPU_M2 C
ON M.MSISDN = C.MSISDN
LEFT JOIN
WORK.WORK_ARPU_M3 D
ON M.MSISDN = D.MSISDN;
        
		
set mapred.job.name = WORK_ARPU_CLI;
DROP TABLE WORK.WORK_ARPU_CLI;
CREATE TABLE WORK.WORK_ARPU_CLI AS 
SELECT
NIF,

SUM(NUM_ARPU_DATOS_M0) AS NUM_ARPU_DATOS_M0,
SUM(NUM_ARPU_DATOS_M1) AS NUM_ARPU_DATOS_M1,
SUM(NUM_ARPU_DATOS_M2) AS NUM_ARPU_DATOS_M2,
SUM(NUM_ARPU_DATOS_M3) AS NUM_ARPU_DATOS_M3,

SUM(NUM_ARPU_VOZ_M0) AS NUM_ARPU_VOZ_M0,
SUM(NUM_ARPU_VOZ_M1) AS NUM_ARPU_VOZ_M1,
SUM(NUM_ARPU_VOZ_M2) AS NUM_ARPU_VOZ_M2,
SUM(NUM_ARPU_VOZ_M3) AS NUM_ARPU_VOZ_M3,

SUM(NUM_ARPU_NOVOZ_M0) AS NUM_ARPU_NOVOZ_M0,
SUM(NUM_ARPU_NOVOZ_M1) AS NUM_ARPU_NOVOZ_M1,
SUM(NUM_ARPU_NOVOZ_M2) AS NUM_ARPU_NOVOZ_M2,
SUM(NUM_ARPU_NOVOZ_M3) AS NUM_ARPU_NOVOZ_M3,

SUM(NUM_ARPU_TOT_M0) AS NUM_ARPU_TOT_M0,
SUM(NUM_ARPU_TOT_M1) AS NUM_ARPU_TOT_M1,
SUM(NUM_ARPU_TOT_M2) AS NUM_ARPU_TOT_M2,
SUM(NUM_ARPU_TOT_M3) AS NUM_ARPU_TOT_M3

FROM
VF_EBU.EMP_EXP_ARPU_LIN
GROUP BY NIF;
	  
	  
	  
	  
set mapred.job.name = EMP_EXP_ARPU_CLI;
DROP TABLE VF_EBU.EMP_EXP_ARPU_CLI;
CREATE TABLE VF_EBU.EMP_EXP_ARPU_CLI AS
SELECT 
NIF,
'${hiveconf:MONTH0}' AS MES,     
	  
NUM_ARPU_DATOS_M0,
NUM_ARPU_VOZ_M0,
NUM_ARPU_NOVOZ_M0,
NUM_ARPU_TOT_M0,

ROUND(VF_FUNC.AvgN(NUM_ARPU_DATOS_M0,NUM_ARPU_DATOS_M1,NUM_ARPU_DATOS_M2,NUM_ARPU_DATOS_M3),2) AS NUM_ARPU_DATOS_AVG,
ROUND(VF_FUNC.AvgN(NUM_ARPU_VOZ_M0,NUM_ARPU_VOZ_M1,NUM_ARPU_VOZ_M2,NUM_ARPU_VOZ_M3),2) AS NUM_ARPU_VOZ_AVG,
ROUND(VF_FUNC.AvgN(NUM_ARPU_NOVOZ_M0,NUM_ARPU_NOVOZ_M1,NUM_ARPU_NOVOZ_M2,NUM_ARPU_NOVOZ_M3),2) AS NUM_ARPU_NOVOZ_AVG,
ROUND(VF_FUNC.AvgN(NUM_ARPU_TOT_M0,NUM_ARPU_TOT_M1,NUM_ARPU_TOT_M2,NUM_ARPU_TOT_M3),2) AS NUM_ARPU_TOT_AVG,

ROUND(VF_FUNC.IncN(NUM_ARPU_DATOS_M0,NUM_ARPU_DATOS_M1,NUM_ARPU_DATOS_M2,NUM_ARPU_DATOS_M3),2) AS NUM_ARPU_DATOS_INC,
ROUND(VF_FUNC.IncN(NUM_ARPU_VOZ_M0,NUM_ARPU_VOZ_M1,NUM_ARPU_VOZ_M2,NUM_ARPU_VOZ_M3),2) AS NUM_ARPU_VOZ_INC,
ROUND(VF_FUNC.IncN(NUM_ARPU_NOVOZ_M0,NUM_ARPU_NOVOZ_M1,NUM_ARPU_NOVOZ_M2,NUM_ARPU_NOVOZ_M3),2) AS NUM_ARPU_NOVOZ_INC,
ROUND(VF_FUNC.IncN(NUM_ARPU_TOT_M0,NUM_ARPU_TOT_M1,NUM_ARPU_TOT_M2,NUM_ARPU_TOT_M3),2) AS NUM_ARPU_TOT_INC

FROM
WORK.WORK_ARPU_CLI;

DROP TABLE WORK.WORK_ARPU_M0;
DROP TABLE WORK.WORK_ARPU_M1;
DROP TABLE WORK.WORK_ARPU_M2;
DROP TABLE WORK.WORK_ARPU_M3;
DROP TABLE WORK.WORK_ARPU_CLI;

EXIT;