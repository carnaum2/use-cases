USE VF_EBU;

     
set mapred.job.name = WORK_TAC_M0;
DROP TABLE WORK.WORK_TAC_M0;
CREATE TABLE WORK.WORK_TAC_M0 AS
SELECT
B.NIF,
A.MSISDN,
CAST(B.NUM_ANDROID AS DOUBLE) AS NUM_DISP_ANDROID,
CAST(B.NUM_IOS AS DOUBLE) AS NUM_DISP_IOS,
CAST(B.NUM_BB AS DOUBLE) AS NUM_DISP_BB,
CAST(B.DISPOSITIVOS AS DOUBLE) AS NUM_DISPOSITIVOS,
CAST(B.DISPOSITIVOS_M AS DOUBLE) AS NUM_DISP_MOVIL,
CAST(B.DISPOSITIVOS_NO_M AS DOUBLE) AS NUM_DISP_NO_MOVIL,
CAST(B.TABLET AS DOUBLE) AS NUM_TABLET,
CAST(B.NOTEBOOK AS DOUBLE) AS NUM_NOTEBOOK,
CAST(B.CAR_PHONE AS DOUBLE) AS NUM_CAR_PHONE,
CAST(B.MESES_NOCHANGE AS DOUBLE) AS NUM_MES_SINCAMBIO_TAC
FROM 
(SELECT DISTINCT MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT * FROM INPUT.VF_TERM_SRV_MES_AG WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') B
ON A.MSISDN = B.MSISDN;
        
set mapred.job.name = WORK_TAC_M1;
DROP TABLE WORK.WORK_TAC_M1;
CREATE TABLE WORK.WORK_TAC_M1 AS
SELECT
B.NIF,
A.MSISDN,
CAST(B.NUM_ANDROID AS DOUBLE) AS NUM_DISP_ANDROID,
CAST(B.NUM_IOS AS DOUBLE) AS NUM_DISP_IOS,
CAST(B.NUM_BB AS DOUBLE) AS NUM_DISP_BB,
CAST(B.DISPOSITIVOS AS DOUBLE) AS NUM_DISPOSITIVOS,
CAST(B.DISPOSITIVOS_M AS DOUBLE) AS NUM_DISP_MOVIL,
CAST(B.DISPOSITIVOS_NO_M AS DOUBLE) AS NUM_DISP_NO_MOVIL,
CAST(B.TABLET AS DOUBLE) AS NUM_TABLET,
CAST(B.NOTEBOOK AS DOUBLE) AS NUM_NOTEBOOK,
CAST(B.CAR_PHONE AS DOUBLE) AS NUM_CAR_PHONE,
CAST(B.MESES_NOCHANGE AS DOUBLE) AS NUM_MES_SINCAMBIO_TAC
FROM (SELECT DISTINCT MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT * FROM INPUT.VF_TERM_SRV_MES_AG WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') B
ON A.MSISDN = B.MSISDN;

        
set mapred.job.name = WORK_TAC_M2;
DROP TABLE WORK.WORK_TAC_M2;
CREATE TABLE WORK.WORK_TAC_M2 AS
SELECT
B.NIF,
A.MSISDN,
CAST(B.NUM_ANDROID AS DOUBLE) AS NUM_DISP_ANDROID,
CAST(B.NUM_IOS AS DOUBLE) AS NUM_DISP_IOS,
CAST(B.NUM_BB AS DOUBLE) AS NUM_DISP_BB,
CAST(B.DISPOSITIVOS AS DOUBLE) AS NUM_DISPOSITIVOS,
CAST(B.DISPOSITIVOS_M AS DOUBLE) AS NUM_DISP_MOVIL,
CAST(B.DISPOSITIVOS_NO_M AS DOUBLE) AS NUM_DISP_NO_MOVIL,
CAST(B.TABLET AS DOUBLE) AS NUM_TABLET,
CAST(B.NOTEBOOK AS DOUBLE) AS NUM_NOTEBOOK,
CAST(B.CAR_PHONE AS DOUBLE) AS NUM_CAR_PHONE,
CAST(B.MESES_NOCHANGE AS DOUBLE) AS NUM_MES_SINCAMBIO_TAC
FROM (SELECT DISTINCT MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT * FROM INPUT.VF_TERM_SRV_MES_AG WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') B
ON A.MSISDN = B.MSISDN;
  
  
set mapred.job.name = WORK_TAC_M3;
DROP TABLE WORK.WORK_TAC_M3;
CREATE TABLE WORK.WORK_TAC_M3 AS
SELECT
B.NIF,
A.MSISDN,
CAST(B.NUM_ANDROID AS DOUBLE) AS NUM_DISP_ANDROID,
CAST(B.NUM_IOS AS DOUBLE) AS NUM_DISP_IOS,
CAST(B.NUM_BB AS DOUBLE) AS NUM_DISP_BB,
CAST(B.DISPOSITIVOS AS DOUBLE) AS NUM_DISPOSITIVOS,
CAST(B.DISPOSITIVOS_M AS DOUBLE) AS NUM_DISP_MOVIL,
CAST(B.DISPOSITIVOS_NO_M AS DOUBLE) AS NUM_DISP_NO_MOVIL,
CAST(B.TABLET AS DOUBLE) AS NUM_TABLET,
CAST(B.NOTEBOOK AS DOUBLE) AS NUM_NOTEBOOK,
CAST(B.CAR_PHONE AS DOUBLE) AS NUM_CAR_PHONE,
CAST(B.MESES_NOCHANGE AS DOUBLE) AS NUM_MES_SINCAMBIO_TAC
FROM (SELECT DISTINCT MSISDN FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT * FROM INPUT.VF_TERM_SRV_MES_AG WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') B
ON A.MSISDN = B.MSISDN;
                  

----------------------------------------------------------------------------------------------
				  
set mapred.job.name = EMP_EXP_TACFAC_LIN;
DROP TABLE VF_EBU.EMP_EXP_TACFAC_LIN;
CREATE TABLE VF_EBU.EMP_EXP_TACFAC_LIN AS
SELECT
M.NIF,
M.MSISDN,
'${hiveconf:MONTH0}'AS MES,

ROUND(VF_FUNC.AvgN(A.NUM_DISP_ANDROID, B.NUM_DISP_ANDROID, C.NUM_DISP_ANDROID, D.NUM_DISP_ANDROID),0) AS NUM_DISP_ANDROID_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_DISP_IOS, B.NUM_DISP_IOS, C.NUM_DISP_IOS, D.NUM_DISP_IOS),0) AS NUM_DISP_IOS_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_DISP_BB, B.NUM_DISP_BB, C.NUM_DISP_BB, D.NUM_DISP_BB),0) AS NUM_DISP_BB_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_DISPOSITIVOS, B.NUM_DISPOSITIVOS, C.NUM_DISPOSITIVOS, D.NUM_DISPOSITIVOS),0) AS NUM_DISPOSITIVOS_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_DISP_MOVIL, B.NUM_DISP_MOVIL, C.NUM_DISP_MOVIL, D.NUM_DISP_MOVIL),0) AS NUM_DISP_MOVIL_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_DISP_NO_MOVIL, B.NUM_DISP_NO_MOVIL, C.NUM_DISP_NO_MOVIL, D.NUM_DISP_NO_MOVIL),0) AS NUM_DISP_NO_MOVIL_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_TABLET, B.NUM_TABLET, C.NUM_TABLET, D.NUM_TABLET),0) AS NUM_TABLET_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_NOTEBOOK, B.NUM_NOTEBOOK, C.NUM_NOTEBOOK, D.NUM_NOTEBOOK),0) AS NUM_NOTEBOOK_AVG,
ROUND(VF_FUNC.AvgN(A.NUM_CAR_PHONE, B.NUM_CAR_PHONE, C.NUM_CAR_PHONE, D.NUM_CAR_PHONE),0) AS NUM_CAR_PHONE_AVG,

ROUND(VF_FUNC.IncN(A.NUM_DISP_ANDROID, B.NUM_DISP_ANDROID, C.NUM_DISP_ANDROID, D.NUM_DISP_ANDROID),0) AS NUM_DISP_ANDROID_INC,
ROUND(VF_FUNC.IncN(A.NUM_DISP_IOS, B.NUM_DISP_IOS, C.NUM_DISP_IOS, D.NUM_DISP_IOS),0) AS NUM_DISP_IOS_INC,
ROUND(VF_FUNC.IncN(A.NUM_DISP_BB, B.NUM_DISP_BB, C.NUM_DISP_BB, D.NUM_DISP_BB),0) AS NUM_DISP_BB_INC,
ROUND(VF_FUNC.IncN(A.NUM_DISPOSITIVOS, B.NUM_DISPOSITIVOS, C.NUM_DISPOSITIVOS, D.NUM_DISPOSITIVOS),0) AS NUM_DISPOSITIVOS_INC,
ROUND(VF_FUNC.IncN(A.NUM_DISP_MOVIL, B.NUM_DISP_MOVIL, C.NUM_DISP_MOVIL, D.NUM_DISP_MOVIL),0) AS NUM_DISP_MOVIL_INC,
ROUND(VF_FUNC.IncN(A.NUM_DISP_NO_MOVIL, B.NUM_DISP_NO_MOVIL, C.NUM_DISP_NO_MOVIL, D.NUM_DISP_NO_MOVIL),0) AS NUM_DISP_NO_MOVIL_INC,
ROUND(VF_FUNC.IncN(A.NUM_TABLET, B.NUM_TABLET, C.NUM_TABLET, D.NUM_TABLET),0) AS NUM_TABLET_INC,
ROUND(VF_FUNC.IncN(A.NUM_NOTEBOOK, B.NUM_NOTEBOOK, C.NUM_NOTEBOOK, D.NUM_NOTEBOOK),0) AS NUM_NOTEBOOK_INC,
ROUND(VF_FUNC.IncN(A.NUM_CAR_PHONE, B.NUM_CAR_PHONE, C.NUM_CAR_PHONE, D.NUM_CAR_PHONE),0) AS NUM_CAR_PHONE_INC,

ROUND(A.NUM_DISP_ANDROID,0)  AS NUM_DISP_ANDROID_M0,
ROUND(A.NUM_DISP_IOS,0)  AS NUM_DISP_IOS_M0,
ROUND(A.NUM_DISP_BB,0)  AS NUM_DISP_BB_M0,
ROUND(A.NUM_DISPOSITIVOS,0)  AS NUM_DISPOSITIVOS_M0,
ROUND(A.NUM_DISP_MOVIL,0)  AS NUM_DISP_MOVIL_M0,
ROUND(A.NUM_DISP_NO_MOVIL,0)  AS NUM_DISP_NO_MOVIL_M0,
ROUND(A.NUM_TABLET,0)  AS NUM_TABLET_M0,
ROUND(A.NUM_NOTEBOOK,0)  AS NUM_NOTEBOOK_M0,
ROUND(A.NUM_CAR_PHONE,0)  AS NUM_CAR_PHONE_M0,
ROUND(A.NUM_MES_SINCAMBIO_TAC,0)  AS NUM_MES_SINCAMBIO_TAC_M0,

ROUND(B.NUM_DISP_ANDROID,0)  AS NUM_DISP_ANDROID_M1,
ROUND(B.NUM_DISP_IOS,0)  AS NUM_DISP_IOS_M1,
ROUND(B.NUM_DISP_BB,0)  AS NUM_DISP_BB_M1,
ROUND(B.NUM_DISPOSITIVOS,0)  AS NUM_DISPOSITIVOS_M1,
ROUND(B.NUM_DISP_MOVIL,0)  AS NUM_DISP_MOVIL_M1,
ROUND(B.NUM_DISP_NO_MOVIL,0)  AS NUM_DISP_NO_MOVIL_M1,
ROUND(B.NUM_TABLET,0)  AS NUM_TABLET_M1,
ROUND(B.NUM_NOTEBOOK,0)  AS NUM_NOTEBOOK_M1,
ROUND(B.NUM_CAR_PHONE,0)  AS NUM_CAR_PHONE_M1,

ROUND(C.NUM_DISP_ANDROID,0)  AS NUM_DISP_ANDROID_M2,
ROUND(C.NUM_DISP_IOS,0)  AS NUM_DISP_IOS_M2,
ROUND(C.NUM_DISP_BB,0)  AS NUM_DISP_BB_M2,
ROUND(C.NUM_DISPOSITIVOS,0)  AS NUM_DISPOSITIVOS_M2,
ROUND(C.NUM_DISP_MOVIL,0)  AS NUM_DISP_MOVIL_M2,
ROUND(C.NUM_DISP_NO_MOVIL,0)  AS NUM_DISP_NO_MOVIL_M2,
ROUND(C.NUM_TABLET,0)  AS NUM_TABLET_M2,
ROUND(C.NUM_NOTEBOOK,0)  AS NUM_NOTEBOOK_M2,
ROUND(C.NUM_CAR_PHONE,0)  AS NUM_CAR_PHONE_M2,

ROUND(D.NUM_DISP_ANDROID,0)  AS NUM_DISP_ANDROID_M3,
ROUND(D.NUM_DISP_IOS,0)  AS NUM_DISP_IOS_M3,
ROUND(D.NUM_DISP_BB,0)  AS NUM_DISP_BB_M3,
ROUND(D.NUM_DISPOSITIVOS,0)  AS NUM_DISPOSITIVOS_M3,
ROUND(D.NUM_DISP_MOVIL,0)  AS NUM_DISP_MOVIL_M3,
ROUND(D.NUM_DISP_NO_MOVIL,0)  AS NUM_DISP_NO_MOVIL_M3,
ROUND(D.NUM_TABLET,0)  AS NUM_TABLET_M3,
ROUND(D.NUM_NOTEBOOK,0)  AS NUM_NOTEBOOK_M3,
ROUND(D.NUM_CAR_PHONE,0)  AS NUM_CAR_PHONE_M3,

CASE WHEN A.NUM_MES_SINCAMBIO_TAC > 1 THEN 1 ELSE 0 END AS IND_SINCAMBIO_TAC_1M,
CASE WHEN A.NUM_MES_SINCAMBIO_TAC > 3 THEN 1 ELSE 0 END AS IND_SINCAMBIO_TAC_3M,
CASE WHEN A.NUM_MES_SINCAMBIO_TAC > 6 THEN 1 ELSE 0 END AS IND_SINCAMBIO_TAC_6M,
CASE WHEN A.NUM_MES_SINCAMBIO_TAC > 12 THEN 1 ELSE 0 END AS IND_SINCAMBIO_TAC_12M

FROM
(SELECT DISTINCT MSISDN, CIF_NIF AS NIF  FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') M
LEFT JOIN
WORK.WORK_TAC_M0 A
ON M.MSISDN = A.MSISDN
LEFT JOIN
WORK.WORK_TAC_M1 B
ON M.MSISDN = B.MSISDN
LEFT JOIN
WORK.WORK_TAC_M2 C
ON M.MSISDN = C.MSISDN
LEFT JOIN
WORK.WORK_TAC_M3 D
ON M.MSISDN = D.MSISDN;
      
set mapred.job.name = WORK_TACFAC_CLI;
DROP TABLE WORK.WORK_TACFAC_CLI;
CREATE TABLE WORK.WORK_TACFAC_CLI AS
SELECT
NIF,
SUM(NUM_DISP_ANDROID_M0) AS NUM_DISP_ANDROID_M0,
SUM(NUM_DISP_IOS_M0) AS NUM_DISP_IOS_M0,
SUM(NUM_DISP_BB_M0) AS NUM_DISP_BB_M0,
SUM(NUM_DISPOSITIVOS_M0) AS NUM_DISPOSITIVOS_M0,
SUM(NUM_DISP_MOVIL_M0) AS NUM_DISP_MOVIL_M0,
SUM(NUM_DISP_NO_MOVIL_M0) AS NUM_DISP_NO_MOVIL_M0,
SUM(NUM_TABLET_M0) AS NUM_TABLET_M0,
SUM(NUM_NOTEBOOK_M0) AS NUM_NOTEBOOK_M0,
SUM(NUM_CAR_PHONE_M0) AS NUM_CAR_PHONE_M0,

SUM(NUM_DISP_ANDROID_M1) AS NUM_DISP_ANDROID_M1,
SUM(NUM_DISP_IOS_M1) AS NUM_DISP_IOS_M1,
SUM(NUM_DISP_BB_M1) AS NUM_DISP_BB_M1,
SUM(NUM_DISPOSITIVOS_M1) AS NUM_DISPOSITIVOS_M1,
SUM(NUM_DISP_MOVIL_M1) AS NUM_DISP_MOVIL_M1,
SUM(NUM_DISP_NO_MOVIL_M1) AS NUM_DISP_NO_MOVIL_M1,
SUM(NUM_TABLET_M1) AS NUM_TABLET_M1,
SUM(NUM_NOTEBOOK_M1) AS NUM_NOTEBOOK_M1,
SUM(NUM_CAR_PHONE_M1) AS NUM_CAR_PHONE_M1,

SUM(NUM_DISP_ANDROID_M2) AS NUM_DISP_ANDROID_M2,
SUM(NUM_DISP_IOS_M2) AS NUM_DISP_IOS_M2,
SUM(NUM_DISP_BB_M2) AS NUM_DISP_BB_M2,
SUM(NUM_DISPOSITIVOS_M2) AS NUM_DISPOSITIVOS_M2,
SUM(NUM_DISP_MOVIL_M2) AS NUM_DISP_MOVIL_M2,
SUM(NUM_DISP_NO_MOVIL_M2) AS NUM_DISP_NO_MOVIL_M2,
SUM(NUM_TABLET_M2) AS NUM_TABLET_M2,
SUM(NUM_NOTEBOOK_M2) AS NUM_NOTEBOOK_M2,
SUM(NUM_CAR_PHONE_M2) AS NUM_CAR_PHONE_M2,

SUM(NUM_DISP_ANDROID_M3) AS NUM_DISP_ANDROID_M3,
SUM(NUM_DISP_IOS_M3) AS NUM_DISP_IOS_M3,
SUM(NUM_DISP_BB_M3) AS NUM_DISP_BB_M3,
SUM(NUM_DISPOSITIVOS_M3) AS NUM_DISPOSITIVOS_M3,
SUM(NUM_DISP_MOVIL_M3) AS NUM_DISP_MOVIL_M3,
SUM(NUM_DISP_NO_MOVIL_M3) AS NUM_DISP_NO_MOVIL_M3,
SUM(NUM_TABLET_M3) AS NUM_TABLET_M3,
SUM(NUM_NOTEBOOK_M3) AS NUM_NOTEBOOK_M3,
SUM(NUM_CAR_PHONE_M3) AS NUM_CAR_PHONE_M3,

SUM(IND_SINCAMBIO_TAC_1M) AS NUM_LIN_SINCAMBIO_TAC_1M,
SUM(IND_SINCAMBIO_TAC_3M) AS NUM_LIN_SINCAMBIO_TAC_3M,
SUM(IND_SINCAMBIO_TAC_6M) AS NUM_LIN_SINCAMBIO_TAC_6M,
SUM(IND_SINCAMBIO_TAC_12M) AS NUM_LIN_SINCAMBIO_TAC_12M

FROM
VF_EBU.EMP_EXP_TACFAC_LIN
GROUP BY NIF;
          
      
set mapred.job.name = EMP_EXP_TACFAC_CLI;
DROP TABLE VF_EBU.EMP_EXP_TACFAC_CLI;
CREATE TABLE VF_EBU.EMP_EXP_TACFAC_CLI AS
SELECT
NIF,
'${hiveconf:MONTH0}' AS MES,

NUM_DISP_ANDROID_M0,
NUM_DISP_IOS_M0,
NUM_DISP_BB_M0,
NUM_DISPOSITIVOS_M0,
NUM_DISP_MOVIL_M0,
NUM_DISP_NO_MOVIL_M0,
NUM_TABLET_M0,
NUM_NOTEBOOK_M0,
NUM_CAR_PHONE_M0,

NUM_LIN_SINCAMBIO_TAC_1M,
NUM_LIN_SINCAMBIO_TAC_3M,
NUM_LIN_SINCAMBIO_TAC_6M,
NUM_LIN_SINCAMBIO_TAC_12M,

ROUND(VF_FUNC.AvgN(NUM_DISP_ANDROID_M0, NUM_DISP_ANDROID_M1, NUM_DISP_ANDROID_M2, NUM_DISP_ANDROID_M3),0) AS NUM_DISP_ANDROID_AVG,
ROUND(VF_FUNC.AvgN(NUM_DISP_IOS_M0, NUM_DISP_IOS_M1, NUM_DISP_IOS_M2, NUM_DISP_IOS_M3),0) AS NUM_DISP_IOS_AVG,
ROUND(VF_FUNC.AvgN(NUM_DISP_BB_M0, NUM_DISP_BB_M1, NUM_DISP_BB_M2, NUM_DISP_BB_M3),0) AS NUM_DISP_BB_AVG,
ROUND(VF_FUNC.AvgN(NUM_DISPOSITIVOS_M0, NUM_DISPOSITIVOS_M1, NUM_DISPOSITIVOS_M2, NUM_DISPOSITIVOS_M3),0) AS NUM_DISPOSITIVOS_AVG,
ROUND(VF_FUNC.AvgN(NUM_DISP_MOVIL_M0, NUM_DISP_MOVIL_M1, NUM_DISP_MOVIL_M2, NUM_DISP_MOVIL_M3),0) AS NUM_DISP_MOVIL_AVG,
ROUND(VF_FUNC.AvgN(NUM_DISP_NO_MOVIL_M0, NUM_DISP_NO_MOVIL_M1, NUM_DISP_NO_MOVIL_M2, NUM_DISP_NO_MOVIL_M3),0) AS NUM_DISP_NO_MOVIL_AVG,
ROUND(VF_FUNC.AvgN(NUM_TABLET_M0, NUM_TABLET_M1, NUM_TABLET_M2, NUM_TABLET_M3),0) AS NUM_TABLET_AVG,
ROUND(VF_FUNC.AvgN(NUM_NOTEBOOK_M0, NUM_NOTEBOOK_M1, NUM_NOTEBOOK_M2, NUM_NOTEBOOK_M3),0) AS NUM_NOTEBOOK_AVG,
ROUND(VF_FUNC.AvgN(NUM_CAR_PHONE_M0, NUM_CAR_PHONE_M1, NUM_CAR_PHONE_M2, NUM_CAR_PHONE_M3),0) AS NUM_CAR_PHONE_AVG,

ROUND(VF_FUNC.IncN(NUM_DISP_ANDROID_M0, NUM_DISP_ANDROID_M1, NUM_DISP_ANDROID_M2, NUM_DISP_ANDROID_M3),0) AS NUM_DISP_ANDROID_INC,
ROUND(VF_FUNC.IncN(NUM_DISP_IOS_M0, NUM_DISP_IOS_M1, NUM_DISP_IOS_M2, NUM_DISP_IOS_M3),0) AS NUM_DISP_IOS_INC,
ROUND(VF_FUNC.IncN(NUM_DISP_BB_M0, NUM_DISP_BB_M1, NUM_DISP_BB_M2, NUM_DISP_BB_M3),0) AS NUM_DISP_BB_INC,
ROUND(VF_FUNC.IncN(NUM_DISPOSITIVOS_M0, NUM_DISPOSITIVOS_M1, NUM_DISPOSITIVOS_M2, NUM_DISPOSITIVOS_M3),0) AS NUM_DISPOSITIVOS_INC,
ROUND(VF_FUNC.IncN(NUM_DISP_MOVIL_M0, NUM_DISP_MOVIL_M1, NUM_DISP_MOVIL_M2, NUM_DISP_MOVIL_M3),0) AS NUM_DISP_MOVIL_INC,
ROUND(VF_FUNC.IncN(NUM_DISP_NO_MOVIL_M0, NUM_DISP_NO_MOVIL_M1, NUM_DISP_NO_MOVIL_M2, NUM_DISP_NO_MOVIL_M3),0) AS NUM_DISP_NO_MOVIL_INC,
ROUND(VF_FUNC.IncN(NUM_TABLET_M0, NUM_TABLET_M1, NUM_TABLET_M2, NUM_TABLET_M3),0) AS NUM_TABLET_INC,
ROUND(VF_FUNC.IncN(NUM_NOTEBOOK_M0, NUM_NOTEBOOK_M1, NUM_NOTEBOOK_M2, NUM_NOTEBOOK_M3),0) AS NUM_NOTEBOOK_INC,
ROUND(VF_FUNC.IncN(NUM_CAR_PHONE_M0, NUM_CAR_PHONE_M1, NUM_CAR_PHONE_M2, NUM_CAR_PHONE_M3),0) AS NUM_CAR_PHONE_INC

FROM 
WORK.WORK_TACFAC_CLI;
    
DROP TABLE WORK.WORK_TAC_M0;
DROP TABLE WORK.WORK_TAC_M1;
DROP TABLE WORK.WORK_TAC_M2;
DROP TABLE WORK.WORK_TAC_M3;
DROP TABLE WORK.WORK_TACFAC_CLI;
	
EXIT;
