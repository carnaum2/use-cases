USE VF_EBU;

set mapred.job.name = WORK_PRODUCTOS;
DROP TABLE WORK.WORK_PRODUCTOS;
CREATE TABLE WORK.WORK_PRODUCTOS AS 
SELECT 
A.CIF_NIF, 
MAX(A.NUM_LINEA_RED_M0) AS NUM_LINEAS_RED,
MAX(VF_FUNC.AVGN(A.NUM_LINEA_RED_M0,B.NUM_LINEA_RED_M0, C.NUM_LINEA_RED_M0, D.NUM_LINEA_RED_M0)) AS NUM_LINEAS_RED_AVG,
MAX(VF_FUNC.INCN(A.NUM_LINEA_RED_M0,B.NUM_LINEA_RED_M0, C.NUM_LINEA_RED_M0, D.NUM_LINEA_RED_M0)) AS NUM_LINEAS_RED_INC
FROM 
(SELECT 
A.CIF_NIF, 
SUM(CASE WHEN B.PLAN IS NOT NULL THEN 1 ELSE 0 END) AS NUM_LINEA_RED_M0 
FROM  
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
LEFT JOIN 
MASTER.VF_EBU_CAT_EMP_PLANES_RED  B 
ON A.PP_VOZ = B.PLAN GROUP BY CIF_NIF) A
LEFT JOIN        
(SELECT 
A.CIF_NIF, 
SUM(CASE WHEN B.PLAN IS NOT NULL THEN 1 ELSE 0 END) AS NUM_LINEA_RED_M0 
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}') A
LEFT JOIN MASTER.VF_EBU_CAT_EMP_PLANES_RED  B ON 
A.PP_VOZ = B.PLAN GROUP BY CIF_NIF) B
ON A.CIF_NIF = B.CIF_NIF
LEFT JOIN
(SELECT 
A.CIF_NIF, 
SUM(CASE WHEN B.PLAN IS NOT NULL THEN 1 ELSE 0 END) AS NUM_LINEA_RED_M0 
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH2}') A
LEFT JOIN MASTER.VF_EBU_CAT_EMP_PLANES_RED  B 
ON A.PP_VOZ = B.PLAN GROUP BY CIF_NIF) C 
ON A.CIF_NIF = C.CIF_NIF
LEFT JOIN
(SELECT 
A.CIF_NIF, 
SUM(CASE WHEN B.PLAN IS NOT NULL THEN 1 ELSE 0 END) AS NUM_LINEA_RED_M0 
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH3}') A
LEFT JOIN MASTER.VF_EBU_CAT_EMP_PLANES_RED  B 
ON A.PP_VOZ = B.PLAN GROUP BY CIF_NIF) D  
ON A.CIF_NIF = D.CIF_NIF
GROUP BY A.CIF_NIF;
     
       
set mapred.job.name = WORK_NUM_LINEAS_M0;
DROP TABLE WORK.WORK_NUM_LINEAS_M0;
CREATE TABLE WORK.WORK_NUM_LINEAS_M0 AS
SELECT 
A.CIF_NIF AS NIF,
MAX(NUM_LINEAS_VOZ) AS NUM_LINEAS_VOZ,
MAX(CASE WHEN NUM_SERVICIOS = 0 THEN 0 ELSE CAST(NUM_ADSL AS DOUBLE) / NUM_SERVICIOS END) AS NUM_PCT_ADSL_LINEAS,
--CASE WHEN NUM_LINEAS_VOZ = 0 THEN 0 ELSE  NUM_RED / NUM_LINEAS_VOZ END AS NUM_PCT_RED_LIN_VOZ,
MAX(CASE WHEN NUM_LINEAS_VOZ = 0 THEN 0 ELSE CAST(NUM_HBD AS DOUBLE) / NUM_LINEAS_VOZ END) AS NUM_PCT_HBD_LIN_VOZ,
MAX(CASE WHEN NUM_LINEAS_VOZ = 0 THEN 0 ELSE  CAST(NUM_LPD AS DOUBLE) / NUM_LINEAS_VOZ END) AS NUM_PCT_LPD_LIN_VOZ,
MAX(NUM_SERVICIOS) AS NUM_SERVICIOS
FROM
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A 
LEFT JOIN
(SELECT
A.NIF,
COUNT(*) AS NUM_SERVICIOS
FROM
(SELECT 
CIF_NIF AS NIF, 
MSISDN  
FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
GROUP BY A.NIF) B
ON A.CIF_NIF = B.NIF
LEFT JOIN
(SELECT 
A.CIF_NIF, 
COUNT(*) AS NUM_LINEAS_VOZ  
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT DISTINCT CODIGO FROM INPUT.VF_EBU_EMP_PLANES_VOZ  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') B
ON A.PP_VOZ=B.CODIGO
GROUP BY CIF_NIF) C
ON A.CIF_NIF = C.CIF_NIF
GROUP BY A.CIF_NIF;

       
set mapred.job.name = WORK_NUM_LINEAS_M1;
DROP TABLE WORK.WORK_NUM_LINEAS_M1;
CREATE TABLE WORK.WORK_NUM_LINEAS_M1 AS
SELECT
A.CIF_NIF AS NIF,
MAX(B.NUM_LINEAS_VOZ) AS NUM_LINEAS_VOZ
FROM
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') A
LEFT JOIN
(SELECT 
A.CIF_NIF, 
COUNT(*) AS NUM_LINEAS_VOZ  
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}') A
INNER JOIN
(SELECT DISTINCT CODIGO FROM INPUT.VF_EBU_EMP_PLANES_VOZ  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}') B
ON A.PP_VOZ=B.CODIGO
GROUP BY CIF_NIF) B
ON A.CIF_NIF = B.CIF_NIF
GROUP BY A.CIF_NIF;
        

set mapred.job.name = WORK_NUM_LINEAS_M2;
DROP TABLE WORK.WORK_NUM_LINEAS_M2;
CREATE TABLE WORK.WORK_NUM_LINEAS_M2 AS
SELECT
A.CIF_NIF AS NIF,
MAX(B.NUM_LINEAS_VOZ) AS NUM_LINEAS_VOZ
FROM
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') A
LEFT JOIN
(SELECT 
A.CIF_NIF, 
COUNT(*) AS NUM_LINEAS_VOZ  
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH2}') A
INNER JOIN
(SELECT DISTINCT CODIGO FROM INPUT.VF_EBU_EMP_PLANES_VOZ  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH2}') B
ON A.PP_VOZ=B.CODIGO
GROUP BY CIF_NIF) B
ON A.CIF_NIF = B.CIF_NIF
GROUP BY A.CIF_NIF;


set mapred.job.name = WORK_NUM_LINEAS_M3;
DROP TABLE WORK.WORK_NUM_LINEAS_M3;
CREATE TABLE WORK.WORK_NUM_LINEAS_M3 AS
SELECT
A.CIF_NIF AS NIF,
MAX(B.NUM_LINEAS_VOZ) AS NUM_LINEAS_VOZ
FROM
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}') A
LEFT JOIN
(SELECT 
A.CIF_NIF, 
COUNT(*) AS NUM_LINEAS_VOZ  
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_SERVICIO WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH3}') A
INNER JOIN
(SELECT DISTINCT CODIGO FROM INPUT.VF_EBU_EMP_PLANES_VOZ  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH3}') B
ON A.PP_VOZ=B.CODIGO
GROUP BY CIF_NIF) B
ON A.CIF_NIF = B.CIF_NIF
GROUP BY A.CIF_NIF;

          
set mapred.job.name = EMP_EXP_VOZ_CLI;
DROP TABLE VF_EBU.EMP_EXP_VOZ_CLI;
CREATE TABLE VF_EBU.EMP_EXP_VOZ_CLI AS
SELECT
A.NIF,
'${hiveconf:MONTH0}' AS MES,
ROUND(NUM_PCT_ADSL_LINEAS,0) AS NUM_PCT_ADSL_LINEAS,
ROUND(NUM_PCT_LPD_LIN_VOZ,0) AS NUM_PCT_LPD_LIN_VOZ,
ROUND(NUM_PCT_HBD_LIN_VOZ,0) AS NUM_PCT_HBD_LIN_VOZ,
--NUM_PCT_RED_LIN_VOZ,
ROUND(A.NUM_SERVICIOS,0) AS NUM_SERVICIOS_M0,
ROUND(A.NUM_LINEAS_VOZ,0) AS NUM_LINEAS_VOZ_M0,
ROUND(VF_FUNC.AVGN(A.NUM_LINEAS_VOZ,B.NUM_LINEAS_VOZ, C.NUM_LINEAS_VOZ, D.NUM_LINEAS_VOZ),0) AS NUM_LINEAS_VOZ_AVG,
ROUND(VF_FUNC.INCN(A.NUM_LINEAS_VOZ,B.NUM_LINEAS_VOZ, C.NUM_LINEAS_VOZ, D.NUM_LINEAS_VOZ),0) AS NUM_LINEAS_VOZ_INC,
ROUND(NUM_LINEAS_RED,0) AS NUM_LINEAS_RED,
ROUND(NUM_LINEAS_RED_AVG,0) AS NUM_LINEAS_RED_AVG,
ROUND(NUM_LINEAS_RED_INC,0) AS NUM_LINEAS_RED_INC
FROM
WORK.WORK_NUM_LINEAS_M0 A
LEFT JOIN
WORK.WORK_NUM_LINEAS_M1 B
ON A.NIF = B.NIF
LEFT JOIN
WORK.WORK_NUM_LINEAS_M2 C
ON A.NIF = C.NIF
LEFT JOIN
WORK.WORK_NUM_LINEAS_M3 D
ON A.NIF = D.NIF
LEFT JOIN WORK.WORK_PRODUCTOS E
ON A.NIF = E.CIF_NIF;  

DROP TABLE WORK.WORK_PRODUCTOS;
DROP TABLE WORK.WORK_NUM_LINEAS_M0;
DROP TABLE WORK.WORK_NUM_LINEAS_M1;
DROP TABLE WORK.WORK_NUM_LINEAS_M2;
DROP TABLE WORK.WORK_NUM_LINEAS_M3;
      
EXIT;
