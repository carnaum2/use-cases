USE VF_EBU;          

set mapred.job.name = WORK_CLIENTE_M0;
DROP TABLE WORK.WORK_CLIENTE_M0;
 CREATE TABLE WORK.WORK_CLIENTE_M0 AS
       SELECT
          CIF_NIF AS NIF,
          MAX(SEGMENTACION_MERCADO) AS SEG_SEGMENTO_MERCADO,
          MAX(TIPO_CIFNIF) AS SEG_TIPO_CIFNIF,
          MAX(NVL(CAST(NUM_AC AS DOUBLE),0)) AS NUM_LINEAS,
          MAX(NVL(CAST(NUM_DT AS DOUBLE),0)) AS NUM_LINEAS_DT,
		  MAX(CASE WHEN CAST(NUM_DT AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS IND_LINEAS_DT,
          MAX(NVL(CAST(NUM_LPD AS DOUBLE),0)) AS NUM_LPD,
          MAX(CASE WHEN CAST(NUM_LPD AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS IND_LPD,
          MAX(NVL(CAST(NUM_HBD AS DOUBLE),0)) AS NUM_HHBD,
          MAX(CASE WHEN CAST(NUM_HBD AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS IND_HHBD,
          MAX(NVL(CAST(NUM_ADSL AS DOUBLE),0)) AS NUM_ADSL,
          MAX(CASE WHEN CAST(NUM_ADSL AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS IND_ADSL,
          MAX(NVL(CAST(NUM_FIJOPORTADO AS DOUBLE),0)) AS NUM_FIJOPORTADO,
          MAX(CASE WHEN CAST(NUM_FIJOPORTADO AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS IND_FIJOPORTADO
       FROM
          INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}'
	GROUP BY CIF_NIF;
          
set mapred.job.name = WORK_CLIENTE_M1;
DROP TABLE WORK.WORK_CLIENTE_M1;
 CREATE TABLE WORK.WORK_CLIENTE_M1 AS
       SELECT
          CIF_NIF AS NIF,
          MAX(NVL(CAST(NUM_AC AS DOUBLE),0)) AS NUM_LINEAS,
          MAX(NVL(CAST(NUM_DT AS DOUBLE),0)) AS NUM_LINEAS_DT,
          MAX(NVL(CAST(NUM_LPD AS DOUBLE),0)) AS NUM_LPD,
          MAX(NVL(CAST(NUM_HBD AS DOUBLE),0)) AS NUM_HHBD,
          MAX(NVL(CAST(NUM_ADSL AS DOUBLE),0)) AS NUM_ADSL,
          MAX(NVL(CAST(NUM_FIJOPORTADO AS DOUBLE),0)) AS NUM_FIJOPORTADO
       FROM
          INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}'
		  GROUP BY CIF_NIF;
          
set mapred.job.name = WORK_CLIENTE_M2;
DROP TABLE WORK.WORK_CLIENTE_M2;
 CREATE TABLE WORK.WORK_CLIENTE_M2 AS
       SELECT
          CIF_NIF AS NIF,
          MAX(NVL(CAST(NUM_AC AS DOUBLE),0)) AS NUM_LINEAS,
          MAX(NVL(CAST (NUM_DT AS DOUBLE),0)) AS NUM_LINEAS_DT,
          MAX(NVL(CAST (NUM_LPD AS DOUBLE),0)) AS NUM_LPD,
          MAX(NVL(CAST (NUM_HBD AS DOUBLE),0)) AS NUM_HHBD,
          MAX(NVL(CAST (NUM_ADSL AS DOUBLE),0)) AS NUM_ADSL,
          MAX(NVL(CAST (NUM_FIJOPORTADO AS DOUBLE),0)) AS NUM_FIJOPORTADO
       FROM
          INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}'
		  	GROUP BY CIF_NIF;
      
set mapred.job.name = WORK_CLIENTE_M3;
DROP TABLE WORK.WORK_CLIENTE_M3;
 CREATE TABLE WORK.WORK_CLIENTE_M3 AS
       SELECT
          CIF_NIF AS NIF,
          MAX(NVL(CAST(NUM_AC AS DOUBLE),0)) AS NUM_LINEAS,
          MAX(NVL(CAST (NUM_DT AS DOUBLE),0)) AS NUM_LINEAS_DT,
          MAX(NVL(CAST (NUM_LPD AS DOUBLE),0)) AS NUM_LPD,
          MAX(NVL(CAST (NUM_HBD AS DOUBLE),0)) AS NUM_HHBD,
          MAX(NVL(CAST (NUM_ADSL AS DOUBLE),0)) AS NUM_ADSL,
          MAX(NVL(CAST (NUM_FIJOPORTADO AS DOUBLE),0)) AS NUM_FIJOPORTADO
       FROM
          INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}'
		  	GROUP BY CIF_NIF;
        
set mapred.job.name = EMP_EXP_CLIENTE;
DROP TABLE VF_EBU.EMP_EXP_CLIENTE;
CREATE TABLE VF_EBU.EMP_EXP_CLIENTE AS
SELECT
A0.NIF, 
A0.IND_LINEAS_DT,
A0.IND_LPD,
A0.IND_HHBD,
A0.IND_ADSL,
A0.IND_FIJOPORTADO,
A0.SEG_SEGMENTO_MERCADO,
A0.SEG_TIPO_CIFNIF,
NVL(CAST(A0.NUM_LINEAS AS DOUBLE),0) AS NUM_LINEAS,   
NVL(CAST(A0.NUM_LINEAS_DT AS DOUBLE),0) AS NUM_LINEAS_DT,
NVL(CAST(A0.NUM_LPD AS DOUBLE),0) AS NUM_LPD,
NVL(CAST(A0.NUM_HHBD AS DOUBLE),0) AS NUM_HHBD,
NVL(CAST(A0.NUM_ADSL AS DOUBLE),0) AS NUM_ADSL,
NVL(CAST (A0.NUM_FIJOPORTADO AS DOUBLE),0) AS NUM_FIJOPORTADO,
ROUND(VF_FUNC.AVGN(A0.NUM_LINEAS,A1.NUM_LINEAS,A2.NUM_LINEAS,A3.NUM_LINEAS),2) AS NUM_LINEAS_AVG,
ROUND(VF_FUNC.AVGN(A0.NUM_LINEAS_DT,A1.NUM_LINEAS_DT,A2.NUM_LINEAS_DT,A3.NUM_LINEAS_DT),2) AS NUM_LINEAS_DT_AVG,
ROUND(VF_FUNC.INCN(A0.NUM_LINEAS,A1.NUM_LINEAS,A2.NUM_LINEAS,A3.NUM_LINEAS),2) AS NUM_LINEAS_INC,
ROUND(VF_FUNC.INCN(A0.NUM_LINEAS_DT,A1.NUM_LINEAS_DT,A2.NUM_LINEAS_DT,A3.NUM_LINEAS_DT),2) AS NUM_LINEAS_DT_INC,
CASE 
WHEN A0.NUM_ADSL - A1.NUM_ADSL = 0 THEN 'ESTABLE'
WHEN A0.NUM_ADSL - A1.NUM_ADSL < 0 THEN 'DOWN'
WHEN A0.NUM_ADSL - A1.NUM_ADSL > 0 AND (A1.NUM_ADSL = 0) THEN 'CROSS'
WHEN A0.NUM_ADSL - A1.NUM_ADSL > 0 AND (A1.NUM_ADSL > 0) THEN 'UP' END AS SEG_NUM_ADSL,
ROUND(VF_FUNC.INCN(A0.NUM_ADSL,A1.NUM_ADSL,A2.NUM_ADSL,A3.NUM_ADSL),2) AS NUM_ADSL_INC,
CASE WHEN A1.NIF IS NULL THEN 1 ELSE 0 END AS IND_ALTA_CLI,
CASE WHEN A1.NIF IS NULL OR A2.NIF IS NULL OR A3.NIF IS NULL THEN 1 ELSE 0 END AS IND_ALTA_CLI_3M,
CASE 
WHEN A0.NUM_LPD - A1.NUM_LPD = 0 THEN 'ESTABLE'
WHEN A0.NUM_LPD - A1.NUM_LPD < 0 THEN 'DOWN'
WHEN A0.NUM_LPD - A1.NUM_LPD > 0 AND (A1.NUM_LPD = 0) THEN 'CROSS'
WHEN A0.NUM_LPD - A1.NUM_LPD > 0 AND (A1.NUM_LPD > 0) THEN 'UP' END AS SEG_NUM_LPD, 
ROUND(VF_FUNC.INCN(A0.NUM_LPD,A1.NUM_LPD,A2.NUM_LPD,A3.NUM_LPD),2) AS NUM_LPD_INC,

CASE 
WHEN A0.NUM_HHBD - A1.NUM_HHBD = 0 THEN 'ESTABLE'
WHEN A0.NUM_HHBD - A1.NUM_HHBD < 0 THEN 'DOWN'
WHEN A0.NUM_HHBD - A1.NUM_HHBD > 0 AND (A1.NUM_HHBD = 0) THEN 'CROSS'
WHEN A0.NUM_HHBD - A1.NUM_HHBD > 0 AND (A1.NUM_HHBD > 0) THEN 'UP'
END AS SEG_NUM_HHBD, 
ROUND(VF_FUNC.INCN(A0.NUM_HHBD,A1.NUM_HHBD,A2.NUM_HHBD,A3.NUM_HHBD),2) AS NUM_HHBD_INC,

CASE 
WHEN A0.NUM_FIJOPORTADO - A1.NUM_FIJOPORTADO = 0 THEN 'ESTABLE'
WHEN A0.NUM_FIJOPORTADO - A1.NUM_FIJOPORTADO < 0 THEN 'DOWN'
WHEN A0.NUM_FIJOPORTADO - A1.NUM_FIJOPORTADO > 0 AND (A1.NUM_FIJOPORTADO = 0) THEN 'CROSS'
WHEN A0.NUM_FIJOPORTADO - A1.NUM_FIJOPORTADO > 0 AND (A1.NUM_FIJOPORTADO > 0) THEN 'UP'
END AS SEG_NUM_FIJOPORTADO,
ROUND(VF_FUNC.INCN(A0.NUM_FIJOPORTADO,A1.NUM_FIJOPORTADO,A2.NUM_FIJOPORTADO,A3.NUM_FIJOPORTADO),2) AS NUM_FIJOPORTADO_INC,
'${hiveconf:MONTH0}'AS MES
FROM
WORK.WORK_CLIENTE_M0  A0
LEFT JOIN
WORK.WORK_CLIENTE_M1  A1
ON A0.NIF = A1.NIF
LEFT JOIN
WORK.WORK_CLIENTE_M2  A2
ON A0.NIF = A2.NIF
LEFT JOIN
WORK.WORK_CLIENTE_M3  A3
ON A0.NIF = A3.NIF;

DROP TABLE WORK.WORK_CLIENTE_M0;
DROP TABLE WORK.WORK_CLIENTE_M1;
DROP TABLE WORK.WORK_CLIENTE_M2;
DROP TABLE WORK.WORK_CLIENTE_M3;
     
EXIT;