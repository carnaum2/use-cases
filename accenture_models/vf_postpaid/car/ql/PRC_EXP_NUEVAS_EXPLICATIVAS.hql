USE VF_POSTPAID;


set mapred.job.name = WORK_GESCALES_EN_HUELLA_MV_JZ;
DROP TABLE WORK.WORK_GESCALES_EN_HUELLA_MV_JZ;
CREATE TABLE WORK.WORK_GESCALES_EN_HUELLA_MV_JZ AS
SELECT
A.COD_GOLDEN
FROM (
SELECT
A.COD_GOLDEN
FROM
(SELECT COD_GOLDEN, COD_BLOQUE  FROM INPUT.VF_HH_GOLDEN  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}') A
LEFT JOIN
(SELECT COD_BLOQUE, COD_GESCAL_17  FROM INPUT.VF_HH_BLOQUE_PB  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}') B
ON A.COD_BLOQUE = B.COD_BLOQUE) A
GROUP BY A.COD_GOLDEN;
  

	  
set mapred.job.name = WORK_EXPLIS_HUELLA;
DROP TABLE WORK.WORK_EXPLIS_HUELLA;
CREATE TABLE WORK.WORK_EXPLIS_HUELLA AS
SELECT
A.NIF,
MAX(CAST(A.FLAG_HUELLA_JAZZTEL AS DOUBLE)) AS IND_HUELLA_JAZZTEL,
MAX(CAST(A.FLAG_HUELLA_MOVISTAR AS DOUBLE)) AS IND_HUELLA_MOVISTAR
FROM
(SELECT
A.NIF,
A.FLAG_HUELLA_JAZZTEL,
A.FLAG_HUELLA_MOVISTAR
FROM
(SELECT
A.NIF,   
A.FLAG_HUELLA_JAZZTEL,
A.FLAG_HUELLA_MOVISTAR,
B.COD_GOLDEN
FROM
(SELECT DISTINCT X_NUM_IDENT AS NIF, FLAG_HUELLA_JAZZTEL, FLAG_HUELLA_MOVISTAR  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
LEFT JOIN
(SELECT * FROM VF_HH.HH_AC_HOGAR_EXC_SD_${hiveconf:MONTH1}) B 
ON A.NIF = B.X_NUM_IDENT)A
LEFT JOIN
WORK.WORK_GESCALES_EN_HUELLA_MV_JZ B
ON A.COD_GOLDEN = B.COD_GOLDEN) A
GROUP BY A.NIF;


----- prc_segm_tipo_cli_lin_prod_3M
--- mejor?
---source /opt/src/hive/vf_postpaid/car/ql/PRC_SEGM_TIPO_CLI_LIN_PROD_3M.hql;

set mapred.job.name = WORK_MAESTRO_SEG;
DROP TABLE WORK.WORK_MAESTRO_SEG;
CREATE TABLE WORK.WORK_MAESTRO_SEG AS
SELECT DISTINCT NIF 
FROM VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH0}
UNION ALL
SELECT DISTINCT NIF 
FROM VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH1}
UNION ALL
SELECT DISTINCT NIF 
FROM VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH2};


set mapred.job.name = DO_PAR_SEG_TIPO_CLI_3M_${hiveconf:MONTH0};
DROP TABLE OUTPUT.DO_PAR_SEG_TIPO_CLI_3M_${hiveconf:MONTH0};
CREATE TABLE OUTPUT.DO_PAR_SEG_TIPO_CLI_3M_${hiveconf:MONTH0} AS
SELECT 
M.NIF,
CASE WHEN A1.NIF IS NULL THEN 'NULO' ELSE A1.SEG_TIPO_CLIENTE END AS TIPO_M0,
CASE WHEN A2.NIF IS NULL THEN 'NULO' ELSE A2.SEG_TIPO_CLIENTE END AS TIPO_M1,
CASE WHEN A3.NIF IS NULL THEN 'NULO' ELSE A3.SEG_TIPO_CLIENTE END AS TIPO_M2,
CASE 
WHEN A1.SEG_TIPO_CLIENTE = A2.SEG_TIPO_CLIENTE AND
 A1.SEG_TIPO_CLIENTE = A3.SEG_TIPO_CLIENTE AND
 A2.SEG_TIPO_CLIENTE = A3.SEG_TIPO_CLIENTE 
 THEN 0
ELSE 1 END AS IND_CAMBIO_3M
FROM
WORK.WORK_MAESTRO_SEG M
LEFT JOIN
VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH0} A1
ON M.NIF = A1.NIF
LEFT JOIN
VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH1} A2
ON M.NIF = A2.NIF
LEFT JOIN
VF_POSTPAID.PAR_SEG_TIPO_CLIENTE_${hiveconf:MONTH2} A3
ON M.NIF = A3.NIF;


-- WORK_CICLO_VIDA_3

set mapred.job.name = WORK_CICLO_VIDA_1;
DROP TABLE WORK.WORK_CICLO_VIDA_1;
CREATE TABLE WORK.WORK_CICLO_VIDA_1 AS
SELECT
NIF,
CASE WHEN TIPO_M0 LIKE 'NULO' THEN 0
WHEN TIPO_M0 LIKE '%01%' THEN 1
WHEN TIPO_M0 LIKE '%02%' THEN 2
WHEN TIPO_M0 LIKE '%03%' THEN 3
WHEN TIPO_M0 LIKE '%04%' THEN 4
WHEN TIPO_M0 LIKE '%05%' THEN 5
WHEN TIPO_M0 LIKE '%06%' THEN 6
WHEN TIPO_M0 LIKE '%07%' THEN 7
WHEN TIPO_M0 LIKE '%08%' THEN 8
WHEN TIPO_M0 LIKE '%09%' THEN 9
WHEN TIPO_M0 LIKE '%10%' THEN 10
WHEN TIPO_M0 LIKE '%11%' THEN 11
ELSE 999
END AS M0_NUM,
CASE WHEN TIPO_M1 LIKE 'NULO' THEN 0
WHEN TIPO_M1 LIKE '%01%' THEN 1
WHEN TIPO_M1 LIKE '%02%' THEN 2
WHEN TIPO_M1 LIKE '%03%' THEN 3
WHEN TIPO_M1 LIKE '%04%' THEN 4
WHEN TIPO_M1 LIKE '%05%' THEN 5
WHEN TIPO_M1 LIKE '%06%' THEN 6
WHEN TIPO_M1 LIKE '%07%' THEN 7
WHEN TIPO_M1 LIKE '%08%' THEN 8
WHEN TIPO_M1 LIKE '%09%' THEN 9
WHEN TIPO_M1 LIKE '%10%' THEN 10
WHEN TIPO_M1 LIKE '%11%' THEN 11
ELSE 999
END AS M1_NUM,
CASE WHEN TIPO_M2 LIKE 'NULO' THEN 0
WHEN TIPO_M2 LIKE '%01%' THEN 1
WHEN TIPO_M2 LIKE '%02%' THEN 2
WHEN TIPO_M2 LIKE '%03%' THEN 3
WHEN TIPO_M2 LIKE '%04%' THEN 4
WHEN TIPO_M2 LIKE '%05%' THEN 5
WHEN TIPO_M2 LIKE '%06%' THEN 6
WHEN TIPO_M2 LIKE '%07%' THEN 7
WHEN TIPO_M2 LIKE '%08%' THEN 8
WHEN TIPO_M2 LIKE '%09%' THEN 9
WHEN TIPO_M2 LIKE '%10%' THEN 10
WHEN TIPO_M2 LIKE '%11%' THEN 11
ELSE 999
END AS M2_NUM
FROM OUTPUT.DO_PAR_SEG_TIPO_CLI_3M_${hiveconf:MONTH0};
  
  
set mapred.job.name = WORK_CICLO_VIDA_2;
DROP TABLE WORK.WORK_CICLO_VIDA_2;
CREATE TABLE WORK.WORK_CICLO_VIDA_2 AS  
select 
NIF,
CASE WHEN (M2_NUM = 0 AND M1_NUM = 0 AND M0_NUM > 0) 
OR ((M2_NUM = 0 AND M1_NUM > 0 AND M0_NUM > 0) 
AND (M1_NUM = M0_NUM)) THEN 1 ELSE 0 END AS IND_VINCULANDO,
CASE WHEN 
((M2_NUM > 0 AND M1_NUM > 0 AND M0_NUM > 0) AND (M2_NUM < M1_NUM) AND (M1_NUM = M0_NUM)) OR 
((M2_NUM > 0 AND M1_NUM > 0 AND M0_NUM > 0) AND (M1_NUM < M0_NUM) AND (M2_NUM = M1_NUM)) OR 
((M2_NUM < M1_NUM) AND (M1_NUM < M0_NUM)) OR ((M1_NUM < M0_NUM) AND (M2_NUM < M0_NUM)) 
THEN 1 ELSE 0 END AS IND_DESARROLLO,
CASE WHEN 
(M2_NUM = M1_NUM) AND (M1_NUM = M0_NUM) OR ((M2_NUM = M0_NUM) AND M1_NUM = 0) OR ((M2_NUM = M0_NUM) 
AND (M1_NUM < M0_NUM)) THEN 1 ELSE 0 END AS IND_ESTABLE,
CASE WHEN 
((M2_NUM > 0 AND M1_NUM > 0 AND M0_NUM > 0) AND (M2_NUM > M1_NUM) AND (M1_NUM = M0_NUM)) OR 
((M2_NUM > 0 AND M1_NUM > 0 AND M0_NUM > 0) AND (M1_NUM > M0_NUM) AND (M2_NUM = M1_NUM)) OR 
((M2_NUM > M1_NUM) AND (M1_NUM > M0_NUM)) OR ((M1_NUM > M0_NUM) AND M0_NUM > 0) OR 
((M2_NUM > M0_NUM) AND ((M1_NUM < M0_NUM)) AND M1_NUM = 0) OR 
((M2_NUM > M1_NUM) AND (M1_NUM < M0_NUM) AND (M2_NUM > M0_NUM)) THEN 1 ELSE 0 END AS IND_DESVINCULANDO,
CASE WHEN 
(M2_NUM > 0 AND M1_NUM = 0 AND M0_NUM = 0) OR 
((M2_NUM > 0 AND M1_NUM > 0 AND M0_NUM = 0) AND (M2_NUM = M1_NUM)) OR (M0_NUM = 0) 
THEN 1 ELSE 0 END AS IND_DESVINCULADO
FROM WORK.WORK_CICLO_VIDA_1;


set mapred.job.name = WORK_CICLO_VIDA_3;
DROP TABLE WORK.WORK_CICLO_VIDA_3;
CREATE TABLE WORK.WORK_CICLO_VIDA_3 AS  
SELECT 
A.*,
CASE 
WHEN A.IND_VINCULANDO = 1 AND A.IND_DESARROLLO = 1 THEN 'VINCULANDOSE+DESARROLLO'
WHEN A.IND_VINCULANDO = 1 AND A.IND_ESTABLE = 1 THEN 'VINCULANDOSE+ESTABLE'
WHEN A.IND_VINCULANDO = 1 AND A.IND_DESVINCULANDO = 1 THEN 'VINCULANDOSE+DESVINCULANDO'
WHEN A.IND_VINCULANDO = 1 AND A.IND_DESVINCULADO = 1 THEN 'VINCULANDOSE+DESVINCULADO'
WHEN A.IND_VINCULANDO = 1 THEN 'VINCULANDOSE'

WHEN A.IND_DESARROLLO = 1 AND A.IND_ESTABLE = 1 THEN 'DESARROLLO+ESTABLE'
WHEN A.IND_DESARROLLO = 1 AND A.IND_DESVINCULANDO = 1 THEN 'DESARROLLO+DESVINCULANDO'
WHEN A.IND_DESARROLLO = 1 AND A.IND_DESVINCULADO = 1 THEN 'DESARROLLO+DESVINCULADO'
WHEN A.IND_DESARROLLO = 1 THEN 'DESARROLLO'

WHEN A.IND_ESTABLE = 1 THEN 'ESTABLE'

WHEN A.IND_DESVINCULANDO = 1 AND A.IND_DESVINCULADO = 1 THEN 'DESVINCULANDO+DESVINCULADO'
WHEN A.IND_DESVINCULANDO = 1 THEN 'DESVINCULANDO'
WHEN A.IND_DESVINCULADO = 1 THEN 'DESVINCULADO'
ELSE '' 
END AS SEG_CICLO_VIDA,
(A.IND_VINCULANDO + A.IND_DESARROLLO + A.IND_ESTABLE + A.IND_DESVINCULANDO + A.IND_DESVINCULADO) AS TOT
FROM WORK.WORK_CICLO_VIDA_2 A;


-- CICLO DE VIDA DEL CLIENTE

      
set mapred.job.name = WORK_CICLO_VIDA;
DROP TABLE WORK.WORK_CICLO_VIDA;
CREATE TABLE WORK.WORK_CICLO_VIDA AS
SELECT
NIF,
MAX(IND_VINCULANDO) AS IND_VINCULANDO,
MAX(IND_DESARROLLO) AS IND_DESARROLLO,
MAX(IND_ESTABLE) AS IND_ESTABLE,
MAX(IND_DESVINCULANDO) AS IND_DESVINCULANDO,
MAX(IND_DESVINCULADO) AS IND_DESVINCULADO,
MAX(SEG_CICLO_VIDA) AS SEG_CICLO_VIDA
FROM
WORK.WORK_CICLO_VIDA_3
GROUP BY NIF;


-- PENETRACIóN PROVINCIA NIVEL HOGAR
      
set mapred.job.name = WORK_HOGAR_NIF;
DROP TABLE WORK.WORK_HOGAR_NIF;
CREATE TABLE WORK.WORK_HOGAR_NIF AS
SELECT 
A.*, 
B.COD_GOLDEN 
FROM
(SELECT DISTINCT X_NUM_IDENT AS NIF  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}')  A
LEFT JOIN
(SELECT * FROM VF_HH.HH_AC_HOGAR_EXC_SD_${hiveconf:MONTH1}) B
ON A.NIF = B.X_NUM_IDENT;


set mapred.job.name = WORK_EXP_PROVINCIA;
DROP TABLE WORK.WORK_EXP_PROVINCIA;
CREATE TABLE WORK.WORK_EXP_PROVINCIA AS
SELECT A.NIF, 
MAX(A.NUM_CLIENTES_PCT_PROV) AS NUM_CLIENTES_PCT_PROV, 
MAX(A.NUM_POBLACION_PROV_PCT) AS NUM_POBLACION_PROV_PCT, 
MAX(A.NUM_HH_ADSL_PCT_PROV) AS NUM_HH_ADSL_PCT_PROV
FROM
(SELECT
A.NIF, 
CAST(B.NUM_CLIENTES_PCT_PROV AS DOUBLE) AS NUM_CLIENTES_PCT_PROV, 
CAST(B.NUM_POBLACION_PROV_PCT AS DOUBLE) AS NUM_POBLACION_PROV_PCT, 
CAST(B.NUM_HH_ADSL_PCT_PROV AS DOUBLE) AS NUM_HH_ADSL_PCT_PROV
FROM
WORK.WORK_HOGAR_NIF A
LEFT JOIN
VF_HH.HH_EXP_SOCIODEMO B 
ON A.COD_GOLDEN = B.COD_GOLDEN) A
GROUP BY A.NIF;


set mapred.job.name = WORK_SUPERINTEGRAL;
DROP TABLE WORK.WORK_SUPERINTEGRAL;
CREATE TABLE WORK.WORK_SUPERINTEGRAL AS
SELECT DISTINCT B.NIF
FROM
(SELECT DISTINCT A.X_ID_CUENTA 
FROM 
(SELECT
A.X_ID_CUENTA,
MAX(A.FLAGVOZ) AS VOZ,
MAX(A.FLAGLPD) AS LPD,
MAX(A.FLAGHZ) AS HZ
FROM
(SELECT 
CAST(A.X_ID_CUENTA AS BIGINT) AS X_ID_CUENTA, 
CAST(A.X_ID_RED AS BIGINT) AS X_ID_RED, 
CAST(A.FLAGVOZ AS DOUBLE) AS FLAGVOZ,
CASE WHEN CAST(A.FLAGLPD AS DOUBLE) > 0 AND A.PLANDATOS LIKE 'MBBAI' THEN 1 ELSE 0 END AS FLAGLPD,
CASE WHEN CAST(A.FLAGHZ AS DOUBLE) > 0 AND A.PROMOCION_VF LIKE 'HZ_SMP_LPD' THEN 1 ELSE 0 END AS FLAGHZ
FROM (SELECT * FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A) A
GROUP BY A.X_ID_CUENTA) A 
WHERE CAST(A.VOZ AS DOUBLE) > 0 AND CAST(A.LPD AS DOUBLE) > 0 AND CAST(A.HZ AS DOUBLE) > 0) A
INNER JOIN
(SELECT A.X_ID_CUENTA, A.X_NUM_IDENT AS NIF FROM  (SELECT * FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A) B
ON A.X_ID_CUENTA = B.X_ID_CUENTA;

	  
	  
set mapred.job.name = PAR_EXP_CLC_HUELLA;
DROP TABLE VF_POSTPAID.PAR_EXP_CLC_HUELLA;
CREATE TABLE VF_POSTPAID.PAR_EXP_CLC_HUELLA AS
SELECT 
A.NIF,
ROUND(B.IND_HUELLA_JAZZTEL) AS IND_HUELLA_JAZZTEL,
ROUND(B.IND_HUELLA_MOVISTAR) AS IND_HUELLA_MOVISTAR,
ROUND(D.NUM_CLIENTES_PCT_PROV,2) AS NUM_CLIENTES_PCT_PROV, 
ROUND(D.NUM_POBLACION_PROV_PCT,2) AS NUM_POBLACION_PROV_PCT, 
ROUND(D.NUM_HH_ADSL_PCT_PROV,2) AS NUM_HH_ADSL_PCT_PROV,
ROUND(E.IND_VINCULANDO) AS IND_VINCULANDO,
ROUND(E.IND_DESARROLLO) AS IND_DESARROLLO,
ROUND(E.IND_ESTABLE) AS IND_ESTABLE,
ROUND(E.IND_DESVINCULANDO) AS IND_DESVINCULANDO,
ROUND(E.IND_DESVINCULADO) AS IND_DESVINCULADO,
E.SEG_CICLO_VIDA,
--CASE WHEN E.X_NUM_IDENT IS NULL THEN 0 ELSE 1 END AS IND_DATASHARING,
CASE WHEN F.NIF IS NULL THEN 0 ELSE 1 END AS IND_SUPERINTEGRAL
--CASE WHEN G.NIF IS NULL THEN 0 ELSE 1 END AS IND_YOMVI
FROM 
(SELECT DISTINCT X_NUM_IDENT AS NIF  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
LEFT JOIN
WORK.WORK_EXPLIS_HUELLA B
ON A.NIF = B.NIF
LEFT JOIN
WORK.WORK_CICLO_VIDA E
ON A.NIF = E.NIF
LEFT JOIN
WORK.WORK_EXP_PROVINCIA D
ON A.NIF = D.NIF
LEFT JOIN
WORK.WORK_SUPERINTEGRAL F
ON A.NIF = F.NIF;



DROP TABLE WORK.WORK_GESCALES_EN_HUELLA_MV_JZ;
DROP TABLE WORK.WORK_EXPLIS_HUELLA;
DROP TABLE WORK.WORK_MAESTRO_SEG;
DROP TABLE WORK.WORK_CICLO_VIDA_1;
DROP TABLE WORK.WORK_CICLO_VIDA_2;
DROP TABLE WORK.WORK_CICLO_VIDA_3;
DROP TABLE WORK.WORK_CICLO_VIDA;
DROP TABLE WORK.WORK_HOGAR_NIF;
DROP TABLE WORK.WORK_EXP_PROVINCIA;
DROP TABLE WORK.WORK_SUPERINTEGRAL;

EXIT;

