USE VF_EBU;    

set mapred.job.name = WORK_INCIDENCIAS_CLASIF_1;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_1;
CREATE TABLE WORK.WORK_INCIDENCIAS_CLASIF_1 AS
SELECT A.*,

CASE 
  WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%FACTURA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%CUOTA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DESC%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%SUSCRIP%' OR 
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%TARIFIC%' 
  THEN 1 ELSE 0 END AS IND_FACTURACION,
  
  CASE WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MI VODAFONE %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MI VF %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% PUNTO%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PUNTO%VODA%' 
  THEN 1 ELSE 0 END AS IND_VODAFONE,

  CASE WHEN 
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%DSL%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% HFC %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% FTTH %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% CASA%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PROVISIO%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% FIJ%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%ROUTER%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% ACCES%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% TV %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%TELEVISI_N%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% CANAL%' 
  THEN 1 ELSE 0 END AS IND_FIJO,
    
  CASE WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% LLAMA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DAT%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% VOZ %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%COBERT%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PORTAB%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%OFICINA VODAFONE%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MENSA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% SIM %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%COMUNIC%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% M_VIL %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DESV_O%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%ROAMIN%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%GSM%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% NAVEG%' 
  THEN 1 ELSE 0 END AS IND_MOVIL
 FROM INPUT.VF_EBU_INCIDENCIAS A WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}';

 
set mapred.job.name = WORK_INCIDENCIAS_CLASIF_1_DEF;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_1_DEF;
CREATE TABLE WORK.WORK_INCIDENCIAS_CLASIF_1_DEF AS
SELECT A.*, 
CASE WHEN IND_FACTURACION=0 AND IND_VODAFONE=0 AND IND_FIJO=0 AND IND_MOVIL=0 THEN 1 ELSE 0 END AS IND_RESTO
FROM  WORK.WORK_INCIDENCIAS_CLASIF_1 A;

 
set mapred.job.name = WORK_INCIDENCIAS_CLASIF_3;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_3;
CREATE TABLE WORK.WORK_INCIDENCIAS_CLASIF_3 AS
SELECT A.*,
CASE 
  WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%FACTURA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%CUOTA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DESC%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%SUSCRIP%' OR 
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%TARIFIC%' 
  THEN 1 ELSE 0 END AS IND_FACTURACION_3,
  
  CASE WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MI VODAFONE %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MI VF %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% PUNTO%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PUNTO%VODA%' 
  THEN 1 ELSE 0 END AS IND_VODAFONE_3,

  CASE WHEN 
	  CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%DSL%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% HFC %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% FTTH %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% CASA%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PROVISIO%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% FIJ%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%ROUTER%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% ACCES%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% TV %' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%TELEVISI_N%' OR
      CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% CANAL%' 
  THEN 1 ELSE 0 END AS IND_FIJO_3,
    
  CASE WHEN
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% LLAMA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DAT%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% VOZ %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%COBERT%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%PORTAB%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%OFICINA VODAFONE%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%MENSA%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% SIM %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%COMUNIC%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% M_VIL %' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% DESV_O%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%ROAMIN%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '%GSM%' OR
    CONCAT(UPPER(A.X_TIPO_PROBLEMA_LVL1),UPPER(A.X_TIPO_PROBLEMA_LVL2),UPPER(A.X_TIPO_PROBLEMA_LVL3)) LIKE '% NAVEG%' 
  THEN 1 ELSE 0 END AS IND_MOVIL_3
-- FROM INPUT.VF_EBU_INCIDENCIAS A WHERE PARTITIONED_MONTH <= '${hiveconf:MONTH0}' AND PARTITIONED_MONTH >= '${hiveconf:MONTH3}';
FROM INPUT.VF_EBU_INCIDENCIAS A WHERE concat(SUBSTR(creation_time,7,4),SUBSTR(creation_time,4,2)) <= '${hiveconf:MONTH0}' 
AND CONCAT(SUBSTR(creation_TIME,7,4),SUBSTR(creation_TIME,4,2)) >= '${hiveconf:MONTH3}';

 set mapred.job.name = WORK_INCIDENCIAS_CLASIF_3_DEF;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_3_DEF;
CREATE TABLE WORK.WORK_INCIDENCIAS_CLASIF_3_DEF AS
SELECT A.*, CASE WHEN IND_FACTURACION_3=0 AND IND_VODAFONE_3=0 AND IND_FIJO_3=0 AND IND_MOVIL_3=0 THEN 1 ELSE 0 END AS IND_RESTO_3
FROM  WORK.WORK_INCIDENCIAS_CLASIF_3 A;

-- TRASPONEMOS 
set mapred.job.name = WORK_INDICADORES_NUM_1;
DROP TABLE WORK.WORK_INDICADORES_NUM_1;
CREATE TABLE WORK.WORK_INDICADORES_NUM_1 AS
SELECT X_NUM_IDENT AS NIFCIF, 
SUM(IND_RESTO) AS NUM_RESTO_1M,
SUM(IND_FACTURACION) AS NUM_FACTURACION_1M,
SUM(IND_FIJO) AS NUM_FIJO_1M,
SUM(IND_MOVIL) AS NUM_MOVIL_1M,
SUM(IND_VODAFONE) AS NUM_VODAFONE_1M
FROM WORK.WORK_INCIDENCIAS_CLASIF_1_DEF
GROUP BY X_NUM_IDENT;

set mapred.job.name = WORK_INDICADORES_NUM_3;
DROP TABLE WORK.WORK_INDICADORES_NUM_3;
CREATE TABLE WORK.WORK_INDICADORES_NUM_3 AS
SELECT X_NUM_IDENT AS NIFCIF,
SUM(IND_RESTO_3) AS NUM_RESTO_3M,
SUM(IND_FACTURACION_3) AS NUM_FACTURACION_3M,
SUM(IND_FIJO_3) AS NUM_FIJO_3M,
SUM(IND_MOVIL_3) AS NUM_MOVIL_3M,
SUM(IND_VODAFONE_3) AS NUM_VODAFONE_3M
FROM WORK.WORK_INCIDENCIAS_CLASIF_3_DEF
GROUP BY X_NUM_IDENT;

set mapred.job.name = WORK_SOLIC_TEC_1M;
DROP TABLE WORK.WORK_SOLIC_TEC_1M;
CREATE TABLE WORK.WORK_SOLIC_TEC_1M AS 
SELECT X_NUM_IDENT AS NIFCIF,
COUNT(*) AS NUM_SOLIC_TEC_1M,
case when count(*) > 0 then 1 else 0 end as IND_SOLIC_TEC_1M
FROM WORK.WORK_INCIDENCIAS_CLASIF_1
WHERE  CONCAT(UPPER(X_TIPO_PROBLEMA_LVL1),UPPER(X_TIPO_PROBLEMA_LVL2),UPPER(X_TIPO_PROBLEMA_LVL3)) LIKE '%SOLICITA%ENV%NICO%'
GROUP BY X_NUM_IDENT
;

set mapred.job.name = WORK_SOLIC_TEC_3M;
DROP TABLE WORK.WORK_SOLIC_TEC_3M;
CREATE TABLE WORK.WORK_SOLIC_TEC_3M AS 
SELECT X_NUM_IDENT AS NIFCIF,
COUNT(*) AS NUM_SOLIC_TEC_3M,
case when count(*) > 0 then 1 else 0 end as IND_SOLIC_TEC_3M
FROM WORK.WORK_INCIDENCIAS_CLASIF_3
WHERE  CONCAT(UPPER(X_TIPO_PROBLEMA_LVL1),UPPER(X_TIPO_PROBLEMA_LVL2),UPPER(X_TIPO_PROBLEMA_LVL3)) LIKE '%SOLICITA%ENV%NICO%'
GROUP BY X_NUM_IDENT;

set mapred.job.name = EMP_EXP_INCIDENCIAS;
DROP TABLE VF_EBU.EMP_EXP_INCIDENCIAS;
CREATE TABLE VF_EBU.EMP_EXP_INCIDENCIAS AS
SELECT A.CIF_NIF, 
       NVL(B.NUM_MOVIL_1M,0) AS NUM_MOVIL_1M,
	   CASE WHEN B.NUM_MOVIL_1M > 0 THEN 1 ELSE 0 END AS IND_MOVIL_1M,
       NVL(B.NUM_FIJO_1M,0) AS NUM_FIJO_1M,
	   CASE WHEN B.NUM_FIJO_1M > 0 THEN 1 ELSE 0 END AS IND_FIJO_1M,
       NVL(B.NUM_RESTO_1M,0) AS NUM_RESTO_1M,
	   CASE WHEN B.NUM_RESTO_1M > 0 THEN 1 ELSE 0 END AS IND_RESTO_1M,
       NVL(B.NUM_FACTURACION_1M,0) AS NUM_FACTURACION_1M,
	   CASE WHEN B.NUM_FACTURACION_1M > 0 THEN 1 ELSE 0 END AS IND_FACTURACION_1M,
       NVL(B.NUM_VODAFONE_1M,0) AS NUM_VODAFONE_1M,
	   CASE WHEN B.NUM_VODAFONE_1M > 0 THEN 1 ELSE 0 END AS IND_VODAFONE_1M,
       NVL(C.NUM_MOVIL_3M,0) AS NUM_MOVIL_3M,
	   CASE WHEN C.NUM_MOVIL_3M > 0 THEN 1 ELSE 0 END AS IND_MOVIL_3M,
       NVL(C.NUM_FIJO_3M,0) AS NUM_FIJO_3M,
	   CASE WHEN C.NUM_FIJO_3M > 0 THEN 1 ELSE 0 END AS IND_FIJO_3M,
       NVL(C.NUM_RESTO_3M,0) AS NUM_RESTO_3M,
	   CASE WHEN C.NUM_RESTO_3M > 0 THEN 1 ELSE 0 END AS IND_RESTO_3M,
       NVL(C.NUM_FACTURACION_3M,0) AS NUM_FACTURACION_3M,
	   CASE WHEN C.NUM_FACTURACION_3M > 0 THEN 1 ELSE 0 END AS IND_FACTURACION_3M,
       NVL(C.NUM_VODAFONE_3M,0) AS NUM_VODAFONE_3M,
	   CASE WHEN C.NUM_VODAFONE_3M > 0 THEN 1 ELSE 0 END AS IND_VODAFONE_3M,
       NVL(E.IND_SOLIC_TEC_1M,0) AS IND_SOLIC_TEC_1M,
       NVL(E.NUM_SOLIC_TEC_1M,0) AS NUM_SOLIC_TEC_1M,
       NVL(F.IND_SOLIC_TEC_3M,0) AS IND_SOLIC_TEC_3M,
       NVL(F.NUM_SOLIC_TEC_3M,0) AS NUM_SOLIC_TEC_3M
FROM 
(SELECT * FROM INPUT.VF_EBU_AC_EMP_CARTERA_CLIENTE WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
LEFT JOIN 
WORK.WORK_INDICADORES_NUM_1 B
ON A.CIF_NIF = B.NIFCIF
LEFT JOIN 
WORK.WORK_INDICADORES_NUM_3 C
ON A.CIF_NIF = C.NIFCIF
LEFT JOIN WORK.WORK_SOLIC_TEC_1M E
ON A.CIF_NIF = E.NIFCIF
LEFT JOIN WORK.WORK_SOLIC_TEC_3M F
ON A.CIF_NIF = F.NIFCIF
; 
       
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_1;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_1_DEF;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_3;
DROP TABLE WORK.WORK_INCIDENCIAS_CLASIF_3_DEF;
DROP TABLE WORK.WORK_INDICADORES_NUM_1;
DROP TABLE WORK.WORK_INDICADORES_NUM_3;
DROP TABLE WORK.WORK_TIEMPOS_RESOLUCION;
DROP TABLE WORK.WORK_SOLIC_TEC_1M;
DROP TABLE WORK.WORK_SOLIC_TEC_3M;

EXIT;
