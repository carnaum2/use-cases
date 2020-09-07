USE VF_POSTPAID;

set mapred.job.name = WORK_LOYALTY_VOZ_AUX_M0;
DROP TABLE WORK.WORK_LOYALTY_VOZ_AUX_M0;
CREATE TABLE WORK.WORK_LOYALTY_VOZ_AUX_M0 AS
SELECT  
TELEFONO AS MSISDN,
CAST(AVG_ARPU_3M AS DOUBLE) AS ARPU,
CAST(EDAD AS DOUBLE) AS EDAD,
'${hiveconf:MONTH0}' AS MES,
CASE WHEN CAST(FLAGVOZ AS DOUBLE) > 0 THEN 0 ELSE 1 END AS IND_NO_VOZ
FROM 
(SELECT * FROM INPUT.VF_POS_PRD_LOYALTY_MSISDN WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
INNER JOIN
(SELECT X_ID_RED, MAX(CAST(FLAGVOZ AS DOUBLE)) AS FLAGVOZ  
FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}' GROUP BY X_ID_RED) B
ON A.TELEFONO = B.X_ID_RED;

set mapred.job.name = WORK_EDAD_EST_M0;
DROP TABLE WORK.WORK_EDAD_EST_M0;
CREATE TABLE WORK.WORK_EDAD_EST_M0 AS
SELECT  
TELEFONO AS MSISDN,
MAX(CAST(EDAD AS DOUBLE)) AS EDAD
FROM INPUT.VF_POS_PRD_LOYALTY_MSISDN WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}'
GROUP BY TELEFONO;

set mapred.job.name = WORK_LOYALTY_VOZ_M0;
DROP TABLE WORK.WORK_LOYALTY_VOZ_M0;
CREATE TABLE WORK.WORK_LOYALTY_VOZ_M0 AS
SELECT  
A.MSISDN,
A.MES, 
(CASE WHEN (A.EDAD IS NOT NULL) THEN A.EDAD WHEN (CAST(B.EDAD AS DOUBLE) > 105 OR CAST(B.EDAD AS DOUBLE) < 0) 
THEN NULL ELSE CAST(B.EDAD AS DOUBLE) END) EDAD2,
(CASE WHEN (A.ARPU >500) THEN 500 ELSE A.ARPU END)ARPU,  
A.IND_NO_VOZ
FROM WORK.WORK_LOYALTY_VOZ_AUX_M0 A 
LEFT JOIN WORK.WORK_EDAD_EST_M0 B
ON A.MSISDN = B.MSISDN;

DROP TABLE WORK.WORK_EDAD_EST_M0;

set mapred.job.name = WORK_MED_EDAD_M0;
DROP TABLE WORK.WORK_MED_EDAD_M0;
CREATE TABLE WORK.WORK_MED_EDAD_M0 AS
SELECT  
CEIL(AVG(EDAD)) AS EDAD
FROM WORK.WORK_LOYALTY_VOZ_AUX_M0;

DROP TABLE WORK.WORK_LOYALTY_VOZ_AUX_M0;

set mapred.job.name = WORK_LOYALTY_VOZ_F_M0;
DROP TABLE WORK.WORK_LOYALTY_VOZ_F_M0;
CREATE TABLE WORK.WORK_LOYALTY_VOZ_F_M0 AS
SELECT  
A.MSISDN,
A.MES, 
(CASE WHEN (A.EDAD2 IS NULL) THEN
B.EDAD
ELSE
A.EDAD2
END)EDAD2,
A.ARPU,
A.IND_NO_VOZ
FROM WORK.WORK_LOYALTY_VOZ_M0 A 
JOIN WORK.WORK_MED_EDAD_M0 B;

DROP TABLE WORK.WORK_LOYALTY_VOZ_M0;
DROP TABLE WORK.WORK_MED_EDAD_M0;

set mapred.job.name = WORK_LOYALTY_EDAD_CATEG_M0;
DROP TABLE WORK.WORK_LOYALTY_EDAD_CATEG_M0;
CREATE TABLE WORK.WORK_LOYALTY_EDAD_CATEG_M0 AS
SELECT 
MSISDN,
MES,
ARPU,
IND_NO_VOZ,
EDAD2 AS EDAD,
case when (EDAD2>=0) and (EDAD2<=31)  then 1
when (EDAD2>=32) and (EDAD2<=37) then 2
when (EDAD2>=38) and (EDAD2<=44) then 3
when (EDAD2>=45) and (EDAD2<=53) then 4
when (EDAD2>=54) and (EDAD2<=105) then 5
end AS CATEGORIA                
FROM WORK.WORK_LOYALTY_VOZ_F_M0;

DROP TABLE WORK.WORK_LOYALTY_VOZ_F_M0;

set mapred.job.name = WORK_LOYALTY_ARPU_MED_AUX_M0;
DROP TABLE WORK.WORK_LOYALTY_ARPU_MED_AUX_M0;
CREATE TABLE WORK.WORK_LOYALTY_ARPU_MED_AUX_M0 AS
SELECT 
CATEGORIA,
ROUND(AVG(ARPU), 2) AS ARPU
FROM WORK.WORK_LOYALTY_EDAD_CATEG_M0
GROUP BY CATEGORIA;   

set mapred.job.name = WORK_LOYALTY_ARPU_MED_M0;
DROP TABLE WORK.WORK_LOYALTY_ARPU_MED_M0;
CREATE TABLE WORK.WORK_LOYALTY_ARPU_MED_M0 AS
SELECT 
A.MSISDN,
A.MES,                                
(CASE WHEN ( A.ARPU IS NULL)
THEN B.ARPU
ELSE  
A.ARPU                           
END) ARPU,
IND_NO_VOZ,
A.EDAD,
A.CATEGORIA AS CATEG_EDAD
FROM WORK.WORK_LOYALTY_EDAD_CATEG_M0 A
LEFT JOIN WORK.WORK_LOYALTY_ARPU_MED_AUX_M0 B
ON A.CATEGORIA=B.CATEGORIA;   

DROP TABLE WORK.WORK_LOYALTY_EDAD_CATEG_M0;
DROP TABLE WORK.WORK_LOYALTY_ARPU_MED_AUX_M0;

set mapred.job.name = WORK_LOYALTY_ARPU_CATEG_M0;
DROP TABLE WORK.WORK_LOYALTY_ARPU_CATEG_M0;
CREATE TABLE WORK.WORK_LOYALTY_ARPU_CATEG_M0 AS
SELECT 
MSISDN,
MES, 
ARPU,
IND_NO_VOZ,
EDAD, 
CATEG_EDAD,
CASE WHEN (ARPU>=0) and (ARPU<=11.29)  then 1
WHEN (ARPU>=11.3) and (ARPU<=18.99) then 2
WHEN (ARPU>=19) and (ARPU<= 29.20) then 3
WHEN (ARPU>=29.21) and (ARPU<=44.32) then 4
WHEN (ARPU>=44.33) and (ARPU<=500) then 5
END AS CATEG_ARPU 
FROM WORK.WORK_LOYALTY_ARPU_MED_M0; 

DROP TABLE WORK.WORK_LOYALTY_ARPU_MED_M0;

set mapred.job.name = WORK_LOYALTY_ARPUEDAD_DIST_AUX;
DROP TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_AUX;
CREATE TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_AUX AS
SELECT 
A.MSISDN,
A.MES,
A.ARPU, 
A.IND_NO_VOZ,
A.EDAD,
A.CATEG_EDAD,
A.CATEG_ARPU,
B.CLUSTER_, 
SQRT(POW(B.C_ARPU - A.CATEG_ARPU,2)+POW(B.C_EDAD -A.CATEG_EDAD,2)) AS DISTANCIA
FROM WORK.WORK_LOYALTY_ARPU_CATEG_M0 A,
master.vf_pos_seg_arpuedad_centro B 
WHERE B.CLUSTER_=1 OR B.CLUSTER_=2 OR B.CLUSTER_=3 OR B.CLUSTER_=4 OR 
B.CLUSTER_=5 OR B.CLUSTER_=6 OR B.CLUSTER_=7 OR B.CLUSTER_=8;

DROP TABLE WORK.WORK_LOYALTY_ARPU_CATEG_M0;

set mapred.job.name = WORK_LOYALTY_ARPUEDAD_DIST_MIN;
DROP TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_MIN;
CREATE TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_MIN AS
SELECT 
MSISDN,
MIN(DISTANCIA) AS DISTANCIA
FROM  WORK.WORK_LOYALTY_ARPUEDAD_DIST_AUX 
GROUP BY MSISDN;


set mapred.job.name = WORK_SEG_CLUSTER_LOYALTY;
DROP TABLE WORK.WORK_SEG_CLUSTER_LOYALTY;
CREATE TABLE WORK.WORK_SEG_CLUSTER_LOYALTY AS
SELECT 
A.MSISDN,
A.MES,   
A.ARPU, 
A.IND_NO_VOZ,
A.EDAD,
A.CATEG_EDAD,
A.CATEG_ARPU,      
A.CLUSTER_, 
B.DISTANCIA
FROM WORK.WORK_LOYALTY_ARPUEDAD_DIST_AUX A 
JOIN WORK.WORK_LOYALTY_ARPUEDAD_DIST_MIN B 
ON A.MSISDN=B.MSISDN AND B.DISTANCIA=A.DISTANCIA
ORDER BY MSISDN;

DROP TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_AUX;
DROP TABLE WORK.WORK_LOYALTY_ARPUEDAD_DIST_MIN;

set mapred.job.name = SEG_ARPU_EDAD;
DROP TABLE VF_POSTPAID.SEG_ARPU_EDAD;
CREATE TABLE VF_POSTPAID.SEG_ARPU_EDAD AS
SELECT     
MSISDN,
MAX(CLUSTER_) AS SEG_PARTICULAR     
FROM WORK.WORK_SEG_CLUSTER_LOYALTY
GROUP BY MSISDN;

DROP TABLE WORK.WORK_SEG_CLUSTER_LOYALTY;

EXIT;
