USE VF_POSTPAID;
  
set mapred.job.name = MIVF_ACTUAL_M0;
DROP TABLE WORK.MIVF_ACTUAL_M0;
CREATE TABLE WORK.MIVF_ACTUAL_M0 AS
SELECT msisdn, 
sum(case when subcanal like 'WEB' then 1 else 0 end) as num_mivf_web,
sum(case when subcanal like 'APP' then 1 else 0 end) as num_mivf_app
FROM input.vf_mivodafone WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}'
group by msisdn;

set mapred.job.name = MIVF_ACTUAL_M1;
DROP TABLE WORK.MIVF_ACTUAL_M1;
CREATE TABLE WORK.MIVF_ACTUAL_M1 AS
SELECT msisdn, 
sum(case when subcanal like 'WEB' then 1 else 0 end) as num_mivf_web,
sum(case when subcanal like 'APP' then 1 else 0 end) as num_mivf_app
FROM input.vf_mivodafone WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}'
group by msisdn;

set mapred.job.name = MIVF_ACTUAL_M2;
DROP TABLE WORK.MIVF_ACTUAL_M2;
CREATE TABLE WORK.MIVF_ACTUAL_M2 AS
SELECT msisdn, 
sum(case when subcanal like 'WEB' then 1 else 0 end) as num_mivf_web,
sum(case when subcanal like 'APP' then 1 else 0 end) as num_mivf_app
FROM input.vf_mivodafone WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}'
group by msisdn;
  
set mapred.job.name = MIVF_INTENSIVO_M0;
DROP TABLE WORK.MIVF_INTENSIVO_M0;
CREATE TABLE WORK.MIVF_INTENSIVO_M0 AS
SELECT
distinct A.MSISDN
FROM
WORK.MIVF_ACTUAL_M0 A
INNER JOIN
WORK.MIVF_ACTUAL_M1 B
ON A.MSISDN = B.MSISDN
INNER JOIN
WORK.MIVF_ACTUAL_M2 C
ON A.MSISDN = C.MSISDN;

set mapred.job.name = MIVF_PASIVO_M0;
DROP TABLE WORK.MIVF_PASIVO_M0;
CREATE TABLE WORK.MIVF_PASIVO_M0 AS
SELECT DISTINCT A.MSISDN FROM 
(SELECT DISTINCT MSISDN FROM WORK.MIVF_ACTUAL_M0 
UNION ALL
SELECT DISTINCT MSISDN FROM WORK.MIVF_ACTUAL_M1 
UNION ALL
SELECT DISTINCT MSISDN FROM WORK.MIVF_ACTUAL_M2) A; 
         
set mapred.job.name = PAR_EXP_MIVF;
DROP TABLE VF_POSTPAID.PAR_EXP_MIVF;
CREATE TABLE  VF_POSTPAID.PAR_EXP_MIVF AS
SELECT     
M.MSISDN,
NVL(a1.num_mivf_web, 0) as num_acc_mivf_web_m0,
NVL(a1.num_mivf_app, 0) as num_acc_mivf_app_m0,
NVL(a1.num_mivf_web, 0) + NVL(a1.num_mivf_app, 0) as num_acc_mivf_m0,

NVL(a1.num_mivf_web, 0) + NVL(a2.num_mivf_web, 0) + NVL(a3.num_mivf_web, 0) as num_acc_mivf_web_3m,
NVL(a1.num_mivf_app, 0) + NVL(a2.num_mivf_app, 0) + NVL(a3.num_mivf_app, 0) as num_acc_mivf_app_3m,
NVL(a1.num_mivf_web, 0) + NVL(a2.num_mivf_web, 0) + NVL(a3.num_mivf_web, 0) + 
NVL(a1.num_mivf_app, 0) + NVL(a2.num_mivf_app, 0) + NVL(a3.num_mivf_app, 0) as num_acc_mivf_3m,

(CASE WHEN A1.MSISDN IS NOT NULL  THEN 1 ELSE 0 end) as ind_mivf_actual,   
(CASE WHEN B.MSISDN IS  NOT NULL  THEN 1 ELSE 0 end) as ind_mivf_intensivo_3m,   
(CASE WHEN C.MSISDN IS NOT NULL THEN 1 ELSE 0 end) as ind_mivf_pasivo_3m    
FROM 
(SELECT DISTINCT X_ID_RED AS MSISDN  
 FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') M
LEFT JOIN
WORK.MIVF_ACTUAL_M0 A1
ON M.MSISDN = A1.MSISDN
LEFT JOIN
WORK.MIVF_ACTUAL_M1 A2
ON M.MSISDN = A2.MSISDN
LEFT JOIN
WORK.MIVF_ACTUAL_M2 A3
ON M.MSISDN = A3.MSISDN
LEFT  JOIN 
WORK.MIVF_INTENSIVO_M0 B 
ON M.MSISDN = B.MSISDN
LEFT JOIN 
WORK.MIVF_PASIVO_M0 C
ON M.MSISDN = C.MSISDN; 
        
set mapred.job.name = PAR_EXP_MIVF_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_MIVF_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_MIVF_CLI AS
SELECT     
M.NIF,
SUM(coalesce(a.num_acc_mivf_web_m0, b.num_acc_mivf_web_m0, 0)) as num_acc_mivf_web_m0,
SUM(coalesce(a.num_acc_mivf_app_m0, b.num_acc_mivf_app_m0, 0)) as num_acc_mivf_app_m0,
SUM(coalesce(a.num_acc_mivf_m0, b.num_acc_mivf_m0, 0)) as num_acc_mivf_m0,

SUM(coalesce(a.num_acc_mivf_web_3m, b.num_acc_mivf_web_3m, 0)) as num_acc_mivf_web_3m,
SUM(coalesce(a.num_acc_mivf_app_3m, b.num_acc_mivf_app_3m, 0)) as num_acc_mivf_app_3m,
SUM(coalesce(a.num_acc_mivf_3m, b.num_acc_mivf_3m, 0)) as num_acc_mivf_3m,

max(coalesce(a.ind_mivf_actual, b.ind_mivf_actual, 0)) as ind_mivf_actual,
max(coalesce(a.ind_mivf_intensivo_3m, b.ind_mivf_intensivo_3m, 0)) as ind_mivf_intensivo_3m,
max(coalesce(a.ind_mivf_pasivo_3m, b.ind_mivf_pasivo_3m, 0)) as ind_mivf_pasivo_3m

FROM 
(SELECT DISTINCT X_ID_RED AS MSISDN, X_NUM_IDENT AS NIF  
 FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') M
LEFT JOIN
VF_POSTPAID.PAR_EXP_MIVF A
ON M.MSISDN = A.MSISDN
LEFT JOIN
VF_POSTPAID.PAR_EXP_MIVF B
ON M.NIF = B.MSISDN
GROUP BY M.NIF;

DROP TABLE WORK.MIVF_ACTUAL_M0;
DROP TABLE WORK.MIVF_ACTUAL_M1;
DROP TABLE WORK.MIVF_ACTUAL_M2;
DROP TABLE WORK.MIVF_INTENSIVO_M0;
DROP TABLE WORK.MIVF_PASIVO_M0;
        
EXIT;

