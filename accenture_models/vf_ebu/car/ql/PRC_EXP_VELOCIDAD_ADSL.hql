USE VF_EBU;


set mapred.job.name = WORK_VELOCIDAD_ADSL_${hiveconf:MONTH0};
DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH0};
CREATE TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH0} AS 
SELECT
A.CIF_NIF,
case when A.SERVICE_ID = '' THEN NULL ELSE A.SERVICE_ID END AS SERVICE_ID,
CAST(B.NUMERICO AS DOUBLE) AS VELOCIDAD_CONTRATADA
FROM 
(SELECT *  FROM INPUT.VF_EBU_AC_EMP_FIJO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}' AND TIPO_PRODUCTO LIKE 'ADSL') A
LEFT JOIN
MASTER.VF_EBU_CAT_EMP_MAESTRO_VEL_ADSL B
ON A.VELOCIDAD=B.VELOCIDAD;

set mapred.job.name = WORK_CALIDAD_ADSL_${hiveconf:MONTH0};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH0};
CREATE TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH0} AS 
SELECT
A.CIF_NIF,
B.SERVICEID,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MEJOR_VEL_DOWN_SERV,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_VEL_DOWN_ACTUAL,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_CONTRATADA,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_SERV,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_MAX_ROUTER_ACTU,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_UP)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_UP_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL_SUBIDA,

VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MAX_VEL_DOWN_ALCAN       ,0)) AS ADSL_MAX_VEL_DOWN_ALCAN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_DOWN           ,0)) AS ADSL_MEJOR_VEL_DOWN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_UP             ,0)) AS ADSL_MEJOR_VEL_UP,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_DOWN_ACTUAL          ,0)) AS ADSL_VEL_DOWN_ACTUAL,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_UP_ACTUAL            ,0)) AS ADSL_VEL_UP_ACTUAL,
VF_FUNC.TONUMBERIFERROR(FLAG_VEL_MENOR_50) AS FLAG_VEL_MENOR_50,
VF_FUNC.TONUMBERIFERROR(FLAG_DEGRADACION_SERV) AS FLAG_DEGRADACION_SERV ,
VF_FUNC.TONUMBERIFERROR(FLAG_CORTES) AS FLAG_CORTES
FROM 
WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH0} A 
LEFT JOIN
(SELECT * FROM INPUT.VF_EBU_VELOCIDAD_ADSL_EMP WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') B
ON A.SERVICE_ID=B.SERVICEID;


-- VELOCIDAD CONTRATADA DE ADSL
set mapred.job.name = WORK_VELOCIDAD_ADSL_${hiveconf:MONTH1};
DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH1};
CREATE TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH1} AS 
SELECT 
A.CIF_NIF,
case when A.SERVICE_ID = '' THEN NULL ELSE A.SERVICE_ID END AS SERVICE_ID,
CAST(B.NUMERICO AS DOUBLE) AS VELOCIDAD_CONTRATADA
FROM 
(SELECT *  FROM INPUT.VF_EBU_AC_EMP_FIJO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}' AND TIPO_PRODUCTO LIKE 'ADSL') A
LEFT JOIN
MASTER.VF_EBU_CAT_EMP_MAESTRO_VEL_ADSL B
ON A.VELOCIDAD=B.VELOCIDAD;

set mapred.job.name = WORK_CALIDAD_ADSL_${hiveconf:MONTH1};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH1};
CREATE TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH1} AS 
SELECT
A.CIF_NIF,
B.SERVICEID,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MEJOR_VEL_DOWN_SERV,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_VEL_DOWN_ACTUAL,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_CONTRATADA,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_SERV,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_MAX_ROUTER_ACTU,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_UP)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_UP_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL_SUBIDA,

VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MAX_VEL_DOWN_ALCAN,0)) AS ADSL_MAX_VEL_DOWN_ALCAN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_DOWN,0)) AS ADSL_MEJOR_VEL_DOWN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_UP,0)) AS ADSL_MEJOR_VEL_UP,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_DOWN_ACTUAL,0)) AS ADSL_VEL_DOWN_ACTUAL,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_UP_ACTUAL,0)) AS ADSL_VEL_UP_ACTUAL,
VF_FUNC.TONUMBERIFERROR(FLAG_VEL_MENOR_50) AS FLAG_VEL_MENOR_50,
VF_FUNC.TONUMBERIFERROR(FLAG_DEGRADACION_SERV) AS FLAG_DEGRADACION_SERV ,
VF_FUNC.TONUMBERIFERROR(FLAG_CORTES) AS FLAG_CORTES
FROM 
WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH1} A
LEFT JOIN
(SELECT * FROM INPUT.VF_EBU_VELOCIDAD_ADSL_EMP WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}') B
ON A.SERVICE_ID=B.SERVICEID;


-- VELOCIDAD CONTRATADA DE ADSL
set mapred.job.name = WORK_VELOCIDAD_ADSL_${hiveconf:MONTH2};
DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH2};
CREATE TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH2} AS 
SELECT 
A.CIF_NIF,
case when A.SERVICE_ID = '' THEN NULL ELSE A.SERVICE_ID END AS SERVICE_ID,
CAST(B.NUMERICO AS DOUBLE) AS VELOCIDAD_CONTRATADA
FROM 
(SELECT *  FROM INPUT.VF_EBU_AC_EMP_FIJO  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH2}' AND TIPO_PRODUCTO LIKE 'ADSL') A
LEFT JOIN
MASTER.VF_EBU_CAT_EMP_MAESTRO_VEL_ADSL B
ON A.VELOCIDAD=B.VELOCIDAD;


set mapred.job.name = WORK_CALIDAD_ADSL_${hiveconf:MONTH2};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH2};
CREATE TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH2} AS 
SELECT
A.CIF_NIF,
B.SERVICEID,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MEJOR_VEL_DOWN_SERV,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_VEL_DOWN_ACTUAL,
NVL(ROUND(A.VELOCIDAD_CONTRATADA - VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_CONTRATADA,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024,2),0) AS NUM_DIF_MAX_ROUTER_SERV,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MAX_VEL_DOWN_ALCAN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_MAX_ROUTER_ACTU,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_DOWN)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_DOWN_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL,
NVL(ROUND(VF_FUNC.TONUMBERIFERROR(B.ADSL_MEJOR_VEL_UP)/1024 - VF_FUNC.TONUMBERIFERROR(B.ADSL_VEL_UP_ACTUAL)/1024,2),0) AS NUM_DIF_SERV_ACTUAL_SUBIDA,

VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MAX_VEL_DOWN_ALCAN       ,0)) AS ADSL_MAX_VEL_DOWN_ALCAN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_DOWN           ,0)) AS ADSL_MEJOR_VEL_DOWN,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_MEJOR_VEL_UP             ,0)) AS ADSL_MEJOR_VEL_UP,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_DOWN_ACTUAL          ,0)) AS ADSL_VEL_DOWN_ACTUAL,
VF_FUNC.TONUMBERIFERROR(NVL(ADSL_VEL_UP_ACTUAL            ,0)) AS ADSL_VEL_UP_ACTUAL,
VF_FUNC.TONUMBERIFERROR(FLAG_VEL_MENOR_50) AS FLAG_VEL_MENOR_50,
VF_FUNC.TONUMBERIFERROR(FLAG_DEGRADACION_SERV) AS FLAG_DEGRADACION_SERV ,
VF_FUNC.TONUMBERIFERROR(FLAG_CORTES) AS FLAG_CORTES
FROM 
WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH2} A
LEFT JOIN
(SELECT * FROM INPUT.VF_EBU_VELOCIDAD_ADSL_EMP WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}') B
ON A.SERVICE_ID=B.SERVICEID;
      
set mapred.job.name = EMP_EXP_CALIDAD_ADSL;
DROP TABLE VF_EBU.EMP_EXP_CALIDAD_ADSL;
CREATE TABLE VF_EBU.EMP_EXP_CALIDAD_ADSL AS
SELECT 
A.CIF_NIF ,
ROUND(MAX(A.NUM_DIF_MEJOR_VEL_DOWN_SERV),2) AS NUM_DIF_MEJOR_VEL_DOWN_SERV ,
ROUND(MAX(A.NUM_DIF_VEL_DOWN_ACTUAL    ),2) AS NUM_DIF_VEL_DOWN_ACTUAL ,
ROUND(MAX(A.NUM_DIF_MAX_ROUTER_CONTRATADA),2) AS NUM_DIF_MAX_ROUTER_CONTRATADA ,
ROUND(MAX(A.NUM_DIF_MAX_ROUTER_SERV   ),2) AS NUM_DIF_MAX_ROUTER_SERV ,
ROUND(MAX(A.NUM_DIF_MAX_ROUTER_ACTU   ),2) AS  NUM_DIF_MAX_ROUTER_ACTU,
ROUND(MAX(A.NUM_DIF_SERV_ACTUAL       ),2) AS  NUM_DIF_SERV_ACTUAL,
ROUND(MAX(A.NUM_DIF_SERV_ACTUAL_SUBIDA),2) AS NUM_DIF_SERV_ACTUAL_SUBIDA ,
ROUND(MAX(A.ADSL_MAX_VEL_DOWN_ALCAN   )/1024,2) AS NUM_MAX_VEL_DOWN_ALCAN,
ROUND(MAX(A.ADSL_MEJOR_VEL_DOWN       )/1024,2) AS NUM_MEJOR_VEL_DOWN ,
ROUND(MAX(A.ADSL_MEJOR_VEL_UP         )/1024,2) AS  NUM_MEJOR_VEL_UP,
ROUND(MAX(A.ADSL_VEL_DOWN_ACTUAL      )/1024,2) AS NUM_VEL_DOWN_ACTUAL ,
ROUND(MAX(A.ADSL_VEL_UP_ACTUAL        )/1024,2) AS NUM_VEL_UP_ACTUAL ,
ROUND(MAX(A.FLAG_VEL_MENOR_50         ),2) AS IND_VEL_MENOR_50 ,
ROUND(MAX(A.FLAG_DEGRADACION_SERV     ),2) AS IND_DEGRADACION_SERV ,
ROUND(MAX(A.FLAG_CORTES               ),2) AS IND_CORTES ,     

ROUND(MAX(GREATEST(A.NUM_DIF_MEJOR_VEL_DOWN_SERV  ,  B.NUM_DIF_MEJOR_VEL_DOWN_SERV  ,  C.NUM_DIF_MEJOR_VEL_DOWN_SERV  )),2) AS NUM_DIF_MAX_MEJ_VEL_D_SERV_3M  ,
ROUND(MAX(GREATEST(A.NUM_DIF_VEL_DOWN_ACTUAL      ,  B.NUM_DIF_VEL_DOWN_ACTUAL      ,  C.NUM_DIF_VEL_DOWN_ACTUAL      )),2) AS NUM_DIF_MAX_VEL_DOWN_ACTUAL_3M      ,
ROUND(MAX(GREATEST(A.NUM_DIF_MAX_ROUTER_CONTRATADA,  B.NUM_DIF_MAX_ROUTER_CONTRATADA,  C.NUM_DIF_MAX_ROUTER_CONTRATADA)),2) AS NUM_DIF_MAX_ROUT_CONTRATADA_3M,
ROUND(MAX(GREATEST(A.NUM_DIF_MAX_ROUTER_SERV      ,  B.NUM_DIF_MAX_ROUTER_SERV      ,  C.NUM_DIF_MAX_ROUTER_SERV      )),2) AS NUM_DIF_MAX_ROUTER_SERV_3M      ,
ROUND(MAX(GREATEST(A.NUM_DIF_MAX_ROUTER_ACTU      ,  B.NUM_DIF_MAX_ROUTER_ACTU      ,  C.NUM_DIF_MAX_ROUTER_ACTU      )),2) AS NUM_DIF_MAX_ROUTER_ACTU_3M      ,
ROUND(MAX(GREATEST(A.NUM_DIF_SERV_ACTUAL          ,  B.NUM_DIF_SERV_ACTUAL          ,  C.NUM_DIF_SERV_ACTUAL          )),2) AS NUM_DIF_MAX_SERV_ACTUAL_3M          ,
ROUND(MAX(GREATEST(A.NUM_DIF_SERV_ACTUAL_SUBIDA   ,  B.NUM_DIF_SERV_ACTUAL_SUBIDA   ,  C.NUM_DIF_SERV_ACTUAL_SUBIDA   )),2) AS NUM_DIF_MAX_SERV_ACTUAL_SUB_3M   ,
ROUND(MAX(GREATEST(A.ADSL_MAX_VEL_DOWN_ALCAN      ,  B.ADSL_MAX_VEL_DOWN_ALCAN      ,  C.ADSL_MAX_VEL_DOWN_ALCAN      ))/1024,2) AS NUM_MAX_VEL_DOWN_ALCAN_3M      ,
ROUND(MAX(GREATEST(A.ADSL_MEJOR_VEL_DOWN          ,  B.ADSL_MEJOR_VEL_DOWN          ,  C.ADSL_MEJOR_VEL_DOWN          ))/1024,2) AS NUM_MAX_MEJOR_VEL_DOWN_3M          ,
ROUND(MAX(GREATEST(A.ADSL_MEJOR_VEL_UP            ,  B.ADSL_MEJOR_VEL_UP            ,  C.ADSL_MEJOR_VEL_UP            ))/1024,2) AS NUM_MAX_MEJOR_VEL_UP_3M            ,
ROUND(MAX(GREATEST(A.ADSL_VEL_DOWN_ACTUAL         ,  B.ADSL_VEL_DOWN_ACTUAL         ,  C.ADSL_VEL_DOWN_ACTUAL         ))/1024,2) AS NUM_MAX_VEL_DOWN_ACTUAL_3M         ,
ROUND(MAX(GREATEST(A.ADSL_VEL_UP_ACTUAL           ,  B.ADSL_VEL_UP_ACTUAL           ,  C.ADSL_VEL_UP_ACTUAL           ))/1024,2) AS NUM_MAX_VEL_UP_ACTUAL_3M           ,
ROUND(MAX(GREATEST(A.FLAG_VEL_MENOR_50            ,  B.FLAG_VEL_MENOR_50            ,  C.FLAG_VEL_MENOR_50            )),2) AS IND_VEL_MENOR_50_3M            ,
ROUND(MAX(GREATEST(A.FLAG_DEGRADACION_SERV        ,  B.FLAG_DEGRADACION_SERV        ,  C.FLAG_DEGRADACION_SERV        )),2) AS IND_DEGRADACION_SERV_3M        ,
ROUND(MAX(GREATEST(A.FLAG_CORTES                  ,  B.FLAG_CORTES                  ,  C.FLAG_CORTES                  )),2) AS IND_CORTES_3M                  ,

ROUND(MAX(LEAST(A.NUM_DIF_MEJOR_VEL_DOWN_SERV      ,  B.NUM_DIF_MEJOR_VEL_DOWN_SERV      ,  C.NUM_DIF_MEJOR_VEL_DOWN_SERV     )),2) AS NUM_DIF_MIN_MAX_MEJ_VEL_D_S_3M   ,
ROUND(MAX(LEAST(A.NUM_DIF_VEL_DOWN_ACTUAL          ,  B.NUM_DIF_VEL_DOWN_ACTUAL          ,  C.NUM_DIF_VEL_DOWN_ACTUAL         )),2) AS NUM_DIF_MIN_VEL_DOWN_ACTUAL_3M       ,
ROUND(MAX(LEAST(A.NUM_DIF_MAX_ROUTER_CONTRATADA    ,  B.NUM_DIF_MAX_ROUTER_CONTRATADA    ,  C.NUM_DIF_MAX_ROUTER_CONTRATADA   )),2) AS NUM_DIF_MIN_MAX_ROUT_CONT_3M ,
ROUND(MAX(LEAST(A.NUM_DIF_MAX_ROUTER_SERV          ,  B.NUM_DIF_MAX_ROUTER_SERV          ,  C.NUM_DIF_MAX_ROUTER_SERV         )),2) AS NUM_DIF_MIN_MAX_ROUTER_SERV_3M       ,
ROUND(MAX(LEAST(A.NUM_DIF_MAX_ROUTER_ACTU          ,  B.NUM_DIF_MAX_ROUTER_ACTU          ,  C.NUM_DIF_MAX_ROUTER_ACTU         )),2) AS NUM_DIF_MIN_MAX_ROUTER_ACTU_3M       ,
ROUND(MAX(LEAST(A.NUM_DIF_SERV_ACTUAL              ,  B.NUM_DIF_SERV_ACTUAL              ,  C.NUM_DIF_SERV_ACTUAL             )),2) AS NUM_DIF_MIN_SERV_ACTUAL_3M           ,
ROUND(MAX(LEAST(A.NUM_DIF_SERV_ACTUAL_SUBIDA       ,  B.NUM_DIF_SERV_ACTUAL_SUBIDA       ,  C.NUM_DIF_SERV_ACTUAL_SUBIDA      )),2) AS NUM_DIF_MIN_SERV_ACTUAL_SUB_3M    ,
ROUND(MAX(LEAST(A.ADSL_MAX_VEL_DOWN_ALCAN          ,  B.ADSL_MAX_VEL_DOWN_ALCAN          ,  C.ADSL_MAX_VEL_DOWN_ALCAN         ))/1024,2) AS NUM_MAX_MIN_VEL_DOWN_ALCAN_3M       ,
ROUND(MAX(LEAST(A.ADSL_MEJOR_VEL_DOWN              ,  B.ADSL_MEJOR_VEL_DOWN              ,  C.ADSL_MEJOR_VEL_DOWN             ))/1024,2) AS NUM_MEJOR_MIN_VEL_DOWN_3M           ,
ROUND(MAX(LEAST(A.ADSL_MEJOR_VEL_UP                ,  B.ADSL_MEJOR_VEL_UP                ,  C.ADSL_MEJOR_VEL_UP               ))/1024,2) AS NUM_MEJOR_MIN_VEL_UP_3M             ,
ROUND(MAX(LEAST(A.ADSL_VEL_DOWN_ACTUAL             ,  B.ADSL_VEL_DOWN_ACTUAL             ,  C.ADSL_VEL_DOWN_ACTUAL            ))/1024,2) AS NUM_VEL_MIN_DOWN_ACTUAL_3M          ,
ROUND(MAX(LEAST(A.ADSL_VEL_UP_ACTUAL               ,  B.ADSL_VEL_UP_ACTUAL               ,  C.ADSL_VEL_UP_ACTUAL              ))/1024,2) AS NUM_VEL_MIN_UP_ACTUAL_3M            ,

ROUND(MAX((A.NUM_DIF_MEJOR_VEL_DOWN_SERV          +  B.NUM_DIF_MEJOR_VEL_DOWN_SERV          +  C.NUM_DIF_MEJOR_VEL_DOWN_SERV        )/3),2) AS NUM_DIF_MEJOR_VEL_DOWN_SERV_3M       ,

ROUND(MAX((A.NUM_DIF_VEL_DOWN_ACTUAL          +  B.NUM_DIF_VEL_DOWN_ACTUAL          +  C.NUM_DIF_VEL_DOWN_ACTUAL        )/3),2) AS NUM_DIF_AVG_VEL_DOWN_ACTUAL_3M       ,
ROUND(MAX((A.NUM_DIF_MAX_ROUTER_CONTRATADA    +  B.NUM_DIF_MAX_ROUTER_CONTRATADA    +  C.NUM_DIF_MAX_ROUTER_CONTRATADA  )/3),2) AS NUM_DIF_AVG_MAX_ROUT_CONTR_3M ,
ROUND(MAX((A.NUM_DIF_MAX_ROUTER_SERV          +  B.NUM_DIF_MAX_ROUTER_SERV          +  C.NUM_DIF_MAX_ROUTER_SERV        )/3),2) AS NUM_DIF_AVG_MAX_ROUTER_SERV_3M       ,
ROUND(MAX((A.NUM_DIF_MAX_ROUTER_ACTU          +  B.NUM_DIF_MAX_ROUTER_ACTU          +  C.NUM_DIF_MAX_ROUTER_ACTU        )/3),2) AS NUM_DIF_AVG_MAX_ROUTER_ACTU_3M       ,
ROUND(MAX((A.NUM_DIF_SERV_ACTUAL              +  B.NUM_DIF_SERV_ACTUAL              +  C.NUM_DIF_SERV_ACTUAL            )/3),2) AS NUM_DIF_AVG_SERV_ACTUAL_3M           ,
ROUND(MAX((A.NUM_DIF_SERV_ACTUAL_SUBIDA       +  B.NUM_DIF_SERV_ACTUAL_SUBIDA       +  C.NUM_DIF_SERV_ACTUAL_SUBIDA     )/3),2) AS NUM_DIF_AVG_SERV_ACTUAL_SUB_3M    ,
ROUND(MAX((A.ADSL_MAX_VEL_DOWN_ALCAN          +  B.ADSL_MAX_VEL_DOWN_ALCAN          +  C.ADSL_MAX_VEL_DOWN_ALCAN        )/3)/1024,2) AS NUM_AVG_MAX_VEL_DOWN_ALCAN_3M       ,
ROUND(MAX((A.ADSL_MEJOR_VEL_DOWN              +  B.ADSL_MEJOR_VEL_DOWN              +  C.ADSL_MEJOR_VEL_DOWN            )/3)/1024,2) AS NUM_AVG_MEJOR_VEL_DOWN_3M           ,
ROUND(MAX((A.ADSL_MEJOR_VEL_UP                +  B.ADSL_MEJOR_VEL_UP                +  C.ADSL_MEJOR_VEL_UP              )/3)/1024,2) AS NUM_AVG_MEJOR_VEL_UP_3M             ,
ROUND(MAX((A.ADSL_VEL_DOWN_ACTUAL             +  B.ADSL_VEL_DOWN_ACTUAL             +  C.ADSL_VEL_DOWN_ACTUAL           )/3)/1024,2) AS NUM_AVG_VEL_DOWN_ACTUAL_3M          ,
ROUND(MAX((A.ADSL_VEL_UP_ACTUAL               +  B.ADSL_VEL_UP_ACTUAL               +  C.ADSL_VEL_UP_ACTUAL             )/3)/1024,2) AS NUM_AVG_VEL_UP_ACTUAL_3M
FROM WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH0} A
INNER JOIN
WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH1} B
ON A.SERVICEID=B.SERVICEID
INNER JOIN
WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH2} C
ON A.SERVICEID=C.SERVICEID
GROUP BY A.CIF_NIF;


DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH0};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH0};
DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH1};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH1};
DROP TABLE WORK.WORK_VELOCIDAD_ADSL_${hiveconf:MONTH2};
DROP TABLE WORK.WORK_CALIDAD_ADSL_${hiveconf:MONTH2};

EXIT;