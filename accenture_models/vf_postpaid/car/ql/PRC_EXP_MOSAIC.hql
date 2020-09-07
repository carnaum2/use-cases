USE VF_POSTPAID;

SET mapred.job.name = WORK_MOSAICO_1;

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

DROP TABLE WORK.WORK_MOSAICO_1;

CREATE TABLE  WORK.WORK_MOSAICO_1  AS
SELECT   
MSISDN,
CAST(IND_USO_TU_Y_YO_1M AS DOUBLE) AS IND_USO_TU_Y_YO_1M,
CAST(IND_USO_TU_Y_YO_3M AS DOUBLE) AS IND_USO_TU_Y_YO_3M,
CAST(LLAMADAS_MOVISTAR_1M AS DOUBLE) AS NUM_LLAM_CC_MV_1M,
CAST(LLAMADAS_MOVISTAR_3M AS DOUBLE) AS NUM_LLAM_CC_MV_3M ,
CAST(LLAMADAS_ORANGE_1M AS DOUBLE) AS NUM_LLAM_CC_OG_1M ,
CAST(LLAMADAS_ORANGE_3M AS DOUBLE) AS NUM_LLAM_CC_OG_3M ,
CAST(LLAMADAS_YOIGO_1M AS DOUBLE) AS NUM_LLAM_CC_YO_1M ,
CAST(LLAMADAS_YOIGO_3M AS DOUBLE) AS NUM_LLAM_CC_YO_3M ,
CAST(LLAMADAS_OOM_1M AS DOUBLE) AS NUM_LLAM_NO_VF_1M,
CAST(LLAMADAS_OOM_3M AS DOUBLE) AS NUM_LLAM_NO_VF_3M,
CAST(MOU_LLAM_MOVISTAR_1M AS DOUBLE)  AS NUM_MIN_CC_MV_1M ,
CAST(MOU_LLAM_MOVISTAR_3M AS DOUBLE)  AS NUM_MIN_CC_MV_3M ,
CAST(MOU_LLAM_ORANGE_1M AS DOUBLE) AS NUM_MIN_CC_OG_1M ,
CAST(MOU_LLAM_ORANGE_3M AS DOUBLE) AS NUM_MIN_CC_OG_3M ,
CAST(MOU_LLAM_YOIGO_1M AS DOUBLE) AS NUM_MIN_CC_YO_1M ,
CAST(MOU_LLAM_YOIGO_3M AS DOUBLE) AS NUM_MIN_CC_YO_3M ,
CAST(MOU_OOM_1M AS DOUBLE) AS NUM_MIN_NO_VF_1M,
CAST(MOU_OOM_3M AS DOUBLE) AS NUM_MIN_NO_VF_3M,
CAST(ARPU_MESANTERIOR AS DOUBLE) AS IMP_ARPU_MES_ANTERIOR,
CAST(LLAMADAS_MOVISTAR_1M AS DOUBLE) + CAST(LLAMADAS_ORANGE_1M AS DOUBLE) + CAST(LLAMADAS_YOIGO_1M AS DOUBLE)  AS NUM_LLAM_NOVF_CC_1M,
CAST(LLAMADAS_MOVISTAR_3M AS DOUBLE) + CAST(LLAMADAS_ORANGE_3M AS DOUBLE) + CAST(LLAMADAS_YOIGO_3M AS DOUBLE)  AS NUM_LLAM_NOVF_CC_3M,
CAST(MOU_LLAM_MOVISTAR_1M AS DOUBLE) + CAST(MOU_LLAM_ORANGE_1M AS DOUBLE) + CAST(MOU_LLAM_YOIGO_1M AS DOUBLE)  AS NUM_MIN_NOVF_CC_1M,
CAST(MOU_LLAM_MOVISTAR_3M AS DOUBLE) + CAST(MOU_LLAM_ORANGE_3M AS DOUBLE) + CAST(MOU_LLAM_YOIGO_3M AS DOUBLE)  AS NUM_MIN_NOVF_CC_3M,
CAST(PARTITIONED_MONTH AS DOUBLE) AS PARTITIONED_MONTH
FROM INPUT.VF_POS_MOSAICO WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}';  

SET mapred.job.name = PAR_EXP_MOSAICO;

DROP TABLE VF_POSTPAID.PAR_EXP_MOSAICO;

CREATE TABLE VF_POSTPAID.PAR_EXP_MOSAICO AS
SELECT
A.MSISDN,
MAX(B.NIF) AS NIF,
MAX(IND_USO_TU_Y_YO_1M) AS IND_USO_TU_Y_YO_1M,
MAX(IND_USO_TU_Y_YO_3M) AS IND_USO_TU_Y_YO_3M,
MAX(NUM_LLAM_CC_MV_1M) AS NUM_LLAM_CC_MV_1M,
MAX(ROUND(NUM_LLAM_CC_MV_3M,2)) AS NUM_LLAM_CC_MV_3M,
MAX(NUM_LLAM_CC_OG_1M) AS NUM_LLAM_CC_OG_1M,
MAX(ROUND(NUM_LLAM_CC_OG_3M,2)) AS NUM_LLAM_CC_OG_3M,
MAX(NUM_LLAM_CC_YO_1M) AS NUM_LLAM_CC_YO_1M,
MAX(ROUND(NUM_LLAM_CC_YO_3M,2)) AS NUM_LLAM_CC_YO_3M,
MAX(NUM_LLAM_NO_VF_1M) AS NUM_LLAM_NO_VF_1M,
MAX(ROUND(NUM_LLAM_NO_VF_3M,2)) AS NUM_LLAM_NO_VF_3M,
MAX(NUM_MIN_CC_MV_1M) AS NUM_MIN_CC_MV_1M,
MAX(ROUND(NUM_MIN_CC_MV_3M,2)) AS NUM_MIN_CC_MV_3M,
MAX(NUM_MIN_CC_OG_1M) AS NUM_MIN_CC_OG_1M,
MAX(ROUND(NUM_MIN_CC_OG_3M,2)) AS NUM_MIN_CC_OG_3M,
MAX(NUM_MIN_CC_YO_1M) AS NUM_MIN_CC_YO_1M,
MAX(ROUND(NUM_MIN_CC_YO_3M,2)) AS NUM_MIN_CC_YO_3M,
MAX(NUM_MIN_NO_VF_1M) AS NUM_MIN_NO_VF_1M,
MAX(ROUND(NUM_MIN_NO_VF_3M,2)) AS NUM_MIN_NO_VF_3M,
MAX(IMP_ARPU_MES_ANTERIOR) AS IMP_ARPU_MES_ANTERIOR,
MAX(NUM_LLAM_NOVF_CC_1M) AS NUM_LLAM_NOVF_CC_1M,
MAX(ROUND(NUM_LLAM_NOVF_CC_3M,2)) AS NUM_LLAM_NOVF_CC_3M,
MAX(NUM_MIN_NOVF_CC_1M) AS NUM_MIN_NOVF_CC_1M,
MAX(ROUND(NUM_MIN_NOVF_CC_3M,2)) AS NUM_MIN_NOVF_CC_3M,
MAX(PARTITIONED_MONTH) AS PARTITIONED_MONTH
FROM WORK.WORK_MOSAICO_1 A
INNER JOIN 
(SELECT X_NUM_IDENT AS NIF, X_ID_RED AS MSISDN  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') B
ON A.MSISDN = B.MSISDN
GROUP BY A.MSISDN
;

SET mapred.job.name = PAR_EXP_MOSAICO_CLI;

DROP TABLE VF_POSTPAID.PAR_EXP_MOSAICO_CLI;

CREATE TABLE VF_POSTPAID.PAR_EXP_MOSAICO_CLI AS
SELECT
NIF,
MAX(IND_USO_TU_Y_YO_1M) AS IND_USO_TU_Y_YO_1M,
MAX(IND_USO_TU_Y_YO_3M) AS IND_USO_TU_Y_YO_3M,
MAX(NUM_LLAM_CC_MV_1M) AS NUM_LLAM_CC_MV_1M,
MAX(NUM_LLAM_CC_MV_3M) AS NUM_LLAM_CC_MV_3M,
MAX(NUM_LLAM_CC_OG_1M) AS NUM_LLAM_CC_OG_1M,
MAX(NUM_LLAM_CC_OG_3M) AS NUM_LLAM_CC_OG_3M,
MAX(NUM_LLAM_CC_YO_1M) AS NUM_LLAM_CC_YO_1M,
MAX(NUM_LLAM_CC_YO_3M) AS NUM_LLAM_CC_YO_3M,
MAX(NUM_LLAM_NO_VF_1M) AS NUM_LLAM_NO_VF_1M,
MAX(NUM_LLAM_NO_VF_3M) AS NUM_LLAM_NO_VF_3M,
MAX(NUM_MIN_CC_MV_1M) AS NUM_MIN_CC_MV_1M,
MAX(NUM_MIN_CC_MV_3M) AS NUM_MIN_CC_MV_3M,
MAX(NUM_MIN_CC_OG_1M) AS NUM_MIN_CC_OG_1M,
MAX(NUM_MIN_CC_OG_3M) AS NUM_MIN_CC_OG_3M,
MAX(NUM_MIN_CC_YO_1M) AS NUM_MIN_CC_YO_1M,
MAX(NUM_MIN_CC_YO_3M) AS NUM_MIN_CC_YO_3M,
MAX(NUM_MIN_NO_VF_1M) AS NUM_MIN_NO_VF_1M,
MAX(NUM_MIN_NO_VF_3M) AS NUM_MIN_NO_VF_3M,
MAX(IMP_ARPU_MES_ANTERIOR) AS IMP_ARPU_MES_ANTERIOR,
MAX(NUM_LLAM_NOVF_CC_1M) AS NUM_LLAM_NOVF_CC_1M,
MAX(NUM_LLAM_NOVF_CC_3M) AS NUM_LLAM_NOVF_CC_3M,
MAX(NUM_MIN_NOVF_CC_1M) AS NUM_MIN_NOVF_CC_1M,
MAX(NUM_MIN_NOVF_CC_3M) AS NUM_MIN_NOVF_CC_3M,
MAX(PARTITIONED_MONTH) AS PARTITIONED_MONTH
FROM VF_POSTPAID.PAR_EXP_MOSAICO
GROUP BY NIF
;

DROP TABLE WORK.WORK_MOSAICO_1;

exit;


