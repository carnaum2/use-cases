USE OUTPUT;

set mapred.job.name = DO_PAR_QUIMERA_${hiveconf:MONTH0};
DROP TABLE OUTPUT.DO_PAR_QUIMERA_${hiveconf:MONTH0};
CREATE TABLE OUTPUT.DO_PAR_QUIMERA_${hiveconf:MONTH0} AS
SELECT
A.MSISDN,
CAST(A.ACTUALVOLUME AS DOUBLE) AS ACTUALVOLUME,
CAST(A.VOLUME3G AS DOUBLE) AS VOLUME3G,
CAST(A.NUM_ACCESSOS AS DOUBLE) AS NUM_ACCESOS,
CAST(A.NUM_DIAS_ACCESOS AS DOUBLE) AS NUM_DIAS
FROM
(SELECT * FROM INPUT.VF_POS_TARIFICADOR WHERE PARTITIONED_MONTH= '${hiveconf:MONTH0}') A;

EXIT;