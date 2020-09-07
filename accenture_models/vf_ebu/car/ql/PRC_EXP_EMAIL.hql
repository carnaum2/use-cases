USE VF_EBU;

set mapred.job.name = WORK_EMAIL_GOO_MICRO;
DROP TABLE WORK.WORK_EMAIL_GOO_MICRO;
CREATE TABLE WORK.WORK_EMAIL_GOO_MICRO AS
SELECT
CIF_NIF,
CASE WHEN EMAIL LIKE '%GMAIL%' THEN 1 ELSE 0 END AS IND_GOOGLE_MAIL,
CASE WHEN EMAIL LIKE '%HOTMAIL%' THEN 1 ELSE 0 END AS IND_MICROSOFT_MAIL
FROM INPUT.VF_EBU_CONTACTO_AUTORIZADO;


set mapred.job.name = EMP_EXP_EMAIL;
DROP TABLE VF_EBU.EMP_EXP_EMAIL;
CREATE TABLE VF_EBU.EMP_EXP_EMAIL AS
SELECT
CIF_NIF,
MAX(IND_MICROSOFT_MAIL) AS IND_MICROSOFT_MAIL,
MAX(IND_GOOGLE_MAIL) AS IND_GOOGLE_MAIL,
SUM(IND_MICROSOFT_MAIL) AS NUM_MICROSOFT_MAIL,
SUM(IND_GOOGLE_MAIL) AS NUM_GOOGLE_MAIL
FROM WORK.WORK_EMAIL_GOO_MICRO A GROUP BY CIF_NIF;


DROP TABLE WORK.WORK_EMAIL_GOO_MICRO;


EXIT;
