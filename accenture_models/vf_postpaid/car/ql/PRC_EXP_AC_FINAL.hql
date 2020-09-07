USE VF_POSTPAID;


set mapred.job.name = WORK_AC_FINAL_KEEP;
DROP TABLE WORK.WORK_AC_FINAL_KEEP;
CREATE TABLE WORK.WORK_AC_FINAL_KEEP AS
      SELECT
A.MSISDN AS MSISDN,
MAX(A.NIF) AS NIF,
MAX(A.SEG_SEXO) AS SEG_SEXO,
MAX(A.NUM_EDAD) AS NUM_EDAD,
MAX(A.SEG_TIPO_IDENT) AS SEG_TIPO_IDENT,
MAX(A.SEG_NACIONALIDAD) AS SEG_NACIONALIDAD,
MAX(A.IND_EXTRANJERO) AS IND_EXTRANJERO,
MAX(A.NUM_ANTIG_LIN) AS NUM_ANTIG_LIN,
MAX(CAST(A.IND_LINEA_VOZ AS DOUBLE)) AS IND_LINEA_VOZ,
MAX(CAST(A.IND_ADSL AS DOUBLE)) AS IND_ADSL,
MAX(CAST(A.IND_LPD AS DOUBLE)) AS IND_LPD,
MAX(CAST(A.IND_HZ AS DOUBLE)) AS IND_HZ,
MAX(CAST(A.IND_FIBRA AS DOUBLE)) AS IND_FIBRA,
MAX(CAST(A.IND_4G AS DOUBLE)) AS IND_4G,
MAX(CAST(A.IND_COBERTURA_4G AS DOUBLE)) AS IND_COBERTURA_4G,
MAX(CAST(A.IND_CONEXIONES_4G AS DOUBLE)) AS IND_CONEXIONES_4G,
MAX(CAST(A.IND_FACT_ELEC AS DOUBLE)) AS IND_FACT_ELEC,
MAX(CAST(A.IND_DEUDA AS DOUBLE)) AS IND_DEUDA,
MAX(A.SEG_SISTEMA_OPERATIVO) AS SEG_SISTEMA_OPERATIVO,
MAX(A.IND_OS_IOS) AS IND_OS_IOS,
MAX(A.IND_OS_ANDROID) AS IND_OS_ANDROID,
MAX(CAST(A.IND_TERMINAL_4G AS DOUBLE)) AS IND_TERMINAL_4G,
MAX(CAST(A.IND_HUELLA_ONO AS DOUBLE)) AS IND_HUELLA_ONO,
MAX(CAST(A.IND_HUELLA_VF AS DOUBLE)) AS IND_HUELLA_VF,
MAX(CAST(A.NUM_LINEAS_PREP AS DOUBLE)) AS NUM_LINEAS_PREP,
MAX(CAST(A.NUM_LINEAS_POS AS DOUBLE)) AS NUM_LINEAS_POS,
MAX(CAST(A.NUM_ARPU_M0 AS DOUBLE)) AS NUM_ARPU_M0     
FROM
(SELECT
X_ID_RED AS MSISDN,
X_NUM_IDENT AS NIF,
X_SEXO AS SEG_SEXO,
FLOOR(VF_FUNC.MONTHS_BETWEEN(VF_FUNC.TO_DATE(CONCAT(SUBSTR('${hiveconf:MONTH0}',1,4),'-',CONCAT(SUBSTR('${hiveconf:MONTH0}',5,2),'-01'))),
VF_FUNC.TO_DATE(SUBSTR(X_FECHA_NACIMIENTO,1,10))) / 12) AS NUM_EDAD,
X_TIPO_IDENT AS SEG_TIPO_IDENT,
X_NACIONALIDAD AS SEG_NACIONALIDAD,
CASE WHEN X_NACIONALIDAD = 'ESPA¿A' THEN 0 ELSE 1 END AS IND_EXTRANJERO,
FLOOR(VF_FUNC.MONTHS_BETWEEN(VF_FUNC.TO_DATE(CONCAT(SUBSTR('${hiveconf:MONTH0}',1,4),'-',CONCAT(SUBSTR('${hiveconf:MONTH0}',5,2), '-01'))),
VF_FUNC.TO_DATE(SUBSTR(X_FECHA_ACTIVACION,1,10))) / 12) AS NUM_ANTIG_LIN,  
FLAGVOZ AS IND_LINEA_VOZ,
FLAGADSL AS IND_ADSL,
FLAGLPD AS IND_LPD,
FLAGHZ AS IND_HZ,
FLAGFTTH AS IND_FIBRA,
FLAG_4G AS IND_4G,
COBERTURA_4G AS IND_COBERTURA_4G,
FLAG_4G_APERTURAS AS IND_CONEXIONES_4G,
FLAG_EBILLING AS IND_FACT_ELEC,
DEUDA AS IND_DEUDA,
CASE  WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%ANDROID%' THEN 'ANDROID' 
WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%WINDOWS%' THEN 'WINDOWS'
WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%NOKIA%' THEN 'NOKIA'
WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%BLACKBERRY%' THEN 'BLACKBERRY'
WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%WEBOS%' THEN 'WEBOS'
WHEN UPPER(SISTEMA_OPERATIVO) LIKE '%IOS%' THEN 'IOS'
ELSE 'RESTO' END
AS SEG_SISTEMA_OPERATIVO,
case when upper(sistema_operativo) like '%ANDROID%' then 1 else 0 end as ind_os_android,
case when upper(sistema_operativo) like '%IOS%' then 1 else 0 end as ind_os_ios,
TERMINAL_4G AS IND_TERMINAL_4G,
FLAG_HUELLA_ONO AS IND_HUELLA_ONO,
FLAG_HUELLA_VF AS IND_HUELLA_VF,
NUM_PREPAGO AS NUM_LINEAS_PREP,
NUM_POSPAGO AS NUM_LINEAS_POS,
ARPU AS NUM_ARPU_M0
FROM
INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}') A
GROUP BY A.MSISDN;
    
set mapred.job.name = WORK_AC_FINAL_KEEP_VAR_M0;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M0;
      CREATE TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M0 AS 
      SELECT 
        X_NUM_IDENT AS NIF,
        SUM(CAST(ARPU AS DOUBLE)) AS NUM_ARPU_M0,
        SUM(CAST(FLAGVOZ AS DOUBLE)) AS NUM_LINEA_VOZ,
        SUM(CAST(FLAGADSL AS DOUBLE)) AS NUM_ADSL,
        SUM(CAST(FLAGLPD AS DOUBLE)) AS NUM_LPD,
        SUM(CAST(FLAGHZ AS DOUBLE)) AS NUM_HZ,
        SUM(CAST(FLAGFTTH AS DOUBLE)) AS NUM_FIBRA,
        MAX(CAST(NUM_PREPAGO AS DOUBLE)) AS NUM_LINEAS_PREP,
        MAX(CAST(NUM_POSPAGO AS DOUBLE)) AS NUM_LINEAS_POS
       FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}'
      GROUP BY X_NUM_IDENT;

	  
set mapred.job.name = WORK_AC_FINAL_KEEP_VAR_M1;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M1;

      CREATE TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M1 AS 
      SELECT 
        X_NUM_IDENT AS NIF,
        SUM(CAST(ARPU AS DOUBLE)) AS NUM_ARPU_M0,
        SUM(CAST(FLAGVOZ AS DOUBLE)) AS NUM_LINEA_VOZ,
        SUM(CAST(FLAGADSL AS DOUBLE)) AS NUM_ADSL,
        SUM(CAST(FLAGLPD AS DOUBLE)) AS NUM_LPD,
        SUM(CAST(FLAGHZ AS DOUBLE)) AS NUM_HZ,
        SUM(CAST(FLAGFTTH AS DOUBLE)) AS NUM_FIBRA,
        MAX(CAST(NUM_PREPAGO AS DOUBLE)) AS NUM_LINEAS_PREP,
        MAX(CAST(NUM_POSPAGO AS DOUBLE)) AS NUM_LINEAS_POS
       FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH1}'
      GROUP BY X_NUM_IDENT;
	  

set mapred.job.name = WORK_AC_FINAL_KEEP_VAR_M2;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M2;

      CREATE TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M2 AS 
      SELECT 
        X_NUM_IDENT AS NIF,
        SUM(CAST(ARPU AS DOUBLE)) AS NUM_ARPU_M0,
        SUM(CAST(FLAGVOZ AS DOUBLE)) AS NUM_LINEA_VOZ,
        SUM(CAST(FLAGADSL AS DOUBLE)) AS NUM_ADSL,
        SUM(CAST(FLAGLPD AS DOUBLE)) AS NUM_LPD,
        SUM(CAST(FLAGHZ AS DOUBLE)) AS NUM_HZ,
        SUM(CAST(FLAGFTTH AS DOUBLE)) AS NUM_FIBRA,
        MAX(CAST(NUM_PREPAGO AS DOUBLE)) AS NUM_LINEAS_PREP,
        MAX(CAST(NUM_POSPAGO AS DOUBLE)) AS NUM_LINEAS_POS
       FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH2}'
      GROUP BY X_NUM_IDENT;
	  
      
set mapred.job.name = WORK_AC_FINAL_KEEP_VAR_M3;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M3;

      CREATE TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M3 AS 
      SELECT 
        X_NUM_IDENT AS NIF,
        SUM(CAST(ARPU AS DOUBLE)) AS NUM_ARPU_M0,
        SUM(CAST(FLAGVOZ AS DOUBLE)) AS NUM_LINEA_VOZ,
        SUM(CAST(FLAGADSL AS DOUBLE)) AS NUM_ADSL,
        SUM(CAST(FLAGLPD AS DOUBLE)) AS NUM_LPD,
        SUM(CAST(FLAGHZ AS DOUBLE)) AS NUM_HZ,
        SUM(CAST(FLAGFTTH AS DOUBLE)) AS NUM_FIBRA,
        MAX(CAST(NUM_PREPAGO AS DOUBLE)) AS NUM_LINEAS_PREP,
        MAX(CAST(NUM_POSPAGO AS DOUBLE)) AS NUM_LINEAS_POS
       FROM INPUT.VF_POS_AC_FINAL WHERE PARTITIONED_MONTH = '${hiveconf:MONTH3}'
      GROUP BY X_NUM_IDENT;
	  
      
set mapred.job.name = PAR_EXP_AC_FINAL_LIN;
DROP TABLE VF_POSTPAID.PAR_EXP_AC_FINAL_LIN;
 CREATE TABLE VF_POSTPAID.PAR_EXP_AC_FINAL_LIN AS
        SELECT
        A.MSISDN,
		A.NIF,
		A.SEG_SEXO,
		ROUND(A.NUM_EDAD,0) AS NUM_EDAD,
		A.SEG_TIPO_IDENT,
		A.SEG_NACIONALIDAD,
		ROUND(A.IND_EXTRANJERO,0) AS IND_EXTRANJERO,
		ROUND(A.NUM_ANTIG_LIN,0) AS NUM_ANTIG_LIN,
		ROUND(A.IND_LINEA_VOZ,0) AS IND_LINEA_VOZ,
		ROUND(A.IND_ADSL,0) AS IND_ADSL,
		ROUND(A.IND_LPD,0) AS IND_LPD,
		ROUND(A.IND_HZ,0) AS IND_HZ,
		ROUND(A.IND_FIBRA,0) AS IND_FIBRA,
		ROUND(A.IND_4G,0) AS IND_4G,
		ROUND(A.IND_COBERTURA_4G,0) AS IND_COBERTURA_4G,
		ROUND(A.IND_CONEXIONES_4G,0) AS IND_CONEXIONES_4G,
		ROUND(A.IND_FACT_ELEC,0) AS IND_FACT_ELEC,
		ROUND(A.IND_DEUDA,0) AS IND_DEUDA,
		A.SEG_SISTEMA_OPERATIVO,
		ROUND(A.IND_OS_IOS,0) AS IND_OS_IOS,
		ROUND(A.IND_OS_ANDROID,0) AS IND_OS_ANDROID,
		ROUND(A.IND_TERMINAL_4G,0) AS IND_TERMINAL_4G,
		ROUND(A.IND_HUELLA_ONO,0) AS IND_HUELLA_ONO,
		ROUND(A.IND_HUELLA_VF,0) AS IND_HUELLA_VF,
		ROUND(A.NUM_LINEAS_PREP,0) AS NUM_LINEAS_PREP,
		ROUND(A.NUM_LINEAS_POS,0) AS NUM_LINEAS_POS,
		ROUND(A.NUM_ARPU_M0,2) AS NUM_ARPU_M0,
        
        A0.NUM_LINEA_VOZ AS NUM_LINEAS_VOZ,
        
        
        ROUND(VF_FUNC.AvgN(A0.NUM_ARPU_M0,A1.NUM_ARPU_M0,A2.NUM_ARPU_M0,A3.NUM_ARPU_M0),2) AS NUM_ARPU_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_LINEA_VOZ,A1.NUM_LINEA_VOZ,A2.NUM_LINEA_VOZ,A3.NUM_LINEA_VOZ),2) AS NUM_LINEAS_VOZ_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_ADSL,A1.NUM_ADSL,A2.NUM_ADSL,A3.NUM_ADSL),2) AS NUM_ADSL_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_LPD,A1.NUM_LPD,A2.NUM_LPD,A3.NUM_LPD),2) AS NUM_LPD_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_HZ,A1.NUM_HZ,A2.NUM_HZ,A3.NUM_HZ),2) AS NUM_HZ_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_FIBRA,A1.NUM_FIBRA,A2.NUM_FIBRA,A3.NUM_FIBRA),2) AS NUM_FIBRA_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_LINEAS_PREP,A1.NUM_LINEAS_PREP,A2.NUM_LINEAS_PREP,A3.NUM_LINEAS_PREP),2) AS NUM_LINEAS_PREP_AVG,
        ROUND(VF_FUNC.AvgN(A0.NUM_LINEAS_POS,A1.NUM_LINEAS_POS,A2.NUM_LINEAS_POS,A3.NUM_LINEAS_POS),2) AS NUM_LINEAS_POS_AVG,
       
        ROUND(VF_FUNC.IncN(A0.NUM_ARPU_M0,A1.NUM_ARPU_M0,A2.NUM_ARPU_M0,A3.NUM_ARPU_M0),2) AS NUM_ARPU_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_LINEA_VOZ,A1.NUM_LINEA_VOZ,A2.NUM_LINEA_VOZ,A3.NUM_LINEA_VOZ),2) AS NUM_LINEAS_VOZ_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_ADSL,A1.NUM_ADSL,A2.NUM_ADSL,A3.NUM_ADSL),2) AS NUM_ADSL_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_LPD,A1.NUM_LPD,A2.NUM_LPD,A3.NUM_LPD),2) AS NUM_LPD_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_HZ,A1.NUM_HZ,A2.NUM_HZ,A3.NUM_HZ),2) AS NUM_HZ_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_FIBRA,A1.NUM_FIBRA,A2.NUM_FIBRA,A3.NUM_FIBRA),2) AS NUM_FIBRA_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_LINEAS_PREP,A1.NUM_LINEAS_PREP,A2.NUM_LINEAS_PREP,A3.NUM_LINEAS_PREP),2) AS NUM_LINEAS_PREP_INC,
        ROUND(VF_FUNC.IncN(A0.NUM_LINEAS_POS,A1.NUM_LINEAS_POS,A2.NUM_LINEAS_POS,A3.NUM_LINEAS_POS),2) AS NUM_LINEAS_POS_INC
        
       FROM
          WORK.WORK_AC_FINAL_KEEP  A
          LEFT JOIN
          WORK.WORK_AC_FINAL_KEEP_VAR_M0  A0
          ON A.NIF = A0.NIF
          LEFT JOIN
          WORK.WORK_AC_FINAL_KEEP_VAR_M1  A1
          ON A.NIF = A1.NIF
          LEFT JOIN
          WORK.WORK_AC_FINAL_KEEP_VAR_M2  A2
          ON A.NIF = A2.NIF
          LEFT JOIN
          WORK.WORK_AC_FINAL_KEEP_VAR_M3  A3
          ON A.NIF = A3.NIF;
          
set mapred.job.name = PAR_EXP_AC_FINAL_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_AC_FINAL_CLI;
 CREATE TABLE VF_POSTPAID.PAR_EXP_AC_FINAL_CLI AS
        SELECT
            NIF,
            MAX(NUM_EDAD) AS NUM_EDAD,
            MAX(IND_EXTRANJERO) AS IND_EXTRANJERO,
            MAX(NUM_ANTIG_LIN) AS NUM_ANTIG_LIN_MAX,
            MAX(IND_LINEA_VOZ) AS IND_LINEA_VOZ,
            MAX(IND_ADSL) AS IND_ADSL,
            MAX(IND_LPD) AS IND_LPD,
            MAX(IND_HZ) AS IND_HZ,
            MAX(IND_FIBRA) AS IND_FIBRA,
            MAX( IND_4G) AS IND_4G,
            MAX(IND_COBERTURA_4G) AS IND_COBERTURA_4G,
            MAX(IND_CONEXIONES_4G) AS IND_CONEXIONES_4G,
            MAX(IND_FACT_ELEC) AS IND_FACT_ELEC,
            MAX(IND_DEUDA) AS IND_DEUDA,
            MAX(IND_OS_IOS) AS IND_OS_IOS,
            MAX(IND_OS_ANDROID) AS IND_OS_ANDROID,
            MAX(IND_TERMINAL_4G) AS IND_TERMINAL_4G,
            MAX(IND_HUELLA_ONO) AS IND_HUELLA_ONO,
            MAX(IND_HUELLA_VF) AS IND_HUELLA_VF,
            MAX(NUM_LINEAS_PREP) AS NUM_LINEAS_PREP,
            MAX(NUM_LINEAS_POS) AS NUM_LINEAS_POS,
            SUM(NUM_ARPU_M0) AS NUM_ARPU_M0,
            MAX(NUM_LINEAS_VOZ) AS NUM_LINEAS_VOZ
        FROM
             VF_POSTPAID.PAR_EXP_AC_FINAL_LIN
        GROUP BY NIF;
      
DROP TABLE WORK.WORK_AC_FINAL_KEEP;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M0;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M1;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M2;
DROP TABLE WORK.WORK_AC_FINAL_KEEP_VAR_M3;


EXIT;