USE VF_POSTPAID;

             
set mapred.job.name = WORK_QUIMERA_M0;
DROP TABLE WORK.WORK_QUIMERA_M0;
CREATE TABLE WORK.WORK_QUIMERA_M0 AS
          SELECT
              MSISDN,
              SUM(CAST(FACT_SIN_SMS AS DOUBLE)) AS NUM_FACT_SIN_SMS,
              MAX(CAST(MESES_EN_COMPANIA AS DOUBLE)) AS NUM_MESES_EN_COMPANIA,
              SUM(CAST(LLAM_INTERNACIONAL AS DOUBLE)) AS NUM_LLAMADAS_INTER,
              SUM(CAST(FACT_INTERNACIONAL AS DOUBLE)) AS NUM_FACT_INTERNAC,
              SUM(CAST(NUM_ACCESSOS AS DOUBLE)) AS NUM_ACCESOS_3G,
              SUM(CAST(VOLUME3G AS DOUBLE)) AS NUM_DATOS_3G
           FROM input.vf_pos_TARIFICADOR WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}'
           GROUP BY MSISDN;
          
set mapred.job.name = WORK_QUIMERA_M1;
DROP TABLE WORK.WORK_QUIMERA_M1;
CREATE TABLE WORK.WORK_QUIMERA_M1 AS
          SELECT
              MSISDN,
              SUM(CAST(FACT_SIN_SMS AS DOUBLE)) AS NUM_FACT_SIN_SMS,
              MAX(CAST(MESES_EN_COMPANIA AS DOUBLE)) AS NUM_MESES_EN_COMPANIA,
              SUM(CAST(LLAM_INTERNACIONAL AS DOUBLE)) AS NUM_LLAMADAS_INTER,
              SUM(CAST(FACT_INTERNACIONAL AS DOUBLE)) AS NUM_FACT_INTERNAC,
              SUM(CAST(NUM_ACCESSOS AS DOUBLE)) AS NUM_ACCESOS_3G,
              SUM(CAST(VOLUME3G AS DOUBLE)) AS NUM_DATOS_3G
           FROM input.vf_pos_TARIFICADOR WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH1}'
           GROUP BY MSISDN;
          
set mapred.job.name = WORK_QUIMERA_M2;
DROP TABLE WORK.WORK_QUIMERA_M2;
CREATE TABLE WORK.WORK_QUIMERA_M2 AS
          SELECT
              MSISDN,
              SUM(CAST(FACT_SIN_SMS AS DOUBLE)) AS NUM_FACT_SIN_SMS,
              MAX(CAST(MESES_EN_COMPANIA AS DOUBLE)) AS NUM_MESES_EN_COMPANIA,
              SUM(CAST(LLAM_INTERNACIONAL AS DOUBLE)) AS NUM_LLAMADAS_INTER,
              SUM(CAST(FACT_INTERNACIONAL AS DOUBLE)) AS NUM_FACT_INTERNAC,
              SUM(CAST(NUM_ACCESSOS AS DOUBLE)) AS NUM_ACCESOS_3G,
              SUM(CAST(VOLUME3G AS DOUBLE)) AS NUM_DATOS_3G
           FROM input.vf_pos_TARIFICADOR WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH2}'
           GROUP BY MSISDN;
          
set mapred.job.name = WORK_QUIMERA_M3;
DROP TABLE WORK.WORK_QUIMERA_M3;
CREATE TABLE WORK.WORK_QUIMERA_M3 AS
          SELECT
              MSISDN,
              SUM(CAST(FACT_SIN_SMS AS DOUBLE)) AS NUM_FACT_SIN_SMS,
              MAX(CAST(MESES_EN_COMPANIA AS DOUBLE)) AS NUM_MESES_EN_COMPANIA,
              SUM(CAST(LLAM_INTERNACIONAL AS DOUBLE)) AS NUM_LLAMADAS_INTER,
              SUM(CAST(FACT_INTERNACIONAL AS DOUBLE)) AS NUM_FACT_INTERNAC,
              SUM(CAST(NUM_ACCESSOS AS DOUBLE)) AS NUM_ACCESOS_3G,
              SUM(CAST(VOLUME3G AS DOUBLE)) AS NUM_DATOS_3G
           FROM input.vf_pos_TARIFICADOR WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH3}'
         GROUP BY MSISDN;
          
set mapred.job.name = PAR_EXP_QUIMERA_LIN;
DROP TABLE VF_POSTPAID.PAR_EXP_QUIMERA_LIN;
CREATE TABLE VF_POSTPAID.PAR_EXP_QUIMERA_LIN AS
          SELECT
              M.X_ID_RED AS MSISDN,
              M.X_NUM_IDENT AS NIF,
              
              ROUND(A.NUM_FACT_SIN_SMS,0) AS NUM_FACT_SIN_SMS_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_FACT_SIN_SMS, B.NUM_FACT_SIN_SMS, C.NUM_FACT_SIN_SMS, D.NUM_FACT_SIN_SMS),0) AS NUM_FACT_SIN_SMS_AVG,
              ROUND(VF_FUNC.INC(A.NUM_FACT_SIN_SMS,A.NUM_FACT_SIN_SMS, B.NUM_FACT_SIN_SMS, C.NUM_FACT_SIN_SMS, D.NUM_FACT_SIN_SMS),2) AS NUM_FACT_SIN_SMS_INC,
              
              ROUND(A.NUM_MESES_EN_COMPANIA,0) AS NUM_MESES_EN_COMPANIA_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_MESES_EN_COMPANIA, B.NUM_MESES_EN_COMPANIA, C.NUM_MESES_EN_COMPANIA, D.NUM_MESES_EN_COMPANIA),0) AS NUM_MESES_EN_COMPANIA_AVG,
              ROUND(VF_FUNC.INCN(A.NUM_MESES_EN_COMPANIA, A.NUM_MESES_EN_COMPANIA, B.NUM_MESES_EN_COMPANIA, C.NUM_MESES_EN_COMPANIA, D.NUM_MESES_EN_COMPANIA),2) AS NUM_MESES_EN_COMPANIA_INC,
              
              ROUND(A.NUM_LLAMADAS_INTER,0) AS NUM_LLAMADAS_INTER_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_LLAMADAS_INTER, B.NUM_LLAMADAS_INTER, C.NUM_LLAMADAS_INTER, D.NUM_LLAMADAS_INTER),0) AS NUM_LLAMADAS_INTER_AVG,
              ROUND(VF_FUNC.INCN(A.NUM_LLAMADAS_INTER, B.NUM_LLAMADAS_INTER, C.NUM_LLAMADAS_INTER, D.NUM_LLAMADAS_INTER),2) AS NUM_LLAMADAS_INTER_INC,
              
              ROUND(A.NUM_FACT_INTERNAC,0) AS NUM_FACT_INTERNAC_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_FACT_INTERNAC, B.NUM_FACT_INTERNAC, C.NUM_FACT_INTERNAC, D.NUM_FACT_INTERNAC),0) AS NUM_FACT_INTERNAC_AVG,
              ROUND(VF_FUNC.INCN(A.NUM_FACT_INTERNAC, B.NUM_FACT_INTERNAC, C.NUM_FACT_INTERNAC, D.NUM_FACT_INTERNAC),2) NUM_FACT_INTERNAC_INC,
              
              ROUND(A.NUM_ACCESOS_3G,0) AS NUM_ACCESOS_3G_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_ACCESOS_3G, B.NUM_ACCESOS_3G, C.NUM_ACCESOS_3G, D.NUM_ACCESOS_3G),0) AS NUM_ACCESOS_3G_AVG,
              ROUND(VF_FUNC.INCN(A.NUM_ACCESOS_3G, B.NUM_ACCESOS_3G, C.NUM_ACCESOS_3G, D.NUM_ACCESOS_3G),2) AS NUM_ACCESOS_3G_INC,
              
              ROUND(A.NUM_DATOS_3G,0) AS NUM_DATOS_3G_M0,
              ROUND(VF_FUNC.AVGN(A.NUM_DATOS_3G, B.NUM_DATOS_3G, C.NUM_DATOS_3G, D.NUM_DATOS_3G),0) AS NUM_DATOS_3G_AVG,
              ROUND(VF_FUNC.INCN(A.NUM_DATOS_3G, B.NUM_DATOS_3G, C.NUM_DATOS_3G, D.NUM_DATOS_3G),2) AS NUM_DATOS_3G_INC
          FROM 
              (SELECT X_ID_RED, MAX(X_NUM_IDENT) AS X_NUM_IDENT  FROM input.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}' GROUP BY X_ID_RED) M
              LEFT JOIN
              WORK.WORK_QUIMERA_M0 A
              ON M.X_ID_RED = A.MSISDN
              LEFT JOIN
              WORK.WORK_QUIMERA_M1 B
              ON M.X_ID_RED = B.MSISDN
              LEFT JOIN
              WORK.WORK_QUIMERA_M2 C
              ON M.X_ID_RED = C.MSISDN
              LEFT JOIN
              WORK.WORK_QUIMERA_M3 D
              ON M.X_ID_RED = D.MSISDN;
              
set mapred.job.name = PAR_EXP_QUIMERA_CLI;
DROP TABLE VF_POSTPAID.PAR_EXP_QUIMERA_CLI;
CREATE TABLE VF_POSTPAID.PAR_EXP_QUIMERA_CLI AS
        SELECT
            NIF,
            
            SUM(NUM_FACT_SIN_SMS_M0) AS NUM_FACT_SIN_SMS_M0,
            SUM(NUM_MESES_EN_COMPANIA_M0) AS NUM_MESES_EN_COMPANIA_M0,
            SUM(NUM_LLAMADAS_INTER_M0) AS NUM_LLAMADAS_INTER_M0,            
            SUM(NUM_FACT_INTERNAC_M0) AS NUM_FACT_INTERNAC_M0,            
            SUM(NUM_ACCESOS_3G_M0) AS NUM_ACCESOS_3G_M0,            
            SUM(NUM_DATOS_3G_M0) AS NUM_DATOS_3G_M0
        FROM 
            VF_POSTPAID.PAR_EXP_QUIMERA_LIN 
        GROUP BY NIF;
        
DROP TABLE WORK.WORK_QUIMERA_M0;
DROP TABLE WORK.WORK_QUIMERA_M1;
DROP TABLE WORK.WORK_QUIMERA_M2;
DROP TABLE WORK.WORK_QUIMERA_M3;    
    
EXIT;
