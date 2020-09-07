USE VF_POSTPAID;
      
set mapred.job.name = WORK_USAC_TABLA_AUX;
DROP TABLE WORK.WORK_USAC_TABLA_AUX;

CREATE TABLE WORK.WORK_USAC_TABLA_AUX AS
     
      SELECT
                            USAC_CHOR.MSISDN,
                            USAC_CHOR.DIRECTION,
                            USAC_CHOR.TYPE_TD,
                            USAC_CHOR.X_WORKGROUP,
                            USAC_CHOR.X_VIA_ENTRADA,
                            USAC_CHOR.RESULT_TD,
                            USAC_CHOR.REASON_1,
                            USAC_CHOR.REASON_2,
                            USAC_CHOR.REASON_3,
                            USAC_CHOR.CONTEO,
                            USAC_CHOR.CREATE_DATE,
                            USAC_CHOR.X_SEGMENTACION,
                            USAC_CHOR.X_NIVEL_SERVICIO,
                            USAC_CHOR.X_NUM_CUENTA,
                            1 AS IND_LLAMADA,
                            0 AS IND_LLAMADA_VALIDA,                            
                            UPPER(REGEXP_REPLACE(USAC_CHOR.REASON_1,' ',''))  AS REASON_11 ,
                            UPPER(REGEXP_REPLACE(USAC_CHOR.REASON_2,' ' ,''))  AS REASON_21,              
                            UPPER(REGEXP_REPLACE(USAC_CHOR.REASON_3,' ' ,''))  AS REASON_31,
                            UPPER(REGEXP_REPLACE(USAC_CHOR.RESULT_TD,' ' ,''))  AS RESULT_TD1      
      FROM    (SELECT
                                        SERIAL_NO AS MSISDN,
                                        DIRECTION,
                                        TYPE_TD ,
                                        X_WORKGROUP,
                                        X_VIA_ENTRADA,
                                        RESULT_TD ,
                                        REASON_1 ,
                                        REASON_2 ,
                                        REASON_3,
                                        CONTEO,
                                        CREATE_DATE ,
                                        X_SEGMENTACION  ,
                                        X_NIVEL_SERVICIO,
                                        X_NUM_CUENTA 
                   FROM INPUT.VF_USAC_CHORDIANT) USAC_CHOR;
                  
                  
                          
set mapred.job.name = WORK_USAC_TABLA;
DROP TABLE WORK.WORK_USAC_TABLA;
CREATE TABLE  WORK.WORK_USAC_TABLA AS
        
                    SELECT 
                              A.MSISDN     ,
                              A.DIRECTION  ,
                              A.TYPE_TD  ,
                              A.X_WORKGROUP  ,
                              A.X_VIA_ENTRADA ,
                              A.RESULT_TD,
                              A.REASON_1   ,
                              A.REASON_2 ,
                              A.REASON_3,
                              A.CONTEO  ,
                              A.CREATE_DATE ,
                              A.X_SEGMENTACION  ,
                              A.X_NIVEL_SERVICIO ,
                              A.X_NUM_CUENTA ,
                              A.IND_LLAMADA,
                              A.IND_LLAMADA_VALIDA ,
                              A.REASON_11,
                              A.REASON_21 ,
                              A.REASON_31,
                              A.RESULT_TD1,
                              A.AGRUPAC_TIPIS_TOTAL,
                              A.TIPO_INCIDENCIA,
                              B.SEG_USAC_VODAFONE
                  
                FROM (            SELECT 
                                      A.MSISDN     ,
                                      A.DIRECTION  ,
                                      A.TYPE_TD  ,
                                      A.X_WORKGROUP  ,
                                      A.X_VIA_ENTRADA ,
                                      A.RESULT_TD,
                                      A.REASON_1   ,
                                      A.REASON_2 ,
                                      A.REASON_3,
                                      A.CONTEO  ,
                                      A.CREATE_DATE ,
                                      A.X_SEGMENTACION  ,
                                      A.X_NIVEL_SERVICIO ,
                                      A.X_NUM_CUENTA ,
                                      A.IND_LLAMADA,
                                      A.IND_LLAMADA_VALIDA ,
                                      A.REASON_11,
                                      A.REASON_21 ,
                                      A.REASON_31,
                                      A.RESULT_TD1,
                                      A.AGRUPAC_TIPIS_TOTAL,
                                      B.TIPO_INCIDENCIA
                          
                                    FROM (            SELECT 
                                                                  A.MSISDN     ,
                                                                  A.DIRECTION  ,
                                                                  A.TYPE_TD  ,
                                                                  A.X_WORKGROUP  ,
                                                                  A.X_VIA_ENTRADA ,
                                                                  A.RESULT_TD,
                                                                  A.REASON_1   ,
                                                                  A.REASON_2 ,
                                                                  A.REASON_3,
                                                                  A.CONTEO  ,
                                                                  A.CREATE_DATE ,
                                                                  A.X_SEGMENTACION  ,
                                                                  A.X_NIVEL_SERVICIO ,
                                                                  A.X_NUM_CUENTA ,
                                                                  A.IND_LLAMADA,
                                                                  A.IND_LLAMADA_VALIDA ,
                                                                  A.REASON_11,
                                                                  A.REASON_21 ,
                                                                  A.REASON_31,
                                                                  A.RESULT_TD1,
                                                                  B.AGRUPAC_TIPIS_TOTAL
                                              
                                    FROM WORK.WORK_USAC_TABLA_AUX A 
                                     LEFT JOIN (SELECT
                                                              CT_TP_Q3.REASON_11 ,
                                                              CT_TP_Q3.REASON_21 ,
                                                              CT_TP_Q3.REASON_31,
                                                              CT_TP_Q3.RESULT_TD1,
                                                              MAX(CT_TP_Q3.AGRUPAC_TIPIS_TOTAL) AS AGRUPAC_TIPIS_TOTAL
                                                          FROM (
                                                            SELECT
                                                              REASON_1 ,
                                                              REASON_2 ,
                                                              REASON_3,
                                                              RESULT_TD,
                                                              UPPER(REGEXP_REPLACE(REASON_1,' ',''))  AS REASON_11,
                                                              UPPER(REGEXP_REPLACE(REASON_2,' ',''))  AS REASON_21,              
                                                              UPPER(REGEXP_REPLACE(REASON_3,' ',''))  AS REASON_31,
                                                              UPPER(REGEXP_REPLACE(RESULT_TD,' ',''))  AS RESULT_TD1,
                                                              AGRUPAC_TIPIS_TOTAL
                                                            FROM MASTER.VF_POS_USAC_CAT_TIPIS_Q3) CT_TP_Q3
                                                            GROUP BY CT_TP_Q3.REASON_11,CT_TP_Q3.REASON_21,CT_TP_Q3.REASON_31,CT_TP_Q3.RESULT_TD1
                                                        ) B 
                                    ON A.REASON_11=B.REASON_11 AND B.REASON_21=A.REASON_21 AND B.REASON_31=A.REASON_31 AND B.RESULT_TD1=A.RESULT_TD1
        
                        ) A 
                        LEFT JOIN (
                                            SELECT
                                                  REASON_1 ,
                                                  TIPO_INCIDENCIA,
                                                  UPPER(REGEXP_REPLACE(REASON_1,' ' ,''))  AS REASON_11
        
                                    FROM MASTER.VF_POS_USAC_CAT_REASON1   
                        )B 
                        ON A.REASON_11=B.REASON_11 
        
        
        ) A 
        LEFT JOIN (
             SELECT
                                  CT_VF.REASON_11,
                                  CT_VF.REASON_21,
                                  CT_VF.REASON_31,
                                  CT_VF.RESULT_TD1,
                                  MAX(CT_VF.SEG_USAC_VODAFONE) AS SEG_USAC_VODAFONE
                    FROM  (
                        SELECT 
                                  REASON_1 ,
                                  REASON_2 ,
                                  REASON_3,
                                  RESULT_TD,
                                  UPPER(REGEXP_REPLACE(REASON_1,' ' ,''))  AS REASON_11,
                                  UPPER(REGEXP_REPLACE(REASON_2,' ' ,''))  AS REASON_21,              
                                  UPPER(REGEXP_REPLACE(REASON_3,' ' ,''))  AS REASON_31,
                                  UPPER(REGEXP_REPLACE(RESULT_TD,' ' ,''))  AS RESULT_TD1,
                                  SEG_USAC_VODAFONE
                        FROM MASTER.VF_POS_USAC_CAT_VODAFONE
                        ) CT_VF
                        GROUP BY CT_VF.REASON_11,CT_VF.REASON_21,CT_VF.REASON_31,CT_VF.RESULT_TD1
        ) B 
        ON A.REASON_11=B.REASON_11 AND B.REASON_21=A.REASON_21 AND B.REASON_31=A.REASON_31 AND B.RESULT_TD1=A.RESULT_TD1;
                
            
set mapred.job.name = WORK_USAC_TABLA_AUX2;
DROP TABLE WORK.WORK_USAC_TABLA_AUX2;
CREATE TABLE WORK.WORK_USAC_TABLA_AUX2 AS
      
      SELECT
                            MSISDN     ,
                            DIRECTION  ,
                            TYPE_TD  ,
                            X_WORKGROUP  ,
                            X_VIA_ENTRADA ,
                            RESULT_TD,
                            REASON_1   ,
                            REASON_2 ,
                            REASON_3,
                            CONTEO  ,
                            CREATE_DATE ,
                            X_SEGMENTACION  ,
                            X_NIVEL_SERVICIO ,
                            X_NUM_CUENTA ,
                            IND_LLAMADA,
                            SEG_USAC_VODAFONE,
                              (CASE   WHEN     (REASON_1 IS NOT NULL) AND (REGEXP_REPLACE(REASON_1,' ' ,'') <> '...' ) AND
                                         (INSTR(UPPER(REASON_1),'SIM')=0 AND INSTR(UPPER(REASON_1),'PIN')=0 AND INSTR(UPPER(REASON_1),'PUK')=0 OR INSTR(UPPER(REASON_1),'SIMLOCK')>0) AND
                                         ((REASON_2 IS NOT NULL )  AND (REGEXP_REPLACE(REASON_2,' ' ,'') <> '...' )) AND
                                         (INSTR(UPPER(REASON_2),'SIM')=0 AND INSTR(UPPER(REASON_2),'PIN')=0 AND INSTR(UPPER(REASON_2),'PUK')=0 OR INSTR(UPPER(REASON_2),'SIMLOCK')>0) AND
                                         (INSTR(UPPER(REASON_2),'BROMA')=0 AND INSTR(UPPER(REASON_2),'CORTA')=0 AND INSTR(UPPER(REASON_2),'CUELGA')=0) AND
                                         (INSTR(UPPER(REASON_2),'ERROR')=0 OR INSTR(UPPER(REASON_2),'CONSULTA')=0) AND
                                         (INSTR(UPPER(REASON_2),'ERROR')=0 OR INSTR(UPPER(REASON_2),'MARCA')=0) AND
                                         ((REASON_3 IS NOT NULL ) AND  (REGEXP_REPLACE(REASON_3,' ' ,'') <> '...' )  AND  (REGEXP_REPLACE(REASON_3,' ' ,'') <> '*' ) ) AND
                                         (INSTR(UPPER(REASON_3),'BROMA')=0 AND INSTR(UPPER(REASON_3),'CORTA')=0 AND INSTR(UPPER(REASON_3),'CUELGA')=0) AND
                                         (INSTR(UPPER(REASON_3),'SIM')=0 AND INSTR(UPPER(REASON_3),'PIN')=0 AND INSTR(UPPER(REASON_3),'PUK')=0 OR INSTR(UPPER(REASON_3),'SIMLOCK')>0 OR INSTR(UPPER(REASON_3),'SIMPLE')>0) AND
                                         (INSTR(UPPER(REASON_3),'ERROR')=0 OR INSTR(UPPER(REASON_3),'MARCA')=0) AND
                                         ((RESULT_TD IS NOT  NULL )  AND  (REGEXP_REPLACE(RESULT_TD,' ' ,'') <> '...' ))  AND
                                         (INSTR(UPPER(RESULT_TD),'BROMA')=0 AND INSTR(UPPER(RESULT_TD),'CORTA')=0 AND INSTR(UPPER(RESULT_TD),'CUELGA')=0) AND
                                         (X_WORKGROUP IS NOT NULL ) AND
                                         (REGEXP_REPLACE(X_VIA_ENTRADA,' ' ,'') <> '...' ) AND
                                         (UPPER(NVL(AGRUPAC_TIPIS_TOTAL,' ')) <> 'F' AND
                                          UPPER( NVL(AGRUPAC_TIPIS_TOTAL,' ')) <>  'FF' AND
                                          UPPER(NVL(AGRUPAC_TIPIS_TOTAL,' ')) <> 'NO LLAMADA' AND
                                          UPPER(NVL(AGRUPAC_TIPIS_TOTAL,' ')) <> 'SALIENTES')
                                          THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_LLAMADA_VALIDA,                                            
                            REASON_11,
                            REASON_21 ,
                            REASON_31,
                            RESULT_TD1
      
      FROM WORK.WORK_USAC_TABLA;
              
      
      
set mapred.job.name = WORK_USAC_TABLA_VARIABLES;
DROP TABLE WORK.WORK_USAC_TABLA_VARIABLES;

      CREATE TABLE WORK.WORK_USAC_TABLA_VARIABLES AS
      
      SELECT
      
                            MSISDN     ,
                            DIRECTION  ,
                            TYPE_TD  ,
                            X_WORKGROUP  ,
                            X_VIA_ENTRADA ,
                            SEG_USAC_VODAFONE,
                            RESULT_TD,
                            REASON_1   ,
                            REASON_2 ,
                            REASON_3,
                            CONTEO  ,
                            CREATE_DATE ,
                            X_SEGMENTACION  ,
                            X_NIVEL_SERVICIO ,
                            X_NUM_CUENTA ,
                            IND_LLAMADA,
                            IND_LLAMADA_VALIDA,                      
                            REASON_11,
                            REASON_21 ,
                            REASON_31,
                            RESULT_TD1,                    
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((( INSTR(UPPER(REASON_11),'BAJA'))>0)   OR (( INSTR(UPPER(REASON_11),'PORTABI'))>0)) OR
                                          ((( INSTR(UPPER(REASON_21),'BAJA'))>0)   OR (( INSTR(UPPER(REASON_21),'PORTABI'))>0)) OR
                                          ((( INSTR(UPPER(REASON_31),'BAJA'))>0)   OR (( INSTR(UPPER(REASON_31),'PORTABI'))>0)) OR
                                          ((( INSTR(UPPER(RESULT_TD1),'BAJA'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'PORTABI'))>0)))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_PORTABILIDAD,  
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'VFCASA'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'VFCASA'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'VFCASA'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'VFCASA'))>0))   
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_VFCASA,
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((( INSTR(UPPER(REASON_11),'ACEPT'))>0)   AND (( INSTR(UPPER(REASON_11),'NOACEPT'))=0)) OR ((( INSTR(UPPER(REASON_11),'RECUPE'))>0)   AND (( INSTR(UPPER(REASON_11),'NORECUPE'))=0)) OR
                                          (( INSTR(UPPER(REASON_11),'COMPLET'))>0)   OR (( INSTR(UPPER(REASON_11),'SOLUCIO'))>0)   OR
                                          
                                          ((( INSTR(UPPER(REASON_21),'ACEPT'))>0)   AND (( INSTR(UPPER(REASON_21),'NOACEPT'))=0)) OR ((( INSTR(UPPER(REASON_21),'RECUPE'))>0)   AND (( INSTR(UPPER(REASON_21),'NORECUPE'))=0)) OR
                                          (( INSTR(UPPER(REASON_21),'COMPLET'))>0)   OR (( INSTR(UPPER(REASON_21),'SOLUCIO'))>0)   OR       
                                                                       
                                          ((( INSTR(UPPER(REASON_31),'ACEPT'))>0)   AND (( INSTR(UPPER(REASON_31),'NOACEPT'))=0)) OR ((( INSTR(UPPER(REASON_31),'RECUPE'))>0)   AND (( INSTR(UPPER(REASON_31),'NORECUPE'))=0)) OR
                                          (( INSTR(UPPER(REASON_31),'COMPLET'))>0)   OR (( INSTR(UPPER(REASON_31),'SOLUCIO'))>0)   OR    
                                          
                                          ((( INSTR(UPPER(RESULT_TD1),'ACEPT'))>0)   AND (( INSTR(UPPER(RESULT_TD1),'NOACEPT'))=0)) OR ((( INSTR(UPPER(RESULT_TD1),'RECUPE'))>0)   AND (( INSTR(UPPER(RESULT_TD1),'NORECUPE'))=0)) OR
                                          (( INSTR(UPPER(RESULT_TD1),'COMPLET'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'SOLUCIO'))>0))                                                                     
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_OK,                                      
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'NORECUPE'))>0)   OR (( INSTR(UPPER(REASON_11),'NOACEPT'))>0) OR (( INSTR(UPPER(REASON_11),'COBERTURA'))>0)   OR (( INSTR(UPPER(REASON_11),'RECLAM'))>0) OR
                                          (( INSTR(UPPER(REASON_11),'INCID'))>0)   OR (( INSTR(UPPER(REASON_11),'QUEJA'))>0)   OR   (( INSTR(UPPER(REASON_11),'INSATISFEC'))>0)   OR
                                          
                                          (( INSTR(UPPER(REASON_21),'NORECUPE'))>0)   OR (( INSTR(UPPER(REASON_21),'NOACEPT'))>0) OR (( INSTR(UPPER(REASON_21),'COBERTURA'))>0)   OR (( INSTR(UPPER(REASON_21),'RECLAM'))>0) OR
                                          (( INSTR(UPPER(REASON_21),'INCID'))>0)   OR (( INSTR(UPPER(REASON_21),'QUEJA'))>0)   OR   (( INSTR(UPPER(REASON_21),'INSATISFEC'))>0)   OR      
                                                                       
                                          (( INSTR(UPPER(REASON_31),'NORECUPE'))>0)   OR (( INSTR(UPPER(REASON_31),'NOACEPT'))>0) OR (( INSTR(UPPER(REASON_31),'COBERTURA'))>0)   OR (( INSTR(UPPER(REASON_31),'RECLAM'))>0) OR
                                          (( INSTR(UPPER(REASON_31),'INCID'))>0)   OR (( INSTR(UPPER(REASON_31),'QUEJA'))>0)   OR   (( INSTR(UPPER(REASON_31),'INSATISFEC'))>0)   OR    
                                          
                                          (( INSTR(UPPER(RESULT_TD1),'NORECUPE'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'NOACEPT'))>0) OR (( INSTR(UPPER(RESULT_TD1),'COBERTURA'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'RECLAM'))>0) OR
                                          (( INSTR(UPPER(RESULT_TD1),'INCID'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'QUEJA'))>0)   OR   (( INSTR(UPPER(RESULT_TD1),'INSATISFEC'))>0))                                                          
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_KO,                           
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((( INSTR(UPPER(REASON_11),'PRODUCT'))>0)   OR (( INSTR(UPPER(REASON_11),'SERVIC'))>0) AND (( INSTR(UPPER(REASON_11),'PORTABI'))=0)) OR
                                          ((( INSTR(UPPER(REASON_21),'PRODUCT'))>0)   OR (( INSTR(UPPER(REASON_21),'SERVIC'))>0) AND (( INSTR(UPPER(REASON_21),'PORTABI'))=0)) OR
                                          ((( INSTR(UPPER(REASON_31),'PRODUCT'))>0)   OR (( INSTR(UPPER(REASON_31),'SERVIC'))>0) AND (( INSTR(UPPER(REASON_31),'PORTABI'))=0)) OR
                                          ((( INSTR(UPPER(RESULT_TD1),'PRODUCT'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'SERVIC'))>0) AND (( INSTR(UPPER(RESULT_TD1),'PORTABI'))=0)))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_PROD_SERV,  
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'TARIF'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'TARIF'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'TARIF'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'TARIF'))>0))   
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_TARIFAS,                        
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((( INSTR(UPPER(REASON_11),'PAGO'))>0)   OR (( INSTR(UPPER(REASON_11),'FACTUR'))>0) OR (( INSTR(UPPER(REASON_11),'ABONO'))>0)   OR (( INSTR(UPPER(REASON_11),'RECARG'))>0)) AND
                                          ((( INSTR(UPPER(REASON_11),'MODE'))=0)   AND (( INSTR(UPPER(REASON_11),'PUNTO'))=0)   AND   (( INSTR(UPPER(REASON_11),'NOACEPT'))=0)   AND  (( INSTR(UPPER(REASON_11),'NOABONO'))=0))   OR 
                                          
                                          ((( INSTR(UPPER(REASON_21),'PAGO'))>0)   OR (( INSTR(UPPER(REASON_21),'FACTUR'))>0) OR (( INSTR(UPPER(REASON_21),'ABONO'))>0)   OR (( INSTR(UPPER(REASON_21),'RECARG'))>0)) AND
                                          ((( INSTR(UPPER(REASON_21),'MODE'))=0)   AND (( INSTR(UPPER(REASON_21),'PUNTO'))=0)   AND   (( INSTR(UPPER(REASON_21),'NOACEPT'))=0)   AND  (( INSTR(UPPER(REASON_21),'NOABONO'))=0))   OR     
                                                                       
                                          ((( INSTR(UPPER(REASON_31),'PAGO'))>0)   OR (( INSTR(UPPER(REASON_31),'FACTUR'))>0) OR (( INSTR(UPPER(REASON_31),'ABONO'))>0)   OR (( INSTR(UPPER(REASON_31),'RECARG'))>0)) AND
                                          ((( INSTR(UPPER(REASON_31),'MODE'))=0)   AND (( INSTR(UPPER(REASON_31),'PUNTO'))=0)   AND   (( INSTR(UPPER(REASON_31),'NOACEPT'))=0)   AND  (( INSTR(UPPER(REASON_31),'NOABONO'))=0))   OR
                                          
                                          ((( INSTR(UPPER(RESULT_TD1),'PAGO'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'FACTUR'))>0) OR (( INSTR(UPPER(RESULT_TD1),'ABONO'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'RECARG'))>0)) AND
                                          ((( INSTR(UPPER(RESULT_TD1),'MODE'))=0)   AND (( INSTR(UPPER(RESULT_TD1),'PUNTO'))=0)   AND   (( INSTR(UPPER(RESULT_TD1),'NOACEPT'))=0)   AND  (( INSTR(UPPER(RESULT_TD1),'NOABONO'))=0)))                                                      
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_PAGOS,                        
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'DEUDA'))>0)   OR (( INSTR(UPPER(REASON_11),'DEVOL'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'DEUDA'))>0)   OR (( INSTR(UPPER(REASON_21),'DEVOL'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'DEUDA'))>0)   OR (( INSTR(UPPER(REASON_31),'DEVOL'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'DEUDA'))>0) OR   (( INSTR(UPPER(RESULT_TD1),'DEVOL'))>0))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_DEUDA,      
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'DSL'))>0)   OR (( INSTR(UPPER(REASON_11),'BROOKLYN'))>0)   OR 
                                          (( INSTR(UPPER(REASON_11),'MANHATTAN'))>0)   OR (( INSTR(UPPER(REASON_11),'MODE'))>0)   OR 
                                          (( INSTR(UPPER(REASON_11),'INTERNET'))>0)   OR (( INSTR(UPPER(REASON_11),'NAVEGA'))>0)   OR 
                                          (( INSTR(UPPER(REASON_11),'ADLOPENSAR'))>0) OR   (( INSTR(UPPER(REASON_11),'AILOPENSAR'))>0)  OR (( INSTR(UPPER(REASON_11),'LPD'))>0)  OR
                                          
                                          (( INSTR(UPPER(REASON_21),'DSL'))>0)   OR (( INSTR(UPPER(REASON_21),'BROOKLYN'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'MANHATTAN'))>0)   OR (( INSTR(UPPER(REASON_21),'MODE'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'INTERNET'))>0)   OR (( INSTR(UPPER(REASON_21),'NAVEGA'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'ADLOPENSAR'))>0) OR   (( INSTR(UPPER(REASON_21),'AILOPENSAR'))>0)  OR (( INSTR(UPPER(REASON_21),'LPD'))>0)  OR
                                          
                                          (( INSTR(UPPER(REASON_31),'DSL'))>0)   OR (( INSTR(UPPER(REASON_31),'BROOKLYN'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'MANHATTAN'))>0)   OR (( INSTR(UPPER(REASON_31),'MODE'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'INTERNET'))>0)   OR (( INSTR(UPPER(REASON_31),'NAVEGA'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'ADLOPENSAR'))>0) OR   (( INSTR(UPPER(REASON_31),'AILOPENSAR'))>0)  OR (( INSTR(UPPER(REASON_31),'LPD'))>0)  OR
                                          
                                          (( INSTR(UPPER(RESULT_TD1),'DSL'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'BROOKLYN'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'MANHATTAN'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'MODE'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'INTERNET'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'NAVEGA'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'ADLOPENSAR'))>0) OR   (( INSTR(UPPER(RESULT_TD1),'AILOPENSAR'))>0)  OR (( INSTR(UPPER(RESULT_TD1),'LPD'))>0))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_ADSL,           
                              
       (CASE 
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(NVL(REASON_11,' ')),'PUNTO'))>0)   OR (( INSTR(UPPER(REASON_11),'TERMINAL'))>0)   OR (( INSTR(UPPER(REASON_11),'BLACKBERRY'))>0)  OR (( INSTR(UPPER(REASON_11),'IPHONE'))>0) OR
                                          (( INSTR(UPPER(REASON_11),'NOKIA'))>0)   OR (( INSTR(UPPER(REASON_11),'SONY'))>0)   OR (( INSTR(UPPER(REASON_11),'ERICSSON'))>0)  OR (( INSTR(UPPER(REASON_11),'SAMSUNG'))>0)  OR
                                          (( INSTR(UPPER(REASON_11),'HTC'))>0)   OR (( INSTR(UPPER(REASON_11),'LG'))>0)   AND (( INSTR(UPPER(REASON_11),'LGA'))=0)  OR (( INSTR(UPPER(REASON_11),'MOTOROLA'))>0)  OR
                                          (( INSTR(UPPER(REASON_11),'SHARP'))>0) OR   (( INSTR(UPPER(REASON_11),'PROMO'))>0)  OR (( INSTR(UPPER(REASON_11),'BONO'))>0)  OR (( INSTR(UPPER(REASON_11),'OFER'))>0)  OR
                                          (( INSTR(UPPER(REASON_11),'DESCUENTO'))>0) OR   (( INSTR(UPPER(REASON_11),'DTO'))>0)  AND ((( INSTR(UPPER(REASON_11),'ABONO'))=0)  AND (( INSTR(UPPER(REASON_11),'RECUPE'))=0)  AND
                                          (( INSTR(UPPER(REASON_11),'ADSL'))=0) AND   (( INSTR(UPPER(REASON_11),'PORTABI'))=0)  AND (( INSTR(UPPER(REASON_11),'TARIFA'))=0)  AND (( INSTR(UPPER(REASON_11),'NOACEP'))=0))  OR 
                                         
                                          (( INSTR(UPPER(REASON_21),'PUNTO'))>0)   OR (( INSTR(UPPER(REASON_21),'TERMINAL'))>0)   OR (( INSTR(UPPER(REASON_21),'BLACKBERRY'))>0)  OR(( INSTR(UPPER(REASON_21),'IPHONE'))>0)  OR
                                          (( INSTR(UPPER(REASON_21),'NOKIA'))>0)   OR (( INSTR(UPPER(REASON_21),'SONY'))>0)   OR (( INSTR(UPPER(REASON_21),'ERICSSON'))>0)  OR (( INSTR(UPPER(REASON_21),'SAMSUNG'))>0)  OR
                                          (( INSTR(UPPER(REASON_21),'HTC'))>0)   OR (( INSTR(UPPER(REASON_21),'LG'))>0)   AND (( INSTR(UPPER(REASON_21),'LGA'))=0)  OR (( INSTR(UPPER(REASON_21),'MOTOROLA'))>0)  OR
                                          (( INSTR(UPPER(REASON_21),'SHARP'))>0) OR   (( INSTR(UPPER(REASON_21),'PROMO'))>0)  OR (( INSTR(UPPER(REASON_21),'BONO'))>0)  OR (( INSTR(UPPER(REASON_21),'OFER'))>0)  OR
                                          (( INSTR(UPPER(REASON_21),'DESCUENTO'))>0) OR   (( INSTR(UPPER(REASON_21),'DTO'))>0)  AND ((( INSTR(UPPER(REASON_21),'ABONO'))=0)  AND (( INSTR(UPPER(REASON_21),'RECUPE'))=0)  AND
                                          (( INSTR(UPPER(REASON_21),'ADSL'))=0) AND   (( INSTR(UPPER(REASON_21),'PORTABI'))=0)  AND (( INSTR(UPPER(REASON_21),'TARIFA'))=0)  AND (( INSTR(UPPER(REASON_21),'NOACEP'))=0))  OR
                                         
                                          (( INSTR(UPPER(REASON_31),'PUNTO'))>0)   OR (( INSTR(UPPER(REASON_31),'TERMINAL'))>0)   OR (( INSTR(UPPER(REASON_31),'BLACKBERRY'))>0)  OR(( INSTR(UPPER(REASON_31),'IPHONE'))>0)  OR
                                          (( INSTR(UPPER(REASON_31),'NOKIA'))>0)   OR (( INSTR(UPPER(REASON_31),'SONY'))>0)   OR (( INSTR(UPPER(REASON_31),'ERICSSON'))>0)  OR (( INSTR(UPPER(REASON_31),'SAMSUNG'))>0)  OR
                                          (( INSTR(UPPER(REASON_31),'HTC'))>0)   OR (( INSTR(UPPER(REASON_31),'LG'))>0)   AND (( INSTR(UPPER(REASON_31),'LGA'))=0)  OR (( INSTR(UPPER(REASON_31),'MOTOROLA'))>0)  OR
                                          (( INSTR(UPPER(REASON_31),'SHARP'))>0) OR   (( INSTR(UPPER(REASON_31),'PROMO'))>0)  OR (( INSTR(UPPER(REASON_31),'BONO'))>0)  OR (( INSTR(UPPER(REASON_31),'OFER'))>0)  OR
                                          (( INSTR(UPPER(REASON_31),'DESCUENTO'))>0) OR   (( INSTR(UPPER(REASON_31),'DTO'))>0)  AND ((( INSTR(UPPER(REASON_31),'ABONO'))=0)  AND (( INSTR(UPPER(REASON_31),'RECUPE'))=0)  AND
                                          (( INSTR(UPPER(REASON_31),'ADSL'))=0) AND (( INSTR(UPPER(REASON_31),'PORTABI'))=0)  AND (( INSTR(UPPER(REASON_31),'TARIFA'))=0)  AND (( INSTR(UPPER(REASON_31),'NOACEP'))=0))                                    OR
                                          
                                           (( INSTR(UPPER(RESULT_TD1),'PUNTO'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'TERMINAL'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'BLACKBERRY'))>0)  OR(( INSTR(UPPER(RESULT_TD1),'IPHONE'))>0)  OR
                                          (( INSTR(UPPER(RESULT_TD1),'NOKIA'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'SONY'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'ERICSSON'))>0)  OR (( INSTR(UPPER(RESULT_TD1),'SAMSUNG'))>0)  OR
                                          (( INSTR(UPPER(RESULT_TD1),'HTC'))>0)   OR (( INSTR(UPPER(RESULT_TD1),'LG'))>0)   AND (( INSTR(UPPER(RESULT_TD1),'LGA'))=0)  OR (( INSTR(UPPER(RESULT_TD1),'MOTOROLA'))>0)  OR
                                          (( INSTR(UPPER(RESULT_TD1),'SHARP'))>0) OR   (( INSTR(UPPER(RESULT_TD1),'PROMO'))>0)  OR (( INSTR(UPPER(RESULT_TD1),'BONO'))>0)  OR (( INSTR(UPPER(RESULT_TD1),'OFER'))>0)  OR
                                          (( INSTR(UPPER(RESULT_TD1),'DESCUENTO'))>0) OR   (( INSTR(UPPER(RESULT_TD1),'DTO'))>0)  AND ((( INSTR(UPPER(RESULT_TD1),'ABONO'))=0)  AND (( INSTR(UPPER(RESULT_TD1),'RECUPE'))=0)  AND
                                          (( INSTR(UPPER(RESULT_TD1),'ADSL'))=0) AND   (( INSTR(UPPER(RESULT_TD1),'PORTABI'))=0)  AND (( INSTR(UPPER(RESULT_TD1),'TARIFA'))=0)  AND (( INSTR(UPPER(RESULT_TD1),'NOACEP'))=0)))
                                    
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_PROMOCIONES,                                         
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((( INSTR(UPPER(REASON_11),'PENDIENTE'))>0)   OR 
                                          (( INSTR(UPPER(REASON_21),'PENDIENTE'))>0)   OR 
                                          (( INSTR(UPPER(REASON_31),'PENDIENTE'))>0)   OR 
                                          (( INSTR(UPPER(RESULT_TD1),'PENDIENTE'))>0))   
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_PENDIENTE,  
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((INSTR(UPPER(REASON_11),'ALTA')>0 OR INSTR(UPPER(REASON_11),'ACTIVACION')>0 OR INSTR(UPPER(REASON_11),'PEDIDO')>0 OR INSTR(UPPER(REASON_11),'INSTAL')>0
                                           OR INSTR(UPPER(REASON_11),'INSTAL')>0) AND INSTR(UPPER(REASON_11),'NONUEVO')=0) OR
                                           
                                           ((INSTR(UPPER(REASON_21),'ALTA')>0 OR INSTR(UPPER(REASON_21),'ACTIVACION')>0 OR INSTR(UPPER(REASON_21),'PEDIDO')>0 OR INSTR(UPPER(REASON_21),'INSTAL')>0
                                           OR INSTR(UPPER(REASON_21),'INSTAL')>0) AND INSTR(UPPER(REASON_21),'NONUEVO')=0) OR
                                           
                                           ((INSTR(UPPER(REASON_31),'ALTA')>0 OR INSTR(UPPER(REASON_31),'ACTIVACION')>0 OR INSTR(UPPER(REASON_31),'PEDIDO')>0 OR INSTR(UPPER(REASON_31),'INSTAL')>0
                                           OR INSTR(UPPER(REASON_31),'INSTAL')>0) AND INSTR(UPPER(REASON_31),'NONUEVO')=0) OR
                                           
                                           ((INSTR(UPPER(RESULT_TD1),'ALTA')>0 OR INSTR(UPPER(RESULT_TD1),'ACTIVACION')>0 OR INSTR(UPPER(RESULT_TD1),'PEDIDO')>0 OR INSTR(UPPER(RESULT_TD1),'INSTAL')>0
                                           OR INSTR(UPPER(RESULT_TD1),'INSTAL')>0) AND INSTR(UPPER(RESULT_TD1),'NONUEVO')=0))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_ALTA,  
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ((INSTR(UPPER(REASON_11),'CANCELACION')>0 AND INSTR(UPPER(REASON_11),'NO')=0) OR
                                           (INSTR(UPPER(REASON_21),'CANCELACION')>0 AND INSTR(UPPER(REASON_21),'NO')=0) OR
                                           (INSTR(UPPER(REASON_31),'CANCELACION')>0 AND INSTR(UPPER(REASON_31),'NO')=0) OR
                                           (INSTR(UPPER(RESULT_TD1),'CANCELACION')>0 AND INSTR(UPPER(RESULT_TD1),'NO')=0))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_CANCELACION,                          
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (((INSTR(UPPER(REASON_11),'DESACTIV')>0 OR INSTR(UPPER(REASON_11),'BAJA')>0) AND INSTR(UPPER(REASON_11),'ACTIVACION/DESACTIVACION')=0) OR
                                           ((INSTR(UPPER(REASON_21),'DESACTIV')>0 OR INSTR(UPPER(REASON_21),'BAJA')>0) AND INSTR(UPPER(REASON_21),'ACTIVACION/DESACTIVACION')=0) OR
                                           ((INSTR(UPPER(REASON_31),'DESACTIV')>0 OR INSTR(UPPER(REASON_31),'BAJA')>0) AND INSTR(UPPER(REASON_31),'ACTIVACION/DESACTIVACION')=0) OR
                                           ((INSTR(UPPER(RESULT_TD1),'DESACTIV')>0 OR INSTR(UPPER(RESULT_TD1),'BAJA')>0) AND INSTR(UPPER(RESULT_TD1),'ACTIVACION/DESACTIVACION')=0))
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_DESACTIVACION, 
                               (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          (   INSTR(UPPER(REASON_11),'DSL')>0 OR INSTR(UPPER(REASON_11),'BROOKLYN')>0 OR
                                               INSTR(UPPER(REASON_11),'MANHATTAN')>0 OR INSTR(UPPER(REASON_11),'MODE')>0 OR
                                               INSTR(UPPER(REASON_11),'INTERNET')>0 OR INSTR(UPPER(REASON_11),'CONFIGURACION')>0 OR
                                               INSTR(UPPER(REASON_11),'WIFI')>0 OR INSTR(UPPER(REASON_11),'ROUTER')>0 OR
      
                                               INSTR(UPPER(REASON_21),'DSL')>0 OR INSTR(UPPER(REASON_21),'BROOKLYN')>0 OR
                                               INSTR(UPPER(REASON_21),'MANHATTAN')>0 OR INSTR(UPPER(REASON_21),'MODE')>0 OR
                                               INSTR(UPPER(REASON_21),'INTERNET')>0 OR INSTR(UPPER(REASON_21),'CONFIGURACION')>0 OR
                                               INSTR(UPPER(REASON_21),'WIFI')>0 OR INSTR(UPPER(REASON_21),'ROUTER')>0 OR
      
                                               INSTR(UPPER(REASON_31),'DSL')>0 OR INSTR(UPPER(REASON_31),'BROOKLYN')>0 OR
                                               INSTR(UPPER(REASON_31),'MANHATTAN')>0 OR INSTR(UPPER(REASON_31),'MODE')>0 OR
                                               INSTR(UPPER(REASON_31),'INTERNET')>0 OR INSTR(UPPER(REASON_31),'CONFIGURACION')>0 OR
                                               INSTR(UPPER(REASON_31),'WIFI')>0 OR INSTR(UPPER(REASON_31),'ROUTER')>0 OR
      
                                               INSTR(UPPER(RESULT_TD1),'DSL')>0 OR INSTR(UPPER(RESULT_TD1),'BROOKLYN')>0 OR
                                               INSTR(UPPER(RESULT_TD1),'MANHATTAN')>0 OR INSTR(UPPER(RESULT_TD1),'MODE')>0 OR
                                               INSTR(UPPER(RESULT_TD1),'INTERNET')>0 OR INSTR(UPPER(RESULT_TD1),'CONFIGURACION')>0 OR
                                               INSTR(UPPER(RESULT_TD1),'WIFI')>0 OR INSTR(UPPER(RESULT_TD1),'ROUTER')>0 )
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_DSL,                                              
                              (CASE
                                          WHEN (IND_LLAMADA_VALIDA=1) AND
                                          ( INSTR(UPPER(REASON_11),'BANCA')>0 OR INSTR(UPPER(REASON_11),'CONSUMO')>0 OR
                                           INSTR(UPPER(REASON_11),'COBROS')>0 OR INSTR(UPPER(REASON_11),'PAGO')>0 OR
                                           INSTR(UPPER(REASON_11),'FACTUR')>0 OR INSTR(UPPER(REASON_11),'ABONO')>0 OR
                                           INSTR(UPPER(REASON_11),'RECARGO')>0 OR
      
                                           INSTR(UPPER(REASON_21),'BANCA')>0 OR INSTR(UPPER(REASON_21),'CONSUMO')>0 OR
                                           INSTR(UPPER(REASON_21),'COBROS')>0 OR INSTR(UPPER(REASON_21),'PAGO')>0 OR
                                           INSTR(UPPER(REASON_21),'FACTUR')>0 OR INSTR(UPPER(REASON_21),'ABONO')>0 OR
                                           INSTR(UPPER(REASON_21),'RECARGO')>0 OR
      
                                           INSTR(UPPER(REASON_31),'BANCA')>0 OR INSTR(UPPER(REASON_31),'CONSUMO')>0 OR
                                           INSTR(UPPER(REASON_31),'COBROS')>0 OR INSTR(UPPER(REASON_31),'PAGO')>0 OR
                                           INSTR(UPPER(REASON_31),'FACTUR')>0 OR INSTR(UPPER(REASON_31),'ABONO')>0 OR
                                           INSTR(UPPER(REASON_31),'RECARGO')>0 OR
      
                                           INSTR(UPPER(RESULT_TD1),'BANCA')>0 OR INSTR(UPPER(RESULT_TD1),'CONSUMO')>0 OR
                                           INSTR(UPPER(RESULT_TD1),'COBROS')>0 OR INSTR(UPPER(RESULT_TD1),'PAGO')>0 OR
                                           INSTR(UPPER(RESULT_TD1),'FACTUR')>0 OR INSTR(UPPER(RESULT_TD1),'ABONO')>0 OR
                                           INSTR(UPPER(RESULT_TD1),'RECARGO')>0)
                                           THEN 
                                                  1
                                                  ELSE
                                                      0
                              END) AS   IND_ECONOMICO
       FROM WORK.WORK_USAC_TABLA_AUX2                            
      ; 
      
              
set mapred.job.name = WORK_USAC_TABLA_AUX3;
DROP TABLE WORK.WORK_USAC_TABLA_AUX3;

            CREATE TABLE WORK.WORK_USAC_TABLA_AUX3 AS 
                SELECT 
                    AUX.*,
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (INSTR(UPPER(REASON_11),'CONDICIONES')>0 OR INSTR(UPPER(REASON_11),'TIENDA')>0 OR
                                             INSTR(UPPER(REASON_11),'SOLICITUD')>0 OR INSTR(UPPER(REASON_11),'CONSULTA')>0 OR
                                             INSTR(UPPER(REASON_11),'SEGUIMIENTO')>0 OR INSTR(UPPER(REASON_11),'ATL')>0 OR
                                             INSTR(UPPER(REASON_11),'ESTADO')>0 OR
        
                                             INSTR(UPPER(REASON_21),'CONDICIONES')>0 OR INSTR(UPPER(REASON_21),'TIENDA')>0 OR
                                             INSTR(UPPER(REASON_21),'SOLICITUD')>0 OR INSTR(UPPER(REASON_21),'CONSULTA')>0 OR
                                             INSTR(UPPER(REASON_21),'SEGUIMIENTO')>0 OR INSTR(UPPER(REASON_21),'ATL')>0 OR
                                             INSTR(UPPER(REASON_21),'ESTADO')>0 OR
        
                                             INSTR(UPPER(REASON_31),'CONDICIONES')>0 OR INSTR(UPPER(REASON_31),'TIENDA')>0 OR
                                             INSTR(UPPER(REASON_31),'SOLICITUD')>0 OR INSTR(UPPER(REASON_31),'CONSULTA')>0 OR
                                             INSTR(UPPER(REASON_31),'SEGUIMIENTO')>0 OR INSTR(UPPER(REASON_31),'ATL')>0 OR
                                             INSTR(UPPER(REASON_31),'ESTADO')>0 OR
        
                                             INSTR(UPPER(RESULT_TD1),'CONDICIONES')>0 OR INSTR(UPPER(RESULT_TD1),'TIENDA')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'SOLICITUD')>0 OR INSTR(UPPER(RESULT_TD1),'CONSULTA')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'SEGUIMIENTO')>0 OR INSTR(UPPER(RESULT_TD1),'ATL')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'ESTADO')>0 )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_INFO,  
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            ( INSTR(UPPER(REASON_11),'NORECUPE')>0 OR INSTR(UPPER(REASON_11),'NOACEPT')>0 OR INSTR(UPPER(REASON_11),'RECLAM')>0 OR
                                              INSTR(UPPER(REASON_11),'INCID')>0 OR INSTR(UPPER(REASON_11),'QUEJA')>0 OR INSTR(UPPER(REASON_11),'INSATISFEC')>0 OR
        
                                              INSTR(UPPER(REASON_21),'NORECUPE')>0 OR INSTR(UPPER(REASON_21),'NOACEPT')>0 OR INSTR(UPPER(REASON_21),'RECLAM')>0 OR
                                              INSTR(UPPER(REASON_21),'INCID')>0 OR INSTR(UPPER(REASON_21),'QUEJA')>0 OR INSTR(UPPER(REASON_21),'INSATISFEC')>0 OR
        
                                              INSTR(UPPER(REASON_31),'NORECUPE')>0 OR INSTR(UPPER(REASON_31),'NOACEPT')>0 OR INSTR(UPPER(REASON_31),'RECLAM')>0 OR
                                              INSTR(UPPER(REASON_31),'INCID')>0 OR INSTR(UPPER(REASON_31),'QUEJA')>0 OR INSTR(UPPER(REASON_31),'INSATISFEC')>0 OR
        
                                              INSTR(UPPER(RESULT_TD1),'NORECUPE')>0 OR INSTR(UPPER(RESULT_TD1),'NOACEPT')>0 OR INSTR(UPPER(RESULT_TD1),'RECLAM')>0 OR
                                              INSTR(UPPER(RESULT_TD1),'INCID')>0 OR INSTR(UPPER(RESULT_TD1),'QUEJA')>0 OR INSTR(UPPER(RESULT_TD1),'INSATISFEC')>0 )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_KOS,
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (     INSTR(UPPER(REASON_11),'MIGRACION')>0 OR INSTR(UPPER(REASON_11),'CAMBIOEMPAAUTON')>0 OR INSTR(UPPER(REASON_11),'CAMBIOAUTONAEMP')>0 OR
                                                  INSTR(UPPER(REASON_11),'CAMBIOAUTONAPART')>0 OR INSTR(UPPER(REASON_11),'CAMBIOPARTAAUTON')>0 OR INSTR(UPPER(REASON_11),'CAMBIOPARTAEMP')>0 OR
                                                  INSTR(UPPER(REASON_11),'CAMBIODEPPAACONTRATO')>0 OR INSTR(UPPER(REASON_11),'CAMBIOEMPAPART')>0 OR
                                                    
                                                  INSTR(UPPER(REASON_21),'MIGRACION')>0 OR INSTR(UPPER(REASON_21),'CAMBIOEMPAAUTON')>0 OR INSTR(UPPER(REASON_21),'CAMBIOAUTONAEMP')>0 OR
                                                  INSTR(UPPER(REASON_21),'CAMBIOAUTONAPART')>0 OR INSTR(UPPER(REASON_21),'CAMBIOPARTAAUTON')>0 OR INSTR(UPPER(REASON_21),'CAMBIOPARTAEMP')>0 OR
                                                  INSTR(UPPER(REASON_21),'CAMBIODEPPAACONTRATO')>0 OR INSTR(UPPER(REASON_21),'CAMBIOEMPAPART')>0 OR
        
                                                  INSTR(UPPER(REASON_31),'MIGRACION')>0 OR INSTR(UPPER(REASON_31),'CAMBIOEMPAAUTON')>0 OR INSTR(UPPER(REASON_31),'CAMBIOAUTONAEMP')>0 OR
                                                  INSTR(UPPER(REASON_31),'CAMBIOAUTONAPART')>0 OR INSTR(UPPER(REASON_31),'CAMBIOPARTAAUTON')>0 OR INSTR(UPPER(REASON_31),'CAMBIOPARTAEMP')>0 OR
                                                  INSTR(UPPER(REASON_31),'CAMBIODEPPAACONTRATO')>0 OR INSTR(UPPER(REASON_31),'CAMBIOEMPAPART')>0 OR
        
                                                  INSTR(UPPER(RESULT_TD1),'MIGRACION')>0 OR INSTR(UPPER(RESULT_TD1),'CAMBIOEMPAAUTON')>0 OR INSTR(UPPER(RESULT_TD1),'CAMBIOAUTONAEMP')>0 OR
                                                  INSTR(UPPER(RESULT_TD1),'CAMBIOAUTONAPART')>0 OR INSTR(UPPER(RESULT_TD1),'CAMBIOPARTAAUTON')>0 OR INSTR(UPPER(RESULT_TD1),'CAMBIOPARTAEMP')>0 OR
                                                  INSTR(UPPER(RESULT_TD1),'CAMBIODEPPAACONTRATO')>0 OR INSTR(UPPER(RESULT_TD1),'CAMBIOEMPAPART')>0)
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_MIGRACION,    
                                 (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (        INSTR(UPPER(REASON_11),'PERMANENC')>0 OR
                                                     INSTR(UPPER(REASON_21),'PERMANENC')>0 OR
                                                     INSTR(UPPER(REASON_31),'PERMANENC')>0 OR
                                                     INSTR(UPPER(RESULT_TD1),'PERMANENC')>0 )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PERMANENCIA,      
                                  (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (         INSTR(UPPER(REASON_11),'PORTABI')>0 OR
                                                 INSTR(UPPER(REASON_21),'PORTABI')>0 OR
                                                 INSTR(UPPER(REASON_31),'PORTABI')>0 OR
                                                 INSTR(UPPER(RESULT_TD1),'PORTABI')>0 )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PORTAB,  
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (          INSTR(UPPER(REASON_11),'INCID')>0 OR INSTR(UPPER(REASON_11),'COBERTURA')>0 OR INSTR(UPPER(REASON_11),'SINCONEXI')>0 OR
                                                  INSTR(UPPER(REASON_11),'INCOMUNICA')>0 OR INSTR(UPPER(REASON_11),'ERRORRECO')>0 OR INSTR(UPPER(REASON_11),'CORTE')>0 OR
                                                  INSTR(UPPER(REASON_11),'EMITE')>0 OR INSTR(UPPER(REASON_11),'RECIBE')>0 OR
                                                    
                                                  INSTR(UPPER(REASON_21),'INCID')>0 OR INSTR(UPPER(REASON_21),'COBERTURA')>0 OR INSTR(UPPER(REASON_21),'SINCONEXI')>0 OR
                                                  INSTR(UPPER(REASON_21),'INCOMUNICA')>0 OR INSTR(UPPER(REASON_21),'ERRORRECO')>0 OR INSTR(UPPER(REASON_21),'CORTE')>0 OR
                                                  INSTR(UPPER(REASON_21),'EMITE')>0 OR INSTR(UPPER(REASON_21),'RECIBE')>0 OR
        
                                                  INSTR(UPPER(REASON_31),'INCID')>0 OR INSTR(UPPER(REASON_31),'COBERTURA')>0 OR INSTR(UPPER(REASON_31),'SINCONEXI')>0 OR
                                                  INSTR(UPPER(REASON_31),'INCOMUNICA')>0 OR INSTR(UPPER(REASON_31),'ERRORRECO')>0 OR INSTR(UPPER(REASON_31),'CORTE')>0 OR
                                                  INSTR(UPPER(REASON_31),'EMITE')>0 OR INSTR(UPPER(REASON_31),'RECIBE')>0 OR
        
                                                  INSTR(UPPER(RESULT_TD1),'INCID')>0 OR INSTR(UPPER(RESULT_TD1),'COBERTURA')>0 OR INSTR(UPPER(RESULT_TD1),'SINCONEXI')>0 OR
                                                  INSTR(UPPER(RESULT_TD1),'INCOMUNICA')>0 OR INSTR(UPPER(RESULT_TD1),'ERRORRECO')>0 OR INSTR(UPPER(RESULT_TD1),'CORTE')>0 OR
                                                  INSTR(UPPER(RESULT_TD1),'EMITE')>0 OR INSTR(UPPER(RESULT_TD1),'RECIBE')>0)
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PROBS_SERVS,                                                      
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (          ((INSTR(UPPER(REASON_11),'PRODUCT')>0 OR INSTR(UPPER(REASON_11),'SERVIC')>0 OR INSTR(UPPER(REASON_11),'DISPOSITIVO')>0) AND INSTR(UPPER(REASON_11),'PORTABI')=0) OR
                                                 ((INSTR(UPPER(REASON_21),'PRODUCT')>0 OR INSTR(UPPER(REASON_21),'SERVIC')>0 OR INSTR(UPPER(REASON_21),'DISPOSITIVO')>0) AND INSTR(UPPER(REASON_21),'PORTABI')=0) OR
                                                 ((INSTR(UPPER(REASON_31),'PRODUCT')>0 OR INSTR(UPPER(REASON_31),'SERVIC')>0 OR INSTR(UPPER(REASON_31),'DISPOSITIVO')>0) AND INSTR(UPPER(REASON_31),'PORTABI')=0) OR
                                                 ((INSTR(UPPER(RESULT_TD1),'PRODUCT')>0 OR INSTR(UPPER(RESULT_TD1),'SERVIC')>0 OR INSTR(UPPER(RESULT_TD1),'DISPOSITIVO')>0) AND INSTR(UPPER(RESULT_TD1),'PORTABI')=0) )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PRODS_SERVS,                                                  
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (    INSTR(UPPER(REASON_11),'VFCASA')>0 OR INSTR(UPPER(REASON_11),'OFICINA')>0 OR INSTR(UPPER(REASON_11),'HZ')>0 OR
                                             INSTR(UPPER(REASON_21),'VFCASA')>0 OR INSTR(UPPER(REASON_21),'OFICINA')>0 OR INSTR(UPPER(REASON_21),'HZ')>0 OR
                                             INSTR(UPPER(REASON_31),'VFCASA')>0 OR INSTR(UPPER(REASON_31),'OFICINA')>0 OR INSTR(UPPER(REASON_31),'HZ')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'VFCASA')>0 OR INSTR(UPPER(RESULT_TD1),'OFICINA')>0 OR INSTR(UPPER(RESULT_TD1),'HZ')>0)
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PRODS_SERVS_FIX,    
                                 (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            ( ((INSTR(UPPER(REASON_11),'USB')>0 OR INSTR(UPPER(REASON_11),'NAVEGA')>0 OR INSTR(UPPER(REASON_11),'LPD')>0 OR INSTR(UPPER(REASON_11),'ANDROID')>0 OR
                                               INSTR(UPPER(REASON_11),'4G')>0 OR INSTR(UPPER(REASON_11),'DATOSMOVIL')>0 OR INSTR(UPPER(REASON_11),'LINEADEDATOS')>0 OR INSTR(UPPER(REASON_11),'AVANZADODATOS')>0 OR
                                             INSTR(UPPER(REASON_11),'DATOSPART')>0 OR INSTR(UPPER(REASON_11),'DATOSEMP')>0 OR INSTR(UPPER(REASON_11),'SMARTPHONE')>0 OR
                                             INSTR(UPPER(REASON_11),'TABLET')>0 OR INSTR(UPPER(REASON_11),'IOYM')>0) AND INSTR(UPPER(REASON_11),'DESACTIVARIOYM')=0 AND INSTR(UPPER(REASON_11),'NOSMARTPHONE')=0) OR
        
                                             ((INSTR(UPPER(REASON_21),'USB')>0 OR INSTR(UPPER(REASON_21),'NAVEGA')>0 OR INSTR(UPPER(REASON_21),'LPD')>0 OR INSTR(UPPER(REASON_21),'ANDROID')>0 OR
                                             INSTR(UPPER(REASON_21),'4G')>0 OR INSTR(UPPER(REASON_21),'DATOSMOVIL')>0 OR INSTR(UPPER(REASON_21),'LINEADEDATOS')>0 OR INSTR(UPPER(REASON_21),'AVANZADODATOS')>0 OR
                                             INSTR(UPPER(REASON_21),'DATOSPART')>0 OR INSTR(UPPER(REASON_21),'DATOSEMP')>0 OR INSTR(UPPER(REASON_21),'SMARTPHONE')>0 OR
                                             INSTR(UPPER(REASON_21),'TABLET')>0 OR INSTR(UPPER(REASON_21),'IOYM')>0) AND INSTR(UPPER(REASON_21),'DESACTIVARIOYM')=0 AND INSTR(UPPER(REASON_21),'NOSMARTPHONE')=0) OR
        
                                             ((INSTR(UPPER(REASON_31),'USB')>0 OR INSTR(UPPER(REASON_31),'NAVEGA')>0 OR INSTR(UPPER(REASON_31),'LPD')>0 OR INSTR(UPPER(REASON_31),'ANDROID')>0 OR
                                             INSTR(UPPER(REASON_31),'4G')>0 OR INSTR(UPPER(REASON_31),'DATOSMOVIL')>0 OR INSTR(UPPER(REASON_31),'LINEADEDATOS')>0 OR INSTR(UPPER(REASON_31),'AVANZADODATOS')>0 OR
                                             INSTR(UPPER(REASON_31),'DATOSPART')>0 OR INSTR(UPPER(REASON_31),'DATOSEMP')>0 OR INSTR(UPPER(REASON_31),'SMARTPHONE')>0 OR
                                             INSTR(UPPER(REASON_31),'TABLET')>0 OR INSTR(UPPER(REASON_31),'IOYM')>0) AND INSTR(UPPER(REASON_31),'DESACTIVARIOYM')=0 AND INSTR(UPPER(REASON_31),'NOSMARTPHONE')=0) OR
        
                                             ((INSTR(UPPER(RESULT_TD1),'USB')>0 OR INSTR(UPPER(RESULT_TD1),'NAVEGA')>0 OR INSTR(UPPER(RESULT_TD1),'LPD')>0 OR INSTR(UPPER(RESULT_TD1),'ANDROID')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'4G')>0 OR INSTR(UPPER(RESULT_TD1),'DATOSMOVIL')>0 OR INSTR(UPPER(RESULT_TD1),'LINEADEDATOS')>0 OR INSTR(UPPER(RESULT_TD1),'AVANZADODATOS')>0 OR 
                                             INSTR(UPPER(RESULT_TD1),'DATOSPART')>0 OR INSTR(UPPER(RESULT_TD1),'DATOSEMP')>0 OR INSTR(UPPER(RESULT_TD1),'SMARTPHONE')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'TABLET')>0 OR INSTR(UPPER(RESULT_TD1),'IOYM')>0) AND INSTR(UPPER(RESULT_TD1),'DESACTIVARIOYM')=0 AND INSTR(UPPER(RESULT_TD1),'NOSMARTPHONE')=0) )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_PRODS_SERVS_MOVS,                         
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (         INSTR(UPPER(REASON_11),'INSTALACION')>0 OR INSTR(UPPER(REASON_11),'TECNIC')>0 OR
                                                 INSTR(UPPER(REASON_21),'INSTALACION')>0 OR INSTR(UPPER(REASON_21),'TECNIC')>0 OR
                                                 INSTR(UPPER(REASON_31),'INSTALACION')>0 OR INSTR(UPPER(REASON_31),'TECNIC')>0 OR
                                                 INSTR(UPPER(RESULT_TD1),'INSTALACION')>0 OR INSTR(UPPER(RESULT_TD1),'TECNIC')>0)
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_SERV_TECNICO,                                  
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (         INSTR(UPPER(REASON_11),'VODAFONEYU')>0 OR INSTR(UPPER(REASON_11),'PLANPRECIOS')>0 OR INSTR(UPPER(REASON_11),'REDVFDATOS')>0 OR INSTR(UPPER(REASON_11),'PLAN')>0 OR
                                                 INSTR(UPPER(REASON_11),'PLANESDATOS')>0 OR INSTR(UPPER(REASON_11),'PLANDATOS')>0 OR INSTR(UPPER(REASON_11),'REDVOL')>0 OR INSTR(UPPER(REASON_11),'TARIF')>0 OR
        
                                                 INSTR(UPPER(REASON_21),'VODAFONEYU')>0 OR INSTR(UPPER(REASON_21),'PLANPRECIOS')>0 OR INSTR(UPPER(REASON_21),'REDVFDATOS')>0 OR INSTR(UPPER(REASON_21),'PLAN')>0 OR
                                                 INSTR(UPPER(REASON_21),'PLANESDATOS')>0 OR INSTR(UPPER(REASON_21),'PLANDATOS')>0 OR INSTR(UPPER(REASON_21),'REDVOL')>0 OR INSTR(UPPER(REASON_21),'TARIF')>0 OR
        
                                                 INSTR(UPPER(REASON_31),'VODAFONEYU')>0 OR INSTR(UPPER(REASON_31),'PLANPRECIOS')>0 OR INSTR(UPPER(REASON_31),'REDVFDATOS')>0 OR INSTR(UPPER(REASON_31),'PLAN')>0 OR
                                                 INSTR(UPPER(REASON_31),'PLANESDATOS')>0 OR INSTR(UPPER(REASON_31),'PLANDATOS')>0 OR INSTR(UPPER(REASON_31),'REDVOL')>0 OR INSTR(UPPER(REASON_31),'TARIF')>0 OR
        
                                                 INSTR(UPPER(RESULT_TD1),'VODAFONEYU')>0 OR INSTR(UPPER(RESULT_TD1),'PLANPRECIOS')>0 OR INSTR(UPPER(RESULT_TD1),'REDVFDATOS')>0 OR INSTR(UPPER(RESULT_TD1),'PLAN')>0 OR
                                                 INSTR(UPPER(RESULT_TD1),'PLANESDATOS')>0 OR INSTR(UPPER(RESULT_TD1),'PLANDATOS')>0 OR INSTR(UPPER(RESULT_TD1),'REDVOL')>0 OR INSTR(UPPER(RESULT_TD1),'TARIF')>0 )
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_TARIFS,                          
                                (CASE
                                            WHEN (IND_LLAMADA_VALIDA=1) AND
                                            (             INSTR(UPPER(REASON_11),'VOZ')>0 OR
                                             INSTR(UPPER(REASON_21),'VOZ')>0 OR
                                             INSTR(UPPER(REASON_31),'VOZ')>0 OR
                                             INSTR(UPPER(RESULT_TD1),'VOZ')>0)
                                             THEN 
                                                    1
                                                    ELSE
                                                        0
                                END) AS   IND_VOZ
                                
            FROM    WORK.WORK_USAC_TABLA_VARIABLES  AUX                   
        ; 
        
set mapred.job.name = WORK_USAC_DD;
DROP TABLE WORK.WORK_USAC_DD;
CREATE TABLE  WORK.WORK_USAC_DD AS
        SELECT  MSISDN, 
                                     SUM(IND_LLAMADA) AS NUM_USAC_LLAMADA,
                                     SUM(IND_LLAMADA_VALIDA) AS NUM_USAC_LLAMADA_VALIDA,
                                     SUM(IND_PORTABILIDAD) AS NUM_USAC_PORTABILIDAD,
                                     SUM(IND_VFCASA) AS NUM_USAC_VFCASA,
                                     SUM(IND_OK) AS NUM_USAC_OK,
                                     SUM(IND_KO) AS NUM_USAC_KO,
                                     SUM(IND_PROD_SERV) AS NUM_USAC_PROD_SERV,
                                     SUM(IND_TARIFAS) AS NUM_USAC_TARIFAS,
                                     SUM(IND_PAGOS) AS NUM_USAC_PAGOS,
                                     SUM(IND_DEUDA) AS NUM_USAC_DEUDA,
                                     SUM(IND_ADSL) AS NUM_USAC_ADSL,
                                     SUM(IND_PROMOCIONES) AS NUM_USAC_PROMOCIONES,
                                     SUM(IND_PENDIENTE) AS NUM_USAC_PENDIENTE,
                                     SUM(IND_ALTA) AS NUM_USAC_ALTA,
                                     SUM(IND_CANCELACION) AS NUM_USAC_CANCELACION,
                                     SUM(IND_DESACTIVACION) AS NUM_USAC_DESACTIVACION,
                                     SUM(IND_DSL) AS NUM_USAC_DSL,
                                     SUM(IND_ECONOMICO) AS NUM_USAC_ECONOMICO,
                                     SUM(IND_INFO) AS NUM_USAC_INFO,
                                     SUM(IND_KOS) AS NUM_USAC_KOS,
                                     SUM(IND_MIGRACION) AS NUM_USAC_MIGRACION,
                                     SUM(IND_PERMANENCIA) AS NUM_USAC_PERMANENCIA,
                                     SUM(IND_PORTAB) AS NUM_USAC_PORTAB,
                                     SUM(IND_PROBS_SERVS) AS NUM_USAC_PROBS_SERVS,
                                     SUM(IND_PRODS_SERVS) AS NUM_USAC_PRODS_SERVS,
                                     SUM(IND_PRODS_SERVS_FIX) AS NUM_USAC_PRODS_SERVS_FIX,
                                     SUM(IND_PRODS_SERVS_MOVS) AS NUM_USAC_PRODS_SERVS_MOVS,
                                     SUM(IND_SERV_TECNICO) AS NUM_USAC_SERV_TECNICO,
                                     SUM(IND_TARIFS) AS NUM_USAC_TARIFS,
                                     SUM(IND_VOZ) AS NUM_USAC_VOZ
                                     
                                     
                                     FROM WORK.WORK_USAC_TABLA_AUX3 
                                     GROUP BY MSISDN;
									 
set mapred.job.name = WORK_USAC_LINEAS;
DROP TABLE WORK.WORK_USAC_LINEAS;
CREATE TABLE WORK.WORK_USAC_LINEAS  AS
        
            SELECT  
                      MSISDN                       ,
                      NUM_USAC_LLAMADA             ,
                      NUM_USAC_LLAMADA_VALIDA      ,
                      NUM_USAC_PORTABILIDAD        ,
                      NUM_USAC_VFCASA              ,
                      NUM_USAC_OK                  ,
                      NUM_USAC_KO                  ,
                      NUM_USAC_PROD_SERV           ,
                      NUM_USAC_TARIFAS             ,
                      NUM_USAC_PAGOS               ,
                      NUM_USAC_DEUDA               ,
                      NUM_USAC_ADSL                ,
                      NUM_USAC_PROMOCIONES         ,
                      NUM_USAC_PENDIENTE           ,
                      NUM_USAC_ALTA                ,
                      NUM_USAC_CANCELACION         ,
                      NUM_USAC_DESACTIVACION       ,
                      NUM_USAC_DSL                 ,
                      NUM_USAC_ECONOMICO           ,
                      NUM_USAC_INFO                ,
                      NUM_USAC_KOS                 ,
                      NUM_USAC_MIGRACION           ,
                      NUM_USAC_PERMANENCIA         ,
                      NUM_USAC_PORTAB              ,
                      NUM_USAC_PROBS_SERVS         ,
                      NUM_USAC_PRODS_SERVS         ,
                      NUM_USAC_PRODS_SERVS_FIX     ,
                      NUM_USAC_PRODS_SERVS_MOVS    ,
                      NUM_USAC_SERV_TECNICO        ,
                      NUM_USAC_TARIFS              ,
                      NUM_USAC_VOZ                       
                        
            FROM WORK.WORK_USAC_DD 
            
            WHERE MSISDN IS NOT NULL;
                      
    
set mapred.job.name = DO_PAR_USAC_CHORDIANT_${hiveconf:MONTH0};
DROP TABLE OUTPUT.DO_PAR_USAC_CHORDIANT_${hiveconf:MONTH0};
CREATE TABLE OUTPUT.DO_PAR_USAC_CHORDIANT_${hiveconf:MONTH0} AS
    SELECT
        B.*,
        NIF
    FROM
      (SELECT DISTINCT X_ID_RED AS MSISDN, X_NUM_IDENT AS NIF  FROM INPUT.VF_POS_AC_FINAL  WHERE PARTITIONED_MONTH =  '${hiveconf:MONTH0}') A
      INNER JOIN
      WORK.WORK_USAC_LINEAS B
      ON A.MSISDN = B.MSISDN
    WHERE
      NUM_USAC_LLAMADA             > 0 OR
                      NUM_USAC_LLAMADA_VALIDA > 0 OR
                      NUM_USAC_PORTABILIDAD        > 0 OR
                      NUM_USAC_VFCASA              > 0 OR
                      NUM_USAC_OK                  > 0 OR
                      NUM_USAC_KO                  > 0 OR
                      NUM_USAC_PROD_SERV           > 0 OR
                      NUM_USAC_TARIFAS             > 0 OR
                      NUM_USAC_PAGOS               > 0 OR
                      NUM_USAC_DEUDA               > 0 OR
                      NUM_USAC_ADSL                > 0 OR
                      NUM_USAC_PROMOCIONES         > 0 OR
                      NUM_USAC_PENDIENTE           > 0 OR
                      NUM_USAC_ALTA                > 0 OR
                      NUM_USAC_CANCELACION         > 0 OR
                      NUM_USAC_DESACTIVACION       > 0 OR
                      NUM_USAC_DSL                 > 0 OR
                      NUM_USAC_ECONOMICO           > 0 OR
                      NUM_USAC_INFO                > 0 OR
                      NUM_USAC_KOS                 > 0 OR
                      NUM_USAC_MIGRACION           > 0 OR
                      NUM_USAC_PERMANENCIA         > 0 OR
                      NUM_USAC_PORTAB              > 0 OR
                      NUM_USAC_PROBS_SERVS         > 0 OR
                      NUM_USAC_PRODS_SERVS         > 0 OR
                      NUM_USAC_PRODS_SERVS_FIX     > 0 OR
                      NUM_USAC_PRODS_SERVS_MOVS    > 0 OR
                      NUM_USAC_SERV_TECNICO        > 0 OR
                      NUM_USAC_TARIFS              > 0 OR
                      NUM_USAC_VOZ > 0                   ; 
    
    
	
DROP TABLE WORK.WORK_USAC_TABLA_AUX;
DROP TABLE WORK.WORK_USAC_TABLA;
DROP TABLE WORK.WORK_USAC_TABLA_AUX2;
DROP TABLE WORK.WORK_USAC_TABLA_VARIABLES;
DROP TABLE WORK.WORK_USAC_TABLA_AUX3;
DROP TABLE WORK.WORK_USAC_DD;
DROP TABLE WORK.WORK_USAC_LINEAS;
	
	
EXIT;
