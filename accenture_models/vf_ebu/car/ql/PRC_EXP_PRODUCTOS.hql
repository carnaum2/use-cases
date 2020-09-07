USE VF_EBU;        
                 
set mapred.job.name = EMP_EXP_PRODUCTOS;
DROP TABLE VF_EBU.EMP_EXP_PRODUCTOS;
CREATE TABLE VF_EBU.EMP_EXP_PRODUCTOS AS
       SELECT
          NIF,
          '${hiveconf:MONTH0}' AS MES,
          MAX(A.IND_ADSL_LISTANEGRA) AS IND_ADSL_LISTANEGRA,
          MAX(A.IND_ADSL_PROVISION) AS IND_ADSL_PROVISION,
          MAX(A.IND_ADSL_PERMANENCIA) AS IND_ADSL_PERMANENCIA,
          MAX(A.SEG_ADSL_TIPO) AS SEG_ADSL_TIPO,
          MAX(A.IND_OV) AS IND_OV,
          MAX(A.IND_FIJO_VF) AS IND_FIJO_VF,
          MAX(A.IND_ZONA_OFICINA) AS IND_ZONA_OFICINA,
          MAX(A.IND_FIJO_OFICINA_MOVIL) AS IND_FIJO_OFICINA_MOVIL,
          MAX(A.IND_FACT_ELECTRONICA) AS IND_FACT_ELECTRONICA
       FROM (
           SELECT 
              CIF_NIF AS NIF,
              case when adsl_listasnegras = 'SI' then 1 else 0 end as ind_adsl_listanegra,
              case when adsl_provision  = 'SI' then 1 else 0 end as ind_adsl_provision,
              case when adsl_permanencia  = 'SI' then 1 else 0 end as ind_adsl_permanencia,
              adsl_tipo as seg_adsl_tipo,
              case when oficinavf  = 'SI' then 1 else 0 end as ind_ov,
              case when vodafonefijo  = 'SI' then 1 else 0 end as ind_fijo_vf,
              case when zonaoficina  = 'SI' then 1 else 0 end as ind_zona_oficina,
              case when fijooficinamovil  = 'SI' then 1 else 0 end as ind_fijo_oficina_movil,
              case when factelect  = 'SI' then 1 else 0 end as ind_fact_electronica,
              '${hiveconf:MONTH0}' AS MES
           FROM 
              INPUT.VF_EBU_AC_EMP_PRODUCTOS WHERE PARTITIONED_MONTH = '${hiveconf:MONTH0}'
              ) A
        GROUP BY NIF;
   
    
EXIT;
