
from pyspark.sql.functions import (col, lit, lower, concat, count, max, avg, desc, asc, row_number, lpad, trim, when, split, to_date, coalesce,  isnull)
from pyspark.sql import Window
import datetime as dt

from my_propensity_operators.churn_nrt.src.data_utils.DataTemplate import DataTemplate
from my_propensity_operators.churn_nrt.src.utils.date_functions import is_null_date



def get_last_date(spark):
    '''
    Return the last date YYYYMMDD with info in the raw source
    :param spark:
    :return:
    '''

    last_date = spark\
    .read\
    .parquet('/data/raw/vf_es/customerprofilecar/SERVICESOW/1.1/parquet/')\
    .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
    .select(max(col('mydate')).alias('last_date'))\
    .rdd\
    .first()['last_date']

    return int(last_date)

class Service(DataTemplate):

    def __init__(self, spark, confs):

        DataTemplate.__init__(self, spark, confs)

        self.confs = confs
        self.confs['module_name']= 'service'
        self.set_path_configs()


        self.PATH_RAW_CUSTOMER = self.confs["customer_raw"] 

        self.PATH_RAW_SERVICE = self.confs["service_raw"]

        self.PATH_RAW_SERVICE_PRICE = self.confs["service_price"] 
        
        self.LOC_RT_PARAM_OW_SERVICES = self.confs["param_ow_services"] 

        self.LOC_RT_EXPORT_MAP = self.confs["export_map"] 




    def build_module(self, closing_day, save_others, **kwargs):


        # data_mapper = self.SPARK.read.csv(self.LOC_RT_EXPORT_MAP, sep='|', header = True)

        data_service_ori = (self.SPARK.read.parquet(self.PATH_RAW_SERVICE)
                        .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

        data_serviceprice_ori = (self.SPARK.read.parquet(self.PATH_RAW_SERVICE_PRICE)
                             .where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day)
                            )

        w_srv = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

        data_service_ori_norm = (data_service_ori
            .withColumn("rowNum", row_number().over(w_srv))
            .where(col('rowNum') == 1)
            )

        w_srv_price = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

        data_serviceprice_ori_norm = (data_serviceprice_ori
            .withColumn("rowNum", row_number().over(w_srv_price))
            .withColumnRenamed('OBJID', 'OBJID2PRICE')
            .withColumnRenamed('PRICE', 'PRICE2PRICE')
            .where(col('rowNum') == 1)
            )

        data_service_param = self.SPARK.read.csv(self.LOC_RT_PARAM_OW_SERVICES, sep='\t', header = True)
        data_service_param = (data_service_param.where(col('rgu').isNotNull())
            .withColumn('rgu', when(data_service_param['rgu'] == 'bam-movil', 'bam_mobile')
                .when(data_service_param['rgu'] == 'movil', 'mobile')
                .otherwise(data_service_param['rgu']))
            )

        ClosingDay_date = dt.date(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:8]))
        yesterday = ClosingDay_date + dt.timedelta(days=-1)

        data_service_tmp1_basic = (data_service_ori_norm['OBJID', 'NUM_CLIENTE', 'NUM_SERIE', 'COD_SERVICIO', 'ESTADO_SERVICIO', 'FECHA_INST', 'FECHA_CAMB', 'INSTANCIA', 'PRIM_RATE', 'SERVICIOS_ACTUALES2PRICE', 'year', 'month', 'day']
            .where(((to_date(col('FECHA_CAMB')) >= yesterday)) | (col('FECHA_CAMB').isNull()) | (is_null_date(col('FECHA_CAMB'))))
            .withColumn("Instancia_P", trim(split(data_service_ori_norm.INSTANCIA, '\\.')[0]))
            .withColumn("Instancia_S", split(data_service_ori_norm.INSTANCIA, '\\.')[1])
            .withColumn("TACADA",concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
            .join(data_service_param['COD_SERVICIO', 'DESC_SERVICIO', 'RGU', 'RGU_MOBILE', 'RGU_BAM', 'TIPO', 'LEGACY'],["COD_SERVICIO"], 'inner')
            .join(data_serviceprice_ori_norm['OBJID2PRICE', 'PRICE2PRICE'], col('SERVICIOS_ACTUALES2PRICE') == col('OBJID2PRICE'), 'leftouter')
            .withColumn('MSISDN',
                when((col('Instancia_S').isNull() & (col('COD_SERVICIO') == 'TVOTG')),
                    concat(lit('FICT_TVOTG_'), data_service_ori_norm.NUM_CLIENTE))
                .when((col('Instancia_S').isNull() & (col('COD_SERVICIO') != 'TVOTG')),
                    data_service_ori_norm.NUM_SERIE)
                .otherwise(lit(None)))
            .withColumn('SERV_PRICE',
                when((col('PRIM_RATE').isNull()) | (trim(col('PRIM_RATE')) == ''),
                    data_serviceprice_ori_norm.PRICE2PRICE.cast('double'))
                .otherwise(data_service_ori_norm.PRIM_RATE.cast('double')))
            )

        data_service_tmp2_basic = (data_service_tmp1_basic
                .groupBy('NUM_CLIENTE', 'Instancia_P')
                .agg(
                max(col('MSISDN')).alias("MSISDN")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("SRV_BASIC")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.DESC_SERVICIO)
                      .otherwise(None)).alias("DESC_SRV_BASIC")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.OBJID)
                      .otherwise(None)).alias("OBJID")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.TACADA)
                      .otherwise(None)).alias("TACADA")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_SRV_BASIC")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_SRV_BASIC")
                , max(when(col('Instancia_S').isNull(), data_service_tmp1_basic.RGU)
                      .otherwise(None)).alias("RGU")
                , max(when((col('TIPO') == 'SIM'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TIPO_SIM")
                , max(when((col('TIPO') == 'SIM'), data_service_tmp1_basic.NUM_SERIE)
                      .otherwise(None)).alias("IMSI")
                , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TARIFF")
                , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TARIFF")
                , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.DESC_SERVICIO)
                      .otherwise(None)).alias("DESC_TARIFF")
                , max(when((col('TIPO') == 'TARIFA'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TARIFF")
                , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("VOICE_TARIFF")
                , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_VOICE_TARIFF")
                , max(when((col('TIPO') == 'TARIFA_VOZ'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_VOICE_TARIFF")
                , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DATA")
                , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DATA")
                , max(when((col('TIPO') == 'MODULO_DATOS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DATA")
                , max(when((col('TIPO').isin('DTO_NIVEL1')), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DTO_LEV1")
                , max(when((col('TIPO').isin('DTO_NIVEL1')), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DTO_LEV1")
                , max(when((col('TIPO') == 'DTO_NIVEL1'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DTO_LEV1")
                , max(when((col('TIPO').isin('DTO_NIVEL2')), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DTO_LEV2")
                , max(when((col('TIPO').isin('DTO_NIVEL2')), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DTO_LEV2")
                , max(when((col('TIPO') == 'DTO_NIVEL2'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DTO_LEV2")
                , max(when((col('TIPO').isin('DTO_NIVEL3')), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DTO_LEV3")
                , max(when((col('TIPO').isin('DTO_NIVEL3')), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DTO_LEV3")
                , max(when((col('TIPO') == 'DTO_NIVEL3'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DTO_LEV3")
                , max(when((col('TIPO').isin('DATOS_ADICIONALES')), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DATA_ADDITIONAL")
                , max(when((col('TIPO').isin('DATOS_ADICIONALES')), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DATA_ADDITIONAL")
                , max(when((col('TIPO') == 'DATOS_ADICIONALES'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DATA_ADDITIONAL")
                , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("OOB")
                , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_OOB")
                , max(when((col('TIPO') == 'OOB'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_OOB")
                , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("NETFLIX_NAPSTER")
                , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_NETFLIX_NAPSTER")
                , max(when((col('TIPO') == 'NETFLIX_NAPSTER'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_NETFLIX_NAPSTER")
                , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("ROAMING_BASIC")
                , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_ROAMING_BASIC")
                , max(when((col('TIPO') == 'ROAMING_BASICO'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_ROAMING_BASIC")
                , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("ROAM_USA_EUR")
                , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_ROAM_USA_EUR")
                , max(when((col('TIPO') == 'ROAM_USA_EUR'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_ROAM_USA_EUR")
                , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("ROAM_ZONA_2")
                , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_ROAM_ZONA_2")
                , max(when((col('TIPO') == 'ROAM_ZONA_2'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_ROAM_ZONA_2")
                , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("CONSUM_MIN")
                , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_CONSUM_MIN")
                , max(when((col('TIPO') == 'CONSUMO_MIN'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_CONSUM_MIN")
                , max(when(col('COD_SERVICIO') == 'SIMVF', 1)
                      .otherwise(0)).alias("SIM_VF")
                , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("HOMEZONE")
                , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_HOMEZONE")
                , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_HOMEZONE")
                , max(when((col('TIPO') == 'HOMEZONE'), data_service_tmp1_basic.NUM_SERIE)
                      .otherwise(None)).alias("MOBILE_HOMEZONE")
                , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("FBB_UPGRADE")
                , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_FBB_UPGRADE")
                , max(when((col('TIPO') == 'UPGRADE'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_FBB_UPGRADE")
                , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DECO_TV")
                , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DECO_TV")
                , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DECO_TV")
                , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.NUM_SERIE)
                      .otherwise(None)).alias("NUM_SERIE_DECO_TV")
                , max(when((col('TIPO') == 'DECO'), data_service_tmp1_basic.OBJID)
                      .otherwise(None)).alias("OBJID_DECO_TV")
                , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_CUOTA_ALTA")
                , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_CUOTA_ALTA")
                , max(when((col('TIPO') == 'TVCUOTAALTA'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_CUOTA_ALTA")
                , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_TARIFF")
                , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_TARIFF")
                , max(when((col('TIPO') == 'TV_PLANES_TARIFAS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_TARIFF")
                , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_CUOT_CHARGES")
                , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_CUOT_CHARGES")
                , max(when((col('TIPO') == 'TV_CUOTASCARGOS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_CUOT_CHARGES")
                , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_PROMO")
                , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_PROMO")
                , max(when((col('TIPO') == 'TVPROMOS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_PROMO")
                , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_PROMO_USER")
                , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_PROMO_USER")
                , max(when((col('TIPO') == 'TVPROMOUSER'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_PROMO_USER")
                , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_ABONOS")
                , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_ABONOS")
                , max(when((col('TIPO') == 'TV_ABONOS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_ABONOS")
                , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_LOYALTY")
                , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_LOYALTY")
                , max(when((col('TIPO') == 'TV_FIDELIZA'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_LOYALTY")
                , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TV_SVA")
                , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TV_SVA")
                , max(when((col('TIPO') == 'TV_SVA'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TV_SVA")
                , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("FOOTBALL_TV")
                , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_FOOTBALL_TV")
                , max(when((col('TIPO') == 'C_PLUS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_FOOTBALL_TV")
                , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("MOTOR_TV")
                , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_MOTOR_TV")
                , max(when((col('TIPO') == 'MOTOR'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_MOTOR_TV")
                , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PVR_TV")
                , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PVR_TV")
                , max(when((col('TIPO') == 'PVR'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PVR_TV")
                , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("ZAPPER_TV")
                , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_ZAPPER_TV")
                , max(when((col('TIPO') == 'ZAPPER'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_ZAPPER_TV")
                , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TRYBUY_TV")
                , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TRYBUY_TV")
                , max(when((col('TIPO') == 'TRYBUY'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TRYBUY_TV")
                , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TRYBUY_AUTOM_TV")
                , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TRYBUY_AUTOM_TV")
                , max(when((col('TIPO') == 'TRYBUY_AUTOM'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TRYBUY_AUTOM_TV")

                , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("CINE")
                , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_CINE")
                , max(when((col('TIPO') == 'CINE'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_CINE")

                , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("SERIES")
                , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_SERIES")
                , max(when((col('TIPO') == 'SERIES'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_SERIES")

                , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("SERIEFANS")
                , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_SERIEFANS")
                , max(when((col('TIPO') == 'SERIEFANS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_SERIEFANS")

                , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("SERIELOVERS")
                , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_SERIELOVERS")
                , max(when((col('TIPO') == 'SERIELOVERS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_SERIELOVERS")

                , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("CINEFANS")
                , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_CINEFANS")
                , max(when((col('TIPO') == 'CINEFANS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_CINEFANS")

                , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PEQUES")
                , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PEQUES")
                , max(when((col('TIPO') == 'PEQUES'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PEQUES")

                , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("DOCUMENTALES")
                , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_DOCUMENTALES")
                , max(when((col('TIPO') == 'DOCUMENTALES'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_DOCUMENTALES")

                , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("HBO")
                , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_HBO")
                , max(when((col('TIPO') == 'HBO'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_HBO")

                , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PROMO_HBO")
                , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PROMO_HBO")
                , max(when((col('TIPO') == 'PROMO_HBO'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PROMO_HBO")

                , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("FILMIN")
                , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_FILMIN")
                , max(when((col('TIPO') == 'FILMIN'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_FILMIN")

                , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PROMO_FILMIN")
                , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PROMO_FILMIN")
                , max(when((col('TIPO') == 'PROMO_FILMIN'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PROMO_FILMIN")

                #VODAFONE PASS
                , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("VIDEOHDPASS")
                , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_VIDEOHDPASS")
                , max(when((col('TIPO') == 'VIDEOHDPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_VIDEOHDPASS")

                , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("MUSICPASS")
                , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_MUSICPASS")
                , max(when((col('TIPO') == 'MUSICPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_MUSICPASS")

                , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("VIDEOPASS")
                , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_VIDEOPASS")
                , max(when((col('TIPO') == 'VIDEOPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_VIDEOPASS")

                , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("SOCIALPASS")
                , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_SOCIALPASS")
                , max(when((col('TIPO') == 'SOCIALPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_SOCIALPASS")

                , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("MAPSPASS")
                , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_MAPSPASS")
                , max(when((col('TIPO') == 'MAPSPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_MAPSPASS")

                , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("CHATPASS")
                , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_CHATPASS")
                , max(when((col('TIPO') == 'CHATPASS'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_CHATPASS")

                , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("AMAZON")
                , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_AMAZON")
                , max(when((col('TIPO') == 'AMAZON'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_AMAZON")

                , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PROMO_AMAZON")
                , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PROMO_AMAZON")
                , max(when((col('TIPO') == 'PROMO_AMAZON'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PROMO_AMAZON")

                , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("TIDAL")
                , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_TIDAL")
                , max(when((col('TIPO') == 'TIDAL'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_TIDAL")

                , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.COD_SERVICIO)
                      .otherwise(None)).alias("PROMO_TIDAL")
                , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.FECHA_INST)
                      .otherwise(None)).alias("FX_PROMO_TIDAL")
                , max(when((col('TIPO') == 'PROMO_TIDAL'), data_service_tmp1_basic.SERV_PRICE)
                      .otherwise(None)).alias("PRICE_PROMO_TIDAL")
                )
                              )

        cols_tv_charges = ['PRICE_SRV_BASIC',\
        'PRICE_TV_CUOT_CHARGES',\
        'PRICE_TV_CUOTA_ALTA',\
        'PRICE_DECO_TV',\
        'PRICE_TV_TARIFF',\
        'PRICE_TV_PROMO',\
        'PRICE_TV_PROMO_USER',\
        'PRICE_TV_LOYALTY',\
        'PRICE_TV_SVA',\
        'PRICE_TV_ABONOS',\
        'PRICE_TRYBUY_AUTOM_TV',\
        'PRICE_TRYBUY_TV',\
        'PRICE_ZAPPER_TV',\
        'PRICE_PVR_TV',\
        'PRICE_MOTOR_TV',\
        'PRICE_FOOTBALL_TV',\
        'PRICE_CINE',\
        'PRICE_SERIES',\
        'PRICE_SERIEFANS',\
        'PRICE_SERIELOVERS',\
        'PRICE_CINEFANS',\
        'PRICE_PEQUES',\
        'PRICE_DOCUMENTALES']

        cols_mobile_charges = ['PRICE_SRV_BASIC',\
        'PRICE_TARIFF',\
        'PRICE_DTO_LEV1',\
        'PRICE_DTO_LEV2',\
        'PRICE_DTO_LEV3',\
        'PRICE_DATA',\
        'PRICE_VOICE_TARIFF',\
        'PRICE_DATA_ADDITIONAL',\
        'PRICE_OOB',\
        'PRICE_NETFLIX_NAPSTER',\
        'PRICE_ROAM_USA_EUR',\
        'PRICE_ROAMING_BASIC',\
        'PRICE_ROAM_ZONA_2',\
        'PRICE_CONSUM_MIN']

        data_service_tmp3_basic = (data_service_tmp2_basic
                               .withColumn('TV_TOTAL_CHARGES_PREV', sum(coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_tv_charges))
                               .withColumn('MOBILE_BAM_TOTAL_CHARGES_PREV', sum(coalesce(data_service_tmp2_basic[c], lit(0)) for c in cols_mobile_charges))
                              )

        w_srv_2 = Window().partitionBy("NUM_CLIENTE", "MSISDN").orderBy(desc("TACADA"))

        data_service_tmp4_basic = (data_service_tmp3_basic
                               .withColumn("rowNum", row_number().over(w_srv_2))
                               .where(col('rowNum') == 1)
                              )


        w_srv_3 = Window().partitionBy("MSISDN").orderBy(desc("TACADA"))
        data_service_tmp5_basic = (data_service_tmp4_basic
                                   .withColumn("rowNum", row_number().over(w_srv_3))
                                   .where(col("rowNum") == 1))

        # data_service_tmp5_basic = (data_service_tmp4_basic
        #                        .withColumn("rowNum", row_number().over(w_srv_3))
        #                        .where(col('rowNum') == 1)
        #                        .join(data_mapper, (col("OBJID") == col("CAMPO3")), 'leftouter')
        #                       )

        data_service_tmp6_basic = (data_service_tmp5_basic
                               .withColumn('flag_msisdn_err',
                                           when(col('msisdn').like('% '), 1)
                                           .otherwise(0))
                               .withColumn('msisdn', trim(col('msisdn')))
                               .where(col('msisdn').isNotNull())
                               .where(col('msisdn') != '')
                               .withColumn('TV_TOTAL_CHARGES',
                                           when(col('rgu') == 'tv', data_service_tmp5_basic.TV_TOTAL_CHARGES_PREV)
                                           .otherwise(0))
                               .withColumn('MOBILE_BAM_TOTAL_CHARGES',
                                           when(col('rgu').isin('bam', 'bam_mobile', 'mobile'),data_service_tmp5_basic.MOBILE_BAM_TOTAL_CHARGES_PREV)
                                           .otherwise(0))
                               .drop(col('TV_TOTAL_CHARGES_PREV'))
                               .drop(col('MOBILE_BAM_TOTAL_CHARGES_PREV'))
                               .drop(col('rowNum'))
                              )

        data_service_tmp6_basic = data_service_tmp6_basic.filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))

        return data_service_tmp6_basic
