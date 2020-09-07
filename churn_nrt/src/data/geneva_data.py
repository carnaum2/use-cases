#!/usr/bin/env python
# -*- coding: utf-8 -*-

from churn_nrt.src.utils.date_functions import move_date_n_days
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from pyspark.sql.types import StringType
from churn_nrt.src.utils.pyspark_utils import sum_horizontal

from pyspark.sql.functions import (udf,
                                   col,
                                   when,
                                   lit,
                                   lower,
                                   count,
                                   expr,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   count as sql_count,
                                   sum as sql_sum,
                                   lag,
                                   unix_timestamp,
                                   from_unixtime,
                                   datediff,
                                   desc,
                                   countDistinct,
                                   asc,
                                   row_number,
                                   upper,
                                   concat,
                                   lpad,
                                   trim,
                                   split,
                                    length,
                                   regexp_replace
                                   )

from pyspark.sql.window import Window
import pandas as pd
import datetime as dt
import sys
import itertools
import re
import time

from churn_nrt.src.utils.hdfs_functions import get_partitions_path_range

from pyspark.sql.types import IntegerType


LIST_GNV_HOURS = list(range(0, 24))
LIST_GNV_USAGE_TYPES = ["Chat_Zero", "Maps_Pass", "VideoHD_Pass", "Video_Pass", "MasMegas", "Music_Pass", "Social_Pass", "RegularData"]
LIST_GNV_PERIODS = ["W", "WE"]

LIST_GNV_ROAM_DATA_ZONES = list(range(1, 5))
LIST_GNV_ROAM_HOUR_RANGES = ['morning', 'evening', 'night']
LIST_GNV_ROAM_VOICE_ZONES = list(range(1, 6)) + ['EEUU', 'MARITIMO']

FIRST_LEVEL_TREATMENT = ['BUZON_DE_VOZ', 'COMUNIDAD_YU_VOZ', 'LLAMADAS_VPN', 'NACIONALES', 'VIDELOTELEFONIA', 'TARIFICACION_ADICIONAL_PRESTADOR']
SECOND_LEVEL_TREATMENT = ['MOVILES', 'INTERNACIONALES', 'OTROS_DESTINOS', 'SERVICIOS_GRATUITOS']

COUNTRIES_GNV_INTERNATIONAL = ['OTHER', 'AFGANISTAN', 'ALASKA', 'ALBANIA', 'ALEMANIA', 'ANDORRA', 'ANGOLA', 'ANGUILLA', 'ANTIGUA_Y_BARBUDA', 'ANTILLAS_HOLANDESAS', 'ARABIA_SAUDITA', 'ARGELIA',
                               'ARGENTINA', 'ARMENIA', 'ARUBA', 'AUSTRALIA', 'AUSTRIA', 'AZERBAIYAN', 'BELGICA', 'BAHAMAS', 'BAHREIN', 'BANGLADESH', 'BARBADOS', 'BELICE', 'BENIN', 'BERMUDAS',
                               'BHUTAN', 'BIELORRUSIA', 'BIRMANIA', 'BOLIVIA', 'BOSNIA', 'BOSNIA_MOSTAR', 'BOSNIA_SRPSKE', 'BOTSWANA', 'BRASIL', 'BRUNEI', 'BULGARIA', 'BURKINA_FASO', 'CABO_VERDE',
                               'CAMBOYA', 'CAMERUN', 'CANADA', 'CHAD', 'CHILE', 'CHINA', 'CHIPRE', 'COLOMBIA', 'CONGO', 'COREA_REP', 'COSTA_DE_MARFIL', 'COSTA_RICA', 'CROACIA', 'CUBA', 'DINAMARCA',
                               'DOMINICANA_REP', 'ECUADOR', 'ECUADOR__PORTA', 'EEUU', 'EGIPTO', 'EL_SALVADOR', 'EMIRATOS_ARABES_UNIDOS', 'EMIRATOS_ARABES_UNIDOS_MOVIL', 'ESLOVENIA', 'ESTONIA',
                               'ETIOPIA', 'FILIPINAS', 'FINLANDIA', 'FIYI', 'FRANCIA', 'GABON', 'GAMBIA', 'GEORGIA', 'GHANA', 'GIBRALTAR', 'GRECIA', 'GUADALUPE', 'GUAM', 'GUATEMALA', 'GUAYANA',
                               'GUAYANA_FRANCESA', 'GUINEA', 'GUINEA_ECUATORIAL', 'GUINEABISSAU', 'HAITI', 'HAWAI', 'HONDURAS', 'HONG_KONG', 'HUNGRIA', 'I_VIRGENES_AMERICANAS',
                               'I_VIRGENES_BRITANICAS', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'IRLANDA', 'ISLA_NIUE', 'ISLANDIA', 'ISLAS_CAIMANES', 'ISLAS_COOK', 'ISLAS_FEROE', 'ISLAS_MALVINAS',
                               'ISLAS_MARSHALL', 'ISLAS_SALOMON', 'ISRAEL', 'ITALIA', 'JAMAICA', 'JAPON', 'JORDANIA', 'KAZAJASTAN', 'KENYA', 'KIRGUIZISTAN', 'KOSOVO', 'KUWAIT', 'LIBANO', 'LAOS',
                               'LESOTHO', 'LETONIA', 'LIBERIA', 'LIBIA', 'LIECHTENSTEIN', 'LITUANIA', 'LUXEMBURGO', 'MEJICO', 'MONACO', 'MACAO', 'MACEDONIA', 'MADAGASCAR', 'MALI', 'MALASIA',
                               'MALDIVAS', 'MALTA', 'MARRUECOS', 'MARTINICA', 'MAURICIO', 'MAURITANIA', 'MAYOTTE', 'MOLDAVIA', 'MONGOLIA', 'MONTENEGRO', 'MOZAMBIQUE', 'NAMIBIA', 'NEPAL', 'NICARAGUA',
                               'NIGER', 'NIGERIA', 'NORUEGA', 'NUEVA_CALEDONIA', 'NUEVA_ZELANDA', 'OMAN', 'PAISES_BAJOS', 'PAKISTAN', 'PALAU', 'PALESTINA', 'PANAMA', 'PAPUA_NUEVA_GUINEA', 'PARAGUAY',
                               'PERU', 'POLINESIA_FRANCESA', 'POLONIA', 'PORTUGAL', 'PUERTO_RICO', 'QATAR', 'REINO_UNIDO', 'REPUBLICA_CHECA', 'REPUBLICA_ESLOVACA', 'REPUBLICA_CENTROAFRICANA',
                               'REUNION', 'RUANDA', 'RUMANIA', 'RUSIA', 'SAN_MARINO', 'SAN_MARTIN', 'SAN_PEDRO_Y_MIQUELON', 'SAN_VICENTE_Y_GRANADINAS', 'SANTO_TOME_Y_PRINCIPE_MOVIL', 'SENEGAL',
                               'SERBIA', 'SEYCHELLES', 'SIERRA_LEONA', 'SINGAPUR', 'SIRIA', 'SOMALIA', 'SRI_LANKA', 'STA_LUCIA', 'SUDAN', 'SUDAFRICA', 'SUECIA', 'SUIZA', 'SURINAM', 'SWAZILANDIA',
                               'TUNEZ', 'TADJIKISTAN', 'TAILANDIA', 'TAIWAN', 'TANZANIA', 'TIMOR_ORIENTAL', 'TOGO', 'TRINIDAD_Y_TOBAGO', 'TURKMENISTAN', 'TURQUIA', 'UCRANIA', 'UGANDA', 'URUGUAY',
                               'UZBEKISTAN', 'VENEZUELA', 'VIETNAM', 'YEMEN', 'ZAIRE', 'ZAMBIA', 'ZIMBABWE']
PROVIDERS_GNV_MOBILE = ['OTHER', 'AIRE', 'BARABLU', 'BT_ESPANA', 'CARREFOUR', 'DIA', 'DIGI_SPAIN', 'EROSKI', 'EUSKALTEL', 'HAPPY_MOVIL', 'HITS_TELECOM', 'INGENIUM', 'JAZZTEL', 'KPN_SPAIN',
                        'LCR_TELECOM', 'LEBARA', 'LYCAMOBILE', 'MAS_MOVIL', 'MORE_MINUTES', 'NEOSKY', 'ORANGE', 'ORBITEL', 'PEPEPHONE', 'PROCONO', 'R', 'R_CABLE', 'TELECABLE', 'TELEFONICA', 'TUENTI',
                        'VODAFONE', 'VODAFONE_ENABLER', 'YOIGO', 'YOU_MOBILE']
SERVICES_GNV_FREE = ['OTHER', 'AMENA', 'ASESOR_COMERCIAL_EMPRESAS', 'ASISTENCIA_TECNICA', 'AT_COMERCIAL', 'AT_EMPRESAS', 'ATENCION_AL_CLIENTE', 'ATT_AL_CLIENTE', 'BOMBEROS_PROV', 'COMUNITEL',
                     'CRUZ_ROJA', 'EMERGENCIA', 'ESPECIAL', 'ESPECIAL_ATENCION_AL_CLIENTE', 'EUSKALTEL_SA', 'GRUPO_VODAFONE', 'INF_MOVISTAR', 'INF_UNI2', 'INFOOFCOMTEL', 'INFORMACION_ORANGE_1515',
                     'JAZZTEL', 'LINEA_800', 'LINEA_800_INTERNACIONAL', 'LINEA_800_VODAFONE', 'LINEA_900', 'LINEA_900_CONFIDENCIAL', 'LINEA_900_N1', 'LINEA_900_VODAFONE', 'R_GALICIA',
                     'SERVICIO_DE_INF_Y_VENTAS', 'TELE2', 'TELEFONICA', 'TFNO_INFORMACION_GRATUITO', 'TONOS_DE_ESPERA', 'VALENCIA_CABLE', 'XTRA_TELECOM_SA']
SERVICE_GNV_OTHER = ['OTHER', 'ADM_GENERAL_ESTADO', 'ATENCION_CIUDADANA', 'ATT_MUNICIPAL', 'BOMBEROS_LOCAL', 'GUARDIA_CIVIL', 'INF_AUTONOMICA', 'INFORMACION_AVYS_TELECOM', 'INFORMACION_EUSKALTEL_SA',
                     'INFORMACION_HELLO_TV', 'LINEA_901', 'LINEA_902', 'LINEA_902_N1', 'LINEA_902_VODAFONE', 'LINEA_905_SOPORTE', 'POLICIA', 'POLICMUNICIPAL', 'URGENC_INSALUD']

OPERATORS = ['JAZZTEL', 'NEOSKY', 'CARREFOUR', 'MORE_MINUTES', 'HITS_TELECOM', 'DIA', 'YOIGO', 'LCR_TELECOM', 'TUENTI', 'TELECABLE', 'ORBITEL', 'DIGI_SPAIN', 'MAS_MOVIL',
             'BT_ESPANA', 'EUSKALTEL', 'INGENIUM', 'HAPPY_MOVIL', 'TELEFONICA', 'PEPEPHONE', 'R_CABLE', 'ORANGE', 'LYCAMOBILE', 'KPN_SPAIN', 'EROSKI', 'YOU_MOBILE',
             'OTHER', 'LEBARA', 'AIRE', 'BARABLU', 'VODAFONE_ENABLER', 'R', 'PROCONO', 'VODAFONE']
VF_OPERATORS = ['VODAFONE', 'VODAFONE_ENABLER']

COMPETITORS = list(set(OPERATORS) - set(VF_OPERATORS))

PREFFIX_COLS = "gnv"

path_raw_base = '/data/raw/vf_es/'
PATH_RAW_GENEVA_TRAFFIC = path_raw_base + 'billingtopsups/TRAFFICTARIFOW/1.0/parquet/'

def get_last_date(spark):

    # hardcoded to avoid py4j.protocol.Py4JJavaError: An error occurred while calling o166.load.: java.lang.OutOfMemoryError: GC
    # overhead limit exceeded

    gnv_last_date = (spark.read.load(PATH_RAW_GENEVA_TRAFFIC+"year=2020/").withColumn("year", lit(2020))
                          .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
                          .select(sql_max(col('mydate')).alias('last_date'))
                         .rdd.first()['last_date'])

    return gnv_last_date


def process_one_day(spark, path_, closing_day):


    from churn_nrt.src.data.customer_base import CustomerBase

    df_base = CustomerBase(spark).get_module(closing_day, save=True, save_others=False)

    start_time = time.time()

    print("CSANC109 DEBUG path={}".format(path_))

    data_usage_gnv_ori_voice = spark.read.load(path_)

    data_usage_gnv_voice_tmp1 = (data_usage_gnv_ori_voice.withColumnRenamed('event_attr_1_msisdn', 'msisdn').join(df_base, on=["msisdn"], how="inner")
                                 .withColumn('Duration',
                                            when(col('event_type_id') == 1, col('event_attr_3').cast('integer'))
                                            .when(col('event_type_id') == 13, col('event_attr_13').cast('integer'))
                                            .when(col('event_type_id') == 18, col('event_attr_16').cast('integer'))
                                            .otherwise(0))
                                .withColumn('Cost_Band', when(col('event_type_id') == 1,                                                                                                                                                      col('event_attr_8').cast('integer')).when(
        col('event_type_id') == 13, col('event_attr_13').cast('integer')).when(col('event_type_id') == 18, col('event_attr_6').cast('integer')).otherwise(0))
                                 .withColumn('usage_type', when(col('event_type_id') == 1,  col('event_attr_9'))
                                                          .when(col('event_type_id') == 13, lit(None))
                                                          .when(col('event_type_id') == 18, col('event_attr_10'))
                                                          .otherwise(None))
                                 .withColumn('usage_type_detail', upper(when(col('event_type_id') == 1, col('event_attr_10'))
                                                                        .when(col('event_type_id') == 13, lit(None))
                                                                        .when(col('event_type_id') == 18, col('event_attr_17'))
                                                                        .otherwise(None)))# cleansing_v2
                                 .withColumn('usage_type_clean', regexp_replace(upper(col('usage_type')), 'Ó|Ã³|ÃÂ³', 'O'))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Í|Ã­|ÃÂ­', 'I'))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'É|Ã©|ÃÂ©', 'E'))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Ú|Ãº|ÃÂº', 'U'))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Á|Ã¡|ÃÂ¡', 'A'))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), '\.|,|\-|\(|\)', ''))
                                 .withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Ñ', 'N'))
                                 .withColumn('usage_type_clean', regexp_replace(trim(col('usage_type_clean')), ' ', '_'))
                                 .withColumn('usage_type_clean', regexp_replace(trim(col('usage_type_clean')), 'VFONO_PROVINCIALES|NACIONAL_VF_CASA|PROVINCIALES', 'NACIONALES'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(upper(col('usage_type_detail')), 'Ó|Ã³|ÃÂ³', 'O'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Í|Ã­|ÃÂ­', 'I'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'É|Ã©|ÃÂ©', 'E'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Ú|Ãº|ÃÂº', 'U'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Ã±|ÃÂ±', 'Ñ'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Á|Ã¡|ÃÂ¡|ÃÂ|Ã', 'A'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'PRESTADOR|\(\)|MOVILES|\.|\-|,', ''))
                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Ñ', 'N'))
                                 .withColumn('usage_type_detail_clean', regexp_replace(trim(col('usage_type_detail_clean')), ' ', '_'))
                                 .withColumn('row_tag', when((col('usage_type_clean') == 'INTERNACIONALES') & (col('usage_type_detail_clean').isin(COUNTRIES_GNV_INTERNATIONAL)),
                                                             concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean')))
                                             .when((col('usage_type_clean') == 'INTERNACIONALES') & (~col('usage_type_detail_clean').isin(COUNTRIES_GNV_INTERNATIONAL)), concat(col('usage_type_clean'), lit('_OTHER')))
                                             .when((col('usage_type_clean') == 'MOVILES') & (col('usage_type_detail_clean').isin(PROVIDERS_GNV_MOBILE)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean')))
                                             .when((col('usage_type_clean') == 'MOVILES') & (~col('usage_type_detail_clean').isin(PROVIDERS_GNV_MOBILE)), concat(col('usage_type_clean'), lit('_OTHER')))
                                             .when((col('usage_type_clean') == 'OTROS_DESTINOS') & (col('usage_type_detail_clean').isin(SERVICE_GNV_OTHER)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean')))
                                             .when((col('usage_type_clean') == 'OTROS_DESTINOS') & (~col('usage_type_detail_clean').isin(SERVICE_GNV_OTHER)), concat(col('usage_type_clean'), lit('_OTHER')))
                                             .when((col('usage_type_clean') == 'SERVICIOS_GRATUITOS') & (col('usage_type_detail_clean').isin(SERVICES_GNV_FREE)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean')))
                                             .when((col('usage_type_clean') == 'SERVICIOS_GRATUITOS') & (~col('usage_type_detail_clean').isin(SERVICES_GNV_FREE)), concat(col('usage_type_clean'), lit('_OTHER')))
                                             .when(col('usage_type_clean').isin(FIRST_LEVEL_TREATMENT), col('usage_type_clean')).otherwise(None)).where(col('row_tag').isNotNull())
                                 .withColumn('row_tag', concat(lit('GNV_Type_Voice_'), col('row_tag')))
                                 .select('msisdn', 'event_attr_2_msisdn', 'event_dtm', 'duration', 'usage_type_clean', 'usage_type_detail_clean', 'row_tag').dropDuplicates())
    # TODO change this for the list SECOND_LEVEL_TREATMENT
    VALUES_TO_PIVOT = ['GNV_Type_Voice_{p}'.format(p=p) for p in FIRST_LEVEL_TREATMENT]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_INTERNACIONALES_{p}".format(p=p) for p in COUNTRIES_GNV_INTERNATIONAL]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_MOVILES_{p}".format(p=p) for p in PROVIDERS_GNV_MOBILE]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_SERVICIOS_GRATUITOS_{p}".format(p=p) for p in SERVICES_GNV_FREE]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_OTROS_DESTINOS_{p}".format(p=p) for p in SERVICE_GNV_OTHER]

    data_usage_gnv_voice_tmp2 = (
        data_usage_gnv_voice_tmp1.groupBy('msisdn').pivot('row_tag', VALUES_TO_PIVOT).agg((sql_sum("Duration") / 60.00).alias("MOU"), sql_count(lit(1)).alias("Num_Of_Calls")).na.fill(0))

    print("CSANC109 DEBUG process_one_day (elapsed time {} minutes".format((time.time()-start_time)/60.0))


    return data_usage_gnv_voice_tmp2


def get_geneva_df(spark, starting_day, closing_day):

    print("CSANC109 DEBUG get_geneva_df {} {}".format(starting_day, closing_day))
    # https://gitlab.rat.bdp.vodafone.com/bda-es/amdocs_inf_dataset/blob/stable_ids/src/main/python/pipelines/geneva_traffic.py
    geneva_traffic_paths = get_partitions_path_range(PATH_RAW_GENEVA_TRAFFIC, starting_day, closing_day)

    parts_ = [process_one_day(spark, path_, closing_day) for path_ in geneva_traffic_paths]

    print("CSANC109 finished all process_one_day")

    aggs_list = [sql_sum(col_).alias(col_) for col_ in parts_[0].columns if col_!="msisdn"]
    data_usage_gnv_ori_voice = reduce(lambda a, b: a.union(b).groupBy("msisdn").agg(*aggs_list), parts_)

    print("CSANC109 finished all groupBy+aggs")
    print("CSANC109 DEBUG - starting cache at {}. It takes about 200 minutes...".format(dt.datetime.now().strftime("%Y%m%d_%H%M%S")))

    #import time
    #start_time = time.time()
    #data_usage_gnv_ori_voice = data_usage_gnv_ori_voice.cache()
    #print("CSANC109 DEBUG count={}".format(data_usage_gnv_ori_voice.count()))
    #print("CSANC109 DEBUG count after groupby={} (elapsed time {} minutes".format(data_usage_gnv_ori_voice.count(), (time.time()-start_time)/60.0))


    na_map = __set_module_metadata()
    #     df_table = Metadata().apply_metadata_dict(data_usage_gnv_voice_tmp2, null_map)
    final_map = {colmn: na_map[colmn][0] for colmn in data_usage_gnv_ori_voice.columns if colmn in na_map.keys() and na_map[colmn][1] != "id"}
    df_table = data_usage_gnv_ori_voice.fillna(final_map)

    df_table = df_table.withColumn("service_processed_at", lit(dt.datetime.now()).cast("timestamp"))

    return df_table


def __set_module_metadata():
    cols = ['{p}'.format(p=p) for p in FIRST_LEVEL_TREATMENT]
    cols += ["INTERNACIONALES_{p}".format(p=p) for p in COUNTRIES_GNV_INTERNATIONAL]
    cols += ["MOVILES_{p}".format(p=p) for p in PROVIDERS_GNV_MOBILE]
    cols += ["SERVICIOS_GRATUITOS_{p}".format(p=p) for p in SERVICES_GNV_FREE]
    cols += ["OTROS_DESTINOS_{p}".format(p=p) for p in SERVICE_GNV_OTHER]

    fill_zero = ["GNV_Type_Voice_{}_{}".format(clmn, pivot) for pivot in ["MOU", "Num_Of_Calls"] for clmn in cols]
    module = "gnv_type"
    na_map = dict([(k, ("", "id", module,)) for k in ["msisdn"]] + [(k, (0, "numerical", module,)) for k in fill_zero])

    return na_map


def compute_l2_attrs(df):
    cols = [col_ for col_ in df.columns if col_.startswith("GNV_Type_Voice_MOVILES_") or col_ in ["msisdn", "formatted_date"]]
    df = df.select(cols)

    df = df.withColumn("num_calls_competitors", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op) for op in COMPETITORS]))
    df = df.withColumn("num_calls_vf", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op) for op in VF_OPERATORS]))
    df = df.withColumn("mou_competitors", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_MOU".format(op) for op in COMPETITORS]))
    df = df.withColumn("mou_vf", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_MOU".format(op) for op in VF_OPERATORS]))

    df_agg = (df.groupby("msisdn").agg(*(
                [sql_sum("num_calls_competitors").alias('total_calls_competitors'), sql_sum("num_calls_vf").alias('total_calls_vf'), sql_sum("mou_competitors").alias('total_mou_competitors'),
                 sql_sum("mou_vf").alias('total_mou_vf')] +
                [sql_sum(col("GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op))).alias("num_calls_{}".format(op)) for op in OPERATORS] + [
                    sql_sum(col("GNV_Type_Voice_MOVILES_{}_MOU".format(op))).alias("mou_{}".format(op)) for op in OPERATORS])).withColumn("num_distinct_comps", reduce(lambda a, b: a + b, [
        when(col("num_calls_{}".format(op)) > 0, 1.0).otherwise(0.0) for op in COMPETITORS])))

    df_agg = df_agg.withColumn("total_calls", col('total_calls_competitors') + col('total_calls_vf'))
    df_agg = df_agg.withColumn("total_mou", col('total_mou_competitors') + col('total_mou_vf'))

    df_agg = df_agg.withColumn("perc_calls_competitors", when(col("total_calls") > 0, col('total_calls_competitors') / col("total_calls")).otherwise(-1))
    df_agg = df_agg.withColumn("perc_mou_competitors", when(col("total_mou") > 0, col('total_mou_competitors') / col("total_mou")).otherwise(-1))

    df_agg = df_agg.select(
        *(df_agg.columns + [when(col("total_calls") > 0, (col('num_calls_{}'.format(op)) / col("total_calls"))).otherwise(-1).alias("perc_calls_{}".format(op)) for op in OPERATORS]))

    return df_agg

class GenevaData(DataTemplate):

    INCREMENTAL_PERIOD = None #days


    def __init__(self, spark, incremental_period):
        self.INCREMENTAL_PERIOD = incremental_period

        DataTemplate.__init__(self, spark, "geneva/{}".format(self.INCREMENTAL_PERIOD))

    def build_module(self, closing_day, save_others, **kwargs):

        starting_day = move_date_n_days(closing_day, n=-self.INCREMENTAL_PERIOD)
        print("[GenevaData] build_module | starting_day={} closing_day={}".format(starting_day, closing_day))

        df = get_geneva_df(self.SPARK, starting_day, closing_day)
        df_agg = compute_l2_attrs(df)
        df_agg = df_agg.cache()
        print("CSANC109 DEBUG df_agg count ={}".format(df_agg.count()))

        new_cols = [PREFFIX_COLS + "_" + col_ if col_ not in ["msisdn"] else col_ for col_ in df_agg.columns]
        df_agg = df_agg.toDF(*new_cols)

        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_agg = apply_metadata(self.get_metadata(), df_agg)

        return df_agg

    def get_metadata(self):

        FIXED_NAMES = ['total_calls_competitors', 'total_calls_vf', 'total_mou_competitors', 'total_mou_vf', "num_distinct_comps", "total_calls", "total_mou", "perc_calls_competitors",
                       "perc_mou_competitors"]

        # FIXED_D = ['inc_num_distinct_comps_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_rel_num_distinct_comps_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_perc_calls_competitors_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_perc_mou_competitors_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_rel_perc_calls_competitors_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_rel_total_calls_competitors_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_rel_total_mou_competitors_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_total_calls_competitors_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_total_mou_vf_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_rel_perc_mou_competitors_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_rel_total_calls_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_rel_total_calls_vf_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_rel_total_mou_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_total_calls_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_total_calls_vf_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_total_mou_competitors_d{}'.format(self.INCREMENTAL_PERIOD),
        #            'inc_total_mou_d{}'.format(self.INCREMENTAL_PERIOD), 'inc_rel_total_mou_vf_d{}'.format(self.INCREMENTAL_PERIOD)]

        NUM_CALLS_OPS = ["num_calls_{}".format(op) for op in OPERATORS]
        MOU_OPS = ["mou_{}".format(op) for op in OPERATORS]
        PERC_OPS = ["perc_calls_{}".format(op) for op in OPERATORS]
        #INC_OPS = ["inc_{}_{}_d{}".format(mm, op, self.INCREMENTAL_PERIOD) for op in OPERATORS for mm in ["num_calls", "mou"]]
        #INC_PERC_OPS = ["inc_perc_{}_{}_d{}".format(mm, op, self.INCREMENTAL_PERIOD) for op in OPERATORS for mm in ["calls"]]
        #INC_REL_OPS = ["inc_rel_{}_{}_d{}".format(mm, op, self.INCREMENTAL_PERIOD) for op in OPERATORS for mm in (["num_calls", "mou", "perc_calls"])]

        all_cols = FIXED_NAMES + NUM_CALLS_OPS + MOU_OPS + PERC_OPS# + INC_OPS + INC_PERC_OPS + INC_REL_OPS + FIXED_D
        all_cols = [PREFFIX_COLS + "_" + col_ for col_ in all_cols]

        na_vals = [str(-1) if col_.startswith("perc_") else str(0) for col_ in all_cols]

        cat_feats = []
        data = {'feature': all_cols, 'imp_value': na_vals}

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data)).withColumn('source', lit("geneva"))
                       .withColumn('type', lit('numeric'))
                       .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type')))
                       .withColumn('level', lit('msisdn')))

        return metadata_df


##### FORMA DE HACER AGREGADOS
#def get_aggregates_geneva(spark, date_) # devuleve DF con una fila por msisdn y una columna para cada agregado
#dates_ = lista de días
#lista_de_dfs = [get_aggregates_geneva(spark, date_)  for d in dates_]
#lista_de_dfs.reduce(x, y: x.union(y).groupBy(msisdn).agg(sql_sum(agregado).alias(agregado)))