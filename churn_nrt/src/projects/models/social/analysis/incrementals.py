#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import time
from pyspark.sql import Row, DataFrame, Column, Window
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import collect_set, concat, size, coalesce, col, trim, lpad, struct, upper, lower, avg as sql_avg, count as sql_count, lag, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum, length, concat_ws, regexp_replace, split


from pyspark.sql.functions import (
                                    col,
#                                    when,
#                                    lit,
#                                    lower,
#                                    count,
#                                    sum as sql_sum,
                                     avg as sql_avg,
#                                    count as sql_count,
#                                    desc,
#                                    asc,
#                                    row_number,
#                                    upper,
#                                    trim
                                    )
# from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# import datetime as dt
# from pyspark.sql import Row, DataFrame, Column, Window
# from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, ArrayType
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import concat, size, coalesce, col, lpad, struct, count as sql_count, lag, lit, min as sql_min, max as sql_max, collect_list, udf, when, desc, asc, to_date, create_map, sum as sql_sum, length, concat_ws, regexp_replace, split
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, StructField, IntegerType
# from pyspark.sql.functions import array, regexp_extract
# from itertools import chain
# from churn.datapreparation.general.data_loader import get_unlabeled_car, get_port_requests_table, get_numclients_under_analysis
# from churn.utils.constants import PORT_TABLE_NAME
# from churn.utils.udf_manager import Funct_to_UDF
# from pyspark.sql.functions import substring, datediff, row_number


#spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=4 --conf spark.dynamicAllocation.maxExecutors=20 --executor-cores 4 --executor-memory 16G --driver-memory 10G --conf spark.driver.maxResultSize=32G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=8096 /var/SP/data/home/csanc109/src/devel/use-cases/churn_nrt/src/projects/models/social/analysis/incrementals.py --day 20191231 --day2 20190930 --days_incremental 90 > /var/SP/data/home/csanc109/logging/social_incremental_20191231_20190930_90.log




import datetime as dt
# from pyspark.sql.functions import from_unixtime,unix_timestamp


import logging
logging.getLogger('py4j').setLevel('ERROR')
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

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


def get_partitions_path_range(path, start, end):
    """
    Returns a list of complete paths with data to read of the source between two dates
    :param path: string path
    :param start: string date start
    :param end: string date end
    :return: list of paths
    """
    star_date = dt.datetime.strptime(start, '%Y%m%d')
    delta = dt.datetime.strptime(end, '%Y%m%d') - dt.datetime.strptime(start, '%Y%m%d')
    days_list = [star_date + dt.timedelta(days=i) for i in range(delta.days + 1)]
    return [path + "year={}/month={}/day={}".format(d.year, d.month, d.day) for d in days_list]


path_raw_base = '/data/raw/vf_es/'
PATH_RAW_GENEVA_TRAFFIC = path_raw_base + 'billingtopsups/TRAFFICTARIFOW/1.0/parquet/'


def get_geneva_df(spark, starting_day, closing_day):
    # https://gitlab.rat.bdp.vodafone.com/bda-es/amdocs_inf_dataset/blob/stable_ids/src/main/python/pipelines/geneva_traffic.py
    geneva_traffic_paths = get_partitions_path_range(PATH_RAW_GENEVA_TRAFFIC, starting_day, closing_day)
    print(geneva_traffic_paths)
    data_usage_gnv_ori_voice = (spark.read.load(geneva_traffic_paths).where((col('event_type_id').isin(1, 13, 18))).drop(*['service_processed_at', 'service_file_id']))

    data_usage_gnv_ori_voice = data_usage_gnv_ori_voice.cache()
    data_usage_gnv_voice_tmp1 = (data_usage_gnv_ori_voice.withColumnRenamed('event_attr_1_msisdn', 'msisdn').withColumn('Duration',
                                                                                                                        when(col('event_type_id') == 1, col('event_attr_3').cast('integer')).when(
                                                                                                                            col('event_type_id') == 13, col('event_attr_13').cast('integer')).when(
                                                                                                                            col('event_type_id') == 18, col('event_attr_16').cast('integer')).otherwise(
                                                                                                                            0)).withColumn('Cost_Band', when(col('event_type_id') == 1,
                                                                                                                                                             col('event_attr_8').cast('integer')).when(
        col('event_type_id') == 13, col('event_attr_13').cast('integer')).when(col('event_type_id') == 18, col('event_attr_6').cast('integer')).otherwise(0)).withColumn('usage_type',
                                                                                                                                                                         when(col('event_type_id') == 1,
                                                                                                                                                                              col('event_attr_9')).when(
                                                                                                                                                                             col('event_type_id') == 13,
                                                                                                                                                                             lit(None)).when(
                                                                                                                                                                             col('event_type_id') == 18,
                                                                                                                                                                             col(
                                                                                                                                                                                 'event_attr_10')).otherwise(
                                                                                                                                                                             None)).withColumn(
        'usage_type_detail',
        upper(when(col('event_type_id') == 1, col('event_attr_10')).when(col('event_type_id') == 13, lit(None)).when(col('event_type_id') == 18, col('event_attr_17')).otherwise(None)))# cleansing_v2
                                 .withColumn('usage_type_clean', regexp_replace(upper(col('usage_type')), 'Ó|Ã³|ÃÂ³', 'O')).withColumn('usage_type_clean',
                                                                                                                                        regexp_replace(col('usage_type_clean'), 'Í|Ã­|ÃÂ­',
                                                                                                                                                       'I')).withColumn('usage_type_clean',
                                                                                                                                                                        regexp_replace(
                                                                                                                                                                            col('usage_type_clean'),
                                                                                                                                                                            'É|Ã©|ÃÂ©',
                                                                                                                                                                            'E')).withColumn(
        'usage_type_clean', regexp_replace(col('usage_type_clean'), 'Ú|Ãº|ÃÂº', 'U')).withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Á|Ã¡|ÃÂ¡', 'A')).withColumn(
        'usage_type_clean', regexp_replace(col('usage_type_clean'), '\.|,|\-|\(|\)', '')).withColumn('usage_type_clean', regexp_replace(col('usage_type_clean'), 'Ñ', 'N')).withColumn(
        'usage_type_clean', regexp_replace(trim(col('usage_type_clean')), ' ', '_')).withColumn('usage_type_clean',
                                                                                                regexp_replace(trim(col('usage_type_clean')), 'VFONO_PROVINCIALES|NACIONAL_VF_CASA|PROVINCIALES',
                                                                                                               'NACIONALES'))

                                 .withColumn('usage_type_detail_clean', regexp_replace(upper(col('usage_type_detail')), 'Ó|Ã³|ÃÂ³', 'O')).withColumn('usage_type_detail_clean',
                                                                                                                                                      regexp_replace(col('usage_type_detail_clean'),
                                                                                                                                                                     'Í|Ã­|ÃÂ­', 'I')).withColumn(
        'usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'É|Ã©|ÃÂ©', 'E')).withColumn('usage_type_detail_clean',
                                                                                                                regexp_replace(col('usage_type_detail_clean'), 'Ú|Ãº|ÃÂº', 'U')).withColumn(
        'usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'Ã±|ÃÂ±', 'Ñ')).withColumn('usage_type_detail_clean',
                                                                                                              regexp_replace(col('usage_type_detail_clean'), 'Á|Ã¡|ÃÂ¡|ÃÂ|Ã', 'A'))

                                 .withColumn('usage_type_detail_clean', regexp_replace(col('usage_type_detail_clean'), 'PRESTADOR|\(\)|MOVILES|\.|\-|,', '')).withColumn('usage_type_detail_clean',
                                                                                                                                                                         regexp_replace(col(
                                                                                                                                                                             'usage_type_detail_clean'),
                                                                                                                                                                                        'Ñ',
                                                                                                                                                                                        'N')).withColumn(
        'usage_type_detail_clean', regexp_replace(trim(col('usage_type_detail_clean')), ' ', '_'))

                                 .withColumn('row_tag', when((col('usage_type_clean') == 'INTERNACIONALES') & (col('usage_type_detail_clean').isin(COUNTRIES_GNV_INTERNATIONAL)),
                                                             concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean'))).when(
        (col('usage_type_clean') == 'INTERNACIONALES') & (~col('usage_type_detail_clean').isin(COUNTRIES_GNV_INTERNATIONAL)), concat(col('usage_type_clean'), lit('_OTHER'))).when(
        (col('usage_type_clean') == 'MOVILES') & (col('usage_type_detail_clean').isin(PROVIDERS_GNV_MOBILE)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean'))).when(
        (col('usage_type_clean') == 'MOVILES') & (~col('usage_type_detail_clean').isin(PROVIDERS_GNV_MOBILE)), concat(col('usage_type_clean'), lit('_OTHER'))).when(
        (col('usage_type_clean') == 'OTROS_DESTINOS') & (col('usage_type_detail_clean').isin(SERVICE_GNV_OTHER)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean'))).when(
        (col('usage_type_clean') == 'OTROS_DESTINOS') & (~col('usage_type_detail_clean').isin(SERVICE_GNV_OTHER)), concat(col('usage_type_clean'), lit('_OTHER'))).when(
        (col('usage_type_clean') == 'SERVICIOS_GRATUITOS') & (col('usage_type_detail_clean').isin(SERVICES_GNV_FREE)), concat(col('usage_type_clean'), lit('_'), col('usage_type_detail_clean'))).when(
        (col('usage_type_clean') == 'SERVICIOS_GRATUITOS') & (~col('usage_type_detail_clean').isin(SERVICES_GNV_FREE)), concat(col('usage_type_clean'), lit('_OTHER'))).when(
        col('usage_type_clean').isin(FIRST_LEVEL_TREATMENT), col('usage_type_clean')).otherwise(None)).where(col('row_tag').isNotNull()).withColumn('row_tag',
                                                                                                                                                    concat(lit('GNV_Type_Voice_'), col('row_tag')))

                                 .select('msisdn', 'event_attr_2_msisdn', 'event_dtm', 'duration', 'usage_type_clean', 'usage_type_detail_clean', 'row_tag').dropDuplicates())
    # TODO change this for the list SECOND_LEVEL_TREATMENT
    VALUES_TO_PIVOT = ['GNV_Type_Voice_{p}'.format(p=p) for p in FIRST_LEVEL_TREATMENT]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_INTERNACIONALES_{p}".format(p=p) for p in COUNTRIES_GNV_INTERNATIONAL]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_MOVILES_{p}".format(p=p) for p in PROVIDERS_GNV_MOBILE]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_SERVICIOS_GRATUITOS_{p}".format(p=p) for p in SERVICES_GNV_FREE]
    VALUES_TO_PIVOT += ["GNV_Type_Voice_OTROS_DESTINOS_{p}".format(p=p) for p in SERVICE_GNV_OTHER]

    data_usage_gnv_voice_tmp2 = (
        data_usage_gnv_voice_tmp1.groupBy('msisdn').pivot('row_tag', VALUES_TO_PIVOT).agg((sql_sum("Duration") / 60.00).alias("MOU"), sql_count(lit(1)).alias("Num_Of_Calls")).na.fill(0))
    print("Ended - Calculating Voice Usage Type Information")

    na_map = set_module_metadata()
    #     df_table = Metadata().apply_metadata_dict(data_usage_gnv_voice_tmp2, null_map)
    final_map = {colmn: na_map[colmn][0] for colmn in data_usage_gnv_voice_tmp2.columns if colmn in na_map.keys() and na_map[colmn][1] != "id"}
    df_table = data_usage_gnv_voice_tmp2.fillna(final_map)

    df_table = df_table.withColumn("service_processed_at", lit(dt.datetime.now()).cast("timestamp"))

    return df_table


def set_module_metadata():
    cols = ['{p}'.format(p=p) for p in FIRST_LEVEL_TREATMENT]
    cols += ["INTERNACIONALES_{p}".format(p=p) for p in COUNTRIES_GNV_INTERNATIONAL]
    cols += ["MOVILES_{p}".format(p=p) for p in PROVIDERS_GNV_MOBILE]
    cols += ["SERVICIOS_GRATUITOS_{p}".format(p=p) for p in SERVICES_GNV_FREE]
    cols += ["OTROS_DESTINOS_{p}".format(p=p) for p in SERVICE_GNV_OTHER]

    fill_zero = ["GNV_Type_Voice_{}_{}".format(clmn, pivot) for pivot in ["MOU", "Num_Of_Calls"] for clmn in cols]
    module = "gnv_type"
    na_map = dict([(k, ("", "id", module,)) for k in ["msisdn"]] + [(k, (0, "numerical", module,)) for k in fill_zero])

    return na_map


def set_paths():
    import os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn_nrt(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))



def get_geneva_feats(spark, starting_day, closing_day):
        df = get_geneva_df(spark, starting_day, closing_day)

        cols = [col_ for col_ in df.columns if col_.startswith("GNV_Type_Voice_MOVILES_") or col_ in ["msisdn", "formatted_date"]]
        df = df.select(cols)
        operators = list(set([col_.replace("GNV_Type_Voice_MOVILES_", "").replace("_Num_Of_Calls", "").replace("_MOU", "") for col_ in cols if col_ not in ["msisdn", "formatted_date"]]))

        other_operators = list(set(operators) - set(['VODAFONE', 'VODAFONE_ENABLER']))
        vf_operators = ['VODAFONE', 'VODAFONE_ENABLER']

        from churn_nrt.src.utils.pyspark_utils import sum_horizontal
        df = df.withColumn("num_calls_other_operators", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op) for op in other_operators]))
        df = df.withColumn("num_calls_vf", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op) for op in vf_operators]))
        df = df.withColumn("mou_other_operators", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_MOU".format(op) for op in other_operators]))
        df = df.withColumn("mou_vf", sum_horizontal(["GNV_Type_Voice_MOVILES_{}_MOU".format(op) for op in vf_operators]))

        df_agg = (df.groupby("msisdn").agg(*([sql_sum("num_calls_other_operators").alias('total_calls_other_operator'), sql_sum("num_calls_vf").alias('total_calls_vf'),
                                              sql_sum("mou_other_operators").alias('total_mou_other_operator'), sql_sum("mou_vf").alias('total_mou_vf')] + [
                                              sql_sum(col("GNV_Type_Voice_MOVILES_{}_Num_Of_Calls".format(op))).alias("num_calls_{}".format(op)) for op in other_operators] +
                                             [sql_sum(col("GNV_Type_Voice_MOVILES_{}_MOU".format(op))).alias("mou_{}".format(op)) for op in other_operators]))
                   .withColumn("num_distinct_comps", reduce(lambda a, b: a + b, [when(col("num_calls_{}".format(op)) > 0, 1.0).otherwise(0.0) for op in other_operators]))
                  )


        df_agg = df_agg.withColumn("total_calls", col('total_calls_other_operator') + col('total_calls_vf'))
        df_agg = df_agg.withColumn("total_mou", col('total_mou_other_operator') + col('total_mou_vf'))

        df_agg = df_agg.withColumn("perc_calls_other_ops", when(col("total_calls") > 0, col('total_calls_other_operator') / col("total_calls")).otherwise(-1))
        df_agg = df_agg.withColumn("perc_mou_other_ops", when(col("total_mou") > 0, col('total_mou_other_operator') / col("total_mou")).otherwise(-1))

        df_agg = df_agg.select(
            *(df_agg.columns + [when(col("total_calls") > 0, (col('num_calls_{}'.format(op)) / col("total_calls"))).otherwise(-1).alias("perc_calls_{}".format(op)) for op in other_operators]))

        return df_agg


if __name__ == "__main__":

    set_paths()

    ##########################################################################################
    # 0. Create Spark context with Spark configuration
    ##########################################################################################



    start_time_total = time.time()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ARGPARSE
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import argparse

    parser = argparse.ArgumentParser(
        description="Run myvf model --tr YYYYMMDD --tt YYYYMMDD [--model rf]",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')

    parser.add_argument('--day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--day2', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    parser.add_argument('--days_incremental', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Date to be used in training')

    args = parser.parse_args()
    print(args)

    closing_day = args.day
    closing_day_2 = args.day2
    days_incremental = int(args.days_incremental)

    from churn_nrt.src.utils.spark_session import get_spark_session_noncommon
    spark, sc = get_spark_session_noncommon("incrementals_social")
    sc.setLogLevel('WARN')

    from churn_nrt.src.utils.date_functions import move_date_n_days
    from churn_nrt.src.utils.hdfs_functions import check_hdfs_exists

    starting_day = move_date_n_days(closing_day, n=-days_incremental)
    starting_day_2 = move_date_n_days(closing_day_2, n=-days_incremental)

    print("RANGE1 - {} to {}".format(starting_day, closing_day))
    print("RANGE2 - {} to {}".format(starting_day_2, closing_day_2))

    path_to_file = "/user/csanc109/projects/churn/data/social/incrementals_df_{}_{}_{}".format(closing_day, closing_day_2, days_incremental)

    if not check_hdfs_exists(path_to_file):

        print("File {} does not exist".format(path_to_file))

        df_agg = get_geneva_feats(spark, starting_day,
                                  closing_day)  # .select("msisdn", "total_calls", "total_mou", "perc_calls_other_ops", "perc_mou_other_ops", 'total_calls_other_operator', 'total_calls_vf')
        df_agg = df_agg.where(col("total_calls") > 0)


        from churn_nrt.src.data.customer_base import CustomerBase
        df_cust_base = CustomerBase(spark).get_module(closing_day, force_gen=True, save=False, save_others=False).where(col("rgu") == "mobile")
        df_agg = df_agg.join(df_cust_base.select("msisdn"), on=["msisdn"], how="inner")

        df_agg = df_agg.cache()
        print("After filter total_calls>0 and inner with base -->  {}".format(df_agg.count()))



        df_agg2 = get_geneva_feats(spark, starting_day_2,
                                   closing_day_2)  # .select("msisdn", "total_calls", "total_mou", "perc_mou_other_ops", "perc_calls_other_ops", 'total_calls_other_operator', 'total_calls_vf')
        new_suffixed_cols = [col_ + "_d{}".format(days_incremental) if col_ not in ["msisdn", "nif_cliente"] else col_ for col_ in df_agg2.columns]
        df_agg2 = df_agg2.toDF(*new_suffixed_cols)


        df_agg_join = df_agg.join(df_agg2, on=["msisdn"], how="left").fillna(0.0)




        cols_list = df_agg.columns

        inc_cols = [(col(col_) - col(col_ + "_d{}".format(days_incremental))).alias("inc_" + col_ + "_d{}".format(days_incremental)) for col_ in cols_list if col_ not in ["msisdn", "nif_cliente"]]
        inc_cols += [((col(col_) - col(col_ + "_d{}".format(days_incremental))) / col(col_ + "_d{}".format(days_incremental))).alias("inc_rel_" + col_ + "_d{}".format(days_incremental)) for col_ in
                     cols_list if col_ not in ["msisdn", "nif_cliente"]]

        df_agg_all = df_agg_join.select(*(df_agg_join.columns + inc_cols))

        df_agg_all = df_agg_all.cache()
        print("df_agg_all.count()", df_agg_all.count())

        from churn_nrt.src.data.sopos_dxs import MobPort

        df_target = MobPort(spark, churn_window=15).get_module(closing_day).withColumnRenamed("label_mob", "label")
        df_target = df_target.cache()
        print("df_target.count()", df_target.count())

        df_all = df_agg_all.join(df_target, on=["msisdn"], how="left").fillna(0.0)

        df_all = df_all.cache()
        print("df_all.count()", df_all.count())

        import time

        start_time_myvf = time.time()
        print("About to save test df - /user/csanc109/projects/churn/data/social/incrementals_df_{}_{}_{}".format(closing_day, closing_day_2, days_incremental))
        df_all.write.mode('overwrite').save("/user/csanc109/projects/churn/data/social/incrementals_df_{}_{}_{}".format(closing_day, closing_day_2, days_incremental))
        print("Ended saving test df - /user/csanc109/projects/churn/data/social/incrementals_df_{}_{}_{} (elapsed time {} minutes)".format(closing_day,
                                                                                                                                                closing_day_2,
                                                                                                                                                days_incremental,
                                                                                                                                                (time.time() - start_time_myvf) / 60.0))

    else:
        print("loading file {}".format(path_to_file))
        df_all = spark.read.load(path_to_file)

    # ch_rate_ref = df_all.select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref']
    # print("Ch Rate Ref = {} %".format(100.0 * ch_rate_ref))
    #
    # bins = []
    # range_inc = range(-110, 110, 10)
    # range_perc = range(0, 110, 10)
    #
    #
    # import datetime as dt
    # import pandas as pd
    # final_filename = "/var/SP/data/home/csanc109/data/social_incrementals_allops_{}_{}_{}.xlsx".format(closing_day, closing_day_2, dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
    # writer = pd.ExcelWriter(final_filename, engine='xlsxwriter')
    # workbook = writer.book
    #
    # for op_ in ["<", ">"]:
    #
    #     print("- - - - - OPERATION {} - - - - -".format(op_))
    #     results_perc = {}
    #     results_inc = {}
    #
    #     for var_ in df_all.columns:
    #         if var_.startswith("perc_") or var_.startswith("inc"):
    #             print(var_)
    #             bins = []
    #             range_ = range_perc if var_.startswith("perc_") else range_inc
    #             for perc in range_:
    #                 try:
    #                     if op_ == "<":
    #                         bins.append(100.0 * df_all.where(col(var_) <= (perc / 100.0)).select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref'])
    #                     else:
    #                         bins.append(100.0 * df_all.where(col(var_) >= (perc / 100.0)).select(sql_avg("label").alias('churn_ref')).rdd.first()['churn_ref'])
    #
    #                 except:
    #                     bins.append(-1)
    #
    #             if var_.startswith("inc"):
    #                 results_inc[var_] = bins
    #             else:
    #                 results_perc[var_] = bins
    #
    #     df_inc = pd.DataFrame(results_inc, index=range_inc)
    #     df_perc = pd.DataFrame(results_perc, index=range_perc)
    #
    #     df_inc.to_excel(writer, sheet_name="INC_{}".format(op_), startrow=7, startcol=0, index=True, header=True)
    #     df_perc.to_excel(writer, sheet_name="PERC_{}".format(op_), startrow=7, startcol=0, index=True, header=True)
    #
    # writer.save()

    print("Ended! - elapsed {} minutes".format( (time.time() - start_time_total)/60.0))
