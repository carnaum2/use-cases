# -*- coding: utf-8 -*-
'''
    Author: Cristina Sanchez Maiz
    Email: cristina.sanchez4@vodafone.com
    Date created: 25/09/2018
    Python Version: 2.7
'''

from churn.utils.constants import *

from pyspark.sql.functions import col
from pykhaos.data.wrap.abstract_data import AbstractData
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
import time
import sys


TARGET = 'label'


class Segmentation_Data(AbstractData):

    __periods = None
    __data = None
    __vars = None
    __input_data = None
    __pkey_cols = None
    __spark = None

    def __init__(self, input_data=None, data=None, vars=None, key=YAML_DATA_PREPARATION, filename=None, pkey_cols=None, spark=None):

        AbstractData.__init__(self)

        if spark:
            print("setting spark")
            self.__spark = spark
        # else:
        #     start_time = time.time()
        #     from pykhaos.utils.pyspark_configuration import get_spark_session
        #     sc, spark, sql_context = get_spark_session(app_name="segmentation_data", log_level="OFF")
        #     print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time, sc.defaultParallelism))
        #     self.__spark = spark

        if data is None:
            self.__data = self.__create_data(key, filename)
            print("input_data")
            print(input_data)
            self.__pkey_cols = pkey_cols if pkey_cols else ([input_data[YAML_DATA_PREPARATION][YAML_AGG_BY]] if input_data else None)
            if not self.__pkey_cols:
                print("pkey_cols MUST BE specified by means of 'pkey_cols' parameters or 'input_data' dict")
            vars = [c for c in list(self.__data.columns) if c not in (self.pkey_cols() + [self.target_col()])]
            self.__vars = vars
        else:
            self.__data = data
            self.__vars = vars


    def __create_data(self, key=YAML_DATA_PREPARATION, filename=None):
        '''
        :param input_data:
        :param key:
        :param filename:
        :return:
        '''
        print("loading data from filename {}".format(filename))
        df = self.__spark.read.option("delimiter", "|").option("header",True).csv(filename)
        df = prepare_data(df)
        if logger: logger.info("df ready to be used!")
        return df

    def data(self):
        return self.__data

    def with_column(self, colname, serie):
        print("with_column not implemented yet in segmentation_data")
        sys.exit()
        #self.__data[colname] = serie

    def copy(self):
        return Segmentation_Data(self.__input_data, self.__data, self.vars())

    def sub_data(self, keys):
        print("sub_data", keys)
        data = self.data()
        #data = data.merge(keys, on=list(keys.columns)) ?????
        return Segmentation_Data(self.__input_data, keys,  self.vars())

    def update(self, data):
        self.__data = data.copy()

    def target(self):
        return self.data()[self.target_col()]

    def periods(self):
        if self.period_col() in self.data().columns:
            return self.data()[self.period_col()].drop_duplicates()
        else:
            print("!!!!Column '{}' does not exist on data".format(self.period_col()))
            return None

    def target_col(self):
        return TARGET

    def pkey_cols(self):
        return self.__pkey_cols

    def period_col(self):
        return None

    def vars(self, v=None):
        if v is not None:
            self.__vars = list(v)
        return self.__vars


# # # # # # # # # # # # # # # # # # # # # # # # # #
# # AUXILIARY
# # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# def union_all(dfs):
#     if len(dfs) > 1:
#         return dfs[0].unionAll(union_all(dfs[1:]))
#     else:
#         return dfs[0]



def prepare_data(df_orig):

    df = df_orig

    print("**** Preparing __prepare_data - rows={} columns={}".format(df.count(), len(df.columns)))

    # FIXME remove imputation of columns here when it is done in data preparation stage
    # Check target variable appears as "enum" in describe function
    df = df.where(col("RGU").rlike("mobile|movil"))
    df = df.withColumn("label", col("label").astype("double"))

    print("**** After rgu reduction - rows={} columns={}".format(df.count(), len(df.columns)))

    bucket_cols = [col_ for col_ in df.columns if col_.lower().startswith("bucket") or col_.lower().startswith("ccc_bucket") or col_.lower().startswith("ccc_raw") or col_.lower().startswith("raw")]

    novalen_penal_cols = [col_ for col_ in df.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("desc_promo") or
                                                                                                    col_.lower().endswith("desc_penal"))]
    penal_cols = [col_ for col_ in df.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("cod_promo") or
                                                                                            col_.lower().endswith("cod_penal"))]
    imputed_avg = [col_ for col_ in df.columns if col_.endswith("imputed_avg")]
    proxy_cols = ["TOTAL_COMERCIAL", "TOTAL_NO_COMERCIAL", "COMERCIAL", "NO_COMERCIAL", "PRECIO", "TERMINAL", "CONTENIDOS", "SERVICIO/ATENCION",
                  "TECNICO", "BILLING", "FRAUD", "NO_PROB", "CAT1_MODE", "CAT2_MODE"]
    useless_cols = ["DIR_LINEA1", "DIR_LINEA2", "DIR_LINEA3", "DIR_LINEA4", "DIR_FACTURA1", "x_user_facebook", "x_user_twitter",
                  "DIR_FACTURA2", "DIR_FACTURA3", "DIR_FACTURA4", "num_cliente_car", "num_cliente", "NOMBRE", "PRIM_APELLIDO",
                    "SEG_APELLIDO", "NOM_COMPLETO", "TRAT_FACT", 'NOMBRE_CLI_FACT', 'APELLIDO1_CLI_FACT', 'APELLIDO2_CLI_FACT',
                    'NIF_CLIENTE', 'DIR_NUM_DIRECCION', 'CTA_CORREO_CONTACTO', 'NIF_FACTURACION', 'CTA_CORREO', 'OBJID', 'TACADA',
                    'IMSI', 'OOB', 'CAMPO1', 'CAMPO2', 'CAMPO3', u'bucket_1st_interaction', u'bucket_latest_interaction',
                u'portout_date', 'codigo_postal_city', "PRICE_SRV_BASIC", "FBB_UPGRADE", "x_tipo_cuenta_corp", "NUM_CLIENTE", "DESC_SRV_BASIC",
                    "cta_correo_flag", "cta_correo_server", "DESC_TARIFF", "ROAM_USA_EUR", u'SUPEROFERTA2', u'rowNum', u'age', "rgu",
                    "CODIGO_POSTAL"]

    to_remove = imputed_avg  + useless_cols + proxy_cols + bucket_cols + novalen_penal_cols + ["codigo_postal_city", u'seg_pospaid_nif', 'flag_prepaid_nc', 'SIM_VF', 'flag_msisdn_err'] + penal_cols
    to_remove = list(set(df.columns) & set(to_remove)) # intersection
    print("About to remove {} columns".format(len(to_remove)))
    df = df.drop(*to_remove)


    for col_ in [u'num_interactions', u'num_NA_buckets', u'num_ivr_interactions',  u'nb_diff_buckets', u'days_since_first_interaction',
                 u'days_since_latest_interaction',  u'COMERCIAL', u'NO_COMERCIAL', u'PRECIO',
                 u'TERMINAL', u'CONTENIDOS', u'SERVICIO/ATENCION', u'TECNICO', u'BILLING', u'FRAUD', u'NO_PROB', u'TOTAL_COMERCIAL',
                 u'TOTAL_NO_COMERCIAL',  u'days_since_prepaid_fx_first_nif', u'fx_tv_fx_first_nif', u'days_since_tv_fx_first_nif',
                 u'days_since_mobile_fx_first_nif', u'days_since_fixed_fx_first_nif',   u'days_since_fbb_fx_first_nif',  u'days_since_bam_mobile_fx_first_nif',
                 u'days_since_bam_fx_first_nif', u'total_num_services_nif',      u'total_football_price_nif',
                 u'mobile_fx_first_nif', u'tv_services_nif', u'tv_fx_first_nif', u'prepaid_services_nif', u'prepaid_fx_first_nif',
                 u'bam_mobile_services_nif', u'bam_mobile_fx_first_nif', u'fixed_services_nif', u'fixed_fx_first_nif',
                 u'bam_services_nif', u'bam_fx_first_nif', u'flag_prepaid_nif', u'num_football_nif',
                 u'fbb_services_nif', u'fbb_fx_first_nif', u'mobile_services_nif', 'num_football_nc', 'total_num_services_nc']:
        if col_ in df.columns:
            df = df.withColumn(col_, col(col_).astype("float"))


    print("**** Ended __prepare_datao - rows={} columns={}".format(df.count(), len(df.columns)))
    return df