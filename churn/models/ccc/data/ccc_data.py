# -*- coding: utf-8 -*-
'''
    Author: Cristina Sanchez Maiz
    Email: cristina.sanchez4@vodafone.com
    Date created: 25/09/2018
    Python Version: 2.7
'''

from churn.utils.constants import *


from pykhaos.data.wrap.abstract_data import AbstractData
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
from pyspark.sql.functions import col
import time
import sys


TARGET = 'label'


class CCC_Data(AbstractData):
    __periods = None
    __data = None
    __vars = None
    __input_data = None
    __pkey_cols = None

    def __init__(self, input_data=None, data=None, vars=None, key=YAML_DATA_PREPARATION, filename=None, pkey_cols=None, hdfs=True):
        AbstractData.__init__(self)
        if data is None:
            self.__data = self.__create_data(input_data, key, filename, hdfs=hdfs)
            self.__pkey_cols = pkey_cols if pkey_cols else ([input_data[YAML_DATA_PREPARATION][YAML_AGG_BY]] if input_data else None)
            if not self.__pkey_cols:
                print("pkey_cols MUST BE specified by means of 'pkey_cols' parameters or 'input_data' dict")
            vars = [c for c in list(self.__data.columns) if c not in (self.pkey_cols() + [self.target_col()])]
            self.__vars = vars
        else:
            self.__data = data
            self.__vars = vars


    def __create_data(self, input_data=None, key=YAML_DATA_PREPARATION, filename=None, hdfs=True):
        '''
        :param input_data:
        :param key:
        :param filename:
        :return:
        '''
        if filename and input_data:
            print("[ERROR] please, provide to __create_data or filename or input_data dict")
            sys.exit()
        elif input_data and not key:
            print("[ERROR] please, provide to __create_data the key parameter")
            sys.exit()

        #df_h2o = set_h2o_training_data(filename, hdfs=hdfs, parquet=False) # type <class 'h2o.frame.H2OFrame'>
        from pykhaos.modeling.h2o.h2o_functions import import_file_to_h2o_frame
        df_h2o_imported = import_file_to_h2o_frame(filename, verbose=True,  hdfs=hdfs, parquet=False)
        if df_h2o_imported is None:
            if logger: logger.error("Data could not be obtained")
            sys.exit()

        df_h2o = prepare_ccc_data_for_h2o(df_h2o_imported)
        if logger: logger.info("df_h2o ready to be used!")


        return df_h2o

    def data(self):
        return self.__data

    def with_column(self, colname, serie):
        self.__data[colname] = serie

    def copy(self):
        return CCC_Data(self.__input_data, self.__data, self.vars())

    def sub_data(self, keys):
        data = self.data()
        #data = data.merge(keys, on=list(keys.columns)) ?????
        return CCC_Data(self.__input_data, keys,  self.vars())

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


# # # # # # # # # # # # # # # # # # # # # # # # #
# AUXILIARY
# # # # # # # # # # # # # # # # # # # # # # # # # #

def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]

# def _initialize():
#     start_time = time.time()
#     # app_name = app_name if app_name else os.environ.get('USER', '') \
#     #                                      + "_" + self.project_name \
#     #                                      + "_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
#     print("_initialize spark")
#     import pykhaos.utils.pyspark_configuration as pyspark_config
#     sc, spark, sql_context = pyspark_config.get_spark_session(app_name="csanc109_app", log_level="OFF")
#     print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
#                                                                          sc.defaultParallelism))
#     return spark



# def set_h2o_training_data(filename, hdfs=True, parquet=False):
#     print("set_h2o_training_data - filename {}".format(filename))
#
#     from pykhaos.modeling.h2o.h2o_functions import import_file_to_h2o_frame
#     from pykhaos.utils.hdfs_functions import check_hdfs_exists, check_local_exists
#
#     if isinstance(filename, str):
#         filename = [filename]
#
#     df_h2o_joined = None
#
#     for ff in filename:
#
#         if not check_hdfs_exists(ff) and hdfs:
#             print("Not found {}. Run data preparation first!".format(ff))
#             sys.exit()
#         elif not check_local_exists(ff) and not hdfs:
#             print("Not found {} locally. Run data preparation first!".format(ff))
#             sys.exit()
#
#         print("Found {}. Loading...".format(ff))
#
#         df_h2o_imported = import_file_to_h2o_frame(ff, verbose=True,  hdfs=hdfs, parquet=parquet)
#
#
#         #df_h2o_imported = df_h2o_imported[df_h2o_imported["label"]!=-1]
#
#         if df_h2o_imported is None:
#             if logger: logger.error("Data could not be obtained")
#             sys.exit()
#
#         df_h2o = __prepare_ccc_data_for_h2o(df_h2o_imported)
#         if logger: logger.info("df_h2o ready to be used!")
#
#
#         if df_h2o_joined is None:
#             df_h2o_joined = df_h2o
#             print("-------")
#         else:
#             not_common = set(df_h2o.columns)  ^ set(df_h2o_joined.columns)
#             print(not_common)
#             # for col_ in not_common:
#             #     if col_ in df_h2o.columns:
#             #         print("Deleting col={} in read".format(col_))
#             #         df_h2o = df_h2o.drop(col_)
#             #     if col_ in df_h2o_joined.columns:
#             #         print("Deleting col={} in joined".format(col_))
#             #         df_h2o_joined = df_h2o_joined.drop(col_)
#             #
#             tr_dict = df_h2o.types
#             ts_dict = df_h2o_joined.types
#
#             for k,v_train in tr_dict.items():
#                 if ts_dict[k] != v_train: # different types --> produce an error in rbind
#                     print("col {} has different types current='{}' vs joined='{}'".format(k, v_train, ts_dict[k]))
#                     print(df_h2o_joined[k].unique())
#                     print(df_h2o[k].unique())
#                     break
#
#             print("BEFORE", len(df_h2o_joined), len(df_h2o_joined.columns))
#             print("BEFORE", len(df_h2o), len(df_h2o.columns))
#             #df_h2o_joined = df_h2o_joined.merge(df_h2o, all_x=True, all_y=True)
#             df_h2o_joined = df_h2o_joined.rbind(df_h2o)
#
#             print("AFTER", len(df_h2o_joined), len(df_h2o_joined.columns))
#             print("++++", len(df_h2o_joined))
#             print(df_h2o_joined)
#     return df_h2o_joined



def prepare_ccc_data_for_h2o(df_h2o_orig):

    df_h2o = df_h2o_orig

    print("**** Preparing __prepare_ccc_data_for_h2o - rows={} columns={}".format(len(df_h2o), len(df_h2o.columns)))

    # FIXME remove imputation of columns here when it is done in data preparation stage
    # Check target variable appears as "enum" in describe function
    if TARGET in df_h2o.columns:
        df_h2o[TARGET] = df_h2o[TARGET].asfactor()

    df_h2o = df_h2o[df_h2o["RGU"]=="mobile"]
    print("After rgu reduction {}".format(len(df_h2o)))

    # fx_cols = [col_ for col_ in df_h2o.columns if
    #            ("fx_" in col_.lower() or "fecha" in col_.lower() or "date" in col_.lower()) and not ("days_since" in col_ or "days_until" in col_)]
    bucket_cols = [col_ for col_ in df_h2o.columns if col_.lower().startswith("bucket") or col_.lower().startswith("ccc_bucket") or col_.lower().startswith("ccc_raw") or col_.lower().startswith("raw")]

    novalen_penal_cols = [col_ for col_ in df_h2o.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("desc_promo") or
                                                                                                    col_.lower().endswith("desc_penal"))]
    penal_cols = [col_ for col_ in df_h2o.columns if col_.lower().startswith("penal_") and (col_.lower().endswith("cod_promo") or
                                                                                            col_.lower().endswith("cod_penal"))]
    imputed_avg = [col_ for col_ in df_h2o.columns if col_.endswith("imputed_avg")]
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

    to_remove = imputed_avg  + useless_cols + proxy_cols + bucket_cols + novalen_penal_cols
    to_remove = list(set(df_h2o.columns) & set(to_remove)) # intersection
    print("About to remove {} columns".format(len(to_remove)))
    for to_remove_ in to_remove:
        df_h2o = df_h2o.drop(to_remove_)


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
        if col_ in df_h2o.columns:
            df_h2o[col_] = df_h2o[col_].asnumeric()
        # else:
        #     print("col {} does not exist".format(col_))



    # for col_ in ["ROAM_USA_EUR"]:
    #     if col_ in df_h2o.columns:
    #         df_h2o[df_h2o[col_].isna(), col_] = "UNKNOWN"
    #         #print(df_h2o[col_].unique())


    for col_ in ["codigo_postal_city", u'seg_pospaid_nif', 'flag_prepaid_nc', 'SIM_VF', 'flag_msisdn_err'] + penal_cols:
        if col_ in df_h2o.columns:
            try:
                df_h2o[col_] = df_h2o[col_].ascharacter().asfactor()
            except Exception as e:
                print(e)
                print("Not possible to change as factor type {}".format(col_))

    print("**** Ended __prepare_ccc_data_for_h2o - rows={} columns={}".format(len(df_h2o), len(df_h2o.columns)))
    return df_h2o