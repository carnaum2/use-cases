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


class Pre2post_Data(AbstractData):
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

        df_h2o = prepare_pre2post_data_for_h2o(df_h2o_imported)

        #df_h2o = prepare_ccc_data_for_h2o(df_h2o_imported)
        if logger: logger.info("df_h2o ready to be used!")



        return df_h2o

    def data(self):
        return self.__data

    def with_column(self, colname, serie):
        self.__data[colname] = serie

    def copy(self):
        return Pre2post_Data(self.__input_data, self.__data, self.vars())

    def sub_data(self, keys):
        data = self.data()
        #data = data.merge(keys, on=list(keys.columns)) ?????
        return Pre2post_Data(self.__input_data, keys,  self.vars())

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


def prepare_pre2post_data_for_h2o(df_h2o):

    print(df_h2o.columns)

    for col_ in df_h2o.columns:
        if col_.startswith("in_pospago") or col_.startswith("campaign_msisdn_contact") or col_ in ["prepaid_Fecha_ejecucion",
                                                                                                   "campaign_DATEID",
                                                                                                   "prepaid_partitioned_month",
                                                                                                   "prepaid_fx_1llamada"]:
            df_h2o = df_h2o.drop(col_)

    if "migrated_to_postpaid" in df_h2o.columns:
        df_h2o[TARGET] = df_h2o["migrated_to_postpaid"]
        df_h2o = df_h2o.drop("migrated_to_postpaid")
    if TARGET in df_h2o.columns:
        df_h2o[TARGET] = df_h2o[TARGET].asfactor()


    for col_ in ["campaign_EsRespondedor", "tariffs_TOTAL_LLAMADAS", "tariffs_Num_accesos", "tariffs_MOU_Week", "tariffs_LLAM_Week"]:
        if col_ in df_h2o.columns:
            df_h2o[df_h2o[col_].isna(), col_] = 0

    col_ = "tariffs_Plan"
    if col_ in df_h2o.columns:
        df_h2o[col_] = df_h2o[col_].ascharacter()
        df_h2o[df_h2o[col_].isna(), col_] = "UNKNOWN"
        df_h2o[col_] = df_h2o[col_].asfactor()

    return df_h2o

