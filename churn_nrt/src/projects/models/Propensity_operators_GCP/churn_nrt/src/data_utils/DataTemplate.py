import os
import time
import yaml

from pyspark.sql.functions import lit, col
from my_propensity_operators.churn_nrt.src.utils.exceptions import Exceptions
from my_propensity_operators.utils.loaders import check_folder_exists_gcp

# GLOBALS:
PATH = None
MODULE_NAME = None
SPARK = None
VERBOSE = True
CONFIGS_PATH = "../../resources/configs.yml"


class DataTemplate(object):

    def __init__(self, spark, confs, verbose=True):
        self.SPARK = spark
        self.confs = confs
        self.VERBOSE = verbose
        print("[DataTemplate] __init__ | {}".format(repr(self)))

    # def __repr__(self):
    #     return "module name = {} | path = {} | verbose = {}".format(self.MODULE_NAME, self.PATH, self.VERBOSE)

    def set_path_configs(self):

        # Names:
        self.churn_nrt_path = self.confs['churn_nrt_path']
        module_name = self.confs.get('module_name', '')
        self.partition_date = self.confs['partition_date']
        self.MODULE_NAME = module_name
        self.PATH= os.path.join(self.churn_nrt_path,self.MODULE_NAME)




    def save(self, df, closing_day, overwrite=False):
        '''
        Use this function to store the module. This function is called from get_module function
        :param df: pyspark dataframe to store in hdfs
        :param closing_day: yyyymmdd of the closing_day
        :return:
        '''

        path_to_save = self.PATH

        df = df.withColumn("day", lit(int(closing_day[6:])))
        df = df.withColumn("month", lit(int(closing_day[4:6])))
        df = df.withColumn("year", lit(int(closing_day[:4])))

        start_time = time.time()

        mode = "append" if not overwrite else "overwrite"

        print("[DataTemplate] save | Started saving at '{}' partitioned by {} (mode={})".format(path_to_save,
                                                                                                self.partition_date.format(int(closing_day[:4]),
                                                                                                    int(closing_day[4:6]),
                                                                                                    int(closing_day[6:])),
                                                                                                    mode))

        (df.write.partitionBy('year', 'month', 'day').mode(mode).format("parquet").save(path_to_save))

        print("[DataTemplate] save | Saved {} [Elapsed time = {} minutes]".format(os.path.join(path_to_save, self.partition_date.format(int(closing_day[:4]),
                                                                                 int(closing_day[4:6]),
                                                                                 int(closing_day[6:]))),
                                                          (time.time() - start_time)/60.0))

    def build_module(self, closing_day, save_others, force_gen=False, *args, **kwargs):
        '''
        This functions must be defined for every class that extends DataTemplate
        :param closing_day:
        :param save_others:
        :param args:
        :param kwargs:
        :return:
        '''
        raise Exceptions.NOT_IMPLEMENTED

    def check_valid_params(self, closing_day, *args, **kwargs):
        return True

    def is_default_module(self, *args, **kwargs):
        '''
        Return True if the module can be saved, i.e., no specific args were specified for building the module
        :param args:
        :param kwargs:
        :return:
        '''
        return True

    def get_module(self, closing_day, save=True, save_others=True, force_gen=False, *args, **kwargs):
        '''
        Chedk if the requested module is already stored. If it is, then return it
        If not, then call to the build_module function to generate it.
        :param spark:
        :param closing_day:
        :param save: save the generated module. if the module already existed, this parameter does not apply
        :param save_others: save the other modules required to generate the current module. if the module already existed, this parameter does not apply
        :param force_gen: if True the module is built even if it exists. If save=True, previous module is overwritten
        :param args:
        :param kwargs:
        :return:
        '''

        print("[DataTemplate] get_module | module {} - for closing_day={} save={} save_others={} force_gen={}".format(self.MODULE_NAME, closing_day, save, save_others, force_gen))
        print("[DataTemplate] get_module | args: {} | kwargs: {}".format(args, kwargs))


        #self.check_valid_params(closing_day, *args, **kwargs)


        module_path = self.get_saving_path(closing_day, add_partition=True)

        module_exists = check_folder_exists_gcp(module_path, delimiter=None)


        if module_exists and not force_gen:
            print("[DataTemplate] get_module | Found already an existing module - '{}'".format(module_path))
            df = self.SPARK.read.parquet(module_path)
            return df
        elif force_gen:
            print("[DataTemplate] get_module | Module will be generated since force_gen parameter was set to True. {}".format("An existing module in path will be ignored" if module_exists else ""))
        else:
            print("[DataTemplate] get_module | Not found a module - '{}'. Starting generation...".format(module_path))

        start_time_ccc = time.time()
        df = self.build_module(closing_day, save_others=save_others, force_gen=force_gen, **kwargs)
        print("[DataTemplate] get_module | module '{}' | Elapsed time in build_module function: {} minutes".format(self.MODULE_NAME,
                                                                                                                     (time.time() - start_time_ccc) / 60.0))

        #is_default = self.is_default_module(*args, **kwargs)
        if save:

            # Be sure dynamic mode is activated. This only overwrites partitions where year/month/day are set in the df.
            # - When mode=overwrite and partitionOverwriteMode=static, then directory 'path_to_save' is clean of previous data and only the one in df is kept!!!
            # - When mode=overwrite and partitionOverwriteMode=dynamic, Spark will only delete the partitions for which it has data to be written to. All the other partitions remain intact.
            # - If mode=append, then this property does not affect
            self.SPARK.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

            print("[DataTemplate] get_module | module '{}' | About to call save for path '{}' with args and closing_day={}".format(self.MODULE_NAME,
                                                                                                                                   self.PATH, closing_day))

            self.save(df, closing_day, overwrite=force_gen)

        else:
            print("[DataTemplate] get_module | module '{}' | Module will not be saved (save={} and is_default_module()={})".format(self.MODULE_NAME,
                                                                                                                                          save, is_default))

        return df

    def get_saving_path(self, closing_day=None, add_partition=True):
        '''
        Returns the path where this module is stored
        :param closing_day: required when add_partition=True
        :param add_partition: add year=yyyy/month=mm/day=dd to the path, where yyyymmdd corresponds to the closing_day
        :return:
        '''
        path_to_save = self.PATH

        if add_partition:
            path_to_save = os.path.join(path_to_save,  self.partition_date.format(int(closing_day[:4]),
                                                                             int(closing_day[4:6]),
                                                                             int(closing_day[6:])))

        return path_to_save


    def get_metadata(self):
        print("*** [DataTemplate] get_metadata | get_metadata not implemented in module {}. Implement it! *** ".format(self.MODULE_NAME))
        return None



class TriggerDataTemplate(DataTemplate):
    '''
    This class is intended to be used for building the modules for triggers (joins of other modules).
    metadata_obj tells the sources to be used and how to impute nulls
    '''
    METADATA = None

    def __init__(self, spark, module_name, metadata_obj, verbose=True):
        self.METADATA = metadata_obj
        DataTemplate.__init__(self, spark, module_name, verbose)


