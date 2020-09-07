
from churn.utils.constants import *
import os
import sys
from pykhaos.utils.date_functions import move_date_n_cycles
import datetime as dt

class Config:

    def __init__(self, filename, internal_config_filename, check_args=True):
        config = read_config_file(filename=filename)
        # Add internal config (config that user does not have to set)
        internal_config = read_internal_config_file(internal_config_filename)
        # Join dicts - user config prevails over internal config
        config = dict(internal_config.items() + config.items())
        self.__config = config
        if check_args:
            self.check_args(config)
        import pprint
        pprint.pprint(config)

    # TODO check more args
    # @classmethod
    def check_args(self, config):
        print("----- CHECKING INPUT PARAMETERS ------")
        if not YAML_CLOSING_DAY in config:
            print('[ERROR] {} must be in config file'.format(YAML_CLOSING_DAY))
            sys.exit(1)

        if YAML_DISCARDED_CYCLES in config and config["discarded_cycles"] < 0:
            print("[ERROR] discarded cycles must be a number greater than 0 or 0 (inserted {})".format(config["discarded_cycles"]))
            sys.exit(1)
        if YAML_CYCLES_HORIZON in config:
            if config[YAML_CYCLES_HORIZON] <= 0:
                print("[ERROR] cycles horizon must be a number greater than 0 (inserted {})".format(config["cycles_horizon"]))
                sys.exit(1)
            else:
                closing_day = config[YAML_CLOSING_DAY]
                print("check_args", closing_day, type(closing_day))
                if isinstance(closing_day,str) or isinstance(closing_day,unicode) or isinstance(closing_day,int):
                    closing_day = [str(closing_day)]

                for cday in closing_day:
                    cycles_horizon = config[YAML_CYCLES_HORIZON]
                    end_date = move_date_n_cycles(str(cday), n=cycles_horizon)
                    print(cday, cycles_horizon, end_date)

                    if end_date > dt.date.today().strftime("%Y%m%d"):
                        print("[ERROR] Program cannot be run with cycles={0} since closing_day {2} + {0} cycles is in the future {1}".format(cycles_horizon, end_date, cday))
                        sys.exit(1)
                    print("----- INPUT PARAMETERS OK! ------")


    def get_config_dict(self):
        return self.__config

    def set_extra_info(self, another_dict):
        self.__config.update(another_dict)

    def get_closing_day(self):
        if YAML_CLOSING_DAY in self.__config:
            closing_day = self.__config[YAML_CLOSING_DAY]
            if isinstance(closing_day, str) or isinstance(closing_day, unicode) or isinstance(closing_day, int):
                closing_day = [str(closing_day)]
            else:
                closing_day = [str(cd) for cd in closing_day]
            return closing_day
        return None

    def get_segment_filter(self):
        if YAML_SEGMENT_FILTER in self.__config:
            return str(self.__config[YAML_SEGMENT_FILTER])  # segmento al que nos dirigimos
        return None

    def get_level(self):
        if YAML_LEVEL in self.__config:
            return str(self.__config[YAML_LEVEL])  # service=(msisdn), num_client, nif
        return None

    def get_service_set(self):
        if YAML_SERVICE_SET in self.__config:
            return str(self.__config[YAML_SERVICE_SET])  # siempre movil
        return None

    def get_model_target(self):
        if YAML_MODEL_TARGET in self.__config:
            return str(self.__config[YAML_MODEL_TARGET])
        return None

    def get_cycles_horyzon(self):
        if YAML_CYCLES_HORIZON in self.__config:
            return int(self.__config[YAML_CYCLES_HORIZON])
        return None

    def get_discarded_cycles(self):
        if YAML_DISCARDED_CYCLES in self.__config:
            return int(self.__config[YAML_DISCARDED_CYCLES])
        return None

    def get_sources_ids_dict(self):
        if YAML_SOURCES in self.__config and YAML_SOURCES_IDS in self.__config[YAML_SOURCES]:
            return self.__config[YAML_SOURCES][YAML_SOURCES_IDS]
        return None

    def get_labeled(self):
        if YAML_LABELED in self.__config:
            return self.__config[YAML_LABELED]
        return None

    def get_save_car(self):
        if YAML_SAVE_CAR in self.__config:
            return self.__config[YAML_SAVE_CAR]
        return None

    def get_internal_config_filename(self):
        if YAML_INTERNAL_CONFIG_FILE in self.__config:
            return self.__config[YAML_INTERNAL_CONFIG_FILE]
        return None

    def get_user_config_filename(self):
        if YAML_USER_CONFIG_FILE in self.__config:
            return self.__config[YAML_USER_CONFIG_FILE]
        return None

    def get_selected_table_names(self, path_to_table=None):
        if not path_to_table: path_to_table = ""

        # This dict contains the equivalence between name in yaml file ---> name of hdfs table
        from churn.utils.constants import IDS_TABLES_DICT
        sources_dict = self.get_sources_ids_dict()  # yaml config
        ids_src_list = [ids_src for ids_src, selected in sources_dict.items() if selected]
        print(ids_src_list)
        import pprint
        pprint.pprint(IDS_TABLES_DICT)
        table_names_list = [ (IDS_TABLES_DICT[ids_src] if isinstance(IDS_TABLES_DICT[ids_src],list) else  [IDS_TABLES_DICT[ids_src]]) for ids_src in ids_src_list if ids_src in IDS_TABLES_DICT]
        table_names_list = [tab for table_list in table_names_list for tab in table_list]  # flatten list of lists
        return table_names_list  # return the names of the amdocs tables

    def get_default_and_selected_table_names(self):
        # return a list of table names (including default tables + yaml selected tables)
        return DEFAULT_AMDOCS_CAR_MODULES + self.get_selected_table_names(path_to_table="")

    def get_source_name(self, ids_src):
        if "sources" in self.__config and YAML_SOURCES_IDS in self.__config[YAML_SOURCES] and ids_src in \
                self.__config[YAML_SOURCES][YAML_SOURCES_IDS]:
            return self.__config[YAML_SOURCES][YAML_SOURCES_IDS][ids_src]
        return None

    def get_amdocs_car_version(self):
        if YAML_CAR_VERSION in self.get_config_dict():
            return self.get_config_dict()[YAML_CAR_VERSION]
        return True # new car by default



def read_config_file(filename=None):
    if filename:
    #     import imp
    #     print(imp.find_module('churn')[1])
    #     filename = os.path.join(imp.find_module('churn')[1], "input", "datapreparation_churn.yaml")
        print("Reading config from file {}".format(filename))
        import yaml
        config = yaml.load(open(filename))
    else:
        config = {}
    config[YAML_USER_CONFIG_FILE] = filename

    return config

def read_internal_config_file(filename):
    import yaml
    #
    print("Reading internal config from file {}".format(filename))
    config = yaml.load(open(filename))
    config[YAML_INTERNAL_CONFIG_FILE] = filename # save this in config dict
    return config
