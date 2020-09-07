

from churn.utils.constants import YAML_CCC_RANGE_DURATION, YAML_START_PORT, YAML_END_PORT
from churn.config_manager.config_mgr import Config
import imp
import os


class TrackBajasConfig(Config):

    config_obj=None

    def __init__(self, filename, check_args=True):
        internal_filename = os.path.join(imp.find_module('churn')[1], "config_manager", "config", "internal_config_bajas.yaml")

        Config.__init__(self, filename, internal_filename, check_args)

    def get_ccc_range_duration(self):
        '''
            Return the number of days for the CCC period
        '''
        if YAML_CCC_RANGE_DURATION in self.get_config_dict():
            return int(self.get_config_dict()[YAML_CCC_RANGE_DURATION])
        return None

    def get_start_port(self):
        if YAML_START_PORT in self.get_config_dict():
            return str(self.get_config_dict()[YAML_START_PORT])
        return None

    def get_end_port(self):
        if YAML_END_PORT in self.get_config_dict():
            return str(self.get_config_dict()[YAML_END_PORT])
        return None




