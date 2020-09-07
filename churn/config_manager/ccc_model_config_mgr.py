

from churn.utils.constants import YAML_CCC_RANGE_DURATION, YAML_START_PORT, YAML_END_PORT, YAML_AGG_BY, \
          YAML_MODEL_TARGET, YAML_ENABLED
from churn.config_manager.config_mgr import Config
import imp
import os
import sys

class CCCmodelConfig(Config):

    config_obj=None

    def __init__(self, filename, check_args=True):
        internal_filename = os.path.join(imp.find_module('churn')[1], "config_manager", "config", "internal_config_ccc_model.yaml")
        Config.__init__(self, filename, internal_filename, check_args)

        if check_args:
            self.check_args(self.get_config_dict())

    def check_args(self, config=None):
        print("----- CHECKING INPUT PARAMETERS CCCmodel------")
        config = config if config else self.get_config_dict()
        if YAML_CCC_RANGE_DURATION in config and config[YAML_CCC_RANGE_DURATION] >= 0:
            print("[ERROR] CCC range must be a negative number (inserted {})".format(config[YAML_CCC_RANGE_DURATION]))
            sys.exit(1)

        if YAML_MODEL_TARGET in self.get_config_dict() and (str(self.get_config_dict()[YAML_MODEL_TARGET]) == "encuestas" and str(self.get_config_dict()[YAML_AGG_BY])=="nif"):
            print("[ERROR] CCC model does not support label 'encuestas' and aggregation by 'nif'")
            sys.exit(1)

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

    def get_agg_by(self):
        if YAML_AGG_BY in self.get_config_dict():
            return str(self.get_config_dict()[YAML_AGG_BY])
        return None

    def get_enabled(self):
        if YAML_ENABLED in self.get_config_dict():
            return self.get_config_dict()[YAML_ENABLED]
        return True


def build_config_dict(cfg_name, agg_by='msisdn', ccc_days=-60, closing_day="20190214", enabled=True,
                      end_port='None', labeled=False, model_target='comercial', new_car_version=False, save_car=True,
                      segment_filter='mobileandfbb', start_port='None'):
    from churn.utils.constants import YAML_ENABLED, YAML_CONFIGS, YAML_CLOSING_DAY, YAML_SEGMENT_FILTER, \
        YAML_START_PORT, YAML_END_PORT, YAML_AGG_BY, YAML_CAR_VERSION, YAML_SAVE_CAR, \
        YAML_MODEL_TARGET, YAML_LABELED, YAML_CCC_RANGE_DURATION

    return {YAML_CONFIGS: {cfg_name: {YAML_AGG_BY: agg_by,
                                      YAML_CCC_RANGE_DURATION: ccc_days,
                                      YAML_CLOSING_DAY: closing_day,
                                      YAML_ENABLED: enabled,
                                      YAML_END_PORT: end_port,
                                      YAML_LABELED: labeled,
                                      YAML_MODEL_TARGET: model_target,
                                      YAML_CAR_VERSION: new_car_version,
                                      YAML_SAVE_CAR: save_car,
                                      YAML_SEGMENT_FILTER: segment_filter,
                                      YAML_START_PORT: start_port}}}


