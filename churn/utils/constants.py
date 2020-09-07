
import os

# - - - - - - - - - - - - - - - - - - - - - - - - -
# NAMES
# - - - - - - - - - - - - - - - - - - - - - - - - -
PROJECT_NAME = "churn"
APP_NAME = "churn_py_dp"



# - - - - - - - - - - - - - - - - - - - - - - - - -
# TABLES
# - - - - - - - - - - - - - - - - - - - - - - - - -
PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"
PATH_TO_TABLE = '/data/udf/vf_es/amdocs_inf_dataset'


# - - - - - - - - - - - - - - - - - - - - - - - - -
# PATHS
# - - - - - - - - - - - - - - - - - - - - - - - - -
# directory to save generated tables
HDFS_DIR_DATA = "/data/udf/vf_es/churn/"
# dir name to store the yaml files used in the generation
YAML_FILES_DIR = "yaml"

# - - - - - - - - - - - - - - - - - - - - - - - - -
# YAML config
# - - - - - - - - - - - - - - - - - - - - - - - - -

# name in yaml file ---> name of hdfs table
IDS_TABLES_DICT = {
    "address": "address",
    "call_centre_calls": "call_centre_calls",
    "customer": "customer",
    "customer_aggregations" : "customer_agg",
    "orders": ["orders", "orders_agg"],
    "tnps": "tnps",
    "billing": "billing",
    "campaigns": ["campaigns_customer", "campaigns_service"],
    "customer_penalties": ["penalties_customer", "penalties_service"],
    "device_catalogue" : "device_catalogue",
    "geneva": ["usage_geneva_voice", "usage_geneva_data"],
    "geneva_roam" : ["usage_geneva_roam_data", "usage_geneva_roam_data"],
    "netscout": "netscout",
    "services": "service",
    "perms_and_prefs" : "perms_and_prefs"}

YAML_DATA_PREPARATION = "data_preparation"
YAML_LEVEL = "level"
YAML_SERVICE_SET = "service_set"
YAML_SEGMENT_FILTER = "segment_filter"
YAML_MODEL_TARGET = "model_target"
YAML_CYCLES_HORIZON = "cycles_horizon"
YAML_DISCARDED_CYCLES = "discarded_cycles"
YAML_CLOSING_DAY = "closing_day"
YAML_SOURCES = "sources"
YAML_SOURCES_IDS = "ids"
YAML_LABELED = "labeled"
YAML_SAVE_CAR = "save_car"
YAML_USER_CONFIG_FILE = "user_config_file"
YAML_INTERNAL_CONFIG_FILE = "internal_config_file"
YAML_CCC_RANGE_DURATION = "ccc_days"
YAML_START_PORT = "start_port"
YAML_END_PORT = "end_port"
YAML_AGG_BY = "agg_by"
YAML_PREDICT = "predict"
YAML_DO_PREDICT = "do_predict"
YAML_CAR_VERSION = "new_car_version"
YAML_CAMPAIGN = "campaign"
YAML_DO_CAMPAIGN = "do_campaign"
YAML_CAMPAIGN_DATE = "campaign_date"
YAML_ENABLED = "enabled"
YAML_CONFIGS = "configs"
YAML_CONFIG_NAME = "config_name"

from collections import OrderedDict

# Always read these tables, without regard to the yaml file
DEFAULT_AMDOCS_CAR_MODULES = ["customer", "customer_agg", "service"]

# name of the table
#[order for the join, [left_columns (must be preffixed)], [right_columns (not preffixed)], type of join]
JOIN_ORDER = OrderedDict([("customer" , [0, None, None]),
                          ("customer_agg" , [1, ['NUM_CLIENTE'], ['NUM_CLIENTE'], 'inner']),
                          ("service", [2, ['NUM_CLIENTE'], ['NUM_CLIENTE'], 'inner']),
                          ("usage_geneva_voice", [3, ["CAMPO2"] , ["id_msisdn"], 'leftouter']),
                          ("usage_geneva_data", [4, ["CAMPO2"], ["id_msisdn_data"], 'leftouter']),
                          #(.join(usage_geneva_roam_voice, (col('CAMPO2')==col('id_msisdn_voice_roam')), 'leftouter')]),
                          #(.join(usage_geneva_roam_data, (serviceDF_load.CAMPO2==RoamDataUsageDF_load.id_msisdn_data_roam), 'leftouter')]),
                          ("billing",[7,["NUM_CLIENTE"],["customeraccount"], 'leftouter']),
                          ("campaigns_customer", [8, ['nif_cliente'], ['cif_nif'], 'leftouter']),
                          ("campaigns_service", [9,["CAMPO2"],['msisdn'], 'leftouter']),
                          ("call_centre_calls", [10, ["CAMPO2"] ,['msisdn'],'leftouter']),
                          ("tnps", [11, ["CAMPO2"] ,['msisdn'],'leftouter']),
                          ("penalties_customer", [12, ['NUM_CLIENTE'], ['NUM_CLIENTE'],'leftouter']),
                          ("penalties_service", [13, ['NUM_CLIENTE','Instancia_P'], ['NUM_CLIENTE','Instancia_P'], 'leftouter']),
                          ("orders_agg", [14, ['NUM_CLIENTE'], ['NUM_CLIENTE'], 'leftouter']),
                          ("orders", [15, ['NUM_CLIENTE'], ['NUM_CLIENTE'], 'leftouter']),
                          ("device_catalogue", [16, ["CAMPO2"] ,['msisdn'],'leftouter'])
                        ])

REASONS_FILENAME = r"/user/csanc109/projects/churn/resources/TrackingBajas_20104_201809.csv"

CHURN_DELIVERIES_DIR = "/var/SP/data/bdpmdses/deliveries_churn"
CHURN_LISTS_DIR = os.path.join(CHURN_DELIVERIES_DIR, "delivery_lists")
CHURN_TMP_DIR = os.path.join(CHURN_DELIVERIES_DIR, "tmp")
DELIVERY_FILENAME = "churn_c{}_trini{}_trend{}_h{}_nochurnreasons"
DELIVERY_FILENAME_CHURNREASONS = "churn_c{}_trini{}_trend{}_h{}_churnreasons"

DELIVERY_FILENAME_EXTENDED = "churn_c{}_trini{}_trend{}_h{}_extended"
DELIVERY_FILENAME_CLASSIC = "churn_c{}_trini{}_trend{}_h{}_classic"
DELIVERY_FILENAME_REASONS = "churn_c{}_trini{}_trend{}_h{}_reasons"