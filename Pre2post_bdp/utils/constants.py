
import os


PREPAID_TABLE_TAG = "prepaid_"
TARIFFS_TABLE_TAG = "tariffs_"
CAMPAIGN_HIST_TABLE_TAG = "campaign_"
CAMPAIGN_NEW_CAR_TABLE_TAG = "campaign_newcar_"

JSON_CAMPAIGN = u'campaign_creation'
JSON_CAMPAIGN_DATE = u'campaign_date'
JSON_CAMPAIGN_CSV = u'create_csv_file'
JSON_CAMPAIGN_STORAGE = u'storage_format'

JSON_DATA_PREPARATION = u'data_preparation'
JSON_DATA_CLASSICAL_CAMP_HIST = u'classical_campaign_msisdn_hist'
JSON_DATA_POSPAGO_TAG = u'dd_pospago_tag'
JSON_DATA_PREPAGO = u'dd_prepago'
JSON_GENERATE_DATA = u'generate_data'
JSON_NUM_SAMPLES = u'num_samples'
JSON_SAVE = u'save'
JSON_FORCE_GENERATION = u'force_generation'
JSON_LATEST_MONTH_COMPLETE = "latest_month_complete"
JSON_LABELED = "labeled"


JSON_PREDICT = u'predict'
JSON_PREDICT_POSPAGO_TAG = u'dd_pospago_tag'
JSON_PREDICT_PREPAGO = u'dd_prepago'
JSON_DO_PREDICT = u'do_predict'
JSON_USE_TRAIN_MODEL = u'use_train_model_predict'

JSON_TRAIN = u'train'
JSON_DO_TRAIN = u'do_train'
JSON_SAVE_TRAIN_RESULTS = u'save_train_results'
JSON_TRAIN_ALG = u'train_alg'
JSON_MODEL_NAME = "model_name"

JSON_USER_CONFIG_FILE = "user_config_file"


PROJECT_NAME = "pre2post"

# directory for local csv
LOCAL_DIR_DATA = '/var/SP/data/home/{}/data/{}/'.format(os.getenv("USER"), PROJECT_NAME)
HDFS_DIR_DATA = '/user/{}/projects/{}/data/'.format(os.getenv("USER"), PROJECT_NAME)
YAML_FILES_DIR = "yaml"