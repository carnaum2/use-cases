
# List of columns to be used in the model
FEATS_COLS = []

CATEGORICAL_COLS = None # None means the code detect them automatically  #FIXME autodetect with metadata project
NON_INFO_COLS = ["msisdn"] #FIXME autodetect with metadata project
OWNER_LOGIN = "csanc109"

DEFAULT_N_DAYS_TARGET = 30

MODEL_OUTPUT_NAME = "trigger_social"
EXTRA_INFO_COLS = ["decil", "flag_propension", 'CALLS_JAZZTEL',
 'MOU_JAZZTEL',
 'CALLS_YOIGO',
 'MOU_YOIGO',
 'CALLS_DIGI_SPAIN',
 'MOU_DIGI_SPAIN',
 'CALLS_MAS_MOVIL',
 'MOU_MAS_MOVIL',
 'CALLS_TELEFONICA',
 'MOU_TELEFONICA',
 'CALLS_PEPEPHONE',
 'MOU_PEPEPHONE',
 'CALLS_R_CABLE',
 'MOU_R_CABLE',
 'CALLS_ORANGE',
 'MOU_ORANGE',
 'CALLS_LEBARA',
 'MOU_LEBARA',
 'CALLS_R',
 'MOU_R',
 'CALLS_VODAFONE',
 'MOU_VODAFONE']

INSERT_TOP_K = None
MARK_AS_RISK = 12000 #  (3x num churners poblacion estudio)