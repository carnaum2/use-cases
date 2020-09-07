
# List of columns to be used in the model
FEATS_COLS = []

CATEGORICAL_COLS = None # None means the code detect them automatically  #FIXME autodetect with metadata project
NON_INFO_COLS = ["msisdn"] #FIXME autodetect with metadata project
OWNER_LOGIN = "csanc109"


MODEL_OUTPUT_NAME = "triggers_navcomp"
EXTRA_INFO_COLS = ["decil", "flag_propension", "operator"]
INSERT_TOP_K = None # no limit

MASK_AS_RISK = 15000 #TO DO: to check this figure



