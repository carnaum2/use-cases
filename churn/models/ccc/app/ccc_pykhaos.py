
# coding: utf-8


# ## Configuration (logging + external libs)

# In[1]:

import os, sys
import datetime as dt
DEVEL_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src", "devel")

if DEVEL_SRC not in sys.path:
    sys.path.append(DEVEL_SRC)

USECASES_SRC = os.path.join(DEVEL_SRC, "use-cases") # TODO when - is removed, remove also this line and adapt imports
if USECASES_SRC not in sys.path: 
    sys.path.append(USECASES_SRC)

# requiered for libraries used in pykhaos and not existing in bdp
sys.path.append(os.path.join(os.environ.get('BDA_USER_HOME', ''), "src", "devel", "pykhaos", "external_lib"))

import pykhaos.utils.custom_logger as clogger
logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging",
                                    "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
logger.info("Logging to file {}".format(logging_file))    
    
    
import pykhaos.utils.notebooks as nb


# In[2]:

import json
RUNNING_FROM_NOTEBOOK = nb.isnotebook()
default_filename = os.path.join(USECASES_SRC, "churn", "models", "ccc", "input", "ccc_train_test.yaml")

import yaml
input_data = yaml.load(open(default_filename))

UNIVARIATE_ANALYSIS = 'univariate'
TRAIN_MODEL = 'train'
SCORE_NEW_DATA = 'score'
MAJOR_VERSION = '0'
MINOR_VERSION = '003'
phase =  TRAIN_MODEL #  UNIVARIATE_ANALYSIS # this parameters must be read as a program argument.
phase = phase.lower()     

# from pykhaos.modeling.model_info import ModelInfo
# from pykhaos.reporting.reporter import Reporter
# from pykhaos.reporting.type.univariate import Univariate
# from pykhaos.reporting.writter.pdf_writter import PDFWritter
# from pykhaos.reporting.utils.measures import completness
STAGES = 2


#######################################
# H2O
#######################################

MODEL_NAME = "ccc_pykhaos"
from pykhaos.modeling.model import Model

xlsx_report_template_path = os.path.join(USECASES_SRC, "churn", "models", "ccc", 'input', 'xlsx_report_template.yaml')
model_yaml_path = os.path.join(USECASES_SRC, "churn", "models", 'input', 'h2o_automl.yaml')
model_name = MODEL_NAME+'_model'



save_model_path = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "churn", "ccc", "results",  '{}_model'.format(MODEL_NAME))

from pykhaos.modeling.h2o.h2o_functions import restart_cluster_loop

restart_cluster_loop(port=54126)


import os
print(os.environ["SPARK_COMMON_OPTS"])



if phase == TRAIN_MODEL:

    if STAGES > 0:

        print("Starting CCC categories data")
        from churn.models.ccc.data.ccc_data import  CCC_Data
        data_abs_data_obj = CCC_Data(input_data)
        print("Ended CCC_Data")

        modeler = Model(model_name, model_yaml_path)

        modeler.fit(data_abs_data_obj)
        modeler.save(save_model_path)
        pass

    if STAGES > 1:

        from pykhaos.reporting.reporter import Reporter
        reporter = Reporter(model_name, save_model_path, xlsx_report_template_path).set_saving_path(os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "churn", "ccc", "results"))
        reporter.create()

        print 'Informe generado!'
