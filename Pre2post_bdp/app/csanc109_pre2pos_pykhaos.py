
# coding: utf-8

# # Pre2Pos - Probando pykhaos fusion con Dani

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
sys.path.append('/var/SP/data/home/csanc109/src/devel/pykhaos/external_lib')

import pykhaos.utils.custom_logger as clogger
logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging",
                                    "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
logger.info("Logging to file {}".format(logging_file))    
    
    
import pykhaos.utils.notebooks as nb

# In[2]:

import json
RUNNING_FROM_NOTEBOOK = nb.isnotebook()
default_filename = os.path.join(USECASES_SRC, "Pre2post_bdp", "input", "train_test.json")

input_file = default_filename #if RUNNING_FROM_NOTEBOOK else input_filename

with open(input_file) as f:
    logger.info("Reading data from {}".format(input_file))
    input_data = json.load(f,encoding='utf-8')
    import pprint
    pprint.pprint(input_data)

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

MODEL_NAME = "pre2post_pykhaos"
from pykhaos.modeling.model import Model

xlsx_report_template_path = os.path.join(USECASES_SRC, "Pre2post_bdp", 'input', 'xlsx_report_template.yaml')
model_yaml_path = os.path.join(USECASES_SRC, "Pre2post_bdp", 'input', 'h2o_automl.yaml')
model_name = MODEL_NAME+'_model'
save_model_path = os.path.join(USECASES_SRC, "Pre2post_bdp", 'output', '{}_model'.format(MODEL_NAME))

from pykhaos.modeling.h2o.h2o_functions import restart_cluster_loop

restart_cluster_loop(port=54122)

if phase == TRAIN_MODEL:

    if STAGES > 0:

        print("Starting Pre2PostData")
        from Pre2post_bdp.src.pre2post_data import Pre2PostData
        data_abs_data_obj = Pre2PostData(input_data)
        print("Ended Pre2PostData")

        modeler = Model(model_name, model_yaml_path)

        modeler.fit(data_abs_data_obj)
        # FIXME output path not in project folder
        modeler.save(save_model_path)
        pass

    if STAGES > 1:

        from pykhaos.reporting.reporter import Reporter
        reporter = Reporter(model_name, save_model_path, xlsx_report_template_path).set_saving_path('/var/SP/data/home/csanc109/data/results/pre2post/')
        reporter.create()

        print 'Informe generado!'


elif phase == UNIVARIATE_ANALYSIS:

        print 'Análisis univariante: '
        periods = [201805, 201806, 201807]

        from Pre2post_bdp.src.pre2post_data import Pre2PostData
        data_abs_data_obj = Pre2PostData(input_data)
        from pykhaos.reporting.type.univariate import Univariate
        univariate_info = Univariate(data_abs_data_obj, MODEL_NAME+'_univariate').analysis()
        univariate_info.save('/var/SP/data/home/csanc109/data/results/pre2post/')
        print 'Fin del análisis: {}'.format(MODEL_NAME+'_univariate')

        pdf_report_template_path = os.path.join(USECASES_SRC, "Pre2post_bdp", 'model', 'pdf_report_template.yaml')


        print 'Generando informe: '
        from pykhaos.reporting.reporter import Reporter

        reporter =  Reporter(model_name, save_model_path, pdf_report_template_path).set_saving_path('/var/SP/data/home/csanc109/data/results/pre2post/')
        reporter.create()
        print 'Informe generado!'