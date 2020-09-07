# coding: utf-8


import sys
import datetime as dt
import os



def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    try:
        USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))
    except Exception as e:
        print(e)
        sys.exit()

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))


    ENGINE_SRC = "/var/SP/data/home/csanc109/src/devel/amdocs_informational_dataset/"
    if ENGINE_SRC not in sys.path:
        sys.path.append(ENGINE_SRC)
        print("Added '{}' to path".format(ENGINE_SRC))

    EXTERNAL_LIB = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src", "devel", "pykhaos", "external_lib")
    if EXTERNAL_LIB not in sys.path:
        sys.path.append(EXTERNAL_LIB)
        print("Added '{}' to path".format(EXTERNAL_LIB))

    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging", "out_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))

    return logger



if __name__ == "__main__":


    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["HADOOP_CONF_DIR"] = "/opt/cloudera/parcels/SPARK2/lib/spark2/conf/yarn-conf"


    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -

    print("Process started...")


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INITIALIZE DATA PREPARATION PROCESS
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #logger = set_paths_and_logger()

    # print("SPARK_HOME", os.environ["SPARK_HOME"])
    # print("HADOOP_CONF_DIR", os.environ["HADOOP_CONF_DIR"])

    # from churn.utils.general_functions import init_spark
    # spark = init_spark("predict_and_delivery")
    #
    # print("SPARK_HOME", os.environ["SPARK_HOME"])
    # print("HADOOP_CONF_DIR", os.environ["HADOOP_CONF_DIR"])

    # import sys, os
    #
    # print('sys.argv[0] =', sys.argv[0]) # file
    # pathname = os.path.dirname(sys.argv[0]) # abs path to file (not included)
    #
    # import imp
    # from os.path import dirname
    # import os
    #
    # if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
    #     from churn.utils.constants import CHURN_DELIVERIES_DIR
    #     root_dir = CHURN_DELIVERIES_DIR
    #     print("Delivery dir found!")
    # else:
    #     import re
    #     root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    # print("Detected {}".format(root_dir))
    #
    # from churn.utils.constants import CHURN_DELIVERIES_DIR
    # for mydir in ["use-cases", "pykhaos", "lib"]:
    #     mypath = os.path.join(root_dir, mydir)
    #     if mypath not in sys.path:
    #         sys.path.append(mypath)
    #         print("Added '{}' to path".format(mypath))


    to = "cristina.sanchez4@vodafone.com"
    subject = "hola!"
    message = "Aquí tienes el fichero. \n. Un saludo,Cristina"
    filename = "/var/SP/data/home/csanc109/data/hola.txt"

    import subprocess
    cmd = '/var/SP/data/home/csanc109 $ ls src/devel/pykhaos/delivery/justshare/justshare_upload_linux.sh {to} {subject} {message} {filename}'.format(to=to,
                                                                                                                                                      subject=subject,
                                                                                                                                                      message=message,
                                                                                                                                                      filename=filename).split()
    return_state = subprocess.check_output(cmd)

    print("exit '{}'".format(return_state))


