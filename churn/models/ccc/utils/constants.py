
import socket
import os
from pykhaos.utils.constants import WHOAMI


HOSTNAME = socket.gethostname()

if HOSTNAME == "E0413723.local" and WHOAMI == "csanc109":
    SAVING_PATH = os.path.join("/Users", "csanc109", "Documents", "Projects", "Churn", "CCC_model")
elif HOSTNAME == "vgddp387hr.dc.sedc.internal.vodafone.com" and WHOAMI == "csanc109":
    SAVING_PATH = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "churn", "ccc")
else:
    print("[ERROR] Hostname {} and user  {} has not set the paths for loading and storing. Please, set models/ccc/utils/constants.py and re-run again".format(HOSTNAME, WHOAMI))
    import sys
    sys.exit()