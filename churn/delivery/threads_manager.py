

import subprocess
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
from sys import version
import time


'''
Trick to store args in python 2.7. So , we can get the command under Popen object

'''

class Popen(subprocess.Popen):
    def __init__(self, *args, **kwds):
        subprocess.Popen.__init__(self, *args, **kwds)
        self.args = args[0]
        self.running_script = get_running_script(args[0], file_extension="py")
        self.start_timestamp = time.time()
        if logger: logger.info("Created Popen '{}'".format(self.running_script))

    def is_still_running(self):
        '''
        Returns True if the process is still running
        :param p_popen: instance of popen call
        :return:
        '''
        return self.poll() == None


    def __repr__(self):
        running_time = time.time() -  self.start_timestamp
        return "Process '{}' is {}".format(self.running_script, "has been running for {} minutes".format(int(running_time/60)) if self.is_still_running() else "not running")

    def __str__(self):
        return self.__repr__


    def wait_until_end(self, delay, max_wait=None):
        '''

        :param p_open_list:
        :param delay:
        :param max_wait:
        :return:
        '''
        if not self.is_still_running():
            return

        wait_time = 0

        while wait_time < max_wait or not max_wait:
            # while the process is still executing and we haven't timed-out yet
            if self.is_still_running():
                # do other things too if necessary e.g. print, check resources, etc.
                print("I am gonna wait {} secs right now".format(delay))
                time.sleep(delay)

                if self.is_still_running():
                    print(repr(self))
                    wait_time += delay
                    delay = min(delay, max_wait - wait_time) if max_wait else delay
                else:
                    print("'{}' has ended!".format(self.running_script))
                    break
            else:
                print("'{}' has ended!".format(self.running_script))
                break

        if self.is_still_running():
            if logger: logger.warning("Expired waiting time and process did not ended!")

        return self.poll()


# if version[0]<"3.3": # pre 3.3
#     subprocess.Popen = Popen
# else:
#     print("This script is not prepared for python >= 3.3")
#     import sys
#     sys.exit()

def launch_process(cmd, *args, **kwds):
    '''
    Launch a process in background
    :param cmd:
    :return:
    '''
    print(cmd)
    if logger: logger.info("Launching command '{}'".format( (" ".join(cmd) if isinstance(cmd,list) else cmd )))
    return Popen(cmd.split(" ") if isinstance(cmd,str) else cmd, *args, **kwds)


def get_running_script(cmd, file_extension="py"):
    '''
    return script in python + args
    :return:
    '''
    cmd = " ".join(cmd) if isinstance(cmd,list) else cmd

    import re
    gg = re.match("^.* (.*\." + file_extension + " .*)$", cmd)
    if gg:
        return gg.group(1)
    return None

def print_threads_status(popen_list):
    for p in popen_list:
        if logger: logger.info(repr(p))

def any_running(popen_list):
    return any([p.is_still_running() for p in popen_list])

