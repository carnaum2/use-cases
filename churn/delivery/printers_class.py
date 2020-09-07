# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc
from pyspark.sql.utils import AnalysisException
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

from churn.delivery.delivery_constants import *


HEADER_FORMAT = "{res: >{padding_res}} {section: <{padding}} | {msg}"

# Computing the position of the | in HEADER_FORMAT to align following lines
SPACES_TO_PIPE = PADDING_RES + FIELD_LENGTH + 4


def print_result(checker_obj):
    return "[OK]" if checker_obj.status else "{KO}"

def print_perc(checker_obj):
    return "{" + str(checker_obj.perc_completed) + "%" + "}" if not checker_obj.status else "[100%]"



def format_modules_msg(checker_obj):
    '''
    format msg of functions by module (amdocs car, extra feats)
    :param checker_obj:
    :param section_name: Section name to appear in the message header
    :return:
    '''
    msg = []
    msg.append(HEADER_FORMAT.format(res=print_perc(checker_obj),
                                               section=checker_obj.section_name,
                                               msg="All modules exist" if checker_obj.status else "",
                                               padding=FIELD_LENGTH, padding_res=PADDING_RES
                          ))
    if not checker_obj.status:
        for mm in checker_obj.msg:
            msg.append("     {}".format(mm))

    return msg


def format_table_msg(checker_obj):
    '''
    format msg for functions that check a table (car preparado, join car, msg_scores, dp_levers
    :param checker_obj:
    :return:
    '''

    if isinstance(checker_obj.msg, str):
        msg = HEADER_FORMAT.format(res=print_result(checker_obj),
                                                               section=checker_obj.section_name,
                                                               msg=checker_obj.msg,
                                                               padding = FIELD_LENGTH, padding_res=PADDING_RES
                                                    )
    else:
        msg = []
        msg.append(HEADER_FORMAT.format(res=print_result(checker_obj) if checker_obj.perc_completed==-1 else print_perc(checker_obj),
                                                               section=checker_obj.section_name,
                                                               msg=checker_obj.msg[0],
                                                               padding = FIELD_LENGTH, padding_res=PADDING_RES
                                                    ))
        if len(checker_obj.msg) > 1:
            for i in range(1, len(checker_obj.msg)):
                msg.append("{}> {}".format(" " * SPACES_TO_PIPE, checker_obj.msg[i]))

    return msg


def print_msg(msg):
    if isinstance(msg, str) and logger: logger.info(msg)
    elif isinstance(msg, list) and logger:
        for mm in msg:
            logger.info(mm)
    else:
        print("No logger or msg has unknown type {}".format(type(msg)))


