# coding: utf-8

import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, desc
from pyspark.sql.utils import AnalysisException
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

FIELD_LENGTH = 19


SECTION_CAR = "CAR"
SECTION_CAR_PREPARADO = "CAR PREPARADO"
SECTION_JOIN_CAR = "JOIN PREVIOUS CAR"
SECTION_EXTRA_FEATS = "EXTRA FEATS"
SECTION_SCORES_NEW = "SCORES"
SECTION_DP_LEVERS = "DP LEVERS"


def print_result(res):
    return "[OK]" if res else "{KO}"

def print_perc(result_ok, perc_completed):
    return "{" + str(perc_completed) + "%" + "}" if not result_ok else "[100%]"




def print_msg_car(car_ok, car_msg, perc_completed):
    if logger: logger.info("{res: >4} {section: <{padding}} | {msg}".format(res="{" + str(perc_completed)+"%"+"}" if not car_ok else "[100%]",
                                               section=SECTION_CAR,
                                               msg="All car modules exist" if car_ok else "",
                                               padding=FIELD_LENGTH
                          ))
    if not car_ok:
        for mm in car_msg:
            #if logger: logger.info("     | {}".format(mm.strip()))
            if logger: logger.info("     {}".format(mm))

def print_msg_car_preparado(car_preparado_ok, car_preparado_msg):
    if logger: logger.info("{res: >6}   {section: <{padding}} | {msg}".format(res=print_result(car_preparado_ok),
                                               section=SECTION_CAR_PREPARADO,
                                               msg=car_preparado_msg.strip(),
                                               padding = FIELD_LENGTH
    ))

def print_msg_join_car(join_car_ok, join_car_msg):
    if logger: logger.info("{res: >6}   {section: <{padding}} | {msg}".format(res=print_result(join_car_ok),
                                               section=SECTION_JOIN_CAR,
                                               msg=join_car_msg.strip(),
                                               padding=FIELD_LENGTH
                           ))

def print_msg_extra_feats_msg(extra_feats_ok, extra_feats_msg, perc_completed):
    if logger: logger.info("{res: >4} {section: <{padding}} | {msg}".format(res="{" + str(perc_completed)+"%"+"}" if not extra_feats_ok else "[100%]",
                                               section=SECTION_EXTRA_FEATS,
                                               msg="All extra feats modules exist" if extra_feats_ok else "",
                                               padding=FIELD_LENGTH
                           ))
    if not extra_feats_ok:
        for mm in extra_feats_msg:
            #if logger: logger.info("     | {}".format(mm.strip()))
            if logger: logger.info("     {}".format(mm))


def print_msg_scores(scores_ok, scores_msg, segment):
    if logger: logger.info("{res: >6}   {section: <{padding}} | {msg}".format(res=print_result(scores_ok),
                                                  section=SECTION_SCORES_NEW+ " " + segment,
                                                  msg=scores_msg.strip(),
                                                  padding = FIELD_LENGTH
                           ))


def print_msg_dp_levers(dp_levers_ok, dp_levers_msg, perc_completed):
    if isinstance(dp_levers_msg, str):
        if logger:logger.info("{res: >6}   {section: <{padding}} | {msg}".format(res=print_result(dp_levers_ok),
                                                               section=SECTION_DP_LEVERS,
                                                               msg=dp_levers_msg,
                                                               padding = FIELD_LENGTH
                                                    ))
    else:

        if logger: logger.info("{res: >6}   {section: <{padding}} | {msg}".format(res=print_result(dp_levers_ok) if perc_completed==-1 else print_perc(dp_levers_ok, perc_completed),
                                                               section=SECTION_DP_LEVERS,
                                                               msg=dp_levers_msg[0],
                                                               padding = FIELD_LENGTH
                                                    ))
        if len(dp_levers_msg) > 1:
            for i in range(1, len(dp_levers_msg)):
                if logger: logger.info("{}{}".format(" " * FIELD_LENGTH, dp_levers_msg[i]))

