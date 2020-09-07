
import datetime as dt

from dateutil.relativedelta import relativedelta
import pandas as pd
import sys

def get_next_month(yearmonth):
    """
    Given a yyyymm string, returns the yyyymm string
    for the next month.
    """
    current_month = dt.datetime.strptime(yearmonth, "%Y%m")
    return (dt.datetime(current_month.year, current_month.month, 28) + dt.timedelta(days=4)).strftime("%Y%m")


def get_closure(yearmonth_str):
    """
    Given a yyyymm string, returns the closure date (last day of month)
    """
    next_yearmonth_str = get_next_month(yearmonth_str)
    next_datetime = dt.datetime.strptime(next_yearmonth_str, "%Y%m")
    return (next_datetime + dt.timedelta(days=-1)).strftime("%Y%m%d")


def get_last_day_of_month(mydate):
    """
        Given a string date (format YYYY-MM-DD or YYYYMMDD) or a datetime object,
        returns the last day of the month.
        Eg. mydate=2018-03-01 --> returns 2018-03-31

        The result is given in the same type as the input
    """

    if isinstance(mydate,str) or isinstance(mydate,unicode):
        my_date_fmt = "%Y-%m-%d" if "-" in mydate else "%Y%m%d"  # type: str
        mydate_obj = dt.datetime.strptime(mydate, my_date_fmt)
    else:
        mydate_obj = mydate
        my_date_fmt=None

    last_day_of_the_month = dt.datetime((mydate_obj + relativedelta(months=1)).year,
                                        (mydate_obj + relativedelta(months=1)).month, 1) - dt.timedelta(days=1)

    if isinstance(mydate, str) or  isinstance(mydate,unicode):
        return dt.datetime.strftime(last_day_of_the_month, my_date_fmt)

    return last_day_of_the_month



def months_range(start_yearmonth, end_yearmonth):
    """
    start_yearmonth = "201803"
    end_yearmonth = "201806"
    returns ['201803', '201804', '201805', '201806']
    """
    rr = pd.date_range(dt.datetime.strptime(start_yearmonth, "%Y%m"), dt.datetime.strptime(end_yearmonth, "%Y%m"), freq="MS")
    return [yearmonth.strftime("%Y%m") for yearmonth in rr]


def get_previous_cycle(date_, str_fmt="%Y%m%d"):

    if isinstance(date_,str):
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_

    day = date_obj.day

    if day <= 7:
        latest_cycle = get_last_day_of_month(date_obj + relativedelta(months=-1))
    elif day>=28:
        latest_cycle = dt.datetime.strptime(
            "{}{}{}".format(date_obj.year, date_obj.month, 21), "%Y%m%d")
    else:
        new_day = date_obj.day - date_obj.day % 7 if date_obj.day%7!=0 else (date_obj.day - 7)
        latest_cycle = dt.datetime.strptime(
            "{}{}{}".format(date_obj.year, date_obj.month, new_day), "%Y%m%d")
    return latest_cycle.strftime(str_fmt) if isinstance(date_, str) else latest_cycle

def get_next_cycle(date_, str_fmt="%Y%m%d"):
    if isinstance(date_,str):
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_

    if date_obj.day < 21:
        next_cycle = dt.datetime.strptime(
            "{}{}{}".format(date_obj.year, date_obj.month, (int(date_obj.day / 7) + 1) * 7), "%Y%m%d")
    elif get_last_day_of_month(date_)==date_:
        next_cycle = get_next_cycle(date_obj + relativedelta(days=+1)) # advance one day and look for the next cycle
    else:
        next_cycle = get_last_day_of_month(date_obj)

    return next_cycle.strftime(str_fmt) if isinstance(date_, str) else next_cycle





def move_date_n_yearmonths(yyyymm, n):
    '''
    if n is positive --> move the date forward
    :param date_: str with format YYYYMM or YYYYMMDD
    :param n:
    :param str_fmt:
    :return:
    '''
    if n==0: return yyyymm
    elif n<0:
        print("Not implemented yet")
        sys.exit(1)
    yyyymm_i = yyyymm[:6]
    for i in range(0, abs(n)):
        yyyymm_i = get_next_month(yyyymm_i) # if n>0 else get_previous_month(date_i, str_fmt=str_fmt)
    return yyyymm_i


def move_date_n_cycles(date_, n, str_fmt="%Y%m%d"):
    '''
    if n is positive --> move the date forward
    :param date_:
    :param n:
    :param str_fmt:
    :return:
    '''
    if n==0: return date_
    date_i = date_
    for i in range(0, abs(n)):
        date_i = get_next_cycle(date_i, str_fmt=str_fmt) if n>0 else get_previous_cycle(date_i, str_fmt=str_fmt)
    return date_i

def move_date_n_days(_date, n, str_fmt="%Y%m%d"):
    """"
    Returns a date corresponding to the previous day. Keeps the input format
    """
    if n==0: return _date
    if isinstance(_date,str):
        date_obj = dt.datetime.strptime(_date, str_fmt)
    else:
        date_obj = _date
    yesterday_obj = (date_obj + dt.timedelta(days=n))

    return yesterday_obj.strftime(str_fmt) if isinstance(_date,str) else yesterday_obj

def convert_to_date(dd_str):
    import datetime as dt
    if dd_str in [None, ""] or dd_str != dd_str: return None

    print(dd_str)

    dd_obj = dt.datetime.strptime(dd_str.replace("-", "").replace("/", ""), "%Y%m%d")
    if dd_obj < dt.datetime.strptime("19000101", "%Y%m%d"):
        return None

    return dd_obj.strftime("%Y-%m-%d %H:%M:%S") if dd_str and dd_str == dd_str else dd_str
