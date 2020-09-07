
import datetime as dt

from dateutil.relativedelta import relativedelta
import pandas as pd
import sys
from pyspark.sql.functions import year

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


def days_range(start_yyyymmdd, end_yyyymmdd):
    """
    start_yyyymmdd = "20190129"
    end_yyyymmdd = "20190204"
    returns ['20190129', '20190130', '20190131', '20190201', '20190202', '20190203', '20190204']
    """
    rr = pd.date_range(dt.datetime.strptime(start_yyyymmdd, "%Y%m%d"), dt.datetime.strptime(end_yyyymmdd, "%Y%m%d"), freq="D")
    return [d.strftime("%Y%m%d") for d in rr]


def get_previous_cycle(date_, str_fmt="%Y%m%d"):

    if isinstance(date_,str) or isinstance(date_,unicode) :
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_

    day = date_obj.day

    if day <= 7:
        latest_cycle = get_last_day_of_month(date_obj + relativedelta(months=-1))
    elif day>=28:
        latest_cycle = dt.datetime.strptime(
            "{}{:02d}{:02d}".format(date_obj.year, date_obj.month, 21), "%Y%m%d")
    else:
        new_day = date_obj.day - date_obj.day % 7 if date_obj.day%7!=0 else (date_obj.day - 7)
        latest_cycle = dt.datetime.strptime(
            "{}{:02d}{:02d}".format(date_obj.year, date_obj.month, new_day), "%Y%m%d")
    return latest_cycle.strftime(str_fmt) if (isinstance(date_, str) or isinstance(date_, unicode)) else latest_cycle

def get_next_cycle(date_, str_fmt="%Y%m%d"):
    if isinstance(date_,str) or isinstance(date_,unicode):
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_

    if date_obj.day < 21:
        next_cycle = dt.datetime.strptime(
            "{}{:02d}{:02d}".format(date_obj.year, date_obj.month, (int(date_obj.day / 7) + 1) * 7), "%Y%m%d")
    elif get_last_day_of_month(date_)==date_:
        next_cycle = get_next_cycle(date_obj + relativedelta(days=+1)) # advance one day and look for the next cycle
    else:
        next_cycle = get_last_day_of_month(date_obj)

    return next_cycle.strftime(str_fmt) if (isinstance(date_, str) or isinstance(date_, unicode)) else next_cycle


def is_cycle(date_, str_fmt="%Y%m%d"):
    '''
    True if date_ is a cycle (valid date to be used in closing_day for car, deliveries, ...)
    :param date_:
    :param str_fmt:
    :return:
    '''

    if isinstance(date_,str) or isinstance(date_,unicode):
        date_obj = dt.datetime.strptime(date_, str_fmt)
    else:
        date_obj = date_

    return date_obj == get_next_cycle(get_previous_cycle(date_obj))





def move_date_n_yearmonths(yyyymm, n):
    '''
    if n is positive --> move the date forward
    :param date_: str with format YYYYMM or YYYYMMDD
    :param n:
    :param str_fmt:
    :return:
    '''
    if n==0: return yyyymm
    return move_date_n_cycles(yyyymm+"01", 4*n, str_fmt="%Y%m%d")[:6]

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
    if isinstance(_date,str) or isinstance(_date,unicode):
        date_obj = dt.datetime.strptime(_date, str_fmt)
    else:
        date_obj = _date
    yesterday_obj = (date_obj + dt.timedelta(days=n))

    return yesterday_obj.strftime(str_fmt) if (isinstance(_date,str) or isinstance(_date, unicode)) else yesterday_obj

def convert_to_date(dd_str):
    import datetime as dt
    if dd_str in [None, ""] or dd_str != dd_str: return None

    dd_obj = dt.datetime.strptime(dd_str.replace("-", "").replace("/", ""), "%Y%m%d")
    if dd_obj < dt.datetime.strptime("19000101", "%Y%m%d"):
        return None

    return dd_obj.strftime("%Y-%m-%d %H:%M:%S") if dd_str and dd_str == dd_str else dd_str


def count_nb_cycles(date_start, date_end):
    '''
    Return the number of cycles between date_start and date_end
    If date_start < date_end --> returns a positive number
    If date_start > date_end --> return a negativa number
    :param date_start:
    :param date_end:
    :return:
    '''
    num_cycles = 0
    date_A = date_start
    date_B = date_end
    delta = +1
    if date_start > date_end:
        delta = -1
        date_B = date_start
        date_A = date_end

    if date_A < date_B:
        dd = date_A
        while dd < date_B:
            dd = move_date_n_cycles(dd, n=+1)
            num_cycles = num_cycles + delta
        return num_cycles

    return num_cycles


def get_next_dow(weekday, from_date=None):
    '''
    weekday: weekday is 1 for monday; 2 for tuesday; ...; 7 for sunday.
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=5) [get next friday] and the function returns datetime.date(2019, 6, 14) [14-June-2019]
    E.g. Today is Tuesday 11-June-2019, we run the function get_next_dow(dow=2) [get next tuesday] and the function returns datetime.date(2019, 6, 11) [11-June-2019, Today]

    Note: weekday=0 is the same as weekday=7
    Note: this function runs with isoweekday (monday is 1 not 0)

    from_date: if from_date != None, instead of using today uses this day.

    '''

    from_date = from_date if from_date else dt.date.today()

    return from_date + dt.timedelta( (weekday-from_date.isoweekday()) % 7 )


def get_diff_days(start_date, end_date, format_date="%Y%m%d"):
    '''
    Compute the difference (in days) between end_date and start_date.
    Difference is positive if end_date > start_date
    :param start_date: str with a date. if not specified, format is assumed to be YYYYMMDD
    :param end_date:  str with a date. if not specified, format is assumed to be YYYYMMDD
    :param format_date:
    :return:
    '''
    d0 = dt.datetime.strptime(start_date, format_date)
    d1 = dt.datetime.strptime(end_date, format_date)
    return (d1 - d0).days



def is_null_date(fecha):
    YEAR_NULL_TERADATA = 1753
    """
    As default null date in Teradata source is 1753, this function compares
    a given date with this value to identify null dates
    :param fecha:
    :return: True when provided date has 1753 as year
    """
    return year(fecha) == YEAR_NULL_TERADATA
