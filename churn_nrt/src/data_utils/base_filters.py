

import os
import sys
import time
import math
from pyspark.sql.functions import (udf,
                                   col,
                                   decode,
                                   when,
                                   lit,
                                   lower,
                                   concat,
                                   translate,
                                   count,
                                   sum as sql_sum,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   greatest,
                                   least,
                                   isnull,
                                   isnan,
                                   struct,
                                   substring,
                                   size,
                                   length,
                                   year,
                                   month,
                                   dayofmonth,
                                   unix_timestamp,
                                   date_format,
                                   from_unixtime,
                                   datediff,
                                   to_date,
                                   desc,
                                   asc,
                                   lpad,
                                   countDistinct,
                                   row_number,
                                   regexp_replace,
                                   upper,
                                   trim,
                                   array,
                                   create_map,
                                   randn,
                                   split)


from churn_nrt.src.utils.constants import LEVEL_MSISDN, LEVEL_NIF, LEVEL_NC
from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data.ccc import CCC


def keep_active_services(df):
    return df.where(col("cod_estado_general").isin("01", "09"))

def keep_active_and_debt_services(df):
    return df.where(col("cod_estado_general").isin("01","03", "09"))



def get_mobile_base(spark, date_, save=False):
    df_cust_base = CustomerBase(spark).get_module(date_, save=save, save_others=save).where(col("rgu") == "mobile")
    # These tariffs are associated to Homezone
    df_cust_base = df_cust_base.where(((col("TARIFF").isNull()) | ((~col("TARIFF").isin("DTVC2", "DTVC5")))))
    return df_cust_base


def get_non_recent_customers_filter(spark, date_, n_days, level=LEVEL_MSISDN, verbose = False, only_active=True):
    '''
    Return the customers that have been in our base for the last n_days
    :param spark:
    :param date_:
    :param n_days:
    :param level:
    :param verbose:
    :param only_active: return only active customers
    :return:
    '''

    if('nif' in level):
        level='nif_cliente'

    valid_rgus = ['mobile'] if level==LEVEL_MSISDN else ['fbb', 'mobile', 'tv', 'bam_mobile', 'fixed', 'bam']

    # REMOVING RECENT CUSTOMERS (EARLY CHURN EFFECT IS REMOVED)

    # Only MSISDN active for at least the last N days
    from churn_nrt.src.utils.date_functions import move_date_n_days
    prev_date_ = move_date_n_days(date_, -n_days)

    from churn_nrt.src.data.customer_base import CustomerBase

    current_base_df = CustomerBase(spark).get_module(date_, save=False, save_others=False, add_tgs=False).filter(col('rgu').isin(valid_rgus)).select(level).distinct()
    if only_active:
        current_base_df = keep_active_services(current_base_df)


    #current_base_df = get_customer_base(spark, date_).filter(col('rgu').isin(valid_rgus)).select(level).distinct()

    size_current_base = current_base_df.count()

    prev_base_df = CustomerBase(spark).get_module(prev_date_, save=False, save_others=False, add_tgs=False).filter(col('rgu').isin(valid_rgus)).select(level).distinct()
    if only_active:
        prev_base_df = keep_active_services(prev_base_df)

    #prev_base_df = get_customer_base(spark, prev_date_).filter(col('rgu').isin(valid_rgus)).select(level).distinct()

    #size_prev_base = prev_base_df.count()

    active_base_df = current_base_df.join(prev_base_df, [level], 'inner').select(level).distinct()

    size_active_base = active_base_df.count()

    if(verbose):
        print '[Info get_active_filter] Services retained after the active filter for ' + str(date_) + ' and ' + str(n_days) + ' is ' + str(size_active_base) + ' out of ' + str(size_current_base)

    return active_base_df

def get_disconnection_process_filter(spark, date_, n_days, verbose = False, level="nif_cliente"):
    ''''
    Return a pyspark df with column nif_cliente if level="nif_cliente" or a column "num_cliente" if level="num_cliente"
    '''

    if n_days <= 0:
        print("get_disconnection_process_filter | n_days must be greater than 0!")
        import sys
        sys.exit()


    # REMOVING CUSTOMERS WHO HAVE INIT THE DISCONNECTION PROCESS
    from churn_nrt.src.data.customer_base import CustomerAdditional
    base_df = CustomerAdditional(spark, days_before=n_days)\
        .get_module(date_, save=False, save_others=False, level=level)\

    #base_df = get_customer_base_attributes(spark, date_, days_before=n_days)

    # Retaining NIFs with >= number of mobile & >= number of FBBs & >= number of TVs
    # 'fbb_services_<nif/nc>', 'mobile_services_<nif/nc>', 'tv_services_<nif/nc>'

    if level.lower() == "nif_cliente":
        suffix = "nif"
    else:
        suffix = "nc"

    non_churning_df = base_df.filter((col('inc_nb_fbb_services_'+suffix) >= 0) & (col('inc_nb_mobile_services_'+suffix) >= 0) & (col('inc_nb_tv_services_'+suffix) >= 0)).select(level).distinct()

    if(verbose):
        print '[Info get_disconnection_process_filter] Customers retained after the disconnection process filter is ' + str(non_churning_df.count()) + ' out of ' + str(base_df.count())

    return non_churning_df

def get_churn_call_filter(spark, date_, n_days, level = LEVEL_MSISDN, filter_function=None, verbose=False):
    '''

    :param spark:
    :param date_:
    :param n_days:
    :param level:
    :param filter_function: Function to filter the customer base in CCC module
    :param verbose:
    :return:
    '''

    level = level.lower()

    if n_days <= 0:
        print("get_disconnection_process_filter | n_days must be greater than 0!")
        import sys
        sys.exit()

    if level not in [LEVEL_NIF, LEVEL_MSISDN, LEVEL_NC]:
        print("[ERROR] Unknown level '{}'. Program will exit here!".format(level))
        import sys
        sys.exit()

    # CCC module works with msisdn and nif. When num_cliente is passed to get_churn_call_filter, we call ccc with level msisdn and then we obtain the corresponding num_cliente
    ccc_df = CCC(spark, level="msisdn" if level != LEVEL_NIF else level).get_module(date_, save=False, save_others=False, n_periods=n_days, period="days", filter_function=filter_function)
    ccc_df = ccc_df.filter(col('CHURN_CANCELLATIONS') == 0)

    no_churn_call_df = None
    if level == LEVEL_MSISDN:
        no_churn_call_df = ccc_df.select('msisdn').distinct()
    elif level == LEVEL_NIF:
        no_churn_call_df = ccc_df.select('nif_cliente').distinct()
    elif level == LEVEL_NC:
        base_df = CustomerBase(spark).get_module(date_, save=False, add_tgs=False).select("num_cliente", "msisdn").distinct()
        no_churn_call_df = ccc_df.select('msisdn').distinct().join(base_df, on=["msisdn"], how="inner").select("num_cliente").distinct()


    if (verbose):
        print '[Info get_disconnection_process_filter] Services retained after the churn call filter is ' + str(no_churn_call_df.count())

    return no_churn_call_df

def get_active_and_mobile(base):
    return base.where(col("cod_estado_general").isin("01", "09")).where(col('rgu')=='mobile')

def get_churn_call_filter_initial(spark, date_, n_days, level = LEVEL_MSISDN, filter_function=None, verbose=False):
    '''

    :param spark:
    :param date_:
    :param n_days:
    :param level:
    :param filter_function: Function to filter the customer base in CCC module
    :param verbose:
    :return:
    '''



    if level not in [LEVEL_NIF, LEVEL_MSISDN]:
        print("[ERROR] Unknown level {}. Program will exit here!".format(level))
        import sys
        sys.exit()

    from churn_nrt.src.data.customer_base import CustomerBase


    base_df = CustomerBase(spark).get_module(date_, save=False, add_tgs=False)
    base_df = keep_active_services(base_df)

    from churn_nrt.src.data.ccc import CCC
    ccc_df = CCC(spark, level=level).get_module(date_, save=False, save_others=False, n_periods=n_days, period="days", filter_function=get_active_and_mobile)

    ccc_base_df = base_df.join(ccc_df,['nif_cliente'],'left').fillna({'CHURN_CANCELLATIONS_w8':0.0})
    ccc_base_df = ccc_base_df.filter(col('CHURN_CANCELLATIONS_w8') == 0)

    if level == LEVEL_MSISDN:
        no_churn_call_df = ccc_base_df.select('msisdn').distinct()
    elif level == LEVEL_NIF:
        no_churn_call_df = ccc_base_df.select('nif_cliente').distinct()


    if (verbose):
        print '[Info get_disconnection_process_filter] Services retained after the churn call filter is ' + str(no_churn_call_df.count()) + ' out of ' + str(base.count())

    return no_churn_call_df

def get_forbidden_orders_filter(spark, date_, level=LEVEL_MSISDN, verbose=False, only_active=True):

    if level.lower() not in ["msisdn", "nif_cliente", "num_cliente"]:
        print("[ERROR] level must be equal to msisdn, nif_cliente, num_cliente")
        import sys
        sys.config(1)

    from churn_nrt.src.data.customer_base import CustomerBase

    current_base_df = CustomerBase(spark).get_module(date_, save=False, add_tgs=False).select('nif_cliente', 'msisdn', "num_cliente")
    if only_active:
        current_base_df = keep_active_services(current_base_df)

    from churn_nrt.src.data.orders_sla import OrdersSLA
    tr_orders = OrdersSLA(spark).get_module(date_).select('nif_cliente','has_forbidden_orders_last90')
    base_orders = current_base_df.join(tr_orders, ['nif_cliente'], 'left').fillna({'has_forbidden_orders_last90': 0.0})
    base_orders = base_orders.where(~(col("has_forbidden_orders_last90") > 0))

    if level == LEVEL_MSISDN:
        base_orders = base_orders.select('msisdn').distinct()
        level_ = 'msisdn'
    elif level == LEVEL_NIF:
        base_orders = base_orders.select('nif_cliente').distinct()
        level_ = 'nif_cliente'
    else:
        base_orders = base_orders.select('num_cliente').distinct()
        level_ = 'num_cliente'
    if (verbose):
        if level_ == 'msisdn':
            print'[Info get_forbidden_orders_filter] Services retained after the forbidden_orders filter is ' + str(base_orders.count()) + ' out of ' + str(current_base_df.select(level_).distinct().count())
        else:
            print'[Info get_forbidden_orders_filter] NIFs retained after the forbidden_orders filter is ' + str(base_orders.count()) + ' out of ' + str(current_base_df.select(level_).distinct().count())

    return base_orders