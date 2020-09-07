
from pyspark.sql.functions import collect_set, col, lit, collect_list, desc, asc, \
    count as sql_count, substring, from_unixtime, unix_timestamp, \
    desc, when, col, lit, udf, upper, lower, concat, max as sql_max, min as sql_min, least, row_number, array

import time
import sys

from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, FloatType, StringType, MapType, IntegerType
from pyspark.sql.functions import datediff

from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.utils.date_functions import convert_to_date, move_date_n_days, get_diff_days, move_date_n_cycles
from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.utils.constants import LEVEL_MSISDN, LEVEL_NIF, LEVEL_NC


def check_args(level, churn_window):
    if level not in [LEVEL_MSISDN, LEVEL_NIF, LEVEL_NC]:
        print("[ERROR] Target | Unknown level {}. Parameter 'level' must be one of '{}', '{}', '{}'".format(level, LEVEL_MSISDN, LEVEL_NIF, LEVEL_NC))
        print("[ERROR] Target | Program will exit here!")
        sys.exit()
    if churn_window < 0:
        print("[ERROR] Target | Churn window must be greater than 0")
        print("[ERROR] Target | Program will exit here!")
        sys.exit()

class FbbDx(DataTemplate):

    CHURN_WINDOW = 30

    def __init__(self, spark, churn_window):
        self.CHURN_WINDOW = churn_window
        DataTemplate.__init__(self, spark, "fbb_dx/{}".format(self.CHURN_WINDOW))

    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):

        self.check_valid_params(closing_day, **kwargs)

        date_init = closing_day

        from churn_nrt.src.data.customer_base import CustomerBase

        # The field msisdn is kept in order to provide a service-level labelling in the output DF

        # Current base (date_init)

        current_base = CustomerBase(self.SPARK) \
            .get_module(date_init, level="NUM_CLIENTE", save=save_others, save_others=save_others, add_tgs=False) \
            .filter(col('rgu') == 'fbb') \
            .select("num_cliente", 'msisdn', 'nb_fbb_services_nc', "cod_estado_general") \
            .distinct() \
            .repartition(400)

        from churn_nrt.src.utils.date_functions import move_date_n_days

        date_end = move_date_n_days(date_init, n=self.CHURN_WINDOW)

        from churn_nrt.src.data_utils.base_filters import keep_active_services
        current_base = keep_active_services(current_base) \
            .select("num_cliente", 'msisdn', 'nb_fbb_services_nc') \
            .distinct() \
            .withColumnRenamed('nb_fbb_services_nc', 'nb_fbb_services_nc_prev')

        current_base.cache()

        print " "
        print "[Info] Size of current_base: " + str(current_base.count()) + " - Num NCs: " + str(current_base.select('num_cliente').distinct().count()) + ' - Num msisdn: ' + str(current_base.select('msisdn').distinct().count())
        print " "

        # Target base (date_end)

        target_base = CustomerBase(self.SPARK) \
            .get_module(date_end, level="NUM_CLIENTE", save=save_others, save_others=save_others, add_tgs=False) \
            .filter(col('rgu') == 'fbb') \
            .select("num_cliente", 'nb_fbb_services_nc', "cod_estado_general") \
            .distinct() \
            .repartition(400)

        from churn_nrt.src.data_utils.base_filters import keep_active_and_debt_services
        target_base = keep_active_and_debt_services(target_base) \
            .select("num_cliente", 'nb_fbb_services_nc') \
            .distinct() \
            .withColumnRenamed('nb_fbb_services_nc', 'nb_fbb_services_nc_target')

        target_base.cache()

        print " "
        print "[Info] Size of target_base: " + str(target_base.count()) + " - Num NCs: " + str(target_base.select('num_cliente').distinct().count())
        print " "

        # It is not clear when the disconnection occurs. Thus, the mid point between both dates is assigned

        portout_date = move_date_n_days(date_init, int(self.CHURN_WINDOW / 2))

        # DX: for a given num_cliente, the number of FBB has decreased and results in 0 services

        churn_base = current_base \
            .join(target_base, ["num_cliente"], 'left') \
            .na.fill({'nb_fbb_services_nc_target': 0}) \
            .withColumn('label_dx', when((col('nb_fbb_services_nc_prev') > col('nb_fbb_services_nc_target')) & (col('nb_fbb_services_nc_target') == 0), 1.0).otherwise(0.0)) \
            .withColumn('portout_date_dx', from_unixtime(unix_timestamp(lit(portout_date), 'yyyyMMdd'))) \
            .select('msisdn', 'label_dx', 'portout_date_dx') \
            .distinct() \
            .filter(col('label_dx') == 1)

        print("[Info get_fbb_dxs] - DXs for FBB services during the period: " + date_init + " - " + date_end + ": " + str(churn_base.count()))

        return churn_base

    def check_valid_params(self, closing_day, **kwargs):
        return check_valid_params_generic(self, closing_day, **kwargs)




class FixPort(DataTemplate):

    CHURN_WINDOW = 30

    def __init__(self, spark, churn_window):
        self.CHURN_WINDOW = churn_window
        DataTemplate.__init__(self, spark, "fix_port/{}".format(self.CHURN_WINDOW))

    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):
        '''
        ['msisdn', 'label_srv', 'portout_date_fix']
        :param spark:
        :param yearmonthday:
        :param yearmonthday_target:
        :return:
        '''

        self.check_valid_params(closing_day, **kwargs)

        # mobile portout
        window_fix = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

        day_target = move_date_n_days(closing_day, n=self.CHURN_WINDOW)

        fixport = (self.SPARK.read.table("raw_es.portabilitiesinout_portafijo").filter(col("INICIO_RANGO") == col("FIN_RANGO"))
            .withColumnRenamed("INICIO_RANGO", "msisdn")
            .select("msisdn", "FECHA_INSERCION_SGP")
            .distinct()
            .withColumn("label_srv", lit(1.0))
            .withColumn("FECHA_INSERCION_SGP", substring(col("FECHA_INSERCION_SGP"), 0, 10))
            .withColumn('FECHA_INSERCION_SGP', from_unixtime(unix_timestamp(col('FECHA_INSERCION_SGP'), "yyyy-MM-dd")))
            .where((col('FECHA_INSERCION_SGP') >= from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")))
                    & (col('FECHA_INSERCION_SGP') <= from_unixtime(unix_timestamp(lit(day_target), "yyyyMMdd"))))
            .withColumnRenamed('FECHA_INSERCION_SGP', 'portout_date_fix')
            .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(closing_day[:4]), lit(closing_day[4:6]), lit(closing_day[6:])), 'yyyyMMdd')))
            .withColumn("days_from_portout", datediff(col("ref_date"), from_unixtime(unix_timestamp(col("portout_date_fix"), "yyyyMMdd"))).cast("int"))
            .withColumn("rank", row_number().over(window_fix))
            .where(col("rank") == 1)
            .select("msisdn", "label_srv", "portout_date_fix"))

        print("[Info get_fix_portout_requests] - Port-out requests for fixed services during period {}-{}: {}".format(closing_day, day_target, fixport.count()))

        return fixport

    def check_valid_params(self, closing_day, **kwargs):
        return check_valid_params_generic(self, closing_day, **kwargs)




class MobPort(DataTemplate):

    CHURN_WINDOW = 30

    def __init__(self, spark, churn_window=30):
        self.CHURN_WINDOW = churn_window
        DataTemplate.__init__(self, spark, "mob_port/{}".format(self.CHURN_WINDOW))

    def build_module(self, closing_day, save_others, **kwargs):
        '''
        ['msisdn', 'label_mob', 'portout_date_mob']
        :param spark:
        :param start_port:
        :param end_port:
        :return:
        '''

        self.check_valid_params(closing_day, **kwargs)


        day_target = move_date_n_days(closing_day, n=self.CHURN_WINDOW)


        # mobile portout
        window_mobile = Window.partitionBy("msisdn").orderBy(desc("days_from_portout"))  # keep the 1st portout

        PORT_TABLE_NAME = "raw_es.portabilitiesinout_sopo_solicitud_portabilidad"

        df_sol_port = (self.SPARK.read.table(PORT_TABLE_NAME)
                       .withColumn("sopo_ds_fecha_solicitud", substring(col("sopo_ds_fecha_solicitud"), 0, 10))
                       .withColumn('sopo_ds_fecha_solicitud', from_unixtime(unix_timestamp(col('sopo_ds_fecha_solicitud'), "yyyy-MM-dd")))
                       .where((col("sopo_ds_fecha_solicitud") >= from_unixtime(unix_timestamp(lit(closing_day), 'yyyyMMdd'))) & (col("sopo_ds_fecha_solicitud") <= from_unixtime(unix_timestamp(lit(day_target), 'yyyyMMdd'))))
                       .withColumnRenamed("SOPO_DS_MSISDN1", "msisdn")
                       .withColumnRenamed("sopo_ds_fecha_solicitud", "portout_date")
                       .withColumn("ref_date", from_unixtime(unix_timestamp(concat(lit(closing_day[:4]), lit(closing_day[4:6]), lit(closing_day[6:])), 'yyyyMMdd')))
                       .withColumn("days_from_portout", datediff(col("ref_date"), col("portout_date")).cast("int")).withColumn("rank", row_number().over(window_mobile)).where(col("rank") == 1))

        df_sol_port = df_sol_port\
            .withColumn('target_operator', when(((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "735044")) | ((col("SOPO_CO_RECEPTOR") == "YOIGO") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0")), "masmovil")\
            .otherwise(when((col("SOPO_CO_RECEPTOR") == "MOVISTAR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), "movistar")\
            .otherwise(when((col("SOPO_CO_RECEPTOR") == "AMENA") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), "orange")\
            .otherwise(when((col("SOPO_CO_RECEPTOR") == "EUSKALTEL") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), "euskaltel") \
            .otherwise(when((col("SOPO_CO_RECEPTOR") == "MUNDOR") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), "r") \
            .otherwise(when((col("SOPO_CO_RECEPTOR") == "TELECABLE") & (col("SOPO_CO_NRN_RECEPTORVIR") == "0"), "telecable") \
            .otherwise("others")))))))

        df_sol_port = df_sol_port.withColumn("label_mob", lit(1.0)) \
            .select("msisdn", "label_mob", "portout_date","target_operator") \
            .withColumnRenamed("portout_date", "portout_date_mob")

        print("[Info get_mobile_portout_requests] - Port-out requests for mobile services during period " + closing_day + "-" + day_target + ": " + str(df_sol_port.count()))

        return df_sol_port

    def check_valid_params(self, closing_day, **kwargs):
        return check_valid_params_generic(self, closing_day, **kwargs)

class Target(DataTemplate):

    CHURN_WINDOW = 30
    LEVEL = None

    def __init__(self, spark, churn_window=30, level=LEVEL_NIF):
        check_args(level,churn_window)
        self.CHURN_WINDOW = churn_window
        self.LEVEL = level

        DataTemplate.__init__(self, spark,  "target/{}/{}".format(self.LEVEL,self.CHURN_WINDOW))


    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):

        self.check_valid_params(closing_day, **kwargs)

        if self.LEVEL == LEVEL_NIF:
            lev = 'nif_cliente'
        elif self.LEVEL == LEVEL_NC:
            lev = 'num_cliente'
        else:
            lev = 'msisdn'

        # Getting portout requests for fix and mobile services, and disconnections of fbb services
        print("******* Asking for FixPort...")
        df_sopo_fix = FixPort(self.SPARK, self.CHURN_WINDOW).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        print("******* Asking for FbbDx...")
        df_baja_fix = FbbDx(self.SPARK, self.CHURN_WINDOW).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        print("******* Asking for MobPort...")
        df_sol_port = MobPort(self.SPARK, self.CHURN_WINDOW).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)

        # The base of active services on closing_day
        df_services = CustomerBase(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)

        # 1 if any of the services of this nif is 1
        window_nc = Window.partitionBy(lev)

        df_target_cust = (df_services.join(df_sopo_fix, ['msisdn'], "left").na.fill({'label_srv': 0.0})
                                     .join(df_baja_fix, ['msisdn'], "left").na.fill({'label_dx': 0.0})
                                     .join(df_sol_port, ['msisdn'], "left").na.fill({'label_mob': 0.0})
                                     .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0), 1.0).otherwise(0.0))
                                     .withColumn('label', sql_max('tmp').over(window_nc)).drop("tmp"))

        def get_churn_reason(dates):

            reasons = ['mob', 'fix', 'fbb']

            sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

            sorted_reasons = [reasons[idx] for idx in sorted_dates if ((dates[idx] is not None) & (dates[idx] != '') & (dates[idx] != ' '))]

            if not sorted_reasons:
                reason = None
            else:
                reason = sorted_reasons[0]

            return reason

        get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

        df_target_cust = df_target_cust.select(lev, "label", 'portout_date_mob', 'portout_date_fix', 'portout_date_dx')\
            .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc)) \
            .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc)) \
            .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc)) \
            .select(lev, "label", 'min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx') \
            .distinct() \
            .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx')) \
            .withColumn('reason', get_churn_reason_udf(col('dates'))) \
            .withColumn('reason', when(col('label' )==0.0, '').otherwise(col('reason'))) \
            .withColumn('portout_date', least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx'))) \
            .select(lev, "label", 'portout_date', 'reason').drop_duplicates()

        return df_target_cust

    def check_valid_params(self, closing_day, **kwargs):
        return check_valid_params_generic(self, closing_day, **kwargs)



class Target_FBB(DataTemplate):

    CHURN_WINDOW = 30
    LEVEL = None

    def __init__(self, spark, churn_window=30, level=LEVEL_NIF):
        check_args(level,churn_window)
        self.CHURN_WINDOW = churn_window
        self.LEVEL = level

        DataTemplate.__init__(self, spark,  "target_fbb/{}/{}".format(self.LEVEL,self.CHURN_WINDOW))


    def build_module(self, closing_day, save_others, force_gen=False, **kwargs):

        self.check_valid_params(closing_day, **kwargs)

        if self.LEVEL == LEVEL_NIF:
            lev = 'nif_cliente'
        elif self.LEVEL == LEVEL_NC:
            lev = 'num_cliente'
        else:
            lev = 'msisdn'

        # Getting portout requests and disconnections of fbb services
        print("******* Asking for FixPort...")
        df_sopo_fix = FixPort(self.SPARK, self.CHURN_WINDOW).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)
        print("******* Asking for FbbDx...")
        df_baja_fix = FbbDx(self.SPARK, self.CHURN_WINDOW).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)

        # The base of active services on closing_day
        df_services = CustomerBase(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=force_gen)

        # 1 if any of the services of this nif is 1
        window_nc = Window.partitionBy(lev)

        df_target_cust = (df_services.join(df_sopo_fix, ['msisdn'], "left").na.fill({'label_srv': 0.0})
                                     .join(df_baja_fix, ['msisdn'], "left").na.fill({'label_dx': 0.0})
                                     .withColumn('tmp', when((col('label_srv') == 1.0) | (col('label_dx') == 1.0), 1.0).otherwise(0.0))
                                     .withColumn('label', sql_max('tmp').over(window_nc)).drop("tmp"))

        def get_churn_reason(dates):

            reasons = ['fix', 'fbb']

            sorted_dates = sorted(range(len(dates)), key=lambda k: dates[k])

            sorted_reasons = [reasons[idx] for idx in sorted_dates if ((dates[idx] is not None) & (dates[idx] != '') & (dates[idx] != ' '))]

            if not sorted_reasons:
                reason = None
            else:
                reason = sorted_reasons[0]

            return reason

        get_churn_reason_udf = udf(lambda z: get_churn_reason(z), StringType())

        df_target_cust = df_target_cust.select(lev, "label", 'portout_date_fix', 'portout_date_dx')\
            .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc)) \
            .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc)) \
            .select(lev, "label", 'min_portout_date_fix', 'min_portout_date_dx') \
            .distinct() \
            .withColumn('dates', array('min_portout_date_fix', 'min_portout_date_dx')) \
            .withColumn('reason', get_churn_reason_udf(col('dates'))) \
            .withColumn('reason', when(col('label' )==0.0, '').otherwise(col('reason'))) \
            .withColumn('portout_date', least( col('min_portout_date_fix'), col('min_portout_date_dx'))) \
            .select(lev, "label", 'portout_date', 'reason').drop_duplicates()

        return df_target_cust

    def check_valid_params(self, closing_day, **kwargs):
        return check_valid_params_generic(self, closing_day, **kwargs)


def check_valid_params_generic(data_template_obj, closing_day, **kwargs):
    '''
    Check:
       1. closing_day + churn_window is compatible with today, i.e., it is not a date in the future
       2. churn_window value is not passed in get_module call. It must be pass when calling the constructor

    :param data_template_obj: object (Target, MobPort, ...)
    :param closing_day: closing_day specified in get_module call
    :param kwargs: Other args in get_module call
    :return:
    '''

    if kwargs.get('churn_window', None) != None:
        print("[ERROR] churn_window must be specified only in the __init__ function, do not pass it in the get_module call")
        sys.exit()
    from churn_nrt.src.utils.date_functions import move_date_n_days
    import datetime as dt
    today_str = dt.datetime.today().strftime("%Y%m%d")
    if move_date_n_days(closing_day, n=(data_template_obj.CHURN_WINDOW + 3)) > today_str:
        print("ERROR: closing_day [{0}] + churn_window [{1}] = {3}. It must be lower than today [{2}] - 3 days.".format(closing_day,
                                                                                                                    data_template_obj.CHURN_WINDOW,
                                                                                                                    today_str,
                                                                                                                    move_date_n_days(closing_day, n=(data_template_obj.CHURN_WINDOW + 3))))
        print("ERROR: Program will exit here!")
        sys.exit()
    print("[{}] check_valid_params | Params ok".format(data_template_obj.__class__.__name__))
    return True