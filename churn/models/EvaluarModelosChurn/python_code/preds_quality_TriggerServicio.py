# coding=utf-8

from common.src.main.python.utils.hdfs_generic import *
import os
import sys
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
                                    countDistinct,
                                    row_number,
                                    skewness,
                                    kurtosis,
                                    concat_ws)

import numpy as np


def set_paths():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and "dirs/dir1/"
    :return:
    '''
    from os.path import dirname
    import os
    import sys

    churnFolder = [ii for ii in sys.path if 'churn' in ii]
    USE_CASES = dirname(re.match("(.*)use-cases/churn", churnFolder[0]).group())

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))


####################################
### Creating Spark Session
###################################

def get_spark_session(app_name="default name", log_level='INFO', min_n_executors=1, max_n_executors=15, n_cores=4,
                      executor_memory="32g", driver_memory="32g"):
    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)

    setting_bdp(app_name=app_name, min_n_executors=min_n_executors, max_n_executors=max_n_executors, n_cores=n_cores,
                executor_memory=executor_memory, driver_memory=driver_memory)
    from common.src.main.python.utils.hdfs_generic import run_sc
    sc, spark, sql_context = run_sc(log_level=log_level)

    return sc, spark, sql_context


# set BDP parameters
def setting_bdp(min_n_executors=1, max_n_executors=15, n_cores=8, executor_memory="16g", driver_memory="8g",
                app_name="Python app", driver_overhead="1g", executor_overhead='3g'):
    MAX_N_EXECUTORS = max_n_executors
    MIN_N_EXECUTORS = min_n_executors
    N_CORES_EXECUTOR = n_cores
    EXECUTOR_IDLE_MAX_TIME = 120
    EXECUTOR_MEMORY = executor_memory
    DRIVER_MEMORY = driver_memory
    N_CORES_DRIVER = 1
    MEMORY_OVERHEAD = N_CORES_EXECUTOR * 2048
    QUEUE = "root.BDPtenants.es.medium"
    BDA_CORE_VERSION = "1.0.0"

    SPARK_COMMON_OPTS = os.environ.get('SPARK_COMMON_OPTS', '')
    SPARK_COMMON_OPTS += " --executor-memory %s --driver-memory %s" % (EXECUTOR_MEMORY, DRIVER_MEMORY)
    SPARK_COMMON_OPTS += " --conf spark.shuffle.manager=tungsten-sort"
    SPARK_COMMON_OPTS += "  --queue %s" % QUEUE

    # Dynamic allocation configuration
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.enabled=true"
    SPARK_COMMON_OPTS += " --conf spark.shuffle.service.enabled=true"
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.maxExecutors=%s" % (MAX_N_EXECUTORS)
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.minExecutors=%s" % (MIN_N_EXECUTORS)
    SPARK_COMMON_OPTS += " --conf spark.executor.cores=%s" % (N_CORES_EXECUTOR)
    SPARK_COMMON_OPTS += " --conf spark.dynamicAllocation.executorIdleTimeout=%s" % (EXECUTOR_IDLE_MAX_TIME)
    # SPARK_COMMON_OPTS += " --conf spark.ui.port=58235"
    SPARK_COMMON_OPTS += " --conf spark.port.maxRetries=100"
    SPARK_COMMON_OPTS += " --conf spark.app.name='%s'" % (app_name)
    SPARK_COMMON_OPTS += " --conf spark.submit.deployMode=client"
    SPARK_COMMON_OPTS += " --conf spark.ui.showConsoleProgress=true"
    SPARK_COMMON_OPTS += " --conf spark.sql.broadcastTimeout=1200"
    SPARK_COMMON_OPTS += " --conf spark.yarn.executor.memoryOverhead={}".format(executor_overhead)
    SPARK_COMMON_OPTS += " --conf spark.yarn.executor.driverOverhead={}".format(driver_overhead)

    BDA_ENV = os.environ.get('BDA_USER_HOME', '')

    # Attach bda-core-ra codebase
    SPARK_COMMON_OPTS += " --files {}/scripts/properties/red_agent/nodes.properties,{}/scripts/properties/red_agent/nodes-de.properties,{}/scripts/properties/red_agent/nodes-es.properties,{}/scripts/properties/red_agent/nodes-ie.properties,{}/scripts/properties/red_agent/nodes-it.properties,{}/scripts/properties/red_agent/nodes-pt.properties,{}/scripts/properties/red_agent/nodes-uk.properties".format(
        *[BDA_ENV] * 7)

    os.environ["SPARK_COMMON_OPTS"] = SPARK_COMMON_OPTS
    os.environ["PYSPARK_SUBMIT_ARGS"] = "%s pyspark-shell " % SPARK_COMMON_OPTS
    # os.environ["SPARK_EXTRA_CONF_PARAMETERS"] = '--conf spark.yarn.jars=hdfs:///data/raw/public/lib_spark_2_1_0_jars_SPARK-18971/*'


def initialize(app_name, min_n_executors=1, max_n_executors=15, n_cores=4, executor_memory="16g", driver_memory="8g"):
    import time
    start_time = time.time()

    print("_initialize spark")
    sc, spark, sql_context = get_spark_session(app_name=app_name, log_level="OFF", min_n_executors=min_n_executors,
                                               max_n_executors=max_n_executors, n_cores=n_cores,
                                               executor_memory=executor_memory, driver_memory=driver_memory)
    print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
                                                                         sc.defaultParallelism))
    return spark


def get_churn_target_nif_period_new(spark, closing_day, end_port):
#New target excluding "03" code
    df_sopo_fix = get_fix_portout_requests(spark, closing_day, end_port)
    from churn.analysis.triggers.orders.customer_master import getFbbDxsForCycleList_anyday
    from pykhaos.utils.date_functions import get_diff_days, move_date_n_days
    # set desconexion date to be in the middle point of both dates
    date_dx = move_date_n_days(closing_day, n=int(get_diff_days(closing_day, end_port, format_date="%Y%m%d")/2))
    df_baja_fix = getFbbDxsForCycleList_anyday(spark, closing_day, end_port).withColumn('portout_date_dx', lit(date_dx))
    # df_baja_fix = get_fbb_dxs(spark,closing_day, end_port)
    df_sol_port = get_mobile_portout_requests(spark, closing_day, end_port)

    # The base of active aervices on closing_day
    df_services = get_customer_base_segment(spark, closing_day)

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn'], "left")
                      .na.fill({'label_srv': 0.0})
                      .join(df_baja_fix, ['msisdn'], "left")
                      .na.fill({'label_dx': 0.0})
                      .join(df_sol_port, ['msisdn'], "left")
                      .na.fill({'label_mob': 0.0})
                      .withColumn('tmp',
                                  when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                       1.0).otherwise(0.0))
                      .withColumn('tmp_portout',
                                  when((col('label_srv') == 1.0) | (col('label_mob') == 1.0), 1.0).otherwise(0.0))
                      .withColumn('label', max('tmp').over(window_nc))
                      .withColumn('label_portout', max('tmp_portout').over(window_nc))
                      .drop("tmp").drop("tmp_portout"))

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'label_portout', 'portout_date_mob',
                                           'portout_date_fix', 'portout_date_dx') \
        .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc)) \
        .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc)) \
        .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc)) \
        .select("nif_cliente", "label", 'label_portout', 'min_portout_date_mob', 'min_portout_date_fix',
                'min_portout_date_dx') \
        .distinct() \
        .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx')) \
        .withColumn('portout_date',
                    least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx'))) \
        .select("nif_cliente", "label", 'label_portout', 'portout_date').drop_duplicates(
        ["nif_cliente", "label", 'portout_date'])

    return df_target_nifs



def get_churn_target_nif_period(spark, closing_day, end_port):

    # Getting portout requests for fix and mobile services, and disconnections of fbb services
    df_sopo_fix = get_fix_portout_requests(spark, closing_day, end_port)
    df_baja_fix = get_fbb_dxs(spark,closing_day, end_port)
    df_sol_port = get_mobile_portout_requests(spark, closing_day, end_port)

    # The base of active aervices on closing_day
    df_services = get_customer_base_segment(spark, closing_day)

    # 1 if any of the services of this nif is 1
    window_nc = Window.partitionBy("nif_cliente")

    df_target_nifs = (df_services.join(df_sopo_fix, ['msisdn'], "left")
                      .na.fill({'label_srv': 0.0})
                      .join(df_baja_fix, ['msisdn'], "left")
                      .na.fill({'label_dx': 0.0})
                      .join(df_sol_port, ['msisdn'], "left")
                      .na.fill({'label_mob': 0.0})
                      .withColumn('tmp',when((col('label_srv') == 1.0) | (col('label_dx') == 1.0) | (col('label_mob') == 1.0),
                                       1.0).otherwise(0.0))
                      .withColumn('tmp_portout',
                                  when((col('label_srv') == 1.0) | (col('label_mob') == 1.0), 1.0).otherwise(0.0))
                      .withColumn('label', max('tmp').over(window_nc))
                      .withColumn('label_portout', max('tmp_portout').over(window_nc))
                      .drop("tmp").drop("tmp_portout"))

    df_target_nifs = df_target_nifs.select("nif_cliente", "label", 'label_portout', 'portout_date_mob',
                                           'portout_date_fix', 'portout_date_dx') \
        .withColumn('min_portout_date_mob', sql_min('portout_date_mob').over(window_nc)) \
        .withColumn('min_portout_date_fix', sql_min('portout_date_fix').over(window_nc)) \
        .withColumn('min_portout_date_dx', sql_min('portout_date_dx').over(window_nc)) \
        .select("nif_cliente", "label", 'label_portout', 'min_portout_date_mob', 'min_portout_date_fix',
                'min_portout_date_dx') \
        .distinct() \
        .withColumn('dates', array('min_portout_date_mob', 'min_portout_date_fix', 'min_portout_date_dx')) \
        .withColumn('portout_date',
                    least(col('min_portout_date_mob'), col('min_portout_date_fix'), col('min_portout_date_dx'))) \
        .select("nif_cliente", "label", 'label_portout', 'portout_date').drop_duplicates(
        ["nif_cliente", "label", 'portout_date'])

    return df_target_nifs



if __name__ == "__main__":

    set_paths()

    from churn.models.fbb_churn_amdocs.utils_general import *
    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *
    from churn.models.fbb_churn_amdocs.utils_model import *
    from churn.models.fbb_churn_amdocs.metadata_fbb_churn import *
    from churn.models.fbb_churn_amdocs.feature_selection_utils import *
    from churn.analysis.triggers.base_utils.base_utils import *

    from churn.analysis.triggers.base_utils.base_utils import get_churn_target_nif

    from churn.models.fbb_churn_amdocs.utils_fbb_churn import *

    ## ARGUMENTS
    ###############
    parser = argparse.ArgumentParser(
        description='To evaluate Service Trigger performance',
        epilog='Please report bugs and issues to Beatriz <beatriz.gonzalez2@vodafone.com>')

    parser.add_argument('-s', '--campaign_day', metavar='<CAMPAIGN_DAY>', type=str, required=True,
                        help='Campaign day YYYYMMDD. Date when the campaign was launched.')
    parser.add_argument('-p', '--campaign_name', metavar='<CAMPAIGN_NAME>', type=str, required=False,
                        default='AUTOMSEM_PXXXT_TRIGG_SERVICIO',help='Prediction day YYYYMMDD.')
    args = parser.parse_args()

    print(args)

    spark = initialize("ComparacionFechas", executor_memory="12g", driver_memory="6g")

    closing_day = args.campaign_day
    campaign = args.campaign_name

    year = closing_day[0:4]
    month = closing_day[4:6]
    day = closing_day[6:8]

    from pykhaos.utils.date_functions import move_date_n_days

    end_port = move_date_n_days(closing_day, n=30)

    #### Lectura de Datos ####
    ##########################

    ## Modelo ##

    print '[Info Trigger Servicio Quality]: Reading the trigger model information'

    trigger = spark.read.parquet('/data/attributes/vf_es/model_outputs/model_scores/model_name=triggers_ml')
    df_trigger_date = trigger.filter((col('month') == int(month)) & (col('day') == int(day)) & (col('year') == int(year)))
    model_day=str(year)+''+str(month)+''+str(day)
    if df_trigger_date.count()==0:
        df_trigger_date = trigger.filter((col('month') == int(month)) & (col('day') == int(day)-1) & (col('year') == int(year)))
        model_day = str(year) + '' + str(month) + '' + str(int(day)-1)
    car_date = str(df_trigger_date.select('predict_closing_date').distinct().collect()[0]["predict_closing_date"])

    df_trigger_date=df_trigger_date.cache()
    print(df_trigger_date.count())

    ## Cartera para el día en que se ha tomado la foto del CAR ##

    print '[Info Trigger Servicio Quality]: Reading the CAR information'

    df_cartera_car = get_churn_target_nif_period(spark, car_date, end_port)
    df_cartera_car = df_cartera_car.withColumnRenamed('nif_cliente', 'nif')
    df_cartera_car=df_cartera_car.cache()
    print(df_cartera_car.count())

    df_cartera_car_new = get_churn_target_nif_period_new(spark, car_date, end_port)
    df_cartera_car_new = df_cartera_car_new.withColumnRenamed('nif_cliente', 'nif').withColumnRenamed('label', 'new_label')

    df_cartera_car= df_cartera_car.join(df_cartera_car_new.select('nif','new_label'), ['nif'], 'left')

    ## Cartera para el día en que se lanzó la campaña ##

    df_cartera_campaign = get_churn_target_nif_period(spark, closing_day, end_port)#churn_window=30)
    df_cartera_campaign = df_cartera_campaign.withColumnRenamed('nif_cliente', 'nif')
    df_cartera_campaign=df_cartera_campaign.cache()
    print(df_cartera_campaign.count())

    df_cartera_campaign_new = get_churn_target_nif_period_new(spark, closing_day, end_port)#churn_window=30)
    df_cartera_campaign_new = df_cartera_campaign_new.withColumnRenamed('nif_cliente', 'nif').withColumnRenamed('label', 'new_label')

    df_cartera_campaign = df_cartera_campaign.join(df_cartera_campaign_new.select('nif', 'new_label'), ['nif'], 'left')

    ## Campaña ##

    print '[Info Trigger Servicio Quality]: Reading the Campaing'

    df_campaign = (spark.read.table('raw_es.campaign_nifcontacthist')
                   .filter((col('year') == int(year)) & (col('month') == int(month)) & (col('CampaignCode').isin(campaign)))
                   .withColumn('Grupo', when(col('cellcode').startswith('CU'), 'Universal')
                    .when(col('cellcode').startswith('CC'), 'Control')
                    .otherwise('Target'))
                   .select('CIF_NIF', 'CampaignCode', 'Grupo', 'ContactDateTime', 'year', 'month', 'day'))

    df_campaign_day = df_campaign.filter(col('day') == day)
    df_campaign_day.cache()
    print(df_campaign_day.count())

    print '[Info Trigger Servicio Quality] Fecha CAR: ' + car_date
    print '[Info Trigger Servicio Quality] Fecha Campaign: ' + closing_day
    print '[Info Trigger Servicio Quality] Fecha Model: ' + model_day
    print '[Info Trigger Servicio Quality] Fecha Fin Portas: ' + end_port


    ### CHURN ###
    #############

    # Churn de cartera
    ####################

    for cartera_date in ['CAR', 'Campaign']:

        if cartera_date == 'CAR':
            df_cartera = df_cartera_car
        elif cartera_date == 'Campaign':
            df_cartera = df_cartera_campaign

        for target in ['label', 'label_portout', 'new_label']:

            nChurn_cartera = df_cartera.filter(col(target) == 1).count()
            nClientes_cartera = df_cartera.count()
            churn_cartera = float(nChurn_cartera) / nClientes_cartera * 100

            print '[Info Trigger Servicio Quality] Churn Cartera ('+target+')- Fecha '+cartera_date+':Fecha Fin Portas'
            print ' nChurners:' + str(nChurn_cartera) + ' nClientes:' + str(nClientes_cartera) + ' churn:' + str(churn_cartera)

    # Churn de modelo
    ###################

    for cartera_date in ['CAR', 'Campaign']:

        if cartera_date == 'CAR':
            df_trigger = df_trigger_date.select(['nif', 'scoring']).join(df_cartera_car.select(['nif', 'label','label_portout', 'new_label']), 'nif', 'left')

        elif cartera_date == 'Campaign':
            df_trigger= df_trigger_date.select(['nif', 'scoring']).join(df_cartera_campaign.select(['nif', 'label','label_portout', 'new_label']), 'nif', 'left')

        df_trigger=df_trigger.cache()
        print(df_trigger.count())

        for target in ['label', 'label_portout', 'new_label']:

            nChurn_trigger = df_trigger.filter(col(target) == 1).count()
            nClientes_trigger = df_trigger.count()
            churn_trigger= float(nChurn_trigger) / nClientes_trigger * 100

            print '[Info Trigger Servicio Quality] Churn Modelo ('+target+')- Fecha '+cartera_date +' : Fecha Fin Portas'
            print 'nChurners:' + str(nChurn_trigger) + ' nClientes:' + str(nClientes_trigger) + ' churn:' + str(churn_trigger)

        nBuckets = 20
        cuenta = df_trigger.count()
        top_cust = np.arange(0, cuenta, cuenta / nBuckets)[1:]

        # Curva de lift
        vec_vol_ = []
        vec_rate_ = []
        vec_lift_ = []
        schema = df_trigger.schema

        myschema = df_trigger.schema

        df_trigger_cartera_car = df_trigger.sort(desc("scoring"))
        df_top = spark.createDataFrame(df_trigger_cartera_car.head(cuenta), schema=myschema)

        print '[Info Trigger Servicio Quality] Curva de Churn Modelo - Fecha '+cartera_date+' : Fecha Fin Portas'

        for target in ['label','label_portout', 'new_label']:
            for i in top_cust:

                selected = df_top.head(i)
                selected_df = spark.createDataFrame(selected, schema=myschema)
                total_churners = selected_df.where(col(target) > 0).count()

                print'Churn Rate ('+target+') for top {} customers: {}'.format(i, 100.0 * total_churners / i)

                churn_rate_ = 100.0 * total_churners / i

                vec_vol_.append(total_churners)
                vec_rate_.append(churn_rate_)
                vec_lift_.append(churn_rate_ / nClientes_trigger)


    # Churn en campaign
    #####################

    for cartera_date in ['CAR','Campaign']:

        if cartera_date=='CAR':
            df_cartera= df_campaign_day.join(df_cartera_car,df_cartera_car['nif'] == df_campaign_day['CIF_NIF'], 'inner')
        elif cartera_date=='Campaign':
            df_cartera=df_campaign_day.join(df_cartera_campaign,df_cartera_campaign['nif'] == df_campaign_day['CIF_NIF'],'inner')

        df_cartera=df_cartera.cache()
        print(df_cartera.count())

        nClientes = df_cartera.count()

        for group in ['Total','Control','Target']:

            for target in ['label', 'label_portout', 'new_label']:
                if group == 'Total':
                    nChurn = df_cartera.filter(col(target) == 1).count()
                    nClientes = df_cartera.count()
                elif group=='Control':
                    nChurn = df_cartera.filter((col(target) == 1) & (col('Grupo') == 'Control')).count()
                    nClientes = df_cartera.filter((col('Grupo') == 'Control')).count()
                elif group=='Target':
                    nChurn= df_cartera.filter((col(target) == 1) & (col('Grupo') == 'Target')).count()
                    nClientes = df_cartera.filter((col('Grupo') == 'Target')).count()

                churn_rate = float(nChurn) / nClientes * 100

                print '[Info Trigger Servicio Quality] Churn Campaign ('+target+') '+str(group)+' - Fecha '+cartera_date+' : Fecha Fin Portas'
                print '[Info Trigger Servicio Quality] nChurners: ('+target+') '+ str(nChurn) + ' nClientes:' + str(nClientes) + ' churn:' + str(churn_rate)


    print '*** Proccess has finished ***'
