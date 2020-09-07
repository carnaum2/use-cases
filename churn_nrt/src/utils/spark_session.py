

import os
import time
import sys

# def init_spark(app_name, log_level="OFF"):
#     start_time = time.time()
#     sc, spark, sql_context = get_spark_session(app_name=app_name, log_level=log_level)
#     print("Ended spark session: {} secs | default parallelism={}".format(time.time() - start_time,
#                                                                          sc.defaultParallelism))
#
#     return spark


# set BDP parameters
def setting_bdp(min_n_executors = 1, max_n_executors = 15, n_cores = 8, executor_memory = "6g", driver_memory="2g",
                   app_name = "Python app", driver_overhead="1g", executor_overhead='3g'):

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
    SPARK_COMMON_OPTS+=" --files {}/scripts/properties/red_agent/nodes.properties,{}/scripts/properties/red_agent/nodes-de.properties,{}/scripts/properties/red_agent/nodes-es.properties,{}/scripts/properties/red_agent/nodes-ie.properties,{}/scripts/properties/red_agent/nodes-it.properties,{}/scripts/properties/red_agent/nodes-pt.properties,{}/scripts/properties/red_agent/nodes-uk.properties".format(*[BDA_ENV]*7)


    os.environ["SPARK_COMMON_OPTS"] = SPARK_COMMON_OPTS
    os.environ["PYSPARK_SUBMIT_ARGS"] = "%s pyspark-shell " % SPARK_COMMON_OPTS

    #os.environ["SPARK_EXTRA_CONF_PARAMETERS"] = '--conf spark.yarn.jars=hdfs:///data/raw/public/lib_spark_2_1_0_jars_SPARK-18971/*'


    # print os.environ.get('SPARK_COMMON_OPTS', '')
    # print os.environ.get('PYSPARK_SUBMIT_ARGS', '')


# print os.environ.get('SPARK_COMMON_OPTS', '')
# print os.environ.get('PYSPARK_SUBMIT_ARGS', '')


def get_spark_session(app_name="default name", log_level='INFO', min_n_executors = 1, max_n_executors = 15, n_cores = 4,
                      executor_memory = "6g", driver_memory="2g"):
    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)


    setting_bdp(app_name=app_name, min_n_executors = min_n_executors, max_n_executors = max_n_executors, n_cores = n_cores, executor_memory = executor_memory, driver_memory=driver_memory)
    from common.src.main.python.utils.hdfs_generic import run_sc
    sc, spark, sql_context = run_sc(log_level=log_level)

    # The property 'spark.sql.sources.partitionOverwriteMode' controls whether Spark should delete all the partitions that match the partition specification
    # regardless of whether there is data to be written to or not (static) or delete only those partitions that will have data written into(dynamic).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return sc, spark, sql_context



def get_spark_conf(spark):
    # list of tuples
    return spark.sparkContext.getConf().getAll()



def get_spark_session_noncommon(app_name):
    '''
    Obtains the spark session without ussing the common libraries. This is because it fails when running through Jenkins
    :param app_name:
    :param log_level:
    :param min_n_executors:
    :param max_n_executors:
    :param n_cores:
    :param executor_memory:
    :param driver_memory:
    :return:
    '''


    from pyspark.sql import SparkSession, SQLContext, DataFrame
    spark = (SparkSession.builder.appName(app_name)
             .master("yarn")
             .config("spark.submit.deployMode", "client")
             .config("spark.ui.showConsoleProgress", "true").enableHiveSupport().getOrCreate())

    # The property 'spark.sql.sources.partitionOverwriteMode' controls whether Spark should delete all the partitions that match the partition specification
    # regardless of whether there is data to be written to or not (static) or delete only those partitions that will have data written into(dynamic).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sc = spark.sparkContext

    return spark,sc




