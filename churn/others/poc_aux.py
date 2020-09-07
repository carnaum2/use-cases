import sys
import datetime as dt
import os

from pyspark.sql.functions import size, coalesce, col, lit, collect_list, udf, when, length
from pyspark.sql.types import StringType, DoubleType, FloatType, IntegerType


def set_paths_and_logger():
    '''
    Deployment should be something like "dirs/dir1/use-cases"
    This function adds to the path "dirs/dir1/use-cases" and ""dirs/dir1/"
    :return:
    '''
    import imp
    from os.path import dirname
    import os

    USE_CASES = dirname(os.path.abspath(imp.find_module('churn')[1]))

    if USE_CASES not in sys.path:
        sys.path.append(USE_CASES)
        print("Added '{}' to path".format(USE_CASES))

    # if deployment is correct, this path should be the one that contains "use-cases", "pykhaos", ...
    # FIXME another way of doing it more general?
    DEVEL_SRC = os.path.dirname(USE_CASES)  # dir before use-cases dir
    if DEVEL_SRC not in sys.path:
        sys.path.append(DEVEL_SRC)
        print("Added '{}' to path".format(DEVEL_SRC))

    ENGINE_SRC = "/var/SP/data/home/csanc109/src/devel/amdocs_informational_dataset/"
    if ENGINE_SRC not in sys.path:
        sys.path.append(ENGINE_SRC)
        print("Added '{}' to path".format(ENGINE_SRC))

    EXTERNAL_LIB = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src", "devel", "pykhaos", "external_lib")
    if EXTERNAL_LIB not in sys.path:
        sys.path.append(EXTERNAL_LIB)
        print("Added '{}' to path".format(EXTERNAL_LIB))


    import pykhaos.utils.custom_logger as clogger
    logging_file = os.path.join(os.environ.get('BDA_USER_HOME', ''), "logging",
                                "poc_aux_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = clogger.configure_logger(log_filename=logging_file, std_channel=sys.stderr, logger_name="")
    logger.info("Logging to file {}".format(logging_file))


    return logger

class SparkListener(object):
    def onApplicationEnd(self, applicationEnd):
        pass
    def onApplicationStart(self, applicationStart):
        pass
    def onBlockManagerRemoved(self, blockManagerRemoved):
        pass
    def onBlockUpdated(self, blockUpdated):
        pass
    def onEnvironmentUpdate(self, environmentUpdate):
        pass
    def onExecutorAdded(self, executorAdded):
        pass
    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        pass
    def onExecutorRemoved(self, executorRemoved):
        pass
    def onJobEnd(self, jobEnd):
        pass
    def onJobStart(self, jobStart):
        pass
    def onOtherEvent(self, event):
        pass
    def onStageCompleted(self, stageCompleted):
        pass
    def onStageSubmitted(self, stageSubmitted):
        pass
    def onTaskEnd(self, taskEnd):
        pass
    def onTaskGettingResult(self, taskGettingResult):
        pass
    def onTaskStart(self, taskStart):
        pass
    def onUnpersistRDD(self, unpersistRDD):
        pass
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]

class TaskEndListener(SparkListener):
    def onTaskEnd(self, taskEnd):
        print(taskEnd.toString())


def task_aux(spark, i):

    # sc, spark, _ = get_spark_session(app_name="poc_scala_python_{}".format(i), log_level="OFF")
    # print("Ended spark session {}: {} secs ".format(i, time.time() - start_time))

    dfs = []
    df_all = None
    import pandas as pd
    for jj in range(0,10):
        if logger: logger.info("aux task{} iter{}".format(i, jj))
        df_pandas = pd.DataFrame({"start_time_secs": range(0, N), "col": [jj] * N})
        df = spark.createDataFrame(df_pandas)
        dfs.append(df)

        from pykhaos.utils.pyspark_utils import union_all
        df_all = union_all(dfs)
        if logger: logger.info("aux task{} iter{} || ) cache + count ".format(i, jj))
        df_all = df_all.cache()

        if logger: logger.info("aux task{} iter{} || ) cache + count {} == ".format(i, jj, df_all.count()))

    (df_all.write
     .format('parquet')
     .mode('append')
     .saveAsTable("tests_es.csanc109_task_aux_{}".format(i)))

    if logger: logger.info("Ended "+ "tests_es.csanc109_task_aux_{}".format(i))

    if logger: logger.info("Ended "+ "tests_es.csanc109_task_aux_{}".format(i))



if __name__ == "__main__":


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    print("Process started...")

    logger = set_paths_and_logger()


    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD (same used for the car generation)')

    args = parser.parse_args()


    closing_day = args.closing_day


    if logger: logger.info("calling task_aux closing_day {}".format(closing_day))


    from pykhaos.utils.scala_wrapper import convert_df_to_pyspark, get_scala_sc
    from pykhaos.utils.pyspark_configuration import setting_bdp

    import time
    start_time = time.time()


    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)

    from pykhaos.utils.pyspark_configuration import get_spark_session
    sc, spark, _ = get_spark_session(app_name="poc_aux", log_level="OFF")
    print("poc_aux Ended spark session: {} secs ".format(time.time() - start_time ))
    if logger: logger.info("poc_aux Ended spark session: {} secs ".format(time.time() - start_time ))




    N = 10


    for i in range(1,15):
        if logger: logger.info("calling task_aux {}".format(i))
        task_aux(spark, 1234+i)

    print("poc_aux_finished")
    if logger: logger.info("poc_aux_finished")
