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



if __name__ == "__main__":


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # - - - - - - - - - - - - - - - -
    #
    # - - - - - - - - - - - - - - - -
    print("Process started...")

    logger = set_paths_and_logger()

    from pykhaos.utils.scala_wrapper import convert_df_to_pyspark, get_scala_sc

    import time
    start_time = time.time()

    from pykhaos.utils.pyspark_configuration import setting_bdp

    HOME_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "src")
    if HOME_SRC not in sys.path:
        sys.path.append(HOME_SRC)

    from pykhaos.utils.pyspark_configuration import get_spark_session
    sc, spark, _ = get_spark_session(app_name="poc_scala_python", log_level="OFF")
    print("Ended spark session: {} secs ".format(time.time() - start_time ))

    # sc._gateway.shutdown_callback_server()
    # sc._gateway.start_callback_server()
    #
    # listener = TaskEndListener()
    # sc._jsc.sc().addSparkListener(listener)
    # sc.parallelize(range(100), 3).saveAsTextFile("/tmp/listener_test_simple")



    #####
    # CALL SCALA FUNCTION
    #####
    scala_sc = get_scala_sc(spark)

    closing_day = "20181231"

    from pykhaos.utils.date_functions import move_date_n_cycles

    while closing_day < "20190608":

        from churn.delivery.churn_delivery_master_threads import get_training_cycles
        tr_cycle_ini, tr_cycle_end = get_training_cycles(closing_day, horizon=8)

        print(closing_day, tr_cycle_ini, tr_cycle_end)

        sql_segment = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
                                                                                 tr_cycle_end, closing_day,
                                                                                 "others")
        closing_day = move_date_n_cycles(closing_day, n=+1)


    # sql_segment_2 = sc._jvm.modeling.churn.ChurnModelPredictor.getModelPredictions(scala_sc, tr_cycle_ini,
    #                                                                              tr_cycle_end, closing_day,
    #                                                                              "mobileandfbb")


    # '''
    # # df_scala = sc._jvm.mypackage.MyObject.returnDfFunction(scala_sc, "20181231")
    # # df_python = convert_df_to_pyspark(spark, df_scala)
    # # df_python.show(2)
    # '''
    #
    # # in spark-defaults.conf
    # import threading
    #
    # N = 10
    #
    #
    # def task(spark, i):
    #
    #     dfs = []
    #     df_all = None
    #     import pandas as pd
    #     for jj in range(0,10):
    #         if logger: logger.info("task{} iter{}".format(i, jj))
    #         df_pandas = pd.DataFrame({"start_time_secs": range(0, N), "col": [jj] * N})
    #         df = spark.createDataFrame(df_pandas)
    #         dfs.append(df)
    #
    #         from pykhaos.utils.pyspark_utils import union_all
    #         df_all = union_all(dfs)
    #         if logger: logger.info("task{} iter{} || ) cache + count ".format(i, jj))
    #         df_all = df_all.cache()
    #
    #         if logger: logger.info("task{} iter{} || ) cache + count {} == ".format(i, jj, df_all.count()))
    #
    #     (df_all.write
    #      .format('parquet')
    #      .mode('append')
    #      .saveAsTable("tests_es.csanc109_task_{}".format(i)))
    #     if logger: logger.info("Ended "+ "tests_es.csanc109_task_{}".format(i))
    #
    #
    # import subprocess
    #
    # if logger: logger.info("POPEN")
    #
    # from pykhaos.utils.threads_manager import launch_process
    #
    # cmd = 'spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=8 --conf spark.dynamicAllocation.maxExecutors=16 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 --conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 --driver-java-options="-Droot.logger=WARN,console" --jars /var/SP/data/bdpmdses/jmarcoso/apps/churnmodel/AutomChurn-assembly-0.8.jar'
    #
    #
    #
    # mycmd = cmd.split(" ") +['/var/SP/data/home/csanc109/src/devel/use-cases/churn/others/poc_aux.py', '-c', '20190331']
    #
    # if logger: logger.info(mycmd)
    # poc_aux_popen = launch_process(mycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #
    # if logger: logger.info("ENDED POPEN")
    #
    # poc_aux_pid = poc_aux_popen.pid
    # if logger: logger.info("pid = {} - cmd '{}'".format(poc_aux_pid, poc_aux_popen.args))
    # #os.spawnl(os.P_NOWAIT, 'python /var/SP/data/home/csanc109/src/devel/use-cases/churn/others/poc_aux.py')
    # #if poc_aux_popen.poll() == None:
    #     #communicate blocks until finished. Check this answer instead
    #     #https: // stackoverflow.com / questions / 375427 / non - blocking - read - on - a - subprocess - pipe - in -python
    #     # result, result_err = poc_aux_popen.communicate()
    #     # if logger: logger.info(result)
    #     # if logger: logger.info(result_err)
    #
    # #time.sleep(60)
    # for ii in [5555]:
    #     start_time = time.time()
    #     # print("POLL")
    #     # if not poc_aux_popen.poll():
    #     #     result, result_err = poc_aux_popen.communicate()
    #     #     print(result)
    #     #     print(result_err)
    #     task(spark, ii)
    #     if logger:logger.info("TIME {}".format(time.time() - start_time))
    #
    #     # if not poc_aux_popen.poll():
    #     #     print("voy a llamar a communicate")
    #     #     result, result_err = poc_aux_popen.communicate()
    #     #     print(result)
    #     #     print(result_err)
    #
    # if logger: logger.info("POLL")
    #
    #
    # # poll() returns
    # # None when the process is still running
    # # otherwise returns the exit code
    # return_code = poc_aux_popen.wait_until_end(delay=60, max_wait=None)
    #
    # if logger: logger.info("ENDED! - return_code {}".format(return_code))
    # if logger: logger.info("Process {} was running for {} minutes".format(poc_aux_popen.running_script, int(time.time() - poc_aux_popen.start_timestamp)/60))

    sys.exit(0)
