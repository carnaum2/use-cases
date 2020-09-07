#!/usr/bin/python
# -*- coding: utf-8 -*-

import platform
# import os
import time
from common.src.main.python.utils.hdfs_generic import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext  #, SQLContext


class Configuration:

    def __init__(self, sparkSession=None, master='yarn'): # master='local[*]'
        self.is_bdp = False
        if platform.node().endswith(".dc.sedc.internal.vodafone.com"):
            self.is_bdp = True

        # print 'locals()', locals()
        # print 'globals()', globals()
        # print 'sc in globals()', 'sc' in globals()
        # print 'spark in globals()', 'spark' in globals()
        # print 'sparkSession in globals()', 'sparkSession' in globals()
        # print 'sqlContext in globals()', 'sqlContext' in globals()

        if sparkSession is not None:
            #print 'SparkContext already loaded.', 'master is', sc.master
            #self.sc = sc
            #self.sqlContext = HiveContext(self.sc) # SQLContext(self.sc)
            self.spark = sparkSession
        elif 'sc' in globals():
            # FIXME
            print 'SparkContext already loaded in globals.', 'master is', sc.master
        else:
            print '['+time.ctime()+']', 'Loading SparkContext ...'

            # print(os.environ["PYSPARK_SUBMIT_ARGS"])
            # os.environ["PYSPARK_SUBMIT_ARGS"] = (
            #     "--master local[*] --queue PySpark1.6.0 --packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell"
            # )

            # FIXME
            conf = SparkConf()#.setAppName('VF_ES_Convergence')#.setMaster(master)
            # global sc
            #self.sc = SparkContext(conf=conf)
            #self.sqlContext = HiveContext(self.sc) # SQLContext(self.sc)

            # sc, sparkSession, sqlContext = run_sc()
            # self.sc = sc
            self.spark = (SparkSession.builder
                .appName("VF-ES Convergence")
                .master("yarn")
                .config("spark.submit.deployMode", "client")
                .config("spark.ui.showConsoleProgress", "true")
                .enableHiveSupport()
                .getOrCreate()
                )

            print '['+time.ctime()+']', 'Loading SparkContext ... done'


    def test(self):
        data = [1, 2, 3, 4, 5]
        dist_data = self.sc.parallelize(data)

        print 'distData has', dist_data.count(), 'elements'


if __name__ == "__main__":
    app = Configuration()
    app.test()
