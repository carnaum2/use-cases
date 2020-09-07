#!/usr/bin/python
# -*- coding: utf-8 -*-

import platform
# import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext  #, SQLContext


class Configuration:

    def __init__(self, sc=None, master='local[*]'):
        self.is_bdp = False
        if platform.node().endswith(".dc.sedc.internal.vodafone.com"):
            self.is_bdp = True

        if sc is not None:
            print 'SparkContext already loaded.', 'master is', sc.master
            self.sc = sc
            self.sqlContext = HiveContext(self.sc) # SQLContext(self.sc)
        elif 'sc' in globals():
            print 'SparkContext already loaded in globals.', 'master is', sc.master
        else:
            # print 'locals()', locals()
            # print 'globals()', globals()
            # print 'sc', sc
            print 'Loading SparkContext' #  , master =', master, '...'

            # print(os.environ["PYSPARK_SUBMIT_ARGS"])
            # os.environ["PYSPARK_SUBMIT_ARGS"] = (
            #     "--master local[*] --queue PySpark1.6.0 --packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell"
            # )

            conf = SparkConf().setAppName('VF_ES_Convergence')#.setMaster(master)
            # global sc
            self.sc = SparkContext(conf=conf)

            self.sqlContext = HiveContext(self.sc) # SQLContext(self.sc)

    def test(self):
        data = [1, 2, 3, 4, 5]
        dist_data = self.sc.parallelize(data)

        print 'distData has', dist_data.count(), 'elements'


if __name__ == "__main__":
    app = Configuration()
    app.test()
