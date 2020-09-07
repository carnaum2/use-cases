#!/usr/bin/python
# -*- coding: utf-8 -*-

from configuration import Configuration
import time

class DPPrepareInputAccParPropensosCrossNif:

    def __init__(self, app, filter_month, debug=False):
        self.app = app
        self.spark = app.spark
        self.filter_month = str(filter_month)
        self.debug = debug
        
        self.data = None

        self.load()

    def load_local(self):
        ifile = "file:///Users/bbergua/Downloads/CVM/" + self.filter_month\
                + "/OUTPUT/par_propensos_cross_nif_" + self.filter_month + ".txt"
        self.data = self.spark.read.load(ifile,
                                              format='com.databricks.spark.csv', header='true', delimiter='\t',
                                              charset='latin1', inferSchema='true')

    def load_bdp(self):
        print '['+time.ctime()+']', 'Loading Accenture\'s Par Propensos Cross Nif', self.filter_month,  '...'

        # self.spark.tableNames("attributes_es")
        # self.spark.tables("attributes_es").show()
        self.data = self.spark.table('attributes_es.do_par_propensos_cross_nif')
        # self.data.groupby(['partitioned_month', 'year', 'month']).count().sort('partitioned_month').show()

        available_months = self.data.select('partitioned_month').distinct().rdd.flatMap(lambda x: x).collect()
        latest_month = str(max([c for c in available_months if c <= str(self.filter_month)]))

        if latest_month != self.filter_month:
            print '['+time.ctime()+']', 'WARNING:', self.filter_month, 'is not available, using', latest_month, 'instead'

        self.data = self.data.filter(self.data.partitioned_month == latest_month)
        # self.data.groupby(['partitioned_month', 'year', 'month']).count().show()

        print '['+time.ctime()+']', 'Loading Accenture\'s Par Propensos Cross Nif', latest_month, '... done'

    def load(self):
        if self.app.is_bdp:
            self.load_bdp()
        else:
            self.load_local()

        if self.debug:
            self.data.groupby('partitioned_month', 'year', 'month', 'day').count().show()

        return self.data

    def to_df(self):
        return self.data


if __name__ == "__main__":
    conf = Configuration()
    prop_cross = DPPrepareInputAccParPropensosCrossNif(conf, '201704')
