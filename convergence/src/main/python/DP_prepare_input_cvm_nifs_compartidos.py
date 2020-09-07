#!/usr/bin/python
# -*- coding: utf-8 -*-

from configuration import Configuration


class DPPrepareInputCvmNifsCompartidos:

    def __init__(self, app, month):
        self.app = app
        self.spark = app.spark
        self.month = month

        self.data = None

        self.load()

    def load_local(self):
        ifile = "file:///Users/bbergua/Downloads/CVM/" + self.month + \
                "/INPUT/EXTR_NIFS_COMPARTIDOS_" + self.month + ".txt"
        self.data = self.spark.read.load(ifile,
                                              format='com.databricks.spark.csv', header='true', delimiter='|',
                                              charset='latin1', inferSchema='true')

    def load_bdp(self):
        # self. spark.tableNames("raw_es")
        # self.spark.tables("raw_es").show()
        self.data = self.spark.table("raw_es.vf_pos_nifs_compartidos")
        # self.data.groupby(['partitioned_month', 'year', 'month']).count().sort('partitioned_month').show()
        self.data = self.data.filter(self.data.partitioned_month == self.month)
        # self.data.groupby(['partitioned_month', 'year', 'month']).count().show()

    def load(self):
        if self.app.is_bdp:
            self.load_bdp()
        else:
            self.load_local()

        # print self.data.count()
        # header = self.data.first()
        # print header
        #
        # filter out the header, make sure the rest looks correct
        # textFile_body = textFile.filter(textFile.value != header.value)
        # temp_var = textFile_body.map(lambda k: k.value.split("|")) # This generates a PythonRDD
        # print temp_var.take(10)
        #
        # self.data = temp_var.toDF(header.value.split("|"))

        # self.data.show()
        # self.data.printSchema()
        # print self.data.count()

    def to_df(self):
        return self.data


if __name__ == "__main__":
    conf = Configuration()
    nifs_comp = DPPrepareInputCvmNifsCompartidos(conf, '20170222')
