import sys

from pyspark.sql.functions import (udf, col, when, lit, collect_list, regexp_replace, max as sql_max)
from pyspark.sql.types import DoubleType, StringType, IntegerType, ArrayType, FloatType
from time import strftime, gmtime



# Numpy stuff
import numpy as np


# Config logging
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(message)s'
                    )


# Import packages
sys.path.append('/var/SP/data/home/adesant3/')


class TimeAggregation(object):

    def __init__(self, depth_training_data, execution_year_month):
        # depth_training_data - number of months for the training data
        # execution_year_month - Execution campaign YYYYMM (201810)

        months_list = range(1,13)
        self.execution_year = str(execution_year_month)[0:4]
        self.execution_month = str(execution_year_month)[4:]

        # Index of the month within month_list
        index_month = (months_list.index(int(self.execution_month)))

        # execution_month = 3
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        # [ 2  3  4  5  6  7  8  9 10 11 12  1]
        # [10 11 12  1]

        ####################
        # Calculate training
        ####################

        self.training_months = list(np.roll(months_list, 12 - index_month + 1)[-depth_training_data:])
        # [10, 11, 12, 1]
        # execution_month = 3
        # [True, True, True, False]
        # [2017, 2017, 2017, 2018]
        self.training_years = [int(self.execution_year) - 1 if month > int(self.execution_month)
                               else int(self.execution_year)
                               for month in self.training_months]

        ####################
        # Calculate target
        ####################

        # Target month is the same as prediction month
        self.target_month = list(np.roll(months_list, 12 - index_month))[-1]
        self.target_year = int(self.execution_year) - 1 if self.target_month > int(self.execution_month) else int(self.execution_year)

    def get_execution_month(self):
        return self.execution_month

    def get_execution_year(self):
        return self.execution_year

    def get_execution_yearmonth(self):
        return str(self.execution_year)+str(self.execution_month).zfill(2)

    def get_training_months(self):
        return self.training_months

    def get_training_years(self):
        return self.training_years

    def get_training_yearmonth(self):
        # self.training_years = [2017, 2017, 2017]
        # self.training_months = [10, 11, 12]
        # return ['201710', '201711', '201712']
        return [str(element_training_years) + str(self.training_months[iterador]).zfill(2)
                for iterador, element_training_years in enumerate(self.training_years)]

    def get_target_month(self):
        return self.target_month

    def get_target_year(self):
        return self.target_year

    def get_target_yearmonth(self):
        return str(self.target_year)+str(self.target_month).zfill(2)

if __name__ == '__main__':
    ta = TimeAggregation(depth_training_data=3,
                         execution_year_month='201803')
    print(ta.get_execution_month())
    print(ta.get_execution_year())
    print(ta.get_execution_yearmonth())
    print(ta.get_training_months())
    print(ta.get_training_years())
    print(ta.get_training_yearmonth())
    print(ta.get_target_month())
    print(ta.get_target_year())
    print(ta.get_target_yearmonth())









