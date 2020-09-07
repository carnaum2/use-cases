

import src.pycharm_workspace.lib_csanc109.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

class MegaTable:

    def __init__(self, spark_session):
        """
        Class that facilitates the use of the mega table.
        :param spark_session: SparkSession.
        """
        self.spark = spark_session


    def load_mega_table(self, table_name):
        self.spark.sql("refresh table {}".format(table_name))
        try:
            logger.info("Reading table {}".format(table_name))
            df_mega = (self.spark.read.table(table_name))
            return df_mega
        except Exception as e:
            logger.error("Problem reading {}... Does it exist?".format(table_name))
            logger.error(e)
            print(e)
            assert(False)


    def get_mega_info_msisdn(self, my_msisdn, cols=["day", "month", "tu_num", "bal_bod", "tu_amount"]):
        df_graph = self.df_mega[self.df_mega["msisdn"].isin([my_msisdn])].toPandas().sort_values(by=["year", "month", "day"])
        if cols:
            return df_graph[cols]
        return df_graph