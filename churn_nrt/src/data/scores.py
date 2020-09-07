


from pyspark.sql.functions import (udf, col, concat, lpad, array, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull, substring, size, length, desc)
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from churn_nrt.src.data_utils.Metadata import IMPUTE_WITH_AVG_LABEL


class Scores(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "scores")


    def build_module(self, closing_day, save_others, **kwargs):
        '''
        Return the latest score until this closing day
        #TODO this module could be improved
        :param closing_day:
        :param args:
        :return:
        '''

        df_scores_all = (self.SPARK.read.load("/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/")\
                             .where(col("predict_closing_date") < closing_day))

        df_dates = df_scores_all.select("year", "month", "day").distinct()
        df_dates = df_dates.withColumn("part_date", concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
        dates = df_dates.where(col("part_date") <= closing_day).sort(desc("part_date")).limit(1).rdd

        print(dates.collect())

        print(dates.take(1))
        if dates.take(1):
            year_ = dates.map(lambda x: x["year"]).collect()[0]
            month_ = dates.map(lambda x: x["month"]).collect()[0]
            day_ = dates.map(lambda x: x["day"]).collect()[0]

            scores_path = "/data/attributes/vf_es/model_outputs/model_scores/model_name=delivery_churn/year={}/month={}/day={}".format(year_, month_, day_)
            print("Reading scores from path '{}'".format(scores_path))
            df_scores = self.SPARK.read.load(scores_path).withColumnRenamed("scoring", "latest_score")
            return df_scores
        else:
            return None


    def get_metadata(self):

        feats = ["latest_score"]

        cat_feats = []

        na_vals = [str(x) for x in [IMPUTE_WITH_AVG_LABEL]]

        data = {'feature': feats, 'imp_value': na_vals}

        import pandas as pd

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
            .withColumn('source', lit('scores'))
            .withColumn('type', lit('numeric'))
            .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))
            .withColumn('level', lit('nif')))

        return metadata_df