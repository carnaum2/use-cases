



from pyspark.sql.functions import lit, col,  desc
from churn_nrt.src.utils.pyspark_utils import impute_with_mean

IMPUTE_WITH_AVG_LABEL = "__AVG__"

class Metadata:

    METADATA_FUNCTION = None
    METADATA_SOURCES = None
    NON_INFO_COLS = None
    SPARK = None

    def __init__(self, spark, func_, non_info_cols, sources):
        self.METADATA_SOURCES = sources
        self.METADATA_FUNCTION = func_
        self.SPARK = spark
        self.NON_INFO_COLS = non_info_cols


    def get_metadata_df(self, sources=None, filter_correlated_feats=False):
        '''
        :param types: None for all types. 'categorical' for categoricals, 'feats' for all feats, 'all' for feats and non-inf cols
        :param filter_correlated_feats filter variables with high correlation. This is controlled in the get_metadata of each model

        :return:
        '''

        if not sources:
            sources = self.METADATA_SOURCES

        if not self.METADATA_FUNCTION:
            print("[Metadata] get_metadata_df | metadata function is None. ")
            return None

        df_metadata = self.METADATA_FUNCTION(self.SPARK, sources=sources, filter_correlated_feats=filter_correlated_feats)

        return df_metadata

    def get_cols(self, type_=None, sources=None, filter_correlated_feats=False):
        '''

        :param types: None for all types. 'categorical' for categoricals, 'feats' for all feats, 'all' for feats and non-inf cols
        :param filter_correlated_feats filter variables with high correlation. This is controlled in the get_metadata of each model
        :return:
        '''

        if not sources:
            sources = self.METADATA_SOURCES

        df_metadata = self.get_metadata_df(sources=sources, filter_correlated_feats=filter_correlated_feats)

        if df_metadata is None:
            print("[Metadata] get_metadata_df | df_metadata is None. ")
            return None


        if type_ == "categorical":
            feats_cols = df_metadata.filter(col('type') == "categorical").rdd.map(lambda x: x['feature']).collect()
        elif type_ == "feats":
            feats_cols = df_metadata.rdd.map(lambda x: x['feature']).collect()
        else:
            feats_cols = df_metadata.rdd.map(lambda x: x['feature']).collect() + self.NON_INFO_COLS
        return feats_cols

    def get_metadata_function(self):
        return self.METADATA_FUNCTION

    def get_metadata_function_abspath(self):
        return self.METADATA_FUNCTION.__code__.co_filename if self.METADATA_FUNCTION else "<no_metadata_function>"

    def get_non_info_cols(self):
        return self.NON_INFO_COLS

    def fillna(self, df, sources=None, cols=None):
        '''
        Apply metadata for columns of sources "sources" and column list "cols" (optional)
        :param df:
        :param sources:
        :param cols:
        :return:
        '''

        print("[Metadata] fillna | sources={} cols={}".format(",".join(sources) if sources else "None", ",".join(cols) if cols else "None"))

        if not cols and not sources:
            print("Calling fillna with sources=None and cols=Non. Filling nans for all sources ({})".format(",".join(self.METADATA_SOURCES)))
            sources = self.METADATA_SOURCES

        if sources: # sources != None
            df_metadata = self.get_metadata_df(sources=sources)
            cols_sources = df_metadata.rdd.map(lambda x: x['feature']).collect()

        else: # sources=None cols != None
            # Ask metadata for all sources and keep only feats in cols list
            df_metadata = self.get_metadata_df(sources=self.METADATA_SOURCES)
            df_metadata = df_metadata.where(col("feature").isin(cols))
            cols_sources = []

        cols = cols_sources if not cols else list(set(cols) | set(cols_sources))


        df = apply_metadata(df_metadata, df, cols)

        return df



def apply_metadata(df_metadata, df, cols=None, verbose=True):
        '''
        Apply metadata for columns in list "cols" (optional) of all columns if cols is None
        :param df:
        :param sources:
        :param cols:
        :return:
        '''

        print("[Metadata] apply_metadata")
        if not cols:
            cols = list(set(df.columns))

        # Intersection
        # Remove from metadata values of 'feature' that do not exist on df columns or cols parameter
        cols = list(set(df.columns) & set(cols))
        df_metadata = df_metadata.where(col("feature").isin(cols))

        # Get feats that must be imputed with mean
        avg_cols = df_metadata.where(col("imp_value") == IMPUTE_WITH_AVG_LABEL).select("feature").rdd.map(lambda x: (x["feature"])).collect()
        if avg_cols:
            print("[Metadata] fillna | impute with mean - {}".format(",".join(avg_cols)))
            print("[Metadata] fillna | imputing nans in {} columns with average".format(len(avg_cols)))

            df = impute_with_mean(df, include="", exclude_set=None, preffix="", suffix="", overwrite=True, include_cols=avg_cols)
        else:
            print("[Metadata] fillna | Nothing to impute with mean")

        # Get feats that must be imputed with the value in imp_value
        metadata_map = df_metadata.where( ((col("imp_value") != IMPUTE_WITH_AVG_LABEL) & (col("type") != "array"))).select("feature", "imp_value", "type").rdd.map(lambda x: (x["feature"], x["imp_value"], x["type"])).collect()
        print("[Metadata] fillna | imputing nans in {} numeric columns".format(len(metadata_map)))

        metadata_map = dict([(x[0], str(x[1])) if x[2] == "categorical" else  (x[0], float(x[1])) for x in metadata_map])
        if verbose:
            pass
            #print("[Metadata] fillna | imputing nans in these numeric columns: {}".format(",".join(metadata_map.keys())))
            #print("[Metadata] fillna | imputing nans: {}".format(metadata_map))

        df = df.na.fill(metadata_map)

        return df

