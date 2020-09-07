# -*- coding: utf-8 -*-
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.functions import (sum as sql_sum, countDistinct, trim
, max as sql_max, split, desc, col, current_date
, datediff, lit, translate, udf
, when, concat_ws, concat, decode, length
, substring, to_date, regexp_replace, lpad
, hour, date_format, count as sql_count
, array, posexplode, greatest, create_map, upper
, expr, coalesce, udf, avg, lower, from_unixtime, unix_timestamp)
from pyspark.sql.functions import regexp_replace, coalesce, when, length, isnan


from pyspark.sql import Row
from pyspark.sql.types import StructType, LongType, StructField
import re

def get_partitions_path_range(path, start, end):
    """
    Returns a list of complete paths with data to read of the source between two dates
    :param path: string path
    :param start: string date start
    :param end: string date end
    :return: list of paths
    """
    from datetime import timedelta, datetime
    star_date = datetime.strptime(start, '%Y%m%d')
    delta = datetime.strptime(end, '%Y%m%d') - datetime.strptime(start, '%Y%m%d')
    days_list = [star_date + timedelta(days=i) for i in range(delta.days + 1)]
    return [path + "year={}/month={}/day={}".format(d.year, d.month, d.day) for d in days_list]


# display(HTML(tabulate.tabulate([header ] +data, tablefmt='html', headers='firstrow')))
def _df_to_csv(df, cols=None, sep='\t'):
    if type(df) is DataFrame:
        collected = df.collect()
        if cols is None:
            cols = df.columns
    elif (type(df) is list) and (type(df[0]) is Row):
        collected = df
        if cols is None:
            cols = collected[0].asDict().keys()
    else:
        raise TypeError('df argument is of  ' +str(type
            (df) ) +'. It must be either a DataFrame or a collected DataFrame (the result of df.collect() or df.take(), which is a list of Row)')

    output = sep.join(cols) + '\n'

    for r in collected:
        line = []
        row_d = r.asDict()
        for c in cols:
            # print c, row_d[c]
            line.append(unicode(row_d[c]).encode("utf-8"))
        # print '-----'
        output = output + sep.join(line) + '\n'

    return output

def df_to_list(df):
    return [r.split('\t') for r in _df_to_csv(df).strip().split('\n')]

def check_partitions (df):
    l = df.rdd.glom().map(len).collect()  # get length of each partition
    print 'Smallest partition {}'.format(min(l))
    print 'Largest partitions {}'.format(max(l))
    print 'Avg. partition size {}'.format(sum(l)/len(l))
    print 'Num partitions {}'.format(len(l))

#Rename columns from df
def rename_columns(df, prefix, sep = "_", nocols = []):
    new_cols = ["".join([prefix, sep, col_]) if ((col_ in nocols) == False) else col_ for col_ in df.columns]
    rendf = df.toDF(*new_cols)
    return rendf

#Rename columns from df
def rename_columns_sufix(df, sufix, sep = "_", nocols = []):
    new_cols = ["".join([col_, sep, sufix]) if ((col_ in nocols) == False) else col_ for col_ in df.columns]
    rendf = df.toDF(*new_cols)
    return rendf

def import_file_to_pyspark_df(spark, path_to_filename_df):
    import subprocess
    out = subprocess.check_output('hdfs getconf -namenodes', shell=True)
    nodes = ['hdfs://' + n for n in out.strip().split(' ')]

    node_plus_filename = [_node + path_to_filename_df for _node in reversed(nodes)]

    for n_n in range(0, len(nodes)):

        try:
            logger.info("Trying to import {}".format(node_plus_filename[n_n]))
            df_imported = spark.read.format('parquet').load(node_plus_filename[n_n])
            logger.info("file {} found at node {}".format(node_plus_filename[n_n], n_n))
            return df_imported
        except Exception as e:
            logger.error("file {} could not be found at node {}".format(node_plus_filename[n_n], n_n))
            print(e)

    return None



def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]



def format_date(col_, filter_dates_1900=True):
    """
    Convert a string date (YYYYMMDD or YYYY-MM-DD or YYYY/MM/DD) to a string date 'YYYY-MM-DD HH:mm:SS'
    Useful to call later to the datediff function
    col_ can be a string (in this case, corresponds to a column name) o a Column
    """
    if not isinstance(col_, str):
        col_upd = col_ # its a Column obj
    else:
        col_upd = col(col_) # col_ is a column name
    col_upd = regexp_replace(col_upd, '-|/', '')
    col_upd = from_unixtime(unix_timestamp(col_upd, "yyyyMMdd"))
    if filter_dates_1900:
        col_upd = when(datediff(lit("1900-01-01 00:00:00"),col_upd).cast("double")>0,None).otherwise(col_upd)
    return col_upd


def compute_diff_days(col_name, ref_date_name):

    col_upd = col_name if not isinstance(col_name, str) else col(col_name)
    col_ref_date = ref_date_name if not isinstance(ref_date_name, str) else col(ref_date_name)

    col_upd = when( (coalesce(length(col_upd),lit(0))==0) | (coalesce(length(col_ref_date),lit(0))==0), -1).otherwise(datediff(col_ref_date, col_upd).cast("double"))
    return col_upd


# In big dataframes (> 100 MB) row_number over a Window without a partiotionBy can be too heavy. To avoid this...
# df = df.withColumn("idx", monotonically_increasing_id())
# df = df.withColumn("rowId", row_number().over(Window.orderBy("idx")))
# https://stackoverflow.com/questions/39057766/spark-equivelant-of-zipwithindex-in-dataframe
def add_column_index (spark, df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(colName,LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda (row,rowId): ([rowId +offset] + list(row)))

    return spark.createDataFrame(new_rdd, new_schema)


def sum_horizontal(cols_list):
    return reduce(lambda x, y: x + y, [col(c) for c in cols_list])

 


def hist(df_pyspark, col_name, nbins=20):
    import pandas as pd
    import matplotlib.pyplot as plt

    gre_histogram = df_pyspark.select(col_name).rdd.flatMap(lambda x: x).histogram(nbins)

    # Loading the Computed Histogram into a Pandas Dataframe for plotting
    df_pandas = pd.DataFrame(
        list(zip(*gre_histogram)),
        columns=['bin', 'frequency']
    ).set_index(
        'bin'
    )

    fig = plt.figure()
    df_pandas.plot(kind='bar')
    return fig # fig.show() to show the figure. Activate inline mode in notebook with '%matplotlib inline'


def count_nans(df):
    '''

    :param df:
    :param
    :return: a dict key:col_name value:number of nans
             empty dict if none column has nulls
    '''
    # https://stackoverflow.com/questions/44627386/how-to-find-count-of-null-and-nan-values-for-each-column-in-a-pyspark-dataframe?rq=1
    #     df = spark.createDataFrame(
    #     [(1, 1, None), (1, 2, float(5)), (1, 3, np.nan), (1, 4, None), (1, 5, float(10)), (1, 6, float('nan')), (1, 6, float('nan'))],
    #     ('session', "timestamp1", "id2"))

    #     session 	timestamp1 	id2
    #     	  0 	0        	3

    # cols_special = [c for c in df.columns if dict(df.dtypes)[c] in ["timestamp", "array<string>"]]

    # Columns to be checked are potential inputs to the model, i.e., numerical and categorical columns
    cols_normal = [c for c in df.columns if not dict(df.dtypes)[c] in ["timestamp", "array<string>"]]

    df_count = df.select([sql_count(when(isnan(c) | col(c).isNull(), 1)).alias(c) for c in cols_normal])
    # Another alternative: df_count = df.select([sql_sum(when(isnan(c) | col(c).isNull() | col(c) == "" | col(c) == " " | lower(col(c)) == "nan", 1).otherwise(0)).alias(c) for c in cols_normal])
    df_count_collect = df_count.rdd.collect()[0]
    return {col_:val for col_,val in df_count_collect.asDict().items() if val>0}

def count_nans_beta(df):

    # Another approach to identify columns with nulls or NaNs

    # For compatibility with the count_nans function, a dictionary is returned.
    # However, it does not contain the number of nulls for each column. Instead, the name of the column with nulls
    # is associated with a 1 in the returned dict

    def filter_dict(d):
        import math
        null_cols = []
        for (k, v) in d.items():
            if ((v == None) | (v == '') | (v == ' ')):
                null_cols.append(k)
            elif (((isinstance(v, str)) | (isinstance(v, unicode))) == False):
                if (math.isnan(float(v))):
                    null_cols.append(k)
        return null_cols

    return df.rdd.flatMap(lambda r: filter_dict(r.asDict())).distinct().map(lambda e: (e, 1)).collectAsMap()

def extract_columns(cols_list, include="", exclude="", exclude_set=None):

    if not exclude_set:
        exclude_set = []
    return [c for c in cols_list if (
            c not in exclude_set and (len(exclude) == 0 or exclude not in c) and re.match('.*' + include + '.*', c, re.IGNORECASE))]

def impute_with_mean(df, include="", exclude_set=None, preffix="", suffix="_imputed_avg", overwrite=False, include_cols=None):
    '''
    overwrite=False
        Return a new pyspark dataframe where column nulls have been filled with the average of this column
        Original dataframe is not modified
        Imputed columns are renamed with suffix + preffix
        Only selected columns are returned
    overwrite=True
        Fill the missing values with the average over the original dataframe
    Both: Columns selected are the ones that match the regular expression ".*" + include + ".*" and are not in the exclude set
    include_cols : list of cols to be included. If "include"!="" then both lists are appended

    '''
    if not include_cols:
        include_cols = []

    exclude_set = set() if not exclude_set else exclude_set
    auto_cols = extract_columns(df.columns, include, exclude=suffix, exclude_set=exclude_set) if include else []
    sel_cols_list = list(set(auto_cols) | set(include_cols))
    stats = df.agg(*(avg(c).alias(c) for c in sel_cols_list))
    stats_dict = stats.first().asDict()
    print(stats_dict)
    if not overwrite:
        df = df.select(*([when(col(c).isNull(), stats_dict[c]).otherwise(col(c)).alias(preffix + c + suffix) for c in
                          sel_cols_list] + df.columns))
        # df_stats = df_stats.select(sel_cols_list+["msisdn"])
        # select_columns = [(c,preffix+c+suffix) for c in sel_cols_list]
        # for existing, new in select_columns:
        #    df_stats = df_stats.withColumnRenamed(existing, new)
        # return df_stats
        return df
    else:
        df_stats = df.na.fill(stats.first().asDict())
        return df_stats
