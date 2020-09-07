#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# kinit ${USER}@INTERNAL.VODAFONE.COM -k -t /home/${USER}/.ssh/${USER}.keytab ; spark2-submit $SPARK_COMMON_OPTS --executor-memory 8G --driver-memory 8G --conf spark.executor.cores=4 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G --conf spark.yarn.driver.memoryOverhead=8G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf spark.shuffle.manager=tungsten-sort --conf spark.shuffle.service.enabled=true --conf spark.executor.cores=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=2 --conf spark.network.timeout=10000001 /var/SP/data/home/bbergua/fy17.capsule/utils/src/main/python/general_model_trainer/model_parameters_parser.py -o tests_es.bbergua_model_params_test_2 2>/dev/null

from common.src.main.python.utils.hdfs_generic import *
import argparse
import os
import re
import subprocess
import time
import pyspark.sql.functions as F
from pyspark.sql.functions import col, isnan, isnull, udf, when
from pyspark.sql.types import DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType

def get_custom_metric(metric_name, cell):
    try:
        metric = eval(str(cell).replace(';', ','))['unbalanced'][metric_name]
    except SyntaxError:
        try:
            metric = eval(str(cell).replace(';', ','))[metric_name]
        except SyntaxError:
            #print 'Calculating 1st REGEXP...'
            #sys.stdout.flush()
            m = re.search('(.*\'%s\':)([^,]*)(,.*)' % metric_name, cell)
            #print 'Calculating 1st REGEXP... done'
            #sys.stdout.flush()
            if m:
                if metric_name == 'lift':
                    #print 'lift', m.group(2), float(m.group(2).split('[')[1])
                    #sys.stdout.flush()
                    return float(m.group(2).split('[')[1])
                #print 'NOT lift', float(m.group(2))
                #sys.stdout.flush()
                return float(m.group(2))
            else:
                if metric_name == 'roc_auc_score':
                    #print 'Calculating 2nd REGEXP...'
                    #sys.stdout.flush()
                    m = re.search('(.*roc=)([^;]*)(;.*)', cell)
                    #print 'Calculating 2nd REGEXP... done'
                    #sys.stdout.flush()
                    if m:
                        return float(m.group(2))
                #print 'NOT m', cell
                #sys.stdout.flush()
                return None
            
            #print 'cell', cell
            #sys.stdout.flush()
            #return None
        
    if metric_name == 'lift':
        return metric[1]

    return metric

def get_metrics_auc(cell):
    return get_custom_metric('roc_auc_score', cell)

def get_metrics_lift(cell):
    return get_custom_metric('lift', cell)

def get_metrics_f1(cell):
    return get_custom_metric('f1_score', cell)

def get_metrics_accuracy(cell):
    return get_custom_metric('accuracy_score', cell)


class ModelParametersParser:

    def __init__(self, spark, debug=False, use_common_path=True, output_table='tests_es.model_parameters_parsed'):
        #print 'os.path.realpath(__file__)', os.path.realpath(__file__)
        #print 'os.path.abspath(__file__)', os.path.abspath(__file__)
        #print 'os.path.dirname(os.path.abspath(__file__))', os.path.dirname(os.path.abspath(__file__))

        spark.sparkContext.addPyFile(os.path.abspath(__file__))
        from model_parameters_parser import get_custom_metric, get_metrics_auc, get_metrics_lift, get_metrics_f1, get_metrics_accuracy

        self.spark = spark
        self.debug = debug
        self.use_common_path = use_common_path
        self.output_table = output_table
        self.ref_columns = [
            'executed_at', 
            'model_level', 
            'training_closing_date', 
            'target', 
            'model_path', 
            'metrics_path', 
            'metrics_train', 
            'metrics_test', 
            'varimp', 
            'algorithm', 
            'author_login', 
            'extra_info', 
            'scores_extra_info_headers', 
            'model_name', 
            'time', 
            'year', 
            'month', 
            'day'
        ]

    @staticmethod
    def get_model_list(use_common_path=True):
        if use_common_path:
            base_path = '/data/attributes/vf_es'
        else:
            base_path = '/user/'+os.environ.get('USER', None)
        opath = base_path+'/model_outputs/model_parameters/'

        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        odir = opath #+ '/model_name=' + str(self.model_name) + '/year=' + str(self.year) + '/month=' + str(self.month) + '/day=' + str(self.day)

        pattern = odir+'/*'
        model_pattern = re.compile('.*model_name=(.*)')
        model_list = []
        for status in fs.globStatus(self.spark._jvm.org.apache.hadoop.fs.Path(pattern)):
            ofile = status.getPath().toString()
            g = model_pattern.match(ofile)
            #print 'Path:', ofile
            if g:
                #print g.group(1)
                model_list = model_list + [g.group(1)]
        
        return model_list

    @staticmethod
    def compare_lists(list1, list2):
        # using sum() + zip() + len() to check if  
        # lists are equal 
        if len(list1)== len(list2) and len(list1) == sum([1 for i, j in zip(list1, list2) if i == j]): 
            #print ("The lists are identical")
            return True
        else: 
            #print ("The lists are not identical")
            return False

    @staticmethod
    def check_model_parameters_columns(list2):
        ref_columns2 = list(ref_columns)
        ref_columns2.remove('model_name')
        return ModelParametersParser.compare_lists(ref_columns2, list2)

    @staticmethod
    def get_valid_models(use_common_path=True):
        if use_common_path:
            base_path = '/data/attributes/vf_es'
        else:
            base_path = '/user/'+os.environ.get('USER', None)
        opath = base_path+'/model_outputs/model_parameters'

        valid_models_list = []
        for c in ModelParametersParser.get_model_list():
            #print c
            try:
                myparams = self.spark.read.parquet(opath+'/model_name='+c)
                #if ModelParametersParser.check_model_parameters_columns(myparams.columns):
                #    valid_models_list = valid_models_list + [c]
                #else:
                #    print c
                #    #myparams.printSchema()
                valid_models_list = valid_models_list + [c]
            except:
                #print 'Falla:', c
                pass

        return valid_models_list

    def run(self):
        if self.use_common_path:
            base_path = '/data/attributes/vf_es'
        else:
            base_path = '/user/'+os.environ.get('USER', None)
        opath = base_path+'/model_outputs'

        print '['+time.ctime()+']', 'Reading model output tables'
        myparams = self.spark.read.parquet(opath+'/model_parameters')
        myscores = self.spark.read.parquet(opath+'/model_scores')

        # print '['+time.ctime()+']', 'Extracting metrics from metrics_test'
        # get_metrics_auc_udf      = udf(get_metrics_auc, FloatType())
        # get_metrics_lift_udf     = udf(get_metrics_lift, FloatType())
        # get_metrics_f1_udf       = udf(get_metrics_f1, FloatType())
        # get_metrics_accuracy_udf = udf(get_metrics_accuracy, FloatType())
        # myparams = (myparams.withColumn('roc_auc_score',  get_metrics_auc_udf(col('metrics_test')))
        #                     .withColumn('lift',           get_metrics_lift_udf(col('metrics_test')))
        #                     .withColumn('f1_score',       get_metrics_f1_udf(col('metrics_test')))
        #                     .withColumn('accuracy_score', get_metrics_accuracy_udf(col('metrics_test')))
        #            )
        # #myparams.show()

        print '['+time.ctime()+']', 'Converting model_parameters table from Spark DataFrame to Pandas DataFrame'
        myparams_pd_df_orig = myparams.toPandas()
        myparams_pd_df = myparams_pd_df_orig.copy()

        print '['+time.ctime()+']', 'Converting model_scores table from Spark DataFrame to Pandas DataFrame'
        myscores_pd_df_orig = myscores.groupby('model_name', 'model_executed_at', 'executed_at', 'predict_closing_date').count().toPandas()
        myscores_pd_df = myscores_pd_df_orig.copy()
        myscores_pd_df.sort_values(['model_name', 'executed_at'])

        print '['+time.ctime()+']', 'Calculating launch date'
        dfc = myscores_pd_df.groupby(['model_name'])['executed_at']
        myscores_pd_df['launch_date' ] = dfc.transform('min')
        myscores_pd_df.sort_values(['model_name', 'executed_at'])

        print '['+time.ctime()+']', 'Extracting from metrics_test column: AUC'
        myparams_pd_df['roc_auc_score']  = myparams_pd_df['metrics_test'].apply(get_metrics_auc)
        print '['+time.ctime()+']', 'Extracting from metrics_test column: Lift'
        myparams_pd_df['lift']           = myparams_pd_df['metrics_test'].apply(get_metrics_lift)
        print '['+time.ctime()+']', 'Extracting from metrics_test column: F1'
        myparams_pd_df['f1_score']       = myparams_pd_df['metrics_test'].apply(get_metrics_f1)
        print '['+time.ctime()+']', 'Extracting from metrics_test column: Accuracy'
        myparams_pd_df['accuracy_score'] = myparams_pd_df['metrics_test'].apply(get_metrics_accuracy)

        myparams_pd_df = myparams_pd_df.sort_values(['model_name', 'executed_at'])

        print '['+time.ctime()+']', 'Calculating metrics\' deltas'
        for_deltas = myparams_pd_df[['model_name', 'roc_auc_score', 'lift', 'f1_score', 'accuracy_score']].copy()
        for_deltas = for_deltas.groupby(['model_name'])['roc_auc_score', 'lift', 'f1_score', 'accuracy_score'].diff()

        print '['+time.ctime()+']', 'Joining parameters table with deltas'
        myparams_pd_df = myparams_pd_df.merge(for_deltas, how='inner', left_index=True, right_index=True, suffixes=('', '_delta'))

        print '['+time.ctime()+']', 'Joining parameters table with scores table'
        joined_pd_df = pd.merge(myparams_pd_df, myscores_pd_df, how='left', left_on=['model_name','executed_at'], right_on = ['model_name','model_executed_at'], suffixes=('_params', '_scores'))
        del joined_pd_df['model_path']
        del joined_pd_df['metrics_path']
        del joined_pd_df['metrics_train']
        del joined_pd_df['metrics_test']
        del joined_pd_df['varimp']
        del joined_pd_df['scores_extra_info_headers']
        joined_pd_df = joined_pd_df.sort_values(['model_name', 'executed_at_params'])

        print '['+time.ctime()+']', 'Calculating metrics\' rates'
        joined_pd_df['roc_auc_score_delta_rate']  = joined_pd_df['roc_auc_score_delta']/joined_pd_df['roc_auc_score']
        joined_pd_df['lift_delta_rate']           = joined_pd_df['lift_delta']/joined_pd_df['lift']
        joined_pd_df['f1_score_delta_rate']       = joined_pd_df['f1_score_delta']/joined_pd_df['f1_score']
        joined_pd_df['accuracy_score_delta_rate'] = joined_pd_df['accuracy_score_delta']/joined_pd_df['accuracy_score']

        print '['+time.ctime()+']', 'Fixing data types'
        #joined_pd_df['time']                 = joined_pd_df['time'].astype(float).astype(int)
        joined_pd_df['model_executed_at']    = joined_pd_df['model_executed_at'].astype(str)
        joined_pd_df['executed_at_scores']   = joined_pd_df['executed_at_scores'].astype(str)
        joined_pd_df['predict_closing_date'] = joined_pd_df['predict_closing_date'].astype(str)

        for c in ['year', 'month', 'day']: # , 'time'
            joined_pd_df[c] = pd.to_numeric(joined_pd_df[c], errors='coerce')
            #if c is not 'time':
            #joined_pd_df = joined_pd_df.dropna(subset=[c])
            joined_pd_df[c] = joined_pd_df[c].fillna('-99')
            joined_pd_df[c] = joined_pd_df[c].astype('int')

        print '['+time.ctime()+']', 'Creating Hive table'
        schema = StructType([
            StructField('executed_at',               StringType(),  False),
            StructField('model_level',               StringType(),  True),
            StructField('training_closing_date',     StringType(),  True),
            StructField('target',                    StringType(),  True),
            #StructField('varimp',                    StringType(),  True),
            StructField('algorithm',                 StringType(),  True),
            StructField('author_login',              StringType(),  True),
            StructField('extra_info',                StringType(),  True),
            StructField('time',                      FloatType(),   True), # To allow NaNs, later will be casted to Integer
            StructField('model_name',                StringType(),  False),
            StructField('year',                      IntegerType(), False),
            StructField('month',                     IntegerType(), False),
            StructField('day',                       IntegerType(), False),
            StructField('roc_auc_score',             DoubleType(),  True),
            StructField('lift',                      DoubleType(),  True),
            StructField('f1_score',                  DoubleType(),  True),
            StructField('accuracy_score',            DoubleType(),  True),
            StructField('roc_auc_score_delta',       DoubleType(),  True),
            StructField('lift_delta',                DoubleType(),  True),
            StructField('f1_score_delta',            DoubleType(),  True),
            StructField('accuracy_score_delta',      DoubleType(),  True),
            StructField('model_executed_at',         StringType(),  True),
            StructField('executed_at_scores',        StringType(),  True),
            StructField('predict_closing_date',      StringType(),  True),
            StructField('count',                     DoubleType(),  True),
            StructField('launch_date',               StringType(),  True),
            StructField('roc_auc_score_delta_rate',  DoubleType(),  True),
            StructField('lift_delta_rate',           DoubleType(),  True),
            StructField('f1_score_delta_rate',       DoubleType(),  True),
            StructField('accuracy_score_delta_rate', DoubleType(),  True)
            #StructField("",                       DoubleType(), True)
        ])
        myparams = self.spark.createDataFrame(joined_pd_df, schema)

        #for c in ['model_executed_at', 'executed_at_scores', 'predict_closing_date']:
        #    myparams = (myparams.withColumn(c, when(col(c) == 'nan', None).otherwise(c)))

        myparams2 = (myparams
            .withColumn('model_executed_at',     when(col('model_executed_at')    == 'nan', None).otherwise(col('model_executed_at')))
            .withColumn('executed_at_scores',    when(col('executed_at_scores')   == 'nan', None).otherwise(col('executed_at_scores')))
            .withColumn('predict_closing_date',  when(col('predict_closing_date') == 'nan', None).otherwise(col('predict_closing_date')))
            .withColumn('training_closing_date', col('training_closing_date').cast(IntegerType()))
            .withColumn('predict_closing_date',  col('predict_closing_date').cast(IntegerType()))
            .withColumn('time',                  col('time').cast(IntegerType()))
            .withColumn('count',                 col('count').cast(IntegerType()))
            .withColumn('executed_at',           col('executed_at').cast(TimestampType()))
            .withColumn('model_executed_at',     col('model_executed_at').cast(TimestampType()))
            .withColumn('executed_at_scores',    col('executed_at_scores').cast(TimestampType()))
            .withColumn('launch_date',           col('launch_date').cast(TimestampType()))
            .select([
                when(~isnan(c), col(c)).alias(c) if t in ("double", "float") else c 
                                    for c, t in myparams.dtypes
                    ])
            )

        print '['+time.ctime()+']', 'Saving into Hive table'
        myparams2.write.saveAsTable(self.output_table, format='parquet', mode='overwrite')

        print '['+time.ctime()+']', 'Updating Impala\'s cache'
        subprocess.call("impala-shell -k -i vgddp348hr -q 'INVALIDATE METADATA %s;'" % self.output_table, shell=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='VF_ES Model Parameters Parsing Process',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    parser.add_argument('-p', '--use_personal_path', action='store_true', help='Use personal user path for model outputs (/user/%s/model_outputs/). If False, use common path (/data/attributes/vf_es/model_outputs). Default: False' % os.environ.get('USER', None))
    parser.add_argument('-o', '--output_table', type=str, default='tests_es.model_parameters_parsed', metavar='<output_table>', help='Destination table to store results. Default: tests_es.model_parameters_parsed')
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages')
    args = parser.parse_args()
    print 'args =', args
    print '       debug =', args.debug
    print '       use_personal_path =', args.use_personal_path
    print '       output_table =', args.output_table

    #conf = Configuration()

    print '['+time.ctime()+']', 'Loading SparkContext ...'
    sc, sparkSession, sqlContext = run_sc()
    spark = (SparkSession.builder
                         .appName("VF_ES Parsing Model Parameters")
                         .master("yarn")
                         .config("spark.submit.deployMode", "client")
                         .config("spark.ui.showConsoleProgress", "true")
                         .enableHiveSupport()
                         .getOrCreate()
            )
    print '['+time.ctime()+']', 'Loading SparkContext ... done'

    mpp = ModelParametersParser(spark, args.debug, not args.use_personal_path, args.output_table)
    mpp.run()

    print '['+time.ctime()+']', 'Stopping SparkContext'
    spark.stop()

    print '['+time.ctime()+']', 'Process finished!'
