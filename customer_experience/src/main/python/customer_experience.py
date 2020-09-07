#!/usr/bin/python
# -*- coding: utf-8 -*-

# PYSPARK_DRIVER_PYTHON=ipython ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/pyspark --packages com.databricks:spark-csv_2.10:1.5.0
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=8G ~/fy17.capsule/convergence/src/main/python/convergence.py 201803 201804 2>&1 | tee salida.convergence
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py --oracle 201708 201709 2>&1 | tee salida.convergence
# tail -f -n +1 salida.cex | grep -v $'..\/..\/.. ..:..:..\|^[\t]\+at\|java.io.IOException'
#
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 16G --driver-memory 8G --conf spark.executor.cores=5 --conf spark.dynamicAllocation.minExecutors=10 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=2G --conf spark.yarn.executor.driverOverhead=800m --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseGCOverheadLimit' /var/SP/data/home/bbergua/fy17.capsule/customer_experience/src/main/python/customer_experience.py 20181031 > salida.cex.20181031
#
# Naveen:
#   spark.executor.memory   6g (--executor-memory 6G
#   spark.executor.memoryOverhead =(minimum of 384) 10% of spark.executor.memory
#   spark.driver.memory      2g (In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-memory command line option or in your default properties file.)
#   spark.driver.memoryOverhead = driverMemory * 0.10, with minimum of 384


from common.src.main.python.utils.hdfs_generic import *
#from configuration import Configuration
#from DP_prepare_input_amdocs_ids import DPPrepareInputAmdocsIds
#from DP_prepare_input_cvm_pospago import DPPrepareInputCvmPospago
import argparse
from collections import OrderedDict
import re
import sys
import tabulate
import time
sys.path.append('/var/SP/data/home/bbergua/fy17.capsule/utils/src/main/python/general_model_trainer')
from general_model_trainer import GeneralModelTrainer
from model_outputs import ModelParameters, ModelScores
from pyspark.sql.functions import asc, col, concat, datediff, lit, min as sql_min, months_between, row_number, split, to_date, when
from pyspark.sql.types import ArrayType, DateType, DoubleType, IntegerType
from pyspark.sql.window import Window
# from pyspark.sql.utils import IllegalArgumentException

class CustomerExperience:

    def __init__(self, spark, model_month, oracle=False, debug=False):
        #import general_model_trainer
        #print general_model_trainer.__file__
        #import os
        #print os.path.abspath(general_model_trainer.__file__)
        self.spark = spark
        #self.spark.sparkContext.setCheckpointDir('/tmp/bbergua/tnps_data/checkpoint')
        self.model_month = model_month
        self.oracle = oracle
        self.debug = debug
        self.train_cols = None
        self.modeller = None
        self.varimp = None
        self.predictions = None
        self.evaluation_predictions_train = None
        self.evaluation_predictions_balanced_test = None
        self.evaluation_predictions_unbalanced_test = None
        self.model_params = None
        self.model_scores = None
        self.id_cols = [
            "Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_CODIGO_POSTAL", "Cust_CODIGO_POSTAL",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE",
            "Serv_NUM_SERIE_DECO_TV",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
             "Order_h_N1_Id", "Order_h_N2_Id", "Order_h_N3_Id", "Order_h_N4_Id", "Order_h_N5_Id", "Order_h_N6_Id",
            "Order_h_N7_Id", "Order_h_N8_Id", "Order_h_N9_Id", "Order_h_N10_Id"
        ]


    def load_bdp_amdocs_ids(self):
        ClosingDay=str(self.model_month)
        hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))

        print '['+time.ctime()+']', 'Reading Amdocs IDS Srv', ClosingDay, '...'

        #self.data = self.spark.read.table('tests_es.bbergua_amdocs_ids_srv')
        #self.data = self.spark.read.table('tests_es.amdocs_ids_srv_v3')
        #self.data = self.spark.read.load('/data/udf/vf_es/amdocs_ids/bbergua_amdocs_ids_service_level/'+hdfs_partition_path)
        #self.ids = self.spark.read.parquet('/data/udf/vf_es/amdocs_inf_dataset/bbergua_all_amdocs_inf_dataset/'+hdfs_partition_path)
        self.ids = self.spark.read.parquet('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/'+hdfs_partition_path)
        #self.ids = self.spark.read.parquet('/user/bbergua/amdocs_inf_dataset/amdocs_ids_service_level/'+hdfs_partition_path)
        #self.ids = self.spark.read.parquet('/user/bbergua/amdocs_inf_dataset_new/amdocs_ids_service_level/'+hdfs_partition_path)

        print '['+time.ctime()+']', 'Reading Amdocs IDS Srv', ClosingDay, '... done'

        print '['+time.ctime()+']', 'Post-processing Amdocs IDS Srv', ClosingDay


        # Fix types
        self.ids = self.ids.withColumn("TNPS_list_VDN", split(col("TNPS_list_VDN"), ",\s*").cast(ArrayType(IntegerType())).alias("TNPS_list_VDN"))

        vars_really_int = [
            'Cust_FACTURA_CATALAN',
            'Cust_L2_nacionalidad_is_spain',
            'Serv_OOB',
            'Serv_CONSUM_MIN',
            'Serv_TV_CUOT_CHARGES',
            'Serv_TV_PROMO',
            'Serv_TV_PROMO_USER',
            'Serv_TV_ABONOS',
            'Serv_TV_LOYALTY',
            'Serv_TV_SVA',
            'Serv_FOOTBALL_TV',
            'Serv_MOTOR_TV',
            'Serv_ZAPPER_TV',
            'Serv_TRYBUY_TV',
            'Serv_TRYBUY_AUTOM_TV'
        ]

        for col_name in vars_really_int:
            if col_name in self.ids.columns:
                #print dict(self.ids.dtypes)[col_name]
                self.ids = self.ids.withColumn(col_name, col(col_name).cast(IntegerType()))

        imputed_vars = [
            'Bill_L2_N1_yearmonth_billing_imputed_avg',
            #[1] 201908 201908 201908 201908 201908 201908
            'Bill_L2_N2_yearmonth_billing_imputed_avg',
            #[1] 201907 201907 201907 201907 201907 201907
            'Bill_L2_N3_yearmonth_billing_imputed_avg',
            #[1] 201906 201906 201905 201906 201906 201906
            'Bill_L2_N4_yearmonth_billing_imputed_avg',
            #[1] 201905.0 201905.0 201888.1 201888.1 201905.0 201905.0
            'Bill_L2_N5_yearmonth_billing_imputed_avg'
            #[1] 201904.0 201904.0 201887.3 201887.3 201904.0 201904.0
        ]
        # TODO: Imputed vars must be recalculated correctly
        for col_name in imputed_vars:
            if col_name in self.ids.columns:
                #print dict(self.ids.dtypes)[col_name]
                self.ids = self.ids.withColumn(col_name, col(col_name).cast(DoubleType()))

        yearmonth_vars = [
            'Bill_N1_yearmonth_billing',
            #[1] 201908 201908 201908 201908 201908 201908
            'Bill_N2_yearmonth_billing',
            #[1] 201907 201907 201907 201907 201907 201907
            'Bill_N3_yearmonth_billing',
            #[1] 201906 201906 201905 201906 201906 201906
            'Bill_N4_yearmonth_billing',
            #[1] 201905 201905 201905 201905 201905 201905
            'Bill_N5_yearmonth_billing'
            #[1] 201904 201904 201904 201904 201904 201904
        ]
        # YearMonth vars
        for col_name in yearmonth_vars:
            if col_name in self.ids.columns:
                self.ids = self.ids.withColumn(col_name, concat(col(col_name), lit('01')))
        #self.ids.select(['Bill_N1_yearmonth_billing', 'Bill_N2_yearmonth_billing', 'Bill_N3_yearmonth_billing', 'Bill_N4_yearmonth_billing', 'Bill_N5_yearmonth_billing']).show()

        date_vars = [
            'CCC_L2_first_interaction',
            #[1] 20190827 20190801 20190822 20190807 20190817 20190813
            'CCC_L2_latest_interaction'
            #[1] 20190827 20190806 20190822 20190813 20190817 20190813
        ]
        #'closing_day'
        #[1] 20190831 20190831 20190831 20190831 20190831 20190831

        for col_name in yearmonth_vars+date_vars+['closing_day']:
            if col_name in self.ids.columns:
                #print dict(self.ids.dtypes)[col_name]
                self.ids = self.ids.withColumn(col_name, to_date(col(col_name), 'yyyyMMdd'))

        now = time.localtime()

        for col_name in yearmonth_vars:
            if col_name in self.ids.columns:
                self.ids = (self.ids.withColumn(col_name+'_months_since', months_between(to_date(lit(time.strftime('%Y-%m', )+'-01')), col(col_name)).cast(IntegerType())))

        for col_name in date_vars:
            if col_name in self.ids.columns:
                self.ids = (self.ids.withColumn(col_name+'_days_since', datediff(to_date(lit(time.strftime('%Y-%m-%d', now))), col(col_name))))

        type_counts = {}
        for v in self.ids.dtypes:
            type_counts[v[1]] = type_counts.get(v[1], []) + [v[0]]
        #for k in type_counts.keys():
        #    print('There are', len(type_counts[k]), k, 'columns')

        for col_name in type_counts['timestamp']:
            self.ids = (self.ids.withColumn(col_name+'_days_since', datediff(to_date(lit(time.strftime('%Y-%m-%d', now))), col(col_name).cast(DateType()))))

        print '['+time.ctime()+']', 'Post-processing Amdocs IDS Srv', ClosingDay, '... done'

    @staticmethod
    def calculate_proportions(df, colname='TNPS_min_VDN'):
        print '['+time.ctime()+']', 'Calculating proportions ...\n'

    	print 'TNPS01 colname:', colname

        df_proportions_dict = dict(df.groupby(colname).count().collect())

        df_all_count = sum(df_proportions_dict.values())
        print 'Total count:', df_all_count, '\n'
        if df_all_count > 0:
            df_count = sum([v for k, v in df_proportions_dict.iteritems() if k is not None])
            print tabulate.tabulate(
                [['null', df_all_count-df_count, 100.0*(df_all_count-df_count)/df_all_count],
                 ['TNPS01', df_count, 100.0*(df_count)/df_all_count]]
                , ['TNPS01', 'count', 'proportion'], tablefmt='orgtbl')

            try:
                del df_proportions_dict[None]
            except KeyError:
                pass

            print '\nTNPS01 count:', df_count, '\n'
            print tabulate.tabulate(
                [[k, v, 100.0*v/df_count, 100.0*v/df_all_count] for k,v in df_proportions_dict.iteritems()]
                , ['TNPS01', 'count', 'proportion', 'proportion_general'], tablefmt='orgtbl')

            df_proportions_dict_tnps4 = OrderedDict(
                [['HARD DETRACTOR', sum([v for k,v in df_proportions_dict.iteritems() if k >= 0   and k < 3.5])],
                 ['SOFT DETRACTOR', sum([v for k,v in df_proportions_dict.iteritems() if k >= 3.5 and k < 6.5])],
                 ['NEUTRAL',        sum([v for k,v in df_proportions_dict.iteritems() if k >= 6.5 and k < 8.5])],
                 ['PROMOTER',       sum([v for k,v in df_proportions_dict.iteritems() if k >= 8.5 and k <= 10])]])
            print '\nTNPS4 count:', df_count, '\n'
            print tabulate.tabulate(
                [[k, v, 100.0*v/df_count, 100.0*v/df_all_count] for k,v in df_proportions_dict_tnps4.iteritems()]
                , ['TNPS4', 'count', 'proportion', 'proportion_general'], tablefmt='orgtbl')

            df_proportions_dict_tnps = OrderedDict(
                [['DETRACTOR', sum([v for k,v in df_proportions_dict_tnps4.iteritems() if 'DETRACTOR' in k])],
                 ['NEUTRAL',   sum([v for k,v in df_proportions_dict_tnps4.iteritems() if 'NEUTRAL'   == k])],
                 ['PROMOTER',  sum([v for k,v in df_proportions_dict_tnps4.iteritems() if 'PROMOTER'  == k])]])
            print '\nTNPS count:', df_count, '\n'
            print tabulate.tabulate(
                [[k, v, 100.0*v/df_count, 100.0*v/df_all_count] for k,v in df_proportions_dict_tnps.iteritems()]
                , ['TNPS', 'count', 'proportion', 'proportion_general'], tablefmt='orgtbl')

            for k,v in df_proportions_dict.iteritems():
                df_proportions_dict[k] = float(v)/df_count

        print '\n['+time.ctime()+']', 'Calculating proportions ... done'

        return df_proportions_dict

    @staticmethod
    def calculate_cutpoints(nrows, proportions):
        print '['+time.ctime()+']', 'Calculating cutpoints ...'

        print 'nrows:', nrows

        print 'proportions:', proportions

        counts = {}
        for k in proportions.keys():
            counts[k] = int(round(nrows*proportions[k]))
        diff = nrows - sum(counts.values())
        if (diff > 0):
            min_key = min(counts, key=counts.get) # Get the key with minimum value
            counts[min_key] = counts[min_key] + diff # Assign rest to key with minimum number of rows
        elif (diff < 0):
            max_key = max(counts, key=counts.get) # Get the key with maximum value
            counts[max_key] = counts[max_key] + diff # Remove remaining to key with maximum number of rows
        print 'counts:', counts
        print 'sum(counts):', sum(counts.values())

        ini = 1
        end = 1
        ranges = {}
        for k in proportions.keys():# It is necessary that the keys are ordered
            if end > 1:
                ini = end + 1
            end = ini + counts[k] - 1
            ranges[k] = [ini, end]
            #print k, ini, end
        print 'ranges:', ranges

        print '['+time.ctime()+']', 'Calculating cutpoints ... done'

        return ranges

    @staticmethod
    def apply_cutpoints_predictions(preds, proportions):
        print '['+time.ctime()+']', 'Appling cutpoints to predictions ...'

        preds_nrows = preds.count()
        ranges = CustomerExperience.calculate_cutpoints(preds_nrows, proportions)

        # Sort by score column before applying cutpoints
        # print 'NumPartitions', preds.rdd.getNumPartitions() # 200
        w_score = Window().orderBy(asc('score')) # .partitionBy("msisdn")
        preds = (preds.withColumn('rowNum', row_number().over(w_score)))
        # print 'NumPartitions', preds.rdd.getNumPartitions() # 1
        # preds at this point has 1 partition, so repartition
        preds = preds.repartition(200)
        preds = preds.cache()
        #preds.select('score', 'rowNum').show()

        preds = preds.withColumn('predict_TNPS01', lit(-1))
        for k in ranges.keys():
            r = range(ranges[k][0], ranges[k][1]+1, 1)
            print k, r[0], r[-1]
            preds = preds.withColumn('predict_TNPS01', when(preds['rowNum'].between(r[0], r[-1]), lit(k)).otherwise(preds['predict_TNPS01']))
        #preds.select('score', 'rowNum', 'predict_TNPS01').show()
        #preds.select('score', 'rowNum', 'predict_TNPS01').orderBy('score', ascending=False).show()

        print '['+time.ctime()+']', 'Appling cutpoints to predictions ... done'

        return preds

    @staticmethod
    def add_tnpsx_cols(df, colname=None):
        m = re.findall('(.*)TNPS(01)?', colname)
        colbasename=m[0][0]
        if colbasename == '':
            colbasename = colname+'_'

        df = df.withColumn(colbasename+'TNPS4',
                            when(df[colname].isin(9, 10),      'PROMOTER')
                           .when(df[colname].isin(7, 8),       'NEUTRAL')
                           .when(df[colname].isin(4, 5, 6),    'SOFT DETRACTOR')
                           .when(df[colname].isin(0, 1, 2, 3), 'HARD DETRACTOR'))
                           #.otherwise(False))
        df = df.withColumn(colbasename+'TNPS',
                            when(df[colbasename+'TNPS4'].isin('HARD DETRACTOR', 'SOFT DETRACTOR'), 'DETRACTOR')
                           .otherwise(df[colbasename+'TNPS4']))
        df = df.withColumn(colbasename+'TNPS3PRONEU',
                            when(df[colbasename+'TNPS4'].isin('NEUTRAL', 'PROMOTER'), 'NON DETRACTOR')
                           .otherwise(df[colbasename+'TNPS4']))
        df = df.withColumn(colbasename+'TNPS3NEUSDET',
                            when(df[colbasename+'TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL'), 'INNER')
                           .otherwise(df[colbasename+'TNPS4']))
        df = df.withColumn(colbasename+'TNPS2HDET',
                            when(df[colbasename+'TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL', 'PROMOTER'), 'NON HARD DETRACTOR')
                           .otherwise(df[colbasename+'TNPS4']))
        df = df.withColumn(colbasename+'TNPS2SDET',
                            when(df[colbasename+'TNPS4'].isin('HARD DETRACTOR', 'NEUTRAL', 'PROMOTER'), 'NON SOFT DETRACTOR')
                           .otherwise(df[colbasename+'TNPS4']))
        df = df.withColumn(colbasename+'TNPS2DET',
                            when(df[colbasename+'TNPS'].isin('NEUTRAL', 'PROMOTER'), 'NON DETRACTOR')
                           .otherwise(df[colbasename+'TNPS']))
        df = df.withColumn(colbasename+'TNPS2NEU',
                            when(df[colbasename+'TNPS'].isin('DETRACTOR', 'PROMOTER'), 'NON NEUTRAL')
                           .otherwise(df[colbasename+'TNPS']))
        df = df.withColumn(colbasename+'TNPS2PRO',
                            when(df[colbasename+'TNPS'].isin('DETRACTOR', 'NEUTRAL'), 'NON PROMOTER')
                           .otherwise(df[colbasename+'TNPS']))
        df = df.withColumn(colbasename+'TNPS2INOUT',
                            when(df[colbasename+'TNPS4'].isin('SOFT DETRACTOR', 'NEUTRAL'), 'INNER')
                           .when(df[colbasename+'TNPS4'].isin('HARD DETRACTOR', 'PROMOTER'), 'OUTER'))

        return df

    def run(self):
        self.load_bdp_amdocs_ids()

        print '['+time.ctime()+']', 'Calculating segment counts ...'
        #self.ids.filter(self.ids['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif').count().sort(['Cust_Agg_seg_pospaid_nif']).show()
        #self.ids.filter(self.ids['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc').count().sort(['Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc']).show()
        #segment_counts_list = self.ids.filter(self.ids['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif').count().sort('Cust_Agg_seg_pospaid_nif').rdd.map(lambda x: [x[0], x[1]]).collect()
        #segment_counts_dict = dict(self.ids.filter(self.ids['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif').count().sort('Cust_Agg_seg_pospaid_nif').rdd.map(lambda x: [x[0], x[1]]).collect())
        segment_counts_list = self.ids.filter(self.ids['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc').count().sort('Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc').rdd.map(lambda x: [x[0], x[1], x[2]]).collect()
        segment_counts_dict = {x[0]:{} for x in segment_counts_list}
        for x in segment_counts_list:
            segment_counts_dict[x[0]][x[1]]=x[2]
        #print segment_counts_dict
        count_all = sum(sum(v.values()) for v in segment_counts_dict.values())

        print '\nTotal row count:', count_all
        print '\n', tabulate.tabulate([[k, sum(segment_counts_dict[k].values())] for k in segment_counts_dict.keys()], ['Cust_Agg_seg_pospaid_nif', 'count'], tablefmt='orgtbl')
        print '\n', tabulate.tabulate(segment_counts_list, ['Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc', 'count'], tablefmt='orgtbl'), '\n'

        self.ids = CustomerExperience.add_tnpsx_cols(self.ids, 'TNPS_min_VDN')
        self.ids = self.ids.withColumn('TNPS_min_VDN_TNPS2DET_i', when(self.ids['TNPS_min_VDN_TNPS2DET'] == 'DETRACTOR', lit(0))
                                                                 .when(self.ids['TNPS_min_VDN_TNPS2DET'] == 'NON DETRACTOR', lit(1))
                                                                 .otherwise(self.ids['TNPS_min_VDN_TNPS2DET']).cast(IntegerType()))

        # Since model_month is last day of month, the data will be partitioned in model_month + 1 month
        partition_year = int(str(self.model_month)[:4])
        partition_month = int(str(self.model_month)[4:6]) + 1
        if partition_month > 12:
            partition_year = partition_year + 1
            partition_month = 1

        modeller = None

        best_cat_vars = {
            'Convergent-OW'    :['Order_h_N1_Description', 'Cust_FLG_LORTAD', 'Serv_dto_lev3', 'Penal_CUST_APPLIED_N5_cod_promo', 'Serv_DECO_TV', 'Serv_FBB_UPGRADE',
         'Cust_Agg_seg_pospaid_nif', 'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Cust_L2_x_user_twitter_has_user',
         'Penal_CUST_PENDING_N4_cod_promo', 'Serv_ROAM_USA_EUR', 'Cust_Agg_GRUPO_EXONO_PREOW_nc', 'Penal_CUST_APPLIED_N4_cod_promo',
         'Penal_SRV_FINISHED_N4_desc_promo', 'Penal_SRV_APPLIED_N5_cod_promo', 'Cust_L2_x_user_facebook_has_user', 'Penal_SRV_PENDING_N5_cod_penal', 'Serv_TV_TARIFF',
         'Cust_x_tipo_cuenta_corp', 'Penal_CUST_APPLIED_N5_desc_promo', 'Serv_TV_CUOTA_ALTA', 'Penal_CUST_APPLIED_N5_cod_penal', 'Penal_CUST_PENDING_N5_cod_penal', 'Serv_PVR_TV',
         'Penal_CUST_APPLIED_N3_cod_promo', 'Penal_SRV_APPLIED_N5_cod_penal',
 'Penal_CUST_APPLIED_N5_desc_penal', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE',
         'Penal_CUST_FINISHED_N5_cod_promo', 'Penal_SRV_PENDING_N1_cod_penal', 'Serv_HOMEZONE', 'Penal_SRV_PENDING_N5_desc_penal', 'Penal_CUST_APPLIED_N4_desc_promo',
         'Penal_SRV_APPLIED_N3_cod_promo', 'Penal_CUST_PENDING_N5_desc_penal', 'Penal_SRV_FINISHED_N5_desc_penal', 'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_CUST_APPLIED_N4_desc_penal',
         'Penal_CUST_APPLIED_N1_desc_promo', 'Penal_CUST_PENDING_N5_cod_promo', 'Penal_SRV_PENDING_N5_cod_promo', 'Penal_SRV_APPLIED_N5_desc_promo', 'Serv_dto_lev2', 'Serv_NETFLIX_NAPSTER',
         'Penal_CUST_PENDING_N5_desc_promo', 'Penal_SRV_APPLIED_N5_desc_penal', 'Penal_SRV_PENDING_N5_desc_promo',  'Cust_x_antiguedad_cuenta', 'Penal_SRV_FINISHED_N5_desc_promo',
         'Penal_CUST_PENDING_N4_desc_promo', 'Cust_x_datos_trafico', 'Penal_SRV_PENDING_N4_cod_promo',
         'Cust_X_IDIOMA_FACTURA', 'Penal_SRV_APPLIED_N4_cod_penal', 'Penal_SRV_FINISHED_N5_cod_promo', 'Device_N4_manufacturer', 'Penal_CUST_APPLIED_N3_desc_promo', 'Penal_CUST_PENDING_N3_cod_promo',
         'Penal_SRV_APPLIED_N4_cod_promo', 'Penal_SRV_FINISHED_N5_cod_penal', 'Penal_SRV_APPLIED_N4_desc_penal', 'Penal_SRV_PENDING_N4_desc_promo', 'Cust_TRATAMIENTO', 'Penal_CUST_APPLIED_N3_cod_penal',
         'Penal_SRV_FINISHED_N4_cod_promo', 'Penal_SRV_APPLIED_N4_desc_promo', 'Penal_CUST_PENDING_N4_cod_penal', 'Penal_CUST_APPLIED_N3_desc_penal', 'Penal_SRV_FINISHED_N2_cod_promo', 'Cust_TIPO_DOCUMENTO',
         'Penal_CUST_PENDING_N4_desc_penal', 'Penal_CUST_FINISHED_N4_desc_promo', 'Penal_SRV_PENDING_N4_cod_penal', 'Penal_SRV_FINISHED_N4_desc_penal', 'Penal_CUST_PENDING_N2_cod_promo',
         'Cust_cta_correo_flag', 'Penal_SRV_PENDING_N4_desc_penal', 'Penal_SRV_FINISHED_N4_cod_penal',
         'Penal_SRV_APPLIED_N2_desc_promo', 'Penal_CUST_FINISHED_N5_desc_promo', 'Penal_SRV_FINISHED_N3_desc_promo',
         'Cust_COD_ESTADO_GENERAL', 'Penal_SRV_PENDING_N3_cod_penal', 'Cust_FLG_ROBINSON', 'Penal_CUST_PENDING_N3_cod_penal', 'Penal_SRV_APPLIED_N3_desc_promo', 'Penal_SRV_FINISHED_N3_desc_penal',
         'Cust_X_FORMATO_FACTURA', 'Penal_CUST_APPLIED_N2_cod_penal', 'Penal_SRV_FINISHED_N1_desc_promo', 'Penal_CUST_PENDING_N3_desc_promo', 'Penal_SRV_FINISHED_N3_cod_promo', 'Penal_CUST_FINISHED_N5_cod_penal',
         'Serv_L2_dto_lev2_proc', 'Penal_SRV_FINISHED_N3_cod_penal', 'Penal_SRV_APPLIED_N3_cod_penal', 'Penal_CUST_PENDING_N3_desc_penal', 'Penal_SRV_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N1_desc_promo',
         'Cust_ENCUESTAS', 'Penal_CUST_FINISHED_N5_desc_penal', 'Device_N1_device_type', 'Penal_SRV_APPLIED_N3_desc_penal', 'Penal_CUST_APPLIED_N2_desc_penal',
         'Cust_x_datos_navegacion', 'Penal_SRV_PENDING_N3_cod_promo', 'Penal_CUST_FINISHED_N3_desc_promo', 'Serv_dto_lev1', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_SRV_PENDING_N3_desc_promo',
         'Order_h_N4_Description', 'Penal_CUST_APPLIED_N2_cod_promo', 'Penal_CUST_FINISHED_N4_cod_promo', 'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_PENDING_N2_cod_promo',
         'Penal_CUST_PENDING_N2_cod_penal', 'Penal_CUST_APPLIED_N2_desc_promo', 'Serv_data_additional', 'Penal_CUST_FINISHED_N4_cod_penal', 'Penal_SRV_FINISHED_N2_cod_penal', 'Penal_SRV_APPLIED_N2_cod_promo',
         'Penal_CUST_FINISHED_N4_desc_penal', 'Cust_L2_X_IDIOMA_FACTURA_proc', 'Device_N2_device_type', 'Cust_PUBLICIDAD', 'Penal_CUST_PENDING_N2_desc_penal', 'Cust_x_cesion_datos', 'Penal_SRV_APPLIED_N1_cod_penal',
         'Penal_CUST_FINISHED_N2_desc_promo', 'Cust_L2_codigo_postal_city', 'Penal_CUST_PENDING_N2_desc_promo', 'Cust_ENCUESTAS2', 'Penal_SRV_PENDING_N2_cod_penal', 'Penal_CUST_FINISHED_N3_cod_penal',
         'Penal_SRV_APPLIED_N2_desc_penal', 'Cust_NACIONALIDAD',  'Penal_CUST_FINISHED_N3_desc_penal', 'Penal_SRV_APPLIED_N2_cod_penal', 'Cust_METODO_PAGO',
         'Penal_SRV_PENDING_N2_desc_penal', 'Cust_L2_SUPEROFERTA_proc', 'Cust_X_PUBLICIDAD_EMAIL', 'Penal_CUST_FINISHED_N3_cod_promo', 'Device_N3_device_type',
         'Cust_FACTURA_ELECTRONICA', 'Serv_data',
          'Penal_SRV_PENDING_N2_desc_promo', 'Order_h_N1_Status', 'Penal_CUST_APPLIED_N1_cod_promo', 'Penal_CUST_FINISHED_N2_cod_penal', 'Penal_CUST_FINISHED_N1_desc_promo', 'Order_h_N10_Status',
         'Penal_SRV_FINISHED_N1_desc_penal', 'Serv_L2_dto_lev1_proc', 'Device_N4_device_type', 'Cust_Agg_seg_pospaid_nc', 'Penal_CUST_FINISHED_N2_desc_penal', 'Cust_L2_X_FORMATO_FACTURA_proc',
         'Penal_CUST_PENDING_N1_cod_penal', 'Penal_SRV_PENDING_N1_desc_penal', 'Penal_SRV_APPLIED_N1_desc_penal',
         'Penal_CUST_APPLIED_N1_cod_penal', 'Order_h_N2_Status', 'Penal_CUST_FINISHED_N2_cod_promo',
          'Serv_ROAMING_BASIC', 'Device_N3_OS_initial_version',
         'Penal_SRV_FINISHED_N1_cod_penal', 'Penal_CUST_APPLIED_N1_desc_penal', 'Cust_Agg_L2_first_srv_basic_name_nc',
         'Device_N1_OS', 'Penal_CUST_PENDING_N1_desc_penal', 'Serv_L2_data_additional_proc',
         'Device_N4_OS_initial_version', 'Penal_SRV_APPLIED_N1_desc_promo', 'Order_h_N3_Status',
         'Penal_SRV_PENDING_N1_desc_promo', 'Cust_SUPEROFERTA', 'Penal_SRV_FINISHED_N1_cod_promo',
         'Device_N2_OS', 'Order_h_N8_Status', 'Penal_CUST_FINISHED_N1_cod_penal', 'Device_N4_OS',
         'Penal_SRV_APPLIED_N1_cod_promo', 'Serv_tipo_sim', 'Order_h_N2_Class', 'Penal_CUST_PENDING_N1_cod_promo',
         'Penal_SRV_PENDING_N1_cod_promo', 'Serv_voice_tariff', 'Device_N3_OS', 'Order_h_N6_Status',
         'Penal_CUST_FINISHED_N1_desc_penal', 'Serv_L2_tipo_sim_proc', 'Order_h_N9_Status', 'Device_N1_OS_initial_version',
         'Order_h_N4_Status', 'Cust_CODIGO_POSTAL', 'Device_N2_OS_initial_version', 'Penal_CUST_FINISHED_N1_cod_promo',
         'Order_h_N1_Class', 'Device_N4_model', 'Order_h_N5_Status', 'Device_N3_manufacturer', 'Serv_L2_data_proc',
         'Order_h_N10_Class', 'Serv_roam_zona_2', 'Order_h_N3_Class', 'Order_h_N7_Status', 'Device_N1_manufacturer',
         'Device_N2_manufacturer', 'Serv_L2_voice_tariff_proc', 'Order_h_N9_Class', 'Serv_RGU', 'Device_N3_model',
         'Order_h_N5_Class', 'Serv_SRV_BASIC', 'Order_h_N8_Class', 'Serv_L2_roam_zona_2_proc', 'Serv_DESC_SRV_BASIC',
         'Order_h_N6_Class', 'Order_h_N4_Class', 'Order_h_N7_Class', 'Device_N2_model', 'Order_h_N2_Description',
         'Order_h_N10_Description', 'Device_N1_model', 'Order_h_N3_Description', 'Order_h_N9_Description',
         'Serv_L2_tariff_proc', 'Order_h_N8_Description', 'Order_h_N5_Description', 'Order_h_N6_Description',
         'Serv_DESC_TARIFF', 'Order_h_N7_Description', 'Serv_L2_desc_tariff_proc', 'Serv_tariff'],


            'Convergent-EX_ONO':['Order_h_N1_Description', 'Cust_FLG_LORTAD', 'Serv_dto_lev3', 'Serv_NETFLIX_NAPSTER', 'Serv_HOMEZONE',
         'Serv_FBB_UPGRADE', 'Cust_L2_x_user_facebook_has_user', 'Serv_DECO_TV', 'Serv_TV_CUOTA_ALTA',
         'Serv_TV_TARIFF', 'Penal_SRV_PENDING_N5_cod_promo', 'Serv_PVR_TV', 'Cust_Agg_seg_pospaid_nif',
         'Cust_Agg_GRUPO_EXONO_PREOW_nc', 'Penal_CUST_APPLIED_N5_cod_promo',
         'Penal_CUST_APPLIED_N5_desc_promo', 'Cust_L2_x_user_twitter_has_user', 'Penal_CUST_APPLIED_N5_cod_penal',
         'Serv_ROAM_USA_EUR', 'Penal_CUST_APPLIED_N5_desc_penal', 'Penal_SRV_PENDING_N4_cod_penal',
         'Penal_CUST_APPLIED_N4_cod_promo', 'Penal_CUST_PENDING_N3_cod_promo', 'Penal_SRV_APPLIED_N4_desc_promo',
         'Penal_CUST_PENDING_N3_desc_promo', 'Cust_x_tipo_cuenta_corp', 'Device_N4_manufacturer',
         'Penal_CUST_PENDING_N3_cod_penal', 'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N4_cod_promo',
         'Penal_CUST_PENDING_N4_desc_promo', 'Penal_CUST_PENDING_N4_cod_penal', 'Cust_NACIONALIDAD',
         'Penal_CUST_PENDING_N4_desc_penal', 'Penal_CUST_PENDING_N5_cod_promo', 'Penal_CUST_PENDING_N5_desc_promo',
         'Penal_CUST_APPLIED_N3_cod_promo', 'Penal_CUST_PENDING_N5_cod_penal', 'Penal_CUST_PENDING_N5_desc_penal',
         'Penal_CUST_APPLIED_N4_desc_promo', 'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_SRV_PENDING_N5_desc_promo',
         'Penal_SRV_FINISHED_N4_desc_promo', 'Penal_CUST_APPLIED_N4_desc_penal', 'Penal_CUST_PENDING_N2_cod_promo',
         'Penal_SRV_PENDING_N5_cod_penal', 'Penal_SRV_PENDING_N5_desc_penal', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE',
         'Penal_CUST_PENDING_N2_desc_promo', 'Cust_Agg_seg_pospaid_nc',
         'Cust_X_IDIOMA_FACTURA', 'Penal_SRV_PENDING_N4_desc_penal', 'Cust_x_cesion_datos',
         'Penal_SRV_APPLIED_N2_desc_promo', 'Serv_dto_lev2', 'Penal_CUST_PENDING_N2_cod_penal',
         'Penal_CUST_FINISHED_N5_cod_promo', 'Penal_CUST_APPLIED_N3_desc_promo', 'Cust_x_antiguedad_cuenta',
         'Penal_CUST_PENDING_N2_desc_penal', 'Penal_SRV_PENDING_N3_cod_promo', 'Cust_ENCUESTAS',
         'Penal_SRV_PENDING_N4_cod_promo', 'Cust_cta_correo_flag', 'Penal_CUST_APPLIED_N3_cod_penal',
         'Penal_CUST_PENDING_N1_desc_promo', 'Cust_METODO_PAGO', 'Penal_SRV_PENDING_N4_desc_promo',
         'Penal_SRV_APPLIED_N5_desc_promo', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_CUST_APPLIED_N3_desc_penal',
         'Penal_CUST_APPLIED_N2_cod_promo', 'Penal_SRV_PENDING_N2_cod_promo', 'Penal_SRV_APPLIED_N5_desc_penal',
         'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Cust_TIPO_DOCUMENTO', 'Penal_SRV_APPLIED_N5_cod_promo',
         'Device_N1_device_type', 'Cust_COD_ESTADO_GENERAL', 'Cust_Agg_L2_first_srv_basic_name_nc',
         'Penal_SRV_FINISHED_N5_desc_promo', 'Cust_X_FORMATO_FACTURA', 'Penal_SRV_APPLIED_N5_cod_penal',
         'Penal_SRV_PENDING_N3_desc_promo', 'Cust_L2_SUPEROFERTA_proc', 'Penal_SRV_APPLIED_N4_cod_promo',
         'Penal_CUST_FINISHED_N4_desc_promo', 'Penal_SRV_FINISHED_N5_desc_penal', 'Penal_SRV_APPLIED_N3_desc_promo',
         'Penal_SRV_PENDING_N3_cod_penal', 'Cust_FLG_ROBINSON', 'Penal_CUST_APPLIED_N1_cod_penal',
         'Penal_SRV_APPLIED_N4_desc_penal', 'Serv_L2_dto_lev2_proc', 'Penal_SRV_FINISHED_N3_desc_promo',
         'Penal_SRV_PENDING_N3_desc_penal', 'Penal_SRV_APPLIED_N4_cod_penal', 'Cust_TRATAMIENTO',
         'Penal_SRV_FINISHED_N5_cod_promo', 'Penal_SRV_APPLIED_N3_desc_penal', 'Penal_CUST_PENDING_N1_cod_penal',
         'Serv_data_additional', 'Penal_CUST_FINISHED_N5_desc_promo', 'Penal_SRV_FINISHED_N5_cod_penal',
         'Penal_CUST_APPLIED_N2_cod_penal', 'Penal_SRV_APPLIED_N3_cod_promo', 'Penal_SRV_PENDING_N1_cod_penal',
          'Penal_SRV_FINISHED_N4_desc_penal', 'Cust_L2_X_IDIOMA_FACTURA_proc',
         'Penal_SRV_APPLIED_N3_cod_penal',  'Penal_SRV_FINISHED_N3_desc_penal',
         'Penal_CUST_FINISHED_N2_desc_promo', 'Order_h_N2_Status', 'Penal_SRV_FINISHED_N4_cod_promo',
         'Penal_CUST_APPLIED_N2_desc_penal', 'Penal_CUST_PENDING_N1_desc_penal', 'Device_N2_device_type',
         'Penal_SRV_FINISHED_N4_cod_penal', 'Penal_CUST_FINISHED_N5_cod_penal', 'Penal_SRV_APPLIED_N2_cod_promo',
         'Penal_SRV_PENDING_N2_cod_penal', 'Penal_SRV_FINISHED_N3_cod_penal', 'Serv_dto_lev1',
         'Penal_CUST_APPLIED_N2_desc_promo', 'Cust_x_datos_trafico', 'Penal_SRV_FINISHED_N3_cod_promo',
         'Cust_L2_codigo_postal_city', 'Penal_CUST_PENDING_N1_cod_promo', 'Cust_PUBLICIDAD',
         'Penal_CUST_FINISHED_N5_desc_penal', 'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_APPLIED_N1_desc_penal',
         'Order_h_N1_Status', 'Penal_SRV_PENDING_N2_desc_penal', 'Penal_SRV_APPLIED_N2_desc_penal',
         'Penal_SRV_FINISHED_N1_desc_penal', 'Order_h_N6_Status', 'Device_N3_device_type',
         'Penal_CUST_FINISHED_N3_desc_promo', 'Penal_SRV_APPLIED_N2_cod_penal', 'Penal_SRV_FINISHED_N2_cod_promo',
         'Penal_SRV_PENDING_N2_desc_promo', 'Cust_x_datos_navegacion', 'Penal_CUST_APPLIED_N1_desc_penal',
         'Penal_SRV_FINISHED_N2_cod_penal', 'Penal_CUST_FINISHED_N4_cod_penal', 'Serv_roam_zona_2',
         'Cust_FACTURA_ELECTRONICA', 'Order_h_N3_Class', 'Device_N4_device_type',
         'Penal_CUST_FINISHED_N1_desc_promo', 'Cust_ENCUESTAS2', 'Cust_X_PUBLICIDAD_EMAIL',
         'Penal_CUST_FINISHED_N4_desc_penal',
         'Penal_CUST_APPLIED_N1_desc_promo', 'Order_h_N10_Status', 'Penal_SRV_FINISHED_N1_desc_promo',
         'Penal_SRV_APPLIED_N1_cod_penal', 'Device_N4_OS_initial_version',
         'Penal_CUST_FINISHED_N4_cod_promo', 'Serv_L2_data_additional_proc', 'Order_h_N2_Class',
         'Serv_L2_dto_lev1_proc', 'Penal_SRV_PENDING_N1_desc_penal', 'Cust_L2_X_FORMATO_FACTURA_proc',
         'Serv_voice_tariff', 'Order_h_N3_Status', 'Device_N3_OS', 'Penal_CUST_APPLIED_N1_cod_promo',
         'Penal_CUST_FINISHED_N3_cod_penal', 'Penal_SRV_FINISHED_N1_cod_penal', 'Penal_SRV_APPLIED_N1_desc_promo',
         'Device_N4_OS', 'Order_h_N4_Status', 'Device_N1_OS', 'Cust_SUPEROFERTA', 'Penal_SRV_PENDING_N1_desc_promo',
         'Penal_CUST_FINISHED_N3_desc_penal', 'Penal_SRV_FINISHED_N1_cod_promo', 'Device_N3_OS_initial_version',
         'Penal_SRV_APPLIED_N1_cod_promo', 'Device_N2_OS', 'Order_h_N5_Status', 'Penal_CUST_FINISHED_N2_cod_penal',
         'Order_h_N1_Class', 'Order_h_N8_Status', 'Penal_SRV_PENDING_N1_cod_promo', 'Penal_CUST_FINISHED_N3_cod_promo',
         'Serv_L2_roam_zona_2_proc', 'Device_N4_model', 'Penal_CUST_FINISHED_N1_cod_penal', 'Device_N1_OS_initial_version',
         'Order_h_N7_Status', 'Device_N3_manufacturer', 'Serv_L2_voice_tariff_proc', 'Order_h_N4_Class', 'Device_N2_OS_initial_version',
         'Penal_CUST_FINISHED_N2_desc_penal', 'Order_h_N9_Status', 'Serv_ROAMING_BASIC', 'Order_h_N10_Class',
         'Penal_CUST_FINISHED_N1_desc_penal', 'Cust_CODIGO_POSTAL', 'Order_h_N5_Class', 'Device_N1_manufacturer',
         'Penal_CUST_FINISHED_N2_cod_promo', 'Device_N2_manufacturer', 'Order_h_N6_Class', 'Serv_L2_data_proc',
         'Device_N3_model', 'Order_h_N9_Class', 'Serv_tipo_sim', 'Order_h_N7_Class', 'Order_h_N2_Description',
         'Serv_L2_tipo_sim_proc', 'Order_h_N8_Class', 'Penal_CUST_FINISHED_N1_cod_promo', 'Serv_RGU', 'Serv_SRV_BASIC',
         'Order_h_N3_Description', 'Serv_data', 'Serv_DESC_SRV_BASIC', 'Order_h_N10_Description', 'Device_N2_model',
         'Order_h_N4_Description', 'Order_h_N9_Description', 'Order_h_N5_Description', 'Device_N1_model',
         'Order_h_N7_Description', 'Order_h_N6_Description', 'Order_h_N8_Description', 'Serv_L2_tariff_proc',
         'Serv_DESC_TARIFF', 'Serv_L2_desc_tariff_proc', 'Serv_tariff'],



            'Convergent-PRE_OW':['Order_h_N1_Description', 'Cust_FLG_LORTAD', 'Serv_dto_lev3', 'Serv_NETFLIX_NAPSTER',
         'Serv_HOMEZONE', 'Serv_FBB_UPGRADE', 'Penal_SRV_PENDING_N4_cod_penal', 'Serv_DECO_TV',
         'Serv_TV_CUOTA_ALTA', 'Cust_L2_x_user_facebook_has_user', 'Serv_TV_TARIFF', 'Serv_PVR_TV',
         'Penal_SRV_FINISHED_N2_desc_promo', 'Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc',
         'Penal_CUST_APPLIED_N5_cod_promo', 'Cust_L2_x_user_twitter_has_user', 'Penal_CUST_APPLIED_N3_cod_promo',
         'Penal_CUST_APPLIED_N5_desc_promo', 'Penal_CUST_APPLIED_N5_cod_penal', 'Penal_CUST_APPLIED_N5_desc_penal',
         'Penal_CUST_PENDING_N3_cod_promo', 'Device_N4_model', 'Penal_CUST_PENDING_N2_cod_penal', 'Penal_CUST_PENDING_N3_desc_promo',
         'Penal_CUST_PENDING_N3_cod_penal', 'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N4_cod_promo',
         'Cust_x_tipo_cuenta_corp', 'Penal_CUST_PENDING_N4_desc_promo', 'Penal_CUST_PENDING_N4_cod_penal',
         'Serv_dto_lev2', 'Penal_CUST_PENDING_N4_desc_penal', 'Penal_CUST_FINISHED_N5_cod_promo',
         'Penal_SRV_PENDING_N4_desc_penal', 'Penal_SRV_APPLIED_N4_cod_penal',
         'Penal_CUST_PENDING_N5_cod_promo', 'Penal_CUST_PENDING_N5_desc_promo', 'Serv_ROAM_USA_EUR',
         'Penal_CUST_PENDING_N5_cod_penal', 'Penal_CUST_APPLIED_N2_cod_promo', 'Penal_CUST_PENDING_N5_desc_penal',
         'Cust_X_IDIOMA_FACTURA', 'Cust_Agg_seg_pospaid_nc', 'Penal_CUST_APPLIED_N4_cod_promo',
         'Penal_CUST_PENDING_N2_desc_penal', 'Penal_SRV_PENDING_N4_cod_promo', 'Penal_CUST_APPLIED_N4_desc_promo',
         'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_SRV_PENDING_N5_cod_penal', 'Penal_CUST_PENDING_N1_cod_promo',
         'Penal_CUST_APPLIED_N4_desc_penal', 'Cust_NACIONALIDAD', 'Penal_SRV_PENDING_N5_desc_penal',
         'Penal_SRV_APPLIED_N5_cod_promo', 'Penal_SRV_FINISHED_N4_desc_promo', 'Serv_L2_dto_lev2_proc',
         'Penal_SRV_PENDING_N5_cod_promo', 'Cust_METODO_PAGO', 'Device_N1_device_type', 'Penal_SRV_PENDING_N5_desc_promo',
         'Penal_CUST_APPLIED_N3_desc_promo', 'Penal_CUST_PENDING_N2_cod_promo', 'Penal_SRV_APPLIED_N5_cod_penal',
         'Penal_SRV_PENDING_N4_desc_promo', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE', 'Penal_CUST_APPLIED_N3_cod_penal',
         'Penal_CUST_PENDING_N2_desc_promo', 'Penal_SRV_APPLIED_N3_desc_promo', 'Cust_ENCUESTAS', 'Order_h_N3_Description',
         'Penal_CUST_APPLIED_N3_desc_penal', 'Penal_SRV_PENDING_N3_cod_penal', 'Penal_SRV_APPLIED_N5_desc_promo',
         'Penal_SRV_FINISHED_N4_desc_penal', 'Cust_x_datos_trafico', 'Cust_cta_correo_flag',
         'Penal_SRV_APPLIED_N5_desc_penal', 'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Penal_CUST_FINISHED_N4_desc_promo',
         'Penal_SRV_PENDING_N3_desc_penal', 'Penal_SRV_APPLIED_N4_desc_penal', 'Penal_SRV_FINISHED_N5_desc_promo',
         'Cust_FACTURA_ELECTRONICA', 'Penal_SRV_APPLIED_N4_desc_promo', 'Cust_L2_X_IDIOMA_FACTURA_proc',
         'Penal_CUST_APPLIED_N1_desc_promo', 'Penal_SRV_PENDING_N3_cod_promo', 'Serv_dto_lev1', 'Penal_SRV_FINISHED_N5_desc_penal',
         'Cust_PUBLICIDAD', 'Penal_SRV_APPLIED_N4_cod_promo', 'Penal_SRV_PENDING_N3_desc_promo', 'Penal_SRV_FINISHED_N3_desc_promo',
         'Penal_SRV_APPLIED_N2_desc_promo', 'Cust_x_antiguedad_cuenta', 'Cust_TIPO_DOCUMENTO',
         'Penal_SRV_PENDING_N2_cod_promo', 'Penal_CUST_PENDING_N1_desc_promo', 'Penal_CUST_FINISHED_N5_desc_promo',
         'Penal_SRV_FINISHED_N5_cod_promo', 'Penal_CUST_APPLIED_N2_desc_promo', 'Cust_FLG_ROBINSON',
         'Penal_SRV_APPLIED_N3_desc_penal', 'Cust_COD_ESTADO_GENERAL', 'Penal_SRV_FINISHED_N5_cod_penal',
         'Penal_SRV_FINISHED_N4_cod_promo', 'Device_N2_device_type', 'Penal_SRV_APPLIED_N3_cod_promo',
         'Penal_SRV_FINISHED_N3_desc_penal', 'Penal_CUST_FINISHED_N5_cod_penal', 'Penal_CUST_PENDING_N1_cod_penal',
         'Penal_SRV_APPLIED_N3_cod_penal', 'Penal_CUST_APPLIED_N2_cod_penal', 'Penal_SRV_FINISHED_N4_cod_penal',
         'Order_h_N1_Status', 'Serv_L2_data_additional_proc', 'Penal_SRV_FINISHED_N2_cod_promo', 'Penal_CUST_FINISHED_N3_desc_promo',
          'Penal_SRV_PENDING_N2_cod_penal', 'Penal_SRV_FINISHED_N3_cod_promo',
         'Penal_CUST_PENDING_N1_desc_penal', 'Penal_CUST_APPLIED_N2_desc_penal', 'Cust_L2_SUPEROFERTA_proc',
         'Penal_CUST_FINISHED_N5_desc_penal', 'Cust_x_cesion_datos', 'Penal_SRV_FINISHED_N3_cod_penal',
         'Penal_SRV_APPLIED_N2_cod_promo', 'Cust_Agg_L2_first_srv_basic_name_nc', 'Penal_SRV_FINISHED_N1_desc_promo',
         'Penal_SRV_PENDING_N1_cod_promo', 'Device_N3_device_type', 'Penal_SRV_APPLIED_N1_desc_promo',
         'Penal_SRV_PENDING_N2_desc_penal', 'Order_h_N2_Status', 'Penal_CUST_FINISHED_N4_cod_promo', 'Cust_ENCUESTAS2',
         'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_APPLIED_N2_desc_penal', 'Penal_CUST_FINISHED_N1_cod_penal',
         'Cust_TRATAMIENTO', 'Penal_SRV_PENDING_N2_desc_promo', 'Penal_CUST_APPLIED_N1_cod_penal',
         'Cust_x_datos_navegacion', 'Cust_X_PUBLICIDAD_EMAIL', 'Cust_X_FORMATO_FACTURA', 'Penal_SRV_FINISHED_N2_cod_penal',
         'Order_h_N10_Status', 'Penal_SRV_APPLIED_N2_cod_penal', 'Penal_CUST_FINISHED_N4_cod_penal', 'Device_N4_device_type',
         'Serv_roam_zona_2', 'Penal_CUST_FINISHED_N2_desc_promo', 'Penal_CUST_APPLIED_N1_desc_penal',
         'Serv_L2_dto_lev1_proc', 'Penal_CUST_FINISHED_N4_desc_penal', 'Order_h_N2_Class',
          'Order_h_N4_Status', 'Cust_L2_codigo_postal_city',
         'Penal_SRV_FINISHED_N1_desc_penal', 'Cust_L2_X_FORMATO_FACTURA_proc', 'Serv_data_additional',
         'Penal_SRV_APPLIED_N1_desc_penal', 'Device_N4_OS', 'Penal_CUST_FINISHED_N3_cod_penal',
         'Penal_CUST_APPLIED_N1_cod_promo', 'Penal_SRV_PENDING_N1_cod_penal', 'Device_N1_OS', 'Order_h_N3_Status',
         'Device_N3_OS', 'Cust_SUPEROFERTA', 'Order_h_N6_Status', 'Penal_SRV_FINISHED_N1_cod_penal',
         'Penal_CUST_FINISHED_N3_desc_penal', 'Device_N2_OS', 'Serv_voice_tariff', 'Penal_SRV_APPLIED_N1_cod_penal',
         'Order_h_N1_Class', 'Penal_SRV_PENDING_N1_desc_penal', 'Device_N4_manufacturer', 'Order_h_N8_Status',
         'Penal_SRV_FINISHED_N1_cod_promo', 'Penal_CUST_FINISHED_N3_cod_promo', 'Penal_CUST_FINISHED_N2_cod_penal',
         'Order_h_N5_Status', 'Serv_ROAMING_BASIC', 'Penal_SRV_APPLIED_N1_cod_promo', 'Device_N3_OS_initial_version',
         'Penal_SRV_PENDING_N1_desc_promo', 'Device_N4_OS_initial_version', 'Penal_CUST_FINISHED_N1_desc_penal', 'Order_h_N9_Status',
         'Order_h_N4_Class', 'Penal_CUST_FINISHED_N2_desc_penal', 'Device_N1_OS_initial_version', 'Order_h_N7_Status',
         'Serv_L2_voice_tariff_proc', 'Device_N3_manufacturer', 'Device_N2_OS_initial_version', 'Serv_tipo_sim',
         'Order_h_N10_Class', 'Penal_CUST_FINISHED_N1_desc_promo', 'Order_h_N3_Class', 'Serv_L2_roam_zona_2_proc',
         'Penal_CUST_FINISHED_N2_cod_promo', 'Order_h_N5_Class', 'Serv_RGU', 'Order_h_N7_Class', 'Device_N1_manufacturer',
         'Device_N2_manufacturer', 'Order_h_N9_Class', 'Serv_L2_tipo_sim_proc', 'Order_h_N6_Class', 'Serv_L2_data_proc',
         'Device_N3_model', 'Order_h_N8_Class', 'Penal_CUST_FINISHED_N1_cod_promo', 'Serv_SRV_BASIC', 'Serv_DESC_SRV_BASIC',
         'Order_h_N2_Description', 'Serv_data', 'Cust_CODIGO_POSTAL', 'Order_h_N10_Description', 'Order_h_N4_Description',
         'Device_N2_model', 'Order_h_N5_Description', 'Order_h_N9_Description', 'Order_h_N6_Description',
         'Serv_L2_tariff_proc', 'Device_N1_model', 'Order_h_N8_Description', 'Order_h_N7_Description', 'Serv_DESC_TARIFF',
         'Serv_L2_desc_tariff_proc', 'Serv_tariff'],

            'Mobile_only'      :['Order_h_N1_Description', 'Cust_FLG_LORTAD', 'Serv_dto_lev3', 'Serv_TV_CUOTA_ALTA', 'Serv_TV_TARIFF',
         'Penal_CUST_APPLIED_N2_cod_promo', 'Cust_L2_x_user_facebook_has_user', 'Penal_SRV_FINISHED_N5_cod_promo',
         'Serv_DECO_TV', 'Cust_L2_x_user_twitter_has_user', 'Penal_CUST_PENDING_N3_cod_promo',
         'Penal_SRV_PENDING_N5_cod_promo', 'Serv_PVR_TV', 'Cust_Agg_seg_pospaid_nif', 'Serv_FBB_UPGRADE',
         'Penal_SRV_APPLIED_N5_cod_penal', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE', 'Serv_ROAM_USA_EUR',
         'Penal_CUST_PENDING_N3_desc_promo', 'Cust_x_tipo_cuenta_corp', 'Penal_SRV_FINISHED_N3_cod_promo',
         'Penal_CUST_APPLIED_N2_desc_promo', 'Serv_dto_lev2', 'Penal_CUST_PENDING_N2_cod_promo',
         'Cust_x_datos_navegacion', 'Device_N4_manufacturer', 'Penal_CUST_PENDING_N3_cod_penal',
         'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N4_cod_promo',
         'Penal_SRV_PENDING_N5_desc_promo', 'Penal_SRV_FINISHED_N5_cod_penal', 'Penal_CUST_APPLIED_N3_cod_promo',
         'Penal_CUST_PENDING_N4_desc_promo', 'Penal_SRV_APPLIED_N4_cod_promo',
         'Penal_CUST_PENDING_N4_cod_penal', 'Penal_CUST_APPLIED_N1_cod_promo', 'Penal_CUST_PENDING_N4_desc_penal',
         'Cust_X_IDIOMA_FACTURA', 'Penal_CUST_PENDING_N5_cod_promo', 'Penal_CUST_PENDING_N5_desc_promo',
         'Penal_CUST_PENDING_N5_cod_penal',  'Penal_CUST_PENDING_N5_desc_penal',
         'Penal_SRV_PENDING_N5_cod_penal', 'Penal_SRV_FINISHED_N5_desc_promo', 'Penal_CUST_APPLIED_N5_cod_penal',
         'Cust_COD_ESTADO_GENERAL', 'Penal_CUST_FINISHED_N3_cod_promo', 'Serv_NETFLIX_NAPSTER',
         'Penal_CUST_PENDING_N1_cod_promo', 'Penal_CUST_APPLIED_N5_desc_penal', 'Serv_L2_dto_lev2_proc',
         'Penal_SRV_FINISHED_N4_cod_promo', 'Penal_SRV_PENDING_N5_desc_penal', 'Penal_SRV_APPLIED_N5_desc_penal',
         'Penal_CUST_APPLIED_N3_desc_promo', 'Cust_NACIONALIDAD', 'Penal_SRV_FINISHED_N5_desc_penal',
         'Penal_SRV_PENDING_N4_cod_promo', 'Penal_CUST_APPLIED_N4_cod_promo',
         'Penal_SRV_APPLIED_N5_cod_promo', 'Penal_CUST_APPLIED_N4_desc_promo',
         'Penal_CUST_PENDING_N2_desc_promo', 'Penal_SRV_APPLIED_N3_cod_promo', 'Penal_CUST_APPLIED_N5_cod_promo',
         'Cust_FLG_ROBINSON', 'Penal_SRV_FINISHED_N4_desc_promo', 'Penal_CUST_APPLIED_N5_desc_promo',
         'Penal_SRV_APPLIED_N5_desc_promo', 'Penal_CUST_PENDING_N2_cod_penal', 'Penal_SRV_PENDING_N3_cod_promo',
         'Penal_SRV_FINISHED_N4_cod_penal', 'Penal_CUST_APPLIED_N2_cod_penal', 'Serv_HOMEZONE',
         'Penal_CUST_APPLIED_N3_cod_penal', 'Penal_CUST_PENDING_N2_desc_penal', 'Penal_SRV_FINISHED_N4_desc_penal',
         'Penal_CUST_APPLIED_N3_desc_penal', 'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_CUST_APPLIED_N4_desc_penal',
         'Penal_SRV_FINISHED_N3_desc_promo', 'Penal_SRV_PENDING_N4_desc_promo', 'Penal_CUST_APPLIED_N2_desc_penal',
         'Penal_SRV_APPLIED_N4_cod_penal', 'Serv_dto_lev1', 'Cust_x_datos_trafico', 'Cust_cta_correo_flag',
         'Penal_SRV_PENDING_N4_cod_penal', 'Penal_SRV_FINISHED_N2_cod_promo', 'Penal_SRV_APPLIED_N4_desc_penal',
         'Cust_x_antiguedad_cuenta', 'Penal_SRV_PENDING_N2_cod_promo', 'Penal_SRV_FINISHED_N3_cod_penal',
         'Penal_CUST_FINISHED_N2_cod_promo', 'Penal_SRV_APPLIED_N2_cod_promo', 'Penal_SRV_PENDING_N4_desc_penal',
         'Penal_CUST_PENDING_N1_desc_promo', 'Penal_SRV_APPLIED_N4_desc_promo', 'Cust_X_FORMATO_FACTURA',
         'Cust_PUBLICIDAD', 'Penal_SRV_FINISHED_N3_desc_penal', 'Cust_ENCUESTAS', 'Penal_CUST_FINISHED_N5_cod_penal',
         'Cust_Agg_seg_pospaid_nc', 'Penal_CUST_PENDING_N1_cod_penal', 'Penal_CUST_APPLIED_N1_desc_promo',
          'Serv_data_additional', 'Cust_TIPO_DOCUMENTO', 'Cust_L2_SUPEROFERTA_proc',
         'Penal_CUST_FINISHED_N5_desc_penal', 'Penal_SRV_PENDING_N3_desc_promo', 'Serv_tipo_sim',
         'Penal_SRV_APPLIED_N3_desc_promo', 'Penal_CUST_PENDING_N1_desc_penal', 'Penal_CUST_FINISHED_N4_cod_penal',
         'Cust_x_cesion_datos', 'Penal_SRV_FINISHED_N2_cod_penal', 'Penal_SRV_PENDING_N3_cod_penal',
         'Device_N1_device_type', 'Penal_CUST_APPLIED_N1_cod_penal', 'Cust_Agg_GRUPO_EXONO_PREOW_nc',
         'Penal_CUST_FINISHED_N5_cod_promo', 'Penal_SRV_APPLIED_N3_cod_penal', 'Cust_L2_X_IDIOMA_FACTURA_proc',
         'Cust_X_PUBLICIDAD_EMAIL', 'Cust_L2_codigo_postal_city', 'Penal_CUST_FINISHED_N4_desc_penal',
         'Penal_SRV_PENDING_N3_desc_penal', 'Cust_METODO_PAGO', 'Penal_SRV_APPLIED_N3_desc_penal',
         'Penal_SRV_FINISHED_N1_desc_penal', 'Penal_CUST_FINISHED_N5_desc_promo', 'Penal_CUST_APPLIED_N1_desc_penal',
         'Order_h_N10_Status', 'Serv_L2_tipo_sim_proc', 'Penal_CUST_FINISHED_N3_cod_penal', 'Cust_ENCUESTAS2',
         'Device_N2_device_type', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_CUST_FINISHED_N4_cod_promo',
         'Penal_CUST_FINISHED_N4_desc_promo', 'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Penal_SRV_PENDING_N1_cod_penal',
         'Penal_CUST_FINISHED_N3_desc_penal', 'Penal_SRV_APPLIED_N1_cod_penal',
         'Serv_ROAMING_BASIC', 'Serv_L2_dto_lev1_proc', 'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_CUST_FINISHED_N3_desc_promo',
         'Device_N3_manufacturer', 'Cust_TRATAMIENTO', 'Penal_SRV_PENDING_N2_desc_promo',
         'Penal_CUST_FINISHED_N2_cod_penal', 'Cust_Agg_L2_first_srv_basic_name_nc', 'Penal_SRV_APPLIED_N2_cod_penal',
         'Penal_CUST_FINISHED_N2_desc_penal', 'Penal_CUST_FINISHED_N1_desc_promo', 'Cust_FACTURA_ELECTRONICA',
         'Penal_CUST_FINISHED_N2_desc_promo', 'Serv_voice_tariff', 'Penal_SRV_PENDING_N2_cod_penal',
         'Penal_SRV_APPLIED_N2_desc_penal', 'Order_h_N1_Status', 'Penal_CUST_FINISHED_N1_cod_penal',
         'Device_N4_device_type', 'Penal_SRV_PENDING_N2_desc_penal', 'Serv_RGU', 'Penal_SRV_APPLIED_N2_desc_promo',
         'Penal_CUST_FINISHED_N1_desc_penal', 'Order_h_N8_Status',
         'Cust_L2_X_FORMATO_FACTURA_proc', 'Order_h_N2_Status', 'Device_N1_OS_initial_version',
         'Penal_CUST_FINISHED_N1_cod_promo', 'Device_N3_device_type', 'Penal_SRV_FINISHED_N1_cod_penal',
         'Serv_SRV_BASIC', 'Serv_L2_data_additional_proc', 'Penal_SRV_APPLIED_N1_desc_penal',
         'Penal_SRV_PENDING_N1_desc_penal', 'Device_N4_OS_initial_version', 'Order_h_N3_Status',
         'Cust_SUPEROFERTA', 'Serv_DESC_SRV_BASIC', 'Order_h_N10_Class', 'Serv_L2_roam_zona_2_proc',
         'Device_N2_OS', 'Device_N4_OS', 'Order_h_N5_Status', 'Penal_SRV_FINISHED_N1_desc_promo',
         'Penal_SRV_APPLIED_N1_cod_promo', 'Penal_SRV_PENDING_N1_cod_promo', 'Device_N3_OS_initial_version',
         'Order_h_N9_Status', 'Serv_L2_data_proc', 'Order_h_N1_Class', 'Cust_CODIGO_POSTAL', 'Device_N1_OS',
         'Order_h_N4_Status', 'Penal_SRV_FINISHED_N1_cod_promo', 'Device_N3_OS', 'Penal_SRV_APPLIED_N1_desc_promo',
         'Serv_roam_zona_2', 'Device_N2_OS_initial_version', 'Order_h_N2_Class', 'Penal_SRV_PENDING_N1_desc_promo',
         'Order_h_N7_Status', 'Order_h_N9_Class', 'Device_N4_model', 'Serv_L2_voice_tariff_proc', 'Order_h_N6_Status',
         'Device_N1_manufacturer', 'Order_h_N3_Class', 'Device_N2_manufacturer', 'Serv_data', 'Order_h_N8_Class',
         'Order_h_N4_Class', 'Order_h_N10_Description', 'Device_N3_model', 'Order_h_N6_Class', 'Order_h_N7_Class',
         'Order_h_N5_Class', 'Order_h_N9_Description', 'Device_N2_model', 'Serv_L2_tariff_proc', 'Order_h_N2_Description',
         'Order_h_N8_Description', 'Device_N1_model', 'Order_h_N3_Description', 'Order_h_N7_Description',
         'Order_h_N6_Description', 'Serv_DESC_TARIFF', 'Order_h_N4_Description', 'Order_h_N5_Description',
         'Serv_L2_desc_tariff_proc', 'Serv_tariff'],


            'others'           :['Cust_CODIGO_POSTAL', 'Cust_x_tipo_cuenta_corp', 'Cust_FLG_LORTAD', 'Cust_L2_x_user_facebook_has_user',
         'Cust_L2_x_user_twitter_has_user', 'Serv_RGU', 'Serv_voice_tariff', 'Serv_dto_lev1', 'Serv_dto_lev2',
         'Penal_CUST_FINISHED_N4_cod_promo', 'Serv_dto_lev3', 'Serv_tipo_sim', 'Serv_data_additional',
         'Serv_NETFLIX_NAPSTER', 'Order_h_N9_Description', 'Serv_ROAM_USA_EUR', 'Serv_roam_zona_2',
         'Serv_FBB_UPGRADE', 'Serv_tariff', 'Serv_DECO_TV', 'Serv_TV_CUOTA_ALTA',
         'Penal_CUST_FINISHED_N4_desc_promo', 'Cust_TIPO_DOCUMENTO', 'Serv_TV_TARIFF', 'Serv_DESC_TARIFF',
         'Cust_x_antiguedad_cuenta', 'Order_h_N3_Description', 'Serv_PVR_TV', 'Serv_L2_dto_lev1_proc',
         'Serv_L2_dto_lev2_proc', 'Serv_data', 'Serv_L2_data_additional_proc', 'Serv_L2_roam_zona_2_proc',
         'Serv_L2_voice_tariff_proc', 'Cust_Agg_seg_pospaid_nc', 'Cust_Agg_L2_first_srv_basic_name_nc',
         'Cust_Agg_seg_pospaid_nif', 'Penal_CUST_APPLIED_N2_cod_promo', 'Penal_CUST_APPLIED_N2_desc_promo',
         'Penal_CUST_APPLIED_N2_cod_penal', 'Penal_CUST_FINISHED_N4_cod_penal', 'Penal_CUST_APPLIED_N2_desc_penal'
         , 'Serv_HOMEZONE', 'Serv_ROAMING_BASIC', 'Penal_CUST_APPLIED_N3_cod_promo',
         'Penal_CUST_APPLIED_N3_desc_promo', 'Penal_CUST_APPLIED_N3_cod_penal', 'Penal_CUST_APPLIED_N3_desc_penal'
         , 'Penal_CUST_APPLIED_N1_cod_promo', 'Serv_L2_desc_tariff_proc', 'Penal_CUST_APPLIED_N4_cod_promo',
         'Order_h_N10_Description', 'Penal_CUST_FINISHED_N4_desc_penal', 'Penal_CUST_APPLIED_N4_desc_promo',
         'Penal_CUST_APPLIED_N4_cod_penal', 'Serv_L2_data_proc', 'Penal_CUST_APPLIED_N4_desc_penal',
         'Penal_CUST_APPLIED_N5_cod_promo', 'Penal_CUST_APPLIED_N5_desc_promo', 'Penal_CUST_APPLIED_N5_cod_penal',
         'Order_h_N4_Description', 'Penal_CUST_APPLIED_N5_desc_penal', 'Serv_L2_tipo_sim_proc',
         'Penal_CUST_FINISHED_N5_cod_promo', 'Penal_CUST_FINISHED_N5_desc_promo', 'Penal_CUST_FINISHED_N5_cod_penal',
         'Penal_CUST_FINISHED_N5_desc_penal', 'Penal_CUST_PENDING_N1_cod_promo', 'Serv_L2_tariff_proc',
         'Penal_CUST_PENDING_N1_desc_promo', 'Penal_CUST_PENDING_N1_cod_penal', 'Penal_CUST_PENDING_N1_desc_penal',
         'Penal_CUST_PENDING_N2_cod_promo', 'Order_h_N1_Description', 'Penal_CUST_PENDING_N2_desc_promo',
         'Penal_CUST_PENDING_N2_cod_penal', 'Penal_CUST_PENDING_N2_desc_penal', 'Penal_CUST_PENDING_N3_cod_promo',
         'Penal_CUST_PENDING_N3_desc_promo', 'Device_N1_manufacturer', 'Penal_CUST_PENDING_N3_cod_penal',
         'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N4_cod_promo', 'Penal_CUST_PENDING_N4_desc_promo',
         'Penal_CUST_PENDING_N4_cod_penal', 'Device_N1_model', 'Penal_CUST_PENDING_N4_desc_penal',
         'Penal_CUST_PENDING_N5_cod_promo', 'Penal_CUST_PENDING_N5_desc_promo', 'Penal_CUST_PENDING_N5_cod_penal',
         'Penal_CUST_PENDING_N5_desc_penal', 'Penal_SRV_APPLIED_N1_cod_promo', 'Device_N1_device_type',
         'Penal_SRV_APPLIED_N1_desc_promo', 'Penal_SRV_APPLIED_N1_cod_penal', 'Penal_SRV_APPLIED_N1_desc_penal',
         'Penal_SRV_APPLIED_N2_cod_promo', 'Penal_SRV_APPLIED_N2_desc_promo', 'Device_N1_OS',
          'Penal_SRV_APPLIED_N2_cod_penal', 'Penal_SRV_APPLIED_N2_desc_penal',
         'Order_h_N6_Description', 'Penal_SRV_APPLIED_N3_cod_promo', 'Device_N1_OS_initial_version',
         'Penal_SRV_APPLIED_N3_desc_promo', 'Penal_SRV_APPLIED_N3_cod_penal', 'Penal_SRV_APPLIED_N3_desc_penal',
         'Penal_SRV_APPLIED_N4_cod_promo', 'Penal_SRV_APPLIED_N4_desc_promo', 'Penal_SRV_APPLIED_N4_cod_penal',
         'Penal_SRV_APPLIED_N4_desc_penal', 'Penal_SRV_APPLIED_N5_cod_promo', 'Penal_SRV_APPLIED_N5_desc_promo',
         'Penal_SRV_APPLIED_N5_cod_penal', 'Penal_SRV_APPLIED_N5_desc_penal', 'Penal_SRV_FINISHED_N1_cod_promo',
         'Penal_SRV_FINISHED_N1_desc_promo', 'Penal_SRV_FINISHED_N1_cod_penal', 'Penal_SRV_FINISHED_N1_desc_penal',
         'Penal_SRV_FINISHED_N2_cod_promo', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_SRV_FINISHED_N2_cod_penal',
         'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_FINISHED_N3_cod_promo', 'Penal_SRV_FINISHED_N3_desc_promo',
         'Penal_SRV_FINISHED_N3_cod_penal', 'Penal_SRV_FINISHED_N3_desc_penal', 'Penal_SRV_FINISHED_N4_cod_promo',
         'Penal_SRV_FINISHED_N4_desc_promo', 'Order_h_N5_Description', 'Penal_SRV_FINISHED_N4_cod_penal',
         'Penal_SRV_FINISHED_N4_desc_penal', 'Penal_SRV_FINISHED_N5_cod_promo', 'Penal_SRV_FINISHED_N5_desc_promo',
         'Penal_SRV_FINISHED_N5_cod_penal', 'Penal_SRV_FINISHED_N5_desc_penal', 'Penal_SRV_PENDING_N1_cod_promo',
         'Penal_SRV_PENDING_N1_desc_promo', 'Penal_SRV_PENDING_N1_cod_penal', 'Penal_SRV_PENDING_N1_desc_penal',
         'Penal_SRV_PENDING_N2_cod_promo', 'Penal_SRV_PENDING_N2_desc_promo', 'Penal_CUST_APPLIED_N1_desc_promo',
         'Penal_SRV_PENDING_N2_cod_penal', 'Penal_SRV_PENDING_N2_desc_penal', 'Penal_SRV_PENDING_N3_cod_promo',
         'Penal_SRV_PENDING_N3_desc_promo', 'Penal_SRV_PENDING_N3_cod_penal', 'Penal_CUST_FINISHED_N1_desc_promo',
         'Penal_SRV_PENDING_N3_desc_penal', 'Penal_SRV_PENDING_N4_cod_promo', 'Penal_SRV_PENDING_N4_desc_promo',
         'Penal_SRV_PENDING_N4_cod_penal', 'Order_h_N8_Description', 'Penal_SRV_PENDING_N4_desc_penal',
         'Penal_SRV_PENDING_N5_cod_promo', 'Penal_SRV_PENDING_N5_desc_promo', 'Penal_SRV_PENDING_N5_cod_penal',
         'Penal_SRV_PENDING_N5_desc_penal', 'Device_N2_manufacturer', 'Device_N2_model', 'Cust_NACIONALIDAD',
         'Device_N2_device_type', 'Device_N2_OS', 'Device_N2_OS_initial_version', 'Device_N3_manufacturer',
         'Device_N3_model', 'Device_N3_device_type', 'Device_N3_OS', 'Device_N3_OS_initial_version',
         'Device_N4_manufacturer', 'Device_N4_model', 'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc',
         'Device_N4_device_type', 'Device_N4_OS', 'Device_N4_OS_initial_version', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE',
         'Cust_COD_ESTADO_GENERAL', 'Penal_CUST_FINISHED_N2_desc_promo', 'Order_h_N7_Description',
         'Serv_SRV_BASIC', 'Penal_CUST_APPLIED_N1_cod_penal', 'Cust_X_IDIOMA_FACTURA', 'Penal_CUST_FINISHED_N3_cod_promo',
         'Cust_METODO_PAGO', 'Penal_CUST_APPLIED_N1_desc_penal', 'Serv_DESC_SRV_BASIC', 'Cust_x_datos_navegacion',
         'Cust_Agg_GRUPO_EXONO_PREOW_nc',  'Cust_L2_X_IDIOMA_FACTURA_proc',
         'Penal_CUST_FINISHED_N3_desc_promo', 'Cust_x_datos_trafico', 'Cust_X_FORMATO_FACTURA', 'Cust_cta_correo_flag',
         'Penal_CUST_FINISHED_N3_cod_penal', 'Cust_x_cesion_datos', 'Cust_FLG_ROBINSON',
         'Penal_CUST_FINISHED_N3_desc_penal', 'Order_h_N2_Description', 'Cust_PUBLICIDAD',
         'Penal_CUST_FINISHED_N2_cod_promo', 'Cust_ENCUESTAS', 'Cust_L2_SUPEROFERTA_proc', 'Cust_TRATAMIENTO',
         'Penal_CUST_FINISHED_N2_cod_penal', 'Cust_X_PUBLICIDAD_EMAIL',
         'Penal_CUST_FINISHED_N2_desc_penal', 'Cust_L2_X_FORMATO_FACTURA_proc', 'Cust_ENCUESTAS2', 'Order_h_N10_Class',
          'Penal_CUST_FINISHED_N1_cod_promo', 'Order_h_N8_Status', 'Cust_FACTURA_ELECTRONICA',
         'Order_h_N10_Status', 'Order_h_N1_Status', 'Order_h_N9_Class',
         'Penal_CUST_FINISHED_N1_cod_penal', 'Order_h_N9_Status', 'Order_h_N8_Class', 'Cust_L2_codigo_postal_city',
         'Order_h_N5_Status', 'Penal_CUST_FINISHED_N1_desc_penal', 'Cust_SUPEROFERTA', 'Order_h_N7_Status',
         'Order_h_N6_Status', 'Order_h_N2_Status', 'Order_h_N7_Class', 'Order_h_N3_Status', 'Order_h_N4_Status', 'Order_h_N6_Class',
         'Order_h_N5_Class', 'Order_h_N4_Class', 'Order_h_N1_Class', 'Order_h_N3_Class', 'Order_h_N2_Class'],


            'Pure_prepaid'     :['Order_h_N1_Description', 'Cust_CLASE_CLI_COD_CLASE_CLIENTE', 'Cust_x_tipo_cuenta_corp',
         'Cust_FLG_LORTAD', 'Penal_SRV_FINISHED_N1_cod_promo', 'Serv_tariff', 'Serv_DESC_TARIFF',
         'Serv_voice_tariff',  'Penal_CUST_FINISHED_N3_cod_promo', 'Serv_data',
         'Serv_dto_lev1', 'Cust_L2_x_user_twitter_has_user', 'Serv_dto_lev2', 'Serv_dto_lev3',
          'Serv_data_additional', 'Penal_SRV_FINISHED_N1_desc_promo',
         'Serv_NETFLIX_NAPSTER', 'Serv_ROAMING_BASIC', 'Serv_ROAM_USA_EUR', 'Device_N1_manufacturer',
         'Serv_roam_zona_2', 'Serv_HOMEZONE', 'Serv_FBB_UPGRADE', 'Serv_DECO_TV', 'Serv_TV_CUOTA_ALTA',
         'Serv_TV_TARIFF', 'Serv_PVR_TV', 'Penal_CUST_FINISHED_N3_desc_promo', 'Cust_COD_ESTADO_GENERAL',
         'Serv_L2_desc_tariff_proc', 'Serv_L2_data_proc', 'Penal_CUST_APPLIED_N1_cod_penal',
         'Serv_L2_dto_lev1_proc', 'Serv_L2_dto_lev2_proc', 'Serv_L2_data_additional_proc',
         'Penal_SRV_PENDING_N1_cod_promo', 'Serv_L2_roam_zona_2_proc', 'Serv_L2_tariff_proc',
         'Serv_L2_voice_tariff_proc', 'Cust_Agg_seg_pospaid_nif', 'Penal_CUST_APPLIED_N3_cod_promo',
         'Penal_CUST_APPLIED_N3_desc_promo', 'Penal_CUST_APPLIED_N3_cod_penal',
         'Penal_CUST_FINISHED_N3_cod_penal', 'Penal_CUST_APPLIED_N3_desc_penal',
         'Penal_CUST_APPLIED_N4_cod_promo', 'Penal_CUST_APPLIED_N4_desc_promo',

         'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_CUST_APPLIED_N4_desc_penal',
         'Penal_CUST_APPLIED_N5_cod_promo', 'Penal_CUST_APPLIED_N5_desc_promo',
         'Order_h_N10_Description', 'Penal_CUST_APPLIED_N5_cod_penal', 'Penal_CUST_APPLIED_N5_desc_penal',
         'Penal_CUST_FINISHED_N4_cod_promo', 'Penal_CUST_FINISHED_N4_desc_promo',
         'Penal_CUST_FINISHED_N4_cod_penal', 'Penal_CUST_FINISHED_N4_desc_penal',
         'Penal_CUST_FINISHED_N5_cod_promo', 'Penal_CUST_FINISHED_N3_desc_penal',
         'Penal_CUST_FINISHED_N5_desc_promo', 'Penal_CUST_FINISHED_N5_cod_penal',
         'Device_N4_manufacturer', 'Penal_CUST_FINISHED_N5_desc_penal', 'Penal_CUST_PENDING_N1_cod_promo',
         'Penal_CUST_PENDING_N1_desc_promo', 'Penal_CUST_PENDING_N1_cod_penal',
         'Penal_CUST_PENDING_N1_desc_penal', 'Penal_CUST_PENDING_N2_cod_promo',
         'Penal_CUST_PENDING_N2_desc_promo', 'Penal_CUST_PENDING_N2_cod_penal',
         'Penal_CUST_PENDING_N2_desc_penal', 'Penal_CUST_PENDING_N3_cod_promo',
         'Penal_CUST_PENDING_N3_desc_promo', 'Penal_CUST_PENDING_N3_cod_penal',
         'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_PENDING_N4_cod_promo',
         'Penal_CUST_PENDING_N4_desc_promo', 'Penal_CUST_PENDING_N4_cod_penal',
         'Penal_CUST_PENDING_N4_desc_penal', 'Penal_CUST_PENDING_N5_cod_promo', 'Penal_CUST_PENDING_N5_desc_promo',
         'Penal_CUST_PENDING_N5_cod_penal', 'Penal_CUST_PENDING_N5_desc_penal', 'Penal_SRV_APPLIED_N1_cod_promo',
         'Cust_X_PUBLICIDAD_EMAIL', 'Penal_SRV_APPLIED_N1_desc_promo', 'Penal_SRV_APPLIED_N1_cod_penal',
         'Penal_SRV_APPLIED_N1_desc_penal', 'Penal_SRV_PENDING_N1_desc_promo',
         'Penal_SRV_APPLIED_N2_cod_promo', 'Penal_SRV_APPLIED_N2_desc_promo', 'Penal_SRV_APPLIED_N2_cod_penal',
         'Penal_SRV_APPLIED_N2_desc_penal', 'Penal_SRV_APPLIED_N3_cod_promo', 'Penal_CUST_FINISHED_N1_cod_promo',
         'Penal_SRV_APPLIED_N3_desc_promo', 'Penal_SRV_APPLIED_N3_cod_penal', 'Penal_SRV_APPLIED_N3_desc_penal',
         'Penal_SRV_APPLIED_N4_cod_promo', 'Penal_SRV_APPLIED_N4_desc_promo', 'Penal_SRV_APPLIED_N4_cod_penal',
         'Penal_SRV_APPLIED_N4_desc_penal', 'Penal_SRV_APPLIED_N5_cod_promo', 'Penal_SRV_APPLIED_N5_desc_promo',
         'Penal_SRV_APPLIED_N5_cod_penal', 'Penal_SRV_APPLIED_N5_desc_penal', 'Penal_CUST_APPLIED_N1_desc_penal',
         'Penal_SRV_FINISHED_N2_cod_promo', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_SRV_FINISHED_N2_cod_penal',
         'Cust_L2_x_user_facebook_has_user', 'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_FINISHED_N3_cod_promo',
         'Penal_SRV_FINISHED_N3_desc_promo', 'Penal_SRV_FINISHED_N3_cod_penal', 'Penal_SRV_FINISHED_N3_desc_penal',
         'Penal_SRV_FINISHED_N4_cod_promo', 'Penal_SRV_FINISHED_N4_desc_promo', 'Penal_SRV_FINISHED_N4_cod_penal',
         'Penal_SRV_FINISHED_N4_desc_penal', 'Penal_SRV_FINISHED_N5_cod_promo', 'Cust_FLG_ROBINSON',
         'Penal_SRV_FINISHED_N5_desc_promo', 'Penal_SRV_FINISHED_N5_cod_penal', 'Penal_SRV_FINISHED_N5_desc_penal',
         'Penal_SRV_PENDING_N2_cod_promo', 'Penal_SRV_PENDING_N2_desc_promo', 'Penal_SRV_PENDING_N2_cod_penal',
         'Penal_SRV_PENDING_N2_desc_penal', 'Penal_SRV_PENDING_N3_cod_promo', 'Penal_SRV_PENDING_N3_desc_promo',
         'Penal_SRV_PENDING_N3_cod_penal', 'Penal_SRV_PENDING_N3_desc_penal', 'Penal_SRV_PENDING_N4_cod_promo',
         'Penal_SRV_PENDING_N4_desc_promo', 'Penal_SRV_PENDING_N1_cod_penal', 'Penal_SRV_PENDING_N4_cod_penal',
         'Penal_SRV_PENDING_N4_desc_penal', 'Penal_SRV_PENDING_N5_cod_promo', 'Penal_SRV_PENDING_N5_desc_promo',
         'Penal_SRV_PENDING_N5_cod_penal', 'Penal_SRV_PENDING_N5_desc_penal', 'Device_N2_manufacturer',
         'Penal_SRV_FINISHED_N1_cod_penal', 'Cust_X_IDIOMA_FACTURA', 'Penal_CUST_APPLIED_N2_cod_promo',
         'Penal_SRV_FINISHED_N1_desc_penal', 'Penal_CUST_APPLIED_N2_desc_promo', 'Penal_SRV_PENDING_N1_desc_penal',
         'Penal_CUST_APPLIED_N2_cod_penal', 'Cust_Agg_GRUPO_EXONO_PREOW_nc', 'Penal_CUST_APPLIED_N2_desc_penal',
         'Cust_x_datos_trafico', 'Penal_CUST_APPLIED_N1_cod_promo', 'Cust_Agg_seg_pospaid_nc',
         'Cust_L2_X_IDIOMA_FACTURA_proc', 'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Penal_CUST_APPLIED_N1_desc_promo',
         'Penal_CUST_FINISHED_N2_desc_promo', 'Cust_x_cesion_datos', 'Penal_CUST_FINISHED_N2_cod_promo',
         'Cust_x_datos_navegacion', 'Cust_ENCUESTAS', 'Cust_NACIONALIDAD', 'Penal_CUST_FINISHED_N2_cod_penal',
         'Cust_cta_correo_flag', 'Penal_CUST_FINISHED_N2_desc_penal', 'Serv_SRV_BASIC',
         'Penal_CUST_FINISHED_N1_desc_promo', 'Cust_ENCUESTAS2', 'Cust_PUBLICIDAD', 'Cust_FACTURA_ELECTRONICA',
         'Serv_DESC_SRV_BASIC', 'Cust_x_antiguedad_cuenta', 'Cust_Agg_L2_first_srv_basic_name_nc', 'Serv_RGU',
         'Penal_CUST_FINISHED_N1_cod_penal', 'Order_h_N7_Description', 'Penal_CUST_FINISHED_N1_desc_penal',
         'Serv_tipo_sim', 'Cust_TIPO_DOCUMENTO', 'Cust_L2_SUPEROFERTA_proc',
          'Order_h_N9_Description', 'Cust_L2_codigo_postal_city', 'Cust_METODO_PAGO',
         'Order_h_N10_Status', 'Serv_L2_tipo_sim_proc', 'Device_N3_device_type',
         'Order_h_N8_Status', 'Device_N1_device_type', 'Order_h_N10_Class',
         'Order_h_N4_Status', 'Cust_SUPEROFERTA', 'Order_h_N9_Status', 'Order_h_N1_Status', 'Device_N3_OS_initial_version', 'Cust_TRATAMIENTO',
         'Order_h_N6_Status', 'Device_N2_device_type', 'Device_N4_device_type', 'Cust_X_FORMATO_FACTURA', 'Order_h_N9_Class', 'Order_h_N2_Status',
         'Order_h_N7_Status', 'Device_N1_OS_initial_version', 'Order_h_N8_Class', 'Cust_L2_X_FORMATO_FACTURA_proc',
         'Order_h_N5_Status', 'Device_N4_OS_initial_version', 'Order_h_N3_Status', 'Order_h_N8_Description',
         'Device_N2_OS_initial_version', 'Order_h_N7_Class', 'Device_N3_OS', 'Order_h_N6_Class', 'Device_N1_OS',
         'Device_N4_OS', 'Order_h_N5_Class', 'Order_h_N1_Class', 'Device_N2_OS', 'Order_h_N6_Description',
         'Order_h_N4_Class', 'Device_N3_manufacturer', 'Order_h_N3_Class', 'Cust_CODIGO_POSTAL', 'Order_h_N2_Class',
         'Order_h_N5_Description', 'Device_N4_model', 'Order_h_N4_Description', 'Order_h_N3_Description',
         'Device_N3_model', 'Order_h_N2_Description', 'Device_N1_model', 'Device_N2_model'],


            'Standalone_FBB'   :['Order_h_N2_Description', 'Cust_FLG_LORTAD', 'Serv_tariff', 'Serv_DESC_TARIFF', 'Serv_voice_tariff',
         'Serv_data', 'Serv_dto_lev1', 'Serv_dto_lev2', 'Serv_dto_lev3', 'Penal_CUST_FINISHED_N4_cod_promo',
         'Device_N4_model', 'Cust_L2_x_user_twitter_has_user', 'Penal_SRV_APPLIED_N1_cod_promo',
         'Penal_SRV_FINISHED_N1_cod_promo', 'Penal_CUST_APPLIED_N2_cod_promo', 'Device_N1_model',
         'Penal_SRV_PENDING_N1_cod_promo', 'Serv_data_additional', 'Serv_NETFLIX_NAPSTER',
         'Serv_ROAMING_BASIC', 'Serv_ROAM_USA_EUR', 'Serv_roam_zona_2', 'Serv_HOMEZONE',
         'Serv_FBB_UPGRADE', 'Serv_DECO_TV', 'Serv_TV_CUOTA_ALTA', 'Order_h_N10_Description',
         'Serv_TV_TARIFF', 'Serv_PVR_TV', 'Serv_L2_desc_tariff_proc', 'Serv_L2_data_proc',
         'Serv_L2_dto_lev1_proc', 'Penal_CUST_FINISHED_N5_cod_promo', 'Serv_L2_dto_lev2_proc',
         'Serv_L2_data_additional_proc', 'Serv_L2_roam_zona_2_proc', 'Device_N2_model',
         'Penal_SRV_APPLIED_N1_desc_promo', 'Cust_X_IDIOMA_FACTURA', 'Penal_CUST_FINISHED_N4_desc_promo',
         'Serv_L2_tariff_proc', 'Serv_L2_voice_tariff_proc', 'Cust_Agg_seg_pospaid_nif',
         'Penal_CUST_APPLIED_N3_cod_promo', 'Penal_CUST_APPLIED_N3_desc_promo', 'Penal_CUST_APPLIED_N3_cod_penal',
         'Penal_CUST_APPLIED_N3_desc_penal', 'Penal_CUST_APPLIED_N4_cod_promo', 'Penal_CUST_APPLIED_N4_desc_promo',
         'Penal_CUST_APPLIED_N4_cod_penal', 'Penal_CUST_APPLIED_N4_desc_penal', 'Penal_CUST_APPLIED_N5_cod_promo',
         'Penal_CUST_APPLIED_N5_desc_promo', 'Penal_CUST_APPLIED_N5_cod_penal', 'Penal_CUST_APPLIED_N5_desc_penal',
         'Penal_SRV_PENDING_N1_desc_promo', 'Penal_CUST_PENDING_N2_cod_promo', 'Order_h_N7_Description',
         'Penal_CUST_PENDING_N3_cod_promo', 'Penal_CUST_PENDING_N3_desc_promo', 'Penal_CUST_PENDING_N3_cod_penal',

         'Penal_SRV_APPLIED_N1_cod_penal', 'Penal_CUST_PENDING_N3_desc_penal', 'Penal_CUST_FINISHED_N4_cod_penal',
         'Penal_CUST_PENDING_N4_cod_promo', 'Device_N3_model', 'Penal_CUST_PENDING_N4_desc_promo',
         'Penal_CUST_PENDING_N4_cod_penal', 'Penal_CUST_PENDING_N4_desc_penal', 'Penal_CUST_PENDING_N5_cod_promo',
         'Penal_CUST_PENDING_N5_desc_promo', 'Penal_CUST_PENDING_N5_cod_penal', 'Penal_CUST_PENDING_N5_desc_penal',
         'Penal_SRV_APPLIED_N2_cod_promo', 'Penal_SRV_APPLIED_N2_desc_promo', 'Penal_CUST_APPLIED_N2_desc_promo',
         'Penal_SRV_FINISHED_N1_desc_promo', 'Penal_SRV_APPLIED_N2_cod_penal', 'Penal_SRV_APPLIED_N2_desc_penal',
         'Penal_SRV_APPLIED_N3_cod_promo', 'Penal_SRV_APPLIED_N3_desc_promo', 'Penal_SRV_APPLIED_N3_cod_penal',
         'Penal_SRV_APPLIED_N3_desc_penal', 'Penal_CUST_FINISHED_N4_desc_penal', 'Penal_SRV_APPLIED_N4_cod_promo',
         'Penal_SRV_APPLIED_N1_desc_penal', 'Order_h_N4_Description', 'Penal_SRV_APPLIED_N4_desc_promo',

         'Penal_SRV_APPLIED_N4_cod_penal', 'Penal_SRV_APPLIED_N4_desc_penal', 'Penal_SRV_APPLIED_N5_cod_promo',
         'Penal_SRV_APPLIED_N5_desc_promo', 'Penal_SRV_APPLIED_N5_cod_penal', 'Penal_SRV_APPLIED_N5_desc_penal',
         'Penal_SRV_FINISHED_N2_cod_promo', 'Penal_SRV_FINISHED_N2_desc_promo', 'Penal_SRV_FINISHED_N2_cod_penal',
         'Penal_SRV_FINISHED_N2_desc_penal', 'Penal_SRV_FINISHED_N3_cod_promo', 'Penal_SRV_FINISHED_N3_desc_promo',
         'Penal_SRV_FINISHED_N3_cod_penal', 'Penal_SRV_FINISHED_N3_desc_penal', 'Penal_SRV_FINISHED_N4_cod_promo',
         'Penal_SRV_FINISHED_N4_desc_promo', 'Penal_SRV_PENDING_N1_cod_penal', 'Penal_SRV_FINISHED_N4_cod_penal',
         'Penal_SRV_FINISHED_N4_desc_penal', 'Penal_SRV_FINISHED_N5_cod_promo', 'Penal_SRV_FINISHED_N5_desc_promo',
         'Penal_SRV_FINISHED_N1_cod_penal', 'Penal_CUST_PENDING_N2_desc_promo', 'Order_h_N9_Description',
         'Penal_SRV_FINISHED_N5_cod_penal', 'Penal_SRV_FINISHED_N5_desc_penal', 'Penal_SRV_PENDING_N3_cod_promo',
         'Penal_SRV_PENDING_N3_desc_promo', 'Penal_SRV_PENDING_N3_cod_penal', 'Penal_SRV_PENDING_N3_desc_penal',
         'Penal_SRV_PENDING_N4_cod_promo', 'Penal_SRV_PENDING_N4_desc_promo', 'Penal_SRV_PENDING_N4_cod_penal',
         'Penal_SRV_PENDING_N4_desc_penal', 'Penal_SRV_PENDING_N5_cod_promo', 'Penal_SRV_PENDING_N5_desc_promo',
         'Penal_SRV_PENDING_N5_cod_penal', 'Penal_SRV_PENDING_N5_desc_penal', 'Penal_CUST_FINISHED_N5_desc_promo',
         'Penal_SRV_PENDING_N1_desc_penal', 'Order_h_N1_Description', 'Penal_SRV_FINISHED_N1_desc_penal',
         'Penal_CUST_APPLIED_N2_cod_penal', 'Penal_SRV_PENDING_N2_cod_promo', 'Penal_CUST_FINISHED_N5_cod_penal',
         'Penal_SRV_PENDING_N2_desc_promo', 'Penal_CUST_PENDING_N2_cod_penal', 'Penal_CUST_APPLIED_N2_desc_penal',
         'Penal_SRV_PENDING_N2_cod_penal', 'Cust_x_tipo_cuenta_corp', 'Penal_CUST_FINISHED_N5_desc_penal',
         'Penal_SRV_PENDING_N2_desc_penal', 'Penal_CUST_PENDING_N2_desc_penal', 'Cust_L2_x_user_facebook_has_user',
         'Cust_CLASE_CLI_COD_CLASE_CLIENTE', 'Cust_L2_X_IDIOMA_FACTURA_proc', 'Penal_CUST_APPLIED_N1_cod_promo',
         'Cust_x_datos_trafico', 'Penal_CUST_FINISHED_N3_cod_promo',
         'Cust_x_datos_navegacion', 'Cust_ENCUESTAS', 'Penal_CUST_APPLIED_N1_desc_promo', 'Penal_CUST_PENDING_N1_cod_penal',
         'Penal_CUST_FINISHED_N3_desc_promo', 'Cust_TIPO_DOCUMENTO', 'Cust_x_cesion_datos',
         'Cust_Agg_SUPEROFERTA_EXONO_PREOW_nc', 'Cust_cta_correo_flag', 'Cust_X_FORMATO_FACTURA',
         'Penal_CUST_PENDING_N1_desc_penal', 'Penal_CUST_FINISHED_N3_cod_penal', 'Device_N1_OS_initial_version',
         'Cust_FLG_ROBINSON','Penal_CUST_APPLIED_N1_cod_penal',
         'Cust_x_antiguedad_cuenta', 'Cust_L2_SUPEROFERTA_proc', 'Penal_CUST_FINISHED_N3_desc_penal',
         'Cust_NACIONALIDAD', 'Cust_COD_ESTADO_GENERAL', 'Cust_ENCUESTAS2', 'Cust_Agg_GRUPO_EXONO_PREOW_nc',
         'Penal_CUST_PENDING_N1_cod_promo', 'Device_N4_OS_initial_version', 'Penal_CUST_APPLIED_N1_desc_penal',
         'Penal_CUST_FINISHED_N2_cod_penal', 'Cust_TRATAMIENTO',
         'Device_N4_device_type', 'Penal_CUST_PENDING_N1_desc_promo', 'Cust_Agg_seg_pospaid_nc',
         'Cust_X_PUBLICIDAD_EMAIL', 'Cust_FACTURA_ELECTRONICA', 'Order_h_N8_Description', 'Device_N4_OS',
         'Order_h_N1_Status', 'Penal_CUST_FINISHED_N2_desc_penal',  'Device_N3_OS_initial_version',
         'Cust_METODO_PAGO', 'Cust_PUBLICIDAD', 'Cust_L2_X_FORMATO_FACTURA_proc', 'Penal_CUST_FINISHED_N2_desc_promo',
         'Device_N3_device_type', 'Penal_CUST_FINISHED_N1_desc_promo',
         'Device_N1_device_type', 'Device_N4_manufacturer', 'Cust_SUPEROFERTA',
         'Penal_CUST_FINISHED_N2_cod_promo', 'Device_N2_OS',  'Device_N3_OS',
         'Order_h_N2_Status', 'Device_N2_device_type', 'Penal_CUST_FINISHED_N1_cod_penal', 'Order_h_N10_Status',
         'Device_N3_manufacturer', 'Order_h_N3_Status', 'Device_N2_manufacturer', 'Device_N1_manufacturer',
         'Penal_CUST_FINISHED_N1_desc_penal', 'Serv_tipo_sim', 'Order_h_N9_Status', 'Cust_L2_codigo_postal_city',
         'Order_h_N5_Status', 'Serv_L2_tipo_sim_proc', 'Device_N1_OS', 'Order_h_N8_Status', 'Device_N2_OS_initial_version',
         'Penal_CUST_FINISHED_N1_cod_promo', 'Order_h_N4_Status', 'Serv_RGU', 'Order_h_N6_Status', 'Cust_Agg_L2_first_srv_basic_name_nc',
         'Order_h_N7_Status', 'Serv_SRV_BASIC', 'Order_h_N1_Class', 'Order_h_N10_Class', 'Serv_DESC_SRV_BASIC', 'Order_h_N9_Class', 'Order_h_N3_Class',
         'Order_h_N8_Class', 'Order_h_N2_Class', 'Order_h_N5_Class', 'Order_h_N7_Class', 'Order_h_N6_Class', 'Order_h_N4_Class',
         'Order_h_N6_Description', 'Cust_CODIGO_POSTAL', 'Order_h_N5_Description', 'Order_h_N3_Description']
        }

        segments = segment_counts_dict.keys()
        #segments = ['Convergent', 'Mobile_only', 'Pure_prepaid', 'Standalone_FBB', 'others']
        #segments = ['Convergent']
        for segment in segments:
            segment_count = sum(segment_counts_dict[segment].values())
            segment_rate = float(segment_count)/count_all
            print '\nSEGMENT:', segment, 'count='+str(segment_count), 'rate='+str(segment_rate), '\n'

            ids_segment = self.ids.where('Cust_Agg_seg_pospaid_nif == "%s"' % segment)

            ex_ono_pre_ow_segments = [None]
            if segment == 'Convergent':
                ex_ono_pre_ow_segments = segment_counts_dict[segment].keys()
                #ex_ono_pre_ow_segments = ['OW', 'EX_ONO', 'PRE_OW']
                #ex_ono_pre_ow_segments = ['OW']

            for exo_pow_segment in ex_ono_pre_ow_segments:
                output_segment_label = segment

                if exo_pow_segment is None:
                    ids_segment_subsegment = ids_segment
                    #ids_segment_subsegment.filter(ids_segment_subsegment['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif').count().sort(['Cust_Agg_seg_pospaid_nif']).show()
                else:
                    #print 'SEGMENT:', segment, 'count='+str(segment_count), 'rate='+str(segment_rate), '\n'
                    # Redefine segment_cout as Grupo_ExOno_PreOW subsegment count
                    segment_count = segment_counts_dict[segment][exo_pow_segment]
                    segment_rate = float(segment_count)/count_all
                    print 'SEGMENT:', segment, 'GRUPO_EXONO_PREOW:', exo_pow_segment, 'count='+str(segment_count), 'rate='+str(segment_rate), '\n'

                    #if exo_pow_segment == 'OW':
                    #    output_segment_label = output_segment_label + '-' + 'Others'
                    #else:
                    output_segment_label = output_segment_label + '-' + exo_pow_segment

                    ids_segment_subsegment = ids_segment.where('Cust_Agg_GRUPO_EXONO_PREOW_nc == "%s"' % exo_pow_segment)
                    #ids_segment_subsegment.filter(ids_segment_subsegment['TNPS_min_VDN'].isNotNull()).groupby('Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc').count().sort(['Cust_Agg_seg_pospaid_nif', 'Cust_Agg_GRUPO_EXONO_PREOW_nc']).show()

                # output_segment_label = segment
                # if exo_pow_segment is not None:
                #     if exo_pow_segment == 'OW':
                #         output_segment_label = output_segment_label + '-' + 'Others'
                #     else:
                #         output_segment_label = output_segment_label + '-' + exo_pow_segment

                ids_segment_subsegment = ids_segment_subsegment.cache()

                proportions_dict = CustomerExperience.calculate_proportions(ids_segment_subsegment)

                if sum(proportions_dict.values()) > 0:
                    # Ignore all TNPS_* columns but label
                    ignore_cols = [x for x in ids_segment_subsegment.columns if x.startswith('TNPS_') and x != 'TNPS_min_VDN_TNPS2DET_i']

                    # Ignore categorical columns to speed up training
                    #ignore_cols = ignore_cols + [item[0] for item in ids_segment_subsegment.dtypes if (item[1] == 'string')]

                    modeller = GeneralModelTrainer(ids_segment_subsegment, 'TNPS_min_VDN_TNPS2DET_i', GeneralModelTrainer.ids_ignore_cols + ignore_cols)

                    # Take top most relevant categorical variables
                    modeller.categorical_columns = best_cat_vars[output_segment_label][:20]

                    #cols_to_relevel = None
                    #modeller.relevelCategoricalVars(cols=cols_to_relevel)
                    modeller.relevelCategoricalVars(maxBins=32)
                    #print modeller.df_orig.explain(extended=True)
                    modeller.generateFeaturesVector(ohe_columns=True, do_checkpoint=False)
                    #print modeller.df.explain(extended=True)
                    modeller.save(filename='tnps_'+output_segment_label+'-'+str(self.model_month)+'.bin',
                                  df_path='/data/udf/vf_es/tnps_data/all_data_featured-'+output_segment_label+'-'+str(self.model_month),
                                  model_path=None,
                                  preds_path=None)


                    modeller.split_train_test(seed=1234)
                    #modeller.standardScale(withMean=False, withStd=True)
                    modeller.minMaxScale()
                    modeller.train(seed=1234)
                    print modeller.datasource_importance()

                    modeller.evaluate_train()
                    tpreds = modeller.evaluation_predictions_train
                    if self.evaluation_predictions_train is None:
                        self.evaluation_predictions_train = tpreds
                    else:
                        self.evaluation_predictions_train = self.evaluation_predictions_train.union(tpreds)

                    modeller.evaluate_balanced_test()
                    bpreds = modeller.evaluation_predictions_balanced_test
                    if self.evaluation_predictions_balanced_test is None:
                        self.evaluation_predictions_balanced_test = bpreds
                    else:
                        self.evaluation_predictions_balanced_test = self.evaluation_predictions_balanced_test.union(bpreds)

                    modeller.evaluate_unbalanced_test()
                    upreds = modeller.evaluation_predictions_unbalanced_test
                    if self.evaluation_predictions_unbalanced_test is None:
                        self.evaluation_predictions_unbalanced_test = upreds
                    else:
                        self.evaluation_predictions_unbalanced_test = self.evaluation_predictions_unbalanced_test.union(upreds)

                    modeller.save(filename='tnps_'+output_segment_label+'-'+str(self.model_month)+'.bin',
                                  df_path=None,
                                  model_path='/data/udf/vf_es/tnps_data/model_'+output_segment_label+'-'+str(self.model_month),
                                  preds_path=None)

                    model_params = ModelParameters(self.spark,
                                                   'tnps_'+output_segment_label,
                                                   partition_year, partition_month, 0, time=0,
                                                   model_level='msisdn',
                                                   training_closing_date=str(self.model_month),
                                                   target='Minimum of all TNPS of every client, of clients surveyed in current month')
                    model_params.extract_from_gmt(modeller)
                    model_params.insert(use_common_path=False, overwrite=True)
                    #model_params.insert(use_common_path=True)

                    # Scale relative_importance according to segment_rate
                    cur_varimp = modeller.varimp.copy()
                    cur_varimp.drop(columns='cum_sum', inplace=True)
                    cur_varimp.set_index('feature', inplace=True)
                    cur_varimp['relative_importance'] = cur_varimp['relative_importance'].apply(lambda x: x*segment_rate)
                    if self.varimp is None:
                        self.varimp = cur_varimp
                    else:
                        self.varimp = self.varimp.reindex_like(cur_varimp).fillna(0) + cur_varimp.fillna(0).fillna(0)

                    ##############
                    # Prediction #
                    ##############

                    modeller.predict()

                    modeller.save(filename='tnps_'+output_segment_label+'-'+str(self.model_month)+'.bin',
                                  df_path=None,
                                  model_path=None,
                                  preds_path='/data/udf/vf_es/tnps_data/predictions_'+output_segment_label+'-'+str(self.model_month)
                                 )


                    preds = modeller.predictions
                    preds = CustomerExperience.apply_cutpoints_predictions(preds, proportions_dict)
                    if self.debug:
                        preds.groupby('predict_TNPS01').count().sort('predict_TNPS01').show()

                    joined = ids_segment_subsegment.select('msisdn', 'TNPS_min_VDN').join(preds, 'msisdn', 'left_outer')
                    #if self.debug:
                    #    joined.groupby('predict_TNPS01').count().sort('predict_TNPS01').show()
                    joined = joined.withColumn('final_TNPS01', when(joined['TNPS_min_VDN'].isNotNull(), joined['TNPS_min_VDN']).otherwise(joined['predict_TNPS01']))
                    joined = joined.withColumn('score', when(joined['TNPS_min_VDN'].isNotNull(), lit(None)).otherwise(joined['score']))

                    joined = CustomerExperience.add_tnpsx_cols(joined, 'final_TNPS01')
                    if self.debug:
                        joined.groupby('final_TNPS01').count().sort('final_TNPS01').show()

                    # Use the just created final_TNPS01 column as 'prediction'
                    joined = joined.withColumn('prediction', joined['final_TNPS01'])

                    #joined.withColumn('tnps_is_null', when(joined['TNPS_min_VDN'].isNotNull(), lit('TNPS')).otherwise(joined['TNPS_min_VDN'])).groupby('TNPS_is_null').count().show()
                    #joined.withColumn('score_is_null', when(joined['score'].isNotNull(), lit('score')).otherwise(joined['score'])).groupby('score_is_null').count().show()

                    joined = joined.withColumn('Segment', lit(output_segment_label))
                    joined.printSchema()

                    model_scores = ModelScores(self.spark,
                                               'tnps_'+output_segment_label,
                                               partition_year, partition_month, 0, time=0,
                                               predict_closing_date=str(self.model_month))

                    model_scores.extract_from_gmt(modeller, do_extract_df=False)
                    model_scores.extract_from_df(joined)
                    model_scores.insert(use_common_path=False, overwrite=True)
                    model_scores.insert(use_common_path=True)

                    final_columns = ['msisdn',
                                     'NUM_CLIENTE',
                                     'NIF_CLIENTE',
                                     'rawPrediction',
                                     'probability',
                                     'prediction',
                                     'score',
                                     'rowNum',
                                     'predict_TNPS01',
                                     'Segment']
                    final_columns = filter(lambda x: x in joined.columns, final_columns)
                    if self.predictions is None:
                        self.predictions = joined.select(final_columns)
                    else:
                        self.predictions = self.predictions.select(final_columns).union(joined.select(final_columns))
                else:
                    print 'WARNING: Segment', output_segment_label, 'has', ids_segment_subsegment.count(), 'elements. So, skipping'

        print ''
        print '/----------------\\'
        print '| Ensemble model |'
        print '\\----------------/'
        print ''

        # Compute overall variable importance
        self.varimp.sort_values('relative_importance', ascending=False, inplace=True)
        self.varimp.reset_index(inplace=True)
        self.varimp['cum_sum'] = self.varimp['relative_importance'].cumsum()

        # Generate a general GeneralModelTrainer instance
        self.modeller = modeller # Take last GMT instance
        self.modeller.trainingData = self.evaluation_predictions_train
        self.modeller.testData = self.evaluation_predictions_balanced_test
        self.modeller.testDataUnbalanced = self.evaluation_predictions_unbalanced_test
        self.modeller.varimp = self.varimp
        self.modeller.predictions = self.predictions

        # Compute overall metrics
        self.modeller.evaluate()

        # Save the general GeneralModelTrainer instance
        self.modeller.save(filename='tnps-'+str(self.model_month)+'.bin',
                           df_path=None,
                           model_path=None,
                           preds_path='/data/udf/vf_es/tnps_data/predictions-'+str(self.model_month))


        # Save general ModelParameters
        self.model_params = ModelParameters(self.spark,
                                            'tnps',
                                            partition_year, partition_month, 0, time=0,
                                            model_level='msisdn',
                                            training_closing_date=str(self.model_month),
                                            target='Minimum of all TNPS of every client, of clients surveyed in current month')
        self.model_params.extract_from_gmt(self.modeller)
        self.model_params.insert(use_common_path=False, overwrite=True)
        self.model_params.insert(use_common_path=True)

        # Save general ModelScores
        self.model_scores = ModelScores(self.spark,
                                        'tnps',
                                        partition_year, partition_month, 0, time=0,
                                        predict_closing_date=str(self.model_month))
        self.model_scores.extract_from_gmt(self.modeller, do_extract_df=False)
        self.model_scores.extract_from_df(self.predictions)
        self.model_scores.insert(use_common_path=False, overwrite=True)
        self.model_scores.insert(use_common_path=True)

        # Save
        # ofile = '/user/bbergua/tnps_predictions-' + str(self.model_month)# + '-' + segment
        # #if self.oracle:
        # #    ofile = '/user/bbergua/tnps_predictions_oracle-' + str(self.model_month)

        # print '['+time.ctime()+']', 'Saving predictions in parquet to', 'hdfs://'+ofile+'/parquet', '...'
        # self.predictions.repartition(1)\
        #                 .write.save(ofile+'/parquet'
        #                            , format='parquet'
        #                            , mode='overwrite'
        #                            #, partitionBy=['partitioned_month', 'year', 'month', 'day']
        #                            )
        # print '['+time.ctime()+']', 'Saving predictions in parquet to', 'hdfs://'+ofile+'/parquet', '... done'
        # # Read with: df = spark.read.load('/user/bbergua/convergence_predictions-' + self.model_month)

        # print '['+time.ctime()+']', 'Saving predictions in csv     to', 'hdfs://'+ofile+'/csv', '...'
        #                 #.orderBy('score', ascending=False).drop('score')\
        # self.predictions.select('msisdn', 'predict_TNPS01').where(col('msisdn').isNotNull()).coalesce(1)\
        #                 .write.save(ofile+'/csv_ms'
        #                             , format='csv'
        #                             , header=True
        #                             , sep='|'
        #                             , mode='overwrite'
        #                             #, partitionBy=['partitioned_month', 'year', 'month', 'day']
        #                             )

        # self.predictions.select('NUM_CLIENTE', 'predict_TNPS01').where(col('NUM_CLIENTE').isNotNull()).coalesce(1)\
        #                 .groupby('NUM_CLIENTE').agg(sql_min('predict_TNPS01').alias('predict_TNPS01'))\
        #                 .write.save(ofile+'/csv_nc'
        #                             , format='csv'
        #                             , header=True
        #                             , sep='|'
        #                             , mode='overwrite'
        #                             #, partitionBy=['partitioned_month', 'year', 'month', 'day']
        #                             )

        # self.predictions.select('NIF_CLIENTE', 'predict_TNPS01').where(col('NIF_CLIENTE').isNotNull()).coalesce(1)\
        #                 .groupby('NIF_CLIENTE').agg(sql_min('predict_TNPS01').alias('predict_TNPS01'))\
        #                 .write.save(ofile+'/csv_id'
        #                             , format='csv'
        #                             , header=True
        #                             , sep='|'
        #                             , mode='overwrite'
        #                             #, partitionBy=['partitioned_month', 'year', 'month', 'day']
        #                             )
        # print '['+time.ctime()+']', 'Saving predictions in csv     to', 'hdfs://'+ofile+'/csv', '... done'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='VF_ES CBU Customer Experience TNPS Model',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    #parser.add_argument('stack', metavar='<stack>', type=str, help='Stack (Oracle/Amdocs)')
    parser.add_argument('-model_month','--model_month', metavar='<model-month>', type=int, help='Date (YYYYMM) to train the model')
    parser.add_argument('-o', '--oracle', action='store_true', help='Use Oracle data')
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages')
    args = parser.parse_args()
    print 'args =', args
    print 'model_month =', args.model_month
    print 'oracle =', args.oracle
    print 'debug =', args.debug

    #conf = Configuration()

    print '['+time.ctime()+']', 'Loading SparkContext ...'
    sc, sparkSession, sqlContext = run_sc()
    spark = (SparkSession.builder
                         .appName("VF_ES CBU CEX TNPS")
                         .master("yarn")
                         .config("spark.submit.deployMode", "client")
                         .config("spark.ui.showConsoleProgress", "true")
                         .enableHiveSupport()
                         .getOrCreate()
            )
    print '['+time.ctime()+']', 'Loading SparkContext ... done'

    cex = CustomerExperience(spark, args.model_month, args.oracle, args.debug)
    cex.run()

    print '['+time.ctime()+']', 'Stopping SparkContext'
    spark.stop()

    print '['+time.ctime()+']', 'Process finished!'
