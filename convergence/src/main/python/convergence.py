#!/usr/bin/python
# -*- coding: utf-8 -*-

# PYSPARK_DRIVER_PYTHON=ipython ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/pyspark --packages com.databricks:spark-csv_2.10:1.5.0
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=8G ~/fy17.capsule/convergence/src/main/python/convergence.py 201803 201804 2>&1 | tee salida.convergence
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py --oracle 201708 201709 2>&1 | tee salida.convergence
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 16G --driver-memory 8G --conf spark.executor.cores=5 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py --only-model 20180831 20181031 > salida.convergence.0810
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 64G --driver-memory 64G --conf spark.executor.cores=4 --conf spark.driver.maxResultSize=64G --conf spark.yarn.executor.memoryOverhead=32G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py 20181031 20181231 | tee salida.convergence.1012.nueva
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 32G --driver-memory 16G --conf spark.executor.cores=4 --conf spark.driver.maxResultSize=16G --conf spark.yarn.executor.memoryOverhead=16G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf spark.shuffle.manager=tungsten-sort --conf spark.shuffle.service.enabled=true --conf spark.executor.cores=4 --conf spark.dynamicAllocation.maxExecutors=20 /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py 20181231 20190228 | tee -a salida.convergence.1202


from configuration import Configuration
from DP_prepare_input_acc_par_propensos_cross_nif import DPPrepareInputAccParPropensosCrossNif
from DP_prepare_input_amdocs_ids import DPPrepareInputAmdocsIds
from DP_prepare_input_cvm_pospago import DPPrepareInputCvmPospago
from general_model_trainer import GeneralModelTrainer
from model_outputs import ModelParameters, ModelScores

import argparse
import subprocess
import time
from pyspark.sql.functions import lit
# from pyspark.sql.utils import IllegalArgumentException

class Convergence:

    def __init__(self, app, model_month, prediction_month, oracle=False, debug=False):
        #import general_model_trainer
        #print general_model_trainer.__file__
        #import os
        #print os.path.abspath(general_model_trainer.__file__)
        self.app = app
        self.model_month = model_month
        self.prediction_month = prediction_month
        self.oracle = oracle
        self.debug = debug
        self.converged = None
        self.not_converged = None
        self.mobile_only = None
        self.acc_output_train = None
        self.acc_output_pred = None
        self.train_cols = None
        self.modeller = None
        self.predictions = None
        self.model_params = None
        self.model_scores = None
        self.id_cols = [
            "Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_CODIGO_POSTAL", "Cust_CODIGO_POSTAL",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE",
            "Serv_NUM_SERIE_DECO_TV", "Order_h_N1_Description", "Order_h_N2_Description",
            "Order_h_N5_Description", "Order_h_N7_Description", "Order_h_N8_Description", "Order_h_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "Order_h_N1_Id", "Order_h_N2_Id", "Order_h_N3_Id", "Order_h_N4_Id", "Order_h_N5_Id", "Order_h_N6_Id",
            "Order_h_N7_Id", "Order_h_N8_Id", "Order_h_N9_Id", "Order_h_N10_Id"
            ]


    #def __del__(self):
    #    print 'Stopping SparkContext'
    #    self.app.spark.stop()

    def load_local(self):
        ifile = "/data/udf/vf_es/convergence_data/converged-" + self.model_month
        self.converged = self.app.spark.read.load(ifile,
                                                       format='com.databricks.spark.csv', header='true',
                                                       charset='latin1')  # , inferSchema='true')
        # converged.cache()
        c = self.converged.count()
        print 'Converged (', self.model_month, ') count =', c

        ifile = "/data/udf/vf_es/convergence_data/not_converged-" + self.model_month
        self.not_converged = self.app.spark.read.load(ifile,
                                                           format='com.databricks.spark.csv', header='true',
                                                           charset='latin1')  # , inferSchema='true')
        # not_converged.cache()
        c = self.not_converged.count()
        print 'Not converged (', self.model_month, ') count =', c

        ifile = "/data/udf/vf_es/convergence_data/mobile_only-" + self.prediction_month
        self.mobile_only = self.app.spark.read.load(ifile,
                                                         format='com.databricks.spark.csv', header='true',
                                                         charset='latin1')  # , inferSchema='true')
        # mobile_only.cache()
        c = self.mobile_only.count()
        print 'Mobile only (', self.prediction_month, ') count =', c

    def load_bdp_cvm_oracle(self):
        ifile_convd = '/tmp/bbergua/convergence_data/oracle_converged-' + str(self.model_month)
        ifile_not_convd = '/tmp/bbergua/convergence_data/oracle_not_converged-' + str(self.model_month)
        ifile_mo = '/tmp/bbergua/convergence_data/oracle_mobile_only-' + str(self.prediction_month)

        fs = self.app.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.app.spark._jsc.hadoopConfiguration())
        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_convd+'/_SUCCESS')) and
                fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_not_convd+'/_SUCCESS')) and
                fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_mo+'/_SUCCESS'))):
            obj = DPPrepareInputCvmPospago(self.app, self.model_month)
            obj.calculate_mobile_only_and_convergent_by_id()

            obj2 = DPPrepareInputCvmPospago(self.app, self.prediction_month)
            obj2.calculate_mobile_only_and_convergent_by_id()
            self.mobile_only = obj2.get_mobile_only()
            ofile = '/tmp/bbergua/convergence_data/oracle_mobile_only-' + str(self.prediction_month)
            print '['+time.ctime()+']', 'Saving  ', ofile, '...'
            self.mobile_only.repartition(1).write.save(ofile
                                          , format='parquet'
                                          , mode='overwrite'
                                          #, partitionBy=['partitioned_month', 'year', 'month', 'day']
                                          )

            self.converged, self.not_converged = obj.get_converged_and_not_converged(obj2)
            ofile = '/tmp/bbergua/convergence_data/oracle_converged-' + str(self.model_month) #+ '_' + self.prediction_month
            print '['+time.ctime()+']', 'Saving  ', ofile, '...'
            self.converged.repartition(1).write.save(ofile
                                            , format='parquet'
                                            , mode='overwrite'
                                            # , partitionBy=['partitioned_month', 'year', 'month', 'day']
                                            )

            ofile = '/tmp/bbergua/convergence_data/oracle_not_converged-' + str(self.model_month) #+ '_' + self.prediction_month
            print '['+time.ctime()+']', 'Saving  ', ofile, '...'
            self.not_converged.repartition(1).write.save(ofile
                                                , format='parquet'
                                                , mode='overwrite'
                                                # , partitionBy=['partitioned_month', 'year', 'month', 'day']
                                                )
        else:
            print '['+time.ctime()+']', 'Loading Oracle converged', self.model_month, ifile_convd
            self.converged = self.app.spark.read.format('parquet').load(ifile_convd)
            self.converged.cache()
            c = self.converged.count()
            print '['+time.ctime()+']', 'Oracle converged     (', self.model_month, ') count =', c
            if c == 0:
                raise ValueError('Oracle converged has 0 rows')

            print '['+time.ctime()+']', 'Loading Oracle not converged', self.model_month, ifile_not_convd
            self.not_converged = self.app.spark.read.format('parquet').load(ifile_not_convd)
            self.not_converged.cache()
            c = self.not_converged.count()
            print '['+time.ctime()+']', 'Oracle not converged (', self.model_month, ') count =', c
            if c == 0:
                raise ValueError('Oracle not converged has 0 rows')

            print '['+time.ctime()+']', 'Loading Oracle mobile only', self.prediction_month, ifile_mo
            self.mobile_only = self.app.spark.read.format('parquet').load(ifile_mo)
            self.mobile_only.cache()
            c = self.mobile_only.count()
            print '['+time.ctime()+']', 'Oracle mobile only   (', self.prediction_month, ') count =', c
            if c == 0:
                raise ValueError('Oracle mobile only has 0 rows')

    def load_bdp_acc_par_propensos_cross_nif(self):
        try:
            #print '['+time.ctime()+']', 'Loading Accenture\'s Par Propensos Cross Nif', self.model_month
            self.acc_output_train = DPPrepareInputAccParPropensosCrossNif(self.app, self.model_month).to_df()\
                .drop('partitioned_month').drop('year').drop('month').drop('day')
            self.acc_output_train.cache()
            c = self.acc_output_train.count()
            print '['+time.ctime()+']', 'Accenture\'s Par Propensos Cross Nif count =', c

            #print '['+time.ctime()+']', 'Loading Accenture\'s Par Propensos Cross Nif', self.prediction_month
            self.acc_output_pred = DPPrepareInputAccParPropensosCrossNif(self.app, self.prediction_month).to_df()\
                .drop('partitioned_month').drop('year').drop('month').drop('day')
            self.acc_output_pred.cache()
            c = self.acc_output_pred.count()
            print '['+time.ctime()+']', 'Accenture\'s Par Propensos Cross Nif count =', c
        except Exception as e:
            print '['+time.ctime()+']', 'ERROR Loading Accenture\'s Par Propensos Cross Nif:', e
            self.acc_output_train = None
            self.acc_output_pred = None

    def load_bdp_amdocs_ids(self):
        ifile_convd = '/data/udf/vf_es/convergence_data/converged-' + str(self.model_month) + '-' + str(self.prediction_month)
        ifile_not_convd = '/data/udf/vf_es/convergence_data/not_converged-' + str(self.model_month) + '-' + str(self.prediction_month)
        ifile_mo = '/data/udf/vf_es/convergence_data/mobile_only-' + str(self.prediction_month)

        fs = self.app.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.app.spark._jsc.hadoopConfiguration())
        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_convd+'/_SUCCESS')) and
                fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_not_convd+'/_SUCCESS')) and
                fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_mo+'/_SUCCESS'))):

            obj = DPPrepareInputAmdocsIds(self.app, self.model_month, False)
            #obj.load_and_join()
            obj.load()
            obj.calculate_mobile_only_and_convergent_by_id()

            obj2 = DPPrepareInputAmdocsIds(self.app, self.prediction_month, False)
            #obj2.load_and_join()
            obj2.load()
            obj2.calculate_mobile_only_and_convergent_by_id()

        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_mo+'/_SUCCESS'))):
            self.mobile_only = obj2.get_mobile_only()

            ofile = '/data/udf/vf_es/convergence_data/mobile_only-'+str(self.prediction_month)
            print '['+time.ctime()+']', 'Saving mobile_only', self.prediction_month, 'hdfs://'+ofile, '...'
            # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/mobile_only/partitioned_month='+str(self.prediction_month), shell=True)
            # self.mobile_only.write.save('/tmp/bbergua/convergence_data/mobile_only', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
            self.mobile_only.repartition(10).write.save(ofile, format='parquet', mode='overwrite')
            print '['+time.ctime()+']', 'Saving mobile_only', self.prediction_month, 'hdfs://'+ofile, '... done'
        else:
            #ifile = '/tmp/bbergua/convergence_data/mobile_only'
            #ifile = '/tmp/bbergua/convergence_data/mobile_only-'+str(self.prediction_month)
            print '['+time.ctime()+']', 'Loading mobile only', self.prediction_month, 'hdfs://'+ifile_mo
            self.mobile_only = self.app.spark.read.format('parquet').load(ifile_mo)#.filter('partitioned_month = %s' % self.prediction_month)
            self.mobile_only.cache()
            c = self.mobile_only.count()
            print '['+time.ctime()+']', 'Mobile only   (', self.prediction_month, ') count =', c

        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_convd+'/_SUCCESS')) and
                fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_not_convd+'/_SUCCESS'))):
            self.converged, self.not_converged = obj.get_converged_and_not_converged(obj2)

        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_convd+'/_SUCCESS'))):
            ofile = '/data/udf/vf_es/convergence_data/converged-'+str(self.model_month)+'-'+str(self.prediction_month)
            print '['+time.ctime()+']', 'Saving converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ofile, '...'
            # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/converged/partitioned_month='+str(self.model_month), shell=True)
            # self.converged.write.save('/tmp/bbergua/convergence_data/converged', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
            self.converged.repartition(1).write.save(ofile, format='parquet', mode='overwrite')
            print '['+time.ctime()+']', 'Saving converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ofile, '... done'
        else:
            #ifile = '/tmp/bbergua/convergence_data/converged'
            #ifile = '/tmp/bbergua/convergence_data/converged-'+str(self.model_month)
            print '['+time.ctime()+']', 'Loading converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ifile_convd
            self.converged = self.app.spark.read.format('parquet').load(ifile_convd)#.filter('partitioned_month = %s' % self.model_month)
            self.converged.cache()
            c = self.converged.count()
            print '['+time.ctime()+']', 'Converged     (', str(self.model_month)+'-'+str(self.prediction_month), ') count =', c

        if not (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(ifile_not_convd+'/_SUCCESS'))):
            ofile = '/data/udf/vf_es/convergence_data/not_converged-'+str(self.model_month)+'-'+str(self.prediction_month)
            print '['+time.ctime()+']', 'Saving not_converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ofile, '...'
            # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/not_converged/partitioned_month='+str(self.model_month), shell=True)
            # self.not_converged.write.save('/tmp/bbergua/convergence_data/not_converged', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
            self.not_converged.repartition(8).write.save(ofile, format='parquet', mode='overwrite')
            print '['+time.ctime()+']', 'Saving not_converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ofile, '... done'
        else:
            #ifile = '/tmp/bbergua/convergence_data/not_converged'
            #ifile = '/tmp/bbergua/convergence_data/not_converged-'+str(self.model_month)
            print '['+time.ctime()+']', 'Loading not converged', str(self.model_month)+'-'+str(self.prediction_month), 'hdfs://'+ifile_not_convd
            self.not_converged = self.app.spark.read.format('parquet').load(ifile_not_convd)#.filter('partitioned_month = %s' % self.model_month)
            self.not_converged.cache()
            c = self.not_converged.count()
            print '['+time.ctime()+']', 'Not converged (', str(self.model_month)+'-'+str(self.prediction_month), ') count =', c

    def load(self):
        if self.app.is_bdp:
            if self.oracle:
                self.load_bdp_cvm_oracle()
                self.load_bdp_acc_par_propensos_cross_nif()
            else:
                self.load_bdp_amdocs_ids()
        else:
            self.load_local()

    def generate_train_predict_data(self, model_month, prediction_month):
        self.load()

        ##############
        # Train data #
        ##############

        # Add 'CLASS' and auxiliary 'PRED' columns
        converged     =     self.converged.withColumn('class', lit(True) ).withColumn('pred', lit(False))
        not_converged = self.not_converged.withColumn('class', lit(False)).withColumn('pred', lit(False))
        train_data = converged.union(not_converged.select(converged.columns))

        # Join with Accenture Output
        # if self.acc_output_train is not None:
        #     acc_output_train_count = self.acc_output_train.count()
        #     # print '['+time.ctime()+']', 'Accenture\'s Par Propensos Cross Nif (', model_month, ') count =', acc_output_train_count

        #     if acc_output_train_count > 0:
        #         train_data = train_data.join(self.acc_output_train, train_data.nif == self.acc_output_train.nif, 'left_outer')\
        #             .drop('nif')

        ################
        # Predict data #
        ################

        # Add 'CLASS' and auxiliary 'PRED' columns
        predict_data = self.mobile_only.withColumn('class', lit(False)).withColumn('pred', lit(True))

        # Join with Accenture Output
        if self.oracle:
            if (self.acc_output_train is not None) and (self.acc_output_pred is not None):
                acc_output_train_count = self.acc_output_train.count()
                # print '['+time.ctime()+']', 'Accenture\'s Par Propensos Cross Nif (', model_month, ') count =', acc_output_train_count
                acc_output_pred_count = self.acc_output_pred.count()
                # print '['+time.ctime()+']', 'Accenture\'s Par Propensos Cross Nif (', prediction_month, ') count =', acc_output_pred_count

                if (acc_output_train_count > 0) and (acc_output_pred_count > 0):
                    train_data   = train_data.join(self.acc_output_train, 'nif', 'left_outer')
                    predict_data = predict_data.join(self.acc_output_pred, 'nif', 'left_outer')
                else:
                    print 'WARNING: Discarding Accenture\'s Par Propensos Cross Nif data' \
                          + ' because there are not rows for both train and prediction'
            else:
                print 'WARNING: Discarding Accenture\'s Par Propensos Cross Nif data' \
                      + ' because it was not possible to load it'

        ############
        # All data #
        ############

        # Drop columns that are not present in both datasets
        diff1 = list(set(train_data.columns) - set(predict_data.columns))
        print len(diff1), 'columns in train_data that are not present in predict_data:', diff1
        diff2 = list(set(predict_data.columns) - set(train_data.columns))
        print len(diff1), 'columns in predict_data that are not present in train_data:', diff2
        diff_cols = diff1 + diff2
        print len(diff_cols), 'columns are not present in both datasets (train and predict), removing:', diff_cols
        for c in diff_cols:
            train_data   =   train_data.drop(c)
            predict_data = predict_data.drop(c)

        common_cols = list(set(train_data.columns + predict_data.columns))
        print len(common_cols), 'columns present in both datasets (train and predict):', common_cols

        # train_data   =   train_data.repartition(200) # FIXME
        # predict_data = predict_data.repartition(200) # FIXME

        train_data   =   train_data.cache()
        predict_data = predict_data.cache()

        print 'Train data:'
        train_data.groupby(['partitioned_month', 'year', 'month', 'day']).count().show()
        print 'Predict data:'
        predict_data.groupby(['partitioned_month', 'year', 'month', 'day']).count().show()


        #self.app.spark.stop()
        #import sys
        #sys.exit(-1)

        # ofile = '/tmp/bbergua/convergence_data/train_data'
        # if self.oracle:
        #     ofile = ofile + '_oracle'
        # print '['+time.ctime()+']', 'Saving train data in parquet'
        # #subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/train_data/parquet/partitioned_month='+str(model_month), shell=True)
        # #train_data.write.save('/tmp/bbergua/convergence_data/train_data/parquet', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
        # train_data.repartition(8).write.save(ofile+'/parquet/'+str(model_month), format='parquet', mode='overwrite')
        # print '['+time.ctime()+']', 'Saving train data in csv'
        # train_data.coalesce(1).write.save(ofile+'/csv/'+str(model_month), format='csv', header=True, sep='\t', mode='overwrite')

        # ofile = '/tmp/bbergua/convergence_data/predict_data'
        # if self.oracle:
        #     ofile = ofile + '_oracle'
        # print '['+time.ctime()+']', 'Saving predict data in parquet'
        # #subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/predict_data/parquet/partitioned_month='+str(prediction_month), shell=True)
        # #predict_data.write.save('/tmp/bbergua/convergence_data/predict_data/parquet', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
        # predict_data.repartition(10).write.save(ofile+'/parquet/'+str(prediction_month), format='parquet', mode='overwrite')
        # print '['+time.ctime()+']', 'Saving predict data in csv'
        # predict_data.coalesce(1).write.save(ofile+'/csv/'+str(prediction_month), format='csv', header=True, sep='\t', mode='overwrite')

        #self.app.spark.stop()
        #import sys
        #sys.exit(-1)

        all_data = train_data.union(predict_data.select(train_data.columns))#.drop('partitioned_month', 'year', 'month', 'day')
        if self.oracle:
            all_data = all_data.coalesce(1)
        else:
            all_data = all_data.repartition(20)
        all_data.cache()
        #all_data.groupBy(['class', 'pred']).count().orderBy('class').show()

        ofile = '/data/udf/vf_es/convergence_data/'
        if self.oracle:
            ofile = ofile + 'oracle_'
        ofile = ofile + 'all_data-' + str(self.model_month)+'-'+str(self.prediction_month)

        parquet_ofile = ofile + '/parquet/'
        print '['+time.ctime()+']', 'Saving all (train & predict) data in parquet in', parquet_ofile, '...'
        #subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/all_data/parquet/partitioned_month='+str(model_month), shell=True)
        #all_data.write.save('/tmp/bbergua/convergence_data/all_data/parquet', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
        all_data.write.save(parquet_ofile, format='parquet', mode='overwrite')
        print '['+time.ctime()+']', 'Saving all (train & predict) data in parquet in', parquet_ofile, '... done'

        # csv_ofile = ofile+'/csv/'
        # print '['+time.ctime()+']', 'Saving all (train & predict) data in csv     in', csv_ofile
        # all_data.coalesce(1).write.save(csv_ofile, format='csv', header=True, sep='\t', mode='overwrite')

        #self.app.spark.stop()
        #import sys
        #sys.exit(-1)

        return all_data

    def select_model_features(self, all_data=None):
        if all_data is None:
            ifile = '/data/udf/vf_es/convergence_data/'
            if self.oracle:
                ifile = ifile + 'oracle_'
            ifile = ifile + 'all_data-' + str(self.model_month)+'-'+str(self.prediction_month)
            parquet_ifile = ifile + '/parquet/'

            fs = self.app.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.app.spark._jsc.hadoopConfiguration())
            if (fs.exists(self.app.spark._jvm.org.apache.hadoop.fs.Path(parquet_ifile+'_SUCCESS'))):
                print '['+time.ctime()+']', 'Reading all (train & predict) data in parquet', parquet_ifile, '...'
                # all_data = self.app.spark.read.parquet(ifile+'/parquet').filter('partitioned_month = %s' % self.model_month)
                all_data = self.app.spark.read.parquet(parquet_ifile)
                print '['+time.ctime()+']', 'Reading all (train & predict) data in parquet', parquet_ifile, '...', len(all_data.columns), 'columns'
                #all_data.groupBy(['class', 'pred']).count().orderBy('class').show()
                #print all_data.columns
                #all_data = all_data.select(list(set(all_data.columns[:50]+['nif', 'class', 'pred']))).sample(False, 0.01, 1234).coalesce(1) # FIXME
                #print all_data.columns
            else:
                all_data = self.generate_train_predict_data(self.model_month, self.prediction_month)


        # Final set of columns
        if self.train_cols is None:
            self.train_cols = list(set(all_data.columns) - set(['NIF_CLIENTE']))

        if self.debug:
            print 'train_cols =', self.train_cols

        #num_cols = 500
        #self.train_cols = ['PROVINCIA', 'FECHA_MIGRACION', 'Bill_N5_Bill_Amount', 'TARIFF', 'DESC_TARIFF', 'Bill_N2_Tax_Amount', 'FX_ROAM_ZONA_2', 'Bill_N3_Bill_Amount', 'birth_date', 'Bill_N4_Tax_Amount', 'Bill_N1_Tax_Amount', 'Bill_N3_Tax_Amount', 'Bill_N5_Tax_Amount', 'central_office', 'Bill_N5_Bill_Date', 'FX_ROAMING_BASIC', 'Bill_N4_Bill_Amount', 'Bill_N1_Bill_Amount', 'MOBILE_BAM_TOTAL_CHARGES', 'Bill_N2_Bill_Amount', 'Camp_NIFs_Delight_SMS_Target_0', 'SRV_BASIC', 'FX_TARIFF', 'DESC_SRV_BASIC', 'PRICE_TARIFF', 'FACTURA_ELECTRONICA', 'Bill_N4_Bill_Date', 'CIUDAD', 'Bill_N3_Bill_Date', 'marriage2hgbst_elm', 'PUBLICIDAD', 'CODIGO_POSTAL', 'FX_VOICE_TARIFF', 'tv_services', 'tv_fx_first', 'FX_SRV_BASIC', 'Camp_SRV_Up_Cross_Sell_MMS_Target_0', 'movil_fx_first', 'Camp_SRV_Delight_SMS_Target_0', 'ROAM_ZONA_2', 'DATA', 'FX_DATA', 'Camp_SRV_Retention_Voice_SAT_Target_0', 'Camp_SRV_Up_Cross_Sell_SMS_Target_0', 'TIPO_SIM', 'Camp_SRV_Up_Cross_Sell_MLT_Universal_0', 'Camp_SRV_Retention_Voice_NOT_Target_0', 'TRATAMIENTO', 'VOICE_TARIFF', 'Camp_SRV_Up_Cross_Sell_EMA_Target_0', 'movil_services', 'Camp_SRV_Up_Cross_Sell_SMS_Control_0', 'Camp_SRV_Legal_Informativa_SMS_Target_0', 'Camp_SRV_Ignite_MMS_Target_0', 'Camp_SRV_Up_Cross_Sell_TEL_Target_1', 'TRAT_FACT', 'Camp_SRV_Delight_SMS_Universal_0', 'HOMEZONE', 'Camp_SRV_Up_Cross_Sell_TEL_Control_0', 'Camp_SRV_Retention_Voice_SMS_Target_0', 'Camp_SRV_Up_Cross_Sell_TEL_Universal_0', 'fixed_fx_first', 'GNV_hour_0_W_RegularData_Num_Of_Connections', 'FX_DATA_ADDITIONAL', 'Camp_SRV_Up_Cross_Sell_EMA_Universal_0', 'Camp_SRV_Up_Cross_Sell_SLS_Control_0', 'Camp_SRV_Retention_Voice_TEL_Target_1', 'Camp_NIFs_Delight_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_0', 'MOVIL_HOMEZONE', 'Camp_SRV_Delight_EMA_Target_0', 'Bill_N2_Bill_Date', 'Camp_SRV_Retention_Voice_SMS_Control_0', 'GNV_hour_0_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_RegularData_Num_Of_Connections', 'GNV_hour_23_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Delight_NOT_Target_0', 'GNV_hour_1_W_RegularData_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_SLS_Universal_0', 'Camp_SRV_Retention_Voice_SLS_Target_0', 'GNV_hour_0_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0', 'GNV_hour_20_W_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_EMA_Target_0', 'GNV_hour_10_W_Num_Of_Calls', 'GNV_hour_11_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_WE_Num_Of_Calls', 'GNV_hour_3_W_RegularData_Num_Of_Connections', 'GNV_hour_9_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_19_W_Social_Pass_Num_Of_Connections', 'GNV_hour_4_W_RegularData_Num_Of_Connections', 'Camp_SRV_Ignite_SMS_Target_0', 'GNV_hour_8_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_W_Num_Of_Calls', 'GNV_hour_19_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_13_W_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_SLS_Target_1', 'Camp_SRV_Up_Cross_Sell_NOT_Target_0', 'GNV_hour_1_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_11_W_Social_Pass_Num_Of_Connections', 'GNV_hour_2_W_RegularData_Num_Of_Connections', 'Camp_NIFs_Retention_Voice_EMA_Target_0', 'GNV_hour_11_WE_Num_Of_Calls', 'GNV_hour_0_W_RegularData_Data_Volume_MB', 'GNV_hour_18_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_19_W_RegularData_Num_Of_Connections', 'GNV_hour_18_W_RegularData_Num_Of_Connections', 'GNV_hour_12_W_RegularData_Num_Of_Connections', 'CICLO', 'Camp_SRV_Retention_Voice_TEL_Target_0', 'GNV_hour_10_W_RegularData_Num_Of_Connections', 'GNV_hour_15_W_Num_Of_Calls', 'GNV_hour_12_WE_Num_Of_Calls', 'GNV_hour_3_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_W_Social_Pass_Num_Of_Connections', 'GNV_hour_12_W_Num_Of_Calls', 'GNV_hour_9_W_Social_Pass_Num_Of_Connections', 'GNV_hour_21_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_20_W_RegularData_Num_Of_Connections', 'GNV_hour_19_W_Num_Of_Calls', 'GNV_hour_12_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_22_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_9_W_RegularData_Num_Of_Connections', 'Camp_SRV_Delight_MLT_Universal_0', 'GNV_hour_21_W_Num_Of_Calls', 'GNV_hour_8_W_RegularData_Num_Of_Connections', 'TACADA', 'GNV_hour_16_W_Num_Of_Calls', 'Camp_SRV_Delight_TEL_Universal_0', 'GNV_hour_20_W_Num_Of_Calls', 'GNV_hour_13_WE_Num_Of_Calls', 'GNV_hour_14_W_Num_Of_Calls', 'GNV_hour_22_W_Social_Pass_Num_Of_Connections', 'RGU', 'GNV_hour_23_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_2_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0', 'GNV_hour_9_W_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_TEL_Target_0', 'gender2hgbst_elm', 'GNV_hour_8_W_Social_Pass_Num_Of_Connections', 'GNV_hour_22_W_RegularData_Num_Of_Connections', 'GNV_hour_21_W_RegularData_Num_Of_Connections', 'GNV_hour_7_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_17_W_Num_Of_Calls', 'GNV_hour_14_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0', 'GNV_hour_19_WE_Num_Of_Calls', 'Camp_NIFs_Legal_Informativa_SMS_Target_0', 'GNV_hour_6_W_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_SLS_Target_0', 'GNV_hour_4_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_13_W_MOU', 'GNV_hour_23_W_RegularData_Num_Of_Connections', 'GNV_hour_1_W_Social_Pass_Num_Of_Connections', 'GNV_hour_17_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_14_WE_Num_Of_Calls', 'GNV_hour_13_W_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Ignite_SMS_Universal_0', 'Camp_SRV_Delight_SMS_Control_0', 'GNV_hour_16_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_16_W_RegularData_Num_Of_Connections', 'GNV_hour_14_W_RegularData_Num_Of_Connections', 'GNV_hour_20_W_Social_Pass_Num_Of_Connections', 'FX_DECO_TV', 'GNV_hour_2_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_11_W_Num_Of_Calls', 'GNV_hour_18_W_Social_Pass_Num_Of_Connections', 'GNV_hour_15_W_RegularData_Num_Of_Connections', 'GNV_hour_7_W_RegularData_Num_Of_Connections', 'GNV_hour_1_WE_RegularData_Num_Of_Connections', 'NACIONALIDAD', 'GNV_hour_4_W_Social_Pass_Num_Of_Connections', 'GNV_hour_10_W_Social_Pass_Num_Of_Connections', 'GNV_hour_13_W_RegularData_Num_Of_Connections', 'Camp_SRV_Retention_Voice_EMA_Target_0', 'GNV_hour_17_W_RegularData_Num_Of_Connections', 'GNV_hour_11_W_MOU', 'GNV_hour_20_WE_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0', 'GNV_hour_12_W_MOU', 'GNV_hour_3_W_Social_Pass_Num_Of_Connections', 'GNV_hour_15_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_WE_Num_Of_Calls', 'GNV_hour_19_WE_MOU', 'GNV_hour_19_W_MOU', 'GNV_hour_17_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0', 'GNV_hour_11_W_RegularData_Num_Of_Connections', 'GNV_hour_12_W_Social_Pass_Num_Of_Connections', 'GNV_hour_7_W_Social_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Num_Of_Calls', 'GNV_hour_12_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_14_W_MOU', 'GNV_hour_8_W_Num_Of_Calls', 'GNV_hour_16_WE_Num_Of_Calls', 'GNV_hour_13_W_Social_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Legal_Informativa_FAC_Target_0', 'Camp_NIFs_Delight_TEL_Universal_0', 'FX_HOMEZONE', 'GNV_hour_20_W_MOU', 'Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0', 'GNV_hour_14_W_Social_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_9_W_MOU', 'GNV_hour_12_WE_MOU', 'GNV_hour_15_WE_Num_Of_Calls', 'GNV_hour_17_WE_Num_Of_Calls', 'ROAMING_BASIC', 'Camp_NIFs_Legal_Informativa_SLS_Target_0', 'Bill_N1_Bill_Date', 'GNV_hour_1_W_RegularData_Data_Volume_MB', 'ccc_Resultado_Retenido', 'GNV_hour_18_W_MOU', 'GNV_hour_3_WE_RegularData_Num_Of_Connections', 'Camp_SRV_Welcome_MMS_Target_0', 'GNV_hour_1_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_Num_Of_Calls', 'GNV_hour_9_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_3_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_22_W_Num_Of_Calls', 'GNV_hour_8_WE_RegularData_Num_Of_Connections', 'GNV_hour_10_W_MOU', 'GNV_hour_2_W_Social_Pass_Num_Of_Connections', 'GNV_hour_17_W_MOU', 'GNV_hour_2_WE_RegularData_Num_Of_Connections', 'GNV_hour_14_WE_MOU', 'GNV_hour_12_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_23_W_Num_Of_Calls', 'GNV_hour_11_WE_MOU', 'DATA_ADDITIONAL', 'GNV_hour_22_WE_Num_Of_Calls', 'Camp_SRV_Retention_Voice_EMA_Control_0', 'GNV_hour_5_W_Chat_Zero_Num_Of_Connections', 'ccc_Collections_Collections_process', 'GNV_hour_9_WE_Num_Of_Calls', 'GNV_hour_20_WE_MOU', 'Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0', 'GNV_hour_16_W_MOU', 'GNV_hour_7_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_16_W_Social_Pass_Num_Of_Connections', 'ccc_Factura', 'ccc_Billing___Postpaid_Invoice_clarification', 'GNV_hour_1_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Retention_Voice_SAT_Control_0', 'GNV_hour_15_W_Social_Pass_Num_Of_Connections', 'GNV_hour_3_W_RegularData_Data_Volume_MB', 'GNV_hour_15_WE_MOU', 'TIPO_DOCUMENTO', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0', 'Camp_SRV_Welcome_EMA_Target_0', 'GNV_hour_10_WE_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0', 'COD_ESTADO_GENERAL', 'GNV_hour_6_W_RegularData_Num_Of_Connections', 'GNV_hour_2_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_SMS_Target_0', 'Camp_NIFs_Ignite_SMS_Target_0', 'ccc_Collections_Debt_recovery', 'GNV_hour_13_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_5_W_RegularData_Num_Of_Connections', 'GNV_hour_4_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_WE_Chat_Zero_Num_Of_Connections', 'SUPEROFERTA', 'GNV_hour_18_WE_MOU', 'GNV_hour_12_WE_RegularData_Num_Of_Connections', 'GNV_hour_4_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_4_WE_RegularData_Num_Of_Connections', 'GNV_hour_2_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_6_W_Social_Pass_Num_Of_Connections', 'GNV_hour_7_W_Num_Of_Calls', 'GNV_hour_19_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_20_WE_RegularData_Num_Of_Connections', 'ccc_Desactivacion_Resto', 'GNV_hour_13_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_15_W_MOU', 'ccc_Device_upgrade_Referrals', 'GNV_hour_23_WE_RegularData_Num_Of_Connections', 'ccc_Product_and_Service_management_Standard_products', 'GNV_hour_19_WE_RegularData_Num_Of_Connections', 'ccc_Churn_Cancellations_Debt_recovery', 'Camp_NIFs_Retention_Voice_SAT_Target_0', 'X_FORMATO_FACTURA', 'GNV_hour_9_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_W_Social_Pass_Data_Volume_MB', 'GNV_hour_20_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_18_W_Social_Pass_Data_Volume_MB', 'tnps_TNPS01', 'GNV_hour_16_WE_RegularData_Num_Of_Connections', 'ccc_Collections', 'Camp_NIFs_Up_Cross_Sell_EMA_Target_1', 'GNV_hour_13_WE_RegularData_Num_Of_Connections', 'ccc_Billing___Postpaid_Billing_errors', 'GNV_hour_11_WE_RegularData_Num_Of_Connections', 'GNV_hour_23_WE_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_EMA_Control_0', 'tnps_max_VDN', 'fixed_services', 'Camp_NIFs_Delight_TEL_Target_0', 'tnps_min_VDN', 'GNV_hour_10_W_Chat_Zero_Data_Volume_MB', 'tnps_mean_VDN', 'ccc_Billing___Postpaid', 'tnps_TNPS4', 'GNV_hour_1_WE_Social_Pass_Num_Of_Connections', 'ccc_Device_upgrade_Device_information', 'Camp_NIFs_Retention_Voice_SMS_Target_0', 'GNV_hour_21_WE_RegularData_Num_Of_Connections', 'GNV_hour_20_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_NOT_Control_0', 'GNV_hour_19_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_8_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_MOU', 'GNV_hour_7_W_RegularData_Data_Volume_MB', 'GNV_hour_22_W_Social_Pass_Data_Volume_MB', 'GNV_hour_17_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_WE_RegularData_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_TEL_Target_0', 'GNV_hour_6_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_WE_RegularData_Num_Of_Connections', 'GNV_hour_4_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_13_WE_MOU', 'GNV_hour_3_WE_RegularData_Data_Volume_MB', 'GNV_hour_0_W_Num_Of_Calls', 'GNV_hour_18_WE_RegularData_Num_Of_Connections', 'GNV_hour_18_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_10_WE_MOU', 'GNV_hour_7_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_WE_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Delight_NOT_Universal_0', 'GNV_hour_20_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_4_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_TER_Universal_0', 'GNV_hour_22_WE_Social_Pass_Data_Volume_MB', 'ccc_Cobro', 'GNV_hour_15_WE_RegularData_Num_Of_Connections', 'GNV_hour_22_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_17_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Chat_Zero_Num_Of_Connections', 'PRICE_DTO_LEV1', 'tnps_TNPS', 'GNV_hour_17_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_W_MOU', 'GNV_hour_14_WE_RegularData_Num_Of_Connections', 'GNV_hour_3_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_1_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_23_WE_Chat_Zero_Num_Of_Connections', 'FX_DTO_LEV1', 'GNV_hour_9_WE_Social_Pass_Num_Of_Connections', 'ccc_Voice_and_mobile_data_incidences_and_support', 'GNV_hour_15_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_14_WE_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_MMS_Target_0', 'Camp_NIFs_Retention_Voice_EMA_Control_0', 'GNV_hour_16_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_14_WE_Social_Pass_Num_Of_Connections', 'ccc_NA_NA', 'GNV_hour_17_WE_RegularData_Data_Volume_MB', 'GNV_hour_19_W_Social_Pass_Data_Volume_MB', 'ccc_NA', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0', 'GNV_hour_1_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_5_W_Social_Pass_Num_Of_Connections', 'GNV_hour_19_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_18_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_6_WE_RegularData_Num_Of_Connections', 'GNV_hour_17_WE_MOU', 'GNV_hour_7_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_21_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_5_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_WE_Social_Pass_Data_Volume_MB', 'Camp_NIFs_Retention_Voice_SMS_Control_0', 'GNV_hour_8_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_17_W_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Retention_Voice_SAT_Control_0', 'GNV_hour_10_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Legal_Informativa_EMA_Target_0', 'GNV_hour_17_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_9_WE_MOU', 'ccc_Device_upgrade', 'GNV_hour_22_W_MOU', 'GNV_hour_2_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_7_WE_Num_Of_Calls', 'GNV_hour_10_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Target_0', 'GNV_hour_8_WE_Num_Of_Calls', 'GNV_hour_22_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_TEL_Target_1', 'GNV_hour_1_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_W_RegularData_Data_Volume_MB', 'GNV_hour_13_W_Social_Pass_Data_Volume_MB', 'tnps_TNPS2PRO', 'GNV_hour_13_WE_Social_Pass_Num_Of_Connections', 'PRICE_DATA_ADDITIONAL', 'ccc_Other_customer_information_management_PIN_PUK', 'GNV_hour_10_W_Social_Pass_Data_Volume_MB', 'GNV_hour_4_WE_RegularData_Data_Volume_MB', 'ccc_Desactivacion_NET', 'GNV_hour_9_W_RegularData_Data_Volume_MB', 'GNV_hour_21_WE_MOU', 'tnps_TNPS2DET', 'GNV_hour_15_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_15_WE_RegularData_Data_Volume_MB', 'DECO_TV', 'Camp_NIFs_Up_Cross_Sell_TEL_Control_0', 'GNV_hour_5_W_Chat_Zero_Data_Volume_MB', 'ccc_Other_customer_information_management_Device_repair', 'ccc_Alta', 'GNV_hour_2_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_20_WE_RegularData_Data_Volume_MB', 'GNV_hour_0_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_3_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_Social_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Chat_Zero_Data_Volume_MB', 'ccc_Tariff_management_Voice_tariff', 'GNV_hour_11_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_13_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Ignite_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_TEL_Universal_0', 'ccc_Voice_and_mobile_data_incidences_and_support_Mobile_Data_incidences', 'GNV_hour_16_W_Social_Pass_Data_Volume_MB', 'ccc_Collections_Referrals', 'GNV_hour_12_W_RegularData_Data_Volume_MB', 'ccc_Product_and_Service_management_Device_upgrade', 'GNV_hour_14_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_15_W_RegularData_Data_Volume_MB', 'GNV_hour_8_W_MOU', 'prepaid_fx_first', 'ccc_Quick_Closing_TV', 'GNV_hour_14_W_RegularData_Data_Volume_MB', 'GNV_hour_2_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_TEL_Universal_1', 'GNV_hour_20_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_23_W_Social_Pass_Data_Volume_MB', 'GNV_hour_1_WE_Chat_Zero_Data_Volume_MB', 'ccc_Product_and_Service_management', 'GNV_hour_0_W_Social_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_3_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_RegularData_Data_Volume_MB', 'ccc_Cierre', 'Camp_NIFs_Up_Cross_Sell_MMS_Control_1', 'GNV_hour_23_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0', 'GNV_hour_19_WE_RegularData_Data_Volume_MB', 'GNV_hour_21_W_RegularData_Data_Volume_MB', 'DTO_LEV1', 'ccc_Desactivacion_Fijo', 'GNV_hour_10_WE_RegularData_Data_Volume_MB', 'ccc_Other_customer_information_management', 'GNV_hour_11_WE_RegularData_Data_Volume_MB', 'ccc_Other_customer_information_management_Referrals', 'GNV_hour_5_WE_Chat_Zero_Data_Volume_MB', 'ccc_Churn_Cancellations_Negotiation', 'GNV_hour_6_W_RegularData_Data_Volume_MB', 'GNV_hour_21_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_2_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_6_W_Num_Of_Calls', 'ccc_New_adds_process_Fix_line', 'GNV_hour_9_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_17_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1', 'ccc_Informacion', 'GNV_hour_5_WE_RegularData_Num_Of_Connections', 'GNV_hour_9_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_TER_Target_0', 'GNV_hour_22_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_SMS_Target_1', 'Camp_NIFs_Up_Cross_Sell_SMS_Control_0', 'Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0', 'ccc_Averia_DSL', 'METODO_PAGO', 'GNV_hour_18_W_RegularData_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_FIBRA_support', 'PRICE_PVR_TV', 'x_cesion_datos', 'ccc_Quick_Closing_Quick_Closing', 'ccc_Billing___Postpaid_Collections_process', 'GNV_hour_13_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_6_WE_Num_Of_Calls', 'GNV_hour_16_W_RegularData_Data_Volume_MB', 'TV_TOTAL_CHARGES', 'GNV_hour_20_W_RegularData_Data_Volume_MB', 'Camp_SRV_Retention_Voice_TEL_Control_0', 'ccc_Provision_Movil', 'X_PUBLICIDAD_EMAIL', 'ccc_Resultado_Envio_tecnico', 'GNV_hour_23_WE_MOU', 'GNV_hour_8_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Chat_Zero_Data_Volume_MB', 'ccc_Quick_Closing', 'GNV_hour_3_W_Social_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Social_Pass_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_Network', 'ccc_Desactivacion_TV', 'ccc_Consulta_Tecnica_Movil', 'GNV_hour_6_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_RegularData_Data_Volume_MB', 'GNV_hour_14_W_Social_Pass_Data_Volume_MB', 'ccc_Other_customer_information_management_Customer_service_process', 'GNV_hour_13_WE_RegularData_Data_Volume_MB', 'GNV_hour_6_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_W_Social_Pass_Data_Volume_MB', 'GNV_hour_23_WE_RegularData_Data_Volume_MB', 'ccc_Consulta_Tecnica_Fibra', 'GNV_hour_20_WE_Social_Pass_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_Modem_Router_support', 'GNV_hour_0_W_Chat_Zero_Data_Volume_MB', 'ccc_Churn_Cancellations', 'GNV_hour_23_W_MOU', 'GNV_hour_3_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_22_WE_MOU', 'GNV_hour_19_W_RegularData_Data_Volume_MB', 'GNV_hour_22_WE_Chat_Zero_Data_Volume_MB', 'PRICE_DATA', 'GNV_hour_9_W_Social_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_WE_RegularData_Data_Volume_MB', 'GNV_hour_17_W_RegularData_Data_Volume_MB', 'GNV_hour_14_WE_RegularData_Data_Volume_MB', 'GNV_hour_11_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MMS_Control_0', 'ccc_Portabilidad', 'FLG_ROBINSON', 'GNV_hour_19_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_9_WE_RegularData_Data_Volume_MB', 'GNV_hour_7_W_Social_Pass_Data_Volume_MB', 'GNV_hour_4_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Social_Pass_Data_Volume_MB', 'Camp_SRV_Welcome_SMS_Target_0', 'FX_TV_TARIFF', 'Camp_NIFs_Up_Cross_Sell_EMA_Control_0', 'GNV_hour_7_WE_Social_Pass_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MMS_Target_1', 'GNV_hour_14_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_23_W_Chat_Zero_Data_Volume_MB', 'ccc_Billing___Postpaid_Other_billing_issues', 'ccc_Product_and_Service_management_Contracted_products', 'ccc_Averia_Resto', 'GNV_hour_12_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1', 'GNV_hour_4_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_WE_Chat_Zero_Data_Volume_MB', 'ccc_Device_delivery_repair_Device_repair', 'ccc_Tariff_management', 'ccc_New_adds_process', 'GNV_hour_21_WE_Chat_Zero_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_Mobile_Data_support', 'ccc_DSL_FIBER_incidences_and_support_DSL_support', 'ccc_Device_delivery_repair', 'GNV_hour_23_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_W_Chat_Zero_Data_Volume_MB', 'ccc_Device_upgrade_Device_upgrade_order', 'GNV_hour_15_W_Social_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Universal_0', 'GNV_hour_8_WE_MOU', 'ccc_Churn_Cancellations_Churn_cancellations_process', 'ccc_Resultado_Solucionado', 'FX_PVR_TV', 'Camp_SRV_Up_Cross_Sell_RCS_Control_0', 'GNV_hour_3_WE_Num_Of_Calls', 'ccc_Device_delivery_repair_SIM', 'ccc_Tariff_management_Data_tariff', 'GNV_hour_23_W_Video_Pass_Num_Of_Connections', 'ccc_Incidencia_Resto', 'ccc_Resultado_No_Aplica', 'GNV_hour_16_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_5_W_RegularData_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_DSL_incidences', 'GNV_hour_5_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_6_W_Social_Pass_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_TV', 'ccc_Resultado_Escalo', 'GNV_hour_4_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_W_MOU', 'ccc_Desactivacion_Movil', 'GNV_hour_23_W_RegularData_Data_Volume_MB', 'FX_FOOTBALL_TV', 'Camp_NIFs_Retention_Voice_SAT_Universal_0', 'ccc_Desactivacion_USB', 'ccc_Productos_Resto', 'ccc_DSL_FIBER_incidences_and_support', 'ccc_Voice_and_mobile_data_incidences_and_support_Modem_Router_support', 'GNV_hour_8_WE_RegularData_Data_Volume_MB', 'GNV_hour_18_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_6_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Control_0', 'GNV_hour_2_WE_Num_Of_Calls', 'GNV_hour_7_WE_MOU', 'ccc_Baja', 'GNV_hour_5_WE_RegularData_Data_Volume_MB', 'GNV_hour_4_WE_Num_Of_Calls', 'GNV_hour_22_WE_RegularData_Data_Volume_MB', 'ccc_Resultado_Informacion', 'GNV_hour_18_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MLT_Universal_0', 'GNV_hour_4_W_Num_Of_Calls', 'ccc_DSL_FIBER_incidences_and_support_Referrals', 'Camp_SRV_Up_Cross_Sell_SMS_Universal_0', 'GNV_hour_5_WE_Num_Of_Calls', 'GNV_hour_15_WE_Social_Pass_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_FIBRA_incidences', 'ccc_Pagar_menos', 'GNV_hour_1_WE_Num_Of_Calls', 'GNV_hour_1_W_Num_Of_Calls', 'ccc_Prepaid_balance', 'prepaid_services', 'Camp_NIFs_Delight_SMS_Control_0', 'GNV_hour_0_W_MOU', 'GNV_hour_1_WE_MOU', 'GNV_hour_3_W_Num_Of_Calls', 'GNV_hour_7_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_1_W_MOU', 'GNV_hour_21_WE_RegularData_Data_Volume_MB', 'GNV_hour_5_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_0_WE_MOU', 'ccc_Product_and_Service_management_Contracted_products_DSL', 'FX_TV_CUOTA_ALTA', 'GNV_hour_11_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_17_WE_Social_Pass_Data_Volume_MB', 'ccc_Other_customer_information_management_Transfers', 'GNV_hour_5_W_Num_Of_Calls', 'bam_fx_first', 'ccc_Collections_Transfers', 'x_antiguedad_cuenta', 'GNV_hour_2_W_Num_Of_Calls', 'GNV_hour_15_WE_Chat_Zero_Data_Volume_MB', 'ccc_Tariff_management_Voice', 'ccc_Incidencia_Provision_Neba', 'ccc_Product_and_Service_management_Billing_errors', 'Camp_SRV_Up_Cross_Sell_RCS_Target_0', 'GNV_hour_5_W_Social_Pass_Data_Volume_MB', 'ccc_Churn_Cancellations_TV', 'GNV_hour_6_WE_MOU', 'ccc_Other_customer_information_management_Customer_data', 'GNV_hour_6_WE_Chat_Zero_Data_Volume_MB', 'ccc_Resultado_Abono', 'ccc_New_adds_process_Referrals', 'ccc_DSL_FIBER_incidences_and_support_TV', 'ccc_New_adds_process_Voice_line', 'TV_TARIFF', 'ccc_Churn_Cancellations_Transfers', 'ccc_Transferencia', 'ccc_Incidencia_Provision_DSL', 'GNV_hour_23_WE_Chat_Zero_Data_Volume_MB', 'ccc_Desactivacion_BA_Movil_TV', 'ccc_Product_and_Service_management_TV', 'ENCUESTAS', 'ccc_DSL_FIBER_incidences_and_support_Transfers', 'ccc_Averia_Fibra', 'ccc_Voice_and_mobile_data_incidences_and_support_Transfers', 'X_IDIOMA_FACTURA', 'GNV_hour_6_W_MOU', 'PRICE_DECO_TV', 'ccc_New_adds_process_Transfers', 'GNV_hour_2_WE_MOU', 'ccc_DSL_FIBER_incidences_and_support_Modem_Router_incidences', 'ccc_DSL_FIBER_incidences_and_support_Network', 'PVR_TV', 'PRICE_FOOTBALL_TV', 'GNV_hour_2_W_MOU', 'bam_services', 'ccc_Resultado_Transferencia', 'GNV_hour_6_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_4_W_MOU', 'ccc_Prepaid_balance_Transfers', 'ccc_Incidencia_Tecnica', 'FOOTBALL_TV', 'ccc_Provision_Neba', 'GNV_hour_0_W_Video_Pass_Num_Of_Connections', 'GNV_hour_3_WE_MOU', 'ccc_Incidencia_Provision_Movil', 'ccc_Voice_and_mobile_data_incidences_and_support_Referrals', 'ccc_Provision_Fibra', 'GNV_hour_3_W_MOU', 'GNV_hour_5_W_MOU', 'ccc_Product_and_Service_management_Voice_tariff', 'NUM_SERIE_DECO_TV', 'DTO_LEV2', 'TV_CUOTA_ALTA', 'x_datos_trafico', 'FX_DTO_LEV2', 'ccc_New_adds_process_TV', 'GNV_hour_4_WE_MOU', 'ccc_Incidencia_Provision_Fibra', 'ccc_Other_customer_information_management_Simlock', 'ccc_Ofrecimiento', 'x_datos_navegacion', 'GNV_hour_5_WE_MOU', 'ccc_Provision_DSL', 'PRICE_DTO_LEV2', 'ccc_Churn_Cancellations_Referrals', 'ccc_Device_upgrade_Simlock', 'ccc_Precios', 'ccc_Consulta_Tecnica_DSL', 'ccc_Churn_Cancellations_Network', 'ccc_Product_and_Service_management_Referrals', 'CLASE_CLI_COD_CLASE_CLIENTE', 'FX_NETFLIX_NAPSTER', 'NETFLIX_NAPSTER', 'FACTURA_CATALAN', 'GNV_hour_16_W_Video_Pass_Num_Of_Connections', 'GNV_hour_15_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Video_Pass_Data_Volume_MB', 'GNV_hour_21_W_Video_Pass_Num_Of_Connections', 'GNV_hour_2_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_Music_Pass_Num_Of_Connections', 'GNV_hour_4_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_3_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_11_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_Music_Pass_Num_Of_Connections', 'GNV_hour_14_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_13_W_Video_Pass_Num_Of_Connections', 'GNV_hour_20_W_Video_Pass_Num_Of_Connections', 'GNV_hour_17_W_Video_Pass_Num_Of_Connections', 'GNV_hour_7_W_Video_Pass_Num_Of_Connections', 'GNV_hour_21_W_Music_Pass_Data_Volume_MB', 'GNV_hour_21_W_Music_Pass_Num_Of_Connections', 'GNV_hour_9_W_Video_Pass_Num_Of_Connections', 'GNV_hour_14_W_Music_Pass_Data_Volume_MB', 'GNV_hour_18_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Music_Pass_Num_Of_Connections', 'GNV_hour_4_W_Video_Pass_Num_Of_Connections', 'GNV_hour_9_W_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_22_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_0_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_14_W_Video_Pass_Num_Of_Connections', 'GNV_hour_13_W_Video_Pass_Data_Volume_MB', 'GNV_hour_1_W_Video_Pass_Num_Of_Connections', 'GNV_hour_7_W_Music_Pass_Data_Volume_MB', 'GNV_hour_5_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_Video_Pass_Num_Of_Connections', 'GNV_hour_6_W_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_12_W_Video_Pass_Num_Of_Connections', 'GNV_hour_12_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_15_W_Music_Pass_Data_Volume_MB', 'GNV_hour_17_W_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_10_W_Music_Pass_Data_Volume_MB', 'GNV_hour_2_W_Video_Pass_Data_Volume_MB', 'GNV_hour_6_W_Video_Pass_Data_Volume_MB', 'GNV_hour_1_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_16_W_Music_Pass_Num_Of_Connections', 'GNV_hour_12_W_Music_Pass_Num_Of_Connections', 'GNV_hour_21_W_Video_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_0_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_Music_Pass_Data_Volume_MB', 'GNV_hour_4_W_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_21_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_4_W_Video_Pass_Data_Volume_MB', 'GNV_hour_3_W_Video_Pass_Num_Of_Connections', 'GNV_hour_2_W_Music_Pass_Num_Of_Connections', 'GNV_hour_22_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_14_W_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_12_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_21_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_10_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_9_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_16_W_Music_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_6_W_Music_Pass_Data_Volume_MB', 'GNV_hour_15_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_W_Music_Pass_Data_Volume_MB', 'GNV_hour_14_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_0_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_11_W_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_15_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_2_W_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_W_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_7_W_Music_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_15_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_9_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_6_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_W_Music_Pass_Num_Of_Connections', 'GNV_hour_21_W_MasMegas_Num_Of_Connections', 'GNV_hour_4_W_Music_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_16_W_Video_Pass_Data_Volume_MB', 'GNV_hour_15_W_MasMegas_Num_Of_Connections', 'GNV_hour_21_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_19_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_19_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_17_W_Music_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_13_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_15_W_Video_Pass_Data_Volume_MB', 'GNV_hour_0_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_23_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_16_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_0_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_10_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_W_MasMegas_Num_Of_Connections', 'GNV_hour_10_W_Music_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_23_W_Video_Pass_Data_Volume_MB', 'GNV_hour_18_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_22_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_10_W_Video_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_1_W_Music_Pass_Num_Of_Connections', 'GNV_hour_6_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_19_W_Music_Pass_Data_Volume_MB', 'GNV_hour_9_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_8_W_Video_Pass_Num_Of_Connections', 'GNV_hour_18_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_13_W_Music_Pass_Data_Volume_MB', 'GNV_hour_8_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_22_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_18_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_4_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_0_W_Music_Pass_Data_Volume_MB', 'GNV_hour_21_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_19_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_11_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_16_W_MasMegas_Num_Of_Connections', 'GNV_hour_14_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_1_W_Music_Pass_Data_Volume_MB', 'GNV_hour_17_W_MasMegas_Num_Of_Connections', 'GNV_hour_23_W_Music_Pass_Data_Volume_MB', 'GNV_hour_6_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_3_W_Music_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_13_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_12_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_12_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_22_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_4_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_20_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_10_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_5_W_Video_Pass_Num_Of_Connections', 'GNV_hour_5_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_11_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_8_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_8_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_14_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_13_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_13_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_10_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_17_W_Video_Pass_Data_Volume_MB', 'GNV_hour_7_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_8_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_22_W_Music_Pass_Num_Of_Connections', 'GNV_hour_5_W_Video_Pass_Data_Volume_MB', 'GNV_hour_7_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_20_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_13_W_Music_Pass_Num_Of_Connections', 'GNV_hour_18_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_18_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_19_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_11_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_3_W_Music_Pass_Data_Volume_MB', 'GNV_hour_20_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_13_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_18_W_VideoHD_Pass_Data_Volume_MB', 'FX_MOTOR_TV', 'GNV_hour_8_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_W_Music_Pass_Data_Volume_MB', 'GNV_hour_3_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_1_W_Video_Pass_Data_Volume_MB', 'GNV_hour_17_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_11_W_Music_Pass_Data_Volume_MB', 'GNV_hour_7_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_6_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_8_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_12_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_14_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_W_Video_Pass_Data_Volume_MB', 'GNV_hour_18_W_MasMegas_Num_Of_Connections', 'GNV_hour_13_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_16_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_17_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_6_WE_Video_Pass_Num_Of_Connections', 'x_user_facebook', 'MOTOR_TV', 'x_tipo_cuenta_corp', 'TRYBUY_TV']
        #self.train_cols = ['PROVINCIA', 'ANYOS_MIGRACION', 'Bill_N5_Bill_Amount', 'TARIFF', 'DESC_TARIFF', 'Bill_N2_Tax_Amount', 'FX_ROAM_ZONA_2', 'Bill_N3_Bill_Amount', 'EDAD', 'Bill_N4_Tax_Amount', 'Bill_N1_Tax_Amount', 'Bill_N3_Tax_Amount', 'Bill_N5_Tax_Amount', 'central_office', 'Bill_N5_Bill_Date', 'FX_ROAMING_BASIC', 'Bill_N4_Bill_Amount', 'Bill_N1_Bill_Amount', 'MOBILE_BAM_TOTAL_CHARGES', 'Bill_N2_Bill_Amount', 'Camp_NIFs_Delight_SMS_Target_0', 'SRV_BASIC', 'FX_TARIFF', 'DESC_SRV_BASIC', 'PRICE_TARIFF', 'FACTURA_ELECTRONICA', 'Bill_N4_Bill_Date', 'CIUDAD', 'Bill_N3_Bill_Date', 'marriage2hgbst_elm', 'PUBLICIDAD', 'CODIGO_POSTAL', 'FX_VOICE_TARIFF', 'tv_services', 'tv_fx_first', 'FX_SRV_BASIC', 'Camp_SRV_Up_Cross_Sell_MMS_Target_0', 'movil_fx_first', 'Camp_SRV_Delight_SMS_Target_0', 'ROAM_ZONA_2', 'DATA', 'FX_DATA', 'Camp_SRV_Retention_Voice_SAT_Target_0', 'Camp_SRV_Up_Cross_Sell_SMS_Target_0', 'TIPO_SIM', 'Camp_SRV_Up_Cross_Sell_MLT_Universal_0', 'Camp_SRV_Retention_Voice_NOT_Target_0', 'TRATAMIENTO', 'VOICE_TARIFF', 'Camp_SRV_Up_Cross_Sell_EMA_Target_0', 'movil_services', 'Camp_SRV_Up_Cross_Sell_SMS_Control_0', 'Camp_SRV_Legal_Informativa_SMS_Target_0', 'Camp_SRV_Ignite_MMS_Target_0', 'Camp_SRV_Up_Cross_Sell_TEL_Target_1', 'TRAT_FACT', 'Camp_SRV_Delight_SMS_Universal_0', 'HOMEZONE', 'Camp_SRV_Up_Cross_Sell_TEL_Control_0', 'Camp_SRV_Retention_Voice_SMS_Target_0', 'Camp_SRV_Up_Cross_Sell_TEL_Universal_0', 'fixed_fx_first', 'GNV_hour_0_W_RegularData_Num_Of_Connections', 'FX_DATA_ADDITIONAL', 'Camp_SRV_Up_Cross_Sell_EMA_Universal_0', 'Camp_SRV_Up_Cross_Sell_SLS_Control_0', 'Camp_SRV_Retention_Voice_TEL_Target_1', 'Camp_NIFs_Delight_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_0', 'MOVIL_HOMEZONE', 'Camp_SRV_Delight_EMA_Target_0', 'Bill_N2_Bill_Date', 'Camp_SRV_Retention_Voice_SMS_Control_0', 'GNV_hour_0_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_RegularData_Num_Of_Connections', 'GNV_hour_23_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Delight_NOT_Target_0', 'GNV_hour_1_W_RegularData_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_SLS_Universal_0', 'Camp_SRV_Retention_Voice_SLS_Target_0', 'GNV_hour_0_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0', 'GNV_hour_20_W_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_EMA_Target_0', 'GNV_hour_10_W_Num_Of_Calls', 'GNV_hour_11_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_WE_Num_Of_Calls', 'GNV_hour_3_W_RegularData_Num_Of_Connections', 'GNV_hour_9_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_19_W_Social_Pass_Num_Of_Connections', 'GNV_hour_4_W_RegularData_Num_Of_Connections', 'Camp_SRV_Ignite_SMS_Target_0', 'GNV_hour_8_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_W_Num_Of_Calls', 'GNV_hour_19_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_13_W_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_SLS_Target_1', 'Camp_SRV_Up_Cross_Sell_NOT_Target_0', 'GNV_hour_1_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_11_W_Social_Pass_Num_Of_Connections', 'GNV_hour_2_W_RegularData_Num_Of_Connections', 'Camp_NIFs_Retention_Voice_EMA_Target_0', 'GNV_hour_11_WE_Num_Of_Calls', 'GNV_hour_0_W_RegularData_Data_Volume_MB', 'GNV_hour_18_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_19_W_RegularData_Num_Of_Connections', 'GNV_hour_18_W_RegularData_Num_Of_Connections', 'GNV_hour_12_W_RegularData_Num_Of_Connections', 'CICLO', 'Camp_SRV_Retention_Voice_TEL_Target_0', 'GNV_hour_10_W_RegularData_Num_Of_Connections', 'GNV_hour_15_W_Num_Of_Calls', 'GNV_hour_12_WE_Num_Of_Calls', 'GNV_hour_3_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_W_Social_Pass_Num_Of_Connections', 'GNV_hour_12_W_Num_Of_Calls', 'GNV_hour_9_W_Social_Pass_Num_Of_Connections', 'GNV_hour_21_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_20_W_RegularData_Num_Of_Connections', 'GNV_hour_19_W_Num_Of_Calls', 'GNV_hour_12_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_22_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_9_W_RegularData_Num_Of_Connections', 'Camp_SRV_Delight_MLT_Universal_0', 'GNV_hour_21_W_Num_Of_Calls', 'GNV_hour_8_W_RegularData_Num_Of_Connections', 'TACADA', 'GNV_hour_16_W_Num_Of_Calls', 'Camp_SRV_Delight_TEL_Universal_0', 'GNV_hour_20_W_Num_Of_Calls', 'GNV_hour_13_WE_Num_Of_Calls', 'GNV_hour_14_W_Num_Of_Calls', 'GNV_hour_22_W_Social_Pass_Num_Of_Connections', 'RGU', 'GNV_hour_23_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_2_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0', 'GNV_hour_9_W_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_TEL_Target_0', 'gender2hgbst_elm', 'GNV_hour_8_W_Social_Pass_Num_Of_Connections', 'GNV_hour_22_W_RegularData_Num_Of_Connections', 'GNV_hour_21_W_RegularData_Num_Of_Connections', 'GNV_hour_7_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_17_W_Num_Of_Calls', 'GNV_hour_14_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0', 'GNV_hour_19_WE_Num_Of_Calls', 'Camp_NIFs_Legal_Informativa_SMS_Target_0', 'GNV_hour_6_W_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_SLS_Target_0', 'GNV_hour_4_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_13_W_MOU', 'GNV_hour_23_W_RegularData_Num_Of_Connections', 'GNV_hour_1_W_Social_Pass_Num_Of_Connections', 'GNV_hour_17_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_14_WE_Num_Of_Calls', 'GNV_hour_13_W_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Ignite_SMS_Universal_0', 'Camp_SRV_Delight_SMS_Control_0', 'GNV_hour_16_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_16_W_RegularData_Num_Of_Connections', 'GNV_hour_14_W_RegularData_Num_Of_Connections', 'GNV_hour_20_W_Social_Pass_Num_Of_Connections', 'FX_DECO_TV', 'GNV_hour_2_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_11_W_Num_Of_Calls', 'GNV_hour_18_W_Social_Pass_Num_Of_Connections', 'GNV_hour_15_W_RegularData_Num_Of_Connections', 'GNV_hour_7_W_RegularData_Num_Of_Connections', 'GNV_hour_1_WE_RegularData_Num_Of_Connections', 'NACIONALIDAD', 'GNV_hour_4_W_Social_Pass_Num_Of_Connections', 'GNV_hour_10_W_Social_Pass_Num_Of_Connections', 'GNV_hour_13_W_RegularData_Num_Of_Connections', 'Camp_SRV_Retention_Voice_EMA_Target_0', 'GNV_hour_17_W_RegularData_Num_Of_Connections', 'GNV_hour_11_W_MOU', 'GNV_hour_20_WE_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0', 'GNV_hour_12_W_MOU', 'GNV_hour_3_W_Social_Pass_Num_Of_Connections', 'GNV_hour_15_W_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_WE_Num_Of_Calls', 'GNV_hour_19_WE_MOU', 'GNV_hour_19_W_MOU', 'GNV_hour_17_W_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0', 'GNV_hour_11_W_RegularData_Num_Of_Connections', 'GNV_hour_12_W_Social_Pass_Num_Of_Connections', 'GNV_hour_7_W_Social_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Num_Of_Calls', 'GNV_hour_12_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_14_W_MOU', 'GNV_hour_8_W_Num_Of_Calls', 'GNV_hour_16_WE_Num_Of_Calls', 'GNV_hour_13_W_Social_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Legal_Informativa_FAC_Target_0', 'Camp_NIFs_Delight_TEL_Universal_0', 'FX_HOMEZONE', 'GNV_hour_20_W_MOU', 'Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0', 'GNV_hour_14_W_Social_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_9_W_MOU', 'GNV_hour_12_WE_MOU', 'GNV_hour_15_WE_Num_Of_Calls', 'GNV_hour_17_WE_Num_Of_Calls', 'ROAMING_BASIC', 'Camp_NIFs_Legal_Informativa_SLS_Target_0', 'Bill_N1_Bill_Date', 'GNV_hour_1_W_RegularData_Data_Volume_MB', 'ccc_Resultado_Retenido', 'GNV_hour_18_W_MOU', 'GNV_hour_3_WE_RegularData_Num_Of_Connections', 'Camp_SRV_Welcome_MMS_Target_0', 'GNV_hour_1_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_0_WE_Num_Of_Calls', 'GNV_hour_9_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_3_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_22_W_Num_Of_Calls', 'GNV_hour_8_WE_RegularData_Num_Of_Connections', 'GNV_hour_10_W_MOU', 'GNV_hour_2_W_Social_Pass_Num_Of_Connections', 'GNV_hour_17_W_MOU', 'GNV_hour_2_WE_RegularData_Num_Of_Connections', 'GNV_hour_14_WE_MOU', 'GNV_hour_12_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_23_W_Num_Of_Calls', 'GNV_hour_11_WE_MOU', 'DATA_ADDITIONAL', 'GNV_hour_22_WE_Num_Of_Calls', 'Camp_SRV_Retention_Voice_EMA_Control_0', 'GNV_hour_5_W_Chat_Zero_Num_Of_Connections', 'ccc_Collections_Collections_process', 'GNV_hour_9_WE_Num_Of_Calls', 'GNV_hour_20_WE_MOU', 'Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0', 'GNV_hour_16_W_MOU', 'GNV_hour_7_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_16_W_Social_Pass_Num_Of_Connections', 'ccc_Factura', 'ccc_Billing___Postpaid_Invoice_clarification', 'GNV_hour_1_WE_RegularData_Data_Volume_MB', 'Camp_SRV_Retention_Voice_SAT_Control_0', 'GNV_hour_15_W_Social_Pass_Num_Of_Connections', 'GNV_hour_3_W_RegularData_Data_Volume_MB', 'GNV_hour_15_WE_MOU', 'TIPO_DOCUMENTO', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0', 'Camp_SRV_Welcome_EMA_Target_0', 'GNV_hour_10_WE_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0', 'COD_ESTADO_GENERAL', 'GNV_hour_6_W_RegularData_Num_Of_Connections', 'GNV_hour_2_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_SMS_Target_0', 'Camp_NIFs_Ignite_SMS_Target_0', 'ccc_Collections_Debt_recovery', 'GNV_hour_13_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_5_W_RegularData_Num_Of_Connections', 'GNV_hour_4_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_WE_Chat_Zero_Num_Of_Connections', 'SUPEROFERTA', 'GNV_hour_18_WE_MOU', 'GNV_hour_12_WE_RegularData_Num_Of_Connections', 'GNV_hour_4_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_4_WE_RegularData_Num_Of_Connections', 'GNV_hour_2_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_6_W_Social_Pass_Num_Of_Connections', 'GNV_hour_7_W_Num_Of_Calls', 'GNV_hour_19_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_20_WE_RegularData_Num_Of_Connections', 'ccc_Desactivacion_Resto', 'GNV_hour_13_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_15_W_MOU', 'ccc_Device_upgrade_Referrals', 'GNV_hour_23_WE_RegularData_Num_Of_Connections', 'ccc_Product_and_Service_management_Standard_products', 'GNV_hour_19_WE_RegularData_Num_Of_Connections', 'ccc_Churn_Cancellations_Debt_recovery', 'Camp_NIFs_Retention_Voice_SAT_Target_0', 'X_FORMATO_FACTURA', 'GNV_hour_9_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_W_Social_Pass_Data_Volume_MB', 'GNV_hour_20_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_18_W_Social_Pass_Data_Volume_MB', 'tnps_TNPS01', 'GNV_hour_16_WE_RegularData_Num_Of_Connections', 'ccc_Collections', 'Camp_NIFs_Up_Cross_Sell_EMA_Target_1', 'GNV_hour_13_WE_RegularData_Num_Of_Connections', 'ccc_Billing___Postpaid_Billing_errors', 'GNV_hour_11_WE_RegularData_Num_Of_Connections', 'GNV_hour_23_WE_Num_Of_Calls', 'Camp_SRV_Up_Cross_Sell_EMA_Control_0', 'tnps_max_VDN', 'fixed_services', 'Camp_NIFs_Delight_TEL_Target_0', 'tnps_min_VDN', 'GNV_hour_10_W_Chat_Zero_Data_Volume_MB', 'tnps_mean_VDN', 'ccc_Billing___Postpaid', 'tnps_TNPS4', 'GNV_hour_1_WE_Social_Pass_Num_Of_Connections', 'ccc_Device_upgrade_Device_information', 'Camp_NIFs_Retention_Voice_SMS_Target_0', 'GNV_hour_21_WE_RegularData_Num_Of_Connections', 'GNV_hour_20_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Social_Pass_Num_Of_Connections', 'Camp_SRV_Up_Cross_Sell_NOT_Control_0', 'GNV_hour_19_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_8_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_MOU', 'GNV_hour_7_W_RegularData_Data_Volume_MB', 'GNV_hour_22_W_Social_Pass_Data_Volume_MB', 'GNV_hour_17_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_WE_RegularData_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_TEL_Target_0', 'GNV_hour_6_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_WE_RegularData_Num_Of_Connections', 'GNV_hour_4_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_13_WE_MOU', 'GNV_hour_3_WE_RegularData_Data_Volume_MB', 'GNV_hour_0_W_Num_Of_Calls', 'GNV_hour_18_WE_RegularData_Num_Of_Connections', 'GNV_hour_18_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_10_WE_MOU', 'GNV_hour_7_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_WE_Chat_Zero_Num_Of_Connections', 'Camp_SRV_Delight_NOT_Universal_0', 'GNV_hour_20_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_4_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_TER_Universal_0', 'GNV_hour_22_WE_Social_Pass_Data_Volume_MB', 'ccc_Cobro', 'GNV_hour_15_WE_RegularData_Num_Of_Connections', 'GNV_hour_22_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_17_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Chat_Zero_Num_Of_Connections', 'PRICE_DTO_LEV1', 'tnps_TNPS', 'GNV_hour_17_WE_RegularData_Num_Of_Connections', 'GNV_hour_21_W_MOU', 'GNV_hour_14_WE_RegularData_Num_Of_Connections', 'GNV_hour_3_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_1_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_23_WE_Chat_Zero_Num_Of_Connections', 'FX_DTO_LEV1', 'GNV_hour_9_WE_Social_Pass_Num_Of_Connections', 'ccc_Voice_and_mobile_data_incidences_and_support', 'GNV_hour_15_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_14_WE_Chat_Zero_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_MMS_Target_0', 'Camp_NIFs_Retention_Voice_EMA_Control_0', 'GNV_hour_16_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_14_WE_Social_Pass_Num_Of_Connections', 'ccc_NA_NA', 'GNV_hour_17_WE_RegularData_Data_Volume_MB', 'GNV_hour_19_W_Social_Pass_Data_Volume_MB', 'ccc_NA', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0', 'GNV_hour_1_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_5_W_Social_Pass_Num_Of_Connections', 'GNV_hour_19_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_18_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_6_WE_RegularData_Num_Of_Connections', 'GNV_hour_17_WE_MOU', 'GNV_hour_7_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_21_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_5_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_18_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_10_WE_Social_Pass_Data_Volume_MB', 'Camp_NIFs_Retention_Voice_SMS_Control_0', 'GNV_hour_8_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_17_W_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Retention_Voice_SAT_Control_0', 'GNV_hour_10_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Legal_Informativa_EMA_Target_0', 'GNV_hour_17_WE_Chat_Zero_Num_Of_Connections', 'GNV_hour_21_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_9_WE_MOU', 'ccc_Device_upgrade', 'GNV_hour_22_W_MOU', 'GNV_hour_2_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_7_WE_Num_Of_Calls', 'GNV_hour_10_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Target_0', 'GNV_hour_8_WE_Num_Of_Calls', 'GNV_hour_22_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_TEL_Target_1', 'GNV_hour_1_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_W_RegularData_Data_Volume_MB', 'GNV_hour_13_W_Social_Pass_Data_Volume_MB', 'tnps_TNPS2PRO', 'GNV_hour_13_WE_Social_Pass_Num_Of_Connections', 'PRICE_DATA_ADDITIONAL', 'ccc_Other_customer_information_management_PIN_PUK', 'GNV_hour_10_W_Social_Pass_Data_Volume_MB', 'GNV_hour_4_WE_RegularData_Data_Volume_MB', 'ccc_Desactivacion_NET', 'GNV_hour_9_W_RegularData_Data_Volume_MB', 'GNV_hour_21_WE_MOU', 'tnps_TNPS2DET', 'GNV_hour_15_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_15_WE_RegularData_Data_Volume_MB', 'DECO_TV', 'Camp_NIFs_Up_Cross_Sell_TEL_Control_0', 'GNV_hour_5_W_Chat_Zero_Data_Volume_MB', 'ccc_Other_customer_information_management_Device_repair', 'ccc_Alta', 'GNV_hour_2_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_20_WE_RegularData_Data_Volume_MB', 'GNV_hour_0_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_3_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_Social_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Chat_Zero_Data_Volume_MB', 'ccc_Tariff_management_Voice_tariff', 'GNV_hour_11_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_13_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Ignite_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_TEL_Universal_0', 'ccc_Voice_and_mobile_data_incidences_and_support_Mobile_Data_incidences', 'GNV_hour_16_W_Social_Pass_Data_Volume_MB', 'ccc_Collections_Referrals', 'GNV_hour_12_W_RegularData_Data_Volume_MB', 'ccc_Product_and_Service_management_Device_upgrade', 'GNV_hour_14_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_15_W_RegularData_Data_Volume_MB', 'GNV_hour_8_W_MOU', 'prepaid_fx_first', 'ccc_Quick_Closing_TV', 'GNV_hour_14_W_RegularData_Data_Volume_MB', 'GNV_hour_2_W_Social_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0', 'Camp_NIFs_Up_Cross_Sell_TEL_Universal_1', 'GNV_hour_20_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_23_W_Social_Pass_Data_Volume_MB', 'GNV_hour_1_WE_Chat_Zero_Data_Volume_MB', 'ccc_Product_and_Service_management', 'GNV_hour_0_W_Social_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_3_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_RegularData_Data_Volume_MB', 'ccc_Cierre', 'Camp_NIFs_Up_Cross_Sell_MMS_Control_1', 'GNV_hour_23_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0', 'GNV_hour_19_WE_RegularData_Data_Volume_MB', 'GNV_hour_21_W_RegularData_Data_Volume_MB', 'DTO_LEV1', 'ccc_Desactivacion_Fijo', 'GNV_hour_10_WE_RegularData_Data_Volume_MB', 'ccc_Other_customer_information_management', 'GNV_hour_11_WE_RegularData_Data_Volume_MB', 'ccc_Other_customer_information_management_Referrals', 'GNV_hour_5_WE_Chat_Zero_Data_Volume_MB', 'ccc_Churn_Cancellations_Negotiation', 'GNV_hour_6_W_RegularData_Data_Volume_MB', 'GNV_hour_21_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_2_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_11_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_6_W_Num_Of_Calls', 'ccc_New_adds_process_Fix_line', 'GNV_hour_9_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_17_WE_Social_Pass_Num_Of_Connections', 'Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1', 'ccc_Informacion', 'GNV_hour_5_WE_RegularData_Num_Of_Connections', 'GNV_hour_9_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_TER_Target_0', 'GNV_hour_22_W_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_SMS_Target_1', 'Camp_NIFs_Up_Cross_Sell_SMS_Control_0', 'Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0', 'ccc_Averia_DSL', 'METODO_PAGO', 'GNV_hour_18_W_RegularData_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_FIBRA_support', 'PRICE_PVR_TV', 'x_cesion_datos', 'ccc_Quick_Closing_Quick_Closing', 'ccc_Billing___Postpaid_Collections_process', 'GNV_hour_13_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_6_WE_Num_Of_Calls', 'GNV_hour_16_W_RegularData_Data_Volume_MB', 'TV_TOTAL_CHARGES', 'GNV_hour_20_W_RegularData_Data_Volume_MB', 'Camp_SRV_Retention_Voice_TEL_Control_0', 'ccc_Provision_Movil', 'X_PUBLICIDAD_EMAIL', 'ccc_Resultado_Envio_tecnico', 'GNV_hour_23_WE_MOU', 'GNV_hour_8_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Chat_Zero_Data_Volume_MB', 'ccc_Quick_Closing', 'GNV_hour_3_W_Social_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Social_Pass_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_Network', 'ccc_Desactivacion_TV', 'ccc_Consulta_Tecnica_Movil', 'GNV_hour_6_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_RegularData_Data_Volume_MB', 'GNV_hour_14_W_Social_Pass_Data_Volume_MB', 'ccc_Other_customer_information_management_Customer_service_process', 'GNV_hour_13_WE_RegularData_Data_Volume_MB', 'GNV_hour_6_W_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_W_Social_Pass_Data_Volume_MB', 'GNV_hour_23_WE_RegularData_Data_Volume_MB', 'ccc_Consulta_Tecnica_Fibra', 'GNV_hour_20_WE_Social_Pass_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_Modem_Router_support', 'GNV_hour_0_W_Chat_Zero_Data_Volume_MB', 'ccc_Churn_Cancellations', 'GNV_hour_23_W_MOU', 'GNV_hour_3_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_22_WE_MOU', 'GNV_hour_19_W_RegularData_Data_Volume_MB', 'GNV_hour_22_WE_Chat_Zero_Data_Volume_MB', 'PRICE_DATA', 'GNV_hour_9_W_Social_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_WE_RegularData_Data_Volume_MB', 'GNV_hour_17_W_RegularData_Data_Volume_MB', 'GNV_hour_14_WE_RegularData_Data_Volume_MB', 'GNV_hour_11_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MMS_Control_0', 'ccc_Portabilidad', 'FLG_ROBINSON', 'GNV_hour_19_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_9_WE_RegularData_Data_Volume_MB', 'GNV_hour_7_W_Social_Pass_Data_Volume_MB', 'GNV_hour_4_W_Social_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Social_Pass_Data_Volume_MB', 'Camp_SRV_Welcome_SMS_Target_0', 'FX_TV_TARIFF', 'Camp_NIFs_Up_Cross_Sell_EMA_Control_0', 'GNV_hour_7_WE_Social_Pass_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MMS_Target_1', 'GNV_hour_14_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_23_W_Chat_Zero_Data_Volume_MB', 'ccc_Billing___Postpaid_Other_billing_issues', 'ccc_Product_and_Service_management_Contracted_products', 'ccc_Averia_Resto', 'GNV_hour_12_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1', 'GNV_hour_4_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_12_WE_Chat_Zero_Data_Volume_MB', 'ccc_Device_delivery_repair_Device_repair', 'ccc_Tariff_management', 'ccc_New_adds_process', 'GNV_hour_21_WE_Chat_Zero_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_Mobile_Data_support', 'ccc_DSL_FIBER_incidences_and_support_DSL_support', 'ccc_Device_delivery_repair', 'GNV_hour_23_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_W_Chat_Zero_Data_Volume_MB', 'ccc_Device_upgrade_Device_upgrade_order', 'GNV_hour_15_W_Social_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Chat_Zero_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Universal_0', 'GNV_hour_8_WE_MOU', 'ccc_Churn_Cancellations_Churn_cancellations_process', 'ccc_Resultado_Solucionado', 'FX_PVR_TV', 'Camp_SRV_Up_Cross_Sell_RCS_Control_0', 'GNV_hour_3_WE_Num_Of_Calls', 'ccc_Device_delivery_repair_SIM', 'ccc_Tariff_management_Data_tariff', 'GNV_hour_23_W_Video_Pass_Num_Of_Connections', 'ccc_Incidencia_Resto', 'ccc_Resultado_No_Aplica', 'GNV_hour_16_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_22_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_5_W_RegularData_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_DSL_incidences', 'GNV_hour_5_WE_Social_Pass_Num_Of_Connections', 'GNV_hour_6_W_Social_Pass_Data_Volume_MB', 'ccc_Voice_and_mobile_data_incidences_and_support_TV', 'ccc_Resultado_Escalo', 'GNV_hour_4_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_7_W_MOU', 'ccc_Desactivacion_Movil', 'GNV_hour_23_W_RegularData_Data_Volume_MB', 'FX_FOOTBALL_TV', 'Camp_NIFs_Retention_Voice_SAT_Universal_0', 'ccc_Desactivacion_USB', 'ccc_Productos_Resto', 'ccc_DSL_FIBER_incidences_and_support', 'ccc_Voice_and_mobile_data_incidences_and_support_Modem_Router_support', 'GNV_hour_8_WE_RegularData_Data_Volume_MB', 'GNV_hour_18_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_6_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Terceros_TER_Control_0', 'GNV_hour_2_WE_Num_Of_Calls', 'GNV_hour_7_WE_MOU', 'ccc_Baja', 'GNV_hour_5_WE_RegularData_Data_Volume_MB', 'GNV_hour_4_WE_Num_Of_Calls', 'GNV_hour_22_WE_RegularData_Data_Volume_MB', 'ccc_Resultado_Informacion', 'GNV_hour_18_WE_RegularData_Data_Volume_MB', 'Camp_NIFs_Up_Cross_Sell_MLT_Universal_0', 'GNV_hour_4_W_Num_Of_Calls', 'ccc_DSL_FIBER_incidences_and_support_Referrals', 'Camp_SRV_Up_Cross_Sell_SMS_Universal_0', 'GNV_hour_5_WE_Num_Of_Calls', 'GNV_hour_15_WE_Social_Pass_Data_Volume_MB', 'ccc_DSL_FIBER_incidences_and_support_FIBRA_incidences', 'ccc_Pagar_menos', 'GNV_hour_1_WE_Num_Of_Calls', 'GNV_hour_1_W_Num_Of_Calls', 'ccc_Prepaid_balance', 'prepaid_services', 'Camp_NIFs_Delight_SMS_Control_0', 'GNV_hour_0_W_MOU', 'GNV_hour_1_WE_MOU', 'GNV_hour_3_W_Num_Of_Calls', 'GNV_hour_7_WE_Chat_Zero_Data_Volume_MB', 'GNV_hour_1_W_MOU', 'GNV_hour_21_WE_RegularData_Data_Volume_MB', 'GNV_hour_5_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_0_WE_MOU', 'ccc_Product_and_Service_management_Contracted_products_DSL', 'FX_TV_CUOTA_ALTA', 'GNV_hour_11_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_17_WE_Social_Pass_Data_Volume_MB', 'ccc_Other_customer_information_management_Transfers', 'GNV_hour_5_W_Num_Of_Calls', 'bam_fx_first', 'ccc_Collections_Transfers', 'x_antiguedad_cuenta', 'GNV_hour_2_W_Num_Of_Calls', 'GNV_hour_15_WE_Chat_Zero_Data_Volume_MB', 'ccc_Tariff_management_Voice', 'ccc_Incidencia_Provision_Neba', 'ccc_Product_and_Service_management_Billing_errors', 'Camp_SRV_Up_Cross_Sell_RCS_Target_0', 'GNV_hour_5_W_Social_Pass_Data_Volume_MB', 'ccc_Churn_Cancellations_TV', 'GNV_hour_6_WE_MOU', 'ccc_Other_customer_information_management_Customer_data', 'GNV_hour_6_WE_Chat_Zero_Data_Volume_MB', 'ccc_Resultado_Abono', 'ccc_New_adds_process_Referrals', 'ccc_DSL_FIBER_incidences_and_support_TV', 'ccc_New_adds_process_Voice_line', 'TV_TARIFF', 'ccc_Churn_Cancellations_Transfers', 'ccc_Transferencia', 'ccc_Incidencia_Provision_DSL', 'GNV_hour_23_WE_Chat_Zero_Data_Volume_MB', 'ccc_Desactivacion_BA_Movil_TV', 'ccc_Product_and_Service_management_TV', 'ENCUESTAS', 'ccc_DSL_FIBER_incidences_and_support_Transfers', 'ccc_Averia_Fibra', 'ccc_Voice_and_mobile_data_incidences_and_support_Transfers', 'X_IDIOMA_FACTURA', 'GNV_hour_6_W_MOU', 'PRICE_DECO_TV', 'ccc_New_adds_process_Transfers', 'GNV_hour_2_WE_MOU', 'ccc_DSL_FIBER_incidences_and_support_Modem_Router_incidences', 'ccc_DSL_FIBER_incidences_and_support_Network', 'PVR_TV', 'PRICE_FOOTBALL_TV', 'GNV_hour_2_W_MOU', 'bam_services', 'ccc_Resultado_Transferencia', 'GNV_hour_6_WE_Social_Pass_Data_Volume_MB', 'GNV_hour_4_W_MOU', 'ccc_Prepaid_balance_Transfers', 'ccc_Incidencia_Tecnica', 'FOOTBALL_TV', 'ccc_Provision_Neba', 'GNV_hour_0_W_Video_Pass_Num_Of_Connections', 'GNV_hour_3_WE_MOU', 'ccc_Incidencia_Provision_Movil', 'ccc_Voice_and_mobile_data_incidences_and_support_Referrals', 'ccc_Provision_Fibra', 'GNV_hour_3_W_MOU', 'GNV_hour_5_W_MOU', 'ccc_Product_and_Service_management_Voice_tariff', 'NUM_SERIE_DECO_TV', 'DTO_LEV2', 'TV_CUOTA_ALTA', 'x_datos_trafico', 'FX_DTO_LEV2', 'ccc_New_adds_process_TV', 'GNV_hour_4_WE_MOU', 'ccc_Incidencia_Provision_Fibra', 'ccc_Other_customer_information_management_Simlock', 'ccc_Ofrecimiento', 'x_datos_navegacion', 'GNV_hour_5_WE_MOU', 'ccc_Provision_DSL', 'PRICE_DTO_LEV2', 'ccc_Churn_Cancellations_Referrals', 'ccc_Device_upgrade_Simlock', 'ccc_Precios', 'ccc_Consulta_Tecnica_DSL', 'ccc_Churn_Cancellations_Network', 'ccc_Product_and_Service_management_Referrals', 'CLASE_CLI_COD_CLASE_CLIENTE', 'FX_NETFLIX_NAPSTER', 'NETFLIX_NAPSTER', 'FACTURA_CATALAN', 'GNV_hour_16_W_Video_Pass_Num_Of_Connections', 'GNV_hour_15_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Video_Pass_Data_Volume_MB', 'GNV_hour_21_W_Video_Pass_Num_Of_Connections', 'GNV_hour_2_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_Music_Pass_Num_Of_Connections', 'GNV_hour_4_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_3_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_11_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_Music_Pass_Num_Of_Connections', 'GNV_hour_14_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_13_W_Video_Pass_Num_Of_Connections', 'GNV_hour_20_W_Video_Pass_Num_Of_Connections', 'GNV_hour_17_W_Video_Pass_Num_Of_Connections', 'GNV_hour_7_W_Video_Pass_Num_Of_Connections', 'GNV_hour_21_W_Music_Pass_Data_Volume_MB', 'GNV_hour_21_W_Music_Pass_Num_Of_Connections', 'GNV_hour_9_W_Video_Pass_Num_Of_Connections', 'GNV_hour_14_W_Music_Pass_Data_Volume_MB', 'GNV_hour_18_W_Video_Pass_Num_Of_Connections', 'GNV_hour_19_W_Music_Pass_Num_Of_Connections', 'GNV_hour_4_W_Video_Pass_Num_Of_Connections', 'GNV_hour_9_W_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_22_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_0_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_14_W_Video_Pass_Num_Of_Connections', 'GNV_hour_13_W_Video_Pass_Data_Volume_MB', 'GNV_hour_1_W_Video_Pass_Num_Of_Connections', 'GNV_hour_7_W_Music_Pass_Data_Volume_MB', 'GNV_hour_5_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_Video_Pass_Num_Of_Connections', 'GNV_hour_6_W_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_12_W_Video_Pass_Num_Of_Connections', 'GNV_hour_12_W_Video_Pass_Data_Volume_MB', 'GNV_hour_9_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_15_W_Music_Pass_Data_Volume_MB', 'GNV_hour_17_W_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_10_W_Music_Pass_Data_Volume_MB', 'GNV_hour_2_W_Video_Pass_Data_Volume_MB', 'GNV_hour_6_W_Video_Pass_Data_Volume_MB', 'GNV_hour_1_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_16_W_Music_Pass_Num_Of_Connections', 'GNV_hour_12_W_Music_Pass_Num_Of_Connections', 'GNV_hour_21_W_Video_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_0_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_Music_Pass_Data_Volume_MB', 'GNV_hour_4_W_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_21_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_4_W_Video_Pass_Data_Volume_MB', 'GNV_hour_3_W_Video_Pass_Num_Of_Connections', 'GNV_hour_2_W_Music_Pass_Num_Of_Connections', 'GNV_hour_22_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_14_W_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_12_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_21_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_10_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_9_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_16_W_Music_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_6_W_Music_Pass_Data_Volume_MB', 'GNV_hour_15_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_W_Music_Pass_Data_Volume_MB', 'GNV_hour_14_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_0_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_11_W_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_15_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_2_W_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_W_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_7_W_Music_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_15_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_9_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_6_W_Video_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_W_Music_Pass_Num_Of_Connections', 'GNV_hour_21_W_MasMegas_Num_Of_Connections', 'GNV_hour_4_W_Music_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_19_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_16_W_Video_Pass_Data_Volume_MB', 'GNV_hour_15_W_MasMegas_Num_Of_Connections', 'GNV_hour_21_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_19_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_19_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_17_W_Music_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_13_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_9_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_15_W_Video_Pass_Data_Volume_MB', 'GNV_hour_0_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_23_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_16_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_0_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_10_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_W_MasMegas_Num_Of_Connections', 'GNV_hour_10_W_Music_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_8_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_23_W_Video_Pass_Data_Volume_MB', 'GNV_hour_18_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_22_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_10_W_Video_Pass_Num_Of_Connections', 'GNV_hour_3_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_8_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_1_W_Music_Pass_Num_Of_Connections', 'GNV_hour_6_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_19_W_Music_Pass_Data_Volume_MB', 'GNV_hour_9_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_8_W_Video_Pass_Num_Of_Connections', 'GNV_hour_18_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_13_W_Music_Pass_Data_Volume_MB', 'GNV_hour_8_W_Video_Pass_Data_Volume_MB', 'GNV_hour_19_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_2_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_22_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_18_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_4_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_0_W_Music_Pass_Data_Volume_MB', 'GNV_hour_21_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_14_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_19_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_11_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_16_W_MasMegas_Num_Of_Connections', 'GNV_hour_14_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_12_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_1_W_Music_Pass_Data_Volume_MB', 'GNV_hour_17_W_MasMegas_Num_Of_Connections', 'GNV_hour_23_W_Music_Pass_Data_Volume_MB', 'GNV_hour_6_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_3_W_Music_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Maps_Pass_Num_Of_Connections', 'GNV_hour_13_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_16_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_10_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_12_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_12_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_22_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_4_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_20_W_Video_Pass_Data_Volume_MB', 'GNV_hour_20_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_11_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_4_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_11_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_10_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_5_W_Video_Pass_Num_Of_Connections', 'GNV_hour_5_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_23_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_11_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_8_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_8_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_14_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_10_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_13_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_10_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_13_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_10_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_17_W_Video_Pass_Data_Volume_MB', 'GNV_hour_7_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_8_WE_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_22_W_Music_Pass_Num_Of_Connections', 'GNV_hour_5_W_Video_Pass_Data_Volume_MB', 'GNV_hour_7_WE_Video_Pass_Data_Volume_MB', 'GNV_hour_20_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_15_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_13_W_Music_Pass_Num_Of_Connections', 'GNV_hour_18_W_Maps_Pass_Data_Volume_MB', 'GNV_hour_18_W_Music_Pass_Num_Of_Connections', 'GNV_hour_20_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_19_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_15_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_11_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_7_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_3_W_Music_Pass_Data_Volume_MB', 'GNV_hour_20_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_13_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_16_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_18_W_VideoHD_Pass_Data_Volume_MB', 'FX_MOTOR_TV', 'GNV_hour_8_WE_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_W_Music_Pass_Data_Volume_MB', 'GNV_hour_3_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_9_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_1_W_Video_Pass_Data_Volume_MB', 'GNV_hour_17_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_13_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_11_W_Music_Pass_Data_Volume_MB', 'GNV_hour_7_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_6_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_8_W_Maps_Pass_Num_Of_Connections', 'GNV_hour_0_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_1_WE_Music_Pass_Data_Volume_MB', 'GNV_hour_12_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_14_WE_Video_Pass_Num_Of_Connections', 'GNV_hour_8_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_W_Video_Pass_Data_Volume_MB', 'GNV_hour_18_W_MasMegas_Num_Of_Connections', 'GNV_hour_13_WE_Music_Pass_Num_Of_Connections', 'GNV_hour_16_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_18_W_Music_Pass_Data_Volume_MB', 'GNV_hour_12_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_22_W_VideoHD_Pass_Data_Volume_MB', 'GNV_hour_23_WE_Maps_Pass_Data_Volume_MB', 'GNV_hour_17_W_VideoHD_Pass_Num_Of_Connections', 'GNV_hour_6_WE_Video_Pass_Num_Of_Connections', 'x_user_facebook', 'MOTOR_TV', 'x_tipo_cuenta_corp', 'TRYBUY_TV']
        #self.train_cols = self.train_cols[:num_cols]

        # if False:
        #     # Column counts
        #     print 'Column distinct counts:'
        #     columns_to_count = list(set(self.train_cols) - set(['class', 'pred']))
        #     columns_to_count.sort()
        #     column_counts = {}
        #     for idx, c in enumerate(columns_to_count):
        #         column_counts[c] = all_data.select(c).distinct().count()
        #         print 'Column', str(idx+1) + '/' + str(len(columns_to_count)), c, '=', column_counts[c]
        #     print 'Column distinct counts:', column_counts, '\n'
        #     #for c in ['ZAPPER_TV', 'FX_TV_ABONOS', 'PRICE_ZAPPER_TV', 'PRICE_TV_ABONOS', 'TV_PROMO', 'FX_TV_PROMO', 'fbb_services', 'FX_TV_CUOT_CHARGES', 'PRICE_TRYBUY_AUTOM_TV', 'PRICE_TV_PROMO', 'is_convergent', 'fbb_fx_first', 'TV_ABONOS', 'FLG_LORTAD', 'TV_CUOT_CHARGES', 'FX_TRYBUY_AUTOM_TV', 'FX_TV_PROMO_USER', 'FX_ZAPPER_TV', 'TV_PROMO_USER', 'is_mobile_only', 'PRICE_TV_PROMO_USER', 'TRYBUY_AUTOM_TV', 'PRICE_TV_CUOT_CHARGES']:
        #     #    column_counts[c] = 0

        #     # Remove columns that do not have at least two distinct values
        #     dumb_columns = list({k for (k, v) in column_counts.items() if v < 2})
        #     print 'Removing columns that does not have at least two distinct values =', len(dumb_columns), ':', \
        #         dumb_columns, '\n'
        #     self.train_cols = list(set(self.train_cols) - set(dumb_columns))

        train_target_cols = self.train_cols
        for c in ['class', 'pred']:
            if c not in train_target_cols:
                train_target_cols = train_target_cols+[c]
        #print train_target_cols

        all_data.groupBy(['class', 'pred']).count().orderBy('class').show()

        #train_df = None
        #while train_df is None:
        #    try:

        #self.train_cols = self.train_cols[:num_cols]

        # plan_indexer = StringIndexer(inputCol = 'CLASS', outputCol = 'CLASS_Indexed')
        # labeller = plan_indexer.fit(cvm_pospago_df)

        #########################
        # # Select rows with at least one null or blank in any column of that row
        #
        # # Compose the appropriate raw SQL query
        # self.app.spark.setCheckpointDir('/tmp/checkpoint/')
        # df = all_data.select(['nif']+train_target_cols+['pred'])
        # sql_query_base = 'SELECT * FROM df WHERE '
        # sql_query_apps = ['{} IS NULL'.format(col_name) for col_name in df.columns]
        # # sql_query_apps = ['{} == ""'.format(col_name) for col_name in df.columns]
        # # sql_query_apps = ['{} IS NULL OR {} == ""'.format(col_name, col_name) for col_name in df.columns]
        # sql_query = sql_query_base + ' OR '.join(sql_query_apps)
        # sql_query
        #
        # # Register the dataframe as a SQL table
        # self.app.spark.registerDataFrameAsTable(df, 'df')
        #
        # # Apply raw SQL
        # self.app.spark.sql(sql_query).show()
        #########################

        # FIXME
        train_orig = all_data.select(['NIF_CLIENTE']+train_target_cols)
        #print train_orig.columns
            #.na.drop()
            #.fillna(0, train_target_cols).replace('', 'NA')  # FIXME Do proper cleaning. Deberia ir en generateBalancedData()
        #train_orig = train_orig.repartition(200)

        categorical_columns = [v[0] for v in train_orig.dtypes if (not v[0] in self.id_cols) and (v[1] == 'string')]
        numeric_columns     = [v[0] for v in train_orig.dtypes if (not v[0] in self.id_cols) and (v[1] in ['decimal', 'double', 'float', 'bigint', 'int', 'smallint', 'tinyint'])]
        train_orig = train_orig.fillna('<NULL>', categorical_columns)
        train_orig = train_orig.fillna(-1, train_target_cols)


        #########################
        # Select rows with at least one null or blank in any column of that row

        # Compose the appropriate raw SQL query
        # self.app.spark.setCheckpointDir('/tmp/checkpoint/')
        # df = train_orig
        # sql_query_base = 'SELECT * FROM df WHERE '
        # sql_query_apps = ['{} IS NULL'.format(col_name) for col_name in df.columns]
        # # sql_query_apps = ['{} == ""'.format(col_name) for col_name in df.columns]
        # # sql_query_apps = ['{} IS NULL OR {} == ""'.format(col_name, col_name) for col_name in df.columns]
        # sql_query = sql_query_base + ' OR '.join(sql_query_apps)
        # sql_query
        #
        # # Register the dataframe as a SQL table
        # self.app.spark.registerDataFrameAsTable(df, 'df')
        #
        # # Apply raw SQL
        # self.app.spark.sql(sql_query).show()
        #########################

        # #formula = "class ~ " + ' + '.join([x for x in self.train_cols if x != 'class'])
        # formula = "class ~ ."
        # #print formula
        # rformula = RFormula(formula=formula, featuresCol='features', labelCol='label')
        # print '['+time.ctime()+']', 'Fitting & Transforming RFormula with', len(self.train_cols), 'variables ...'
        # train_orig.cache()
        # train_df = rformula.fit(train_orig).transform(train_orig)
        # # train_df.show()
        # train_df = train_df.select(['nif', 'features', 'label', 'pred'])

        #    except IllegalArgumentException as e:
        #        print '\tpyspark.sql.utils.IllegalArgumentException:', e
        #        num_cols = num_cols - 1

        return train_orig#, self.mobile_only

    @staticmethod
    def filter_predictions_oracle(predictions, mobile_only):
        joined = predictions.join(mobile_only, 'nif', 'left_outer')

        # Filter clients according to CVM - Campaigns & Analytics (CBU) business rules

        # Without ADSL or Fibre -> This is already filtered by DPPrepareInputCvmPospago

        # With fingerprint of something
        # if "max_flag_huella_ono" not in joined.columns:
        #     joined = joined.withColumn('max_flag_huella_ono', lit(0))
        # if "max_flag_huella_vf" not in joined.columns:
        #     joined = joined.withColumn('max_flag_huella_vf', lit(0))
        if "max_flag_huella_neba" not in joined.columns:
            joined = joined.withColumn('max_flag_huella_neba', lit(0))
        if "max_flag_huella_ariel" not in joined.columns:
            joined = joined.withColumn('max_flag_huella_ariel', lit(0))
        # No indica que el cliente esté en cobertura, sino que si está en cobertura (con el resto de flags) si lo está
        # en zona competitiva o no, vamos que hay clientes con este flag a 1 pero que no están en huella
        # if "max_flag_zona_competitiva" not in joined.columns:
        #     joined = joined.withColumn('max_flag_zona_competitiva', lit(0))
        if "flag_cobertura_adsl" not in joined.columns:
            joined = joined.withColumn('flag_cobertura_adsl', lit(""))
        # joined.groupby('FLAG_COBERTURA_ADSL').count().show()
        joined = joined.where('max_flag_huella_ono = 1 ' +
                              'OR max_flag_huella_vf = 1 '
                              'OR max_flag_huella_neba = 1 '
                              'OR max_flag_huella_ariel = 1 '
                              'OR flag_cobertura_adsl = "D"')

        # Without HZ
        # joined.groupby('FLAGHZ').count().show()
        joined = joined.where('sum_flaghz = 0')

        # Without Lortad
        # joined.groupby('max_LORTAD').count().show()
        joined = joined.where('max_lortad = 0')

        # Without debt
        # joined.groupby('max_DEUDA').count().show()
        joined = joined.where('max_deuda = 0')

        # With voice active
        # joined.groupby('part_status').count().show()
        joined = joined.where('part_status = "AC"')

        # TODO: Load Ids shared with Ono stack from SUN_INFO_CRUCE
        # Not shared with Ono stack (using SUN_INFO_CRUCE)
        # dt.MobileOnly <- dt.MobileOnly[!nif %in% dt_SUN_INFO_CRUCE$NIF]

        return joined.select(['nif', 'score']).orderBy('score', ascending=False)

    def run(self, do_generate_data=True, do_train_model=True):
        print 'do_generate_data =', do_generate_data
        print 'do_train_model =', do_train_model

        #####################################
        # Generate Train & Predict datasets #
        #####################################

        all_data = None
        # if do_generate_data:
        #     #(all_data, mobile_only) = self.generate_train_predict_data(self.model_month, self.prediction_month)
        #     all_data = self.generate_train_predict_data(self.model_month, self.prediction_month)
        #     #print len(all_data.columns), 'columns'
        #     #all_data.groupBy(['class', 'pred']).count().orderBy('class').show()

        if do_train_model:
            ############
            # Training #
            ############

            all_data = self.select_model_features(all_data)
            #print len(all_data.columns), 'columns'
            #all_data.groupBy(['class', 'pred']).count().orderBy('class').show()

            if not self.oracle:
                self.id_cols = self.id_cols + [x for x in all_data.columns if (x.startswith('Order_') or x.startswith('orders_'))]

            # Ignore categorical columns to speed up training
            self.id_cols = self.id_cols + [item[0] for item in all_data.dtypes if (item[1] == 'string')]

            self.modeller = GeneralModelTrainer(all_data, 'class', self.id_cols)
            #self.modeller.categorical_columns = self.modeller.categorical_columns[:3]
            #self.modeller.relevelCategoricalVars()

            cols_to_relevel = {}
            if self.oracle:
                cols_to_relevel = {'x_plan': [u'TREDM', u'TSMAS', u'TREDL', u'TMINS', u'2LMXS', u'MEYTA', u'SYUTA', u'TARRM', u'2L3RM', u'TRMPL', u'V6TAR', u'RSTAR', u'TREXL', u'2LINS', u'TARSS', u'B4TAR', u'TARMS', u'TARXS', u'V15TA', u'BMTAR', u'SSTAR', u'2L1PM', u'TMIN0', u'TRLPL', u'2LIRL', u'TARSM', u'PCV12', u'RLTAR', u'SMTAR', u'R1TAR', u'B2TAR'],
                                   'promocion_vf': [u'NA', u'CPP24000', u'COLPAPLZ', u'CAPVFPAPLZ', u'TOLPAPLZ', u'D121215', u'D181840', u'LINADIPAPL', u'CPP12069', u'CRUPAPLZ', u'RETEN24000', u'PROM5012CP', u'CPP12036', u'D121230', u'MIGVFPAPLZ', u'PRMTOL20', u'PROM0650CP', u'CAPSIM15MC', u'COLSINTER', u'CGRPAPLZ', u'PRMTOL20_6', u'RETEN24200', u'OFEXPAPLZ', u'CPP24200', u'D121210', u'RETEN18100', u'VFCASA1210', u'OFEXSINTER', u'D181830', u'RETM5024CP', u'RETM5012CP'],
                                   'codigo_postal': [u'10409', u'NA', u'73939', u'48467', u'16924', u'34890', u'11694', u'25457', u'48242', u'65679', u'39310', u'96175', u'78849', u'86008', u'44205', u'12222', u'47751', u'84194', u'96320', u'80366', u'28729', u'40455', u'79163', u'24283', u'79464', u'42712', u'72660', u'71311', u'27750', u'31911', u'20664'],
                                   'pprecios_destino': [u'TREDM', u'NA', u'TSMAS', u'TREDL', u'TMINS', u'MEYTA', u'TRMPL', u'TARRM', u'V6TAR', u'SYUTA', u'RSTAR', u'TREXL', u'2L3RM', u'B4TAR', u'TARXS', u'TARSS', u'TARMS', u'2LINS', u'V15TA', u'TRLPL', u'SSTAR', u'TMIN0', u'BMTAR', u'TARSM', u'2LIRL', u'RLTAR', u'PCV12', u'SMTAR', u'R1TAR', u'TARRL', u'B2TAR'],
                                   'plandatos': [u'IREDM', u'ISMAS', u'IREDL', u'IMINS', u'I2MXS', u'IPDMY', u'IPDSY', u'IPRM1', u'IP2RM', u'IPRMP', u'IPTVZ', u'IPTRS', u'IREXL', u'I2LIS', u'IPSS1', u'IPTB4', u'IPMS1', u'IPTB0', u'IPSX1', u'IPTBM', u'IPTSS', u'TPMIN', u'IP2L1', u'IPRLP', u'I2LRL', u'IPSM1', u'IPC12', u'IPTRL', u'IPTSM', u'IPTR1', u'IPTB2'],
                                   'sistema_operativo': [u'Android 7', u'Android 6', u'iOS 10', u'Android 6.0.1', u'NA', u'Android 8', u'Android 5', u'Android 5.1.1', u'iOS 11', u'Android 5.1', u'Android 4.4.4', u'iOS 8', u'Android 5.0.2', u'Android 7.1', u'Android 4.4.2', u'iOS 11.1.2', u'iOS 9', u'Android 4.2.2', u'iOS 7', u'iOS 10.0', u'Android 7.1.2', u'N/A N.A', u'Android 7.1.1', u'iOS 9.3', u'Android 4.4', u'Android 8.1', u'Android 4.1.2', u'Android 4.3', u'Proprietary N.A', u'iOS 6', u'Android 4.2'],
                                   'ppid_destino': [u'NA', u'NVMIN', u'TREDM', u'TSMAS', u'NTARD', u'SYUTA', u'NVTOD', u'TMINS', u'PTSEG', u'UNIXS', u'TARDP', u'TARSS', u'PTMIN', u'XS8NF', u'UIXS2', u'V15TA', u'MEYTA', u'BMTAR', u'TOIXS', u'ILOC1', u'RSTAR', u'TTYF1', u'TJDLC', u'SSTAR', u'TARRM', u'B2TAR', u'TJDTO', u'TJD24', u'V6TAR', u'TARMS', u'UNLIM'],
                                   'modelo': [u'Apple iPhone 7', u'Apple iPhone 6S', u'Samsung Galaxy S7 Edge', u'Apple iPhone 7 Plus', u'Apple iPhone 6', u'Apple iPhone X', u'Samsung Galaxy S8', u'Huawei P8 lite 2017', u'Samsung Galaxy S7', u'Huawei P8 Lite', u'Samsung Galaxy J5 2016', u'Apple iPhone 8', u'Samsung Galaxy S8+', u'Apple iPhone 8 Plus', u'Samsung Galaxy J3', u'Samsung Galaxy S6', u'Apple iPhone 6S Plus', u'Samsung Galaxy J7 2016', u'Samsung Galaxy S9+', u'Samsung Galaxy A5 2017', u'Huawei P9 Lite', u'Samsung Galaxy Note 8', u'Huawei Y6 2017', u'Huawei P10', u'Apple iPhone 5S', u'Apple iPhone SE', u'Samsung Galaxy J5', u'Huawei P9', u'Samsung Galaxy J5 2017', u'NA', u'Huawei P20 Lite'],
                                   'x_nacionalidad': [u'Espa\xf1a', u'Antarctica', u'Rumania', u'Marruecos', u'Ecuador', u'No Comunitaria', u'Colombia', u'Gran Breta\xf1a', u'Italia', u'Paraguay', u'Bolivia', u'Venezuela', u'Portugal', u'Bulgaria', u'Per\xfa', u'Brasil', u'Rep.Dominicana', u'Alemania', u'Argentina', u'Francia', u'Ukrania', u'Honduras', u'Cuba', u'Rusia Blanca', u'China', u'Nicaragua', u'Polonia', u'Holanda', u'Senegal', u'Uruguay', u'Estados Unidos']
                                  }
            else:
                cols_to_relevel = '/var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/cols_to_relevel-20181029132240.txt'
            cols_to_relevel = None

            self.modeller.relevelCategoricalVars(cols=None)
            #self.modeller.relevelCategoricalVars(cols=cols_to_relevel)
            #self.modeller.relevelCategoricalVars(cols='cols_to_relevel-20190410165803.txt')
            self.modeller.generateFeaturesVector(ohe_columns=True, do_checkpoint=False)
            self.modeller.save(filename='convergence-'+str(self.model_month)+'-'+str(self.prediction_month)+'.bin',
                               df_path='/data/udf/vf_es/convergence_data/all_data_featured-'+str(self.model_month)+'-'+str(self.prediction_month),
                               model_path=None,
                               preds_path=None)
            #self.modeller.debug = True
            self.modeller.split_train_test(seed=1234)
            self.modeller.train(seed=1234)
            #self.modeller.debug = False
            print self.modeller.datasource_importance()
            self.modeller.evaluate()
            #general_trainer = GeneralModelTrainer()
            #model, all_preds = general_trainer.train(all_data_rf)
            # all_preds.select(['nif', labelCol, 'prediction', 'score']).write.format('com.databricks.spark.csv')
            # .options(header='true').save('file:///Users/bbergua/preds-allinput-'+model_month)
            self.modeller.save(filename='convergence-'+str(self.model_month)+'-'+str(self.prediction_month)+'.bin',
                               df_path=None,
                               model_path='/data/udf/vf_es/convergence_data/model-'+str(self.model_month)+'-'+str(self.prediction_month),
                               preds_path=None)

            # Since prediction_month is last day of month, the data will be partitioned in prediction_month + 1 month
            partition_year = int(str(self.prediction_month)[:4])
            partition_month = int(str(self.prediction_month)[4:6]) + 1
            if partition_month > 12:
                partition_year = partition_year + 1
                partition_month = 1

            self.model_params = ModelParameters(self.app.spark,
                                                'convergence',
                                                partition_year, partition_month, 0, time=0,
                                                model_level='nif',
                                                training_closing_date=str(self.model_month),
                                                target='Difference of mobile-only clients who became convergent, two months later')
            self.model_params.extract_from_gmt(self.modeller)
            self.model_params.insert(use_common_path=False, overwrite=True)
            self.model_params.insert(use_common_path=True)
            #return

        else:
            # TODO: Fix paths and columns list
            self.modeller = GeneralModelTrainer.load(self.app.spark,
                filename='convergence-20190930.bin',
                df_path='/tmp/bbergua/convergence_data/all_data-20190930/parquet/')
            self.modeller.df = self.modeller.df.where(col('pred') == True)
            self.modeller.label_col = 'class'
            new_label = self.modeller.label_col+'_i'
            print('Converting label', self.modeller.label_col, 'from', self.modeller.label_type, 'to int (%s)' % new_label)
            self.modeller.df = (self.modeller.df.withColumn(new_label, self.modeller.df[self.modeller.label_col].cast(IntegerType()))
                                                .withColumn("TNPS_VDN_20038", lit(-1))
                                                .withColumn("TNPS_VDN_10416", lit(-1))
                                                .withColumn("TNPS_VDN_10346", lit(-1)))
            self.modeller.label_col_orig = self.modeller.label_col
            self.modeller.label_col = new_label
            self.modeller.df_orig = modeller.df
            self.modeller.relevelCategoricalVars(cols='cols_to_relevel-20190410165803.txt')
            self.modeller.generateFeaturesVector(ohe_columns=True, do_checkpoint=False)


        ##############
        # Prediction #
        ##############

        #self.modeller.set_debug(True)
        self.modeller.predict()
        self.predictions = self.modeller.predictions
        # cols_to_select = ['nif', 'NIF_CLIENTE', 'rawPrediction', 'probability', 'prediction', 'score', 'partitioned_month', 'year', 'month', 'day', 'CAMPO1']
        # cols_to_select = filter(lambda x: x in self.predictions.columns, cols_to_select)
        # if self.oracle:
        #     self.predictions = self.predictions.select(cols_to_select).withColumn('Stack', lit('Oracle'))
        # else:
        #     self.predictions = self.predictions.select(cols_to_select).withColumn('Stack', lit('Amdocs'))
        self.predictions = self.predictions.repartition(1)
        #self.modeller.set_debug(False)
        #predictions = general_trainer.predict(model, all_data_rf)
        print('['+time.ctime()+']', 'Caching predictions data ...')
        self.predictions.cache()
        print('['+time.ctime()+']', 'Caching predictions data ... done')
        #print self.predictions.columns

        self.modeller.save(filename='convergence-'+str(self.model_month)+'-'+str(self.prediction_month)+'.bin',
                           df_path=None,
                           model_path=None,
                           preds_path='/data/udf/vf_es/convergence_data/predictions-'+str(self.model_month)+'-'+str(self.prediction_month)
                           )

        self.model_scores = ModelScores(self.app.spark,
                                        'convergence',
                                        partition_year, partition_month, 0, time=0,
                                        predict_closing_date=str(self.prediction_month))
        self.model_scores.extract_from_gmt(self.modeller)
        for c in ['msisdn', 'client_id']:
            self.model_scores.df = self.model_scores.df.withColumn(c, lit('')) # Cannot write a column with all nulls
        self.model_scores.insert(use_common_path=False, overwrite=True)
        self.model_scores.insert(use_common_path=True)

        #return

        #self.predictions = self.filter_predictions_oracle(self.predictions, mobile_only)
        if self.app.is_bdp:
            ofile = '/data/udf/vf_es/convergence_data/predictions-' + str(self.model_month)+'-'+str(self.prediction_month)
            if self.oracle:
                ofile = '/data/udf/vf_es/convergence_data/predictions_oracle-' + str(self.prediction_month)

            # print '['+time.ctime()+']', 'Saving predictions in parquet to', 'hdfs://'+ofile+'/parquet', '...'
            # self.predictions.repartition(1)\
            #                 .write.save(ofile+'/parquet'
            #                            , format='parquet'
            #                            , mode='overwrite'
            #                            #, partitionBy=['partitioned_month', 'year', 'month', 'day']
            #                            )
            # print '['+time.ctime()+']', 'Saving predictions in parquet to', 'hdfs://'+ofile+'/parquet', '... done'
            # Read with: df = spark.read.load('/user/bbergua/convergence_predictions-' + self.prediction_month)

            print '['+time.ctime()+']', 'Saving predictions  in csv     to', 'hdfs://'+ofile+'_csv', '...'
            if self.oracle:
                preds = self.predictions.select('nif', 'score')
            else:
                preds = self.predictions.select('NUM_CLIENTE', 'score').withColumnRenamed('NUM_CLIENTE', 'NUM_CLIENTE_INT').na.drop(subset=['NUM_CLIENTE_INT'])
            # TODO: Quitar nulos
            preds = preds.coalesce(1)
            preds.orderBy('score', ascending=False)\
                             .write.save(ofile+'_csv'
                                        , format='csv'
                                        , header=True
                                        , sep='|'
                                        , mode='overwrite'
                                        #, partitionBy=['partitioned_month', 'year', 'month', 'day']
                                        )
            print '['+time.ctime()+']', 'Saving predictions  in csv     to', 'hdfs://'+ofile+'_csv', '... done'

            # TODO: Inserto into table?

            # Also, save predictions locally
            # self.predictions.map(x => x.mkString("|")).saveAsTextFile("file.csv")
            # self.predictions.write.mode('overwrite').format('json').options(header='true') \
            #                 .save('file:///home/bbergua/convergence_predictions-' + self.prediction_month)

            # impala-shell -i vgddp354hr.dc.sedc.internal.vodafone.com -q 'use tests_es; show tables;'
        else:
            self.predictions.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
                            .save('/data/udf/vf_es/convergence_data/convergence-predictions-' + str(self.prediction_month))

    @staticmethod
    def test(spark):
        # In IPython:
        #   %load_ext autoreload
        #   %autoreload 2
        #
        # import sys
        # reload(sys.modules['GeneralModelTrainer'])
        # reload(sys.modules['convergence'])
        # from GeneralModelTrainer import GeneralModelTrainer
        # from convergence import Convergence
        from configuration import Configuration
        from convergence import Convergence
        # from GeneralModelTrainer import GeneralModelTrainer

        # train_cols = ['part_status', 'x_sexo', 'x_tipo_ident', 'x_nacionalidad', 'COD_SEGFID']

        conf2 = Configuration(spark)

        converg = Convergence(conf2)
        converg.run('201803', '201804')


if __name__ == "__main__":
    # if '/var/SP/data/home/bbergua/fy17.capsule/utils/src/main/python/general_model_trainer' not in sys.path:
    #     print 'Adding [...]/utils/src/main/python/general_model_trainer to sys.path'
    #     sys.path.append('/var/SP/data/home/bbergua/fy17.capsule/utils/src/main/python/general_model_trainer')
    # else:
    #     print 'Already in sys.path [...]/utils/src/main/python/general_model_trainer to sys.path'

    # PYTHONIOENCODING=utf-8 ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 ~/IdeaProjects/convergence/src/main/python/convergence.py 201704 201705
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py 201803 201804 2>&1 | tee salida.convergence
    # tail -f -n +1 salida.convergence | grep -v $'..\/..\/.. ..:..:..\|^[\t]\+at\|java.io.IOException'

    # spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=500 --conf spark.driver.memory=16g --conf spark.executor.memory=16g amdocs_car_main.py --starting-day 20180401 --closing-day 20180430
    # spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=500 --conf spark.driver.memory=16g --conf spark.executor.memory=16g amdocs_car_main.py --starting-day 20180601 --closing-day 20180626
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G ~/fy17.capsule/convergence/src/main/python/DP_prepare_input_rbl_ids_srv.py --debug 201804 201806
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py --only-data 201804 201806 2>&1 | tee salida.convergence
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=8G ~/fy17.capsule/convergence/src/main/python/convergence.py --only-model 20180731 20180930 2>&1 | tee salida.convergence
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 16G --driver-memory 8G --conf spark.executor.cores=5 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py --only-model 20180831 20181031 > salida.convergence.0810
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 32G --driver-memory 16G --conf spark.executor.cores=5 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py 20180930 20181130 | tee salida.convergence.0911
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 64G --driver-memory 64G --conf spark.executor.cores=4 --conf spark.driver.maxResultSize=64G --conf spark.yarn.executor.memoryOverhead=32G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py 20181031 20181231 | tee salida.convergence.1012.nueva
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 32G --driver-memory 16G --conf spark.executor.cores=4 --conf spark.driver.maxResultSize=16G --conf spark.yarn.executor.memoryOverhead=16G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/convergence.py 20181130 20190131 | tee salida.convergence.1101
    parser = argparse.ArgumentParser(description='VF_ES CBU Convergence model',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    #parser.add_argument('stack', metavar='<stack>', type=str, help='Stack (Oracle/Amdocs)')
    parser.add_argument('model_month', metavar='<model-month>', type=int, help='Date (YYYYMM) to train the model')
    parser.add_argument('prediction_month', metavar='<prediction-month>', type=int,
                        help='Date (YYYYMM) to make the predictions')
    parser.add_argument('-t', '--only-data', action='store_true', help='Only generate train and predict datasets')
    parser.add_argument('-m', '--only-model', action='store_true', help='Only train model')
    parser.add_argument('-o', '--oracle', action='store_true', help='Use Oracle data')
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages')
    args = parser.parse_args()
    print 'args =', args
    print 'model_month =', args.model_month
    print 'prediction_month =', args.prediction_month
    print 'only_data =', args.only_data
    print 'only_model =', args.only_model
    print 'oracle =', args.oracle
    print 'debug =', args.debug

    if args.only_data and args.only_model:
        print 'ERROR: Cannot provide both --only-data and --only-model, just one of them, or none.'
        sys.exit(1)

    do_generate_data = True
    do_train_model = True
    if args.only_data:
        do_generate_data = True
        do_train_model = False
    if args.only_model:
        do_generate_data = False
        do_train_model = True

    if args.oracle:
        args.model_month = str(args.model_month)[:6]
        args.prediction_month = str(args.prediction_month)[:6]

    conf = Configuration()
    convergence = Convergence(conf, args.model_month, args.prediction_month, args.oracle, args.debug)
    convergence.run(do_generate_data, do_train_model)

    print '['+time.ctime()+']', 'Process finished!'
