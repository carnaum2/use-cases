#!/usr/bin/python
# -*- coding: utf-8 -*-

#SPARK_COMMON_OPTS+=" --queue root.datascience.normal "
#SPARK_COMMON_OPTS+=" --conf spark.port.maxRetries=50 "
#SPARK_COMMON_OPTS+=" --conf spark.network.timeout=10000000 "
#SPARK_COMMON_OPTS+=" --conf spark.executor.heartbeatInterval=60 "
#SPARK_COMMON_OPTS+=" --conf spark.yarn.executor.memoryOverhead=2G "
#SPARK_COMMON_OPTS+=" --conf spark.sql.broadcastTimeout=1200 "
#export SPARK_COMMON_OPTS
#spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/nps_estimation.py 2> /var/SP/data/home/bbergua/stderr.log 1> salida.nps
#tail -f -n +1 salida.raw | grep --color=auto -v -E '..\/..\/.. ..:..:..'

# sys.path.append('/home/bbergua/fy17.capsule/convergence/src/main/python/')
 
from configuration import Configuration
from DP_Call_Centre_Calls import DPCallCentreCalls
#from GeneralModelTrainer import GeneralModelTrainer
from DP_prepare_input_cvm_pospago import DPPrepareInputCvmPospago
import argparse
import re
import subprocess
import sys
import time
# from pyspark.ml.feature import StringIndexer
#from pyspark.ml.feature import RFormula
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, date_format, dayofmonth, format_string, from_unixtime, length, lit, lower, lpad, month, regexp_replace, struct, translate, trim, udf, unix_timestamp, year, when
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
#from pyspark.sql.types import BooleanType, FloatType, TimestampType
#import pyspark.sql.utils.IllegalArgumentException
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

class NpsEstimation:
    def __init__(self, spark):  #, model_month, prediction_month):
        self.spark = spark
        # self.model_month = model_month
        # self.prediction_month = prediction_month

        self.nps = None
        self.tnps = None
        self.tnps_pivoted = None
		
        self.use_vf_pos = True
        self.use_vf_pre = True
        self.use_ono = False
		
        self.vfpos_data = None
        self.vfpre_data = None
        self.ono_data = None
		
        self.ccc_msisdn = None
        self.ccc_id = None
		
        self.cem = None
        self.nps_cem = None
        self.vfpos_data_cem = None
        self.vfpre_data_cem = None
        self.ono_data_cem = None
		
        self.qt = None
        self.nps_qt = None
        self.vfpos_data_qt = None
        self.vfpre_data_qt = None
        self.ono_data_qt = None
		
        # self.netperform = None

        self.corr_npsi_dict = None
        self.corr_nps01_dict = None

	# def join_postpaid(self):
	    # # self.vf_data = self.spark.table('raw_es.vf_pos_ac_final')
	    # ifile = '/tmp/bbergua/vf_pos_ac_final_clean'
	    # self.vf_data = self.spark.read.load(ifile)
	
    def join_car(self):
        # There is only Accenture's CAR since 201701
        # months = self.nps.filter('partitioned_month >= "201701"').select('partitioned_month').distinct().sort('partitioned_month').rdd.flatMap(lambda x: x).collect()
        months = self.nps.select('partitioned_month').filter('partitioned_month >= "201701"').distinct().sort('partitioned_month').rdd.flatMap(lambda x: x).collect()
        months = map(str, months)
        print 'NPS months:', months

        # Vodafone Postpaid
        if self.use_vf_pos:
            print 'Loading VF Postpaid CAR ...'
        
            actual_months = []
            for m in months:
                try:
#                    if self.by_msisdn:
#                        tmp = self.spark.table('udf_es.par_explic_lin_6m_' + m)
#                    else:
				    tmp = self.spark.table('udf_es.par_explic_cli_6m_' + m)
                except AnalysisException as e:
    				print '\tFailed', m, ':', e
                else:
                    tmp = tmp.withColumn('partitioned_month', lit(m))
                    if 'partitioned_month_2' in tmp.columns:
                        tmp = tmp.drop('partitioned_month_2')
                    actual_months.append(m)
                    if self.vfpos_data is None:
                        self.vfpos_data = tmp
                    else:
                        self.vfpos_data = self.vfpos_data.union(tmp)
                    # self.vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
                    print '\tLoaded', m

            print 'Appending CAR prefix to columns ...'
            for c in self.vfpos_data.columns:
                if c not in ['nif', 'year', 'month', 'day', 'partitioned_month']:
                    self.vfpos_data = self.vfpos_data.withColumnRenamed(c, 'CAR_' + c)
    
            self.vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Joining VF Postpaid CAR with NPS ...'

            coun = self.nps.select('partitioned_month').filter("partitioned_month in "+"('"+"', '".join(actual_months)+"')").count()
            print 'NPS before join with VF Postpaid CAR count =', coun
            # self.vfpos_data.select(['nif', 'msisdn', 'partitioned_month']).filter('partitioned_month = "201705"').sort('msisdn').show(5)
            #self.vfpos_data = self.nps.withColumnRenamed('nif', 'nif_nps').join(self.vfpos_data, ['nif', 'partitioned_month'], 'inner')
            self.vfpos_data = self.nps.join(self.vfpos_data, ['nif', 'partitioned_month'], 'inner')
            # self.vfpos_data = self.nps.join(self.vfpos_data, ['nif', 'partitioned_month'], 'left_outer')
            coun = self.vfpos_data.count()
            print 'NPS after  join with VF Postpaid CAR count =', coun
            self.vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
            # print self.vfpos_data.columns
            # self.vfpos_data.select(['nif', 'msisdn', 'partitioned_month']).filter('partitioned_month = "201705"').sort('msisdn').show(5)
            
            print 'Saving NPS + VF Postpaid CAR ...'
            done = False
            while not done:
                try:
                    self.vfpos_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpos')#.coalesce(1)
                    # self.vfpos_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpos.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except Exception as e:
                    print(e)
                    print 'Retrying to save nps-vfpos.txt'
        
        # Vodafone Prepaid
        if self.use_vf_pre:
            print 'Loading VF Prepaid CAR ...'

            actual_months = []
            for m in months:
                try:
				    tmp = self.spark.table('udf_es.pre_explicativas_4m_' + m)
                except AnalysisException as e:
    				print '\tFailed', m, ':', e
                else:
                    tmp = tmp.withColumn('partitioned_month', lit(m))
                    actual_months.append(m)
                    if self.vfpre_data is None:
                        self.vfpre_data = tmp
                    else:
                        self.vfpre_data = self.vfpre_data.union(tmp)
                    # self.vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
                    print '\tLoaded', m

            print 'Appending CAR prefix to columns ...'
            for c in self.vfpre_data.columns:
                if c not in ['nif', 'msisdn', 'year', 'month', 'day', 'partitioned_month']:
                    self.vfpre_data = self.vfpre_data.withColumnRenamed(c, 'CAR_' + c)

            self.vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Joining VF Prepaid CAR with NPS ...'

            coun = self.nps.select('partitioned_month').filter("partitioned_month in "+"('"+"', '".join(actual_months)+"')").count()
            print 'NPS before join with VF Prepaid CAR count =', coun
            #self.vfpre_data = self.nps.withColumnRenamed('nif', 'nif_nps').join(self.vfpre_data, ['msisdn', 'partitioned_month'], 'inner')
            self.vfpre_data = self.nps.drop('nif').join(self.vfpre_data, ['msisdn', 'partitioned_month'], 'inner')
            # self.vfpre_data = self.nps.join(self.vfpre_data, ['nif', 'partitioned_month'], 'left_outer')
            coun = self.vfpre_data.count()
            print 'NPS after  join with VF Prepaid CAR count =', coun
            self.vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
            # self.vfpre_data.select(['nif', 'msisdn', 'partitioned_month']).filter('partitioned_month = "201705"').sort('msisdn').show()

            print 'Saving NPS + VF Prepaid CAR ...'
            done = False
            while not done:
                try:
                    self.vfpre_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpre')#.coalesce(1)
                    # self.vfpre_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpre.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-vfpre.txt'
       
        # Ono
        if self.use_ono:
            print 'Loading Ono CAR ...'

            actual_months = []
            for m in months:
                try:
				    # tmp = self.spark.table('udf_es.ono_explicativas_lin_' + m)
				    tmp = self.spark.table('udf_es.ono_explicativas_nif_rs_' + m)
                except AnalysisException as e:
    				print '\tFailed', m, ':', e
                else:
                    tmp = tmp.withColumn('partitioned_month', lit(m))
                    actual_months.append(m)
                    if self.ono_data is None:
                        self.ono_data = tmp
                    else:
                        self.ono_data = self.ono_data.union(tmp)
                    # self.ono_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
                    print '\tLoaded', m

            print 'Appending CAR prefix to columns ...'
            for c in self.ono_data.columns:
                if c not in ['nif', 'year', 'month', 'day', 'partitioned_month']:
                    self.ono_data = self.ono_data.withColumnRenamed(c, 'CAR_' + c)

            self.ono_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Joining Ono CAR with NPS by NIF ...'

            coun = self.nps.select('partitioned_month').filter("partitioned_month in "+"('"+"', '".join(actual_months)+"')").count()
            print 'NPS before join with ONO CAR count =', coun
            self.ono_data = self.nps.join(self.ono_data, ['nif', 'partitioned_month'], 'inner')
            # self.ono_data = self.nps.join(self.ono_data, ['nif', 'partitioned_month'], 'left_outer')
            coun = self.ono_data.count()
            print 'NPS after  join with ONO CAR count =', coun
            self.ono_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
            # self.ono_data.select(['nif', 'msisdn', 'partitioned_month']).filter('partitioned_month = "201705"').sort('msisdn').show()

            print 'Saving NPS + ONO CAR ...'
            done = False
            while not done:
                try:
                    self.ono_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-ono')#.coalesce(1)
                    # self.ono_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-ono.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-ono.txt'
        
    def join_callcentrecalls(self):
		print 'Loading Call Centre Calls ...'
		ccc = DPCallCentreCalls(self.spark)

		print 'Loading Call Centre Calls by MSISDN ...'
		# ifile = '/tmp/bbergua/ccc/msisdn/'
		# self.ccc_msisdn = self.spark.read.parquet(ifile)
		# # self.ccc_msisdn.select('year', 'month').groupby('year', 'month').count().sort('year', 'month').show()
		# self.ccc_msisdn.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		ccc.generateFeaturesByMsisdn()
		self.ccc_msisdn = ccc.pivoted_by_msisdn

		print 'Loading Call Centre Calls by ID ...'
		# ifile = '/tmp/bbergua/ccc/id/'
		# self.ccc_id = self.spark.read.parquet(ifile)
		# # self.ccc_id.select('year', 'month').groupby('year', 'month').count().sort('year', 'month').show()
		# self.ccc_id.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		ccc.generateFeaturesById()
		self.ccc_id = ccc.pivoted_by_id

		print 'Appending CCC prefix to columns ...'
		for c in list(set(self.ccc_msisdn.columns + self.ccc_id.columns)):
			if c not in ['msisdn', 'nif', 'year', 'month', 'day', 'partitioned_month']:
				self.ccc_msisdn = self.ccc_msisdn.withColumnRenamed(c, 'CCC_' + c)
				self.ccc_id = self.ccc_id.withColumnRenamed(c, 'CCC_' + c)

		# Join NPS with Call Centre Calls without previous joins with customer base

		print 'Joining NPS with Call Centre Calls ...'

		print 'Joining NPS Prepaid with Call Centre Calls ...'
		
		# TODO: Select those months present in ccc_msisdn or ccc_id, instead of .filter('partitioned_month >= "201701"')

		self.nps_pre_ccc = self.nps.filter('partitioned_month >= "201701"').filter('SEGMENTACION == "Prepaid"').join(self.ccc_msisdn, ['msisdn', 'partitioned_month'], 'left_outer').fillna(0)
		# self.nps_pre_ccc = self.nps.join(self.ccc_msisdn, ['msisdn', 'partitioned_month'], 'left_outer')
		self.nps_pre_ccc.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

		print 'Joining NPS No Prepaid with Call Centre Calls ...'

		self.nps_nopre_ccc = self.nps.filter('partitioned_month >= "201701"').filter('SEGMENTACION != "Prepaid"').join(self.ccc_id, ['nif', 'partitioned_month'], 'left_outer').fillna(0)
		# self.nps_nopre_ccc = self.nps.join(self.ccc_id, ['nif', 'partitioned_month'], 'left_outer')
		self.nps_nopre_ccc.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

		print 'Union of NPS Prepaid + No Prepaid with Call Centre Calls ...'

		nps_ccc_cols = list(set(self.nps_pre_ccc.columns + self.nps_nopre_ccc.columns))
		self.nps_ccc = self.nps_pre_ccc.select(nps_ccc_cols).union(self.nps_nopre_ccc.select(nps_ccc_cols))
		self.nps_ccc.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

		print 'Saving NPS + Call Centre Calls ...'
		done = False
		while not done:
			try:
				self.nps_ccc.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-ccc')#.coalesce(1)
				# self.nps_ccc.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-ccc.txt', sep='\t', index=False, encoding='utf-8')
				done = True
			except:
				print 'Retrying to save nps-ccc.txt'

		# Postpaid
		if self.use_vf_pos:
			print 'Joining NPS + VF Postpaid CAR with Call Centre Calls ...'

			coun = self.vfpos_data.count()
			print 'NPS + VF Postpaid CAR before join with Call Centre Calls count =', coun
			self.vfpos_data = self.vfpos_data.join(self.ccc_id, ['nif', 'partitioned_month'], 'left_outer').fillna(0)
			coun = self.vfpos_data.count()
			print 'NPS + VF Postpaid CAR after  join with Call Centre Calls count =', coun
			self.vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

			print 'Saving NPS + VF Postpaid CAR + Call Centre Calls ...'
			done = False
			while not done:
				try:
					self.vfpos_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpos-ccc')#.coalesce(1)
					# self.vfpos_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpos-ccc.txt', sep='\t', index=False, encoding='utf-8')
					done = True
				except:
					print 'Retrying to save nps-vfpos-ccc.txt'

		# Prepaid
		if self.use_vf_pre:
			print 'Joining NPS + VF Prepaid CAR with Call Centre Calls ...'

			coun = self.vfpre_data.count()
			print 'NPS + VF Prepaid CAR before join with Call Centre Calls count =', coun
			#print self.vfpre_data.columns
			self.vfpre_data = self.vfpre_data.join(self.ccc_msisdn, ['msisdn', 'partitioned_month'], 'left_outer').fillna(0)
			coun = self.vfpre_data.count()
			print 'NPS + VF Prepaid CAR after  join with Call Centre Calls count =', coun
			self.vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

			print 'Saving NPS + VF Prepaid CAR + Call Centre Calls ...'
			done = False
			while not done:
				try:
					self.vfpre_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpre-ccc')#.coalesce(1)
					# self.vfpre_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpre-ccc.txt', sep='\t', index=False, encoding='utf-8')
					done = True
				except:
					print 'Retrying to save nps-vfpre-ccc.txt'
        
		# Ono
		if self.use_ono:
			print 'Joining NPS + ONO CAR with Call Centre Calls ...'

			coun = self.ono_data.count()
			print 'NPS + ONO CAR before join with Call Centre Calls count =', coun
			self.ono_data = self.ono_data.join(self.ccc_id, ['nif', 'partitioned_month'], 'left_outer').fillna(0)
			coun = self.ono_data.count()
			print 'NPS + ONO CAR after  join with Call Centre Calls count =', coun
			self.ono_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

			print 'Saving NPS + ONO CAR + Call Centre Calls ...'
			done = False
			while not done:
				try:
					self.ono_data.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-ono-ccc')#.coalesce(1)
					# self.ono_data.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-ono-ccc.txt', sep='\t', index=False, encoding='utf-8')
					done = True
				except:
					print 'Retrying to save nps-ono-ccc.txt'
        
    def join_cem(self):
        print 'Loading CEM ...'

        self.cem = self.spark.table('tests_es.cem_dpa_aggregates')
        self.cem = self.cem.withColumn('partitioned_month', format_string('%d%02d', self.cem.year, self.cem.month))

        print 'Appending CEM prefix to columns ...'
        for c in self.cem.columns:
            if c not in ['msisdn', 'year', 'month', 'day', 'partitioned_month']:
                self.cem = self.cem.withColumnRenamed(c, 'CEM_' + c)

        # self.cem.select(['year', 'month', 'partitioned_month']).groupby(['year', 'month', 'partitioned_month']).count().sort('partitioned_month').show()
        self.cem.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
        

        # Join NPS with CEM without previous joins with customer base

        print 'Joining NPS with CEM ...'

        self.nps_cem = self.nps.join(self.cem, ['msisdn', 'partitioned_month'], 'inner')
        # self.nps_cem = self.nps.join(self.cem, ['msisdn', 'partitioned_month'], 'left_outer')
        self.nps_cem.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

        print 'Saving NPS + CEM ...'
        done = False
        while not done:
            try:
                self.nps_cem.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-cem')#.coalesce(1)
                # self.nps_cem.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-cem.txt', sep='\t', index=False, encoding='utf-8')
                done = True
            except:
                print 'Retrying to save nps-cem.txt'
        
        if self.use_vf_pos:
            print 'Joining NPS + VF Postpaid CAR + Call Centre Calls with CEM ...'

            coun = self.vfpos_data.count()
            print 'NPS + VF Postpaid CAR + Call Centre Calls before join with CEM count =', coun
            self.vfpos_data_cem = self.vfpos_data.join(self.cem, ['msisdn', 'partitioned_month'], 'inner')
            # self.vfpos_data_cem = self.vfpos_data.join(self.cem, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.vfpos_data_cem.count()
            print 'NPS + VF Postpaid CAR + Call Centre Calls after  join with CEM count =', coun
            self.vfpos_data_cem.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + VF Postpaid CAR + Call Centre Calls + CEM ...'
            done = False
            while not done:
                try:
                    self.vfpos_data_cem.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpos-ccc-cem')#.coalesce(1)
                    # self.vfpos_data_cem.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpos-ccc-cem.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-vfpos-ccc-cem.txt'
        
        print 'Joining NPS + VF Prepaid CAR + Call Centre Calls with CEM ...'

        if self.use_vf_pre:
            coun = self.vfpre_data.count()
            print 'NPS + VF Prepaid CAR + Call Centre Calls before join with CEM count =', coun
            self.vfpre_data_cem = self.vfpre_data.join(self.cem, ['msisdn', 'partitioned_month'], 'inner')
            # self.vfpre_data_cem = self.vfpre_data.join(self.cem, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.vfpre_data_cem.count()
            print 'NPS + VF Prepaid CAR + Call Centre Calls after  join with CEM count =', coun
            self.vfpre_data_cem.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + VF Prepaid CAR + Call Centre Calls + CEM ...'
            done = False
            while not done:
                try:
                    self.vfpre_data_cem.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpre-ccc-cem')#.coalesce(1)
                    # self.vfpre_data_cem.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpre-ccc-cem.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-vfpre-ccc-cem.txt'
        
        if self.use_ono:
            print 'Joining NPS + ONO CAR + Call Centre Calls with CEM ...'

            coun = self.ono_data.count()
            print 'NPS + ONO CAR + Call Centre Calls before join with CEM count =', coun
            self.ono_data_cem = self.ono_data.join(self.cem, ['msisdn', 'partitioned_month'], 'inner')
            # self.ono_data_cem = self.ono_data.join(self.cem, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.ono_data_cem.count()
            print 'NPS + ONO CAR + Call Centre Calls after  join with CEM count =', coun
            self.ono_data_cem.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + ONO CAR + Call Centre Calls + CEM ...'
            done = False
            while not done:
                try:
                    self.ono_data_cem.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-ono-ccc-cem')#.coalesce(1)
                    # self.ono_data_cem.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-ono-ccc-cem.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-ono-ccc-cem.txt'
    
    # FIXME
    def join_qt(self):
        print 'Loading Quality Tool ...'

        self.qt = self.spark.table('tests_es.quality_tool_aggregates').withColumnRenamed('metadata_serviceid', 'msisdn')
        self.qt = self.qt.withColumn('partitioned_month', format_string('%d%02d', self.qt.year, self.qt.month))
        # self.qt.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/qt')#.coalesce(1)

        print 'Appending QT prefix to columns ...'
        for c in self.qt.columns:
            if c not in ['msisdn', 'nif', 'year', 'month', 'day', 'partitioned_month']:
                self.qt = self.qt.withColumnRenamed(c, 'QT_' + c)

        # self.qt.select(['year', 'month', 'partitioned_month']).groupby(['year', 'month', 'partitioned_month']).count().sort('partitioned_month').show()
        self.qt.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		
		
        # Join NPS with Quality Tool without previous joins with customer base

        print 'Joining NPS with Quality Tool ...'
		
        self.nps_qt = self.nps.join(self.qt, ['msisdn', 'partitioned_month'], 'inner')
        # self.nps_qt = self.nps.join(self.qt, ['msisdn', 'partitioned_month'], 'left_outer')
        self.nps_qt.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

        print 'Saving NPS + QT ...'
        done = False
        while not done:
            try:
                self.nps_qt.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-qt')#.coalesce(1)
                # self.nps_qt.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-qt.txt', sep='\t', index=False, encoding='utf-8')
                done = True
            except:
                print 'Retrying to save nps-qt.txt'
        
        if self.use_vf_pos:
            print 'Joining NPS + VF Postpaid CAR + Call Centre Calls with QT ...'

            coun = self.vfpos_data.count()
            print 'NPS + VF Postpaid CAR + Call Centre Calls before join with QT count =', coun
            self.vfpos_data_qt = self.vfpos_data.join(self.qt, ['msisdn', 'partitioned_month'], 'inner')
            # self.vfpos_data_qt = self.vfpos_data.join(self.qt, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.vfpos_data_qt.count()
            print 'NPS + VF Postpaid CAR + Call Centre Calls after  join with QT count =', coun
            self.vfpos_data_qt.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + VF Postpaid CAR + Call Centre Calls + QT ...'
            done = False
            while not done:
                try:
                    self.vfpos_data_qt.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpos-ccc-qt')#.coalesce(1)
                    # self.vfpos_data_qt.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpos-ccc-qt.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-vfpos-ccc-qt.txt'
        
        if self.use_vf_pre:
            print 'Joining NPS + VF Prepaid CAR + Call Centre Calls with QT ...'

            coun = self.vfpre_data.count()
            print 'NPS + VF Prepaid CAR + Call Centre Calls before join with QT count =', coun
            self.vfpre_data_qt = self.vfpre_data.join(self.qt, ['msisdn', 'partitioned_month'], 'inner')
            # self.vfpre_data_qt = self.vfpre_data.join(self.qt, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.vfpre_data_qt.count()
            print 'NPS + VF Prepaid CAR + Call Centre Calls after  join with QT count =', coun
            self.vfpre_data_qt.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + VF Prepaid CAR + Call Centre Calls + QT ...'
            done = False
            while not done:
                try:
                    self.vfpre_data_qt.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-vfpre-ccc-qt')#.coalesce(1)
                    # self.vfpre_data_qt.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-vfpre-ccc-qt.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-vfpre-ccc-qt.txt'
        
        if self.use_ono:
            print 'Joining NPS + ONO CAR + Call Centre Calls with QT ...'

            coun = self.ono_data.count()
            print 'NPS + ONO CAR + Call Centre Calls before join with QT count =', coun
            self.ono_data_qt = self.ono_data.join(self.qt, ['msisdn', 'partitioned_month'], 'inner')
            # self.ono_data_qt = self.ono_data.join(self.qt, ['msisdn', 'partitioned_month'], 'left_outer')
            coun = self.ono_data_qt.count()
            print 'NPS + ONO CAR + Call Centre Calls after  join with QT count =', coun
            self.ono_data_qt.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

            print 'Saving NPS + ONO CAR + Call Centre Calls + QT ...'
            done = False
            while not done:
                try:
                    self.ono_data_qt.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/nps-analisis/nps-ono-ccc-qt')#.coalesce(1)
                    # self.ono_data_qt.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-ono-ccc-qt.txt', sep='\t', index=False, encoding='utf-8')
                    done = True
                except:
                    print 'Retrying to save nps-ono-ccc-qt.txt'
        
		
		
		
        # ifile = '/tmp/bbergua/raw/Quality_Tool'
        # qt = self.spark.read.csv(ifile, header=True, sep='|', encoding='utf8', inferSchema=True)
        # qt = qt.withColumnRenamed('Metadata_ServiceID', 'id_ftth')
        # vf_pos = spark.table('raw_es.vf_pos_ac_final')
        # c = qt.join(vf_pos, 'id_ftth').count()
        
        # qt = self.spark.read.csv(ifile, header=True, sep='|', encoding='utf8', inferSchema=True)
        # qt = qt.withColumnRenamed('Metadata_ServiceID', 'x_id_red')
        # vf_pos = spark.table('raw_es.vf_pos_ac_final')
        # c = qt.join(vf_pos, 'x_id_red').count()
        
#        ifile = '/tmp/bbergua/Quality_Tool/'
#        schema = StructType([
#            StructField('CPEID', StringType(), True),
#            StructField('Metadata_ServiceID', StringType(), True),
#            StructField('month', StringType(), True),
#            StructField('ADSL_MejorVel_Down', StringType(), True),
#            StructField('ADSL_MejorVel_Up', StringType(), True),
#            StructField('ADSL_CortesADSL_1hora', StringType(), True),
#            StructField('VOICE_Line_Stats_CallDropped', StringType(), True),
#            StructField('VOICE_Line_ServerDowntime', StringType(), True),
#            StructField('WAN_Stats_MBytesReceived', StringType(), True),
#            StructField('WAN_Stats_MBytesSent', StringType(), True),
#            StructField('LANEthernet_Stats_MBytesReceived', StringType(), True),
#            StructField('LANEthernet_Stats_MBytesSent', StringType(), True),
#            StructField('WLAN_2_4_Stats_MbytesReceived', StringType(), True),
#            StructField('WLAN_2_4_Stats_MbytesSent', StringType(), True),
#            StructField('WLAN_2_4_Stats_ErrorsReceived', StringType(), True),
#            StructField('WLAN_2_4_Stats_ErrorsSent', StringType(), True),
#            StructField('WLAN_5_Stats_MbytesReceived', StringType(), True),
#            StructField('WLAN_5_Stats_MbytesSent', StringType(), True),
#            StructField('WLAN_5_Stats_ErrorsReceived', StringType(), True),
#            StructField('WLAN_5_Stats_ErrorsSent', StringType(), True)
#            ])
#        qt = self.spark.read.csv(ifile, header=True, sep='\t', schema = schema, encoding='utf8')
        
        # ifile = "/tmp/bergua/output/sample/QT_Data_Processed_Aggregates.csv"
        # qt = self.spark.read.csv(ifile, header=True, sep='\t', encoding='utf8', inferSchema=True)

        # qt = qt.withColumnRenamed('Metadata_ServiceID', 'x_id_red')
        # vf_pos = spark.table('raw_es.vf_pos_ac_final')
        # c = qt.join(vf_pos, 'x_id_red').count()
        
		
    # def join_netperform(self):
    #     # np_maps = self.spark.table('tests_es.vf_pass_msisdn_maps_from_np')  # FIXME
    #     np_maps = self.spark.table('tests_es.vf_pass_msisdn_maps_from_np_prepared')  # FIXME
    #     np_maps.select(['year', 'month']).groupby(['year', 'month']).count().sort(['year', 'month']).show()
    #     # 
	
	def load_portabilities(self):
		porta = spark.table('raw_es.vf_solicitudes_portabilidad')
		# porta.select('msisdn', 'nif', 'partitioned_month', 'year', 'month', 'day').distinct().count()
		porta.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/solicitudes_portabilidad.txt', sep='\t', index=False, encoding='utf-8')

    def calculate_correlations(self):
        l = list(set(self.nps_cem.columns) - set(['nif', 'msisdn', 'year', 'month', 'partitioned_month', 'NPS01', 'NPS']))
        l.sort()

#        self.corr_npsi_dict = {}
#        for c in l:
#            try:
#                co = self.nps_cem.corr('NPS_I', c)
#                self.corr_npsi_dict[c] = co
#                print 'Pearson Correlation ( NPS_I ,', c, ') =', co
#            except IllegalArgumentException:
#                # print 'Skipping', c
#                pass

        self.corr_nps01_dict = {}
        nps_cem_no_dk = self.nps_cem.filter('NPS01 != "DK"').withColumn('NPS01', self.data['NPS01'].cast(IntegerType()))
        for c in l:
            try:
                co = nps_cem_no_dk.corr('NPS01', c)
                self.corr_nps01_dict[c] = co
                print 'Pearson Correlation ( NPS01 ,', c, ') =', co
            except IllegalArgumentException:  # pyspark.sql.utils.IllegalArgumentException
                # print 'Skipping', c
                pass

    def run(self):
		# Let's use TNPS as a replacement of real NPS
        self.nps = self.tnps_pivoted.withColumnRenamed('TNPS01', 'NPS01').withColumnRenamed('TNPS', 'NPS').withColumnRenamed('TNPS4', 'NPS4')

        self.join_car()

        self.join_callcentrecalls()

        self.join_cem()
        
        self.join_qt()
        
        # self.join_netperform()

        # self.calculate_correlations()


if __name__ == "__main__":
    print '[' + time.ctime() + ']', 'Starting process ...'

    spark = SparkSession \
        .builder \
        .appName("VF_ES NPS characterization model") \
        .enableHiveSupport() \
        .getOrCreate()
#        .config("spark.port.maxRetries", "50")
#        .config("spark.network.timeout", "10000000")
#        .config("spark.executor.heartbeatInterval", "60")
#        .config("spark.some.config.option", "some-value")

    print '[' + time.ctime() + ']', 'SparkSession created'
	
    # PYTHONIOENCODING=utf-8 ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 ~/IdeaProjects/convergence/src/main/python/convergence.py 201704 201705
    # spark-submit --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=8G ~/fy17.capsule/convergence/src/main/python/convergence.py 201704 201705
    parser = argparse.ArgumentParser(description='VF_ES NPS characterization model',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    # parser.add_argument('model_month', metavar='<model-month>', type=str, help='Date (YYYYMM) to train the model')
    # parser.add_argument('prediction_month', metavar='<prediction-month>', type=str,
    #                     help='Date (YYYYMM) to make the predictions')
    parser.add_argument('-d', '--debug', action='store_true', help='show debug messages')
    args = parser.parse_args()
    print 'args =', args
    # print 'model_month =', args.model_month, ', prediction_month =', args.prediction_month

    nps_est = NpsEstimation(spark)  # , args.model_month, args.prediction_month)
    nps_est.run()

    print '[' + time.ctime() + ']', 'Process finished'

    spark.stop()
	
    print '[' + time.ctime() + ']', 'SparkSession stopped'
