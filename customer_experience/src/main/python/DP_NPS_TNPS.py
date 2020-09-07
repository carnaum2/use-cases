#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
import argparse
from datetime import datetime
import numpy
import re
import subprocess
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col, collect_set, concat, lit, lpad, size, struct, trim, udf, when
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import FloatType, IntegerType, StringType

class DPNpsTnps:

	def __init__(self, spark_other=None, filter_month=None, debug=False):
		self.debug = debug

		self.filter_month = filter_month
		if filter_month is None:
			from datetime import datetime
			self.filter_month = '%d%02d' % (datetime.today().year, datetime.today().month)
		else:
			m = re.search('^(\d{6}|all)$', filter_month)
			if m is None:
				print 'ERROR: month must be a YearMonth, i.e. a six digit number (YYYYMM), or \'all\''
				sys.exit(1)
			
		#self.months_found_set = set()
		
		self.master_by_msisdn = None
		
		self.nps = None
		self.tnps_by_msisdn = None
		self.tnps_by_id = None
		
		global spark
		spark = spark_other
		if spark_other is None:
			spark = SparkSession \
				.builder \
				.appName("VF_ES NPS & TNPS") \
				.enableHiveSupport() \
				.getOrCreate()
				#		.config("spark.port.maxRetries", "50")
				#		.config("spark.network.timeout", "10000000")
				#		.config("spark.executor.heartbeatInterval", "60")
				#		.config("spark.some.config.option", "some-value")

			print '[' + time.ctime() + ']', 'SparkSession created'

	def union_plain_nps(self):
		# hdfs dfs -mkdir /tmp/bbergua/raw/nps
		# hdfs dfs -put Acumulado_*.txt Particulares_*17.txt /tmp/bbergua/raw/nps/
		# hdfs dfs -ls /tmp/bbergua/raw/nps/
		# hdfs dfs -rm -r /tmp/bbergua/raw/nps/nps_plain
	
		ifile_list = ['/tmp/bbergua/raw/nps/Acumulado_Abril_15_a_Sep_16_NPS_Consumer.txt',
					  '/tmp/bbergua/raw/nps/Acumulado_Particulares_Octubre16_Abril17.txt',
					  '/tmp/bbergua/raw/nps/Acumulado_Particulares_Mayo17_Julio17.txt',
					  '/tmp/bbergua/raw/nps/Particulares_Septiembre17.txt',
					  '/tmp/bbergua/raw/nps/Particulares_Octubre17.txt',
					  '/tmp/bbergua/raw/nps/Particulares_Noviembre17.txt']
		nps_l = []
		for ifile in ifile_list:
			print 'Reading file', ifile
			nps_l.append(spark.read.csv(ifile, header=True, sep='\t', encoding='latin1', inferSchema=True))

		# For columns that are not present in all datasets insert them as NULL
		print 'Generate NPS column if it does not exists ...'
		tot_cols = list(set().union(*[x.columns for x in nps_l]))
		for c in tot_cols:
			for i in range(len(nps_l)):
				if c not in nps_l[i].columns:
					if c == 'NPS':
						nps_l[i] = nps_l[i].withColumn('NPS',
														when(nps_l[i]['NPS01'].isin('10', '9'), 'PROMOTER')
													   .when(nps_l[i]['NPS01'].isin('8', '7', 'DK'), 'NEUTRAL')
													   .otherwise('DETRACTOR'))
					else:
						nps_l[i] = nps_l[i].withColumn(c, lit(None))
		
		print 'Union of different files ...'
		nps_plain = nps_l[0].select(tot_cols)
		for i in range(1, len(nps_l)):
			nps_plain = nps_plain.union(nps_l[i].select(tot_cols))
			
		nps_plain.groupby('OLEAYEAR').count().show(50)
		
		nps_plain = nps_plain.withColumn('OLEAYEAR', lower(nps_plain.OLEAYEAR))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'enero', 'jan'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'febrero', 'feb'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'marzo', 'mar'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abril', 'apr'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'mayo', 'may'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'junio', 'jun'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'julio', 'jul'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'agosto', 'aug'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'septiembre', 'sep'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'octubre', 'oct'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'noviembre', 'nov'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'diciembre', 'dic'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ene', 'jan'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abr', 'apr'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ago', 'aug'))
		nps_plain = nps_plain.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'dic', 'dec'))
		nps_plain = nps_plain.withColumn('OLEAYEAR_DATE', from_unixtime(unix_timestamp('OLEAYEAR', 'MMM yy')).cast(DateType()))
		# nps_plain.select(['OLEAYEAR', 'OLEAYEAR_DATE']).groupby(['OLEAYEAR', 'OLEAYEAR_DATE']).count().show()
		nps_plain = nps_plain.withColumn('year', date_format('OLEAYEAR_DATE', 'yyyy'))
		nps_plain = nps_plain.withColumn('month', date_format('OLEAYEAR_DATE', 'MM'))
		nps_plain = nps_plain.withColumn('partitioned_month', date_format('OLEAYEAR_DATE', 'yyyyMM'))
		nps_plain = nps_plain.drop('OLEAYEAR_DATE')
		# nps_plain.select(['OLEAYEAR', 'partitioned_month']).groupby(['OLEAYEAR', 'partitioned_month']).count().show()
		# nps_plain.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(27)
			
		nps_plain.groupby('OLEAYEAR', 'year', 'month', 'partitioned_month').count().sort('year', 'month').show(50)
		
		# Write unified plain file to HDFS and local disk
		
		print 'Saving union of files to HDFS in /tmp/bbergua/raw/nps/nps_plain ...'
		nps_plain.coalesce(1).write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/raw/nps/nps_plain')
		subprocess.call('hdfs dfs -chmod o+rx /tmp/bbergua/raw/nps/nps_plain/', shell=True)
		subprocess.call('hdfs dfs -chmod o+r /tmp/bbergua/raw/nps/nps_plain/*', shell=True)
		# nps_plain = spark.read.csv('/tmp/bbergua/raw/nps/nps_plain', header=True, sep='\t', encoding='utf8', inferSchema=True)
		
		print 'Saving union of files to local disk in /var/SP/data/home/bbergua/raw/nps/nps_full_plain.txt ...'
		nps_plain.toPandas().to_csv('/var/SP/data/home/bbergua/raw/nps/nps_full_plain.txt', sep='\t', index=False, encoding='utf-8')
		
	def test_volumetries(self):
		ifile = '/tmp/bbergua/nps/Acumulado_Abril_15_a_Sep_16_NPS_Consumer'
		dt = spark.read.csv(ifile, header=True, sep='\t', encoding='latin1', inferSchema=True)
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Enero', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Febrero', 'feb'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Marzo', 'mar'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Abril', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Mayo', 'may'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Junio', 'jun'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Julio', 'jul'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Agosto', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Septiembre', 'sep'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Octubre', 'oct'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Noviembre', 'nov'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Diciembre', 'dic'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ene', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abr', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ago', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'dic', 'dec'))
		dt = dt.withColumn('OLEAYEAR_DATE', from_unixtime(unix_timestamp('OLEAYEAR', 'MMM yy')).cast(DateType()))
		# dt.select(['OLEAYEAR', 'OLEAYEAR_DATE']).groupby(['OLEAYEAR', 'OLEAYEAR_DATE']).count().show()
		dt = dt.withColumn('partitioned_month', date_format('OLEAYEAR_DATE', 'yyyyMM')).drop('OLEAYEAR_DATE')
		dt = dt.withColumnRenamed('GFH', 'nif')
		dt = dt.withColumnRenamed('_c19', 'msisdn')
		dt.select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show(28)

		m = '201701'
		pre1 = spark.table('udf_es.pre_explicativas_4m_' + m).drop('partitioned_month')
		pos1 = spark.table('udf_es.par_explic_cli_6m_' + m)
		ono1 = spark.table('udf_es.ono_explicativas_nif_rs_' + m)
		for m in ['201504', '201505', '201506', '201507', '201509', '201510', '201511', '201512', '201601', '201602', '201603', '201604', '201605', '201606', '201607', '201609']:
			pre = pre1.withColumn('partitioned_month', lit(m))
			pos = pos1.withColumn('partitioned_month', lit(m))
			ono = ono1.withColumn('partitioned_month', lit(m))
			
			print(m+' PRE:')
			dt.where('partitioned_month = "'+m+'"').join(pre, ['partitioned_month', 'msisdn'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' POS:')
			dt.where('partitioned_month = "'+m+'"').join(pos, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' ONO:')
			dt.where('partitioned_month = "'+m+'"').join(ono, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
		
		
		ifile = '/tmp/bbergua/nps/Acumulado_Particulares_Octubre16_Abril17'
		dt = spark.read.csv(ifile, header=True, sep='\t', encoding='latin1', inferSchema=True)
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Enero', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Febrero', 'feb'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Marzo', 'mar'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Abril', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Mayo', 'may'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Junio', 'jun'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Julio', 'jul'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Agosto', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Septiembre', 'sep'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Octubre', 'oct'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Noviembre', 'nov'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Diciembre', 'dic'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ene', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abr', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ago', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'dic', 'dec'))
		dt = dt.withColumn('OLEAYEAR_DATE', from_unixtime(unix_timestamp('OLEAYEAR', 'MMM yy')).cast(DateType()))
		# dt.select(['OLEAYEAR', 'OLEAYEAR_DATE']).groupby(['OLEAYEAR', 'OLEAYEAR_DATE']).count().show()
		dt = dt.withColumn('partitioned_month', date_format('OLEAYEAR_DATE', 'yyyyMM')).drop('OLEAYEAR_DATE')
		dt = dt.withColumnRenamed('GFH', 'nif')
		dt = dt.withColumnRenamed('FC24', 'msisdn')
		dt.select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show(28)

		m = '201701'
		pre1 = spark.table('udf_es.pre_explicativas_4m_' + m).drop('partitioned_month')
		pos1 = spark.table('udf_es.par_explic_cli_6m_' + m)
		ono1 = spark.table('udf_es.ono_explicativas_nif_rs_' + m)
		for m in ['201610', '201611', '201612']:
			pre = pre1.withColumn('partitioned_month', lit(m))
			pos = pos1.withColumn('partitioned_month', lit(m))
			ono = ono1.withColumn('partitioned_month', lit(m))
			
			print(m+' PRE:')
			dt.where('partitioned_month = "'+m+'"').join(pre, ['partitioned_month', 'msisdn'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' POS:')
			dt.where('partitioned_month = "'+m+'"').join(pos, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' ONO:')
			dt.where('partitioned_month = "'+m+'"').join(ono, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
		
		for m in ['201701', '201702', '201703', '201704']:
			pre = spark.table('udf_es.pre_explicativas_4m_' + m)
			pos = spark.table('udf_es.par_explic_cli_6m_' + m)
			pos = pos.withColumn('partitioned_month', lit(m))
			ono = spark.table('udf_es.ono_explicativas_nif_rs_' + m)
			ono = ono.withColumn('partitioned_month', lit(m))
			
			print(m+' PRE:')
			dt.where('partitioned_month = "'+m+'"').join(pre, ['partitioned_month', 'msisdn'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' POS:')
			dt.where('partitioned_month = "'+m+'"').join(pos, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' ONO:')
			dt.where('partitioned_month = "'+m+'"').join(ono, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()


		ifile = '/tmp/bbergua/nps/Acumulado_Particulares_Mayo17_Julio17'
		dt = spark.read.csv(ifile, header=True, sep='\t', encoding='latin1', inferSchema=True)
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Enero', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Febrero', 'feb'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Marzo', 'mar'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Abril', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Mayo', 'may'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Junio', 'jun'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Julio', 'jul'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Agosto', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Septiembre', 'sep'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Octubre', 'oct'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Noviembre', 'nov'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Diciembre', 'dic'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ene', 'jan'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abr', 'apr'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ago', 'aug'))
		dt = dt.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'dic', 'dec'))
		dt = dt.withColumn('OLEAYEAR_DATE', from_unixtime(unix_timestamp('OLEAYEAR', 'MMM yy')).cast(DateType()))
		# dt.select(['OLEAYEAR', 'OLEAYEAR_DATE']).groupby(['OLEAYEAR', 'OLEAYEAR_DATE']).count().show()
		dt = dt.withColumn('partitioned_month', date_format('OLEAYEAR_DATE', 'yyyyMM')).drop('OLEAYEAR_DATE')
		dt = dt.withColumnRenamed('GFH', 'nif')
		dt = dt.withColumnRenamed('_c7', 'msisdn')
		dt.select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show(12)
		
		for m in ['201705', '201706', '201707']:
			pre = spark.table('udf_es.pre_explicativas_4m_' + m)
			pos = spark.table('udf_es.par_explic_cli_6m_' + m)
			pos = pos.withColumn('partitioned_month', lit(m))
			ono = spark.table('udf_es.ono_explicativas_nif_rs_' + m)
			ono = ono.withColumn('partitioned_month', lit(m))
			
			print(m+' PRE:')
			dt.where('partitioned_month = "'+m+'"').join(pre, ['partitioned_month', 'msisdn'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' POS:')
			dt.where('partitioned_month = "'+m+'"').join(pos, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()
			print(m+' ONO:')
			dt.where('partitioned_month = "'+m+'"').join(ono, ['partitioned_month', 'nif'], 'inner').select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show()

	def fix_column_names(self, df):
		names = df.schema.names
		
		for n in names:
			m = re.search('([^()]*)\(([^()]*)\)', n)
			if m is not None:
				# print m.group(0), '->', m.group(1) + '_' + m.group(2)
				df = df.withColumnRenamed(n, m.group(1) + '_' + m.group(2))

			m = re.sub('[^a-zA-Z0-9_]', '_', n)
			if n != m:
				df = df.withColumnRenamed(n, m)

		return df

	def generateNPS(self):
		print 'Loading NPS ...'
		
		ifile = '/tmp/bbergua/nps/nps_anon/'
		self.nps = spark.read.csv(ifile, header=True, sep='\t', encoding='utf8', inferSchema=True)
		
		# TODO
		self.nps = self.nps.withColumnRenamed('GFH', 'CIF')
		self.nps = self.nps.withColumnRenamed('_c9', 'FC24')
		
#		self.nps = self.nps.withColumn('NPS',
#										when(self.nps['NPS01'].isin('10', '9'), 'PROMOTER')
#									   .when(self.nps['NPS01'].isin('8', '7', 'DK'), 'NEUTRAL')
#									   .otherwise('DETRACTOR'))
		self.nps = self.nps.withColumn('NPS4',
										when(self.nps['NPS01'].isin('10', '9'),          'PROMOTER')
									   .when(self.nps['NPS01'].isin('8', '7', 'DK'),     'NEUTRAL')
									   .when(self.nps['NPS01'].isin('6', '5', '4'),      'SOFT DETRACTOR')
									   .when(self.nps['NPS01'].isin('3', '2', '1', '0'), 'HARD DETRACTOR'))
									   #.otherwise(False))
#		self.nps = self.nps.withColumn('NPS_I',
#										when(self.nps['NPS'] == 'PROMOTER', 2)
#									   .when(self.nps['NPS'] == 'NEUTRAL', 1)
#									   .otherwise(0))

		# FIXME: This is temporal: Generate partitioned_month
		# # self.nps.select('OLEAYEAR').groupby('OLEAYEAR').count().show()
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Enero', 'jan'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Febrero', 'feb'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Marzo', 'mar'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Abril', 'apr'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Mayo', 'may'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Junio', 'jun'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Julio', 'jul'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Agosto', 'aug'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Septiembre', 'sep'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Octubre', 'oct'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Noviembre', 'nov'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'Diciembre', 'dic'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ene', 'jan'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'abr', 'apr'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'ago', 'aug'))
		# self.nps = self.nps.withColumn('OLEAYEAR', regexp_replace('OLEAYEAR', 'dic', 'dec'))
		# self.nps = self.nps.withColumn('OLEAYEAR_DATE', from_unixtime(unix_timestamp('OLEAYEAR', 'MMM yy')).cast(DateType()))
		# # self.nps.select(['OLEAYEAR', 'OLEAYEAR_DATE']).groupby(['OLEAYEAR', 'OLEAYEAR_DATE']).count().show()
		# self.nps = self.nps.withColumn('partitioned_month', date_format('OLEAYEAR_DATE', 'yyyyMM')).drop('OLEAYEAR_DATE')
		# # self.nps.select(['OLEAYEAR', 'partitioned_month']).groupby(['OLEAYEAR', 'partitioned_month']).count().show()
		# # self.nps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		self.nps = self.nps.withColumn('year', self.nps['partitioned_month'].substr(1, 4))\
						   .withColumn('month', self.nps['partitioned_month'].substr(5, 2))\
						   .withColumn('day', lit(0))
		self.nps.select('partitioned_month', 'year', 'month', 'day').groupby('partitioned_month', 'year', 'month', 'day').count().sort('partitioned_month', 'year', 'month', 'day').show(50)

		self.nps = self.nps.withColumnRenamed('CIF', 'nif')
		self.nps = self.nps.withColumnRenamed('FC24', 'msisdn')
		
		self.nps.select('partitioned_month', 'SEGMENTACION').groupby('partitioned_month', 'SEGMENTACION').count().sort('partitioned_month', 'SEGMENTACION').show(200)
		# self.nps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

		# self.nps.write.mode('overwrite').format('csv').save('/tmp/bbergua/nps-analisis/nps_full.txt')
		#	 .partitionBy('partitioned_month', 'year', 'month', 'day')
		# self.nps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps_full.txt', sep='\t', index=False, encoding='utf-8')

		# if self.by_msisdn:
		#	 self.nps = self.nps.select(['msisdn', 'NPS', 'partitioned_month'])
		# else:
		#	 self.nps = self.nps.select(['nif', 'NPS', 'partitioned_month'])
		self.nps = self.nps.select(['nif', 'msisdn', 'SEGMENTACION', 'NPS01', 'NPS', 'NPS4', 'partitioned_month', 'year', 'month', 'day'])

		# self.nps.write.mode('overwrite').format('csv').save('/tmp/bbergua/nps-analisis/nps_short.txt')
		# self.nps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps_short.txt', sep='\t', index=False, encoding='utf-8')

		self.nps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		
		coun = self.nps.filter('partitioned_month >= "201701"').count()
		print 'NPS Jan-end count =', coun

		# coun = self.nps.filter('partitioned_month >= "201705" and partitioned_month <= "201707"').count()
		# print 'NPS May-Jul count =', coun

	def calculate_vfpos_segment(self):
		flags_fields = ['flagvoz', 'flagadsl', 'flagftth'] #, 'flaglpd', 'flaghz', 'flagtivo', 'flagvfbox', 'flagfutbol', 'flagmotor']
		
		data = spark.table('raw_es.vf_pos_ac_final').select(['x_num_ident', 'x_id_red', 'partitioned_month']+flags_fields)
		for c in flags_fields:
			data = data.withColumn(c, data[c].cast(IntegerType()))
		
		data_by_id = data.groupBy(['x_num_ident', 'partitioned_month']).sum(*flags_fields)
		data_by_id = self.fix_column_names(data_by_id)
		flags_fixed = ['sum_' + c for c in flags_fields if c != 'flagvoz']
		
		# Calculate Mobile-Only
		mo_condition = (data_by_id['sum_flagvoz'] > 0)
		for flag in flags_fixed:
			if flag in data_by_id.columns:
				# print 'Adding fixed flag', flag, 'to mo_condition'
				mo_condition = mo_condition & (data_by_id[flag] == 0)
		
		data_by_id = data_by_id.withColumn('is_mobile_only', when(mo_condition, True).otherwise(False))
		
		# Calculate Convergent
		# co_condition = None
		# for flag in flags_fixed:
		#	 if flag in data_by_id.columns:
		#		 # print 'Adding flag', flag, 'to co_condition'
		#		 if co_condition is None:
		#			 co_condition = (data_by_id[flag] > 0)
		#		 else:
		#			 co_condition = co_condition | (data_by_id[flag] > 0)
		# 
		# co_condition = (data_by_id['sum_flagvoz'] > 0) & co_condition
		co_condition = (data_by_id['sum_flagadsl'] > 0) | (data_by_id['sum_flagftth'] > 0)
		data_by_id = data_by_id.withColumn('is_convergent', when(co_condition, True).otherwise(False))
		
		data_by_id = data_by_id.withColumn('SEGMENTACION',   when(mo_condition, lit('VF Mobile-Only'))
															.when(co_condition, lit('VF Convergent'))
															.otherwise(         lit('VF Other')))
		
		data_by_id = data_by_id.withColumnRenamed('x_num_ident', 'nif').select(['nif', 'partitioned_month', 'SEGMENTACION']) # , 'is_mobile_only', 'is_convergent'
		
		# data_by_id.filter('is_mobile_only==FALSE AND is_convergent==FALSE').groupby('partitioned_month').count().sort('partitioned_month').show()
		# data_by_id.groupby(['partitioned_month', 'SEGMENTACION']).count().sort(['partitioned_month', 'SEGMENTACION']).show(60)
		
		return data_by_id

	def generateTNPSByMsisdn(self,closing_day,starting_day):
		if self.debug:
			print 'Loading TNPS ...'

		self.filter_month = str(closing_day)[:6]
		
		tnps = spark.table('raw_es.tnps')
		#print tnps.count()
		# TODO: Can I trust in year, month, and day? Meanwhile I regenerate them using FechaLLamYDILO?
		tnps = tnps.withColumn('year', year('FechaLLamYDILO')).withColumn('month', month('FechaLLamYDILO')).withColumn('day', dayofmonth('FechaLLamYDILO'))
		#print tnps.count()
		tnps = tnps.withColumn('partitioned_month', 100*tnps.year + tnps.month)
		#print tnps.count()
		#self.months_found_set = self.months_found_set.union(set(tnps.select('partitioned_month').distinct().rdd.map(lambda x: x['partitioned_month']).collect()))
		#if self.filter_month != 'all':
		#	if self.debug:
		#		print 'Filtering TNPS by month =', self.filter_month
		#	tnps = tnps.filter('partitioned_month == %s' % self.filter_month)
		tnps = tnps.where ( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                          & (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
		tnps = tnps.distinct()
		tnps = tnps.withColumnRenamed('SERIAL_NUMBER', 'msisdn')
		#tnps = tnps.repartition('msisdn')
		#print tnps.count()
		if self.debug:
			tnps.groupby('partitioned_month', 'year', 'month').count().sort('partitioned_month', 'year', 'month').show(50)

		# tnps.groupby('Num_Pregunta', 'Literal').count().sort('count', ascending=False).show()
		# tnps.filter('Literal LIKE "_Con qu_ probabilidad recomendar_a Vodafone a un amigo o compa_ero?"').groupby('Num_Pregunta', 'Literal').count().sort('count', ascending=False).show(truncate=False)
		# tnps.filter('Literal LIKE "%on qu_ probabilidad recomendar_a Vodafone%"').groupby('Num_Pregunta', 'Literal').count().sort('count', ascending=False).show(truncate=False)
		# tnps_nps = tnps.filter('Literal LIKE "_Con qu_ probabilidad recomendar_a Vodafone a un amigo o compa_ero?"')
		# tnps.filter('Literal LIKE "%on qu_ probabilidad recomendar_a Vodafone%"').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		# tnps.filter('Literal LIKE "%on qu_ probabilidad recomendar_as Vodafone%"').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		# tnps.filter('Literal LIKE "%on qu_ probabilidad recomendar_a ONO%"').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		# tnps.filter('Literal LIKE "%on qu_ probabilidad recomendar_a Ono%"').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		# tnps.filter('Literal LIKE "%mb quina probabilitat recomanar_a Vodafone%').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		# tnps.filter('Literal LIKE "¿_ecomendar_a Vodafone%?"').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
		
		# tnps.filter('(Literal LIKE "%on qu_ probabilidad recomendar_a Vodafone%")  OR ' + \
					# '(Literal LIKE "%on qu_ probabilidad recomendar_as Vodafone%") OR ' + \
					# '(Literal LIKE "%on qu_ probabilidad recomendar_a ONO%")       OR ' + \
					# '(Literal LIKE "%on qu_ probabilidad recomendar_a Ono%")       OR ' + \
					# '(Literal LIKE "%mb quina probabilitat recomanar_a Vodafone%") OR ' + \
					# '(Literal LIKE "__ecomendar_a Vodafone%?")').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
					 
		# tnps.filter('(Literal LIKE "%on qu_ probabilidad recomendar_a Vodafone%") OR (Literal LIKE "%on qu_ probabilidad recomendar_as Vodafone%")').select('Num_Pregunta').distinct().sort('Num_Pregunta').show()
					 
		nums_pregunta_recomendaria = ['4155.0', '4161.0', '4167.0', '4173.0',
									  '4179.0', '4185.0', '4191.0', '4197.0',
									  '5001.0', '5018.0', '5190.0', '5774.0',
									  '5775.0', '5776.0', '5805.0', '5818.0',
									  '5821.0', '5825.0', '5835.0', '5847.0',
									  '5860.0', '5894.0', '5910.0', '5974.0',
									  '6025.0', '6034.0', '6064.0', '6066.0',
									  '6128.0', '6191.0', '6260.0', '6286.0',
									  '6295.0', '6303.0', '6308.0', '6319.0',
									  '6473.0', '6595.0']
		# tnps.filter(tnps['Num_Pregunta'] == '4155.0').select('Num_Pregunta', 'Literal').show()
		# tnps.filter(tnps['Num_Pregunta'] == '5190.0').select('Num_Pregunta', 'Literal').show()
		
		tnps_nps = tnps.filter(tnps['Num_Pregunta'].isin(nums_pregunta_recomendaria))
		vdns = [x.VDN for x in tnps_nps.select('VDN').distinct().collect()]

		#tnps_nps.select('Respuesta').distinct().show()
		#tnps_nps.select('Respuesta').groupby('Respuesta').count().sort('count').show()
		tnps_nps = tnps_nps.filter('Respuesta != "ERROR"').withColumn('Respuesta_Num',
													  when(tnps_nps.Respuesta.like('CERO'),   lit(0))
													 .when(tnps_nps.Respuesta.like('UNO'),    lit(1))
													 .when(tnps_nps.Respuesta.like('DOS'),    lit(2))
													 .when(tnps_nps.Respuesta.like('TRES'),   lit(3))
													 .when(tnps_nps.Respuesta.like('CUATRO'), lit(4))
													 .when(tnps_nps.Respuesta.like('CINCO'),  lit(5))
													 .when(tnps_nps.Respuesta.like('SEIS'),   lit(6))
													 .when(tnps_nps.Respuesta.like('SIETE'),  lit(7))
													 .when(tnps_nps.Respuesta.like('OCHO'),   lit(8))
													 .when(tnps_nps.Respuesta.like('NUEVE'),  lit(9))
													 .when(tnps_nps.Respuesta.like('DIEZ'),   lit(10)))
		#tnps_nps.select('Respuesta_Num').groupby('Respuesta_Num').count().sort('count').show()
		#tnps_nps.select('Respuesta', 'Respuesta_Num').groupby('Respuesta', 'Respuesta_Num').count().sort('Respuesta_Num').show()
		if self.debug:
			tnps_nps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

		if self.debug:
			print 'Pivoting TNPS on VDN ...'
		tnps_pivoted = tnps_nps.groupby('msisdn').pivot('VDN', values=vdns).min('Respuesta_Num').withColumn('day', lit(0))
		#tnps_pivoted = tnps_pivoted.withColumn('day', lit(0))

		if self.debug:
			print 'Appending TNPS prefix to columns ...'
		for c in tnps_pivoted.columns:
			if c not in ['msisdn', 'partitioned_month', 'year', 'month', 'day']:
				tnps_pivoted = tnps_pivoted.withColumnRenamed(c, 'TNPS_VDN_' + c)
		
		# Calculate min, mean, and max of all TNPS_VDN_*
		min_vdn = udf(lambda row: min(filter(lambda x: x is not None, row)) if len(filter(lambda x: x is not None, row))>0 else None, IntegerType())
		#list_vdn = udf(lambda row: str(filter(lambda x: x is not None, row)), StringType())
		#sum_vdn = udf(lambda row: sum(filter(lambda x: x is not None, row)) if len(filter(lambda x: x is not None, row))>0 else None, IntegerType())
		#len_vdn = udf(lambda row: len(filter(lambda x: x is not None, row)) if len(filter(lambda x: x is not None, row))>0 else None, IntegerType())
		#npmean_vdn = udf(lambda row: numpy.mean(filter(lambda x: x is not None, row)) if len(filter(lambda x: x is not None, row))>0 else None, FloatType())
		mean_vdn = udf(lambda row: sum(filter(lambda x: x is not None, row))/float(len(filter(lambda x: x is not None, row))) if len(filter(lambda x: x is not None, row))>0 else None, FloatType())
		max_vdn = udf(lambda row: max(filter(lambda x: x is not None, row)) if len(filter(lambda x: x is not None, row))>0 else None, IntegerType())
		tnps_pivoted = tnps_pivoted.withColumn('min_VDN', min_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		#tnps_pivoted = tnps_pivoted.withColumn('list_VDN', list_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		#tnps_pivoted = tnps_pivoted.withColumn('sum_VDN', sum_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		#tnps_pivoted = tnps_pivoted.withColumn('len_VDN', len_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		#tnps_pivoted = tnps_pivoted.withColumn('npmean_VDN', npmean_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		tnps_pivoted = tnps_pivoted.withColumn('mean_VDN', mean_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		tnps_pivoted = tnps_pivoted.withColumn('max_VDN', max_vdn(struct([tnps_pivoted[x] for x in tnps_pivoted.columns if x.startswith('TNPS_VDN_')])))#.fillna(0)
		# tnps_pivoted.show()
		# tnps_pivoted.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		
		# Take min_VDN as the reference variable for TNPS01
		tnps_pivoted = tnps_pivoted.withColumn('TNPS01', tnps_pivoted['min_VDN'])
		tnps_pivoted = tnps_pivoted.withColumn('TNPS4',
										 when(tnps_pivoted['TNPS01'].isin(9, 10),      'PROMOTER')
										.when(tnps_pivoted['TNPS01'].isin(7, 8),       'NEUTRAL')
										.when(tnps_pivoted['TNPS01'].isin(4, 5, 6),    'SOFT DETRACTOR')
										.when(tnps_pivoted['TNPS01'].isin(0, 1, 2, 3), 'HARD DETRACTOR'))
										#.otherwise(False))
		tnps_pivoted = tnps_pivoted.withColumn('TNPS',
										 when(tnps_pivoted['TNPS4'].isin('HARD DETRACTOR', 'SOFT DETRACTOR'), 'DETRACTOR')
										.otherwise(tnps_pivoted['TNPS4']))
		tnps_pivoted = tnps_pivoted.withColumn('TNPS2DET',
										 when(tnps_pivoted['TNPS'] == 'DETRACTOR', tnps_pivoted['TNPS'])
										.otherwise('NON DETRACTOR'))
		tnps_pivoted = tnps_pivoted.withColumn('TNPS2PRO',
										 when(tnps_pivoted['TNPS'] == 'PROMOTER', tnps_pivoted['TNPS'])
										.otherwise('NON PROMOTER'))

		if self.debug:
			tnps_pivoted.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)

		# Add SEGMENTACION
		
		if self.debug:
			print 'Reading master_customers_services by MSISDN from HDFS'
		self.master_by_msisdn = spark.read.parquet('/tmp/bbergua/master_customers_services/msisdn/')
		if self.filter_month != 'all':
			if self.debug:
				print 'Filtering Master Customers Services by month =', self.filter_month
			# FIXME: Master Customers Services must have been generated previously, otherwise SEGMENTACION will be empty
			self.master_by_msisdn = self.master_by_msisdn.filter('partitioned_month == %s' % self.filter_month)
			#print self.master_by_msisdn.rdd.getNumPartitions()
			#self.master_by_msisdn = self.master_by_msisdn.repartition('msisdn')
			#print self.master_by_msisdn.rdd.getNumPartitions()
		if self.debug:
			self.master_by_msisdn.printSchema()
			print self.master_by_msisdn.count()
			self.master_by_msisdn.groupby(['partitioned_month', 'SEGMENTACION']).count().sort(['partitioned_month', 'count'], ascending=False).show()

		tnps_pivoted_segment = tnps_pivoted.join(self.master_by_msisdn, ['msisdn'], 'left_outer')
		if self.debug:
			tnps_pivoted_segment.groupby('partitioned_month').count().sort('partitioned_month').show(50)

		# tnps_pivoted_segment.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		# self.tnps = tnps_pivoted_segment.select(['msisdn', 'TNPS01', 'TNPS4', 'TNPS', 'TNPS2DET', 'TNPS2PRO', 'partitioned_month', 'year', 'month', 'day'])
		self.tnps_by_msisdn = tnps_pivoted_segment

		if self.debug:
			self.tnps_by_msisdn.groupby(['partitioned_month', 'SEGMENTACION']).count().sort(['partitioned_month', 'count'], ascending=False).show()
		# self.tnps_by_msisdn.select(['msisdn', 'SEGMENTACION', 'TNPS01', 'TNPS4', 'TNPS', 'TNPS2DET', 'TNPS2PRO', 'partitioned_month', 'year', 'month', 'day']).show()		
		
		# self.tnps_by_msisdn = tnps_pivoted.select(['msisdn', 'partitioned_month', 'TNPS01', 'TNPS', 'TNPS4'])
		# self.tnps_by_msisdn = tnps
		
		# print 'Saving TNPS ...'
		# #self.tnps.write.mode('overwrite').format('csv').option('sep', '\t').option('header', 'true').save('/tmp/bbergua/tnps/msisdn/')#.coalesce(1)
		# self.tnps.write.mode('overwrite').format('parquet').save('/tmp/bbergua/tnps/msisdn/')
		# # tnps = spark.read.parquet('/tmp/bbergua/tnps/msisdn/')
		# subprocess.call('hdfs dfs -chmod o+rx /tmp/bbergua/tnps/msisdn/',  shell=True)
		# subprocess.call('hdfs dfs -chmod o+r  /tmp/bbergua/tnps/msisdn/*', shell=True)
		# # self.tnps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/tnps.txt', sep='\t', index=False, encoding='utf-8')

		return self.tnps_by_msisdn
	
	@staticmethod
	def drop_prepaid_from_array_(arr):
		if len(arr) > 1:
			arr.remove('Prepaid')
			return arr[0]
		return arr
		
	def generateTNPSById(self):
		if self.debug:
			print 'Adding NIF to TNPS ...'
		
		# Now, aggregate TNPS by client's id (NIF)
		
		tnps_pivoted_segment = self.tnps_by_msisdn
		#nif_segment = tnps_pivoted.select('nif', 'SEGMENTACION', 'partitioned_month').groupby('nif', 'partitioned_month').agg(collect_list('SEGMENTACION').alias('SEGMENTACION'))
		nif_segment = tnps_pivoted_segment.select('nif', 'SEGMENTACION', 'partitioned_month', 'year', 'month', 'day') \
			.groupby('nif', 'partitioned_month', 'year', 'month', 'day').agg(collect_set('SEGMENTACION').alias('SEGMENTACION'))
		#nif_segment.show()
		#nif_segment.groupby('partitioned_month', 'SEGMENTACION').count() \
		#			.sort('partitioned_month', 'count', ascending=False).show()
		
		nif_segment = nif_segment.na.drop(subset='nif')
		#nif_segment.groupby('partitioned_month', 'SEGMENTACION').count() \
		#			.sort('partitioned_month', 'count', ascending=False).show()
					
		nif_segment = nif_segment.withColumn('SEGMENTACION_Prepaid', 
							when(array_contains(nif_segment['SEGMENTACION'], 'Prepaid'), 'Prepaid'))
		#nif_segment.where(size(col('SEGMENTACION')) >= 2).show()
		
		drop_prepaid_from_array = udf(DPNpsTnps.drop_prepaid_from_array_, StringType())
		#nif_segment = nif_segment.withColumn('SEGMENTACION_', drop_prepaid_from_array('SEGMENTACION'))
		nif_segment = nif_segment.withColumn('SEGMENTACION_', 
											 when(size(col('SEGMENTACION')) == 1, col('SEGMENTACION').getItem(0))\
											.when(size(col('SEGMENTACION')) >= 1, drop_prepaid_from_array('SEGMENTACION'))\
											.otherwise(None))

		#nif_segment.where(size(col('SEGMENTACION')) == 1).show()
		#nif_segment.where(size(col('SEGMENTACION')) >= 2).show()

		nif_segment = nif_segment.drop('SEGMENTACION').withColumnRenamed('SEGMENTACION_', 'SEGMENTACION')
		#nif_segment.show()

		#tnps_pivoted_segment.printSchema()###
		#tnps_pivoted_by_id = tnps_pivoted_segment.groupby('nif', 'partitioned_month', 'year', 'month', 'day').min()
		tnps_pivoted_by_id = tnps_pivoted_segment.groupby('nif', 'partitioned_month', 'year', 'month', 'day').min()
		tnps_pivoted_by_id = tnps_pivoted_by_id.join(tnps_pivoted_segment.groupby('nif', 'partitioned_month', 'year', 'month', 'day').mean(), ['nif', 'partitioned_month', 'year', 'month', 'day'])
		tnps_pivoted_by_id = tnps_pivoted_by_id.join(tnps_pivoted_segment.groupby('nif', 'partitioned_month', 'year', 'month', 'day').max(), ['nif', 'partitioned_month', 'year', 'month', 'day'])
		# TODO: Use pyspark.sql.functions.collect_list(col) to collect all values of VDNs from all MSISDNs for every NIF
		#       Finally aggregate by min, avg, and max
		#tnps_pivoted_by_id.printSchema()###
		tnps_pivoted_by_id = self.fix_column_names(tnps_pivoted_by_id)
		tnps_pivoted_by_id = tnps_pivoted_by_id.drop('min_partitioned_month', 'min_year', 'min_month', 'min_day')
		#tnps_pivoted_by_id.printSchema()
		# tnps_pivoted_by_id = tnps_pivoted_by_id.withColumnRenamed('min_TNPS01', 'TNPS01')
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS01', tnps_pivoted_by_id['min_TNPS01'])
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS4',
							 when(tnps_pivoted_by_id['TNPS01'].isin(9, 10),      'PROMOTER')
							.when(tnps_pivoted_by_id['TNPS01'].isin(7, 8),       'NEUTRAL')
							.when(tnps_pivoted_by_id['TNPS01'].isin(4, 5, 6),    'SOFT DETRACTOR')
							.when(tnps_pivoted_by_id['TNPS01'].isin(0, 1, 2, 3), 'HARD DETRACTOR'))
							#.otherwise(False))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS',
							 when(tnps_pivoted_by_id['TNPS4'].isin('SOFT DETRACTOR', 'HARD DETRACTOR'), 'DETRACTOR')
							.otherwise(tnps_pivoted_by_id['TNPS4']))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS2DET',
							 when(tnps_pivoted_by_id['TNPS'] == 'DETRACTOR', tnps_pivoted_by_id['TNPS'])
							.otherwise('NON DETRACTOR'))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS2PRO',
							 when(tnps_pivoted_by_id['TNPS'] == 'PROMOTER', tnps_pivoted_by_id['TNPS'])
							.otherwise('NON PROMOTER'))
		# tnps_pivoted_by_id.select(['nif', 'TNPS01', 'TNPS4', 'TNPS', 'TNPS2DET', 'TNPS2PRO', 'partitioned_month']).show()#tnps_pivoted_by_id.printSchema()
		#tnps_pivoted_by_id.printSchema()

		# Join with SEGMENTACION, previously calculated

		tnps_pivoted_by_id = tnps_pivoted_by_id.join(nif_segment, ['nif', 'partitioned_month', 'year', 'month', 'day'], 'left_outer')
		#tnps_pivoted_by_id.select(['nif', 'SEGMENTACION_Prepaid', 'SEGMENTACION', 'TNPS01', 'TNPS', 'TNPS4', 'partitioned_month', 'year', 'month', 'day']).show(10)
		#tnps_pivoted_by_id.printSchema()

		#tnps_pivoted_by_id.filter('SEGMENTACION == NULL').count()
		self.tnps_by_id = tnps_pivoted_by_id

	def generateTNPSById_Old(self):
		print 'Adding NIF to TNPS ...'

		tnps_pivoted = self.tnps_by_msisdn
		
		# vf_pre_nif_msisdn = spark.table('raw_es.vf_pre_ac_final').select(['msisdn',   'num_documento_cliente', 'partitioned_month', 'year', 'month', 'day']).distinct().withColumnRenamed('num_documento_cliente', 'nif'   ).withColumn('SEGMENTACION', lit('Prepaid' )).select(['nif', 'msisdn', 'SEGMENTACION', 'partitioned_month', 'year', 'month', 'day'])
		# vf_pos_nif_msisdn = spark.table('raw_es.vf_pos_ac_final').select(['x_id_red', 'x_num_ident',           'partitioned_month', 'year', 'month', 'day']).distinct().withColumnRenamed('x_id_red',              'msisdn').withColumnRenamed('x_num_ident',     'nif').withColumn('SEGMENTACION', lit('Postpaid')).select(['nif', 'msisdn', 'SEGMENTACION', 'partitioned_month', 'year', 'month', 'day'])
		
		# spark.table('raw_es.customerprofilecar_servicesow').select('year', 'month', 'day').distinct().groupby('year', 'month').count().sort('year', 'month').show(30)
		# ono_msisdn_numcliente = spark.table('raw_es.customerprofilecar_servicesow').select(['NUM_SERIE', 'NUM_CLIENTE']).withColumnRenamed('NUM_SERIE', 'msisdn').withColumnRenamed('NUM_CLIENTE', 'num_cliente')
		# ono_msisdn_numcliente = ono_msisdn_numcliente.withColumn('msisdn', trim(ono_msisdn_numcliente.msisdn)).distinct()
		# ono_numcliente_nif = spark.table('raw_es.customerprofilecar_customerow').distinct().select(['NUM_CLIENTE', 'NIF_CLIENTE']).withColumnRenamed('NIF_CLIENTE', 'nif').withColumnRenamed('NUM_CLIENTE', 'num_cliente')
		# ono_msisdn_nif = ono_msisdn_numcliente.join(ono_numcliente_nif, ['num_cliente'], 'inner').distinct().select(['msisdn', 'num_cliente', 'nif']).withColumn('SEGMENTACION', lit('Ono')).select(['nif', 'num_cliente', 'msisdn', 'SEGMENTACION'])
		
		# master_nif_services = vf_pre_nif_msisdn.union(vf_pos_nif_msisdn)#.union(ono_msisdn_nif)
		
		cols = tnps_pivoted.columns
		# tnps_pivoted.count() # 18 323 505
		
		vf_pos = spark.table('raw_es.vf_pos_ac_final').select(['x_id_red', 'x_num_ident', 'partitioned_month', 'year', 'month', 'day']).withColumnRenamed('x_id_red', 'msisdn').withColumnRenamed('x_num_ident', 'nif').withColumn('SEGMENTACION', lit('Postpaid'))
		nps_tnps_pos = tnps_pivoted.join(vf_pos, ['msisdn', 'partitioned_month', 'year', 'month', 'day'], 'inner').select(['nif', 'SEGMENTACION'] + cols)
		# nps_tnps_pos.count() #  6 019 774
		
		vf_pre = spark.table('raw_es.vf_pre_ac_final').select(['msisdn', 'num_documento_cliente', 'partitioned_month', 'year', 'month', 'day']).withColumnRenamed('num_documento_cliente', 'nif').withColumn('SEGMENTACION', lit('Prepaid'))
		nps_tnps_pre = tnps_pivoted.join(vf_pre, ['msisdn', 'partitioned_month', 'year', 'month', 'day'], 'inner').select(['nif', 'SEGMENTACION'] + cols)
		# nps_tnps_pre.count() #  1 205 594
		
		# TODO: Ono
		# ono_msisdn_numcliente = spark.table('raw_es.customerprofilecar_serviceow').select(['msisdn', 'num_cliente'])
		# ono_numcliente_nif = spark.table('raw_es.customerprofilecar_customerow').select(['num_cliente', 'nif'])
		# ono_msisdn_nif = ono_msisdn_numcliente.join(ono_numcliente_nif, ['num_cliente'], 'inner').select(['msisdn', 'nif'])
		# nps_tnps_ono = tnps_pivoted.join(ono_msisdn_nif, ['msisdn'], 'inner')
		# udfes_tables = SQLContext(spark.sparkContext).tables('udf_es').select('tableName').rdd.flatMap(lambda x: x).collect()
		# import re
		# r = re.compile('^pre_explicativas_4m_(\d{6})$')
		# months = [r.match(x).group(1) for x in udfes_tables if r.match(x)]
		# 
		# nps_tnps_ono.count() #
		
		self.tnps_pivoted = nps_tnps_pos.union(nps_tnps_pre) # TODO: .union(nps_tnps_ono)
		
		# Group by Id (NIF)
		#self.tnps_pivoted.printSchema()
		tnps_pivoted_by_id = self.tnps_pivoted.groupby('nif', 'partitioned_month', 'year', 'month', 'day').min()
		# FIXME: SEGMENTACION has disappeared here
		tnps_pivoted_by_id = self.fix_column_names(tnps_pivoted_by_id)
		#tnps_pivoted_by_id.printSchema()
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumnRenamed('min_TNPS01', 'TNPS01')
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS4',
					 when(tnps_pivoted_by_id['TNPS01'].isin(10, 9),      'PROMOTER')
					.when(tnps_pivoted_by_id['TNPS01'].isin(8, 7),       'NEUTRAL')
					.when(tnps_pivoted_by_id['TNPS01'].isin(6, 5, 4),    'SOFT DETRACTOR')
					.when(tnps_pivoted_by_id['TNPS01'].isin(3, 2, 1, 0), 'HARD DETRACTOR'))
					#.otherwise(False))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS',
					 when(tnps_pivoted_by_id['TNPS01'].isin('HARD DETRACTOR', 'SOFT DETRACTOR'), 'DETRACTOR')
					.otherwise(tnps_pivoted_by_id['TNPS01']))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS2DET',
					 when(tnps_pivoted_by_id['TNPS'] == 'DETRACTOR', tnps_pivoted_by_id['TNPS'])
					.otherwise('NON DETRACTOR'))
		tnps_pivoted_by_id = tnps_pivoted_by_id.withColumn('TNPS2PRO',
					 when(tnps_pivoted_by_id['TNPS'] == 'PROMOTER', tnps_pivoted_by_id['TNPS'])
					.otherwise('NON PROMOTER'))
		#tnps_pivoted_by_id.printSchema()

		# TODO: vf_pos_nif_segment = self.calculate_vfpos_segment()
		
		self.tnps_by_id = tnps_pivoted_by_id
		
		# print 'Joining NPS with TNPS (inner) ...'

		# self.nps_tnps = self.nps.join(self.tnps_pivoted, ['msisdn', 'partitioned_month', 'year', 'month', 'day'], 'inner')#.fillna(-1)
		# #self.nps_tnps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		# print 'Saving NPS with TNPS (inner) ...'
		# self.nps_tnps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-tnps-inner.txt', sep='\t', index=False, encoding='utf-8')
		# self.nps_tnps.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/nps_tnps_inner/')
		# subprocess.call('hdfs dfs -chmod -R o+rx /tmp/bbergua/nps_tnps_inner/', shell=True)
		# subprocess.call('hdfs dfs -chmod    o-x  /tmp/bbergua/nps_tnps_inner/partitioned_month=*/year=*/month=*/day=*/*', shell=True)

		# print 'Joining NPS with TNPS (left_outer) ...'

		# self.nps_tnps = self.nps.join(self.tnps_pivoted, ['msisdn', 'partitioned_month', 'year', 'month', 'day'], 'left_outer')#.fillna(-1)
		# #self.nps_tnps.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show(50)
		# print 'Saving NPS with TNPS (left_outer) ...'
		# self.nps_tnps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps-tnps-left_outer.txt', sep='\t', index=False, encoding='utf-8')
		# self.nps_tnps.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/nps_tnps_left_outer/')
		# subprocess.call('hdfs dfs -chmod -R o+rx /tmp/bbergua/nps_tnps_left_outer/', shell=True)
		# subprocess.call('hdfs dfs -chmod    o-x  /tmp/bbergua/nps_tnps_left_outer/partitioned_month=*/year=*/month=*/day=*/*', shell=True)

		# self.tnps_pivoted = self.tnps_pivoted.select(['nif', 'msisdn', 'SEGMENTACION', 'TNPS01', 'TNPS', 'TNPS4', 'partitioned_month', 'year', 'month', 'day'])
		
		# self.tnps_pivoted.printSchema()
		
		# tnps.select('year', 'month', 'day').distinct().groupby('year', 'month').count().sort('year', 'month').show(30)
		# tnps.filter('year=2017 AND month=4').groupby('year', 'month', 'day').count().sort('year', 'month', 'day').show(31)
		# tnps.filter('year=2017 AND month=4').select('year', 'month', 'day').distinct().groupby('year', 'month').count().sort('year', 'month').show(31)

		# tnps.select('VDN').distinct().count() # 164
		# tnps.filter('Literal LIKE "_Con qu_ probabilidad recomendar_a Vodafone a un amigo o compa_ero?"').select('VDN').distinct().count() # 74

		# tnps.filter('Literal LIKE "_Con qu_ probabilidad recomendar_a Vodafone a un amigo o compa_ero?"').


if __name__ == "__main__":
	# spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/customer_experience/src/main/python/DP_NPS_TNPS.py 2>&1 | tee salida.nps
	# tail -f -n +1 salida.nps | grep -v $'..\/..\/.. ..:..:..\|^[\t]\+at\|java.io.IOException'
	print '[' + time.ctime() + ']', 'Starting process ...'

	parser = argparse.ArgumentParser(description='VF_ES NPS & TNPS',
									 epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
	parser.add_argument('-m', '--month', metavar='<month>', type=str, help='YearMonth (YYYYMM) of the month to process, \'all\' to proces all months available, or leave empty for current month')
	parser.add_argument('-d', '--debug', action='store_true', help='show debug messages')
	args = parser.parse_args()
	print 'args =', args
	print 'month =', args.month
	#import sys
	#sys.exit()
	
	nps_tnps = DPNpsTnps(None, args.month, True)
	
	# nps_tnps.union_plain_nps() # The result of this must be anonymized
	# spark.stop()
	# sys.exit()
	
	# TODO
	# nps_tnps.generateNPS()
	# print 'Saving NPS ...'
	# nps_tnps.nps.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/nps/nps')
	# subprocess.call('hdfs dfs -chmod -R 755 /tmp/bbergua/nps/nps', shell=True)
	# subprocess.call('hdfs dfs -chmod    644 /tmp/bbergua/nps/nps/partitioned_month=*/year=*/month=*/day=*/*', shell=True)
	# nps_tnps.nps.toPandas().to_csv('/var/SP/data/home/bbergua/nps-analisis/nps_full.txt', sep='\t', index=False, encoding='utf-8')
	
	nps_tnps.generateTNPSByMsisdn()
	print 'Saving TNPS by Msisdn ...'
	nps_tnps.tnps_by_msisdn.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').saveAsTable('tests_es.tnps_msisdn')
	nps_tnps.tnps_by_msisdn.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/tnps/msisdn/')
	subprocess.call('hdfs dfs -chmod -R 755 /tmp/bbergua/tnps/msisdn/', shell=True)
	subprocess.call('hdfs dfs -chmod    644 /tmp/bbergua/tnps/msisdn/partitioned_month=*/year=*/month=*/day=*/*', shell=True)

	nps_tnps.generateTNPSById()
	print 'Saving TNPS by Id ...'
	nps_tnps.tnps_by_id.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').saveAsTable('tests_es.tnps_id')
	nps_tnps.tnps_by_id.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/tnps/id/')
	subprocess.call('hdfs dfs -chmod -R 755 /tmp/bbergua/tnps/id/', shell=True)
	subprocess.call('hdfs dfs -chmod    644 /tmp/bbergua/tnps/id/partitioned_month=*/year=*/month=*/day=*/*', shell=True)
	
	print '[' + time.ctime() + ']', 'Process finished'

	spark.stop()
	
	print '[' + time.ctime() + ']', 'SparkSession stopped'
