#!/usr/bin/env python
# -*- coding: utf-8 -*-

#SPARK_COMMON_OPTS+=" --conf spark.port.maxRetries=50 "
#SPARK_COMMON_OPTS+=" --conf spark.network.timeout=10000000 "
#SPARK_COMMON_OPTS+=" --conf spark.executor.heartbeatInterval=60 "
#SPARK_COMMON_OPTS+=" --conf spark.yarn.executor.memoryOverhead=2G "
#export SPARK_COMMON_OPTS
#pyspark2 $SPARK_COMMON_OPTS
#spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G ~/fy17.capsule/customer_experience/src/main/python/DP_Call_Centre_Calls.py 2>&1 | tee salida.ccc
#tail -f -n +1 salida.ccc | grep --color=auto -v -E '..\/..\/.. ..:..:..'

import argparse
import re
import subprocess
import sys
import time
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, concat, concat_ws, date_format, dayofmonth, format_string, from_unixtime, length, lit, lower, lpad, month, regexp_replace, translate, udf, unix_timestamp, year, when
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

class DPCallCentreCalls:

	def __init__(self, spark_other=None, filter_month=None, debug=False):
		self.debug = debug

		self.filter_month = filter_month
		if filter_month is None:
			from datetime import datetime
			self.filter_month = '%d%02d' % (datetime.today().year, datetime.today().filter_month)
		else:
			m = re.search('^(\d{6}|all)$', filter_month)
			if m is None:
				print 'ERROR: month must be a YearMonth, i.e. a six digit number (YYYYMM), or \'all\''
				sys.exit(1)
			
		#self.months_found_set = set()

		self.all_interactions = None

		self.pivoted_by_msisdn = None
		self.pivoted_by_id = None

		global spark
		spark = spark_other
		if spark_other is None:
			spark = SparkSession \
				.builder \
				.appName("VF_ES Call Centre Calls") \
				.enableHiveSupport() \
				.getOrCreate()
				#        .config("spark.port.maxRetries", "50")
				#        .config("spark.network.timeout", "10000000")
				#        .config("spark.executor.heartbeatInterval", "60")
				#        .config("spark.some.config.option", "some-value")

			print '[' + time.ctime() + ']', 'SparkSession created'
		
		#self.prepareFeatures()
		
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

	def df_to_csv(df, cols=None, sep='\t'):
		from pyspark.sql.dataframe import DataFrame
		from pyspark.sql.types import Row

		if type(df) is DataFrame:
			collected = df.collect()
			if cols is None:
				cols = df.columns
		elif (type(df) is list) and (type(df[0]) is Row):
			collected = df
			if cols is None:
				cols = collected[0].asDict().keys()
		else:
			raise TypeError('df argument is of '+str(type(df))+'. It must be either a DataFrame or a collected DataFrame (a list of Row)')
		
		output = sep.join(cols) + '\n'
		
		for r in collected:
			line = []
			row_d = r.asDict()
			for c in cols:
				#print c, row_d[c]
				line.append(str(row_d[c]))
			#print '-----'
			output = output + sep.join(line) + '\n'
		
		return output
    
	def joinNIFVFCAR(self, interactions_VF):
		# sqlContext.tableNames("udf_es")
		# spark.catalog.listTables()
		
		# sqlContext = SQLContext(spark.sparkContext)
		udfes_tables = SQLContext(spark.sparkContext).tables('udf_es').select('tableName').rdd.flatMap(lambda x: x).collect()
		# udfes_tables = SQLContext(spark.sparkContext).tableNames('udf_es')
		
		print 'Loading VF Prepaid CAR ...'

		import re
		r = re.compile('^pre_explicativas_4m_(\d{6})$')
		months = [r.match(x).group(1) for x in udfes_tables if r.match(x)]
		# months = map(lambda i: i[-6:], filter(lambda item: item.startswith('pre_explicativas_4m_'), udfes_tables))
		
		actual_months = []
		vfpre_data = None
		for m in months:
			try:
				tmp = spark.table('udf_es.pre_explicativas_4m_' + m)
			except AnalysisException as e:
				print '\tFailed', m, ':', e
			else:
				tmp = tmp.select('nif', 'msisdn').withColumn('partitioned_month', lit(m))
				actual_months.append(m)
				if vfpre_data is None:
					vfpre_data = tmp
				else:
					vfpre_data = vfpre_data.union(tmp)
				# vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
				print '\tLoaded', m

		print 'Appending CAR prefix to columns ...'
		for c in vfpre_data.columns:
			if c not in ['nif', 'msisdn', 'year', 'month', 'day', 'partitioned_month']:
				vfpre_data = vfpre_data.withColumnRenamed(c, 'CAR_' + c)

		vfpre_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		
		print 'Loading VF Postpaid CAR ...'
        
		months = map(lambda i: i[-6:], filter(lambda item: item.startswith('par_explic_lin_6m_'), udfes_tables))
		print months
		actual_months = []
		vfpos_data = None
		for m in months:
			try:
				tmp = spark.table('udf_es.par_explic_lin_6m_' + m)
			except AnalysisException as e:
				print '\tFailed', m, ':', e
			else:
				tmp = tmp.select('nif', 'msisdn').withColumn('partitioned_month', lit(m))
				if 'partitioned_month_2' in tmp.columns:
					tmp = tmp.drop('partitioned_month_2')
				actual_months.append(m)
				if vfpos_data is None:
					vfpos_data = tmp
				else:
					vfpos_data = vfpos_data.union(tmp)
				# vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
				print '\tLoaded', m

		print 'Appending CAR prefix to columns ...'
		for c in vfpos_data.columns:
			if c not in ['nif', 'msisdn', 'year', 'month', 'day', 'partitioned_month']:
				vfpos_data = vfpos_data.withColumnRenamed(c, 'CAR_' + c)

		vfpos_data.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()

		# print vfpre_data.columns
		# print vfpos_data.columns
		
		vfcar = vfpre_data.select('nif', 'msisdn', 'partitioned_month').union(vfpos_data.select('nif', 'msisdn', 'partitioned_month')).withColumnRenamed('nif', 'CAR_nif')
		
		interactions_VF = interactions_VF.join(vfcar, on=['msisdn', 'partitioned_month'], how='left_outer')
		
		interactions_VF = interactions_VF.withColumn('nif', when(interactions_VF['CAR_nif'].isNotNull(), interactions_VF['CAR_nif'])
															.otherwise(interactions_VF['nif'])).drop('CAR_nif')

		return interactions_VF

	def prepareFeatures(self,closing_day,starting_day):
		# Maestro de Agrupaciones en buckets de COPS
		buckets = spark.read.csv('/tmp/bbergua/raw/Agrup_Buckets_unific/', sep=';', header=True)
		buckets = buckets.fillna('NA', ['Bucket', 'Sub_Bucket'])
		buckets = buckets.withColumn('Bucket',  when(buckets['Bucket'] == 'Churn/cancellations', 'Churn/Cancellations')
											   .when(buckets['Bucket'] == 'device delivery/repair', 'Device delivery/repair')
											   .when(buckets['Bucket'] == 'product and Service management', 'Product and Service management')
											   .when(buckets['Bucket'] == 'Voice and Mobile data incidences and support', 'Voice and mobile data incidences and support')
											   .otherwise(buckets['Bucket']))
		buckets = buckets.withColumn('Sub_Bucket',   when(buckets['Sub_Bucket'] == 'Customer Service Process', 'Customer service process')
													.when(buckets['Sub_Bucket'] == 'Pin/Puk', 'PIN/PUK')
													.when(buckets['Sub_Bucket'] == 'Standard products ', 'Standard products')
													.otherwise(buckets['Sub_Bucket']))
		buckets = buckets.withColumn('Bucket_Sub_Bucket', concat_ws('_', lit('Bucket_Sub_Bucket'), buckets.Bucket, buckets.Sub_Bucket))
		buckets = buckets.withColumn('Bucket', concat_ws('_', lit('Bucket'), buckets.Bucket))
		buckets = buckets.withColumn('Sub_Bucket', concat_ws('_', lit('Sub_Bucket'), buckets.Sub_Bucket))
		if self.debug:
			buckets.printSchema()
			print 'Num Bucket', buckets.select('Bucket').distinct().count()
			print 'Num Sub_Bucket', buckets.select('Sub_Bucket').distinct().count()
			print 'Num Bucket-Sub_Bucket', buckets.select('Bucket', 'Sub_Bucket').distinct().count()
			print 'Num Bucket_Sub_Bucket', buckets.select('Bucket_Sub_Bucket').distinct().count()
			# buckets.show()

		############
		# Vodafone #
		############

		# Alicia cruza Interacciones VF por Id_servicio
		# Beatriz: vista_nc_interact_actual.serial_no

		if self.debug:
			print 'Loading raw VF Call Centre Calls ...'
		
		# En DWH: clientesp_iq_v.nc_interact
		interactions_VF = spark.table('raw_es.callcentrecalls_interactionvf')
		interactions_VF = interactions_VF.withColumn('year', year('CREATE_DATE')).withColumn('month', month('CREATE_DATE')).withColumn('day', dayofmonth('CREATE_DATE'))
		interactions_VF = interactions_VF.withColumn('partitioned_month', format_string('%d%02d', interactions_VF.year, interactions_VF.month))
		#self.months_found_set = self.months_found_set.union(set(interactions_VF.select('partitioned_month').distinct().rdd.map(lambda x: x['partitioned_month']).collect()))
		#if self.filter_month is not 'all':
		#	interactions_VF = interactions_VF.filter('partitioned_month == "%s"' % self.filter_month)
		interactions_VF = interactions_VF.where ( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                          						& (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
		interactions_VF = interactions_VF.distinct()

		if self.debug:
			interactions_VF.select('year', 'month').groupby('year', 'month').count().sort('year', 'month').show()
			# interactions_VF.groupby('DIRECTION').count().sort('count', ascending=False).show()

		interactions_VF = interactions_VF.filter("DIRECTION IN ('De entrada', 'De Entrada', 'Entrante')")
		# interactions_VF.groupby('TYPE_TD').count().sort('count', ascending=False).show(100)
		interactions_VF = interactions_VF.filter("TYPE_TD IN ('Llamada', 'llamada', 'Llamada Telefono', 'BILLSHOCK', '...')") # TYPE_TD IN ('Llamada', '...')
		# interactions_VF.groupby('X_WORKGROUP').count().sort('count', ascending=False).show(37)
		interactions_VF = interactions_VF.filter("X_WORKGROUP NOT LIKE 'BO%'")   # X_WORKGROUP NOT LIKE 'BO%'
		interactions_VF = interactions_VF.filter("X_WORKGROUP NOT LIKE 'IVR%'")  # X_WORKGROUP NOT LIKE 'IVR%'
		interactions_VF = interactions_VF.filter("X_WORKGROUP NOT LIKE 'SIVA%'") # X_WORKGROUP NOT LIKE 'SIVA%'
		interactions_VF = interactions_VF.filter("X_WORKGROUP NOT LIKE 'B.O%'")  # X_WORKGROUP NOT LIKE 'B.O%'
		# interactions_VF.select('INTERACT_ID', 'year', 'month').groupby('INTERACT_ID', 'year', 'month').count().sort('count', ascending=False).show()
		# interactions_VF.select('INTERACT_ID', 'year', 'month').distinct().groupby('year', 'month').count().sort('year', 'month').show()
		# interactions_VF.drop('service_processed_at', 'service_file_id').distinct().select('INTERACT_ID', 'year', 'month').groupby('year', 'month').count().sort('year', 'month').show()
		interactions_VF = interactions_VF.drop('service_processed_at', 'service_file_id').distinct()
		# interactions_VF.select('year', 'month').groupby('year', 'month').count().sort('year', 'month').show()
		# interactions_VF.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		# interactions_VF.columns

		# udf1 = udf(lambda x:x[0:-1], StringType())
		# interactions_VF = interactions_VF.withColumn('X_INTERACT2RELATED',
												 # when(interactions_VF['X_INTERACT2RELATED'].endswith('.'), udf1('X_INTERACT2RELATED'))
												 # .otherwise(interactions_VF['X_INTERACT2RELATED']))
		# interactions_VF.select('X_INTERACT2RELATED', 'X_ANI', 'X_MSISDN_ORIGINAL').filter('X_INTERACT2RELATED != "-2"').show()
		# +------------------+---------+-----------------+
		# |X_INTERACT2RELATED|    X_ANI|X_MSISDN_ORIGINAL|
		# +------------------+---------+-----------------+
		# |         843094231|         |                 |
		# |         843128807|968595467|        913583327|
		# |         843098661|         |                 |
		# |         842746638|639339242|        645745382|
		# |         842983403|         |                 |
		# |         843107509|657044660|        608550258|
		# |         843117381|914393419|        951943694|
		# |         843133991|663003562|        682482138|
		# |         842974335|         |                 |
		# |         842894448|         |                 |
		# |         843126149|635101896|        621168100|
		# |         842785466|         |                 |
		# |         843014524|637365370|        694087898|
		# |         842722532|616206054|        639258464|
		# |         842888486|         |                 |
		# |         843106743|         |                 |
		# |         842745448|695663108|        672430109|
		# |         843171341|603472243|        680477184|
		# |         842676467|677453370|        621152361|
		# |         843089995|         |                 |
		# +------------------+---------+-----------------+

		# interactions_VF.select('X_INTERACT2RELATED').filter('X_INTERACT2RELATED != "-2"').count() #  14 971 038
		# interactions_VF.select('X_ANI').filter('X_ANI != ""').count()                             #  20 064 173
		# interactions_VF.select('X_MSISDN_ORIGINAL').filter('X_MSISDN_ORIGINAL != ""').count()     # 132 255 732
		# interactions_VF.select('X_SERVICE_ID').filter('X_SERVICE_ID != ""').count()               # 136 186 992

		# interactions_VF.select('X_INTERACT2RELATED').distinct().count() #  2 862 654
		# interactions_VF.select('X_ANI').distinct().count()              #  4 078 374
		# interactions_VF.select('X_MSISDN_ORIGINAL').distinct().count()  # 14 031 215
		# interactions_VF.select('X_SERVICE_ID').distinct().count()       # 15 012 359


		# vf_pos_ac_final = spark.table("raw_es.vf_pos_ac_final")

		# coun = interactions_VF.select('X_INTERACT2RELATED', 'partitioned_month').filter('X_INTERACT2RELATED != "-2"').withColumnRenamed('X_INTERACT2RELATED', 'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 313 488

		# coun = interactions_VF.select('X_ANI',              'partitioned_month').filter('X_ANI              != ""').withColumnRenamed('X_ANI',                'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 930 963

		# coun = interactions_VF.select('X_MSISDN_ORIGINAL',  'partitioned_month').filter('X_MSISDN_ORIGINAL  != ""').withColumnRenamed('X_MSISDN_ORIGINAL',    'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 57 215 461

		# coun = interactions_VF.select('X_SERVICE_ID',       'partitioned_month').filter('X_SERVICE_ID       != ""').withColumnRenamed('X_SERVICE_ID',         'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 57 413 835


		# coun = interactions_VF.select('X_INTERACT2RELATED', 'partitioned_month').distinct().withColumnRenamed('X_INTERACT2RELATED', 'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 60 747

		# coun = interactions_VF.select('X_ANI',              'partitioned_month').distinct().withColumnRenamed('X_ANI',              'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 329 607

		# coun = interactions_VF.select('X_MSISDN_ORIGINAL',  'partitioned_month').distinct().withColumnRenamed('X_MSISDN_ORIGINAL',  'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 10 366 687

		# coun = interactions_VF.select('X_SERVICE_ID',       'partitioned_month').distinct().withColumnRenamed('X_SERVICE_ID',       'x_id_red').join(vf_pos_ac_final, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 10 366 687

		# interactions_VF.select('X_INTERACT2RELATED', 'X_ANI', 'X_MSISDN_ORIGINAL').filter('X_MSISDN_ORIGINAL == "" AND X_ANI != ""').show()
		# interactions_VF.select('X_INTERACT2RELATED', 'X_ANI', 'X_MSISDN_ORIGINAL').filter('X_MSISDN_ORIGINAL == "" AND X_ANI != ""').count() # 3 931 260

		# Fill NA's in X_MSISDN_ORIGINAL column
		interactions_VF = interactions_VF.withColumn('X_MSISDN_ORIGINAL',
													 when(interactions_VF['X_MSISDN_ORIGINAL'] == "", interactions_VF['X_ANI'])
													 .otherwise(interactions_VF['X_MSISDN_ORIGINAL']))

		# interactions_VF = spark.table('raw_es.callcentrecalls_interactionvf')
		# interactions_VF = interactions_VF.withColumn('length', length('X_MSISDN_ORIGINAL'))
		# interactions_VF.groupby('length').count().sort('length').show(50)
		# +------+---------+
		# |length|    count|
		# +------+---------+
		# |     0| 21323715|
		# |     1|        5|
		# |     3|        2|
		# |     4|   405563|
		# |     5|    44852|
		# |     6|        2|
		# |     7|       95|
		# |     8|      356|
		# |     9|134605037|
		# |    10|      988|
		# |    11|     2901|
		# |    12|     3856|
		# |    13|   871936|
		# |    14|   179560|
		# |    15|    62212|
		# |    16|       19|
		# |    17|      329|
		# |    18|     9118|
		# |    19|       38|
		# |    20|        4|
		# |    21|        8|
		# |    22|       27|
		# |    23|        6|
		# |    24|       12|
		# |    25|       39|
		# |    26|        4|
		# |    27|        1|
		# |    28|       18|
		# |    29|        2|
		# |    31|        1|
		# |    33|        1|
		# +------+---------+
		# interactions_VF.filter('length >= 19').show(50)
		# interactions_VF.filter(interactions_VF.X_MSISDN_ORIGINAL.like('%VF%')).show()

		# Clean X_MSISDN_ORIGINAL
		get_valid_msisdn = udf(lambda v: v if v.startswith('VF') else v[-9:], StringType())
		interactions_VF = interactions_VF.withColumn('length', length('X_MSISDN_ORIGINAL')).filter('length >= 9').withColumn('X_MSISDN_ORIGINAL', get_valid_msisdn('X_MSISDN_ORIGINAL')).drop('length')
		# interactions_VF.select('X_MSISDN_ORIGINAL').withColumn('length', length('X_MSISDN_ORIGINAL')).groupby('length').count().sort('length').show()
		# +------+---------+
		# |length|    count|
		# +------+---------+
		# |     9|134649155|
		# |    13|   858479|
		# |    14|   171660|
		# |    15|    56823|
		# +------+---------+


		# interactions_VF.groupby('TYPE_TD').count().sort('count', ascending=False).show()
		# interactions_VF.groupby('REASON_1').count().sort('count', ascending=False).show()
		# interactions_VF.groupby('REASON_2').count().sort('count', ascending=False).show()
		# interactions_VF.groupby('REASON_3').count().sort('count', ascending=False).show()
		# interactions_VF.groupby('RESULT_TD').count().sort('count', ascending=False).show()

		# Dict con todos los conceptos y subconceptos de interacciones que se han analizado
		global conceptos_interacciones
		conceptos_interacciones = {}

		# interactions_VF.filter(interactions_VF.REASON_1.like('%veria%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +-----------------------+------+
		# |REASON_1               |count |
		# +-----------------------+------+
		# |Averia DSL NET         |535591|
		# |Averia Fibra NET       |298133|
		# |Averia DSL TF          |97387 |
		# |Averia Fibra TV        |85115 |
		# |Averia Fibra TF        |66208 |
		# |Averia Neba Fibra NET  |49649 |
		# |Averia Movil           |48160 |
		# |Averia APP Stream      |47077 |
		# |Averia DSL TV          |39281 |
		# |Averia Neba Fibra TV   |14175 |
		# |Averia Modem/Rout movil|11635 |
		# |Averia Neba Fibra TF   |6327  |
		# |Averia Oficina Vf      |5018  |
		# |Averia VF WF/WF Neg    |2804  |
		# |Averia Modem/Rout(mv)  |197   |
		# |Averia cliente         |13    |
		# +-----------------------+------+
		interactions_VF = interactions_VF.withColumn('Raw_Averia',
													  when(interactions_VF.REASON_1.like('Averia%TV'),          'Raw_Averia_TV')
													 .when(interactions_VF.REASON_1.like('Averia Fibra%'),      'Raw_Averia_Fibra')
													 .when(interactions_VF.REASON_1.like('Averia Neba%'),       'Raw_Averia_Neba')
													 .when(interactions_VF.REASON_1.like('Averia DSL%'),        'Raw_Averia_DSL')
													 .when(interactions_VF.REASON_1.like('Averia Modem/Rout%'), 'Raw_Averia_Modem/Router')
													 .when(interactions_VF.REASON_1.like('Averia%'),            'Raw_Averia_Resto'))
		conceptos_interacciones['Raw_Averia'] = ['Raw_Averia_DSL', 
												 'Raw_Averia_Fibra', 
												 'Raw_Averia_TV', 
												 'Raw_Averia_Resto', 
												 'Raw_Averia_Neba', 
												 'Raw_Averia_Modem/Router']
		# interactions_VF.groupby('Averia').count().sort('count', ascending=False).show(truncate=False)
		# +-------------------+---------+
		# |Averia             |count    |
		# +-------------------+---------+
		# |null               |156203937|
		# |Averia DSL         |632978   |
		# |Averia Fibra       |364341   |
		# |Averia TV          |138571   |
		# |Averia Resto       |103072   |
		# |Averia Neba        |55976    |
		# |Averia Modem/Router|11832    |
		# +-------------------+---------+

		# interactions_VF.filter(interactions_VF.REASON_1.like('%rovis%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +-------------------------+------+
		# |REASON_1                 |count |
		# +-------------------------+------+
		# |Provision DSL            |114238|
		# |Provision Fibra          |92904 |
		# |Provisión Neba Fibra     |56797 |
		# |Provision                |45504 |
		# |Inc Provis Neba Fibr     |41164 |
		# |Inc Provision Fibra      |34051 |
		# |Inc Provision DSL        |30116 |
		# |Tecnico provision        |23521 |
		# |Tecnico Provision        |12306 |
		# |KO ADSL provision        |8418  |
		# |Provision PYMES          |4002  |
		# |Provision Neba Fibra     |2627  |
		# |Provisioning             |709   |
		# |Baja Postprovision       |16    |
		# |Postprovision            |2     |
		# |Provision Fibra Indirecta|2     |
		# +-------------------------+------+
		interactions_VF = interactions_VF.withColumn('Raw_Provision',
													  when(interactions_VF.REASON_1.like('Provisi%Neba%'),      'Raw_Provision_Neba')
													 .when(interactions_VF.REASON_1.like('Provisi%Indirecta%'), 'Raw_Provision_Neba')
													 .when(interactions_VF.REASON_1.like('Provisi%Fibra%'),     'Raw_Provision_Fibra')
													 .when(interactions_VF.REASON_1.like('Provisi%DSL%'),       'Raw_Provision_DSL')
													 .when(interactions_VF.REASON_1.like('%DSL%rovisi%'),       'Raw_Provision_DSL')
													 .when(interactions_VF.REASON_1.like('%Postprovision'),     'Raw_Provision_Resto')
													 .when(interactions_VF.REASON_1.like('Tecnico%rovision%'),  'Raw_Provision_Resto')
													 .when(interactions_VF.REASON_1.like('Provision%'),         'Raw_Provision_Resto'))
		conceptos_interacciones['Raw_Provision'] = ['Raw_Provision_Neba', 
													'Raw_Provision_Fibra', 
													'Raw_Provision_DSL', 
													'Raw_Provision_Resto']
		# interactions_VF.groupby('Provision').count().sort('count', ascending=False).show(truncate=False)
		# +---------------+---------+
		# |Provision      |count    |
		# +---------------+---------+
		# |null           |157149661|
		# |Provision DSL  |122656   |
		# |Provision Fibra|92904    |
		# |Provision Resto|86060    |
		# |Provision Neba |59426    |
		# +---------------+---------+

		# interactions_VF.filter(interactions_VF.REASON_1.like('%Inc%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +--------------------+------+
		# |REASON_1            |count |
		# +--------------------+------+
		# |Incidencia          |466446|
		# |Incidencia tecnica  |69183 |
		# |Inc Provis Neba Fibr|41164 |
		# |Inc Provision Fibra |34051 |
		# |Inc Provision DSL   |30116 |
		# |Reclam Incidencia   |1815  |
		# |Incidencia SGI      |741   |
		# |Incomunicación Fija |534   |
		# |Retraso/Incidencia  |278   |
		# |Incidencias SGI     |160   |
		# |Inc. Clarify        |10    |
		# |Incidencia Tecnica  |4     |
		# |Incidencias tecnicas|1     |
		# +--------------------+------+
		interactions_VF = interactions_VF.withColumn('Raw_Incidencia',
													  when(interactions_VF.REASON_1.like('Inc Provis%Neba%'),     'Raw_Incidencia_Provision_Neba')
													 .when(interactions_VF.REASON_1.like('Inc Provision Fibra%'), 'Raw_Incidencia_Provision_Fibra')
													 .when(interactions_VF.REASON_1.like('Inc Provision DSL%'),   'Raw_Incidencia_Provision_DSL')
													 .when(interactions_VF.REASON_1.like('%Incidencia%ecnica%'),  'Raw_Incidencia_Tecnica')
													 .when(interactions_VF.REASON_1.like('%Incidencia%SGI%'),     'Raw_Incidencia_SGI')
													 .when(interactions_VF.REASON_1.like('%Inc.%'),               'Raw_Incidencia_Resto')
													 .when(interactions_VF.REASON_1.like('%ncidencia%'),          'Raw_Incidencia_Resto'))
		conceptos_interacciones['Raw_Incidencia'] = ['Raw_Incidencia_Provision_Neba', 
													 'Raw_Incidencia_Provision_Fibra', 
													 'Raw_Incidencia_Provision_DSL', 
													 'Raw_Incidencia_Tecnica', 
													 'Raw_Incidencia_SGI', 
													 'Raw_Incidencia_Resto']
		# interactions_VF.groupby('Incidencia').count().sort('count', ascending=False).show(truncate=False)
		# +--------------------------+---------+
		# |Incidencia                |count    |
		# +--------------------------+---------+
		# |null                      |156866738|
		# |Incidencia Resto          |468549   |
		# |Incidencia Tecnica        |69188    |
		# |Incidencia Provision Neba |41164    |
		# |Incidencia Provision Fibra|34051    |
		# |Incidencia Provision DSL  |30116    |
		# |Incidencia SGI            |901      |
		# +--------------------------+---------+

		# interactions_VF.filter(interactions_VF.REASON_1.like('%Cons%')).groupby('REASON_1').count().sort('count', ascending=False).show(50, truncate=False)
		# +-----------------------+------+
		# |REASON_1               |count |
		# +-----------------------+------+
		# |Consulta ficha         |386441|
		# |Cons tec Movil         |213780|
		# |Cons tec DSL NET       |127571|
		# |Cons tec Fibra NET     |104457|
		# |Cons tecn Oficina Vf   |40147 |
		# |Cons tec Modem/Rout(mv)|38805 |
		# |Cons tec DSL TF        |22814 |
		# |Cons tec Fibra TV      |20302 |
		# |Cons tec Fibra TF      |17365 |
		# |Reclam Consumo excesivo|15940 |
		# |Cons tec DSL TV        |15609 |
		# |Cons tec APP Stream    |12419 |
		# |Cons tec Neb Fibr NET  |8909  |
		# |Consultas              |7137  |
		# |Cons tec Neb Fibr TV   |3960  |
		# |Consulta Estado        |2554  |
		# |Cons tec VF WF/WF Neg  |2170  |
		# |Consulta Post-venta    |2002  |
		# |Cons tec Neba Fibra TF |1568  |
		# |Consulta Ficha         |976   |
		# +-----------------------+------+
		interactions_VF = interactions_VF.withColumn('Raw_Consulta',
													  when(interactions_VF.REASON_1.like('%Cons tec%TV%'),         'Raw_Consulta_Tecnica_TV')
													 .when(interactions_VF.REASON_1.like('%Cons tec Fibra%'),      'Raw_Consulta_Tecnica_Fibra')
													 .when(interactions_VF.REASON_1.like('%Cons tec Neb%'),        'Raw_Consulta_Tecnica_Neba')
													 .when(interactions_VF.REASON_1.like('%Cons tec DSL%'),        'Raw_Consulta_Tecnica_DSL')
													 .when(interactions_VF.REASON_1.like('%Cons tec Movil%'),      'Raw_Consulta_Tecnica_Movil')
													 .when(interactions_VF.REASON_1.like('%Cons tec Modem/Rout%'), 'Raw_Consulta_Tecnica_Modem/Router')
													 .when(interactions_VF.REASON_1.like('%Cons tec%'),            'Raw_Consulta_Tecnica_Resto')
													 .when(interactions_VF.REASON_1.like('%Consulta _icha%'),      'Raw_Consulta_Ficha')
													 .when(interactions_VF.REASON_1.like('%Consulta%'),            'Raw_Consulta_Resto'))
		conceptos_interacciones['Raw_Consulta'] = ['Raw_Consulta_Tecnica_TV', 
												   'Raw_Consulta_Tecnica_Fibra', 
												   'Raw_Consulta_Tecnica_Neba', 
												   'Raw_Consulta_Tecnica_DSL', 
												   'Raw_Consulta_Tecnica_Movil', 
												   'Raw_Consulta_Tecnica_Modem/Router', 
												   'Raw_Consulta_Tecnica_Resto', 
												   'Raw_Consulta_Ficha', 
												   'Raw_Consulta_Resto']
		# # interactions_VF.groupby('Consulta').count().sort('count', ascending=False).show(truncate=False)
		# # +---------------------+---------+
		# # |Consulta             |count    |
		# # +---------------------+---------+
		# # |null                 |156536026|
		# # |Consulta Resto       |399541   |
		# # |Consulta Movil       |213780   |
		# # |Consulta DSL         |150385   |
		# # |Consulta Fibra       |121822   |
		# # |Consulta TV          |39871    |
		# # |Consulta Modem/Router|38805    |
		# # |Consulta Neba        |10477    |
		# # +---------------------+---------+

		# interactions_VF.filter(interactions_VF.REASON_1.like('%Inf%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +----------------------+--------+
		# |REASON_1              |count   |
		# +----------------------+--------+
		# |Informacion           |70464205|
		# |Info/Modificacion     |283717  |
		# |Información           |189427  |
		# |Informacion proceso   |135037  |
		# |Informacion Comercial |24280   |
		# |VF_Informacion        |9325    |
		# |Informacion ADSL      |1103    |
		# |Informacion al cliente|660     |
		# |Información al cliente|8       |
		# |Informacion fibra     |6       |
		# +----------------------+--------+
		interactions_VF = interactions_VF.withColumn('Raw_Informacion',
													  when(interactions_VF.REASON_1.like('%Info%'), 'Raw_Informacion'))
		conceptos_interacciones['Raw_Informacion'] = ['Raw_Informacion']
		# interactions_VF.groupby('Informacion').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%actur%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +---------------+-------+
		# |REASON_1       |count  |
		# +---------------+-------+
		# |Factura        |1755986|
		# |Facturacion    |423928 |
		# |Factura/Recarga|7480   |
		# |Error factura  |9      |
		# +---------------+-------+
		interactions_VF = interactions_VF.withColumn('Raw_Factura',
													  when(interactions_VF.REASON_1.like('%actura%'), 'Raw_Factura'))
		conceptos_interacciones['Raw_Factura'] = ['Raw_Factura']
		# interactions_VF.groupby('Factura').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%Prod%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +-----------------------------+-------+
		# |REASON_1                     |count  |
		# +-----------------------------+-------+
		# |Productos/Servicios          |1758937|
		# |Productos y Servicios        |365491 |
		# |Productos y servicios - Datos|19037  |
		# |Productos Voz                |4195   |
		# |Productos Datos              |3388   |
		# |Productos y servicios - Voz  |3322   |
		# |Productos                    |35     |
		# +-----------------------------+-------+
		interactions_VF = interactions_VF.withColumn('Raw_Productos',
													  when(interactions_VF.REASON_1.like('%Productos%Voz'),   'Raw_Productos_Voz')
													 .when(interactions_VF.REASON_1.like('%Productos%Datos'), 'Raw_Productos_Datos')
													 .when(interactions_VF.REASON_1.like('%Productos%'),      'Raw_Productos_Resto'))
		conceptos_interacciones['Raw_Productos'] = ['Raw_Productos_Voz', 
													'Raw_Productos_Datos', 
													'Raw_Productos_Resto']
		# interactions_VF.groupby('Productos').count().sort('count', ascending=False).show(truncate=False)
		# +---------------+---------+
		# |Productos      |count    |
		# +---------------+---------+
		# |null           |155356302|
		# |Productos Resto|2124463  |
		# |Productos Datos|22425    |
		# |Productos Voz  |7517     |
		# +---------------+---------+

		# interactions_VF.filter(interactions_VF.REASON_1.like('%obro%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Cobro',
													  when(interactions_VF.REASON_1.like('%obro%'), 'Raw_Cobro'))
		conceptos_interacciones['Raw_Cobro'] = ['Raw_Cobro']
		# interactions_VF.groupby('Cobro').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%ransf%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Transferencia',
													  when(interactions_VF.REASON_1.like('%ransf%'), 'Raw_Transferencia'))
		conceptos_interacciones['Raw_Transferencia'] = ['Raw_Transferencia']
		# interactions_VF.groupby('Transferencia').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%ierre%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Cierre',
													  when(interactions_VF.REASON_1.like('%ierre%'), 'Raw_Cierre'))
		conceptos_interacciones['Raw_Cierre'] = ['Raw_Cierre']
		# interactions_VF.groupby('Cierre').count().sort('count', ascending=False).show(truncate=False)

		# interactions_VF.filter(interactions_VF.REASON_1.like('%frecimiento%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +----------------------+-----+
		# |REASON_1              |count|
		# +----------------------+-----+
		# |Ofrecimiento comercial|663  |
		# +----------------------+-----+
		interactions_VF = interactions_VF.withColumn('Raw_Ofrecimiento',
													  when(interactions_VF.REASON_1.like('%frecimiento%'), 'Raw_Ofrecimiento'))
		conceptos_interacciones['Raw_Ofrecimiento'] = ['Raw_Ofrecimiento']
		# interactions_VF.groupby('Ofrecimiento').count().sort('count', ascending=False).show(truncate=False)

		# interactions_VF.filter(interactions_VF.REASON_1.like('%esactivaci%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +------------------------+-----+
		# |REASON_1                |count|
		# +------------------------+-----+
		# |Desactivacion Movil     |75036|
		# |Desactivacion NET       |37215|
		# |Desactivacion TV        |22032|
		# |Desactivacion Total     |17218|
		# |Desactivacion USB       |9345 |
		# |Desactivacion Fijo      |7491 |
		# |Desactivacion NET+Movil |3351 |
		# |Desactivacion ITC       |3345 |
		# |Desactivacion           |2587 |
		# |Desactivacion NET+TV    |2401 |
		# |Desactivacion Movil+Fijo|1050 |
		# |Desactivacion Movil+TV  |395  |
		# +------------------------+-----+
		interactions_VF = interactions_VF.withColumn('Raw_Desactivacion',
													   when(interactions_VF.REASON_1.like('%Desactivacion BA%'),    'Raw_Desactivacion_BA+Movil+TV')
													  .when(interactions_VF.REASON_1.like('%Desactivacion TV%'),    'Raw_Desactivacion_TV')
													  .when(interactions_VF.REASON_1.like('%Desactivacion Movil%'), 'Raw_Desactivacion_Movil')
													  .when(interactions_VF.REASON_1.like('%Desactivacion Total%'), 'Raw_Desactivacion_Total')
													  .when(interactions_VF.REASON_1.like('%Desactivacion NET%'),   'Raw_Desactivacion_NET')
													  .when(interactions_VF.REASON_1.like('%Desactivacion Fijo%'),  'Raw_Desactivacion_Fijo')
													  .when(interactions_VF.REASON_1.like('%Desactivacion USB%'),   'Raw_Desactivacion_USB')
													  .when(interactions_VF.REASON_1.like('%Desactivaci_n%'),       'Raw_Desactivacion_Resto'))
		conceptos_interacciones['Raw_Desactivacion'] = ['Raw_Desactivacion_BA+Movil+TV', 
														'Raw_Desactivacion_TV', 
														'Raw_Desactivacion_Movil', 
														'Raw_Desactivacion_Total', 
														'Raw_Desactivacion_NET', 
														'Raw_Desactivacion_Fijo', 
														'Raw_Desactivacion_USB', 
														'Raw_Desactivacion_Resto']

		# interactions_VF.filter(interactions_VF.REASON_1.like('%menos%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Pagar_menos',
													  when(interactions_VF.REASON_1.like('%menos%'), 'Raw_Pagar_menos'))
		conceptos_interacciones['Raw_Pagar_menos'] = ['Raw_Pagar_menos']
		# interactions_VF.groupby('Pagar menos').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%recios%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Precios',
													  when(interactions_VF.REASON_1.like('%recios%'), 'Raw_Precios'))
		conceptos_interacciones['Raw_Precios'] = ['Raw_Precios']
		# interactions_VF.groupby('Precios').count().sort('count', ascending=False).show(truncate=False)

		# interactions_VF.filter(interactions_VF.REASON_1.like('%orta%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +----------------------------+------+
		# |REASON_1                    |count |
		# +----------------------------+------+
		# |Portabilidad-negociacion    |204895|
		# |Bajas/Porta Saliente        |105091|
		# |Alta/Porta                  |67982 |
		# |Portabilidad-Otros          |61426 |
		# |Alta/Porta Entrante         |61135 |
		# |Movil Portabilidad          |21980 |
		# |Cancelacion Portabilidad    |16243 |
		# |Baja portabilidad fija      |14284 |
		# |Portabilidad                |8232  |
		# |Bajas/Porta                 |6005  |
		# |Porta saliente fijo         |5256  |
		# |B.O Portabilidad            |3630  |
		# |Alta/Porta/Baja             |1594  |
		# |Portabilidad movil          |1130  |
		# |Oferta Portabilidad         |914   |
		# |Servicios portabilidad      |891   |
		# |Broma/Se Corta/Cuelga       |541   |
		# |Desactiv-Portabilidad       |185   |
		# |Porta Saliente Móvil        |73    |
		# |BO Porta Entrante Fijo/Movil|65    |
		# +----------------------------+------+
		interactions_VF = interactions_VF.withColumn('Raw_Portabilidad',
													   when(interactions_VF.REASON_1.like('%orta%nversa%'), 'Raw_Portabilidad_Inversa')
													  .when(interactions_VF.REASON_1.like('%orta%'),        'Raw_Portabilidad'))
		conceptos_interacciones['Raw_Portabilidad'] = ['Raw_Portabilidad_Inversa', 
													   'Raw_Portabilidad']
		# interactions_VF.groupby('Portabilidad').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%aja%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Baja',
													  when(interactions_VF.REASON_1.like('%aja%'), 'Raw_Baja'))
		conceptos_interacciones['Raw_Baja'] = ['Raw_Baja']
		# interactions_VF.groupby('Baja').count().sort('count', ascending=False).show(truncate=False)


		# interactions_VF.filter(interactions_VF.REASON_1.like('%Alta%')).groupby('REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_VF = interactions_VF.withColumn('Raw_Alta',
													  when(interactions_VF.REASON_1.like('%Alta%'), 'Raw_Alta'))
		conceptos_interacciones['Raw_Alta'] = ['Raw_Alta']
		# interactions_VF.groupby('Alta').count().sort('count', ascending=False).show(truncate=False)

		# interactions_VF.groupby('RESULT_TD').count().sort('count', ascending=False).show(50, truncate=False)
		# +-------------------------------+--------+
		# |RESULT_TD                      |count   |
		# +-------------------------------+--------+
		# |n/a                            |11348242|
		# |Control                        |7573429 |
		# |NULL                           |6196647 |
		# |Informacion                    |690436  |
		# |Completado                     |372796  |
		# |Documento  Premium             |323008  |
		# |IVR Cobros                     |221643  |
		# |Modificacion                   |197938  |
		# |Informar                       |159858  |
		# |No aplica                      |124763  |
		# |Resuelto                       |109155  |
		# |OK                             |103422  |
		# |Informo                        |99250   |
		# |Solucionada                    |92059   |
		# |IVR 123                        |86684   |
		# |Ayudo cliente                  |84141   |
		# |Modificar                      |73058   |
		# |Otros                          |67736   |
		# |n/a cierre rapido/transferencia|67606   |
		# |Retenido Sin Oferta            |63005   |
		# |Acepta                         |62512   |
		# |Rechaza                        |54845   |
		# |Abro caso                      |52143   |
		# |Cliente Satisfecho             |45051   |
		# |Retenido                       |44883   |
		# |Lo pensara                     |43250   |
		# |IVR Prepago                    |43204   |
		# |Pagara mas tarde               |42253   |
		# |Completada                     |40989   |
		# |IVR Pospago                    |40384   |
		# |No Retenido                    |39106   |
		# |IVR Empresas                   |38103   |
		# |Mod.Serv.Procede               |30649   |
		# |Inf.Serv.Procede               |29775   |
		# |Sol. Aceptada                  |27784   |
		# |Completado con llamada         |27702   |
		# |Solicito Envio Tecnico         |24705   |
		# |Modifico/Corrijo               |23424   |
		# |Pendiente                      |21973   |
		# |IVR1704                        |21665   |
		# |Incidencia Abierta             |20801   |
		# |Escalado subcaso 2N            |19951   |
		# |Recuperado total               |19744   |
		# |Activo/Desactivo               |19643   |
		# |Abono importe exacto           |17727   |
		# |Envío Técnico                  |15467   |
		# |Escalado caso Provision        |14988   |
		# |Incidencia general             |12929   |
		# |Transferencia                  |12518   |
		# |IVR 1443                       |11997   |
		# +-------------------------------+--------+
		interactions_VF = interactions_VF.withColumn('Raw_Resultado',
													   when(interactions_VF.RESULT_TD.like('No _plica%'),         'Raw_Resultado_No_Aplica')
													  .when(interactions_VF.RESULT_TD.like('Inform%'),            'Raw_Resultado_Informacion')
													  .when(interactions_VF.RESULT_TD.like('%Solucion%'),         'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Ayudo cliente%'),     'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Realizado'),          'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('OK'),                 'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Recuperado total'),   'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Resuelto'),           'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Completado%'),        'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Cliente Satisfecho'), 'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Sol. Aceptada'),      'Raw_Resultado_Solucionado')
													  .when(interactions_VF.RESULT_TD.like('Retenido%'),          'Raw_Resultado_Retenido')
													  .when(interactions_VF.RESULT_TD.like('No Retenido%'),       'Raw_Resultado_No_Retenido')
													  .when(interactions_VF.RESULT_TD.like('Escal%'),             'Raw_Resultado_Escalo')
													  .when(interactions_VF.RESULT_TD.like('%Env_o __cnico%'),    'Raw_Resultado_Envio_tecnico')
													  .when(interactions_VF.RESULT_TD.like('%ransfer%'),          'Raw_Resultado_Transferencia')
													  .when(interactions_VF.RESULT_TD.like('%Abono%'),            'Raw_Resultado_Abono')
													  .when(interactions_VF.RESULT_TD.like('%BAJAS%'),            'Raw_Resultado_Bajas')
													  .when(interactions_VF.RESULT_TD.like('%Reclam%'),           'Raw_Resultado_Reclamacion'))
		conceptos_interacciones['Raw_Resultado'] = ['Raw_Resultado_No_Aplica', 
													'Raw_Resultado_Informacion', 
													'Raw_Resultado_Solucionado', 
													'Raw_Resultado_Retenido', 
													'Raw_Resultado_No_Retenido', 
													'Raw_Resultado_Escalo', 
													'Raw_Resultado_Envio_tecnico', 
													'Raw_Resultado_Transferencia', 
													'Raw_Resultado_Abono', 
													'Raw_Resultado_Bajas', 
													'Raw_Resultado_Reclamacion']
		# interactions_VF.groupby('Resultado').count().sort('count', ascending=False).show(truncate=False)

		# Join con NIF en casos donde nif no esté informado, y ver qué pasa en los que está informado, si difiere o no

		# interactions_VF.select('X_IDENTIFICATION').filter('X_IDENTIFICATION is NULL').count() # 0
		# interactions_VF.select('X_IDENTIFICATION').distinct().count() # 5 819 651
		# interactions_VF.select('X_IDENTIFICATION', 'year', 'month').distinct().groupby('year', 'month').count().sort('year', 'month').show()
		# +----+-----+-------+
		# |year|month|  count|
		# +----+-----+-------+
		# |2017|    4|2549136|
		# |2017|    5|2772207|
		# |2017|    6|2786064|
		# |2017|    7|2452103|
		# |2017|    8|2084206|
		# |2017|    9|2237326|
		# |2017|   10|1894619|
		# +----+-----+-------+
		# interactions_VF.select('X_MSISDN_ORIGINAL').filter('X_MSISDN_ORIGINAL is NULL').count() # 0
		# interactions_VF.select('X_MSISDN_ORIGINAL').distinct().count() # 15 007 690
		# interactions_VF.select('X_MSISDN_ORIGINAL', 'year', 'month').distinct().groupby('year', 'month').count().sort('year', 'month').show()
		# +----+-----+-------+
		# |year|month|  count|
		# +----+-----+-------+
		# |2017|    4|4562303|
		# |2017|    5|5045890|
		# |2017|    6|5324795|
		# |2017|    7|4360234|
		# |2017|    8|3697699|
		# |2017|    9|3922801|
		# |2017|   10|3237903|
		# +----+-----+-------+

		# No es necesario hacer join con nif donde no esté informado porque no hay ni nifs ni msisdn no informados

		interactions_VF = interactions_VF.withColumnRenamed('X_IDENTIFICATION', 'nif').withColumnRenamed('X_MSISDN_ORIGINAL', 'msisdn')

		##################################################################
		# Join VF interactions with master_customers_services to get NIF #
		##################################################################
		#interactions_VF = self.joinNIFVFCAR(interactions_VF)
		
		if self.debug:
			print 'Joining Call Centre Calls with master_customers_services by Msisdn ...'
		
		master_by_msisdn = spark.read.parquet('/tmp/bbergua/master_customers_services/msisdn/')
		
		interactions_VF = interactions_VF.withColumnRenamed('nif', 'CCC_nif')
		interactions_VF = interactions_VF.join(master_by_msisdn.drop('day'), on=['msisdn', 'partitioned_month', 'year', 'month'], how='left_outer')
		
		interactions_VF = interactions_VF.withColumn('nif', when(interactions_VF['nif'].isNotNull(), interactions_VF['nif'])
															.otherwise(interactions_VF['CCC_nif'])).drop('CCC_nif')

		# pivoted = interactions_VF.groupby('X_IDENTIFICATION', 'X_MSISDN_ORIGINAL', 'partitioned_month').pivot('Averia', values=averias_values).count().fillna(0)

		# pivoted = None
		# for c in conceptos_interacciones.keys():
			# print 'Pivoting on', c, '...'
			# tmp = interactions_VF.groupby('X_IDENTIFICATION', 'X_MSISDN_ORIGINAL', 'partitioned_month').pivot(c, values=conceptos_interacciones[c]).count().fillna(0)
			# if pivoted is None:
				# pivoted = tmp
			# else:
				# pivoted = pivoted.join(tmp, on=['X_IDENTIFICATION', 'X_MSISDN_ORIGINAL', 'partitioned_month'], how='outer')

		# pivoted.show()



		#######
		# Ono #
		#######

		# Beatriz: customerp_v.GA_INTERACT_CLIENTE.DS_X_TELF_ENTRANTE

		if self.debug:
			print 'Loading raw ONO Call Centre Calls ...'
		
		# Cruzar Interacciones Ono por NIF
		
		# En DWH: customerp_v.ga_interact
		interactions_ONO = spark.table('raw_es.callcentrecalls_interactionono')
		interactions_ONO = interactions_ONO.withColumn('year', year('FX_CREATE_DATE')).withColumn('month', month('FX_CREATE_DATE')).withColumn('day', dayofmonth('FX_CREATE_DATE'))
		interactions_ONO = interactions_ONO.dropna(how='any', subset=['year', 'month']).withColumn('partitioned_month', format_string('%d%02d', interactions_ONO.year, interactions_ONO.month))
		#self.months_found_set = self.months_found_set.union(set(interactions_ONO.select('partitioned_month').distinct().rdd.map(lambda x: x['partitioned_month']).collect()))
		#if self.filter_month is not 'all':
		#	interactions_ONO = interactions_ONO.filter('partitioned_month == "%s"' % self.filter_month)
		interactions_ONO = interactions_ONO.where ( (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))<=closing_day)
                          						  & (concat(col('year'),lpad(col('month'),2, '0'),lpad(col('day'),2, '0'))>=starting_day) )
		interactions_ONO = interactions_ONO.distinct()

		if self.debug:
			interactions_ONO.select('year', 'month').groupby('year', 'month').count().sort('year', 'month').show()

		interactions_ONO = interactions_ONO.filter("DS_DIRECTION IN ('De entrada', 'Entrante')") # DS_DIRECTION IN ('De entrada', 'Entrante')
		interactions_ONO = interactions_ONO.filter("CO_TYPE IN ('Llamada Telefono', 'Telefonica', '...')")
		interactions_ONO = interactions_ONO.filter("DS_X_GROUP_WORK NOT LIKE 'BO%'")
		interactions_ONO = interactions_ONO.filter("DS_X_GROUP_WORK NOT LIKE 'B.O%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'Emis%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT IN ('Gestion B.O.' , 'Gestion casos' , 'Gestion Casos Resueltos' , \
																		'Gestion Casos Resueltos' , 'Gestion documental' ,'Gestion documental fax' , \
																		'2ª Codificación' , 'BAJ_BO Televenta' , 'BAJ_B.O. Top 3000' , 'BBOO' , \
																		'BO Fraude' , 'B.O Gestion' , 'B.O Portabilidad' , 'BO Scoring' , \
																		'Bo Scoring Permanencia' , 'Consulta ficha' , 'Callme back' , \
																		'Consultar ficha','Backoffice Reclamaciones','BACKOFFICE','BackOffice Retención','NBA')")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'CIERRE RAPID%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'BackOffice%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'SMS FollowUP Always Solv%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'BackOffice%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'Ilocalizable/%'")
		interactions_ONO = interactions_ONO.filter("DS_REASON_1 NOT LIKE 'Detractor%'")
		#interactions_ONO.select('CO_INTERACT_ID', 'year', 'month').groupby('CO_INTERACT_ID', 'year', 'month').count().sort('count', ascending=False).show()
		#interactions_ONO.select('CO_INTERACT_ID', 'year', 'month').distinct().groupby('year', 'month').count().sort('year', 'month').show()
		#interactions_ONO = interactions_ONO.drop('TACADA', 'HO_DATE_CHARGE', 'service_processed_at', 'service_file_id').distinct()
		# interactions_ONO.select('partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		# interactions_ONO.columns

		# interactions_ONO = interactions_ONO.withColumn('length', length('DS_X_PHONE_CONSULTATION'))
		# interactions_ONO.withColumn('length', length('DS_X_PHONE_CONSULTATION')).groupby('length').count().sort('length').show()
		# +------+--------+
		# |length|   count|
		# +------+--------+
		# |     0|22160275|
		# |     1|     637|
		# |     4|    3473|
		# |     5|     101|
		# |     7|     367|
		# |     9| 7145415|
		# |    10|       3|
		# |    11|      40|
		# |    13|       2|
		# |    14|       1|
		# +------+--------+
		# interactions_ONO.withColumn('length', length('DS_X_PHONE_CONSULTATION')).filter('length >= 10').select('DS_X_PHONE_CONSULTATION').show(50)

		# Clean DS_X_PHONE_CONSULTATION
		interactions_ONO = interactions_ONO.withColumn('length', length('DS_X_PHONE_CONSULTATION')).filter('length <= 9').drop('length')
		# interactions_ONO.withColumn('length', length('DS_X_PHONE_CONSULTATION')).groupby('length').count().sort('length').show()

		# interactions_ONO.select('DS_X_PHONE_INBOUND').filter('DS_X_PHONE_INBOUND != ""').count()           # 7149369
		# interactions_ONO.select('DS_X_PHONE_CONSULTATION').filter('DS_X_PHONE_CONSULTATION != ""').count() # 7149993

		# interactions_ONO.select('DS_X_PHONE_INBOUND').distinct().count()      # 1739907
		# interactions_ONO.select('DS_X_PHONE_CONSULTATION').distinct().count() # 1872064

		# interactions_ONO.select('DS_X_PHONE_INBOUND', 'DS_X_PHONE_CONSULTATION').filter('DS_X_PHONE_CONSULTATION == "" AND DS_X_PHONE_INBOUND != ""').count() # 0

		# coun = interactions_ONO.select('DS_X_PHONE_INBOUND',      'partitioned_month').filter('DS_X_PHONE_INBOUND      != ""').withColumnRenamed('DS_X_PHONE_INBOUND',      'x_id_red').join(ono_car, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 

		# coun = interactions_ONO.select('DS_X_PHONE_CONSULTATION', 'partitioned_month').filter('DS_X_PHONE_CONSULTATION != ""').withColumnRenamed('DS_X_PHONE_CONSULTATION', 'x_id_red').join(ono_car, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 


		# coun = interactions_ONO.select('DS_X_PHONE_INBOUND',      'partitioned_month').distinct().withColumnRenamed('DS_X_PHONE_INBOUND',      'x_id_red').join(ono_car, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 

		# coun = interactions_ONO.select('DS_X_PHONE_CONSULTATION', 'partitioned_month').distinct().withColumnRenamed('DS_X_PHONE_CONSULTATION', 'x_id_red').join(ono_car, ['x_id_red', 'partitioned_month'], 'inner').count()
		# print(coun) # 


		# interactions_ONO.groupby('CO_TYPE').count().sort('count', ascending=False).show()
		# interactions_ONO.groupby('CO_S_TYPE').count().sort('count', ascending=False).show()
		# interactions_ONO.groupby('DS_REASON_1').count().sort('count', ascending=False).show()
		# interactions_ONO.groupby('DS_REASON_1').count().sort('count', ascending=False).show()
		# interactions_ONO.groupby('DS_REASON_1').count().sort('count', ascending=False).show()
		# interactions_ONO.groupby('DS_RESULT').count().sort('count', ascending=False).show()


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('Aver_a%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +-----------------------+------+
		# |DS_REASON_1            |count |
		# +-----------------------+------+
		# |Averia Fibra NET       |167497|
		# |Averia Fibra TV        |106040|
		# |Averia Fibra TF        |69139 |
		# |Averia Movil           |19074 |
		# |Averia APP Stream      |9431  |
		# |Averia DSL NET         |7336  |
		# |Averia DSL TV          |4523  |
		# |Averia DSL TF          |4406  |
		# |Averia Modem/Rout movil|1195  |
		# |Avería APP Stream      |516   |
		# |Avería                 |506   |
		# |Averia Neba Fibra NET  |321   |
		# |Averia Neba Fibra TF   |120   |
		# |Averia Neba Fibra TV   |113   |
		# |Averia                 |29    |
		# +-----------------------+------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Averia',
													  when(interactions_ONO.DS_REASON_1.like('Aver_a%TV'),          'Raw_Averia TV')
													 .when(interactions_ONO.DS_REASON_1.like('Aver_a Fibra%'),      'Raw_Averia Fibra')
													 .when(interactions_ONO.DS_REASON_1.like('Aver_a Neba%'),       'Raw_Averia Neba')
													 .when(interactions_ONO.DS_REASON_1.like('Aver_a DSL%'),        'Raw_Averia DSL')
													 .when(interactions_ONO.DS_REASON_1.like('Aver_a Modem/Rout%'), 'Raw_Averia Modem/Router')
													 .when(interactions_ONO.DS_REASON_1.like('Aver_a%'),            'Raw_Averia Resto'))
		# interactions_ONO.groupby('Averia').count().sort('count', ascending=False).show(truncate=False)
		# +-------------------+--------+
		# |Averia             |count   |
		# +-------------------+--------+
		# |null               |19709268|
		# |Averia Fibra       |236636  |
		# |Averia TV          |110676  |
		# |Averia Resto       |29556   |
		# |Averia DSL         |11742   |
		# |Averia Modem/Router|1195    |
		# |Averia Neba        |441     |
		# +-------------------+--------+

		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%rovis%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +---------------------+-----+
		# |DS_REASON_1          |count|
		# +---------------------+-----+
		# |Provision Fibra      |21832|
		# |Provision            |13118|
		# |Provision Movil      |6790 |
		# |Inc Provision Fibra  |6420 |
		# |Inc Provision Movil  |2502 |
		# |Seguimiento Provision|1727 |
		# |Provision Neba Fibra |977  |
		# |Provision-PlanB      |796  |
		# |Inc Provis Neba Fibr |511  |
		# |Provision DSL        |237  |
		# |Inc Provision DSL    |145  |
		# |Demora Provisión     |21   |
		# +---------------------+-----+
		interactions_ONO = interactions_ONO.withColumn('Raw_Provision',
													  when(interactions_ONO.DS_REASON_1.like('Provision Neba%'),  'Raw_Provision_Neba')
													 .when(interactions_ONO.DS_REASON_1.like('Provision Fibra%'), 'Raw_Provision_Fibra')
													 .when(interactions_ONO.DS_REASON_1.like('Provision DSL%'),   'Raw_Provision_DSL')
													 .when(interactions_ONO.DS_REASON_1.like('Provision Movil%'), 'Raw_Provision_Movil')
													 .when(interactions_ONO.DS_REASON_1.like('Provisi_n%'),       'Raw_Provision_Resto')
													 .when(interactions_ONO.DS_REASON_1.like('%Provisi_n'),       'Raw_Provision_Resto'))
		conceptos_interacciones['Raw_Provision'].append('Raw_Provision_Movil')
		# interactions_ONO.groupby('Provision').count().sort('count', ascending=False).show(truncate=False)
		# +---------------+--------+
		# |Provision      |count   |
		# +---------------+--------+
		# |null           |20054016|
		# |Provision Fibra|21832   |
		# |Provision Resto|15662   |
		# |Provision Movil|6790    |
		# |Provision Neba |977     |
		# |Provision DSL  |237     |
		# +---------------+--------+

		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%Inc%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +----------------------+-----+
		# |DS_REASON_1           |count|
		# +----------------------+-----+
		# |Inc Provision Fibra   |6420 |
		# |Inc Provision Movil   |2502 |
		# |Incidencia            |2259 |
		# |Incidencia ? Contrata?|804  |
		# |Inc Provis Neba Fibr  |511  |
		# |Incidencia tecnica    |351  |
		# |Incidencias SGI       |177  |
		# |Inc Provision DSL     |145  |
		# +----------------------+-----+
		interactions_ONO = interactions_ONO.withColumn('Raw_Incidencia',
													  when(interactions_ONO.DS_REASON_1.like('Inc Provis%Neba%'),     'Raw_Incidencia_Provision_Neba')
													 .when(interactions_ONO.DS_REASON_1.like('Inc Provision Fibra%'), 'Raw_Incidencia_Provision_Fibra')
													 .when(interactions_ONO.DS_REASON_1.like('Inc Provision Movil%'), 'Raw_Incidencia_Provision_Movil')
													 .when(interactions_ONO.DS_REASON_1.like('Inc Provision DSL%'),   'Raw_Incidencia_Provision_DSL')
													 .when(interactions_ONO.DS_REASON_1.like('Incidencia _ecnica%'),  'Raw_Incidencia_Tecnica')
													 .when(interactions_ONO.DS_REASON_1.like('Incidencia%SGI%'),      'Raw_Incidencia_SGI')
													 .when(interactions_ONO.DS_REASON_1.like('Incidencia%'),          'Raw_Incidencia_Resto'))
		conceptos_interacciones['Raw_Incidencia'].append('Raw_Incidencia_Provision_Movil')
		# interactions_ONO.groupby('Incidencia').count().sort('count', ascending=False).show(truncate=False)
		# +--------------------------+--------+
		# |Incidencia                |count   |
		# +--------------------------+--------+
		# |null                      |20086345|
		# |Incidencia Provision Fibra|6420    |
		# |Incidencia Resto          |3063    |
		# |Incidencia Provision Movil|2502    |
		# |Incidencia Provision Neba |511     |
		# |Incidencia Tecnica        |351     |
		# |Incidencia SGI            |177     |
		# |Incidencia Provision DSL  |145     |
		# +--------------------------+--------+

		interactions_ONO = interactions_ONO.withColumn('DS_REASON_1_LC', lower(interactions_ONO.DS_REASON_1)).withColumn('DS_REASON_1_LC', translate('DS_REASON_1_LC', 'áéíóú', 'aeiou'))
		# interactions_ONO.filter(interactions_ONO.DS_REASON_1_LC.like('%cons%')).groupby('DS_REASON_1_LC').count().sort('count', ascending=False).show(50, truncate=False)
		# +-------------------------------------+------+
		# |DS_REASON_1_LC                       |count |
		# +-------------------------------------+------+
		# |cons tec fibra net                   |122275|
		# |consulta postventa                   |109411|
		# |cons tec movil                       |79228 |
		# |cons tec fibra tv                    |44937 |
		# |consulta ficha                       |40404 |
		# |consulta tecnica internet            |27922 |
		# |cons tec fibra tf                    |25017 |
		# |consulta producto                    |24545 |
		# |consulta cliente                     |14701 |
		# |consulta factura                     |8442  |
		# |cons tec app stream                  |7534  |
		# |consulta tecnica tivo                |5659  |
		# |consulta tecnica telefono movil - red|4409  |
		# |cons tec dsl net                     |3157  |
		# |cons tec modem/rout movil            |2762  |
		# |consulta tecnica telefono            |2224  |
		# |permisos - construccion              |2121  |
		# |consulta tecnica telefono movil      |1906  |
		# |consulta/modificacion datos          |1768  |
		# |cons tec dsl tv                      |1709  |
		# |consulta tecnica telefono movil ? red|1514  |
		# |cons tec dsl tf                      |1119  |
		# |2 consulta                           |884   |
		# |consulta facturacion                 |792   |
		# |consulta tecnica television          |780   |
		# |cons tec                             |751   |
		# |consulta deuda                       |576   |
		# |cons tec neb fibr net                |505   |
		# |consulta / modificacion datos        |500   |
		# |consulta tecnica movil- red          |470   |
		# |consulta tecnica hbo                 |436   |
		# |cons tecn movil                      |380   |
		# |consulta tecnica bam- red            |231   |
		# |consulta tecnica wifi                |161   |
		# |consulta tecnica ftth                |156   |
		# |error selecc/consulta ficha          |92    |
		# |consulta tecnica tivo ftth           |60    |
		# |consulta tecnica autoi               |55    |
		# |cons tec neba fibra tf               |55    |
		# |cons tec neb fibr tv                 |38    |
		# |consulta tecnica netflix             |29    |
		# |consulta tecnica bam                 |24    |
		# |consulta tecnica tv online           |23    |
		# |consulta tecnica otg                 |9     |
		# |consulta                             |7     |
		# |consulta tecnica tveverywhere        |2     |
		# |cons tec internet                    |2     |
		# |cons tec vf wf/wf neg                |1     |
		# +-------------------------------------+------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Consulta',
													  when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%tv%'),         'Raw_Consulta Tecnica TV')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%television%'), 'Raw_Consulta Tecnica TV')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%tivo%'),       'Raw_Consulta Tecnica TV')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%hbo'),         'Raw_Consulta Tecnica TV')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%netflix'),     'Raw_Consulta Tecnica TV')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%fibra%'),      'Raw_Consulta Tecnica Fibra')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%ftth%'),       'Raw_Consulta Tecnica Fibra')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%internet'),    'Raw_Consulta Tecnica Fibra')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%neb%fibr%'),   'Raw_Consulta Tecnica Neba')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%dsl%'),        'Raw_Consulta Tecnica DSL')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%modem/rout%'), 'Raw_Consulta Tecnica Modem/Router')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%wifi%'),       'Raw_Consulta Tecnica Modem/Router')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%movil%'),      'Raw_Consulta Tecnica Movil')
													 .when(interactions_ONO.DS_REASON_1_LC.like('cons%tec%'),            'Raw_Consulta Tecnica Resto')
													 .when(interactions_ONO.DS_REASON_1_LC.like('%consulta ficha'),      'Raw_Consulta Ficha')
													 .when(interactions_ONO.DS_REASON_1_LC.like('consulta cliente'),     'Raw_Consulta Ficha')
													 .when(interactions_ONO.DS_REASON_1_LC.like('consulta%datos'),       'Raw_Consulta Ficha')
													 .when(interactions_ONO.DS_REASON_1_LC.like('%consulta%'),           'Raw_Consulta Resto'))
		# interactions_ONO.groupby('Consulta').count().sort('count', ascending=False).show(truncate=False)
		# +-----------------------------+--------+
		# |Consulta                     |count   |
		# +-----------------------------+--------+
		# |null                         |19713931|
		# |Consulta Tecnica Fibra       |175427  |
		# |Consulta Resto               |144657  |
		# |Consulta Tecnica Movil       |87907   |
		# |Consulta Ficha               |57465   |
		# |Consulta Tecnica TV          |53673   |
		# |Consulta Tecnica Resto       |10829   |
		# |Consulta Tecnica DSL         |4276    |
		# |Consulta Tecnica Modem/Router|2923    |
		# |Consulta Tecnica Neba        |505     |
		# +-----------------------------+--------+

		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%Inf%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +------------------------+-------+
		# |DS_REASON_1             |count  |
		# +------------------------+-------+
		# |Informacion             |4636762|
		# |Información             |435300 |
		# |Informacion proceso     |17480  |
		# |Informo nº ATT Cliente  |3338   |
		# |Porta Salientes Emp Info|153    |
		# |Info/Modificacion       |1      |
		# +------------------------+-------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Informacion',
													  when(interactions_ONO.DS_REASON_1.like('%Info%'), 'Raw_Informacion'))
		# interactions_ONO.groupby('Informacion').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%actura%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +--------------------------+------+
		# |DS_REASON_1               |count |
		# +--------------------------+------+
		# |Factura                   |709892|
		# |Consulta Factura          |8442  |
		# |Dudas facturación         |7332  |
		# |Accion factura electronica|3088  |
		# |Dudas Facturacion         |2572  |
		# |Facturacion               |1294  |
		# |Consulta facturación      |730   |
		# |Acción factura electrónica|465   |
		# |Factura/Recarga           |389   |
		# |Consulta Facturación      |62    |
		# |Empresas Facturacion      |15    |
		# |Envío Mail Video Factura  |1     |
		# |Envio Mail Video Factura  |1     |
		# +--------------------------+------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Factura',
													  when(interactions_ONO.DS_REASON_1.like('%actura%'), 'Raw_Factura'))
		# interactions_ONO.groupby('Factura').count().sort('count', ascending=False).show(truncate=False)

		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%Prod%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# TODO: Check real list of products in ONO
		interactions_ONO = interactions_ONO.withColumn('Raw_Productos',
													  when(interactions_ONO.DS_REASON_1.like('%Productos%Voz'),   'Raw_Productos_Voz')
													 .when(interactions_ONO.DS_REASON_1.like('%Productos%Datos'), 'Raw_Productos_Datos')
													 .when(interactions_ONO.DS_REASON_1.like('%Productos%'),      'Raw_Productos_Resto'))
		conceptos_interacciones['Raw_Productos'] = ['Raw_Productos_Voz', 
													'Raw_Productos_Datos', 
													'Raw_Productos_Resto']
		# interactions_ONO.groupby('Productos').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%obro%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Cobro',
													  when(interactions_ONO.DS_REASON_1.like('%obro%'), 'Raw_Cobro'))
		conceptos_interacciones['Raw_Cobro'] = ['Raw_Cobro']
		# interactions_ONO.groupby('Cobro').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%ransf%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Transferencia',
													  when(interactions_ONO.DS_REASON_1.like('%ransf%'), 'Raw_Transferencia'))
		conceptos_interacciones['Raw_Transferencia'] = ['Raw_Transferencia']
		# interactions_ONO.groupby('Transferencia').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%ierre%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Cierre',
													  when(interactions_ONO.DS_REASON_1.like('%ierre%'), 'Raw_Cierre'))
		conceptos_interacciones['Raw_Cierre'] = ['Raw_Cierre']
		# interactions_ONO.groupby('Cierre').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%frecimiento%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +----------------------+------+
		# |DS_REASON_1           |count |
		# +----------------------+------+
		# |Ofrecimiento comercial|194277|
		# |No Ofrecimiento       |3163  |
		# |Ofrecimiento Comercial|22    |
		# +----------------------+------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Ofrecimiento',
													  when(interactions_ONO.DS_REASON_1.like('%frecimiento%'), 'Raw_Ofrecimiento'))
		# interactions_ONO.groupby('Ofrecimiento').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%esactivaci%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +-------------------------+-----+
		# |DS_REASON_1              |count|
		# +-------------------------+-----+
		# |Desactivacion BA+Movil+TV|96072|
		# |Desactivacion TV         |47049|
		# |Desactivacion Movil      |39597|
		# |Desactivacion Total      |29455|
		# |Desactivacion NET        |21415|
		# |Desactivación            |16046|
		# |Desactivacion Fijo       |7060 |
		# |Desactivacion USB        |1333 |
		# |Desactivacion NET+TV     |552  |
		# |Desactivacion Movil+TV   |373  |
		# |Desactivacion NET+Movil  |253  |
		# |Desactivacion ITC        |193  |
		# |Desactivacion NET+Fijo   |63   |
		# |Desactivacion Movil+Fijo |52   |
		# |Desactivacion            |4    |
		# +-------------------------+-----+
		interactions_ONO = interactions_ONO.withColumn('Raw_Desactivacion',
													   when(interactions_ONO.DS_REASON_1.like('%Desactivacion BA%'),    'Raw_Desactivacion_BA+Movil+TV')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion TV%'),    'Raw_Desactivacion_TV')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion Movil%'), 'Raw_Desactivacion_Movil')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion Total%'), 'Raw_Desactivacion_Total')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion NET%'),   'Raw_Desactivacion_NET')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion Fijo%'),  'Raw_Desactivacion_Fijo')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivacion USB%'),   'Raw_Desactivacion_USB')
													  .when(interactions_ONO.DS_REASON_1.like('%Desactivaci_n%'),       'Raw_Desactivacion_Resto'))
		# interactions_ONO.groupby('Desactivacion').count().sort('count', ascending=False).show(truncate=False)
		# +-------------------------+--------+
		# |Desactivacion            |count   |
		# +-------------------------+--------+
		# |null                     |19992076|
		# |Desactivacion BA+Movil+TV|96072   |
		# |Desactivacion TV         |47049   |
		# |Desactivacion Movil      |40022   |
		# |Desactivacion Total      |29455   |
		# |Desactivacion NET        |22283   |
		# |Desactivacion Resto      |16243   |
		# |Desactivacion Fijo       |7060    |
		# |Desactivacion USB        |1333    |
		# +-------------------------+--------+


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%menos%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Pagar_menos',
													  when(interactions_ONO.DS_REASON_1.like('%menos%'), 'Raw_Pagar_menos'))
		conceptos_interacciones['Raw_Pagar_menos'] = ['Raw_Pagar_menos']
		# interactions_ONO.groupby('Pagar menos').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%recios%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Precios',
													  when(interactions_ONO.DS_REASON_1.like('%recios%'), 'Raw_Precios'))
		conceptos_interacciones['Raw_Precios'] = ['Raw_Precios']
		# interactions_ONO.groupby('Precios').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%orta%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		# +------------------------------+------+
		# |DS_REASON_1                   |count |
		# +------------------------------+------+
		# |Envío email Portabilidad móvil|291913|
		# |SMS Portabilidad móvil        |266588|
		# |SMS Portabilidad Inversa Fijo |176019|
		# |Portabilidad-negociacion      |147204|
		# |SMS Portabilidad Movil        |144700|
		# |SMS Portabilidad Fijo         |120185|
		# |Mail Portabilidad Fijo        |90773 |
		# |Mail Portabilidad Movil       |89405 |
		# |Portabilidad-Otros            |56249 |
		# |MAIL Portabilidad Inversa Fijo|42424 |
		# |Gestion Devoluciones Portab.  |4978  |
		# |B.O Portabilidad              |3247  |
		# |Portabilidad                  |2213  |
		# |Desactiv-Portabilidad         |404   |
		# |Porta Saliente Fijo           |255   |
		# |Alta/Porta Entrante           |236   |
		# |Portabilidad inversa          |169   |
		# |Llamada Cortada               |158   |
		# |Porta Salientes Emp Info      |153   |
		# |Cortada                       |62    |
		# +------------------------------+------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Portabilidad',
													   when(interactions_ONO.DS_REASON_1.like('%orta%nversa%'), 'Raw_Portabilidad_Inversa')
													  .when(interactions_ONO.DS_REASON_1.like('%orta%'),        'Raw_Portabilidad'))
		conceptos_interacciones['Raw_Portabilidad'] = ['Raw_Portabilidad_Inversa', 
													   'Raw_Portabilidad']
		# interactions_ONO.groupby('Portabilidad').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%aja%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Baja',
													  when(interactions_ONO.DS_REASON_1.like('%aja%'), 'Raw_Baja'))
		conceptos_interacciones['Raw_Baja'] = ['Raw_Baja']
		# interactions_ONO.groupby('Baja').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.filter(interactions_ONO.DS_REASON_1.like('%Alta%')).groupby('DS_REASON_1').count().sort('count', ascending=False).show(truncate=False)
		interactions_ONO = interactions_ONO.withColumn('Raw_Alta',
													  when(interactions_ONO.DS_REASON_1.like('%Alta%'), 'Raw_Alta'))
		conceptos_interacciones['Raw_Alta'] = ['Raw_Alta']
		# interactions_ONO.groupby('Alta').count().sort('count', ascending=False).show(truncate=False)


		# interactions_ONO.groupby('DS_RESULT').count().sort('count', ascending=False).show(50, truncate=False)
		# +---------------------------+-------+
		# |DS_RESULT                  |count  |
		# +---------------------------+-------+
		# |No Aplica                  |7680131|
		# |                           |3539793|
		# |A Especificar              |2245020|
		# |Control                    |1348813|
		# |Info/Soluciono             |621876 |
		# |Ayudo cliente              |603788 |
		# |Información                |568242 |
		# |Informacion                |524323 |
		# |Filtrado                   |356148 |
		# |No aplica                  |225477 |
		# |Retenido                   |179668 |
		# |Escalo                     |153763 |
		# |No Retenido                |127028 |
		# |Gestión interna            |111559 |
		# |FILTRADA                   |110885 |
		# |Envio tecnico              |108028 |
		# |N/A                        |106970 |
		# |Abro ticket                |100019 |
		# |IVR123                     |92126  |
		# |Retenido Sin Oferta        |66658  |
		# |Cierre rápido / transfer   |63966  |
		# |Inc General                |61734  |
		# |Modificacion               |56321  |
		# |EXTERNA ONLINE             |54059  |
		# |SERVICIO NO DISPONIBLE     |52445  |
		# |IVR Cobros                 |51179  |
		# |Realizado                  |45758  |
		# |Titular                    |45520  |
		# |OK                         |43797  |
		# |Abono                      |40820  |
		# |cierre rapido/transferencia|39303  |
		# |NO FILTRADA                |38881  |
		# |Retenido 2N                |38048  |
		# |Modificación servicios     |31220  |
		# |Agendo llamada             |27694  |
		# |BAJAS                      |25558  |
		# |FACTURACION                |24630  |
		# |ESTADO DE SU PEDIDO        |23917  |
		# |Reclamación                |23809  |
		# |Cierre rapido              |22939  |
		# |Recuperado total           |22046  |
		# |Incidencia general         |22014  |
		# |Acepta                     |21272  |
		# |n/a                        |20741  |
		# |CIERRE RAPIDO              |20598  |
		# |No recuperado              |19639  |
		# |No Acepta                  |16365  |
		# |Reclam: Procede            |15256  |
		# |Abono prox factura         |15188  |
		# |Abono compensacion         |15026  |
		# +---------------------------+-------+
		interactions_ONO = interactions_ONO.withColumn('Raw_Resultado',
													   when(interactions_ONO.DS_RESULT.like('No _plica%'),       'Raw_Resultado No Aplica')
													  .when(interactions_ONO.DS_RESULT.like('Informaci_n%'),     'Raw_Resultado Informacion')
													  .when(interactions_ONO.DS_RESULT.like('%Soluciono%'),      'Raw_Resultado Solucionado')
													  .when(interactions_ONO.DS_RESULT.like('Ayudo cliente%'),   'Raw_Resultado Solucionado')
													  .when(interactions_ONO.DS_RESULT.like('Realizado'),        'Raw_Resultado Solucionado')
													  .when(interactions_ONO.DS_RESULT.like('OK'),               'Raw_Resultado Solucionado')
													  .when(interactions_ONO.DS_RESULT.like('Recuperado total'), 'Raw_Resultado Solucionado')
													  .when(interactions_ONO.DS_RESULT.like('Retenido%'),        'Raw_Resultado Retenido')
													  .when(interactions_ONO.DS_RESULT.like('No Retenido%'),     'Raw_Resultado No Retenido')
													  .when(interactions_ONO.DS_RESULT.like('Escalo%'),          'Raw_Resultado Escalo')
													  .when(interactions_ONO.DS_RESULT.like('%Envio tecnico%'),  'Raw_Resultado Envio tecnico')
													  .when(interactions_ONO.DS_RESULT.like('%ransfer%'),        'Raw_Resultado Transferencia')
													  .when(interactions_ONO.DS_RESULT.like('%Abono%'),          'Raw_Resultado Abono')
													  .when(interactions_ONO.DS_RESULT.like('%BAJAS%'),          'Raw_Resultado Bajas')
													  .when(interactions_ONO.DS_RESULT.like('%Reclam%'),         'Raw_Resultado Reclamacion'))
		# interactions_ONO.groupby('Resultado').count().sort('count', ascending=False).show(truncate=False)



		#############################################
		# Join ONO interactions with CAR to get NIF #
		#############################################

		# spark.table("raw_es.ono_rs_cartera_cli").select('num_cliente', 'partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+-------+
		# |partitioned_month|  count|
		# +-----------------+-------+
		# |           201608|1542529|
		# |           201609|1542695|
		# |           201610|1537612|
		# |           201611|1529732|
		# |           201612|1522054|
		# |           201701|1508030|
		# |           201702|1496312|
		# |           201703|1492595|
		# |           201704|1484359|
		# |           201705|1473253|
		# |           201706|1463600|
		# |           201707|1490249|
		# |           201708|1535012|
		# |           201709|3082486|
		# |           201710|1859506|
		# +-----------------+-------+

		# spark.table("raw_es.ono_rs_cartera_cli").select('num_cliente', 'partitioned_month').distinct().groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+-------+
		# |partitioned_month|  count|
		# +-----------------+-------+
		# |           201608|1542529|
		# |           201609|1542695|
		# |           201610|1537612|
		# |           201611|1529732|
		# |           201612|1522054|
		# |           201701|1508030|
		# |           201702|1496312|
		# |           201703|1492595|
		# |           201704|1484359|
		# |           201705|1473253|
		# |           201706|1463600|
		# |           201707|1490249|
		# |           201708|1535012|
		# |           201709|1541243|
		# |           201710|1859506|
		# +-----------------+-------+

		ono = spark.table("raw_es.ono_rs_cartera_cli").select('num_cliente', 'nif_cliente', 'partitioned_month', 'year', 'month')#.filter('partitioned_month == "201710"') # 'num_cliente', 'msisdn_ono_vdf', 'nif_cliente'
		# ono.groupby('partitioned_month').count().sort('partitioned_month').show()
		# ono.distinct().groupby('partitioned_month').count().sort('partitioned_month').show()
		ono = ono.distinct()
		# ono.select('num_cliente').count() # 1 535 012

		# interactions_ONO.count() # 29 311 147
		# interactions_ONO.groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+--------+
		# |partitioned_month|   count|
		# +-----------------+--------+
		# |           201704| 4484663|
		# |           201705| 5053931|
		# |           201707|12232026|
		# |           201708| 2159246|
		# |           201709| 3209047|
		# |           201710| 2172234|
		# +-----------------+--------+
		# interactions_ONO.select('CO_CUSTOMER_CM').withColumnRenamed('CO_CUSTOMER_CM', 'num_cliente').join(ono, 'num_cliente', 'inner').count() # 20 651 628
		# interactions_ONO.select('CO_CUSTOMER_CM').distinct().count() # 1 954 148
		# interactions_ONO.select('CO_CUSTOMER_CM').distinct().withColumnRenamed('CO_CUSTOMER_CM', 'num_cliente').join(ono, 'num_cliente', 'inner').count() # 1 241 346

		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').count() # 29 311 147
		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+--------+
		# |partitioned_month|   count|
		# +-----------------+--------+
		# |           201704| 4484663|
		# |           201705| 5053931|
		# |           201707|12232026|
		# |           201708| 2159246|
		# |           201709| 3209047|
		# |           201710| 2172234|
		# +-----------------+--------+
		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').withColumnRenamed('CO_CUSTOMER_CM', 'num_cliente').join(ono, ['num_cliente', 'partitioned_month'], 'inner').groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+-------+
		# |partitioned_month|  count|
		# +-----------------+-------+
		# |           201704|3641551|
		# |           201705|3949092|
		# |           201707|9118828|
		# |           201708|1746205|
		# |           201710|1643838|
		# +-----------------+-------+
		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').distinct().count() # 4 089 064
		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').distinct().groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+-------+
		# |partitioned_month|  count|
		# +-----------------+-------+
		# |           201704|1020291|
		# |           201705| 660463|
		# |           201707|1121602|
		# |           201708| 362085|
		# |           201709| 530224|
		# |           201710| 394399|
		# +-----------------+-------+
		# interactions_ONO.select('CO_CUSTOMER_CM', 'partitioned_month').distinct().withColumnRenamed('CO_CUSTOMER_CM', 'num_cliente').join(ono, ['num_cliente', 'partitioned_month'], 'inner').groupby('partitioned_month').count().sort('partitioned_month').show()
		# +-----------------+------+
		# |partitioned_month| count|
		# +-----------------+------+
		# |           201704|847365|
		# |           201705|465890|
		# |           201707|750684|
		# |           201708|271418|
		# |           201710|287350|
		# +-----------------+------+

		interactions_ONO = interactions_ONO.withColumnRenamed('CO_CUSTOMER_CM', 'num_cliente').withColumnRenamed('DS_X_PHONE_CONSULTATION', 'msisdn')

		interactions_ONO = interactions_ONO.join(ono, ['num_cliente', 'partitioned_month', 'year', 'month'], 'left_outer').withColumnRenamed('nif_cliente', 'nif')
		# interactions_ONO.select('num_cliente', 'nif', 'partitioned_month').filter('num_cliente != "" AND nif is NULL').count()

		# # ono_car = spark.table("udf_es.ono_explicativas_nif_rs_201709") # 'nif'
		# # ono_car = spark.table("udf_es.ono_explicativas_lin_201709") # 'msisdn', 'id_cliente'
		# # ono_car.select('id_cliente').count() # 206506

		# # ono.select('num_cliente').distinct().count() # 1535012
		# # ono_car.select('msisdn', 'id_cliente').join(
			# # ono.select('num_cliente', 'nif_cliente').withColumnRenamed('num_cliente', 'id_cliente'), 'id_cliente', 'inner').count() # 290

		# # ono_bs = spark.table("raw_es.ono_bs_cartera") # 'num_cliente', 'nif_cliente'
		# # ono_car.select('msisdn', 'id_cliente').join(
			# # ono_bs.select('num_cliente', 'nif_cliente').withColumnRenamed('num_cliente', 'id_cliente'), 'id_cliente', 'inner').count() # 25615

		# # ono_bs = spark.table("raw_es.ono_bs_cartera_movil") # 'num_cliente', 'nif_cliente'
		# # ono_car.select('msisdn', 'id_cliente').join(
			# # ono_bs.select('num_cliente', 'nif_cliente').withColumnRenamed('num_cliente', 'id_cliente'), 'id_cliente', 'inner').count() # 53923


		#################################################
		# Prefix all hand-made interactions with 'Raw_' #
		#################################################

		# for c in conceptos_interacciones.keys():
		#     #print conceptos_interacciones[c]
		#     conceptos_interacciones[c] = ['Raw_'+v for v in conceptos_interacciones[c]]


		##################################
		# Union of VF + ONO Interactions #
		##################################

		# Añadimos los buckets de COPS
		interactions_VF = interactions_VF.withColumnRenamed('REASON_1',  'INT_Tipo')\
										 .withColumnRenamed('REASON_2',  'INT_Subtipo')\
										 .withColumnRenamed('REASON_3',  'INT_Razon')\
										 .withColumnRenamed('RESULT_TD', 'INT_Resultado')
		interactions_VF = interactions_VF.join(buckets.drop('Stack').distinct(), \
											   ['INT_Tipo', 'INT_Subtipo', 'INT_Razon', 'INT_Resultado'], \
											   'left_outer')
		interactions_VF = interactions_VF.distinct() # FIXME: Maybe this is not necessary
		interactions_VF = interactions_VF.fillna('NA', ['Bucket', 'Sub_Bucket'])
		interactions_VF = interactions_VF.fillna('NA_NA', 'Bucket_Sub_Bucket')
		#interactions_VF = interactions_VF.withColumn('Bucket_Sub_Bucket', concat_ws('_', interactions_VF.Bucket, interactions_VF.Sub_Bucket))
		#if self.debug:
		#	interactions_VF.groupby('Bucket', 'Sub_Bucket', 'Bucket_Sub_Bucket').count().sort('count', ascending=False).show(truncate=False)

		interactions_ONO = interactions_ONO.withColumnRenamed('DS_REASON_1', 'INT_Tipo')\
		                                   .withColumnRenamed('DS_REASON_2', 'INT_Subtipo')\
		                                   .withColumnRenamed('DS_REASON_3', 'INT_Razon')\
		                                   .withColumnRenamed('DS_RESULT',   'INT_Resultado')
		# TEST
		#print '1:', interactions_ONO.where('msisdn = 947925435').count()#, interactions_ONO.where('msisdn = 947925435').where('Raw_Pagar_menos = "Raw_Pagar_menos"').count()
		#interactions_ONO.where('msisdn = 947925435').select('msisdn', 'DS_REASON_1', 'Raw_Pagar_menos').show()
	#	return interactions_ONO, buckets

	#def fakeFun(self):
		interactions_ONO = interactions_ONO.join(buckets.drop('Stack').distinct(), \
												 ['INT_Tipo', 'INT_Subtipo', 'INT_Razon', 'INT_Resultado'], \
												 'left_outer')
		# TEST
		#print '2:', interactions_ONO.where('msisdn = 947925435').count()#, interactions_ONO.where('msisdn = 947925435').where('Raw_Pagar_menos = "Raw_Pagar_menos"').count()
		#interactions_ONO.where('msisdn = 947925435').select('msisdn', 'INT_Tipo', 'Raw_Pagar_menos').show()
		interactions_ONO = interactions_ONO.distinct() # FIXME: Maybe this is not necessary
		# TEST
		#print '3:', interactions_ONO.where('msisdn = 947925435').count()#, interactions_ONO.where('msisdn = 947925435').where('Raw_Pagar_menos = "Raw_Pagar_menos"').count()
		#interactions_ONO.where('msisdn = 947925435').select('msisdn', 'INT_Tipo', 'Raw_Pagar_menos').show()
		interactions_ONO = interactions_ONO.fillna('NA', ['Bucket', 'Sub_Bucket'])
		interactions_ONO = interactions_ONO.fillna('NA_NA', 'Bucket_Sub_Bucket')
		#if self.debug:
		#	interactions_ONO.groupby('Bucket', 'Sub_Bucket', 'Bucket_Sub_Bucket').count().sort('count', ascending=False).show(truncate=False)

		conceptos_interacciones['Bucket'] = buckets.select('Bucket').distinct().rdd.flatMap(lambda x: x).collect()# + ['NA']
		#print conceptos_interacciones['Bucket']
		#conceptos_interacciones['Sub_Bucket'] = buckets.select('Sub_Bucket').distinct().rdd.flatMap(lambda x: x).collect() + ['NA']
		#print conceptos_interacciones['Sub_Bucket']
		conceptos_interacciones['Bucket_Sub_Bucket'] = buckets.select('Bucket_Sub_Bucket').distinct().rdd.flatMap(lambda x: x).collect()# + ['NA_NA']
		#print conceptos_interacciones['Bucket_Sub_Bucket']

		interactions_cols = ['nif', 'msisdn', 'partitioned_month', 'year', 'month'] + conceptos_interacciones.keys()

		# interactions_VF.count()  # 135 736 117
		# interactions_ONO.count() #  21 770 620
		self.all_interactions = interactions_VF.select(interactions_cols).union(interactions_ONO.select(interactions_cols))
		# self.all_interactions.count() # 157 506 737

		# self.all_interactions.select(       'msisdn', 'partitioned_month').distinct().count() #  7 686 645
		# self.all_interactions.select('nif', 'msisdn', 'partitioned_month').distinct().count() # 10 850 082
		# self.all_interactions.select('nif', 'msisdn', 'partitioned_month').distinct().groupby('msisdn', 'partitioned_month').count().filter('count > 1').sort('count', ascending=False).count() # 903 674
		# self.all_interactions.select('nif', 'msisdn', 'partitioned_month').distinct().groupby('msisdn', 'partitioned_month').count().filter('count > 1').sort('count', ascending=False).show()
		# +---------+-----------------+------+
		# |   msisdn|partitioned_month| count|
		# +---------+-----------------+------+
		# |         |           201701|513442|
		# |         |           201703|441664|
		# |         |           201702|422266|
		# |         |           201708|371429|
		# |073331536|           201701| 18271|
		# |667181549|           201701| 13858|
		# |667181549|           201702|  8386|
		# |073331536|           201702|  7440|
		# |073331536|           201703|  3826|
		# |000000000|           201701|  2022|
		# |922858440|           201701|  1314|
		# |667181549|           201703|   904|
		# |073331536|           201708|   894|
		# |970902570|           201703|   797|
		# |970902570|           201701|   649|
		# |970902570|           201702|   624|
		# |970902570|           201708|   504|
		# |925696310|           201701|   395|
		# |000000000|           201702|   357|
		# |908712290|           201701|   337|
		# +---------+-----------------+------+
		# self.all_interactions.select('nif', 'msisdn', 'partitioned_month').filter('msisdn == "073331536" AND partitioned_month == "201701"').show()

	def generateFeaturesByMsisdn(self,closing_day,starting_day):
		self.prepareFeatures(closing_day,starting_day)

		#self.all_interactions.printSchema()
		# pivoted = interactions_ONO.groupby('msisdn', 'partitioned_month').pivot('Averia', values=averias_values).count().fillna(0)
		if self.debug:
			print 'Pivoting by Msisdn ...'
		self.pivoted_by_msisdn = None
		for c in conceptos_interacciones.keys():
			#if self.debug:
			#	print 'Pivoting by Msisdn on', c, '...'
			tmp = self.all_interactions.groupby('msisdn').pivot(c, values=conceptos_interacciones[c]).count().fillna(0)
			if self.pivoted_by_msisdn is None:
				self.pivoted_by_msisdn = tmp
			else:
				self.pivoted_by_msisdn = self.pivoted_by_msisdn.join(tmp, on=['msisdn'], how='outer')

		self.pivoted_by_msisdn = self.pivoted_by_msisdn.withColumn('day', lit(0))
		self.pivoted_by_msisdn = self.fix_column_names(self.pivoted_by_msisdn)
		#if self.debug:
		#	self.pivoted_by_msisdn.show()

		return self.pivoted_by_msisdn

	def generateFeaturesById(self):
		if self.debug:
			print 'Pivoting by Id ...'
		self.pivoted_by_id = None
		for c in conceptos_interacciones.keys():
			#if self.debug:
			#	print 'Pivoting by Id on', c, '...'
			tmp = self.all_interactions.groupby('nif').pivot(c, values=conceptos_interacciones[c]).count().fillna(0)
			if self.pivoted_by_id is None:
				self.pivoted_by_id = tmp
			else:
				self.pivoted_by_id = self.pivoted_by_id.join(tmp, on=['nif'], how='outer')

		self.pivoted_by_id = self.pivoted_by_id.dropna(subset='nif')
		self.pivoted_by_id = self.pivoted_by_id.withColumn('day', lit(0))
		self.pivoted_by_id = self.fix_column_names(self.pivoted_by_id)
		#if self.debug:
		#	self.pivoted_by_id.show()

		return self.pivoted_by_id


if __name__ == "__main__":
	# spark2-submit $SPARK_COMMON_OPTS --conf spark.yarn.executor.memoryOverhead=8G --executor-memory 8G ~/fy17.capsule/customer_experience/src/main/python/DP_Call_Centre_Calls.py 2>&1 | tee salida.ccc
	print '[' + time.ctime() + ']', 'Starting process ...'

	parser = argparse.ArgumentParser(description='VF_ES Call Centre Calls',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
	parser.add_argument('-m', '--month', metavar='<month>', type=str, help='YearMonth (YYYYMM) of the month to process, \'all\' to proces all months available, or leave empty for current month')
	parser.add_argument('-d', '--debug', action='store_true', help='show debug messages')
	args = parser.parse_args()
	print 'args =', args
	print 'month =', args.month
	#import sys
	#sys.exit()
	
	ccc = DPCallCentreCalls(None, args.month, True)

	ccc.generateFeaturesByMsisdn()
	#ccc.pivoted_by_msisdn.show()
	print '[' + time.ctime() + ']', 'Saving Call Centre Calls by Msisdn ...'
	#ccc.pivoted_by_msisdn = ccc.pivoted_by_msisdn.filter('partitioned_month <= "201802"')
	ccc.pivoted_by_msisdn.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').saveAsTable('tests_es.ccc_msisdn')#.repartition('partitioned_month')
	ccc.pivoted_by_msisdn.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/ccc/msisdn/')#.repartition('partitioned_month')
	subprocess.call('hdfs dfs -chmod -R o+rx /tmp/bbergua/ccc/msisdn/', shell=True)
	subprocess.call('hdfs dfs -chmod    o-x  /tmp/bbergua/ccc/msisdn/partitioned_month=*/year=*/month=*/day=*/*', shell=True)

	ccc.generateFeaturesById()
	#ccc.pivoted_by_id.show()
	print '[' + time.ctime() + ']', 'Saving Call Centre Calls by Id ...'
	#ccc.pivoted_by_id = ccc.pivoted_by_id.filter('partitioned_month <= "201802"')
	ccc.pivoted_by_id.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').saveAsTable('tests_es.ccc_id')#.repartition('partitioned_month')
	ccc.pivoted_by_id.write.partitionBy('partitioned_month', 'year', 'month', 'day').mode('overwrite').format('parquet').save('/tmp/bbergua/ccc/id/')#.repartition('partitioned_month')
	subprocess.call('hdfs dfs -chmod -R o+rx /tmp/bbergua/ccc/id/', shell=True)
	subprocess.call('hdfs dfs -chmod    o-x  /tmp/bbergua/ccc/id/partitioned_month=*/year=*/month=*/day=*/*', shell=True)
	
	print '[' + time.ctime() + ']', 'Process finished'

	spark.stop()
	
	print '[' + time.ctime() + ']', 'SparkSession stopped'
