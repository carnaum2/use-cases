#!/usr/bin/env python
# -*- coding: utf-8 -*-


from configuration import Configuration
from DP_prepare_input_cvm_nifs_compartidos import DPPrepareInputCvmNifsCompartidos
import argparse
import re
import subprocess
import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, current_date, datediff, lit, lpad, translate, udf, when  # struct, UserDefinedFunction
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType, StringType, TimestampType


class DPPrepareInputAmdocsIds:

    id_cols = ("Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_CODIGO_POSTAL", "Cust_NOMBRE_CLI_FACT", "Cust_APELLIDO1_CLI_FACT",
            "Cust_APELLIDO2_CLI_FACT", "Cust_DIR_LINEA1", "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2",
            "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4", "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE", "CCC_L2_bucket_list",
            "CCC_L2_bucket_set", "Serv_NUM_SERIE_DECO_TV", "Order_N1_Description", "Order_N2_Description",
            "Order_N5_Description", "Order_N7_Description", "Order_N8_Description", "Order_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "device_n5_imei", "Order_N1_Id", "Order_N2_Id", "Order_N3_Id", "Order_N4_Id", "Order_N5_Id", "Order_N6_Id",
            "Order_N7_Id", "Order_N8_Id", "Order_N9_Id", "Order_N10_Id" )

    date_cols = (
        'FECHA_MIGRACION', # YYYY-MM-DD
        'birth_date', # YYYY-MM-DD
        'bam_fx_first',
        'bam_movil_fx_first',
        'fbb_fx_first',
        'fixed_fx_first',
        'movil_fx_first',
        'prepaid_fx_first',
        'tv_fx_first',
        'TACADA',
        'FX_SRV_BASIC',
        'FX_TARIFF',
        'FX_VOICE_TARIFF',
        'FX_DATA',
        'FX_DTO_LEV1',
        'FX_DTO_LEV2',
        'FX_DTO_LEV3',
        'FX_DATA_ADDITIONAL',
        'FX_OOB',
        'FX_NETFLIX_NAPSTER',
        'FX_ROAMING_BASIC',
        'FX_ROAM_USA_EUR',
        'FX_ROAM_ZONA_2',
        'FX_CONSUM_MIN',
        'FX_HOMEZONE',
        'FX_FBB_UPGRADE',
        'FX_DECO_TV',
        'FX_TV_CUOTA_ALTA',
        'FX_TV_TARIFF',
        'FX_TV_CUOT_CHARGES',
        'FX_TV_PROMO',
        'FX_TV_PROMO_USER',
        'FX_TV_ABONOS',
        'FX_TV_LOYALTY',
        'FX_TV_SVA',
        'FX_FOOTBALL_TV',
        'FX_MOTOR_TV',
        'FX_PVR_TV',
        'FX_ZAPPER_TV',
        'FX_TRYBUY_TV',
        'FX_TRYBUY_AUTOM_TV',
        'Bill_N1_Bill_Date',
        'Bill_N5_Bill_Date',
        'Bill_N4_Bill_Date',
        'Bill_N2_Bill_Date',
        'Bill_N3_Bill_Date'
    )
    # date_cols = tuple([x.lower() for x in date_cols])

    timestamp_cols = (
    )
    # timestamp_cols = tuple([x.lower() for x in timestamp_cols])

    dates_cols = date_cols + timestamp_cols

    # all_cols = dates_cols
    # list(set(self.data.columns) - set(all_cols))
    # list(set(all_cols) - set(self.data.columns))

    def __init__(self, app, filter_month, debug=False):
        self.app = app
        self.filter_month = filter_month
        self.debug = debug

        self.spark = app.spark

        self.data = None
        self.dataById = None

        #self.load()
        #self.load_and_join()
        #self.fillna()

    def load(self):
        #if self.debug:
            #self.data.printSchema()
            #self.data.show()
            #print self.data.count()

        # impala-shell -i vgddp354hr.dc.sedc.internal.vodafone.com -q 'use tests_es; show tables;' | grep amdocs_ids_srv
        # hive -e 'use tests_es; show tables;' | grep amdocs_ids_srv
        # hive -e 'use tests_es; describe amdocs_ids_srv_v3;'
        # self.spark.tableNames("tests_es")
        # self.spark.tables("tests_es").show()

        # FIXME
        # result = ' '.join(spark.tableNames("tests_es"))
        # result = subprocess.check_output(['hdfs', 'dfs', '-ls', '-d', '/user/hive/warehouse/tests_es.db/rbl_ids_srv_*'])
        # result = subprocess.check_output(['hdfs', 'dfs', '-ls', '-d', '/user/hive/warehouse/tests_es.db/rbl_ids_test_srv_*'])
        # result = subprocess.check_output(['hdfs', 'dfs', '-ls', '-d', '/user/hive/warehouse/tests_es.db/bbergua_ids_*srv_*'])
        # bbergua_ids_test_srv_20180501_20180531

        # m = re.findall('rbl_ids_test_srv_([\d]{8})', result)
        # m = re.findall('bbergua_ids_.*srv_([\d]{8}_[\d]{8}).*', result)

        ClosingDay=str(self.filter_month)
        print('[Info]', ClosingDay)
        hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))
        path_finaltable = '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/v1.1.0/'+hdfs_partition_path
        #path_finaltable = '/data/udf/vf_es/amdocs_ids/bbergua_amdocs_ids_service_level/'+hdfs_partition_path
        #path_finaltable = '/tmp/bbergua/amdocs_inf_dataset/all_amdocs_inf_dataset/'+hdfs_partition_path
        #path_finaltable = '/user/bbergua/amdocs_inf_dataset_new/amdocs_ids_service_level/'+hdfs_partition_path
        yearMonthDay = ClosingDay

        print '['+time.ctime()+']', 'Amdocs IDS Srv path', path_finaltable
        print '['+time.ctime()+']', 'Reading Amdocs IDS Srv', ClosingDay, ' ...'

        #self.data = self.spark.read.table('tests_es.bbergua_amdocs_ids_srv')
        #self.data = self.spark.read.table('tests_es.amdocs_ids_srv_v3')
        self.data = self.spark.read.load(path_finaltable)
        #self.data = self.spark.read.load('/data/udf/vf_es/amdocs_ids/')
        #for c in self.data.columns:
        #    if c.startswith('Raw_') or c.startswith('Bucket_'):
        #        self.data = self.data.withColumnRenamed(c, 'ccc_'+c)

        # if 'closingday' not in self.data.columns:
        #     self.data = self.data.withColumn('closingday', concat(col('year'),lpad(col('month'),2,'0'),lpad(col('day'),2,'0')))

        # m = self.data.select('closingday').distinct().rdd.flatMap(lambda x: x).collect()
        # if self.filter_month is not None:
        #     yearMonthDay = max([c for c in m if c <= str(self.filter_month)+'31'])
        # else:
        #     yearMonthDay = max([c for c in m])
        # ymd_year = yearMonthDay[0:4]
        # ymd_month = yearMonthDay[4:6]
        # ymd_day = yearMonthDay[6:8]

        print '['+time.ctime()+']', 'Read    Amdocs IDS Srv', ClosingDay, ' ... done'

        # FIXME
        #self.data = self.spark.table("tests_es.rbl_ids_srv_"+yearMonthDay)
        #self.data = self.spark.read.parquet('/user/hive/warehouse/tests_es.db/rbl_ids_srv_'+yearMonthDay)
        #self.data = self.spark.read.parquet('/user/hive/warehouse/tests_es.db/rbl_ids_test_srv_'+yearMonthDay)
        # try:
        #     self.data = self.spark.read.parquet('/user/hive/warehouse/tests_es.db/bbergua_ids_srv_'+yearMonthDay)
        # except:
        #     self.data = self.spark.read.parquet('/user/hive/warehouse/tests_es.db/bbergua_ids_test_srv_'+yearMonthDay)
        #self.data = self.data.filter('closingday == '+str(yearMonthDay))

        self.data = (self.data#.withColumnRenamed('NIF_CLIENTE', 'nif')
                              .withColumn('partitioned_month', lit(int(ClosingDay[:6])))
                              .withColumn('year',  lit(int(ClosingDay[:4])))
                              .withColumn('month', lit(int(ClosingDay[4:6])))
                              .withColumn('day',   lit(int(ClosingDay[6:8])))
                    )

        #self.data.cache()

        # self.data.groupBy(['partitioned_month', 'year', 'month']).count().sort('partitioned_month').show()
        #if self.filter_month is not None:
            #self.data = self.data.filter(self.data.partitioned_month == self.filter_month)
            # self.data.groupBy(['partitioned_month', 'year', 'month']).count().show()

        # #print textFile.count()
        # header = textFile.first()
        # #print header
        #
        # #filter out the header, make sure the rest looks correct
        # #textFile_body = textFile.filter(lambda line: line != header)
        # textFile_body = textFile.filter(textFile.value != header.value)
        # #temp_var = textFile_body.map(lambda k: k.split("|"))
        # temp_var = textFile_body.map(lambda k: k.value.split("|")) # This generates a PythonRDD
        # #print temp_var.take(10)
        #
        # #self.data = temp_var.to_df(header.split("|"))
        # self.data = temp_var.to_df(header.value.split("|"))
        # #self.data.show()
        # #self.data.printSchema()
        # #print self.data.count()

    '''
     def load_and_join(self):
        
        #ClosingDay='20180930'
        ClosingDay=str(self.filter_month)
        spark = self.spark

        hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))

        hdfs_read_path_common = '/data/raw/vf_es/'
        hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'

        path_customer = hdfs_write_path_common +'customer/'
        path_service = hdfs_write_path_common +'service/'
        path_customer_agg = hdfs_write_path_common +'customer_agg/'
        path_voiceusage = hdfs_write_path_common +'usage_geneva_voice/'
        path_datausage = hdfs_write_path_common +'usage_geneva_data/'
        path_billing = hdfs_write_path_common +'billing/'
        path_campaignscustomer = hdfs_write_path_common +'campaigns_customer/'
        path_campaignsservice = hdfs_write_path_common +'campaigns_service/'
        path_roamvoice = hdfs_write_path_common +'usage_geneva_roam_voice/'
        path_roamdata = hdfs_write_path_common +'usage_geneva_roam_data/'
        path_orders_hist = hdfs_write_path_common +'orders/'
        path_orders_agg = hdfs_write_path_common +'orders_agg/'
        path_penal_cust = hdfs_write_path_common +'penalties_customer/'
        path_penal_srv = hdfs_write_path_common +'penalties_service/'
        path_devices = hdfs_write_path_common +'device_catalogue/'
        path_ccc = hdfs_write_path_common +'call_centre_calls/'
        path_tnps = hdfs_write_path_common +'tnps/'
        path_perms_and_prefs = hdfs_write_path_common +'perms_and_prefs/'

        path_finaltable = hdfs_write_path_common + 'bbergua_amdocs_ids_service_level/' + hdfs_partition_path

        path_customer_save = path_customer + hdfs_partition_path
        path_service_save = path_service + hdfs_partition_path
        path_customer_agg_save = path_customer_agg + hdfs_partition_path
        path_voiceusage_save = path_voiceusage + hdfs_partition_path
        path_datausage_save = path_datausage + hdfs_partition_path
        path_billing_save = path_billing + hdfs_partition_path
        path_campaignscustomer_save = path_campaignscustomer + hdfs_partition_path
        path_campaignsservice_save = path_campaignsservice + hdfs_partition_path
        path_roamvoice_save = path_roamvoice + hdfs_partition_path
        path_roamdata_save = path_roamdata + hdfs_partition_path
        path_orders_hist_save = path_orders_hist + hdfs_partition_path
        path_orders_agg_save = path_orders_agg + hdfs_partition_path
        path_penal_cust_save = path_penal_cust + hdfs_partition_path
        path_penal_srv_save = path_penal_srv + hdfs_partition_path
        path_devices_save = path_devices + hdfs_partition_path
        path_ccc_save = path_ccc + hdfs_partition_path
        path_tnps_save = path_tnps + hdfs_partition_path
        path_perms_and_prefs_save = path_perms_and_prefs + hdfs_partition_path

        # Load HDFS files for final join
        print '['+time.ctime()+']', 'Loading datasources...'
        customerDF_load = (spark.read.load(path_customer_save))
        serviceDF_load = (spark.read.load(path_service_save))
        customerAggDF_load = (spark.read.load(path_customer_agg_save))
        voiceUsageDF_load = (spark.read.load(path_voiceusage_save))
        dataUsageDF_load = (spark.read.load(path_datausage_save))
        billingDF_load = (spark.read.load(path_billing_save))
        customerCampaignsDF_load = (spark.read.load(path_campaignscustomer_save))
        serviceCampaignsDF_load = (spark.read.load(path_campaignsservice_save))
        #RoamVoiceUsageDF_load = (spark.read.load(path_roamvoice_save))
        #RoamDataUsageDF_load = (spark.read.load(path_roamdata_save))
        customer_orders_hist_load = (spark.read.load(path_orders_hist_save))
        customer_orders_agg_load = (spark.read.load(path_orders_agg_save))
        penalties_cust_level_df_load = (spark.read.load(path_penal_cust_save))
        penalties_serv_level_df_load = (spark.read.load(path_penal_srv_save))
        devices_srv_df_load = (spark.read.load(path_devices_save))
        df_ccc_load = (spark.read.load(path_ccc_save))
        df_tnps_load = (spark.read.load(path_tnps_save))
        # Addresses
        #addressDF = get_address(spark, ClosingDay)

        print '['+time.ctime()+']', 'Join datasources...'
        data_CAR_SRV=(customerDF_load
            .join(customerAggDF_load, 'NUM_CLIENTE', 'inner')
            .join(serviceDF_load, 'NUM_CLIENTE', 'inner')
            #.join(addressDF[data_address_fields], ['DIR_NUM_DIRECCION', 'NUM_CLIENTE'], 'leftouter')
            .join(voiceUsageDF_load, (col('CAMPO2') == col('id_msisdn')), 'leftouter')
            .join(dataUsageDF_load, (col('CAMPO2')==col('id_msisdn_data')), 'leftouter')
            #.join(RoamVoiceUsageDF_load, (col('CAMPO2')==col('id_msisdn_voice_roam')), 'leftouter') NEEDED IMSI FOR JOIN!!!!!!
            #.join(RoamDataUsageDF_load, (col('CAMPO2')==col('id_msisdn_data_roam')), 'leftouter')
            .join(billingDF_load, col('customeraccount') == customerDF_load.NUM_CLIENTE, 'leftouter')
            .join(customerCampaignsDF_load, col('nif_cliente')==col('cif_nif'), 'leftouter')
            .join(serviceCampaignsDF_load, 'msisdn', 'leftouter')
            .join(df_ccc_load, 'msisdn','leftouter')
            .join(df_tnps_load, 'msisdn','leftouter')
            .join(penalties_cust_level_df_load,'NUM_CLIENTE','leftouter')
            .join(penalties_serv_level_df_load, ['NUM_CLIENTE','Instancia_P'], 'leftouter')
            .join(customer_orders_agg_load, 'NUM_CLIENTE', 'leftouter')
            .join(customer_orders_hist_load, 'NUM_CLIENTE', 'leftouter')
            .join(devices_srv_df_load, 'msisdn','leftouter')
           #.join(netscout_cells_df, 'msisdn', 'leftouter')
           #.join(netscout_quality_df, 'msisdn', 'leftouter')
           #.join(netscout_apps_df, 'msisdn', 'leftouter')
            .drop(*['id_msisdn_data', 'id_msisdn', 'cif_nif', 'customeraccount','id_msisdn_data_roam','id_msisdn_voice_roam','rowNum'])
            .withColumn('ClosingDay',lit(ClosingDay))
            )

        self.data = data_CAR_SRV

        yearMonthDay = ClosingDay
        ymd_year = yearMonthDay[0:4]
        ymd_month = yearMonthDay[4:6]
        ymd_day = yearMonthDay[6:8]
        self.data = self.data.withColumnRenamed('NIF_CLIENTE', 'nif')\
                             .withColumn('partitioned_month', lit(int(ymd_year+ymd_month)))\
                             .withColumn('year',  lit(int(ymd_year)))\
                             .withColumn('month', lit(int(ymd_month)))\
                             .withColumn('day',   lit(int(ymd_day)))

        for c in self.data.columns:
            if c.startswith('Raw_') or c.startswith('Bucket_'):
                self.data = self.data.withColumnRenamed(c, 'ccc_'+c)

        print '[' + time.ctime() + '] ' + 'Saving started in: ' + path_finaltable
        self.data.repartition(70).write.mode('overwrite').format('parquet').save(path_finaltable)
        print '[' + time.ctime() + '] ' + 'Saving finished!'

    '''
    def fillna(self):
        # train_df = cvm_pospago_df.select(train_cols).replace("", "NA")

        # Replace null (NA), and empty values with 0 in numeric fields
        # self.data.select('SEG_CLIENTE').distinct().show()
        # self.data = self.data.fillna(0, self.numeric_cols+tuple(['SEG_CLIENTE']))
        #                      .replace("", "0", self.numeric_cols+tuple(['SEG_CLIENTE']))
        # self.data = self.data.fillna(0, self.numeric_cols)#.replace('', '0', self.numeric_cols)
        # FIXME: Handle nulls, NAs, "", etc.
        # self.data.select('SEG_CLIENTE').distinct().show()
        # sys.exit()

        self.integer_cols = tuple(set(self.integer_cols) & set(self.data.columns))
        self.float_cols = tuple(set(self.float_cols) & set(self.data.columns))
        self.numeric_cols = self.integer_cols + self.float_cols
        self.boolean_cols = tuple(set(self.boolean_cols) & set(self.data.columns))
        self.date_cols = tuple(set(self.date_cols) & set(self.data.columns))
        self.timestamp_cols = tuple(set(self.timestamp_cols) & set(self.data.columns))
        self.dates_cols = self.date_cols + self.timestamp_cols
        self.string_cols = tuple(set(self.string_cols) & set(self.data.columns))

        self.data = self.data.replace('', '0', self.numeric_cols + self.boolean_cols)
        # TODO: self.data = self.data.replace('', 'YYYYMMDD', self.date_cols)
        # TODO: self.data = self.data.replace('', 'YYYYMMDD 00:00:00', self.timestamp_cols)
        for c in ['FECHA_MIGRACION', 'birth_date']:
            self.data = self.data.withColumn(c, translate(c, '-', ''))
        self.data = self.data.replace('', 'NA', self.string_cols)

        # Now lets cast the columns that we actually care about to dtypes we want

        # print self.integer_cols
        for c in self.integer_cols:
            self.data = self.data.withColumn(c, self.data[c].cast(IntegerType()))
        # self.data.printSchema()

        for c in self.float_cols:
            self.data = self.data.withColumn(c, self.data[c].cast(FloatType()))

        for c in self.boolean_cols:
            # self.data = self.data.withColumn(c, self.data[c].cast(BooleanType()))
            self.data = self.data.withColumn(c, self.data[c].cast(IntegerType()))

        for c in self.date_cols:
            self.data = self.data.withColumn(c, self.data[c].cast(DateType()))

        for c in self.timestamp_cols:
            self.data = self.data.withColumn(c, self.data[c].cast(TimestampType()))

        # self.long_cat_cols = tuple(set(self.long_cat_cols) & set(self.data.columns))
        # self.id_cols = tuple(set(self.id_cols) & set(self.data.columns))
        for c in self.string_cols:
            self.data = self.data.withColumn(c, self.data[c].cast(StringType()))

        self.categ_fields = tuple(set(self.categ_fields) & set(self.data.columns))
        self.service_fields = tuple(set(self.service_fields) & set(self.data.columns))
        self.numeric_fields = tuple(set(self.numeric_fields) & set(self.data.columns))
        self.max_fields = tuple(set(self.max_fields) & set(self.data.columns))

        #print 'Original variables to take:'
        #self.data.printSchema()
        # self.data.show()

    # Replace column names of the type 'fun(colname)' by 'fun_colname'
    # Also replace any character not in [a-zA-Z0-9_] with '_'
    @staticmethod
    def fix_column_names(df):
        names = df.schema.names

        for n in names:
            m = re.search('([^()]*)\(([^()]*)\)', n)
            if m is not None:
                #print m.group(0), '->', m.group(1) + '_' + m.group(2)
                df = df.withColumnRenamed(n, m.group(1) + '_' + m.group(2))

            m = re.sub('[^a-zA-Z0-9_]', '_', n)
            if n != m:
                df = df.withColumnRenamed(n, m)

        return df

    def generate_features(self):
        # Is NA EMAIL_CLIENTE?
        self.data = self.data.withColumn('no_dispone_cta_correo', when((self.data['CTA_CORREO'] == 'NO DISPONE'), True)
                                         .otherwise(False))
        self.data = self.data.withColumn('is_na_cta_correo', when((self.data['CTA_CORREO'] == ''), True)
                                         .otherwise(False))
        self.data = self.data.withColumn('is_na_cta_correo_contacto', when((self.data['CTA_CORREO_CONTACTO'] == ''), True)
                                         .otherwise(False))

        # Calculate days and years

        # TODO
        # self.data = self.data.withColumn('AGE', datediff(current_date(), self.data.x_fecha_nacimiento)/365.2425)
        for c in self.dates_cols:
            self.data = self.data.withColumn('days_since_'+c,
                                             datediff(current_date(), self.data[c]).cast(IntegerType()))
            self.data = self.data.withColumn('years_since_'+c,
                                             (datediff(current_date(), self.data[c])/365.2425).cast(FloatType()))
        # self.data = self.data.withColumn('AGE', self.data['DAYS_SINCE_x_fecha_nacimiento']/365.2425)

        # Fill NAs
        days_since_cols = [item for item in self.data.columns if item.startswith('days_since_')]
        years_since_cols = [item for item in self.data.columns if item.startswith('years_since_')]
        dates_to_fill_cols = days_since_cols + years_since_cols
        self.data = self.data.fillna(-1, dates_to_fill_cols)#.replace('', '-1', dates_to_fill_cols)

        # TODO: One-hot encoder for fields that have a different (categorical) value for every service
        # cols = ['x_num_ident', #'SEG_CLIENTE',
        #         'x_plan', 'PLANDATOS', #'NOMBRE_TARIFA', 'NOMBRE_TARIFA_VF',
        #         #'PRODADSLENPROV', 'PRODADSL', 'PRODLPD', 'PRODHZ', 'PRODFTTH', 'PRODTIVO', 'PRODVFBOX', 'PRODFUTBOL',
        #         'PROMOCION_VF', 'PROMOCION_TARIFA', 'modelo', 'sistema_operativo',
        #         'PPRECIOS_DESTINO', 'ppid_destino', 'TARIFA_CANJE']
        # self.data.where('x_num_ident = \'72072005X\'').select(*[cols]).show()
        # joined = joined.join(self.data.select(*cols).groupBy('x_num_ident', 'partitioned_month').max(), ['x_num_ident', 'partitioned_month'])
        # joined.show()

        self.data.cache()

        if self.debug:
            self.data.show()
            self.data.printSchema()
            self.data.describe().show()
            # print self.data.count()

        print 'Finished generate_features()'

    def to_df(self):
        return self.data

    def calculate_mobile_only_and_convergent_by_id(self):
        print '['+time.ctime()+']', 'Starting calculate_mobile_only_and_convergent_by_id()'

        # Aggregate categorical (string) fields by taking the first value,
        # since all appearances under the same id should be equal
        #
        # #cols = ['part_status', 'x_sexo', 'x_tipo_ident', 'x_nacionalidad', 'CODIGO_POSTAL']
        # joined =             self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.part_status))
        # print joined.count()
        # joined = joined.join(self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.x_sexo)), ['nif', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.x_tipo_ident)), ['nif', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.x_nacionalidad)), ['nif', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.CODIGO_POSTAL)), ['nif', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('nif', 'partitioned_month').agg(F.first(self.data.PUNTOS)), ['nif', 'partitioned_month'])
        # joined.show()
        # #self.spark.stop()

        #########################################
        # df = self.data.where('nif = \'71993020F\'').select(*cols+['ARPU'])
        # newDF = df.groupBy('nif', 'partitioned_month').agg(F.max(struct('ARPU', *cols)).alias('tmp')).select("nif","partitioned_month","tmp.*")
        # #newDF = df.groupBy('nif', 'partitioned_month').agg(F.max(struct(df.ARPU, df.x_plan)).alias('tmp'))
        # #.select("nif", "partitioned_month", "tmp.*")
        #########################################

        # For those categorical (string) fields that may have different values depending on the service,
        # take the row corresponding to the service with greatest ARPU
        joined = (self.data.na.drop(subset=['NIF_CLIENTE'])
                           .groupBy('NIF_CLIENTE', 'partitioned_month', 'year', 'month', 'day')
                           .agg(F.max(F.struct('Serv_PRICE_SRV_BASIC', *self.data.columns)).alias('tmp'))
                           .select("tmp.*")
                           .drop('Serv_PRICE_SRV_BASIC')
                 )
        #if self.debug:
        #    joined.show()

        #if self.debug:
            # joined.filter('movil_services >= 4').show()
        #    joined.filter(joined['movil_services'] >= 4).show()

        if 'Cust_Agg_seg_pospaid_nif' in joined.columns:
            joined = joined.withColumn('is_mobile_only', when(col('Cust_Agg_seg_pospaid_nif') == 'Mobile_only', True).otherwise(False))
            joined = joined.withColumn('is_convergent',  when(col('Cust_Agg_seg_pospaid_nif') == 'Convergent', True).otherwise(False))
            #joined.groupby('Cust_Agg_seg_pospaid_nif', 'is_mobile_only', 'is_convergent').count().show()
        else:
            if 'movil_services' in joined.columns:
                mob_col = 'movil_services'
            elif 'mobile_services_nif' in joined.columns:
                mob_col = 'mobile_services_nif'
            else:
                mob_col = 'Cust_Agg_mobile_services_nif'

            if 'fbb_services' in joined.columns:
                fbb_col = 'fbb_services'
            elif 'fbb_services_nif' in joined.columns:
                fbb_col = 'fbb_services_nif'
            else:
                mob_col = 'Cust_Agg_fbb_services_nif'

            # Postpaid mobile, and optionally with prepaid mobile, and no FBB
            mo_condition = (joined[mob_col] > 0) & (joined[fbb_col] == 0)
            joined = joined.withColumn('is_mobile_only', when(mo_condition, True).otherwise(False))

            # FBB, and optionally with pre or postpaid mobile
            co_condition = (joined[mob_col] > 0) & (joined[fbb_col] > 0)
            joined = joined.withColumn('is_convergent', when(co_condition, True).otherwise(False))

        #if self.debug:
        #    joined.show()
            # joined.filter('movil_services >= 4').show() # WTF: For some reason this sentence no longer works!!!
        #    joined.filter(joined['movil_services'] >= 4).show()
            # print joined.count()

        self.dataById = joined

        # train.select('Age','Gender').dropDuplicates().show()

        # Aggregate dates

  #       days_since_cols = [item for item in self.data.columns if item.startswith('days_since_')]
  #       years_since_cols = [item for item in self.data.columns if item.startswith('years_since_')]
  #       dates_to_agg_cols = ['nif', 'partitioned_month'] + days_since_cols + ['x_dia_emision'] + years_since_cols
  #       self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
  #                                          .groupBy('nif', 'partitioned_month').min(), ['nif', 'partitioned_month'])
  #       self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
  #                                          .groupBy('nif', 'partitioned_month').avg(), ['nif', 'partitioned_month'])
  #       self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
  #                                          .groupBy('nif', 'partitioned_month').max(), ['nif', 'partitioned_month'])
  #       self.dataById = self.fix_column_names(self.dataById)
		# #self.dataById.printSchema()

  #       if self.debug:
  #           # data.select(*['nif','partitioned_month']+days_since_cols)
  #           # .groupBy('nif','partitioned_month').agg(stddev(data.DAYS_SINCE_x_fecha_nacimiento)).show()
  #           self.dataById.show()
  #           self.dataById.filter(self.dataById['sum_flagvoz'] >= 4).show()
  #           # self.dataById.where('nif = \'72072005X\'').show()
  #           print self.dataById.count()
  #           print self.dataById.columns

        # self.data.groupBy('nif', 'partitioned_month').count().where('count >= 5').show()
        # cols = ['nif', 'partitioned_month', 'COD_SEGFID', 'DESC_SEGFID', 'x_dia_emision']
        # self.data.where('nif = \'72072005X\'').select(cols).show()
        # self.data.where('nif = \'71993020F\'').select(*service_fields+numeric_fields).show()
        # self.data.where('nif = \'51057935M\'').select(['nif','partitioned_month']+service_fields+integer_fields).show()
        # print [self.data.groupBy('nif', 'partitioned_month').count().count(),
        # cvm_sums.count(), cvm_maxs.count(), cvm_dias_max.count(), cvm_dias_min.count(), cvm_dias_avg.count()]

        self.dataById = self.fix_column_names(self.dataById)
        self.dataById.cache()

        print '['+time.ctime()+']', 'Finished calculate_mobile_only_and_convergent_by_id()'

    def get_mobile_only(self):
        mobile_only = self.dataById.where(self.dataById['is_mobile_only'] == True)
        if self.debug:
            print '['+time.ctime()+']', 'Mobile-Only:'
            mobile_only.cache()
            # mobile_only.count()
            # mobile_only.printSchema()
            mobile_only.groupby('partitioned_month', 'year', 'month', 'day').count().show()
            if (mobile_only.groupby('partitioned_month', 'year', 'month', 'day').count() <= 0):
                spark.stop()

        return mobile_only

    def get_convergent(self):
        convergent = self.dataById.where(self.dataById['is_convergent'] == True)
        if self.debug:
            print '['+time.ctime()+']', 'Convergent:'
            convergent.cache()
            # convergent.count()
            # convergent.printSchema()
            convergent.groupby('partitioned_month', 'year', 'month', 'day').count().show()
            if (convergent.groupby('partitioned_month', 'year', 'month', 'day').count() <= 0):
                spark.stop()

        return convergent

    def get_not_convergent(self):
        not_convergent = self.dataById.where(self.dataById['is_convergent'] == False)
        if self.debug:
            print '['+time.ctime()+']', 'Not convergent:'
            not_convergent.cache()
            # not_convergent.count()
            # not_convergent.printSchema()
            not_convergent.groupby('partitioned_month', 'year', 'month', 'day').count().show()

        return not_convergent

    def get_converged_and_not_converged(self, other):
        if self.debug:
            print '['+time.ctime()+']', 'Starting get_converged_and_not_converged()'

        # other = other.to_df()

        this_mo = self.get_mobile_only()

        # Converged = this month are Mobile-Only and in a future month are Convergent
        other_co = other.get_convergent().select('nif_cliente')
        converged = this_mo.join(other_co, 'nif_cliente')
        if self.debug:
            print '['+time.ctime()+']', 'Converged:'
            converged.cache()
            # converged.count()
            # converged.printSchema()
            converged.groupby('partitioned_month', 'year', 'month', 'day').count().show()

        # Not-Converged = this month are Mobile-Only and in a future month are Mobile-Only,
        # so those who did churn are left out
        other_nco = other.get_mobile_only().select('nif_cliente')
        not_converged = this_mo.join(other_nco, 'nif_cliente')
        if self.debug:
            print '['+time.ctime()+']', 'Not converged:'
            not_converged.cache()
            # not_converged.count()
            # not_converged.printSchema()
            not_converged.groupby('partitioned_month', 'year', 'month', 'day').count().show()

        if self.debug:
            print '['+time.ctime()+']', 'Finished get_converged_and_not_converged()'

        return converged, not_converged


if __name__ == "__main__":
    # PYTHONIOENCODING=utf-8 ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 ~/IdeaProjects/convergence/src/main/python/DP_prepare_input_rbl_ids_srv.py 201803 201804
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G ~/fy17.capsule/convergence/src/main/python/DP_prepare_input_amdocs_ids.py --debug 201804 201806
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 60G --driver-memory 8G --conf spark.executor.cores=5 --conf spark.dynamicAllocation.maxExecutors=40 --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G --conf 'spark.driver.extraJavaOptions=-XX:-UseCodeCacheFlushing' --conf 'spark.executor.extraJavaOptions=-XX:-UseCodeCacheFlushing' /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/DP_prepare_input_amdocs_ids.py --debug 20180731 20180930 2>&1 | tee -a salida.ids
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=8G --conf spark.yarn.executor.memoryOverhead=8G ~/fy17.capsule/convergence/src/main/python/DP_prepare_input_amdocs_ids.py --debug 20180930 20181130 | tee salida.ids.20181130
    parser = argparse.ArgumentParser(description='VF_ES CVM Prepare Input Amdocs Informational Dataset',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    parser.add_argument('-ini_month', '--ini_month',metavar='<ini-month>', type=str, help='Date (YYYYMM) of the initial month')
    parser.add_argument('-end_month','--end_month', metavar='<end-month>', type=str, help='Date (YYYYMM) of the end month')
    parser.add_argument('-d', '--debug', action='store_true', help='show debug messages')
    args = parser.parse_args()
    print 'args =', args
    print 'ini_month =', args.ini_month, ', end_month =', args.end_month, ', debug =', args.debug



    # print os.environ.get('SPARK_COMMON_OPTS', '')
    # print os.environ.get('PYSPARK_SUBMIT_ARGS', '')

    # sc, sparkSession, sqlContext = run_sc()

    # spark = (SparkSession.builder
    #        .appName("VF_ES AMDOCS Informational Dataset")
    #        .master("yarn")
    #        .config("spark.submit.deployMode", "client")
    #        .config("spark.ui.showConsoleProgress", "true")
    #        .enableHiveSupport()
    #        .getOrCreate()
    #        )

    conf = Configuration()

    # obj = DPPrepareInputAmdocsIds(conf, None)

    # obj.to_df().write\
    #     .mode('overwrite')\
    #     .partitionBy('partitioned_month', 'year', 'month', 'day')\
    #     .format('parquet')\
    #     .save('/tmp/bbergua/convergence_data/clean')

    # print 'rbl_ids_srv cleaned!'

    # conf.spark.stop()
    # sys.exit()

    #########
    # By Id #
    #########

    obj = DPPrepareInputAmdocsIds(conf, args.ini_month, args.debug)

    #obj.load_and_join()
    obj.load()
    obj.calculate_mobile_only_and_convergent_by_id()

    mo = obj.get_mobile_only()
    # ofile = '/tmp/bbergua/convergence_data/mobile_only-'+str(args.ini_month)
    # print '['+time.ctime()+']', 'Saving mobile_only', args.ini_month, ofile
    # # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/mobile_only/partitioned_month='+str(args.ini_month), shell=True)
    # # mo.write.save('/tmp/bbergua/convergence_data/mobile_only', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    # mo.repartition(12).write.save(ofile, format='parquet', mode='overwrite')

    # convnt = obj.get_convergent()
    # ofile = '/tmp/bbergua/convergence_data/convergent-'+str(args.ini_month)
    # print '['+time.ctime()+']', 'Saving convergent', args.ini_month, ofile
    # # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/convergent/partitioned_month='+str(args.ini_month), shell=True)
    # # convnt.write.save('/tmp/bbergua/convergence_data/convergent', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    # convnt.repartition(6).write.save(ofile, format='parquet', mode='overwrite')

    obj2 = DPPrepareInputAmdocsIds(conf, args.end_month, args.debug)
    #obj2.load_and_join()
    obj2.load()
    obj2.calculate_mobile_only_and_convergent_by_id()

    #conf.spark.stop()
    #sys.exit()

    mo2 = obj2.get_mobile_only()
    ofile = '/data/udf/vf_es/convergence_data/mobile_only-'+str(args.end_month)
    print '['+time.ctime()+']', 'Saving mobile_only', args.end_month, ofile
    # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/mobile_only/partitioned_month='+str(args.end_month), shell=True)
    # mo2.write.save('/tmp/bbergua/convergence_data/mobile_only', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    mo2.repartition(12).write.save(ofile, format='parquet', mode='overwrite')

    # convnt2 = obj2.get_convergent()
    # ofile = '/tmp/bbergua/convergence_data/convergent-'+str(args.end_month)
    # print '['+time.ctime()+']', 'Saving convergent', args.end_month, ofile
    # # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/convergent/partitioned_month='+str(args.end_month), shell=True)
    # # convnt2.write.save('/tmp/bbergua/convergence_data/convergent', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    # convnt2.repartition(6).write.save(ofile, format='parquet', mode='overwrite')

    convd, not_convd = obj.get_converged_and_not_converged(obj2)

    ofile = '/data/udf/vf_es/convergence_data/converged-'+str(args.ini_month)
    print '['+time.ctime()+']', 'Saving converged', args.ini_month, ofile
    # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/converged/partitioned_month='+str(args.ini_month), shell=True)
    # convd.write.save('/tmp/bbergua/convergence_data/converged', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    convd.repartition(1).write.save(ofile, format='parquet', mode='overwrite')

    ofile = '/data/udf/vf_es/convergence_data/not_converged-'+str(args.ini_month)
    print '['+time.ctime()+']', 'Saving not_converged', args.ini_month, ofile
    # subprocess.call('hdfs dfs -rm -r /tmp/bbergua/convergence_data/not_converged/partitioned_month='+str(args.ini_month), shell=True)
    # not_convd.write.save('/tmp/bbergua/convergence_data/not_converged', format='parquet', mode='append', partitionBy=['partitioned_month', 'year', 'month', 'day'])
    not_convd.repartition(8).write.save(ofile, format='parquet', mode='overwrite')

    print '['+time.ctime()+']', 'Process finished!'

    conf.spark.stop()