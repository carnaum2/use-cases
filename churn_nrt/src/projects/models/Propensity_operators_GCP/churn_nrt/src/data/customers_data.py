
from pyspark.sql.functions import (col, lit, lower, concat, count, max, avg, desc, asc, row_number, lpad, trim, when, isnull)
from pyspark.sql import Window
from my_propensity_operators.churn_nrt.src.data_utils.DataTemplate import DataTemplate


CONFIGS_PATH = "../../resource/configs.yml"

def get_last_date(spark):
    '''
    Return the last date YYYYMMDD with info in the raw source
    :param spark:
    :return:
    '''

    last_date = spark\
    .read\
    .parquet('/data/raw/vf_es/customerprofilecar/CUSTOMEROW/1.0/parquet/')\
    .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))\
    .select(max(col('mydate')).alias('last_date'))\
    .rdd\
    .first()['last_date']

    return int(last_date)




class Customer(DataTemplate):

    def __init__(self, spark, confs):
        DataTemplate.__init__(self, spark, confs)
        self.confs = confs
        self.confs['module_name'] = 'customer'
        self.set_path_configs()
        self.PATH_RAW_CUSTOMER = self.confs["customer_raw"] 





    # def set_path_configs(self):
    #     super(Customer, self).set_path_configs()



    def is_default_module(self, *args, **kwargs):
        print("[Customer] is_default_module | args: {} | kwargs: {}".format(args, kwargs))

        # if None --> then it can be saved, since it means default module was requested
        check =  kwargs.get('add_columns', None) == None
        if not check:
            print("[Customer] is_default_module | Module {} cannot be saved since 'add_columns' is different than None".format(self.MODULE_NAME))
        return check

    def build_module(self, closing_day, save_others, add_columns=None, **kwargs):
        '''
        ['NUM_CLIENTE',
         'CLASE_CLI_COD_CLASE_CLIENTE',
         'COD_ESTADO_GENERAL',
         'NIF_CLIENTE',
         'X_CLIENTE_PRUEBA',
         'NIF_FACTURACION',
         'FECHA_MIGRACION',
         'SUPEROFERTA']
        :param closing_day:
        :param add_columns:
        :param args:
        :return:
        '''

        data_customer_fields = ['NUM_CLIENTE', 'CLASE_CLI_COD_CLASE_CLIENTE', 'COD_ESTADO_GENERAL', 'NIF_CLIENTE', 'X_CLIENTE_PRUEBA',
                                'NIF_FACTURACION', 'X_FECHA_MIGRACION', 'SUPEROFERTA', 'year', \
                                'month', 'day']

        print("[Customer] build_module | Requested additional_columns {}".format(add_columns))
        if add_columns:
            data_customer_fields = list(set(data_customer_fields) | set(add_columns))



        data_customer_ori = (self.SPARK.read.load(self.PATH_RAW_CUSTOMER).where(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')) <= closing_day))

        # We use window functions to avoid duplicates of NUM_CLIENTE
        w = Window().partitionBy("NUM_CLIENTE").orderBy(desc("year"), desc("month"), desc("day"))
        data_customer = (data_customer_ori[data_customer_fields].where(data_customer_ori['COD_ESTADO_GENERAL'].isin('01', '03', '07', '09'))
                           .where(col("CLASE_CLI_COD_CLASE_CLIENTE").isin('NE', 'RV', 'DA', 'BA', 'RS'))
                           .where((col("X_CLIENTE_PRUEBA").isNull()) | (col("X_CLIENTE_PRUEBA") != '1')).withColumnRenamed('X_FECHA_MIGRACION', 'FECHA_MIGRACION')
                         # INSIGHTS FILTER
                           #.where(~((col("X_CLIENTE_PRUEBA") == '1') | (col("X_CLIENTE_PRUEBA") == 1) | ((col("NIF_CLIENTE").rlike('^999')) & (~(col("TIPO_DOCUMENTO").rlike("(?i)Pasaporte")))))).withColumnRenamed('X_FECHA_MIGRACION', 'FECHA_MIGRACION')
                           .withColumn("rowNum", row_number().over(w)).drop('year', 'month', 'day'))

        data_customer = data_customer.where(col('rowNum') == 1).drop('rowNum')

        # For those clients without NIF, take NIF_FACTURACION if exists
        data_customer = (data_customer.withColumn('NIF_CLIENTE', when((col('NIF_CLIENTE') == '') & (col('NIF_FACTURACION') != '0'), col('NIF_FACTURACION')).otherwise(data_customer['NIF_CLIENTE'])))

        data_customer = data_customer.filter((~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))
        #print("[Customer] build_module | data_customer.columns={}".format(",".join(data_customer.columns)))

        return data_customer
