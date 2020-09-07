#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration import Configuration
from DP_prepare_input_cvm_nifs_compartidos import DPPrepareInputCvmNifsCompartidos
import argparse
import re
import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_date, datediff, lit, translate, udf, when  # struct, UserDefinedFunction
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType, StringType, TimestampType


class DPPrepareInputCvmPospago:
    '''
    fields = ('x_id_red',
             'x_id_cuenta',
             'x_num_ident',
             'ID_FTTH',
             'part_status',
             'x_sexo',
             'x_fecha_nacimiento',
             'x_tipo_ident',
             'x_nacionalidad',
             'COD_SEGFID',
             'DESC_SEGFID',
             'x_fecha_activacion',
             'X_FECHA_CREACION_CUENTA',
             'INSTALL_DATE',
             'X_FECHA_CREACION_SERVICIO',
             'x_fecha_ini_prov',
             'FECHA_ALTA_SERVICIO',
             'FECHA_ANTIGUEDAD_CLIENTE',
             'FLAGVOZ',
             'FLAGADSL',
             'FLAGLPD',
             'FLAGHZ',
             'FLAGFTTH',
             'FLAGTIVO',
             'FLAGVFBOX',
             'FLAGFUTBOL',
             'x_plan',
             'NOMBRE_TARIFA',
             'NOMBRE_TARIFA_VF',
             'PLANDATOS',
             'PRODADSLENPROV',
             'FECHA_ALTA_SS_ADSLENPROV',
             'SFID_ADSLENPROV',
             'PRODADSL',
             'FECHA_ALTA_SS_ADSL',
             'SFID_ADSL',
             'PRODLPD',
             'FECHA_ALTA_LPD',
             'SFID_LPD',
             'PRODHZ',
             'FECHA_ALTA_SS_HZ',
             'SFID_HZ',
             'PRODFTTH',
             'FECHA_ALTA_SS_FTTH',
             'SFID_FTTH',
             'PRODTIVO',
             'FECHA_ALTA_TIVO',
             'SFID_TV',
             'PRODVFBOX',
             'FECHA_ALTA_VFBOX',
             'PRODFUTBOL',
             'FECHA_ALTA_FUTBOL',
             'SFID_FUTBOL',
             'SFID_SIMO',
             'FECHA_ALTA_SIMO',
             'COBERTURA_4G',
             'FLAG_4G_APERTURAS',
             'FLAG_4G_NODOS',
             'COBERTURA_4G_PLUS',
             'FLAG_4G_PLUS_APERTURAS',
             'FLAG_4G_PLUS_NODOS',
             'FLAG_DESC_CONV',
             'FLAG_SIEBEL',
             'Flag_4G',
             'FLAG_EBILLING',
             'LORTAD',
             'DEUDA',
             'phone',
             'NAME',
             'DIRECCION',
             'CODIGO_POSTAL',
             'LOCALIDAD',
             'PROVINCIA',
             'LINEA_FIJA',
             'EMAIL_CLIENTE',
             'FLAG_EMAIL_UNSUBSCRIBE',
             'PROMOCION_VF',
             'FECHA_FIN_CP_VF',
             'MESES_FIN_CP_VF',
             'PROMOCION_TARIFA',
             'FECHA_FIN_CP_TARIFA',
             'MESES_FIN_CP_TARIFA',
             'IMEI',
             'TAC_FAC',
             'TerminalMMS',
             'modelo',
             'sistema_operativo',
             'TERMINAL_4G',
             'VFSMARTPHONE',
             'COD_GOLDEN',
             'COD_GESCAL_17',
             'FLAG_HUELLA_ONO',
             'FLAG_HUELLA_VF',
             'FLAG_HUELLA_VON',
             'FLAG_NH_PREVISTA',
             'FLAG_NH_REAL',
             'FECHA_NH_REAL',
             'FLAG_HUELLA_MOVISTAR',
             'FLAG_HUELLA_JAZZTEL',
             'FLAG_HUELLA_NEBA',
             'FLAG_HUELLA_EUSKALTEL',
             'FLAG_COBERTURA_ADSL',
             'X_SFID_CUENTA',
             'x_dia_emision',
             'X_SFID_SERVICIO',
             'x_fecha_recepcion',
             'objid_GSM',
             'LAST_UPD',
             'NUM_POSPAGO',
             'NUM_PREPAGO',
             'NUM_TOTAL',
             'PPRECIOS_DESTINO',
             'FECHA_CAMBIO',
             'ppid_destino',
             'SFID_CAMBIO_PPRECIOS',
             'fecha_transferencia',
             'TARIFA_CANJE',
             'FECHA_CANJE',
             'SFID_CANJE',
             'PUNTOS',
             'ARPU',
             'FLAG_FINANCIA',
             'FLAG_FINANCIA_SIMO',
             'CUOTAS_PENDIENTES',
             'CANTIDAD_PENDIENTE',
             'FECHA_ULTIMA_FINANCIACION',
             'FLAG_CUENTA_SUPERINTEGRAL',
             'FLAG_EXISTE_FBB_HOGAR')
    '''

    ########################################
    # Grouping of variables by their types #
    ########################################

    integer_cols = (
        'SEG_CLIENTE',  # 8 levels
        'COD_SEGFID',  # 4 levels
        'x_dia_emision',
        'NUM_POSPAGO',
        'NUM_PREPAGO',
        'NUM_TOTAL',
        'PUNTOS',
        'CUOTAS_PENDIENTES',
        'FLAGVOZ',
        'FLAGADSL',
        'FLAGLPD',
        'FLAGHZ',
        'FLAGFTTH',
        'FLAGTIVO',
        'FLAGVFBOX',
        'FLAGFUTBOL',
        'FLAGMOTOR',

        'MESES_FIN_CP_VF',
        'MESES_FIN_CP_TARIFA'
    )
    integer_cols = tuple([x.lower() for x in integer_cols])

    float_cols = (
        'ARPU',
        'CANTIDAD_PENDIENTE',
        'USO_3M_MB',
        'MIN_LLAM_3M',
        'USO_ULTMES_MB',
        'MIN_LLAM_ULTMES'
    )
    float_cols = tuple([x.lower() for x in float_cols])

    numeric_cols = integer_cols + float_cols

    date_cols = (
        'x_fecha_nacimiento',
        'x_fecha_activacion',
        'INSTALL_DATE',
        'FECHA_ALTA_SERVICIO',
        'FECHA_ANTIGUEDAD_CLIENTE',
        'FECHA_ALTA_SS_ADSLENPROV',
        'FECHA_ALTA_SS_ADSL',
        'FECHA_ALTA_LPD',
        'FECHA_ALTA_SS_HZ',
        'FECHA_ALTA_SS_FTTH',
        'FECHA_ALTA_TIVO',
        'FECHA_ALTA_VFBOX',
        'FECHA_ALTA_FUTBOL',
        'FECHA_ALTA_SIMO',
        'FECHA_FIN_CP_VF',
        'FECHA_FIN_CP_TARIFA',
        'FECHA_NH_REAL',
        'FECHA_CANJE',
        'FECHA_ULTIMA_FINANCIACION',
        'fecha_transferencia',
        'FECHA_ALTA_MOTOR'
    )
    date_cols = tuple([x.lower() for x in date_cols])

    timestamp_cols = (
        'X_FECHA_CREACION_CUENTA',
        'X_FECHA_CREACION_SERVICIO',
        'x_fecha_ini_prov',
        'x_fecha_recepcion',
        'LAST_UPD',
        'FECHA_CAMBIO'
    )
    timestamp_cols = tuple([x.lower() for x in timestamp_cols])

    dates_cols = date_cols + timestamp_cols

    boolean_cols = (
        'COBERTURA_4G',
        'FLAG_4G_APERTURAS',
        'FLAG_4G_NODOS',
        'COBERTURA_4G_PLUS',
        'FLAG_4G_PLUS_APERTURAS',
        'FLAG_4G_PLUS_NODOS',
        'FLAG_DESC_CONV',
        'FLAG_SIEBEL',
        'Flag_4G',
        'FLAG_EBILLING',
        'LORTAD',
        'DEUDA',
        'FLAG_EMAIL_UNSUBSCRIBE',
        'TerminalMMS',
        'TERMINAL_4G',
        'VFSMARTPHONE',
        'FLAG_HUELLA_ONO',
        'FLAG_HUELLA_VF',
        'FLAG_NH_PREVISTA',
        'FLAG_NH_REAL',
        'FLAG_HUELLA_MOVISTAR',
        'FLAG_HUELLA_JAZZTEL',
        'FLAG_HUELLA_NEBA',
        'FLAG_HUELLA_EUSKALTEL',
        'FLAG_FINANCIA',
        'FLAG_FINANCIA_SIMO',
        'FLAG_CUENTA_SUPERINTEGRAL',
        'FLAG_EXISTE_FBB_HOGAR',
        'FLAG_HUELLA_ARIEL_NO_ZC',
        'FLAG_HUELLA_ARIEL',
        'FLAG_HUELLA_NEBA_ZC'
    )
    boolean_cols = tuple([x.lower() for x in boolean_cols])

    string_cols = (
        'part_status',  # 3 levels
        'x_tipo_cliente',  # 2 levels
        'x_subtipo',  # 3 levels
        'x_sexo',  # 9 levels : FIXME: ... H Hombre M Mujer NULL Var\xf3n Var\177n
        'x_tipo_ident',  # 6 levels
        'x_nacionalidad',  # 224 levels
        'DESC_SEGFID',
        'CODIGO_POSTAL',  # 12318 levels
        'x_plan',  # 231 levels
        'PLANDATOS',  # 161 levels
        'PPRECIOS_DESTINO',  # 324 levels
        'ppid_destino',  # 462 levels
        'TARIFA_CANJE',  # 26 levels
        'PROMOCION_VF',  # 244 levels
        'PROMOCION_TARIFA',  # 28 levels
        'FLAG_COBERTURA_ADSL',  # 3 levels
        'FLAG_HUELLA_VON',  # 5 levels
        'TRAMO_PREFERIDO',

        'NOMBRE_TARIFA',
        'NOMBRE_TARIFA_VF',
        'PRODADSLENPROV',  # 8 levels
        'PRODADSL',   # 68 levels
        'PRODLPD',  # 48 levels
        'PRODHZ',  # 5 levels
        'PRODFTTH',  # 18 levels
        'PRODTIVO',  # 9 levels
        'PRODVFBOX',  # 2 levels
        'PRODFUTBOL',  # 7 levels
        'modelo',  # 8587 levels
        'sistema_operativo',  # 251 levels
        'PRODMOTOR',

        'x_id_red',
        'x_id_cuenta',
        'x_num_ident',
        'ID_FTTH',
        'SFID_ADSLENPROV',
        'SFID_ADSL',
        'SFID_LPD',
        'SFID_HZ',
        'SFID_FTTH',
        'SFID_TV',
        'SFID_FUTBOL',
        'SFID_SIMO',
        'phone',
        'NAME',
        'DIRECCION',
        'LOCALIDAD',
        'PROVINCIA',
        'LINEA_FIJA',
        'EMAIL_CLIENTE',
        'IMEI',
        'TAC_FAC',
        'COD_GOLDEN',
        'COD_GESCAL_17',
        'X_SFID_CUENTA',
        'X_SFID_SERVICIO',
        'objid_GSM',
        'SFID_CAMBIO_PPRECIOS',
        'SFID_CANJE',
        'SFID_MOTOR'
    )
    string_cols = tuple([x.lower() for x in string_cols])

    all_cols = numeric_cols + dates_cols + boolean_cols + string_cols
    # list(set(self.data.columns) - set(all_cols))
    # list(set(all_cols) - set(self.data.columns))

    ###############################################
    # Grouping of variables by their intended use #
    ###############################################

    categ_fields = (
        'part_status',  # 3 levels
        'x_tipo_cliente',  # 2 levels
        'x_subtipo',  # 3 levels
        'x_sexo',  # 9 levels : FIXME: ... H Hombre M Mujer NULL Var\xf3n Var\177n
        'x_tipo_ident',  # 6 levels
        'x_nacionalidad',  # 224 levels
        'SEG_CLIENTE',  # 8 levels
        'COD_SEGFID',  # 4 levels
        'DIRECCION',
        'LOCALIDAD',
        'PROVINCIA',
        'CODIGO_POSTAL',  # 12318 levels
        'TRAMO_PREFERIDO',
        'NOMBRE_TARIFA',
        'NOMBRE_TARIFA_VF',
        'x_plan',  # 231 levels
        'PLANDATOS',  # 161 levels
        'PPRECIOS_DESTINO',  # 324 levels
        'ppid_destino',  # 462 levels
        'TARIFA_CANJE',  # 26 levels
        'PROMOCION_VF',  # 244 levels
        'PROMOCION_TARIFA',  # 28 levels
        'FLAG_COBERTURA_ADSL',  # 3 levels
        'FLAG_HUELLA_VON',  # 5 levels
        'modelo',  # 8587 levels
        'sistema_operativo',  # 251 levels
        'x_dia_emision',
        'PUNTOS',
        'PRODADSLENPROV',  # 8 levels
        'PRODADSL',   # 68 levels
        'PRODLPD',  # 48 levels
        'PRODHZ',  # 5 levels
        'PRODFTTH',  # 18 levels
        'PRODTIVO',  # 9 levels
        'PRODVFBOX',  # 2 levels
        'PRODFUTBOL',  # 7 levels
        'PRODMOTOR',  # 3 levels

        # Generated
        'IS_EMAIL_CLIENTE_NA'
    )
    categ_fields = tuple([x.lower() for x in categ_fields])

    service_fields = ('FLAGVOZ', 'FLAGADSL', 'FLAGLPD', 'FLAGHZ', 'FLAGFTTH', 'FLAGTIVO', 'FLAGVFBOX', 'FLAGFUTBOL',
                      'FLAGMOTOR')
    service_fields = tuple([x.lower() for x in service_fields])

    numeric_fields = tuple(['CUOTAS_PENDIENTES']) + float_cols
    numeric_fields = tuple([x.lower() for x in numeric_fields])

    max_fields = ('SEG_CLIENTE', 'NUM_POSPAGO', 'NUM_PREPAGO', 'NUM_TOTAL') + boolean_cols
    max_fields = tuple([x.lower() for x in max_fields])

    unused_fields = (
        # Redundant
        'DESC_SEGFID',
        'MESES_FIN_CP_VF',
        'MESES_FIN_CP_TARIFA',

        # Ids
        'x_id_red',
        'x_id_cuenta',
        'x_num_ident',
        'ID_FTTH',
        'SFID_ADSLENPROV',
        'SFID_ADSL',
        'SFID_LPD',
        'SFID_HZ',
        'SFID_FTTH',
        'SFID_TV',
        'SFID_FUTBOL',
        'SFID_SIMO',
        'phone',
        'NAME',
        'LINEA_FIJA',
        'EMAIL_CLIENTE',
        'IMEI',
        'TAC_FAC',
        'COD_GOLDEN',
        'COD_GESCAL_17',
        'X_SFID_CUENTA',
        'X_SFID_SERVICIO',
        'objid_GSM',
        'SFID_CAMBIO_PPRECIOS',
        'SFID_CANJE',
        'SFID_MOTOR'
    )
    unused_fields = tuple([x.lower() for x in unused_fields])

    all_fields = categ_fields + service_fields + numeric_fields + max_fields + unused_fields + dates_cols
    # list(set(self.data.columns) - set(all_fields))
    # list(set(all_fields) - set(self.data.columns))

    # Fields used in R model
    '''
    convergidos_feats = ("x_num_ident",
                         "part_status",
                         "x_tipo_ident",
                         "x_fecha_nacimiento",
                         "x_nacionalidad",
                         "SEG_CLIENTE",
                         "COD_SEGFID",
                         "x_fecha_activacion",
                         "X_FECHA_CREACION_CUENTA",
                         "X_FECHA_CREACION_SERVICIO",
                         "x_fecha_ini_prov",
                         "x_plan",
                         "PLANDATOS",
                         "Flag_4G",
                         "COBERTURA_4G",
                         "FLAG_4G_APERTURAS",
                         "FLAG_4G_NODOS",
                         "FLAG_EBILLING",
                         "LORTAD",
                         "DEUDA",
                         "EMAIL_CLIENTE",
                         "PROMOCION_VF",
                         "FECHA_FIN_CP_VF",
                         "MESES_FIN_CP_VF",
                         "PROMOCION_TARIFA",
                         "FECHA_FIN_CP_TARIFA",
                         "MESES_FIN_CP_TARIFA",
                         "modelo",
                         "sistema_operativo",
                         "TERMINAL_4G",

                         "LAST_UPD",
                         "NUM_POSPAGO",
                         "NUM_PREPAGO",
                         "NUM_TOTAL",
                         "PPRECIOS_DESTINO",
                         "ppid_destino",
                         "PUNTOS",
                         "ARPU",
                         "COBERTURA_4G_PLUS",

                         "CODIGO_POSTAL",
                         "VFSMARTPHONE",
                         "FLAG_NH_REAL",
                         "FLAG_NH_PREVISTA",
                         "FLAG_FINANCIA",
                         "FLAG_FINANCIA_SIMO",
                         "CUOTAS_PENDIENTES",
                         "CANTIDAD_PENDIENTE",

                         "FLAG_HUELLA_ONO",
                         "FLAG_HUELLA_VF",
                         "FLAG_HUELLA_NEBA",
                         # "FLAG_HUELLA_VON",
                         "FLAG_HUELLA_MOVISTAR",
                         "FLAG_HUELLA_JAZZTEL"
                         # "FLAG_COBERTURA_ADSL"
                         )
    '''

    def __init__(self, app, month, debug=False):
        self.debug = debug

        self.app = app
        self.month = month

        self.spark = app.spark

        self.data = None
        self.dataById = None

        self.load()
        self.clean()
        self.fillna()
        self.generate_features()

    def load_local(self):
        # textFile = spark.textFile("file:///Users/bbergua/Downloads/CVM/"+month+
        # "/INPUT/EXTR_AC_FINAL_POSPAGO_"+month+".txt")
        ifile = "file:///Users/bbergua/Downloads/CVM/" + self.month \
                + "/INPUT/EXTR_AC_FINAL_POSPAGO_" + self.month + ".txt"
        self.data = self.spark.read.load(ifile,
                                              format='com.databricks.spark.csv',
                                              header='true', delimiter='|', charset='latin1')  # , inferSchema='true')

    def load_bdp(self):
        print '['+time.ctime()+']', 'Loading raw_es.vf_pos_ac_final (', self.month, ')'
        # self.spark.tableNames("raw_es")
        # self.spark.tables("raw_es").show()
        self.data = self.spark.table("raw_es.vf_pos_ac_final")
        # self.data.groupBy(['partitioned_month', 'year', 'month', 'day']).count().sort('partitioned_month').show()
        if self.month is not None:
            self.data = self.data.filter(self.data.partitioned_month == self.month)
            c = self.data.count()
            print '['+time.ctime()+']', 'Loaded  raw_es.vf_pos_ac_final (', self.month, ') count =', c
            if c == 0:
                raise ValueError('raw_es.vf_pos_ac_final (%s) has no data' % str(self.month))
        if self.debug:
            self.data.groupBy(['partitioned_month', 'year', 'month', 'day']).count().show()

    def load(self):
        if self.debug:
            self.data.printSchema()
            self.data.show()
            print self.data.count()

        if self.app.is_bdp:
            self.load_bdp()
        else:
            self.load_local()

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

    def clean(self):
        print '['+time.ctime()+']', 'Removing spurious header in raw_es.vf_pos_ac_final ...'
        self.data = self.data.where(self.data['id_ftth'] != 'ID_FTTH')

        # FIXME: This is temporal, until the data are reingested with proper treatment of tabs
        print '['+time.ctime()+']', 'Removing corrupt rows from raw_es.vf_pos_ac_final ...'
        c = 'tac_fac'
        test_num_digits = udf(lambda v: True if (len(v) == 15) & (v.isdigit()) else False, BooleanType())
        # print data.where(test_num_digits(c) == True).count()  # 274
        # data.where(test_num_digits(c) == True).show()
        self.data = self.data.where(test_num_digits(c) == False)
        c = 'cod_golden'
        test_num_digits = udf(lambda v: True if v == '0' or v == '1' else False, BooleanType())
        # print data.where(test_num_digits(c) == True).count()  # 95
        # data.where(test_num_digits(c) == True).show()
        self.data = self.data.where(test_num_digits(c) == False)
        c = 'cobertura_4g_plus'
        test_num_digits = udf(lambda v: True if v == '0' or v == '1' else False, BooleanType())
        # print data.where(test_num_digits(c) == False).count()  #
        # data.where(test_num_digits(c) == False).show()
        self.data = self.data.where(test_num_digits(c) == True)

        if not self.app.is_bdp:
            # self.data.select('ARPU').map(lambda k: k['ARPU'].replace(',', '.'))
            # self.data.select('CANTIDAD_PENDIENTE').map(lambda k: k['CANTIDAD_PENDIENTE'].replace(',', '.'))

            # Replace ',' with '.' in columns ARPU and CANTIDAD_PENDIENTE
            col_names = ['ARPU', 'CANTIDAD_PENDIENTE']
            col_names = [x.lower() for x in col_names]
            # new_df = self.data
            # udf = UserDefinedFunction(lambda x: x.replace(',', '.'), StringType())
            # for name in col_names:
            #     new_df = new_df.select(
            #                         *[udf(column).alias(name) if column == name else column for column in new_df.columns])
            # self.data = new_df
            self.data = self.data.select(*[translate(column, ',', '.')
                                         .alias(column) if column in col_names else column for column in self.data.columns])
            # self.data.show()

        # Fix some fingerprint flags, so that all missing values are '' instead of null
        cols = ['flag_cobertura_adsl', 'flag_cuenta_superintegral', 'flag_existe_fbb_hogar']
        self.data = self.data.fillna('', cols)

        # Fix nationality
        # TODO: Replace 'ñ', and accented vowels
        self.data = self.data.withColumn('x_nacionalidad',
                                          when(self.data['x_nacionalidad'].startswith('Espa'), 'España')
                                         .when(self.data['x_nacionalidad'].startswith('Antar'), 'Antarctica')
                                         .when(self.data['x_nacionalidad'].startswith('Gran Breta'), 'Gran Bretaña')
                                         .when(self.data['x_nacionalidad'].startswith('...'), '')
                                         .otherwise(self.data['x_nacionalidad']))

        # Fix gender
        self.data = self.data.withColumn('x_sexo',
                                          when(self.data['x_sexo'].startswith('H'), 'Varon')
                                         .when(self.data['x_sexo'].startswith('V'), 'Varon')
                                         .when(self.data['x_sexo'].startswith('M'), 'Mujer')
                                         .otherwise(''))

        # Fix custmer's subtype
        self.data = self.data.withColumn('x_subtipo',
                                          when(self.data['x_subtipo'] == 'sin  IAE', 'sin IAE')
                                         .otherwise(self.data['x_subtipo']))

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
        self.data = self.data.replace('', 'NA', self.string_cols)

        # Now lets cast the columns that we actually care about to dtypes we want

        # print self.integer_cols
        for col in self.integer_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(IntegerType()))
        # self.data.printSchema()

        for col in self.float_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(FloatType()))

        for col in self.boolean_cols:
            # self.data = self.data.withColumn(col, self.data[col].cast(BooleanType()))
            self.data = self.data.withColumn(col, self.data[col].cast(IntegerType()))

        for col in self.date_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(DateType()))

        for col in self.timestamp_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(TimestampType()))

        # self.long_cat_cols = tuple(set(self.long_cat_cols) & set(self.data.columns))
        # self.id_cols = tuple(set(self.id_cols) & set(self.data.columns))
        for col in self.string_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(StringType()))

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
        print '['+time.ctime()+']', 'Starting generate_features()'

        # Is NA EMAIL_CLIENTE?
        self.data = self.data.withColumn('is_na_email_cliente', when((self.data['email_cliente'] == ''), True)
                                         .otherwise(False))
        # TODO: is_na_*: IS_NA_FECHA_NACIMIENTO, IS_NA_SEG_CLIENTE, IS_NA_COD_SEGFID, ...

        # Calculate days and years

        # self.data = self.data.withColumn('AGE', datediff(current_date(), self.data.x_fecha_nacimiento)/365.2425)
        for col in self.dates_cols:
            self.data = self.data.withColumn('days_since_'+col,
                                             datediff(current_date(), self.data[col]).cast(IntegerType()))\
                                 .withColumn('years_since_'+col,
                                             (datediff(current_date(), self.data[col])/365.2425).cast(FloatType()))
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

        if self.debug:
            self.data.show()
            self.data.printSchema()
            self.data.describe().show()
            # print self.data.count()
            self.data.groupBy(['partitioned_month', 'year', 'month']).count().show()

        print '['+time.ctime()+']', 'Finished generate_features()'

    def to_df(self):
        return self.data

    def calculate_mobile_only_and_convergent_by_id(self):
        print '['+time.ctime()+']', 'Starting calculate_mobile_only_and_convergent_by_id()'

        # Load NIFS_COMPARTIDOS
        nifs_comp = DPPrepareInputCvmNifsCompartidos(self.app, self.month)

        # Aggregate categorical (string) fields by taking the first value,
        # since all appearances under the same id should be equal
        #
        # #cols = ['part_status', 'x_sexo', 'x_tipo_ident', 'x_nacionalidad', 'CODIGO_POSTAL']
        # joined =             self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.part_status))
        # print joined.count()
        # joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.x_sexo)), ['x_num_ident', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.x_tipo_ident)), ['x_num_ident', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.x_nacionalidad)), ['x_num_ident', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.CODIGO_POSTAL)), ['x_num_ident', 'partitioned_month'])
        # joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month').agg(F.first(self.data.PUNTOS)), ['x_num_ident', 'partitioned_month'])
        # joined.show()
        # #self.spark.stop()

        #########################################
        # df = self.data.where('x_num_ident = \'71993020F\'').select(*cols+['ARPU'])
        # newDF = df.groupBy('x_num_ident', 'partitioned_month').agg(F.max(struct('ARPU', *cols)).alias('tmp')).select("x_num_ident","partitioned_month","tmp.*")
        # #newDF = df.groupBy('x_num_ident', 'partitioned_month').agg(F.max(struct(df.ARPU, df.x_plan)).alias('tmp'))
        # #.select("x_num_ident", "partitioned_month", "tmp.*")
        #########################################

        # For those categorical (string) fields that may have different values depending on the service,
        # take the row corresponding to the service with greatest ARPU
        joined = self.data.groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day')\
                          .agg(F.max(F.struct('arpu', *self.categ_fields)).alias('tmp'))\
                          .select('x_num_ident', 'partitioned_month', 'year', 'month', 'day', "tmp.*")\
                          .drop('arpu')
        if self.debug:
            joined.show()

        joined = joined.join(self.data.groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day')
                                      .sum(*self.service_fields+self.numeric_fields), ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'])
        joined = self.fix_column_names(joined)

        if self.debug:
            # joined.filter('sum_flagvoz >= 4').show()
            joined.filter(joined['sum_flagvoz'] >= 4).show()
            joined.groupBy(['partitioned_month']).count().show()

        # flags_fixed = [  'sum_flagadsl', 'sum_flaghz', 'sum_flagftth', 'sum_flaglpd', 'sum_flagtivo', 'sum_flagvfbox', 'sum_flagfutbol'
        #                , 'sum_flagmotor'
        #               ]

        # mo_condition = (joined['sum_flagvoz'] > 0)
        # for flag in flags_fixed:
        #     if flag in joined.columns:
        #         #print 'Adding fixed flag', flag, 'to mo_condition'
        #         mo_condition = mo_condition & (joined[flag] == 0)
        #     else:
        #         print 'Flag', flag, 'not in dataframe'
        # joined = joined.withColumn('is_mobile_only_new', when(mo_condition, True).otherwise(False))

        joined = joined.withColumn('is_mobile_only', when(  (joined['sum_flagvoz']     > 0)
                                                          & (joined['sum_flagadsl']   == 0)
                                                          & (joined['sum_flaghz']     == 0)
                                                          & (joined['sum_flagftth']   == 0)
                                                          & (joined['sum_flaglpd']    == 0)
                                                          & (joined['sum_flagtivo']   == 0)
                                                          & (joined['sum_flagvfbox']  == 0)
                                                          & (joined['sum_flagfutbol'] == 0)
                                                          #& (joined['sum_flagmotor']  == 0)
                                                          , True).otherwise(False))
        # joined.groupby('is_mobile_only', 'is_mobile_only_new').count().show()

        # co_condition = None
        # for flag in flags_fixed:
        #     if flag in joined.columns:
        #         #print 'Adding flag', flag, 'to co_condition'
        #         if co_condition is None:
        #             co_condition = (joined[flag] > 0)
        #         else:
        #             co_condition = co_condition | (joined[flag] > 0)
        #     else:
        #         print 'Flag', flag, 'not in dataframe'
        # co_condition = (joined['sum_flagvoz'] > 0) & co_condition
        # joined = joined.withColumn('is_convergent_new', when(co_condition, True).otherwise(False))

        joined = joined.withColumn('is_convergent', when(     (joined['sum_flagvoz']    > 0)
                                                         & (  (joined['sum_flagadsl']   > 0)
                                                            | (joined['sum_flaghz']     > 0)
                                                            | (joined['sum_flagftth']   > 0)
                                                            | (joined['sum_flaglpd']    > 0)
                                                            | (joined['sum_flagtivo']   > 0)
                                                            | (joined['sum_flagvfbox']  > 0)
                                                            | (joined['sum_flagfutbol'] > 0)
                                                            #| (joined['sum_flagmotor']  > 0)
                                                           ), True).otherwise(False))
        # joined.groupby('is_convergent', 'is_convergent_new').count().show()
        
        if self.debug:
            joined.show()
            # joined.filter('sum_flagvoz >= 4').show() # WTF: For some reason this sentence no longer works!!!
            joined.filter(joined['sum_flagvoz'] >= 4).show()
            # print joined.count()

        self.dataById = joined

        if nifs_comp is not None:
            self.set_convergent_ids(nifs_comp)

        if self.debug:
            self.dataById.groupBy(['partitioned_month']).count().show()

        # train.select('Age','Gender').dropDuplicates().show()

        # Aggregate fingerprint flags and other categorical fields

        # agg_fingerprints = self.data.select(*cols).groupBy('x_num_ident', 'partitioned_month').agg({"*": "max", "*": "sum"})
        # agg_fingerprints = self.data.select(*cols).groupBy('x_num_ident', 'partitioned_month').agg({"*": "max"})
        print '1.1:', filter(lambda x: x in ['max_partitioned_month', 'max_year', 'max_month', 'max_day'], self.dataById.columns)
        self.dataById = self.dataById.drop('seg_cliente')\
                                     .join(self.data.select(*tuple(['x_num_ident', 'partitioned_month', 'year', 'month', 'day']) + self.max_fields)
                                                    .groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day').max(),
                                           ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'])
        self.dataById = self.fix_column_names(self.dataById)
        print '1.2:', filter(lambda x: x in ['max_partitioned_month', 'max_year', 'max_month', 'max_day'], self.dataById.columns)
        for c in ['max_partitioned_month', 'max_year', 'max_month', 'max_day']:
            self.dataById = self.dataById.drop(c)
        if self.debug:
            self.dataById.filter(self.dataById['sum_flagvoz'] >= 4).show()
        # sys.exit()

        if self.debug:
            # self.dataById.where('x_num_ident = \'71993020F\' or x_num_ident = \'72072005X\'').show()
            self.data\
                .where('x_num_ident = \'25944813P\' or x_num_ident = \'25974835S\' or x_num_ident = \'71993020F\'')\
                .show()
            self.dataById.groupBy(['partitioned_month']).count().show()
            
        # Aggregate dates

        days_since_cols = [item for item in self.data.columns if item.startswith('days_since_')]
        years_since_cols = [item for item in self.data.columns if item.startswith('years_since_')]
        dates_to_agg_cols = ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'] + days_since_cols + ['x_dia_emision'] + years_since_cols
        print '2.1:', filter(lambda x: x in ['max_partitioned_month', 'max_year', 'max_month', 'max_day'], self.dataById.columns)
        self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
                                           .groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day').min(), ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'])
        self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
                                           .groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day').avg(), ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'])
        self.dataById = self.dataById.join(self.data.select(*dates_to_agg_cols)
                                           .groupBy('x_num_ident', 'partitioned_month', 'year', 'month', 'day').max(), ['x_num_ident', 'partitioned_month', 'year', 'month', 'day'])
        self.dataById = self.fix_column_names(self.dataById)
        print '2.2:', filter(lambda x: x in ['max_partitioned_month', 'max_year', 'max_month', 'max_day'], self.dataById.columns)
        for c in ['max_partitioned_month', 'max_year', 'max_month', 'max_day']:
            self.dataById = self.dataById.drop(c)
		#self.dataById.printSchema()

        if self.debug:
            # data.select(*['x_num_ident','partitioned_month']+days_since_cols)
            # .groupBy('x_num_ident','partitioned_month').agg(stddev(data.DAYS_SINCE_x_fecha_nacimiento)).show()
            self.dataById.show()
            self.dataById.filter(self.dataById['sum_flagvoz'] >= 4).show()
            # self.dataById.where('x_num_ident = \'72072005X\'').show()
            print self.dataById.count()
            print self.dataById.columns

        # self.data.groupBy('x_num_ident', 'partitioned_month').count().where('count >= 5').show()
        # cols = ['x_num_ident', 'partitioned_month', 'COD_SEGFID', 'DESC_SEGFID', 'x_dia_emision']
        # self.data.where('x_num_ident = \'72072005X\'').select(cols).show()
        # self.data.where('x_num_ident = \'71993020F\'').select(*service_fields+numeric_fields).show()
        # self.data.where('x_num_ident = \'51057935M\'').select(['x_num_ident','partitioned_month']+service_fields+integer_fields).show()
        # print [self.data.groupBy('x_num_ident', 'partitioned_month').count().count(),
        # cvm_sums.count(), cvm_maxs.count(), cvm_dias_max.count(), cvm_dias_min.count(), cvm_dias_avg.count()]

        self.dataById = self.fix_column_names(self.dataById).withColumnRenamed('x_num_ident', 'nif')
		
        if self.debug:
            self.dataById.groupBy(['partitioned_month']).count().show()

        print '['+time.ctime()+']', 'Finished calculate_mobile_only_and_convergent_by_id()'

    def set_convergent_ids(self, other_shared_ids):
        print '['+time.ctime()+']', 'Starting set_convergent_ids()'

        other = other_shared_ids.to_df()
        # nifs.count() # 308569

        other = other.select('nif', 'partitioned_month').distinct()\
                     .withColumnRenamed('nif', 'x_num_ident')\
					 .withColumn('is_in_nifs_compartidos', lit(True))
        # nifs.show()

        joined = self.dataById.join(other, ['x_num_ident', 'partitioned_month'], 'left_outer')
        # joined.count() # 470396
        # joined.select(*['nif', 'partitioned_month', 'IS_MOBILE_ONLY', 'IS_CONVERGENT', 'IS_IN_NIFS_COMPARTIDOS'])
        # .filter('IS_MOBILE_ONLY = True AND IS_IN_NIFS_COMPARTIDOS = True').count() # 28506
        # joined.select(*['nif', 'partitioned_month', 'IS_MOBILE_ONLY', 'IS_CONVERGENT', 'IS_IN_NIFS_COMPARTIDOS'])
        # .filter('IS_MOBILE_ONLY = True AND IS_IN_NIFS_COMPARTIDOS = True').show()

        mo_count = joined.where('is_mobile_only = True AND is_in_nifs_compartidos = True').count()
        print '['+time.ctime()+']', 'Making', mo_count, 'mobile-only clients convergent'
        joined = joined.withColumn('is_mobile_only',
                                   when((joined['is_mobile_only'] == True) & (joined['is_in_nifs_compartidos'] == True),
                                        False).otherwise(joined['is_mobile_only']))

        nc_count = joined.where('is_convergent = False AND is_in_nifs_compartidos = True AND sum_flagvoz > 0').count()
        print '['+time.ctime()+']', 'Making', nc_count, 'non-convergent clients convergent'
        joined = joined.withColumn('is_convergent',
                                   when((joined['is_convergent'] == False) & (joined['is_in_nifs_compartidos'] == True)
                                        & (joined['sum_flagvoz'] > 0),
                                        True).otherwise(joined['is_convergent']))

        # joined.count() # 470396
        # joined.select(*['nif', 'partitioned_month', 'IS_MOBILE_ONLY', 'IS_CONVERGENT', 'IS_IN_NIFS_COMPARTIDOS'])
        # .filter('IS_MOBILE_ONLY = True AND IS_IN_NIFS_COMPARTIDOS = True').show()
        joined = joined.drop('is_in_nifs_compartidos')

        joined = self.fix_column_names(joined)

        # self.spark.stop()

        self.dataById = joined
		
        print '['+time.ctime()+']', 'Finished set_convergent_ids()'

    def get_mobile_only(self):
        mobile_only = self.dataById.where(self.dataById['is_mobile_only'] == True)

        return mobile_only

    def get_convergent(self):
        convergent = self.dataById.where(self.dataById['is_convergent'] == True)

        return convergent

    def get_not_convergent(self):
        not_convergent = self.dataById.where(self.dataById['is_convergent'] == False)

        return not_convergent

    def get_converged_and_not_converged(self, other_cvm_postpaid):
        # other = other_cvm_postpaid.to_df()

        this_mo = self.get_mobile_only()

        # Converged = this month are Mobile-Only and in a future month are Convergent
        other_co = other_cvm_postpaid.get_convergent().select('nif')
        converged = this_mo.join(other_co, 'nif')
        # converged.count() #

        # Not-Converged = this month are Mobile-Only and in a future month are Mobile-Only,
        # so those who did churn are left out
        other_nco = other_cvm_postpaid.get_mobile_only().select('nif')
        not_converged = (this_mo.join(other_nco, 'nif')
                               #.where('max_flag_huella_ono == 1 OR max_flag_huella_vf == 1 OR max_flag_huella_neba == 1')
                                .where('max_flag_huella_ono == 1 OR max_flag_huella_vf == 1 OR max_flag_huella_neba == 1 OR flag_cobertura_adsl == "D"')
                               #.where('max_flag_huella_ono == 1 OR max_flag_huella_vf == 1 OR max_flag_huella_neba == 1 OR flag_cobertura_adsl == "D" OR flag_huella_ariel == 1')
                        )

        return converged, not_converged

if __name__ == "__main__":
    # PYTHONIOENCODING=utf-8 ~/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 ~/IdeaProjects/convergence/src/main/python/DP_prepare_input_cvm_pospago.py 201704 201705
    # spark-submit --executor-memory 8G ~/fy17.capsule/convergence/src/main/python/DP_prepare_input_cvm_pospago.py 201704 201705
    # spark2-submit $SPARK_COMMON_OPTS --executor-memory 50G --driver-memory 50G --conf spark.driver.maxResultSize=50G --conf spark.yarn.executor.memoryOverhead=4G --conf spark.kryoserializer.buffer.max=2047m /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/DP_prepare_input_cvm_pospago.py 201807 201808 2>&1 | tee salida.cvm_pospago
    # tail -f -n +1 salida.cvm_pospago | grep -v $'..\/..\/.. ..:..:..\|^[\t]\+at\|java.io.IOException'
    parser = argparse.ArgumentParser(description='VF_ES CVM vf_pos_ac_final',
                                     epilog='Please report bugs and issues to Borja Bergua <borja.bergua@vodafone.com>')
    parser.add_argument('ini_month', metavar='<ini-month>', type=str, help='Date (YYYYMM) of the initial month')
    parser.add_argument('end_month', metavar='<end-month>', type=str, help='Date (YYYYMM) of the end month')
    parser.add_argument('-d', '--debug', action='store_true', help='show debug messages')
    args = parser.parse_args()
    print 'args =', args
    print 'ini_month =', args.ini_month
    print 'end_month =', args.end_month

    conf = Configuration()

    print '['+time.ctime()+']', 'Process starting'

    #fs = conf.spark._jvm.org.apache.hadoop.fs.FileSystem.get(conf.spark._jsc.hadoopConfiguration())

    obj = DPPrepareInputCvmPospago(conf, args.ini_month)

    # obj = DPPrepareInputCvmPospago(conf, None)

    # if conf.is_bdp:
    #     obj.to_df().write\
    #         .mode('overwrite')\
    #         .partitionBy('partitioned_month', 'year', 'month', 'day')\
    #         .format('parquet')\
    #         .save('/tmp/bbergua/convergence_data/oracle_clean')
    # else:
    #     obj.to_df().write.mode('overwrite').format('com.databricks.spark.csv').options(header='true') \
    #         .save('file:///Users/bbergua/vf_pos_ac_final_clean-' + args.ini_month)

    # print 'vf_pos_ac_final cleaned!'

    # conf.spark.stop()
    # sys.exit()

    #########
    # By Id #
    #########

    obj.calculate_mobile_only_and_convergent_by_id()

    mo = obj.get_mobile_only()
    # if conf.is_bdp:
    #     ofile = '/tmp/bbergua/convergence_data/oracle_mobile_only-' + args.ini_month
    #     #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
    #     print '['+time.ctime()+']', 'Saving  ', ofile, '...'
    #     mo.repartition(1).write.save(ofile
    #                                  , format='parquet'
    #                                  , mode='overwrite'
    #                                  # , partitionBy=['partitioned_month', 'year', 'month', 'day']
    #                                  )
    #     #else:
    #     #    print 'Skipping', ofile
    # else:
    #     mo.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
    #       .save('file:///Users/bbergua/oracle_mobile-only-' + args.ini_month)

    convnt = obj.get_convergent()
    # if conf.is_bdp:
    #     ofile = '/tmp/bbergua/convergence_data/oracle_convergent-' + args.ini_month
    #     #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
    #     print '['+time.ctime()+']', 'Saving  ', ofile, '...'
    #     convnt.repartition(1).write.save(ofile
    #                                      , format='parquet'
    #                                      , mode='overwrite'
    #                                      # , partitionBy=['partitioned_month', 'year', 'month', 'day']
    #                                      )
    #     #else:
    #     #    print 'Skipping', ofile
    # else:
    #     convnt.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
    #           .save('file:///Users/bbergua/oracle_convergent-' + args.ini_month)

    obj2 = DPPrepareInputCvmPospago(conf, args.end_month)

    obj2.calculate_mobile_only_and_convergent_by_id()

    mo2 = obj2.get_mobile_only()
    if conf.is_bdp:
        ofile = '/tmp/bbergua/convergence_data/oracle_mobile_only-' + args.end_month
        #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
        print '['+time.ctime()+']', 'Saving  ', ofile, '...'
        mo2.repartition(1).write.save(ofile
                                      , format='parquet'
                                      , mode='overwrite'
                                      #, partitionBy=['partitioned_month', 'year', 'month', 'day']
                                      )
        #else:
        #    print 'Skipping', ofile
    else:
        mo2.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
           .save('file:///Users/bbergua/oracle_mobile-only-' + args.end_month)

    convnt2 = obj2.get_convergent()
    # if conf.is_bdp:
    #     ofile = '/tmp/bbergua/convergence_data/oracle_convergent-' + args.end_month
    #     #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
    #     print '['+time.ctime()+']', 'Saving  ', ofile, '...'
    #     convnt2.repartition(1).write.save(ofile
    #                                       , format='parquet'
    #                                       , mode='overwrite'
    #                                       #, partitionBy=['partitioned_month', 'year', 'month', 'day']
    #                                       )
    #     #else:
    #     #    print 'Skipping', ofile
    # else:
    #     convnt2.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
    #            .save('file:///Users/bbergua/oracle_convergent-' + args.end_month)

    convd, not_convd = obj.get_converged_and_not_converged(obj2)
    if conf.is_bdp:
        ofile = '/tmp/bbergua/convergence_data/oracle_converged-' + args.ini_month #+ '_' + args.end_month
        #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
        print '['+time.ctime()+']', 'Saving  ', ofile, '...'
        convd.repartition(1).write.save(ofile
                                        , format='parquet'
                                        , mode='overwrite'
                                        # , partitionBy=['partitioned_month', 'year', 'month', 'day']
                                        )
        #else:
        #    print 'Skipping', ofile

        ofile = '/tmp/bbergua/convergence_data/oracle_not_converged-' + args.ini_month #+ '_' + args.end_month
        #if not fs.exists(conf.spark._jvm.org.apache.hadoop.fs.Path(ofile+'/_SUCCESS')):
        print '['+time.ctime()+']', 'Saving  ', ofile, '...'
        not_convd.repartition(1).write.save(ofile
                                            , format='parquet'
                                            , mode='overwrite'
                                            # , partitionBy=['partitioned_month', 'year', 'month', 'day']
                                            )
        #else:
        #    print 'Skipping', ofile
    else:
        convd.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
             .save('file:///Users/bbergua/oracle_converged-' + args.ini_month)
        not_convd.write.mode('overwrite').format('com.databricks.spark.csv').options(header='true')\
             .save('file:///Users/bbergua/oracle_not-converged-' + args.ini_month)

    print '['+time.ctime()+']', 'Process finished!'

    # cvm_pospago_df = obj.to_df()

    # Testing in R
    #
    # library(data.table)
    #
    # load("fy17/datasets/201704/dt.MobileOnly-NIFS-201704.RData")
    # r <- dt.MobileOnly.nifs
    # Encoding(r$x_num_ident) <- "latin1"
    #
    # p <- fread("mobile-only-201704.csv")
    # p <- p[x_num_ident != "x_num_ident"]
    #
    # suppressWarnings(p[! x_num_ident %in% r$x_num_ident, .(x_num_ident)])
    # suppressWarnings(r[! x_num_ident %in% p$x_num_ident, .(x_num_ident)])
