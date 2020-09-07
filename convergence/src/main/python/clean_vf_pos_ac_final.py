#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext  #, SQLContext
from pyspark.sql.functions import udf, when  # current_date, datediff, lit, translate, struct, UserDefinedFunction
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, TimestampType  # BooleanType

class CleanVfPosAcFinal:
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
        'x_fecha_nacimiento', #  Is anonimized in the BDP as year
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
        'fecha_transferencia', # This is a Timestamp but with no time
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

    def __init__(self):
        pass

    def count(self, data):
        print 'Removing spurious header in raw_es.vf_pos_ac_final ...'
        data = data.where(data['id_ftth'] != 'ID_FTTH')

        print 'Removing corrupt rows from raw_es.vf_pos_ac_final ...'
        from pyspark.sql.types import BooleanType
        # from functools import partial
        # test_num_digits_raw = udf(lambda v, l: True if (len(v) == l) & (v.isdigit()) else False, BooleanType())
        # # We register previous function as a udf:
        # def test_num_digits(my_column, one_number):
        #     return udf(partial(test_num_digits_raw, a_number=one_number), BooleanType())(my_column)
        # test_num_digits = udf(lambda v: True if (len(v) == 15) & (v.isdigit()) else False, BooleanType())
        # test_cod_postal = udf(lambda v: True if ((len(v) == 4) | (len(v) == 5)) & v.isdigit() else False, BooleanType())

        # data = data.drop('email_cliente')
        c = 'tac_fac'
        test_num_digits = udf(lambda v: True if (len(v) == 15) & (v.isdigit()) else False, BooleanType())
        print data.where(test_num_digits(c) == True).count()  # 274
        data.where(test_num_digits(c) == True).show()
        corrupt_data = data.where(test_num_digits(c) == True)
        data = data.where(test_num_digits(c) == False)
        # data.where(test_num_digits(data[col]) == True).show()
        # data.where(test_num_digits(data[col], 8) == True).show()
        # data = data.withColumn('tmp', test_num_digits(col))
        # data = data.withColumn('tmp', test_num_digits(col, 8))
        # data = data.withColumn(col,   test_num_digits(col, 17))
        # data.where(data['tmp'] == True).show()

        # c = 'terminal_4g'
        # test_num_digits = udf(lambda v: True if v != '0' and v != '1' and v != '' else False, BooleanType())
        # print data.where(test_num_digits(c) == True).count()
        # data.where(test_num_digits(c) == True).show()
        # data = data.where(test_num_digits(c) == False)
        c = 'cod_golden'
        test_num_digits = udf(lambda v: True if v == '0' or v == '1' else False, BooleanType())
        print data.where(test_num_digits(c) == True).count()  # 95
        data.where(test_num_digits(c) == True).show()
        corrupt_data = corrupt_data.unionAll(data.where(test_num_digits(c) == True))
        data = data.where(test_num_digits(c) == False)
        c = 'cobertura_4g_plus'
        test_num_digits = udf(lambda v: True if v == '0' or v == '1' else False, BooleanType())
        print data.where(test_num_digits(c) == False).count()  # 6
        data.where(test_num_digits(c) == False).show()
        corrupt_data = corrupt_data.unionAll(data.where(test_num_digits(c) == False))
        data = data.where(test_num_digits(c) == True)

        print 'Corrupt data:'
        corrupt_data.select('partitioned_month').groupBy('partitioned_month').count().sort('partitioned_month').show()

        # return

        # Clean

        data = data.withColumn('x_sexo',
                                         when(data['x_sexo'].startswith('H'), 'Varon')
                                         .when(data['x_sexo'].startswith('V'), 'Varon')
                                         .when(data['x_sexo'].startswith('M'), 'Mujer')
                                         .otherwise(''))

        # Fix nationality
        # TODO
        data = data.withColumn('x_nacionalidad',
                                         when(data['x_nacionalidad'].startswith('Espa'), 'España')
                                         .when(data['x_nacionalidad'].startswith('Antar'), 'Antarctica')
                                         .when(data['x_nacionalidad'].startswith('Gran Breta'), 'Gran Bretaña')
                                         .when(data['x_nacionalidad'].startswith('...'), '')
                                         .otherwise(data['x_nacionalidad']))

        data = data.withColumn('x_subtipo',
                                         when(data['x_subtipo'] == 'sin  IAE', 'sin IAE')
                                         .otherwise(data['x_subtipo']))


        # Column counts
        columns_to_count = data.columns
        columns_to_count.sort()
        column_counts = {}
        cols_to_skip =   [col for col in columns_to_count if 'fecha' in col] \
                       + [col for col in columns_to_count if col.startswith('sfid_')] \
                       + ['x_num_ident', 'x_id_red', 'install_date', 'last_upd', 'arpu', 'cantidad_pendiente',
                          'codigo_postal', 'modelo', 'cod_gescal_17', 'cod_golden', 'email_cliente', 'id_ftth', 'imei',
                          'linea_fija', 'objid_gsm', 'phone', 'puntos', 'sistema_operativo', 'tac_fac', 'x_id_cuenta',
                          'x_sfid_cuenta', 'x_sfid_servicio']
        print 'Cols to skip:', cols_to_skip
        print 'Column distinct counts ...'
        for idx, col in enumerate(columns_to_count):
            column_counts[col] = data.select(col).distinct().count()
            print 'Column', str(idx+1) + '/' + str(len(columns_to_count)), col, '=', column_counts[col]
            if col not in cols_to_skip:
                data.select(col).groupBy(col).count().sort('count', ascending=False).show(500)
            else:
                print 'SKIPPING', col
                data.select(col).show()
        # print 'Column distinct counts:', column_counts, '\n'

    def clean(self, data):
        self.data = data

        #########
        # Clean #
        #########

        # self.data.where(self.data['id_ftth'] == 'ID_FTTH').show()
        self.data = self.data.where(self.data['id_ftth'] != 'ID_FTTH')

        # Flag fields
        other_flag_fields = ['cobertura_4g', 'cobertura_4g_plus', 'deuda', 'flag_4g', 'flag_4g_aperturas',
                             'flag_4g_nodos', 'flag_cuenta_superintegral', 'flag_desc_conv', 'flag_ebilling',
                             'flag_existe_fbb_hogar', 'flag_financia', 'flag_financia_simo', 'flag_huella_euskaltel',
                             'flag_huella_jazztel', 'flag_huella_movistar', 'flag_huella_neba', 'flag_huella_ono',
                             'flag_huella_vf', 'flag_nh_prevista', 'flag_nh_real', 'flag_siebel', 'lortad',
                             'terminal_4g', 'vfsmartphone']
        cols_to_clean = self.service_fields + other_flag_fields
        for col in self.service_fields + other_flag_fields:
            if col in self.data.columns:
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
                self.data = self.data.withColumn(col,
                                                  when((self.data[col] == 0) | (self.data[col] == 1), self.data[col])
                                                 .otherwise(None))
                # self.data = self.data.withColumn(col, self.data[col].cast(IntegerType()))
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        # self.data.select('flagvfbox').groupBy('flagvfbox').count().sort('count', ascending=False).show()

        # Numeric fields
        # TODO: ['puntos']
        # cols_to_clean = cols_to_clean + self.numeric_fields + self.max_fields + ['cobertura_4g_plus']
        for col in self.numeric_fields + self.max_fields: #  ['arpu', 'cantidad_pendiente']
            if col in self.data.columns:
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
                self.data = self.data.withColumn(col, self.data[col].cast(FloatType()))
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        # Date fields
        # TODO
        # cols_to_clean = cols_to_clean +
        for col in self.date_fields:
            if col in self.data.columns:
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
                self.data = self.data.withColumn(col, self.data[col].cast(DateType()))
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        # Categoric fields

        test_isdigit = udf(lambda v: v if v.isdigit() else None, StringType())
        test_num_digits = udf(lambda v, l: v if (len(v) == l) & (v.isdigit()) else None, StringType())
        test_less_equal_num_digits = udf(lambda v, l: v if (len(v) <= l) & v.isdigit() else None, StringType())

        col = 'cod_gescal_17'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col, test_num_digits(col, 17))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'cod_golden'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col, test_isdigit(col))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'cod_segfid'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('19', '29', '39', '49'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'cod_postal'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        test_cod_postal = udf(lambda v: v if ((len(v) == 4) | (len(v) == 5)) & v.isdigit() else None, StringType())
        self.data = self.data.withColumn(col, test_cod_postal(col))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'cuotas_pendientes'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col, test_less_equal_num_digits(col, 3))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'flag_cobertura_adsl'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('D', 'I'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'flag_huella_von'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('J', 'N', 'O', 'V'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'imei'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col, test_num_digits(col, 15))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        # TODO: cols = ['meses_fin_cp_tarifa', 'meses_fin_cp_vf']

        cols = ['num_pospago', 'num_prepago', 'num_total', 'x_dia_emision']
        cols_to_clean = cols_to_clean + cols
        for col in cols:
            if col in self.data.columns:
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
                self.data = self.data.withColumn(col, test_less_equal_num_digits(col, 2))
                self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'part_status'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('AC', 'HL', 'DT'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'prodvfbox'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when((self.data[col].startswith('F')) | (self.data[col].startswith('-')),
                                               self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'seg_cliente'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].betwee('1', '8'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'tac_fac'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col, test_num_digits(col, 8))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'terminalmms'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                          when(self.data[col] == 'Yes', '1')
                                         .when(self.data[col] == 'No', '0')
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'x_sexo'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                          when(self.data[col].startswith('H'), 'Varon')
                                         .when(self.data[col].startswith('V'), 'Varon')
                                         .when(self.data[col].startswith('M'), 'Mujer')
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'x_subtipo'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                          when(self.data[col] == 'sin  IAE', 'sin IAE')
                                         .when(self.data[col].isin('Microempresas con IAE', 'sin IAE', 'Otros'),
                                               self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'x_tipo_cliente'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('Particulares', 'Otros'), self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        col = 'x_tipo_ident'
        cols_to_clean = cols_to_clean + col
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()
        self.data = self.data.withColumn(col,
                                         when(self.data[col].isin('CIF', 'NIF', 'Particulares', 'Otros'),
                                              self.data[col])
                                         .otherwise(None))
        self.data.select(col).groupBy(col).count().sort('count', ascending=False).show()

        fields_dont_need_cleaning = ['day', 'email_cliente', 'modelo', 'month', 'partitioned_month',
                                     'sistema_operativo', 'x_subtipo', 'x_tipo_cliente', 'x_tipo_ident', 'year',
                                     'x_nacionalidad',
                                     'linea_fija', 'phone', 'plandatos', 'ppid_destino', 'pprecios_destino', 'prodadsl',
                                     'prodadslenprov', 'prodftth', 'prodhz', 'prodlpd', 'prodtivo', 'promocion_tarifa',
                                     'promocion_vf', 'tarifa_canje', 'x_plan',
                                     'id_ftth', 'objid_gsm', 'sfid_adsl', 'sfid_adslenprov', 'sfid_cambio_pprecios',
                                     'sfid_canje', 'sfid_ftth', 'sfid_futbol', 'sfid_hz', 'sfid_lpd', 'sfid_simo',
                                     'sfid_tv', 'x_id_cuenta', 'x_id_red', 'x_num_ident', 'x_sfid_cuenta',
                                     'x_sfid_servicio']
        cols_to_clean = cols_to_clean + fields_dont_need_cleaning

        # list(set(self.data.columns) - set(cols_to_clean))
        # list(set(cols_to_clean) - set(self.data.columns))

        ##############
        # Cast types #
        ##############

        # now lets cast the columns that we actually care about to dtypes we want

        self.integer_cols = tuple(set(self.integer_cols) & set(self.data.columns))
        # print self.integer_cols
        for col in self.integer_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(IntegerType()))
        # self.data.printSchema()

        self.float_cols = tuple(set(self.float_cols) & set(self.data.columns))
        for col in self.float_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(FloatType()))

        self.numeric_cols = self.integer_cols + self.float_cols

        self.date_cols = tuple(set(self.date_cols) & set(self.data.columns))
        for col in self.date_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(DateType()))

        self.timestamp_cols = tuple(set(self.timestamp_cols) & set(self.data.columns))
        for col in self.timestamp_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(TimestampType()))

        self.dates_cols = self.date_cols + self.timestamp_cols

        self.boolean_cols = tuple(set(self.boolean_cols) & set(self.data.columns))
        for col in self.boolean_cols:
            # self.data = self.data.withColumn(col, self.data[col].cast(BooleanType()))
            self.data = self.data.withColumn(col, self.data[col].cast(IntegerType()))

        self.string_cols = tuple(set(self.string_cols) & set(self.data.columns))
        # self.long_cat_cols = tuple(set(self.long_cat_cols) & set(self.data.columns))
        # self.id_cols = tuple(set(self.id_cols) & set(self.data.columns))
        for col in self.string_cols:
            self.data = self.data.withColumn(col, self.data[col].cast(StringType()))

        self.categ_fields = tuple(set(self.categ_fields) & set(self.data.columns))
        self.service_fields = tuple(set(self.service_fields) & set(self.data.columns))
        self.numeric_fields = tuple(set(self.numeric_fields) & set(self.data.columns))
        self.max_fields = tuple(set(self.max_fields) & set(self.data.columns))

        print 'Original variables to take: ', self.data.columns
        # self.data.show()

        return self.data

if __name__ == "__main__":
    conf = SparkConf().setAppName('VF_ES_Clean-vf_pos_ac_final')#.setMaster(master)
    # global sc
    sc = SparkContext(conf=conf)

    sqlContext = HiveContext(sc) # SQLContext(self.sc)

    data = sqlContext.table("raw_es.vf_pos_ac_final")

    obj = CleanVfPosAcFinal()
    obj.count(data)
    # data = obj.clean(data)
