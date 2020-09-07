# -*- coding: utf-8 -*-


from pyspark.sql.functions import (udf,col,max as sql_max, when, isnull, concat, lpad, trim, lit, sum as sql_sum,
                                   length, upper, datediff, to_date, from_unixtime, unix_timestamp)
#from pykhaos.utils.functions_df_columns import (get_customer_age_udf, substitute_crappy_characters_udf)
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

# In this acFinalPrepago DF we have a column (nationality)
# with lot's of different values (high cardinality), which
# is terrible for ML models, so we will get the most frequent
# countries, and replace all others with "Other":
most_frequent_countries = [u"España",
                           u"Marruecos",
                           u"Rumania",
                           u"Colombia",
                           u"Italia",
                           u"Ecuador",
                           u"Alemania",
                           u"Estados Unidos",
                           u"Francia",
                           u"Brasil",
                           u"Argentina",
                           u"Afganistan",
                           u"Bolivia",
                           u"Gran Bretaña",
                           u"Portugal",
                           u"Paraguay",
                           u"China",
                           u"Venezuela",
                           u"Honduras",
                           u"Corea del Sur"]

most_frequent_countries = [cc.upper() for cc in most_frequent_countries]

# TODO cliente vs comprador
useful_columns_from_acFinalPrepago = ["Fecha_ejecucion",
                                      "msisdn",
                                      # "num_documento_cliente", # NIF del cliente propietario del servicio
                                      "nacionalidad",
                                      "num_prepago",
                                      "num_pospago",
                                      "age_in_years",
                                      # "tipo_documento_cliente", very uninformed
                                      "tipo_documento_comprador",
                                      # Tipo de documento (NIF, Pasaporte, Tarj. Residencia…) asociado al comprador del servicio
                                      "x_fecha_nacimiento",
                                      "partitioned_month",
                                      # "fx_activacion",
                                      "fx_1llamada"  # more reliable than fx_activacion
                                      # "fx_alta_postpago"
                                      ]

def get_data(spark, reference_month, num_samples=None):

    logger.info("\t\tGetting prepaid data from month={}. num_samples={}".format(reference_month, num_samples))

    df_prepago = (
        spark.read.table("raw_es.vf_pre_ac_final")  # se calcula a cierre de mes, en concreto el dia 2 del mes sgte
        .where((col("partitioned_month") == int(reference_month)))
            .where(col("X_FECHA_NACIMIENTO") != "X_FE")
            .withColumn("X_FECHA_NACIMIENTO", when(length(col("X_FECHA_NACIMIENTO")) > 0, col("X_FECHA_NACIMIENTO")))
            .withColumn("NUM_DOCUMENTO_CLIENTE",
                    when(length(col("NUM_DOCUMENTO_CLIENTE")) > 0, col("NUM_DOCUMENTO_CLIENTE")))
            .withColumn("NUM_DOCUMENTO_COMPRADOR",
                    when(length(col("NUM_DOCUMENTO_COMPRADOR")) > 0, col("NUM_DOCUMENTO_COMPRADOR")))
            # .withColumn("age_in_years", get_customer_age_udf(col("X_FECHA_NACIMIENTO"), reference_month))
            .withColumn("age_in_years", lit(1))
            #.withColumn("nacionalidad", substitute_crappy_characters_udf(col("nacionalidad")))
        ).select(*useful_columns_from_acFinalPrepago)

    if len(df_prepago.take(1)) == 0:
        logger.warning("\t !!!! No data in PREPAID {}".format(df_prepago))
        return df_prepago

    #################################
    # Reduce the number of rows if debug mode - this will reduce the size of the dataframes due to left joins
    if num_samples and isinstance(num_samples,str) and num_samples!="complete":  # and len(df_prepago.take(1)) > 0:
        print("\t\tTaking a sample of {}".format(num_samples))
        df_prepago = spark.createDataFrame(df_prepago.take(num_samples))

    df_prepago = (df_prepago.withColumn("nacionalidad",
                                       when(upper(col("nacionalidad")).isin(most_frequent_countries),
                                            upper(col("nacionalidad")))
                                       .when(upper(df_prepago['nacionalidad']) == "GRAN BRETANA", "GRAN BRETAÑA")
                                       .when(upper(df_prepago['nacionalidad']) == "UNITED STATES", "ESTADOS UNIDOS")
                                       .otherwise(lit("Other"))))

    df_prepago = (df_prepago.withColumn('tipo_documento_comprador',
                                       when(upper(df_prepago['tipo_documento_comprador']).like('N%I%F%'), 'N.I.F.')
                                       .when(upper(df_prepago['tipo_documento_comprador']).like('D%N%I%'), 'N.I.F.')
                                       .when(upper(df_prepago['tipo_documento_comprador']).like('C%I%F%'), 'C.I.F.')
                                       .when(upper(df_prepago['tipo_documento_comprador']).like('N%I%E%'), 'N.I.E.')
                                       .when(upper(df_prepago['tipo_documento_comprador']).like('TARJ%RESI%'), 'N.I.E.')
                                       .when(upper(df_prepago['tipo_documento_comprador']).like('PAS%'), 'Pasaporte')
                                       .otherwise('UNKNOWN')))

    df_prepago = df_prepago.withColumn("tipo_documento_comprador", upper(col("tipo_documento_comprador")))

    col_ = "fx_1llamada"
    from pykhaos.utils.date_functions import get_last_day_of_month
    closing_day = get_last_day_of_month(reference_month+"01")
    print("Computing days using closing_day {}".format(closing_day))
    df_prepago = (df_prepago.withColumn("days_since_{}".format(col_), when(col(col_).isNotNull(),
                                                                   datediff(
                                                                       from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                                                                       from_unixtime(unix_timestamp(col_, "yyyyMMdd"))
                                                                    ).cast("double")).otherwise(-1)))

    df_prepago = df_prepago.withColumn("age_in_years", when(col("x_fecha_nacimiento") != 1753, lit(int(reference_month[:4])) - col("x_fecha_nacimiento")).otherwise(-1))
    df_prepago = df_prepago.drop(*["Fecha_ejecucion", "fx_1llamada"])


    return df_prepago