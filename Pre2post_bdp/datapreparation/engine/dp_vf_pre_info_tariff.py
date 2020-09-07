from pyspark.sql.functions import (udf,col,max as sql_max, when, isnull, concat, lpad, trim, lit, sum as sql_sum, length, upper)
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
cols_tabla_tarifas_prepago = ['MSISDN',
                              'MOU',
                              'TOTAL_LLAMADAS',
                              'TOTAL_SMS',
                              'MOU_Week',
                              'LLAM_Week',
                              'SMS_Week',
                              'MOU_Weekend',
                              'LLAM_Weekend',
                              'SMS_Weekend',
                              'MOU_VF',
                              'LLAM_VF',
                              'SMS_VF',
                              'MOU_Fijo',
                              'LLAM_Fijo',
                              'SMS_Fijo',
                              'MOU_OOM',
                              'LLAM_OOM',
                              'SMS_OOM',
                              'MOU_Internacional',
                              'LLAM_Internacional',
                              'SMS_Internacional',
                              'ActualVolume',
                              'Num_accesos',
                              'Plan',
                              'Num_Cambio_Planes',
                              'LLAM_COMUNIDAD_SMART',
                              'MOU_COMUNIDAD_SMART',
                              'LLAM_SMS_COMUNIDAD_SMART',
                              'Flag_Uso_Etnica',
                              'cuota_SMART8',
                              'cuota_SMART12',
                              'cuota_SMART16']


plan_categories = ['PPIB7',
                'PPFCL',
                'PPIB8',
                'PPIB9',
                'PPVIS',
                'PPIB4',
                'PPXS8',
                'PPTIN',
                'PPIB1',
                'PPREX',
                'PPREU',
                'PPFCS',
                'PPIB5']



def get_data(spark, reference_month):

    logger.info("\t\tGetting tariffs data from month={}".format(reference_month))


    df_tarifas = (spark.read.table("raw_es.vf_pre_info_tarif")
                  .where((col("year") == int(reference_month[:4]))
                         & (col("month") == int(reference_month[4:]))
                         )
                  .select(*cols_tabla_tarifas_prepago)
                  )

    df_tarifas = df_tarifas.withColumn("Plan",
                                       when(col("Plan").isin(plan_categories),
                                            col("Plan")
                                            ).otherwise(lit("Other"))
                                       )
    return df_tarifas