#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark.sql.functions import col, when
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as psf
from pyspark.sql.functions import lit

def set_paths():

    import os
    import sys

    USECASES_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios", "use-cases")
    if USECASES_SRC not in sys.path:
        sys.path.append(USECASES_SRC)

    PYKHAOS_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios")
    if PYKHAOS_SRC not in sys.path:
        sys.path.append(PYKHAOS_SRC)

if __name__ == "__main__":

    set_paths()

    from churn.models.operators.porta_functions_refactor import *

    from churn_nrt.src.utils.spark_session import get_spark_session

    #Spark session

    sc, spark, sql_context=get_spark_session(app_name="Portability_model")

    spark = (spark.builder
             .master("yarn")
             .config("spark.submit.deployMode", "client")
             .config("spark.ui.showConsoleProgress", "true")
             .enableHiveSupport()
             .getOrCreate()
             )

    #Data preparation

    print('[Info]: Getting IDS for training set...')

    df_no_etiquetado_train=get_ids_filtered('20190531',spark) #poner fecha como parámetro del main

    print('[Info]: Getting IDS for test set...')

    test_final=get_ids_filtered('20190531',spark)

    print('[Info]: Training and test set obtained')

    print('[Info]: Labelling training set...')

    portab_final_train = get_portab_next_month('20190531', spark)

    train_final=get_portab_labels(portab_final_train, df_no_etiquetado_train)

    print('[Info]: Operators volume for training set...')

    train_final.groupby('Operador_target').count().show()

    #Get important variables to include in each model (actualizar de vez en cuando)

    print('[Info]: Getting input variables for the models...')

    var_masmovil=['Spinners_total_yoigo',
 'GNV_Type_Voice_MOVILES_YOIGO_Num_Of_Calls',
 'Comp_YOIGO_min_days_since_navigation',
 'GNV_Type_Voice_MOVILES_TELEFONICA_Num_Of_Calls',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_MOU',
 'Cust_Agg_num_tariff_smart',
 'Cust_Agg_L2_tv_fx_first_days_since_nif',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_MOU',
 'GNV_Type_Voice_MOVILES_TELEFONICA_MOU',
 'Cust_Agg_L2_mobile_fx_first_days_since_nif',
 'netscout_ns_apps_web_lowi_http_timestamps',
 'GNV_Type_Voice_MOVILES_YOIGO_MOU',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_MOU',
 'Comp_YOIGO_sum_count',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_Num_Of_Calls',
 'Comp_sum_count_comps',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_ATENCION_AL_CLIENTE_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_MOU',
 'Spinners_movistar_ACON',
 'Cust_age',
 'tgs_days_since_f_inicio_bi',
 'GNV_Type_Voice_MOVILES_R_Num_Of_Calls',
 'Spinners_otros_ACON',
 'Comp_MOVISTAR_min_days_since_navigation',
 'tgs_has_discount',
 'netscout_ns_apps_sports_timestamps',
 'Comp_MASMOVIL_sum_count',
 'Cust_Agg_total_tv_total_charges',
 'Spinners_yoigo_ACAN',
 'Order_L2_avg_time_bw_orders',
 'Ord_sla_disminucion_last_order_last120',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_Num_Of_Calls',
 'Spinners_nif_min_days_since_port',
 'netscout_ns_apps_twitter_total_effective_dl_mb',
 'netscout_ns_apps_sports_days',
 'netscout_ns_apps_web_o2_http_total_effective_ul_mb',
 'netscout_ns_apps_news_timestamps',
 'netscout_ns_apps_whatsapp_voice_calling_days',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_Num_Of_Calls',
 'Spinners_movistar_ACAN',
 'Ord_sla_ord_equipo_last_order_last240',
 'NUM_AVERIAS_NIF_w4vsw4',
 'netscout_ns_apps_web_masmovil_http_total_effective_ul_mb',
 'Spinners_nif_avg_days_since_port',
 'Ord_sla_aumento_first_order_last240',
 'Penal_L2_CUST_PENDING_end_date_total_max_days_until',
 'Ord_sla_aumento_last_order_last365',
 'Spinners_yoigo_APOR',
 'Spinners_total_otros',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_LINEA_900_N1_Num_Of_Calls',
 'msisdn',
 'Operador_target',
 'masmovil']

    var_movistar=['GNV_Type_Voice_MOVILES_TELEFONICA_Num_Of_Calls',
 'GNV_Type_Voice_MOVILES_DIGI_SPAIN_MOU',
 'GNV_Type_Voice_MOVILES_ORANGE_Num_Of_Calls',
 'Cust_Agg_L2_tv_fx_first_days_since_nif',
 'GNV_Type_Voice_MOVILES_TELEFONICA_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_MOU',
 'Comp_MOVISTAR_min_days_since_navigation',
 'Cust_age',
 'Cust_Agg_mobile_services_nc',
 'GNV_Type_Voice_MOVILES_YOIGO_Num_Of_Calls',
 'Spinners_nif_port_freq_per_day',
 'tgs_has_discount',
 'Spinners_total_orange',
 'netscout_ns_apps_web_lowi_http_data_mb',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_Num_Of_Calls',
 'Spinners_total_movistar',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_MOU',
 'GNV_Type_Voice_MOVILES_KPN_SPAIN_Num_Of_Calls',
 'Spinners_nif_min_days_since_port',
 'netscout_ns_apps_web_o2_http_total_effective_ul_mb',
 'Spinners_total_yoigo',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_JAZZTEL_Num_Of_Calls',
 'netscout_ns_apps_itunes_days',
 'Cust_Agg_total_tv_total_charges',
 'Ord_sla_aumento_nb_running_last60_gt10',
 'Spinners_movistar_ACAN',
 'Cust_Agg_L2_mobile_fx_first_days_since_nc',
 'netscout_ns_apps_apple_total_effective_ul_mb',
 'Spinners_nif_max_days_since_port',
 'GNV_Type_Voice_MOVILES_YOIGO_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_Num_Of_Calls',
 'Cust_Agg_L2_fixed_fx_first_days_since_nif',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_ATENCION_AL_CLIENTE_MOU',
 'tgs_sum_ind_under_use',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_MOU',
 'device_num_devices',
 'netscout_ns_apps_itunes_timestamps',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_MOU',
 'Spinners_otros_ACON',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_Num_Of_Calls',
 'Penal_L2_SRV_PENDING_N2_end_date_days_until',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_Num_Of_Calls',
 'Spinners_yoigo_ACON',
 'GNV_Type_Voice_MOVILES_DIGI_SPAIN_Num_Of_Calls',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_MOU',
 'Spinners_num_distinct_operators',
 'netscout_ns_apps_web_movistar_http_total_effective_ul_mb',
 'Penal_L2_SRV_PENDING_end_date_total_max_days_until',
 'netscout_ns_apps_sports_total_effective_ul_mb',
 'netscout_ns_apps_web_movistar_http_total_effective_dl_mb',
 'msisdn',
 'Operador_target',
 'movistar']

    var_orange=['GNV_Type_Voice_MOVILES_ORANGE_Num_Of_Calls',
 'Comp_JAZZTEL_min_days_since_navigation',
 'GNV_Type_Voice_MOVILES_YOIGO_Num_Of_Calls',
 'Spinners_total_movistar',
 'GNV_Type_Voice_MOVILES_TELEFONICA_Num_Of_Calls',
 'netscout_ns_apps_web_lowi_http_total_effective_ul_mb',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_AMENA_MOU',
 'Cust_Agg_mobile_services_nc',
 'GNV_Type_Voice_MOVILES_KPN_SPAIN_Num_Of_Calls',
 'Cust_age',
 'netscout_ns_apps_web_orange_http_total_effective_ul_mb',
 'Comp_ORANGE_sum_count',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_TFNO_INFORMACION_GRATUITO_MOU',
 'netscout_ns_apps_whatsapp_voice_calling_days',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_JAZZTEL_MOU',
 'GNV_Type_Voice_MOVILES_JAZZTEL_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_Num_Of_Calls',
 'Spinners_total_yoigo',
 'netscout_ns_apps_whatsapp_voice_calling_timestamps',
 'netscout_ns_apps_itunes_days',
 'Spinners_total_simyo',
 'Comp_ORANGE_min_days_since_navigation',
 'Spinners_orange_ACON',
 'netscout_ns_apps_web_movistar_http_total_effective_dl_mb',
 'Spinners_total_jazztel',
 'Spinners_total_pepephone',
 'Cust_Agg_L2_fixed_fx_first_days_since_nif',
 'Cust_Agg_L2_fbb_fx_first_days_since_nif',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_Num_Of_Calls',
 'GNV_Type_Voice_MOVILES_R_MOU',
 'GNV_Type_Voice_MOVILES_JAZZTEL_Num_Of_Calls',
 'netscout_ns_apps_amazon_data_mb',
 'netscout_ns_apps_twitter_total_effective_ul_mb',
 'Comp_MOVISTAR_min_days_since_navigation',
 'Cust_Agg_num_football_nif',
 'Penal_L2_SRV_PENDING_N1_end_date_days_until',
 'Ord_sla_ord_esp_last_order_last90',
 'Spinners_total_orange',
 'Spinners_orange_ACAN',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_MOU',
 'tgs_days_until_f_fin_bi',
 'Spinners_movistar_ACON',
 'GNV_Type_Voice_MOVILES_LYCAMOBILE_MOU',
 'Cust_Agg_L2_fbb_fx_first_days_since_nc',
 'Penal_L2_SRV_PENDING_N1_penal_amount_total',
 'Spinners_otros_ACON',
 'netscout_ns_apps_web_masmovil_http_data_mb',
 'Ord_sla_ord_admin_first_order_last90',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_MOU',
 'msisdn',
 'Operador_target',
 'orange']

    var_otros=['Spinners_total_pepephone',
 'GNV_Type_Voice_MOVILES_TELEFONICA_Num_Of_Calls',
 'GNV_Type_Voice_MOVILES_DIGI_SPAIN_Num_Of_Calls',
 'Cust_age',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_ATENCION_AL_CLIENTE_MOU',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_INFOOFCOMTEL_MOU',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_Num_Of_Calls',
 'Spinners_otros_ACON',
 'Cust_Agg_tv_services_nif',
 'Ord_sla_nb_started_orders_disminucion_last14',
 'Cust_Agg_L2_fixed_fx_first_days_since_nif',
 'Spinners_total_otros',
 'Spinners_total_movistar',
 'netscout_ns_apps_hacking_total_effective_ul_mb',
 'GNV_Type_Voice_MOVILES_VODAFONE_ENABLER_MOU',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_Num_Of_Calls',
 'Comp_PEPEPHONE_sum_count',
 'Spinners_nif_min_days_since_port',
 'GNV_Type_Voice_MOVILES_DIGI_SPAIN_MOU',
 'GNV_Voice_L2_max_num_calls_W',
 'GNV_Type_Voice_MOVILES_VODAFONE_Num_Of_Calls',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_SERVICIO_DE_INF_Y_VENTAS_MOU',
 'Spinners_yoigo_ACAN',
 'Cust_Agg_L2_mobile_fx_first_days_since_nc',
 'GNV_Type_Voice_MOVILES_R_Num_Of_Calls',
 'Ord_sla_nb_forbidden_orders_last14',
 'tgs_days_until_f_fin_bi',
 'GNV_Type_Voice_MOVILES_EUSKALTEL_MOU',
 'Order_L2_N6_StartDate_days_since',
 'GNV_Type_Voice_MOVILES_R_MOU',
 'Cust_Agg_total_tv_total_charges',
 'Spinners_total_acan',
 'netscout_ns_apps_samsung_timestamps',
 'Cust_Agg_L2_tv_fx_first_days_since_nif',
 'GNV_Type_Voice_MOVILES_INGENIUM_MOU',
 'Cust_Agg_num_football_nif',
 'GNV_Type_Voice_MOVILES_ORANGE_MOU',
 'netscout_ns_apps_linkedin_total_effective_dl_mb',
 'Cust_Agg_L2_fixed_fx_first_days_since_nc',
 'Spinners_nif_port_number',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_AMENA_MOU',
 'tgs_sum_ind_under_use',
 'GNV_Type_Voice_MOVILES_LYCAMOBILE_Num_Of_Calls',
 'Cust_Agg_L2_tv_fx_first_days_since_nc',
 'netscout_ns_apps_web_pepephone_http_total_effective_dl_mb',
 'Comp_sum_distinct_days_with_navigation_comps',
 'netscout_ns_apps_classified_data_mb',
 'Ord_sla_first_order_last120',
 'GNV_Type_Voice_SERVICIOS_GRATUITOS_JAZZTEL_MOU',
 'netscout_ns_apps_web_jazztel_http_total_effective_ul_mb',
 'msisdn',
 'Operador_target',
 'otros']


    #Select variables for each model set

    train_final_masmovil = train_final.select([c for c in train_final.columns if c in var_masmovil])
    train_final_movistar = train_final.select([c for c in train_final.columns if c in var_movistar])
    train_final_orange = train_final.select([c for c in train_final.columns if c in var_orange])
    train_final_otros = train_final.select([c for c in train_final.columns if c in var_otros])

    #for test set we need to exclude target variables ('Operador_target','masmovil',etc.)
    test_final_masmovil = test_final.select([c for c in train_final.columns if c in var_masmovil[0:len(var_masmovil) - 3]])
    test_final_movistar = test_final.select([c for c in train_final.columns if c in var_movistar[0:len(var_movistar) - 3]])
    test_final_orange = test_final.select([c for c in train_final.columns if c in var_orange[0:len(var_orange) - 3]])
    test_final_otros = test_final.select([c for c in train_final.columns if c in var_otros[0:len(var_otros) - 3]])

    print('[Info]: Variables selected for training and test set')

    #Assembler, train-validation and balancing (for each operator)

    variables_elim = ['msisdn', 'Operador_target', 'masmovil', 'movistar', 'orange', 'otros']

    print('[Info]: Assembling and splitting data for masmovil model...')

    train_unbal_masmovil, train_masmovil, validation_masmovil, test_masmovil = preparation_production('masmovil', train_final_masmovil,
                                                                                           test_final_masmovil, variables_elim)

    print('[Info]: Fitting Masmovil model...')

    model = GBTClassifier(featuresCol = 'features',labelCol='label', maxDepth=5,maxIter=20)

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    model_masmovil = model.fit(train_masmovil)

    print('[Info]: Getting predictions for masmovil...')


    preds_masmovil_train, preds_masmovil_test, preds_masmovil_validation=get_predictions(model_masmovil,
                                                                            train_masmovil, test_masmovil, validation_masmovil, getScore,'masmovil')

    print('[Info]: Getting AUC...')

    evaluator = BinaryClassificationEvaluator(labelCol='masmovil', metricName='areaUnderROC')

    auc_masmovil_train = evaluator.evaluate(preds_masmovil_train)
    print('AUC masmovil train', auc_masmovil_train)
    auc_masmovil_validation = evaluator.evaluate(preds_masmovil_validation)
    print('AUC masmovil validation' , auc_masmovil_validation)




    print('[Info]: Assembling and splitting data for movistar model...')

    train_unbal_movistar, train_movistar, validation_movistar, test_movistar = preparation('movistar',
                                                                                           train_final_movistar,
                                                                                           test_final_movistar,
                                                                                           variables_elim)


    print('[Info]: Fitting Movistar model...')

    model_movistar = model.fit(train_movistar)

    print('[Info]: Getting predictions for movistar ...')

    preds_movistar_train, preds_movistar_test, preds_movistar_validation = get_predictions(model_movistar,
                                                                                           train_movistar,
                                                                                           test_movistar,
                                                                                           validation_movistar,
                                                                                           getScore,'movistar')

    evaluator = BinaryClassificationEvaluator(labelCol='movistar', metricName='areaUnderROC')
    print('[Info]: Getting AUC...')
    auc_movistar_train = evaluator.evaluate(preds_movistar_train)
    print('AUC movistar train' , auc_movistar_train)
    auc_movistar_validation = evaluator.evaluate(preds_movistar_validation)
    print('AUC movistar validation' , auc_movistar_validation)



    print('[Info]: Assembling and splitting data for orange model...')

    train_unbal_orange, train_orange, validation_orange, test_orange = preparation('orange',
                                                                                           train_final_orange,
                                                                                           test_final_orange,
                                                                                           variables_elim)
    print('[Info]: Fitting orange model...')

    model_orange = model.fit(train_orange)

    print('[Info]: Getting predictions for orange ...')

    preds_orange_train, preds_orange_test, preds_orange_validation = get_predictions(model_orange,
                                  train_orange,
                                  test_orange,
                                  validation_orange,
                                  getScore,'orange')

    print('[Info]: Getting AUC...')

    evaluator = BinaryClassificationEvaluator(labelCol='orange', metricName='areaUnderROC')
    auc_orange_train = evaluator.evaluate(preds_orange_train)
    print('AUC orange train' , auc_movistar_train)
    auc_orange_validation = evaluator.evaluate(preds_orange_validation)
    print('AUC orange validation' , auc_movistar_validation)




    print('[Info]: Assembling and splitting data for otros model...')

    train_unbal_otros, train_otros, validation_otros, test_otros = preparation('otros',
                                                                                   train_final_otros,
                                                                                   test_final_otros,
                                                                                   variables_elim)
    print('[Info]: Fitting otros model...')

    model_otros = model.fit(train_otros)

    print('[Info]: Getting predictions for otros...')

    preds_otros_train, preds_otros_test, preds_otros_validation = get_predictions(model_otros,
                                                                                     train_otros,
                                                                                     test_otros,
                                                                                     validation_otros,
                                                                                     getScore,'otros')


    evaluator = BinaryClassificationEvaluator(labelCol='otros', metricName='areaUnderROC')
    print('[Info]: Getting AUC...')

    auc_otros_train = evaluator.evaluate(preds_otros_train)
    print( 'AUC otros train' , auc_otros_train)
    auc_otros_validation = evaluator.evaluate(preds_otros_validation)
    print( 'AUC otros validation' , auc_otros_validation)

    preds_masmovil_test = preds_masmovil_test.select('msisdn', 'score', 'prediction')
    preds_movistar_test = preds_movistar_test.select('msisdn', 'score', 'prediction')
    preds_orange_test = preds_orange_test.select('msisdn', 'score', 'prediction')
    preds_otros_test=preds_otros_test.select('msisdn','score','prediction')

    #Calibration

    masmovil=get_calibration_function(spark, preds_masmovil_validation, 'masmovil', numpoints = 10)
    movistar=get_calibration_function(spark, preds_movistar_validation, 'movistar', numpoints = 10)
    orange=get_calibration_function(spark, preds_orange_validation, 'orange', numpoints = 10)
    otros=get_calibration_function(spark, preds_otros_validation, 'otros', numpoints = 10)

    print('[Info]: Calibration function for each operator fitted')

    pred_masmovil_test_calib=masmovil[0].transform(preds_masmovil_test).withColumnRenamed('msisdn','msisdn_MasMovil').withColumnRenamed('calib_model_score','calib_score_masmovil')
    pred_movistar_test_calib=movistar[0].transform(preds_movistar_test).withColumnRenamed('msisdn','msisdn_Movistar').withColumnRenamed('calib_model_score','calib_score_movistar')
    pred_orange_test_calib=orange[0].transform(preds_orange_test).withColumnRenamed('msisdn','msisdn_Orange').withColumnRenamed('calib_model_score','calib_score_orange')
    pred_otros_test_calib=otros[0].transform(preds_otros_test).withColumnRenamed('msisdn','msisdn_Otros').withColumnRenamed('calib_model_score','calib_score_otros')

    print('[Info]: Scores for each operator calibrated')


    #Join predictions and standardize calibrated probabilities

    predicciones_union=pred_masmovil_test_calib.join(pred_movistar_test_calib,
                                                on=(pred_masmovil_test_calib['msisdn_MasMovil']==pred_movistar_test_calib['msisdn_Movistar'])
                                                    ,how='inner')

    predicciones_union=predicciones_union.join(pred_orange_test_calib,
                                                on=(predicciones_union['msisdn_MasMovil']==pred_orange_test_calib['msisdn_Orange'])
                                                    ,how='inner')

    predicciones_union=predicciones_union.join(pred_otros_test_calib,
                                                on=(predicciones_union['msisdn_MasMovil']==pred_otros_test_calib['msisdn_Otros'])
                                                    ,how='inner')

    predicciones_union=predicciones_union.select('msisdn_MasMovil','calib_score_masmovil','calib_score_movistar','calib_score_orange','calib_score_otros','masmovil','movistar','orange','otros')

    predicciones_union=predicciones_union.withColumnRenamed('msisdn_MasMovil','msisdn')

    print('[Info]: Joined prediction dataframe created')

    predicciones_union_norm=predicciones_union.withColumn('calib_score_masmovil',col('calib_score_masmovil')/(col('calib_score_masmovil')+
                                                                                                                          col('calib_score_movistar')+
                                                                                                                          col('calib_score_orange')+
                                                                                                                          col('calib_score_otros')))

    predicciones_union_norm=predicciones_union_norm.withColumn('calib_score_movistar',col('calib_score_movistar')/(col('calib_score_masmovil')+
                                                                                                                             col('calib_score_movistar')+
                                                                                                                          col('calib_score_orange')+
                                                                                                                          col('calib_score_otros')))


    predicciones_union_norm=predicciones_union_norm.withColumn('calib_score_orange',col('calib_score_orange')/(col('calib_score_masmovil')+
                                                                                                                             col('calib_score_movistar')+
                                                                                                                          col('calib_score_orange')+
                                                                                                                          col('calib_score_otros')))


    prediccionesUnion=predicciones_union_norm.withColumn('calib_score_otros',col('calib_score_otros')/(col('calib_score_masmovil')+
                                                                                                                             col('calib_score_movistar')+
                                                                                                                          col('calib_score_orange')+
                                                                                                                          col('calib_score_otros')))

    print('[Info]: Joined prediction dataframe normalized')


    #Add max probability for each row

    cond = "psf.when" + ".when".join(["(psf.col('" + c + "') == psf.col('Prob_max'), psf.lit('" + c + "'))" for c in prediccionesUnion.columns[1:5]])

    prediccionesFinal=prediccionesUnion.withColumn("Prob_max", psf.greatest(*prediccionesUnion.columns[1:5])).withColumn("Operador_predicho", eval(cond))


    #Add predicted target in order to compare with real target

    prediccionesFinal = prediccionesFinal.withColumn("Operador_predicho_num",
                    when(prediccionesFinal["Operador_predicho"]=='calib_score_masmovil',1).otherwise(
                        when(prediccionesFinal["Operador_predicho"]=='calib_score_movistar',2).otherwise(
                            when(prediccionesFinal["Operador_predicho"]=='calib_score_orange',3).otherwise(
                                when(prediccionesFinal["Operador_predicho"]=='calib_score_otros',4)))))

    prediccionesFinal=prediccionesFinal.drop('Operador_predicho')

    prediccionesFinal=prediccionesFinal.withColumnRenamed("Operador_predicho_num",'Operador_predicho')


    #add column with label 1 if top 5000, 0 otherwise

    prediccionesFinal = prediccionesFinal.orderBy('Prob_max', ascending=False)

    top=prediccionesFinal.limit(5000)

    top = top.withColumn('Incertidumbre', lit(1)).select('msisdn')

    predicciones_final=prediccionesFinal.select('msisdn','Operador_predicho')

    #join with churn model scores

    predictions_final=predicciones_final.join(top,on='msisdn',how='left')

    predictions_final=predictions_final.fillna(0)

    print('[Info]: Final predictions dataframe created')

    predictions_final.repartition(300).write.save('/data/udf/vf_es/churn/portabPropension_model/predictions_final',
                                              format='parquet', mode='overwrite')

    print('[Info]: Final predictions dataframe saved')

    #predicciones_top=prediccionesFinal.limit(10000).select('msisdn') añadir el flag
