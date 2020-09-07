package metadata



object Metadata {

  def getIdFeats(): List[String] = {
    List("msisdn",
      "rgu",
      "num_cliente",
      "campo1",
      "campo2",
      "campo3")
  }

  ///// _______ Gnv _______

  // Specific functions
  def getGnvHours(): List[Int] = (0 to 23).by(1).toList

  def getGnvUsageTypes(): List[String] = List("Chat_Zero",
    "Maps_Pass",
    "VideoHD_Pass",
    "Video_Pass",
    "MasMegas",
    "Music_Pass",
    "Social_Pass",
    "RegularData")

  def getGnvPeriods(): List[String] = List("W", "WE")

  // Generic functions
  def getInitialGnvColumns(): List[String] = {

    val gnv_hours = getGnvHours
    val gnv_periods = getGnvPeriods
    val gnv_types = getGnvUsageTypes

    /*

   GNV_hour_0_WE_Chat_Zero_Data_Volume_MB, GNV_hour_0_WE_Maps_Pass_Data_Volume_MB

   gnv_hour_<X>_<Y>_<Z>_data_volume_mb represents data consumption in MBs in the corresponding frame based on:
         - <X>: value between 0 and 23 representing the hour
         - <Y>: w--> week / we--> weekend
         - <Z>:  defines usage type: chatzero/maps_pass/video_pass/video_hd_pass/mas_megas/music_pass/social_pass/regular_data(default)

    */

    /*

    GNV_hour_0_WE_Chat_Zero_Num_Of_Connections, GNV_hour_0_WE_Maps_Pass_Num_Of_Connections

    gnv_hour_<X>_<Y>_<Z>_num_of_connections represents number of data connections in the corresponding frame based on:
        - <X>: value between 0 and 23 representing the hour
        - <Y>: w--> week / we--> weekend
        - <Z>:  defines usage type: chatzero/maps_pass/video_pass/video_hd_pass/mas_megas/music_pass/social_pass/regular_data(default)

     */

    val gnv_data_consumption_init = for(h <- gnv_hours; p <- gnv_periods; t <- gnv_types) yield "GNV_hour_" + h + "_" + p + "_" + t + "_Data_Volume_MB"

    val gnv_data_connections_init = for(h <- gnv_hours; p <- gnv_periods; t <- gnv_types) yield "GNV_hour_" + h + "_" + p + "_" + t + "_Num_Of_Connections"

    /*

    GNV_hour_0_WE_MOU, GNV_hour_10_WE_MOU, GNV_hour_11_WE_MOU

    GNV_hour_0_W_Num_Of_Calls, GNV_hour_0_WE_Num_Of_Calls, GNV_hour_10_W_Num_Of_Calls

    gnv_hour_<X>_<Y>_mou represents voice consumption in minutes in the corresponding frame based on:
        - <X>: value between 0 and 23 representing the hour
        - <Y>: w--> week / we--> weekend

    */

    val gnv_call_mou_init = for(h <- gnv_hours; p <- gnv_periods) yield "GNV_hour_" + h + "_" + p + "_MOU"

    val gnv_call_num_init = for(h <- gnv_hours; p <- gnv_periods) yield "GNV_hour_" + h + "_" + p + "_Num_Of_Calls"

    gnv_data_consumption_init.union(gnv_data_connections_init).union(gnv_call_mou_init).union(gnv_call_num_init)



  }

  def getNewGnvColumns(): List[String] = {

    // New feats computed from usage (Geneva) columns

    val gnv_hours = (0 to 23).by(1).toList

    val gnv_hourly_aggs = gnv_hours
      .flatMap(h => Array("total_data_volume_w_" + h,
        "total_connections_w_" + h,
        "data_per_connection_w_" + h,
        "total_data_volume_we_" + h,
        "total_connections_we_" + h,
        "data_per_connection_we_" + h,
        "mou_per_call_w_" + h,
        "mou_per_call_we_" + h))

    val gnv_daily_aggs = Array(
      "total_data_volume_w",
      "total_data_volume_we",
      "total_data_volume",
      "total_connections_w",
      "total_connections_we",
      "total_connections",
      "data_per_connection_w",
      "data_per_connection_we",
      "data_per_connection",
      "total_mou_w",
      "total_mou_we"
    )

    val gnv_max_aggs = Array(
      "max_data_volume_we",
      "hour_max_data_volume_we",
      "max_connections_we",
      "hour_max_connections_we",
      "max_data_per_connection_we",
      "hour_max_data_per_connection_we",

      "max_data_volume_w",
      "hour_max_data_volume_w",
      "max_connections_w",
      "hour_max_connections_w",
      "max_data_per_connection_w",
      "hour_max_data_per_connection_w",

      "max_mou_w",
      "hour_max_mou_w",
      "max_num_calls_w",
      "hour_max_num_calls_w",
      "max_mou_per_call_w",
      "hour_max_mou_per_call_w",

      "max_mou_we",
      "hour_max_mou_we",
      "max_num_calls_we",
      "hour_max_num_calls_we",
      "max_mou_per_call_we",
      "hour_max_mou_per_call_we"
    )

    // All the new feats computed from usage columns is given by the union of the subsets

    gnv_hourly_aggs
      .union(gnv_daily_aggs)
      .union(gnv_max_aggs)

  }

  def getCatFeatsGnv(): List[String] = List[String]()

  def getProcessedGnvColumns(): List[String] = getInitialGnvColumns()
    .union(getNewGnvColumns())

  ///// _______ Campaigns _______

  // Specific functions

  def getCampLevels(): List[String] = List("NIFs", "SRV")

  def getCampTypes(): List[String] = List(
    //"Retention_HH", No target 1
    "Retention_Voice",
    //"Legal_Informativa", No target 1
    //"Delight", No target 1
    // "Ignite", No target 1
    "Up_Cross_Sell"
    // TODO: "Up_Cross_Sell_HH", "Up_Cross_Sell" to be renamed to avoid confusion
    // "Welcome", No target 1
    // "Terceros", No SRV
  )

  def getCampChannels(): List[String] = List("EMA",
    "TEL",
    "SLS",
    "MLT",
    "SAT",
    // "TER",
    "NOT",
    "SMS",
    "MMS")

  def getCampGroups(): List[String] = List("Target", "Control", "Universal")

  def getCampRed(): List[String] = List("0", "1")

  def getCampGroupsChannels(): List[(String, String)] = for(x <- getCampGroups(); y <- getCampChannels()) yield (x, y)

  def getCampGroupsTypes(): List[(String, String)] = for(x <- getCampGroups(); y <- getCampTypes()) yield (x, y)

  def getCampNifColumns(): List[String] = List("Camp_NIFs_Delight_EMA_Target_0",
    "Camp_NIFs_Delight_SMS_Control_0",
    "Camp_NIFs_Delight_SMS_Target_0",
    "Camp_NIFs_Delight_TEL_Target_0",
    "Camp_NIFs_Delight_TEL_Universal_0",
    "Camp_NIFs_Ignite_EMA_Target_0",
    "Camp_NIFs_Ignite_SMS_Control_0",
    "Camp_NIFs_Ignite_SMS_Target_0",
    "Camp_NIFs_Ignite_TEL_Universal_0",
    "Camp_NIFs_Legal_Informativa_EMA_Target_0",
    //"Camp_NIFs_Legal_Informativa_fac_Target_0", TODO: fac?
    "Camp_NIFs_Legal_Informativa_SLS_Target_0",
    "Camp_NIFs_Legal_Informativa_SMS_Target_0",
    "Camp_NIFs_Retention_HH_SAT_Target_0",
    "Camp_NIFs_Retention_HH_TEL_Target_0",
    "Camp_NIFs_Retention_Voice_EMA_Control_0",
    "Camp_NIFs_Retention_Voice_EMA_Control_1",
    "Camp_NIFs_Retention_Voice_EMA_Target_0",
    "Camp_NIFs_Retention_Voice_EMA_Target_1",
    "Camp_NIFs_Retention_Voice_SAT_Control_0",
    "Camp_NIFs_Retention_Voice_SAT_Control_1",
    "Camp_NIFs_Retention_Voice_SAT_Target_0",
    "Camp_NIFs_Retention_Voice_SAT_Target_1",
    "Camp_NIFs_Retention_Voice_SAT_Universal_0",
    "Camp_NIFs_Retention_Voice_SAT_Universal_1",
    "Camp_NIFs_Retention_Voice_SMS_Control_0",
    "Camp_NIFs_Retention_Voice_SMS_Control_1",
    "Camp_NIFs_Retention_Voice_SMS_Target_0",
    "Camp_NIFs_Retention_Voice_SMS_Target_1",
    "Camp_NIFs_Terceros_TEL_Universal_0",
    "Camp_NIFs_Terceros_TER_Control_0",
    "Camp_NIFs_Terceros_TER_Control_1",
    "Camp_NIFs_Terceros_TER_Target_0",
    "Camp_NIFs_Terceros_TER_Target_1",
    "Camp_NIFs_Terceros_TER_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_EMA_Control_0",
    "Camp_NIFs_Up_Cross_Sell_EMA_Control_1",
    "Camp_NIFs_Up_Cross_Sell_EMA_Target_0",
    "Camp_NIFs_Up_Cross_Sell_EMA_Target_1",
    "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0",
    "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_1",
    "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0",
    "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1",
    "Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_HH_NOT_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0",
    "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_1",
    "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0",
    "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_1",
    "Camp_NIFs_Up_Cross_Sell_HH_SMS_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_1",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_0",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_1",
    "Camp_NIFs_Up_Cross_Sell_MLT_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_MMS_Control_0",
    "Camp_NIFs_Up_Cross_Sell_MMS_Control_1",
    "Camp_NIFs_Up_Cross_Sell_MMS_Target_0",
    "Camp_NIFs_Up_Cross_Sell_MMS_Target_1",
    "Camp_NIFs_Up_Cross_Sell_SMS_Control_0",
    "Camp_NIFs_Up_Cross_Sell_SMS_Control_1",
    "Camp_NIFs_Up_Cross_Sell_SMS_Target_0",
    "Camp_NIFs_Up_Cross_Sell_SMS_Target_1",
    "Camp_NIFs_Up_Cross_Sell_SMS_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_TEL_Control_0",
    "Camp_NIFs_Up_Cross_Sell_TEL_Control_1",
    "Camp_NIFs_Up_Cross_Sell_TEL_Target_0",
    "Camp_NIFs_Up_Cross_Sell_TEL_Target_1",
    "Camp_NIFs_Up_Cross_Sell_TEL_Universal_0",
    "Camp_NIFs_Up_Cross_Sell_TEL_Universal_1",
    "Camp_NIFs_Up_Cross_Sell_TER_Target_0",
    "Camp_NIFs_Up_Cross_Sell_TER_Universal_0",
    "Camp_NIFs_Welcome_EMA_Target_0",
    "Camp_NIFs_Welcome_TEL_Target_0",
    "Camp_NIFs_Welcome_TEL_Universal_0"
    //"Camp_NIFs_unknown_SAT_Target_0"
    )

  def getCampSrvColumns(): List[String] = List("Camp_SRV_Delight_EMA_Target_0",
    "Camp_SRV_Delight_MLT_Universal_0",
    "Camp_SRV_Delight_NOT_Target_0",
    "Camp_SRV_Delight_NOT_Universal_0",
    "Camp_SRV_Delight_SMS_Control_0",
    "Camp_SRV_Delight_SMS_Target_0",
    "Camp_SRV_Delight_SMS_Universal_0",
    "Camp_SRV_Delight_TEL_Universal_0",
    "Camp_SRV_Ignite_MMS_Target_0",
    "Camp_SRV_Ignite_NOT_Target_0",
    "Camp_SRV_Ignite_SMS_Target_0",
    "Camp_SRV_Ignite_SMS_Universal_0",
    "Camp_SRV_Legal_Informativa_EMA_Target_0",
    "Camp_SRV_Legal_Informativa_MLT_Universal_0",
    "Camp_SRV_Legal_Informativa_MMS_Target_0",
    "Camp_SRV_Legal_Informativa_NOT_Target_0",
    "Camp_SRV_Legal_Informativa_SMS_Target_0",
    "Camp_SRV_Retention_Voice_EMA_Control_0",
    "Camp_SRV_Retention_Voice_EMA_Control_1",
    "Camp_SRV_Retention_Voice_EMA_Target_0",
    "Camp_SRV_Retention_Voice_EMA_Target_1",
    "Camp_SRV_Retention_Voice_MLT_Universal_0",
    "Camp_SRV_Retention_Voice_NOT_Control_0",
    "Camp_SRV_Retention_Voice_NOT_Target_0",
    "Camp_SRV_Retention_Voice_NOT_Target_1",
    "Camp_SRV_Retention_Voice_NOT_Universal_0",
    "Camp_SRV_Retention_Voice_SAT_Control_0",
    "Camp_SRV_Retention_Voice_SAT_Control_1",
    "Camp_SRV_Retention_Voice_SAT_Target_0",
    "Camp_SRV_Retention_Voice_SAT_Target_1",
    "Camp_SRV_Retention_Voice_SAT_Universal_0",
    "Camp_SRV_Retention_Voice_SAT_Universal_1",
    "Camp_SRV_Retention_Voice_SLS_Control_0",
    "Camp_SRV_Retention_Voice_SLS_Control_1",
    "Camp_SRV_Retention_Voice_SLS_Target_0",
    "Camp_SRV_Retention_Voice_SLS_Target_1",
    "Camp_SRV_Retention_Voice_SLS_Universal_0",
    "Camp_SRV_Retention_Voice_SLS_Universal_1",
    "Camp_SRV_Retention_Voice_SMS_Control_0",
    "Camp_SRV_Retention_Voice_SMS_Control_1",
    "Camp_SRV_Retention_Voice_SMS_Target_0",
    "Camp_SRV_Retention_Voice_SMS_Target_1",
    "Camp_SRV_Retention_Voice_SMS_Universal_0",
    "Camp_SRV_Retention_Voice_SMS_Universal_1",
    "Camp_SRV_Retention_Voice_TEL_Control_0",
    "Camp_SRV_Retention_Voice_TEL_Control_1",
    "Camp_SRV_Retention_Voice_TEL_Target_0",
    "Camp_SRV_Retention_Voice_TEL_Target_1",
    "Camp_SRV_Retention_Voice_TEL_Universal_0",
    "Camp_SRV_Retention_Voice_TEL_Universal_1",
    "Camp_SRV_Up_Cross_Sell_EMA_Control_0",
    "Camp_SRV_Up_Cross_Sell_EMA_Control_1",
    "Camp_SRV_Up_Cross_Sell_EMA_Target_0",
    "Camp_SRV_Up_Cross_Sell_EMA_Target_1",
    "Camp_SRV_Up_Cross_Sell_EMA_Universal_0",
    "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0",
    "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_1",
    "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0",
    "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_1",
    "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_0",
    "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_1",
    "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0",
    "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_1",
    "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0",
    "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_1",
    "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0",
    "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_1",
    "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0",
    "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_1",
    "Camp_SRV_Up_Cross_Sell_HH_SMS_Universal_0",
    "Camp_SRV_Up_Cross_Sell_MLT_Universal_0",
    "Camp_SRV_Up_Cross_Sell_MMS_Control_0",
    "Camp_SRV_Up_Cross_Sell_MMS_Control_1",
    "Camp_SRV_Up_Cross_Sell_MMS_Target_0",
    "Camp_SRV_Up_Cross_Sell_MMS_Target_1",
    "Camp_SRV_Up_Cross_Sell_NOT_Control_0",
    "Camp_SRV_Up_Cross_Sell_NOT_Target_0",
    "Camp_SRV_Up_Cross_Sell_NOT_Target_1",
    "Camp_SRV_Up_Cross_Sell_NOT_Universal_0",
    //"Camp_SRV_Up_Cross_Sell_rcs_Control_0", TODO: rcs?
    //"Camp_SRV_Up_Cross_Sell_rcs_Target_0", TODO: rcs?
    "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
    "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
    "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
    "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
    "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
    "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
    "Camp_SRV_Up_Cross_Sell_SMS_Target_0",
    "Camp_SRV_Up_Cross_Sell_SMS_Target_1",
    "Camp_SRV_Up_Cross_Sell_SMS_Universal_0",
    "Camp_SRV_Up_Cross_Sell_SMS_Universal_1",
    "Camp_SRV_Up_Cross_Sell_TEL_Control_0",
    "Camp_SRV_Up_Cross_Sell_TEL_Control_1",
    "Camp_SRV_Up_Cross_Sell_TEL_Target_0",
    "Camp_SRV_Up_Cross_Sell_TEL_Target_1",
    "Camp_SRV_Up_Cross_Sell_TEL_Universal_0",
    "Camp_SRV_Up_Cross_Sell_TEL_Universal_1",
    "Camp_SRV_Welcome_EMA_Target_0",
    "Camp_SRV_Welcome_MMS_Target_0",
    "Camp_SRV_Welcome_SMS_Target_0")

  // Generic functions

  def getInitialCampColumns(): List[String] = {

    /*
    camp_nifs_<type>_<channel>_<group>_<red>. Number of registers associated to the corresponding segment based on :
      - <type>: campaign tipology:  Delight/Ignite/Legal/RetentionHH/RetentionVocie/UpCrossSell/UpCrossSellHH/Terceros/Welcome
      - <channel>: email/sms/telesales/satisfaction/terceros/mms/notification
      - <group>: three values --> Target/Universal Control Group/ Campaign Control Group
      - <red>: 1 when redemption detected, otherwise 0
    */

    /*
    camp_srv_<type>_<channel>_<group>_<red>. Number of registers associated to the corresponding segment
     */

    // Camp_SRV_Up_Cross_Sell_MLT_Universal_0, Camp_SRV_Up_Cross_Sell_MLT_Universal_1

    // for(v <- getCampLevels(); w <- getCampTypes(); x <- getCampChannels(); y <- getCampGroups(); z <- getCampRed()) yield "Camp_" + v + "_" + w + "_" + x + "_" + y + "_" + z

    getCampNifColumns() ++ getCampSrvColumns()

  }

  def getNewCampColumns(): List[String] = {

    val camp_groups_aggs = getCampGroups()
      .flatMap(g => Array("total_camps_" + g, "total_redem_" + g, "pcg_redem_" + g))

    val camp_channels_aggs = getCampChannels()
      .flatMap(ch => Array("total_camps_" + ch, "total_redem_" + ch, "pcg_redem_" + ch))

    val camp_types_aggs = getCampTypes()
      .flatMap(t => Array("total_camps_" + t, "total_redem_" + t, "pcg_redem_" + t))

    /*
    val camp_groups_channels_aggs = getCampGroupsChannels()
      .flatMap{case(g, ch) => Array("total_camps_" + g + "_" + ch, "total_redem_" + g + "_" + ch, "pcg_redem_" + g + "_" + ch)}

    val camp_groups_types_aggs = getCampGroupsTypes()
      .flatMap{case(g, t) => Array("total_camps_" + g + "_" + t, "total_redem_" + g + "_" + t, "pcg_redem_" + g + "_" + t)}

    camp_groups_aggs ++ camp_channels_aggs ++ camp_types_aggs ++ camp_groups_channels_aggs ++ camp_groups_types_aggs

    */

    camp_groups_aggs ++ camp_channels_aggs ++ camp_types_aggs

  }

  def getCatFeatsCamp(): List[String] = List[String]()

  def getProcessedCampColumns(): List[String] = getInitialCampColumns()
    .union(getNewCampColumns())

  ///// _______ Billing _______

  // Specific functions

  def getBillingMonths(): List[Int] = (1 to 5).toList

  // Generic functions

  def getInitialBillingColumns(): List[String] = {

    // Bill_N1_InvoiceCharges, Bill_N1_Amount_To_Pay, Bill_N1_Tax_Amount, Bill_N1_Debt_Amount, Bill_N1_Bill_Date

    getBillingMonths().flatMap(n => List("Bill_N" + n + "_InvoiceCharges", "Bill_N" + n + "_Amount_To_Pay", "Bill_N" + n + "_Tax_Amount", "Bill_N" + n + "_Debt_Amount"))

    // getBillingMonths().flatMap(n => List("Bill_N" + n + "_Bill_Amount", "Bill_N" + n + "_Tax_Amount"))

  }

  def getNewBillingColumns(): List[String] = {

    getBillingMonths().map(m => "bill_n" + m + "_net")

  }

  def getCatFeatsBilling(): List[String] = List[String]()

  def getProcessedBillingColumns(): List[String] = getInitialBillingColumns()
    .union(getNewBillingColumns())

  // Crm

  def getInitialCrmColumns(): List[String] = List("codigo_postal",
    "metodo_pago",
    "cta_correo",
    "factura_electronica",
    "superoferta",
    "fecha_migracion",
    "tipo_documento",
    "nacionalidad",
    "x_antiguedad_cuenta",
    "x_datos_navegacion",
    "x_datos_trafico",
    "x_cesion_datos",
    "x_user_facebook",
    "x_user_twitter",
    "marriage2hgbst_elm",
    "gender2hgbst_elm",
    "flg_robinson",
    "x_formato_factura",
    "x_idioma_factura",
    "bam_services",
    "bam_fx_first",
    "bam-movil_services",
    "bam-movil_fx_first",
    "fbb_services",
    "fbb_fx_first",
    "fixed_services",
    "fixed_fx_first",
    "movil_services",
    "movil_fx_first",
    "prepaid_services",
    "prepaid_fx_first",
    "tv_services",
    "tv_fx_first",
    "fx_srv_basic",
    "tipo_sim",
    "desc_tariff",
    "tariff",
    "fx_tariff",
    "price_tariff",
    "voice_tariff",
    "fx_voice_tariff",
    "data",
    "fx_data",
    "dto_lev1",
    "fx_dto_lev1",
    "price_dto_lev1",
    "dto_lev2",
    "fx_dto_lev2",
    "price_dto_lev2",
    "data_additional",
    "fx_data_additional",
    "roam_zona_2",
    "fx_roam_zona_2",
    "sim_vf"
  )

  def getCatFeatsCrm(): List[String] = List(
    "metodo_pago",
    "cta_correo",
    "factura_electronica",
    "superoferta",
    "cliente_migrado",
    "tipo_documento",
    "nacionalidad",
    "x_antiguedad_cuenta",
    "x_datos_navegacion",
    "x_datos_trafico",
    "x_cesion_datos",
    "marriage2hgbst_elm",
    "gender2hgbst_elm",
    "flg_robinson",
    "x_formato_factura",
    "x_idioma_factura",
    "tipo_sim",
    "segunda_linea",
    "desc_tariff",
    "tariff",
    "voice_tariff",
    "data",
    "dto_lev1",
    "dto_lev2",
    "data_additional",
    "roam_zona_2",
    "sim_vf"
  )

  def getNewCrmColumns(): List[String] = List("dias_desde_fecha_migracion",
    "cliente_migrado",
    "dias_desde_bam_fx_first",
    "dias_desde_bam-movil_fx_first",
    "dias_desde_fbb_fx_first",
    "dias_desde_fixed_fx_first",
    "dias_desde_movil_fx_first",
    "dias_desde_prepaid_fx_first",
    "dias_desde_tv_fx_first",
    "dias_desde_fx_srv_basic",
    "segunda_linea",
    "dias_desde_fx_tariff",
    "dias_desde_fx_voice_tariff",
    "dias_desde_fx_data",
    "dias_desde_fx_dto_lev1",
    "dias_desde_fx_dto_lev2",
    "dias_desde_fx_data_additional",
    "dias_desde_fx_roam_zona_2",
    "total_num_services",
    "real_price"
  )

  def getProcessedCrmColumns(): List[String] = getInitialCrmColumns()
    .union(getNewCrmColumns())
    .diff(List(
    "fecha_migracion",
    "bam_fx_first",
    "bam-movil_fx_first",
    "fbb_fx_first",
    "fixed_fx_first",
    "movil_fx_first",
    "prepaid_fx_first",
    "tv_fx_first",
    "fx_srv_basic",
    "fx_tariff",
    "fx_voice_tariff",
    "fx_data",
    "fx_dto_lev1",
    "fx_dto_lev2",
    "fx_data_additional",
    "fx_roam_zona_2"))

  // General

  def getRawMobileColumns(): List[String] = getIdFeats()
    .union(getInitialCrmColumns())
    .union(getInitialGnvColumns())
    .union(getInitialCampColumns())
    .union(getInitialBillingColumns())
    .distinct

  def getProcessedColumns(): List[String] = getIdFeats()
    .union(getProcessedCrmColumns())
    .union(getProcessedGnvColumns())
    .union(getProcessedCampColumns())
    .union(getProcessedBillingColumns())

  def getFeatSubset(feats2rem: List[String]): List[String] = {

    val removed_feats = getFeats2RemFromList(feats2rem)

    getProcessedColumns().diff(removed_feats)

  }

  def getFeats2RemFromList(feats2rem: List[String]): List[String] = {

    def getFeats2Rem(f: String): List[String] = {

      f match {

        case "id" => getIdFeats()

        case "gnv_raw" => getInitialGnvColumns()

        case "gnv_new" => getNewGnvColumns()

        case "camp_raw" => getInitialCampColumns()

        case "camp_new" => getNewCampColumns()

        case "billing_raw" => getInitialBillingColumns()

        case "billing_new" => getNewBillingColumns()
      }
    }

    if (feats2rem.isEmpty) {
      List[String]()
    } else {
      getFeats2Rem(feats2rem.head) ++ getFeats2RemFromList(feats2rem.tail)

    }

  }

  def getCatFeats(): List[String] = getCatFeatsCrm() ++ getCatFeatsGnv() ++ getCatFeatsCamp() ++ getCatFeatsBilling()

  def getNoInputFeats(): List[String] = List(
    "x_user_facebook",
    "x_user_twitter",
    "codigo_postal"
  )

}
