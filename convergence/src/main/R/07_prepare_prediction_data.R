source("configuration.R")
suppressMessages(library(h2o))

cleanDataCurrentSnapshot <- function(dt.data, month) {
  cat("[INFO] cleanDataCurrentSnapshot( month =", month, ")\n")
  # Get the last day of the month
  month <- paste0(month, "01")
  month.posix <- as.POSIXct(month, format="%Y%m%d")
  last.day <- seq.POSIXt(month.posix, length=2, by="months")[2]
  
  dt.data[, part_status := as.factor(part_status)]
  dt.data[, x_tipo_ident := as.factor(x_tipo_ident)]
  
  dt.data[, x_fecha_nacimiento := as.POSIXct(x_fecha_nacimiento, format="%Y-%m-%d %H:%M:%S")]
  dt.data[, IS_NA_FECHA_NACIMIENTO := 0]
  dt.data[is.na(x_fecha_nacimiento), IS_NA_FECHA_NACIMIENTO := 1]
  dt.data[is.na(x_fecha_nacimiento), x_fecha_nacimiento := as.POSIXct("1970-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S")]
  dt.data[, EDAD := as.numeric(last.day - x_fecha_nacimiento)/365]
  dt.data[, x_fecha_nacimiento := NULL]
  
  dt.data[, x_nacionalidad := as.factor(x_nacionalidad)]
  
  if ("SEG_CLIENTE" %in% colnames(dt.data)) {
    dt.data[, IS_NA_SEG_CLIENTE := 0]
    dt.data[is.na(SEG_CLIENTE), IS_NA_SEG_CLIENTE := 1]
    dt.data[is.na(SEG_CLIENTE), SEG_CLIENTE := 0]
  }
  
  dt.data[, IS_NA_COD_SEGFID := 0]
  dt.data[is.na(COD_SEGFID), IS_NA_COD_SEGFID := 1]
  dt.data[is.na(COD_SEGFID), COD_SEGFID := 0]
  
  dt.data[, x_fecha_activacion := as.POSIXct(substr(x_fecha_activacion, 1, 10))]
  if (nrow(dt.data[x_fecha_activacion > last.day])) {
    cat("[ERROR] x_fecha_activacion Greater than Last Monthly day in ",
        nrow(dt.data[x_fecha_activacion >= last.day]), " records \n")
    dt.data <- dt.data[x_fecha_activacion < last.day]
  }
  dt.data[, DIAS_DESDE_ACTIVACION := round(as.numeric(last.day - x_fecha_activacion))]
  dt.data[, x_fecha_activacion := NULL]
  
  dt.data[, X_FECHA_CREACION_CUENTA := as.POSIXct(substr(X_FECHA_CREACION_CUENTA,1, 10))]
  if (nrow(dt.data[X_FECHA_CREACION_CUENTA >= last.day])) {
    cat("[ERROR] X_FECHA_CREACION_CUENTA Greather than Last Monthly day in ",
        nrow(dt.data[X_FECHA_CREACION_CUENTA >= last.day]), " records \n")
    dt.data <- dt.data[X_FECHA_CREACION_CUENTA < last.day]
  }
  dt.data[, DIAS_DESDE_CREACION_CUENTA := round(as.numeric(last.day - X_FECHA_CREACION_CUENTA))]
  dt.data[, X_FECHA_CREACION_CUENTA := NULL]
  
  dt.data[, X_FECHA_CREACION_SERVICIO := as.POSIXct(X_FECHA_CREACION_SERVICIO, format="%Y-%m-%d %H:%M:%S")]
  if (nrow(dt.data[X_FECHA_CREACION_SERVICIO >= last.day])) {
    cat("[ERROR] X_FECHA_CREACION_SERVICIO Greather than Last Monthly day in ",
        nrow(dt.data[X_FECHA_CREACION_SERVICIO >= last.day]), " records \n")
    dt.data <- dt.data[X_FECHA_CREACION_SERVICIO < last.day]
  }
  dt.data[, DIAS_DESDE_CREACION_SERVICIO := round(as.numeric(last.day - X_FECHA_CREACION_SERVICIO)/1440)]
  dt.data[, X_FECHA_CREACION_SERVICIO := NULL]
  
  dt.data[, x_fecha_ini_prov := as.POSIXct(substr(x_fecha_ini_prov,1 ,10))]
  if (nrow(dt.data[x_fecha_ini_prov >= last.day])) {
    cat("[ERROR] x_fecha_ini_prov Greather than Last Monthly day in ",
        nrow(dt.data[x_fecha_ini_prov >= last.day]), " records \n")
    dt.data <- dt.data[x_fecha_ini_prov < last.day]
  }
  dt.data[, DIAS_DESDE_INI_PROV := round(as.numeric(last.day - x_fecha_ini_prov))]
  dt.data[, x_fecha_ini_prov := NULL]
  
  dt.data[, x_plan := as.factor(x_plan)]
  dt.data[, PLANDATOS := as.factor(PLANDATOS)]
  
  dt.data[, WITH_EMAIL := 0]
  dt.data[nchar(enc2utf8(EMAIL_CLIENTE)) > 0, WITH_EMAIL := 1]
  dt.data[, EMAIL_CLIENTE := NULL]
  
  dt.data[, PROMOCION_VF := as.factor(PROMOCION_VF)]
  
  dt.data[, IS_NA_FECHA_FIN_CP_VF := 0]
  dt.data[is.na(FECHA_FIN_CP_VF), IS_NA_FECHA_FIN_CP_VF := 1]
  dt.data[FECHA_FIN_CP_VF == "", IS_NA_FECHA_FIN_CP_VF := 1]
  dt.data[, FECHA_FIN_CP_VF := as.character(FECHA_FIN_CP_VF)]
  dt.data[IS_NA_FECHA_FIN_CP_VF == 1, FECHA_FIN_CP_VF := "1970-01-01"]
  
  dt.data[, FECHA_FIN_CP_VF := as.POSIXct(FECHA_FIN_CP_VF, format="%Y-%m-%d")]
  dt.data[, DIAS_HASTA_FIN_CP_VF := as.numeric(FECHA_FIN_CP_VF - last.day)/86400]
  dt.data[, FECHA_FIN_CP_VF := NULL]
  
  dt.data[, IS_NA_MESES_FIN_CP_VF := 0]
  dt.data[is.na(MESES_FIN_CP_VF), IS_NA_MESES_FIN_CP_VF := 1]
  dt.data[is.na(MESES_FIN_CP_VF), MESES_FIN_CP_VF := 0]
  
  dt.data[, PROMOCION_TARIFA := as.factor(PROMOCION_TARIFA)]
  
  # FECHA_FIN_CP_TARIFA
  dt.data[, IS_NA_FECHA_FIN_CP_TARIFA := 0]
  dt.data[is.na(FECHA_FIN_CP_TARIFA), IS_NA_FECHA_FIN_CP_TARIFA := 1]
  dt.data[FECHA_FIN_CP_TARIFA == "", IS_NA_FECHA_FIN_CP_TARIFA := 1]
  
  dt.data[, FECHA_FIN_CP_TARIFA := as.character(FECHA_FIN_CP_TARIFA)]
  dt.data[IS_NA_FECHA_FIN_CP_TARIFA == 1, FECHA_FIN_CP_TARIFA := "1970-01-01"]
  dt.data[, FECHA_FIN_CP_TARIFA := as.POSIXct(FECHA_FIN_CP_TARIFA, format="%Y-%m-%d")]
  
  dt.data[, DIAS_DESDE_FECHA_FIN_CP_TARIFA := as.numeric(FECHA_FIN_CP_TARIFA - last.day)/86400]
  dt.data[, FECHA_FIN_CP_TARIFA := NULL]
  
  dt.data[, IS_NA_MESES_FIN_CP_TARIFA := 0]
  dt.data[is.na(MESES_FIN_CP_TARIFA), IS_NA_MESES_FIN_CP_TARIFA := 1]
  dt.data[is.na(MESES_FIN_CP_TARIFA), MESES_FIN_CP_TARIFA := 0]
  
  dt.data[, modelo := as.factor(modelo)]
  
  dt.data[, sistema_operativo := as.factor(sistema_operativo)]
  
  dt.data[, LAST_UPD := as.POSIXct(substr(LAST_UPD, 1, 10), format="%Y-%m-%d")]  
  dt.data[is.na(LAST_UPD), LAST_UPD := as.POSIXct("1970-01-01", format="%Y-%m-%d")]
  dt.data[, DIAS_DESDE_LAST_UPD := as.numeric(last.day - LAST_UPD)]
  dt.data[, LAST_UPD := NULL]
  
  dt.data[, PPRECIOS_DESTINO := as.factor(PPRECIOS_DESTINO)]
  
  dt.data[, ppid_destino := as.factor(ppid_destino)]
  
  dt.data[, IS_NA_PUNTOS := 0]
  dt.data[is.na(PUNTOS), IS_NA_PUNTOS := 1]
  dt.data[is.na(PUNTOS), PUNTOS := 0]
  
  dt.data[, IS_NA_ARPU := 0]
  dt.data[is.na(ARPU), IS_NA_ARPU := 1]
  dt.data[is.na(ARPU), ARPU := 0 ]
  
  dt.data[, CODIGO_POSTAL := as.factor(CODIGO_POSTAL)]
  
  dt.data[, IS_NA_VFSMARTPHONE := 0]
  dt.data[is.na(VFSMARTPHONE), IS_NA_VFSMARTPHONE := 1]
  dt.data[is.na(VFSMARTPHONE), VFSMARTPHONE := 0]
  
  if ("CUOTAS_PENDIENTES" %in% colnames(dt.data)) {
    dt.data[, IS_NA_CUOTAS_PENDIENTES := 0]
    dt.data[is.na(CUOTAS_PENDIENTES), IS_NA_CUOTAS_PENDIENTES := 1]
    dt.data[is.na(CUOTAS_PENDIENTES), CUOTAS_PENDIENTES := 0]
  }
  
  if ("CANTIDAD_PENDIENTE" %in% colnames(dt.data)) {
    dt.data[, IS_NA_CANTIDAD_PENDIENTE := 0]
    dt.data[is.na(CANTIDAD_PENDIENTE), IS_NA_CANTIDAD_PENDIENTE := 1]
    dt.data[is.na(CANTIDAD_PENDIENTE), CANTIDAD_PENDIENTE := 0]
  }
  
  return(dt.data)
}

cleanNA <- function(dt.data) {
  cat("[INFO] cleanNA()\n")
  
  if ("IND_PROP_ADSL_FIBRA" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_ADSL_FIBRA), IND_PROP_ADSL_FIBRA := -1]
  if ("IND_PROP_HZ" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_HZ), IND_PROP_HZ := -1]
  if ("IND_PROP_TABLET" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_TABLET), IND_PROP_TABLET := -1]
  if ("IND_PROP_LPD" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_LPD), IND_PROP_LPD := -1]
  if ("IND_PROP_UP_VOZ_MONO" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_UP_VOZ_MONO), IND_PROP_UP_VOZ_MONO := -1]
  if ("IND_PROP_UP_VOZ_MULTI" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_UP_VOZ_MULTI), IND_PROP_UP_VOZ_MULTI := -1]
  if ("IND_MUY_PROP_ADSL_FIBRA" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_ADSL_FIBRA), IND_MUY_PROP_ADSL_FIBRA := -1]
  
  if ("IND_MUY_PROP_HZ" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_HZ), IND_MUY_PROP_HZ := -1]
  if ("IND_MUY_PROP_TABLET" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_TABLET), IND_MUY_PROP_TABLET := -1]
  if ("IND_MUY_PROP_LPD" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_LPD), IND_MUY_PROP_LPD := -1]
  if ("IND_MUY_PROP_UP_VOZ_MONO" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_UP_VOZ_MONO), IND_MUY_PROP_UP_VOZ_MONO := -1]
  if ("IND_MUY_PROP_UP_VOZ_MULTI" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_UP_VOZ_MULTI), IND_MUY_PROP_UP_VOZ_MULTI := -1]
  
  if ("IND_PROP_AOU" %in% colnames(dt.data))
    dt.data[is.na(IND_PROP_AOU), IND_PROP_AOU := -1]
  if ("IND_MUY_PROP_AOU" %in% colnames(dt.data))
    dt.data[is.na(IND_MUY_PROP_AOU), IND_MUY_PROP_AOU := -1]
  
  if ("IND_PROP_TV_FIBRA" %in% colnames(dt.data)) {
    dt.data[,IND_PROP_TV := 0]
    dt.data[IND_PROP_TV_ADSL == 1 | IND_PROP_TV_FIBRA == 1, IND_PROP_TV := 1]
    dt.data[,IND_MUY_PROP_TV := 0]
    dt.data[IND_MUY_PROP_TV_ADSL == 1 | IND_MUY_PROP_TV_FIBRA == 1, IND_MUY_PROP_TV := 1]
    dt.data[, IND_PROP_TV_ADSL := NULL]
    dt.data[, IND_MUY_PROP_TV_ADSL := NULL]
    suppressWarnings(dt.data[, IND_PROP_TV_FIBRAL := NULL])
    dt.data[, IND_MUY_PROP_TV_FIBRA := NULL]
  } else {
    if ("IND_PROP_TV" %in% colnames(dt.data))
      dt.data[is.na(IND_PROP_TV), IND_PROP_TV := -1]
    if ("IND_MUY_PROP_TV" %in% colnames(dt.data))
      dt.data[is.na(IND_MUY_PROP_TV), IND_MUY_PROP_TV := -1]
  }
  
  return(dt.data)
}

prepareData <- function(dt.data, month) {
  cat("[INFO] prepareData( month =", month, ")\n")
  
  prev.month <- paste0(month, "01")
  prev.month <- as.POSIXct(prev.month, format="%Y%m%d")
  prev.month <- prev.month - 2
  prev.month <- as.character(prev.month)
  prev.month <- substr(prev.month, 1, 7)
  prev.month <- gsub("-", "", prev.month)
  
  #ifile <- paste0(data.folder,"/Campaign/", month, "/dt.Campaigns.", month, ".RData")
  #cat("[LOAD] ", ifile, "\n")
  #load(ifile)
  #setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  #setkey(dt.Campaigns, x_num_ident)
  
  setkey(dt.data, x_num_ident)
  
  # There is not par_propensos_cross_nif in the first month of data
  if (strptime(paste0(prev.month, "01"), "%Y%m%d") >= strptime(paste0(first.month.with.data.prop, "01"), "%Y%m%d")) {
    ifile <- file.path(data.folder, "ACC", prev.month, paste0("dt_par_propensos_cross_nif_", prev.month, ".RData"))
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    
    setkey(dt_par_propensos_cross_nif, x_num_ident)
    
    # Join with Accenture Output
    dt.data <- dt_par_propensos_cross_nif[dt.data]
  } else {
    cat("[INFO] Skipping", paste0("dt_par_propensos_cross_nif_", prev.month, ".RData"), "because the first month with this data file is", first.month.with.data.prop, "\n")
  }
  
  dt.data <- cleanNA(dt.data)

  # Join with Campaigns
  #dt.data <- dt.Campaigns[dt.data]
  #nombres <- names(dt.Campaigns)
  #nombres <- nombres[!nombres %in% "x_num_ident" ]
  #dt.data[is.na(number_of_contacts), nombres := -1, with = F]
  
  # TODO: Join with clusters
  
  # Join with channel propensity
  # ofolder <- file.path(data.folder, "Contacts", prev.month)
  # ifile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", prev.month, ".RData"))
  # cat("[LOAD] ", ifile, "\n")
  # load(ifile)
  # setkey(dt.contacts.hist.byid, x_num_ident)
  # dt.contacts.hist.byid[, GENESIS_HIST_TASA_INTENTO := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_INTENTO := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_PROP_CANAL_TELEVENTA := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := NULL]
  # 
  # dt.data <- dt.contacts.hist.byid[dt.data]
  # nombres <- names(dt.contacts.hist.byid)
  # nombres <- nombres[!nombres %in% "x_num_ident" ]
  # dt.data[is.na(GENESIS_HIST_NUM_INTENTOS), nombres := 0, with = F]
  
  
  dt.data[, x_nacionalidad := as.numeric(x_nacionalidad)]
  dt.data[, x_plan := as.numeric(x_plan)]
  dt.data[, PLANDATOS := as.numeric(PLANDATOS)]
  
  dt.data[, PROMOCION_VF := as.numeric(PROMOCION_VF)]
  dt.data[, sistema_operativo := as.numeric(sistema_operativo)]
  dt.data[, PPRECIOS_DESTINO := as.numeric(PPRECIOS_DESTINO)]
  dt.data[, ppid_destino := as.numeric(ppid_destino)]
  dt.data[, CODIGO_POSTAL := as.numeric(CODIGO_POSTAL)]
  dt.data[, modelo := as.numeric(modelo)]
  
  dt.data[, DIAS_DESDE_CREACION_CUENTA := NULL]
  dt.data[, DIAS_DESDE_CREACION_SERVICIO := NULL]
  dt.data[, DIAS_DESDE_INI_PROV := NULL]
  #dt.data[, DIAS_DESDE_ACTIVACION := NULL]
  dt.data[, DIAS_DESDE_LAST_UPD := NULL]
  dt.data[, FLAG_HUELLA_ONO := NULL]
  dt.data[, FLAG_HUELLA_VF := NULL]
  
  # dt.data[, x_num_ident := NULL] 
  
  dt.data[, x_tipo_ident := NULL]
  
  dt.data[, PROMOCION_TARIFA := NULL]
  dt.data[, part_status := as.numeric(part_status)]
  
  return(dt.data)
}

main_07_prepare_prediction_data <- function(model.month, pred.day, dt.MobileOnly = NULL, dt_EXTR_NIFS_COMPARTIDOS = NULL, dt_SUN_INFO_CRUCE = NULL, dt_EXTR_AC_FINAL_POSPAGO = NULL) {
  cat ("\n[MAIN] 07_prepare_prediction_data", model.month, pred.day, "\n")
  
  ifile <- file.path(models.folder, model.month, "rf.model.h2o", "h2orf")
  if (!file.exists(ifile)) {
    cat("[ERROR] Model", ifile, "does not exist\n")
    stop(paste0("Please, train a model for month ", model.month), call.=FALSE)
  }
  
  pred.day.date <- as.Date(pred.day, format = "%Y%m%d")
  if(is.na(pred.day.date)) {
    #print_help(opt_parser)
    #stop("Parameter 'input pred.day' must be in the form: YYYYMMDD", call.=FALSE)
    
    month <- pred.day
  } else {
    month <- format(pred.day.date, format = "%Y%m")
  }
  
  # Load current month
  ifile <- file.path(datasets.folder, pred.day, paste0("dt.MobileOnly-", pred.day, ".RData"))
  if (is.null(dt.MobileOnly)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  setkey(dt.MobileOnly, x_num_ident)
  
  # If the pred.day provided is an extraction of an on-going month (i.e. day is YYYYMMDD)
  if(!is.na(pred.day.date)) {
    # Get the first day of the month
    month.posix <- as.POSIXct(paste0(month, "01"), format="%Y%m%d")
    prev.pred.day <- seq.POSIXt(month.posix, length=2, by="-1 months")[2]
    prev.month <- format(prev.pred.day, format = "%Y%m")
    
    # Load previous month to get SEG_CLIENTE and paste it to current month
    #ifile <- file.path(datasets.folder, prev.month, paste0("dt.MobileOnly-", prev.month, ".RData"))
    ifile <- file.path(data.folder, "CVM", prev.month, paste0("dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE_", prev.month, ".RData"))
    if (is.null(dt_EXTR_AC_FINAL_POSPAGO)) {
      cat("[LOAD]", ifile, "(to get SEG_CLIENTE)\n")
      load(ifile)
    } else {
      cat("[INFO] Skipping", ifile, "(to get SEG_CLIENTE), already loaded\n")
    }
    
    #dt.mobileOnly.prev <- dt.MobileOnly[, .(x_num_ident, SEG_CLIENTE)]
    dt.MobileOnly.prev <- dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE[, .(x_num_ident, SEG_CLIENTE)]
    setkey(dt.MobileOnly.prev, x_num_ident)
    
    dt.MobileOnly.prev <- dt.MobileOnly.prev[!is.na(SEG_CLIENTE)]
    dt.MobileOnly.prev <- unique(dt.MobileOnly.prev, by=key(dt.MobileOnly.prev)) # FIXME: En dt_EXTR_AC_FINAL_POSPAGO un mismo x_num_ident puede tener varios SEG_CLIENTE diferentes (uno por cada servicio)
    #dt.MobileOnly.prev[duplicated(dt.MobileOnly.prev)]
    
    dt.MobileOnly <- dt.MobileOnly.prev[dt.MobileOnly]
    #dt.MobileOnly[duplicated(dt.MobileOnly)]
  }
  
  # Load Ids shared with Ono stack
  
  # ifile <- file.path(data.folder, "CVM", pred.day, paste0("dt_EXTR_NIFS_COMPARTIDOS_", pred.day, ".RData"))
  # if (is.null(dt_EXTR_NIFS_COMPARTIDOS)) {
  #   cat("[LOAD]", ifile, "\n")
  #   load(ifile)
  # } else {
  #   cat("[INFO] Skipping", ifile, ", already loaded\n")
  # }
  
  ifile <- file.path(data.folder, "CVM", pred.day, paste0("dt_SUN_INFO_CRUCE_", pred.day, ".RData"))
  if (is.null(dt_SUN_INFO_CRUCE)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  
  # Filter clients according to CVM - Campaigns & Analytics (CBU) business rules
  
  # Without ADSL or Fibre -> This is already filtered by 02_get_mobile_only_postpaid.R
  
  # With fingerprint of something
  # if (! "FLAG_HUELLA_ONO" %in% colnames(dt.MobileOnly)) {
  #   dt.MobileOnly[, FLAG_HUELLA_ONO := 0]
  # }
  # if (! "FLAG_HUELLA_VF" %in% colnames(dt.MobileOnly)) {
  #   dt.MobileOnly[, FLAG_HUELLA_VF := 0]
  # }
  if (! "FLAG_HUELLA_NEBA" %in% colnames(dt.MobileOnly)) {
    dt.MobileOnly[, FLAG_HUELLA_NEBA := 0]
  }
  if (! "FLAG_HUELLA_ARIEL" %in% colnames(dt.MobileOnly)) {
    dt.MobileOnly[, FLAG_HUELLA_ARIEL := 0]
  }
  # No indica que el cliente esté en cobertura, sino que si está en cobertura (con el resto de flags) si lo está en zona competitiva o no, vamos que hay clientes con este flag a 1 pero que no están en huella
  # if (! "FLAG_ZONA_COMPETITIVA" %in% colnames(dt.MobileOnly)) {
  #   dt.MobileOnly[, FLAG_ZONA_COMPETITIVA := 0]
  # }
  if (! "FLAG_COBERTURA_ADSL" %in% colnames(dt.MobileOnly)) {
    dt.MobileOnly[, FLAG_COBERTURA_ADSL := NA]
  }
  dt.MobileOnly <- dt.MobileOnly[FLAG_HUELLA_ONO == 1 | FLAG_HUELLA_VF == 1 | FLAG_HUELLA_NEBA == 1 | FLAG_HUELLA_ARIEL == 1 | FLAG_COBERTURA_ADSL == "D"]
  
  # Without HZ
  dt.MobileOnly <- dt.MobileOnly[FLAGHZ == 0]
  
  # Without Lortad
  dt.MobileOnly <- dt.MobileOnly[LORTAD == 0]
  
  # Without debt
  dt.MobileOnly <- dt.MobileOnly[DEUDA == 0]
  
  # With voice active
  dt.MobileOnly <- dt.MobileOnly[part_status == "AC"]
  
  # Not shared with Ono stack
  # if (! is.null(dt_EXTR_NIFS_COMPARTIDOS)) {
  #   dt.MobileOnly <- dt.MobileOnly[!x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF]
  # }
  if (! is.null(dt_SUN_INFO_CRUCE)) {
    dt.MobileOnly <- dt.MobileOnly[!x_num_ident %in% dt_SUN_INFO_CRUCE$NIF]
  }
  
  dt.MobileOnly <- dt.MobileOnly[, colnames(dt.MobileOnly) %in% noconvergidos.feats, with = F]
  
  if (ncol(dt.MobileOnly) == length(noconvergidos.feats)) {
    setcolorder(dt.MobileOnly, noconvergidos.feats)
  } else {
    cat("[WARN] Wrong number of columns in dt.MobileOnly =", ncol(dt.MobileOnly), "!=", length(noconvergidos.feats), "= noconvergidos.feats\n")
    cat("Only in dt.MobileOnly:   ", colnames(dt.MobileOnly)[(! colnames(dt.MobileOnly) %in% noconvergidos.feats)], "\n")
    cat("Only in noconvergidos.feats:", noconvergidos.feats[(! noconvergidos.feats %in% colnames(dt.MobileOnly))], "\n")
  }
  
  dt.test <- cleanDataCurrentSnapshot(dt.MobileOnly, month) # FIXME
  dt.test <- prepareData(dt.test, month) # FIXME: Copiados manualmente los datos de campañas (dt.Campaigns.201612.RData) de 20161222 a 201612 y la salida de ACC (dt_par_propensos_cross_nif_201612.RData) de 201610 a 201612
  
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  dev.null <- h2o.removeAll()
  cat("[INFO] Loading Data into H2O Cluster...\n")
  test.hex <- as.h2o(dt.test, "test.hex")
  
  ifile <- file.path(models.folder, model.month, "rf.model.h2o", "h2orf")
  cat("[LOAD]", ifile, "\n")
  h2o.rf <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  preds.new <- h2o.predict(object = h2o.rf, newdata = test.hex)
  preds.new <- as.data.frame(preds.new)
  preds.new <- as.data.table(preds.new)
  
  preds.new[, predict := NULL]
  preds.new[, x_num_ident := dt.test$x_num_ident]
  #preds.new[, FLAG_HUELLA_MOVISTAR := dt.test$FLAG_HUELLA_MOVISTAR]
  #preds.new[, FLAG_HUELLA_JAZZTEL := dt.test$FLAG_HUELLA_JAZZTEL]
  #preds.new[, FLAG_HUELLA_ONO := dt.test$FLAG_HUELLA_ONO]
  #preds.new[, FLAG_HUELLA_VF := dt.test$FLAG_HUELLA_VF]
  #preds.new[, FLAG_HUELLA_NEBA := dt.test$FLAG_HUELLA_NEBA]
  #preds.new[, FLAG_COBERTURA_ADSL := dt.test$FLAG_COBERTURA_ADSL]
  
  #setnames(preds.new, c("SCORE_NO_PROPENSO_CONV", "SCORE_PROPENSO_CONV", "x_num_ident", "FLAG_HUELLA_MOVISTAR", "FLAG_HUELLA_JAZZTEL", "FLAG_HUELLA_NEBA"))
  #setcolorder(preds.new,  c("x_num_ident", "SCORE_PROPENSO_CONV", "SCORE_NO_PROPENSO_CONV", "FLAG_HUELLA_MOVISTAR", "FLAG_HUELLA_JAZZTEL", "FLAG_HUELLA_NEBA")  )
  
  setnames(preds.new, c("SCORE_NO_PROPENSO_CONV", "SCORE_PROPENSO_CONV", "x_num_ident"))
  
  setcolorder(preds.new, c("x_num_ident", "SCORE_PROPENSO_CONV", "SCORE_NO_PROPENSO_CONV"))
  
  setkey(preds.new, SCORE_NO_PROPENSO_CONV)
  #preds <- preds.new[SCORE_PROPENSO_CONV > .5]
  preds <- preds.new
  preds[, SCORE_NO_PROPENSO_CONV := NULL]
  
  ofolder <- file.path(predictions.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(ofolder, paste0("convergence.predictions.", pred.day, ".RData"))
  cat("[SAVE]", ofile, "\n")
  save(preds.new, file = ofile)
  
  preds[, SCORE_PROPENSO_CONV := round(SCORE_PROPENSO_CONV, digits = 2)]
  ofile <- file.path(ofolder, paste0("convergence.predictions.", pred.day, ".csv"))
  cat("[SAVE]", ofile, "\n")
  write.table(preds, file = ofile, sep = "|", quote = F, row.names = F)
  
  # FIXME: Add test: search for duplicated
  
  #####################################
  ### Reorder by number of contacts ###
  #####################################
  # 
  # first.month <- "201609"
  # first.month.posix <- as.POSIXct(paste0(first.month, "01"), format="%Y%m%d")
  # month.posix <- as.POSIXct(paste0(month, "01"), format="%Y%m%d")
  # #prev.month.posix <- seq.POSIXt(month.posix, length=2, by="-1 months")[2]
  # #prev.month <- format(prev.month.posix, format = "%Y%m")
  # sequence <- seq.POSIXt(from = first.month.posix, to = month.posix, by = "months")
  # period <- unlist(lapply(sequence, format, "%Y%m"))
  # 
  # contactos <- NULL
  # for (m in period) {
  #   ifile <- file.path("~/Downloads/planificados_convergencia", paste0("Campaigns-Contactos-", m, ".txt"))
  #   if (file.exists(ifile)) {
  #     cat("[LOAD]", ifile)
  #     contactos_m <- fread(ifile)
  #     cat(" -", nrow(contactos_m), "\n")
  #     contactos <- rbind(contactos, contactos_m)
  #   } else {
  #     cat("[WARN] File", ifile, "does not exist\n")
  #   }
  # }
  # rm(contactos_m)
  # 
  # contactos_by_nif <- contactos[, .N, by = CIF_NIF]
  # setnames(contactos_by_nif, "CIF_NIF", "x_num_ident")
  # setkey(contactos_by_nif, x_num_ident)
  # 
  # preds.new.contacts <- contactos_by_nif[preds.new]
  # preds.new.contacts[is.na(N), N := 0]
  # #plot(preds.new.contacts$SCORE_PROPENSO_CONV, type = "l")
  # preds.new.contacts <- preds.new.contacts[with(preds.new.contacts, order(N, -SCORE_PROPENSO_CONV)), ]
  # #plot(preds.new.contacts$SCORE_PROPENSO_CONV, type = "l")
  # #hist(preds.new.contacts[, SCORE_PROPENSO_CONV], breaks = 100)
  # #hist(preds.new.contacts[N == 0, SCORE_PROPENSO_CONV], breaks = 100)
  # 
  # # Promote the 20000 clients with highest score that have never been contacted before (N == 0) to the top
  # 
  # threshold_score <- preds.new.contacts[N == 0][20000]$SCORE_PROPENSO_CONV
  # cat("[INFO] Threshold score for first 20000 not-contacted is ", threshold_score, "\n")
  # preds.new.contacts[, SCORE_PROPENSO_CONV_2 := SCORE_PROPENSO_CONV]
  # preds.new.contacts[N == 0 & SCORE_PROPENSO_CONV >= threshold_score, SCORE_PROPENSO_CONV_2 := 1]
  # preds.new.contacts <- preds.new.contacts[with(preds.new.contacts, order(-SCORE_PROPENSO_CONV)), ]
  # #hist(preds.new.contacts[, SCORE_PROPENSO_CONV_2], breaks = 100)
  # #hist(preds.new.contacts[N == 0, SCORE_PROPENSO_CONV_2], breaks = 100)
  # 
  # ofile <- file.path(ofolder, paste0("convergence.predictions.contacts.", pred.day, ".RData"))
  # cat("[SAVE]", ofile, "\n")
  # save(preds.new.contacts, file = ofile)
  # 
  # preds <- preds.new.contacts
  # preds[, SCORE_PROPENSO_CONV := round(SCORE_PROPENSO_CONV_2, digits = 2)]
  # preds[, SCORE_PROPENSO_CONV_2 := NULL]
  # preds[, N := NULL]
  # ofile <- file.path(ofolder, paste0("convergence.predictions.contacts.", pred.day, ".csv"))
  # cat("[SAVE]", ofile, "\n")
  # write.table(preds, file = ofile, sep = "|", quote = F, row.names = F)
  # 
  #####################################
}

#---------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--model"), type = "character", default = NULL, help = "model month (YYYYMM)", 
                metavar = "character"),
    make_option(c("-d", "--day"), type = "character", default = NULL, help = "predictions day (YYYYMMDD)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if ((is.null(opt$model)) || (is.null(opt$day))) {
    print_help(opt_parser)
    stop("Please, provide model month (YYYYMM), and predictions day (YYYYMMDD)", call.=FALSE)
  } else {
    main_07_prepare_prediction_data(opt$model, opt$day, NULL, NULL, NULL, NULL)
  }
}
