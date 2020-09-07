source("configuration.R")

cleanData <- function(dt.data, month) {
  cat("[INFO] CleanData() \n")
  # Get the last day of the month
  month <- paste0(month, "01")
  month.posix <- as.POSIXct(strptime(month, "%Y%m%d"))
  if (is.na(month.posix)) {
    cat("[ERROR] month must be a valid date in the form YYYYMM, to calculate AGE, and other features related with time shifts\n")
    quit()
  }
  last.day <- seq.POSIXt(month.posix, length=2, by="months")[2]

  dt.data[, part_status := as.factor(part_status)]
  dt.data[, x_tipo_ident := as.factor(x_tipo_ident)]
  
  dt.data[, x_fecha_nacimiento := as.POSIXct(strptime(x_fecha_nacimiento, "%Y-%m-%d %H:%M:%S"))]
  dt.data[, IS_NA_FECHA_NACIMIENTO := 0]
  dt.data[is.na(x_fecha_nacimiento), IS_NA_FECHA_NACIMIENTO := 1]
  dt.data[is.na(x_fecha_nacimiento), x_fecha_nacimiento := as.POSIXct(strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))] # FIXME
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
    cat("[ERROR] x_fecha_activacion Greather than Last Monthly day in ",
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
  
  dt.data[, X_FECHA_CREACION_SERVICIO := as.POSIXct(strptime(X_FECHA_CREACION_SERVICIO, "%Y-%m-%d %H:%M:%S"))]
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
  dt.data[, FECHA_FIN_CP_VF := as.character(FECHA_FIN_CP_VF)]
  dt.data[IS_NA_FECHA_FIN_CP_VF == 1, FECHA_FIN_CP_VF := "19700101"] # FIXME
  
  dt.data[, FECHA_FIN_CP_VF := as.POSIXct(strptime(FECHA_FIN_CP_VF, "%Y%m%d"))]
  dt.data[, DIAS_HASTA_FIN_CP_VF := as.numeric(FECHA_FIN_CP_VF - last.day)]
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
  dt.data[IS_NA_FECHA_FIN_CP_TARIFA == 1, FECHA_FIN_CP_TARIFA := "19700101"] # FIXME
  dt.data[, FECHA_FIN_CP_TARIFA := as.POSIXct(strptime(FECHA_FIN_CP_TARIFA, "%Y%m%d"))]
  
  dt.data[, DIAS_DESDE_FECHA_FIN_CP_TARIFA := as.numeric(FECHA_FIN_CP_TARIFA - last.day)]
  dt.data[, FECHA_FIN_CP_TARIFA := NULL]
  
  dt.data[, IS_NA_MESES_FIN_CP_TARIFA := 0]
  dt.data[is.na(MESES_FIN_CP_TARIFA), IS_NA_MESES_FIN_CP_TARIFA := 1]
  dt.data[is.na(MESES_FIN_CP_TARIFA), MESES_FIN_CP_TARIFA := 0]
  
  dt.data[, modelo := as.factor(modelo)]
  
  dt.data[, sistema_operativo := as.factor(sistema_operativo)]
  
  dt.data[, LAST_UPD := as.POSIXct(strptime(substr(LAST_UPD, 1, 10), "%Y-%m-%d"))]  
  dt.data[is.na(LAST_UPD), LAST_UPD := as.POSIXct(strptime("1970-01-01", "%Y-%m-%d"))] # FIXME
  dt.data[, DIAS_DESDE_LAST_UPD := as.numeric(last.day - LAST_UPD)]
  dt.data[, LAST_UPD := NULL]
  
  dt.data[, PPRECIOS_DESTINO := as.factor(PPRECIOS_DESTINO)]
  
  dt.data[, ppid_destino := as.factor(ppid_destino)]
  
  dt.data[, IS_NA_PUNTOS := 0]
  dt.data[is.na(PUNTOS), IS_NA_PUNTOS := 1]
  dt.data[is.na(PUNTOS), PUNTOS := 0]
  
  dt.data[, IS_NA_ARPU := 0]
  dt.data[is.na(ARPU), IS_NA_ARPU := 1]
  dt.data[is.na(ARPU), ARPU := 0]
  
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

main_05_clean_convergidos_and_not_convergidos <- function(month, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  cat ("\n[MAIN] 05_clean_convergidos_and_not_convergidos", month, "\n")
  
  ifile <- file.path(datasets.folder, month, paste0("dt.Convergidos-", month, ".RData"))
  if (is.null(dt.Convergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    cat("[INFO] Number of Rows: ", nrow(dt.Convergidos),"\n")
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  if (!"FLAG_HUELLA_NEBA" %in% colnames(dt.Convergidos)) { # in 201605 we do not have FLAG_HUELLA_NEBA
    cat("[INFO] Adding FLAG_HUELLA_NEBA to dt.Convergidos\n")
    dt.Convergidos[, FLAG_HUELLA_NEBA := 0]
  }
  dt.Convergidos <- dt.Convergidos[, colnames(dt.Convergidos) %in% convergidos.feats, with = F]
  if (ncol(dt.Convergidos) == length(convergidos.feats)) {
    setcolorder(dt.Convergidos, convergidos.feats)
  } else {
    cat("[WARN] Wrong number of columns in dt.Convergidos =", ncol(dt.Convergidos), "!=", length(convergidos.feats), "= convergidos.feats\n")
    cat("Only in dt.Convergidos:   ", colnames(dt.Convergidos)[(! colnames(dt.Convergidos) %in% convergidos.feats)], "\n")
    cat("Only in convergidos.feats:", convergidos.feats[(! convergidos.feats %in% colnames(dt.Convergidos))], "\n")
  }
  
  dt.Convergidos <- cleanData(dt.Convergidos, month)
  
  ofile <- file.path(datasets.folder, month, paste0("dt.Convergidos-train-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.Convergidos), "\n")
  save(dt.Convergidos, file = ofile)
  #rm(dt.Convergidos); dev.null <- gc()
  
  ifile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-", month, ".RData"))
  if (is.null(dt.NoConvergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    cat("[INFO Number of Rows: ", nrow(dt.NoConvergidos),"\n")
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  if (!"FLAG_HUELLA_NEBA" %in% colnames(dt.NoConvergidos)) { # in 201605 we do not have FLAG_HUELLA_NEBA
    cat("[INFO] Adding FLAG_HUELLA_NEBA to dt.NoConvergidos\n")
    dt.NoConvergidos[, FLAG_HUELLA_NEBA := 0]
  }
  
  # FIXME: Why not FLAG_COBERTURA_ADSL?
  #dt.NoConvergidos <- dt.NoConvergidos[FLAG_HUELLA_ONO == 1 | FLAG_HUELLA_VF == 1 | FLAG_HUELLA_NEBA == 1  | FLAG_COBERTURA_ADSL == "D"]
  dt.NoConvergidos <- dt.NoConvergidos[FLAG_HUELLA_ONO == 1 | FLAG_HUELLA_VF == 1 | FLAG_HUELLA_NEBA == 1]
  
  dt.NoConvergidos <- dt.NoConvergidos[, colnames(dt.NoConvergidos) %in% noconvergidos.feats, with = F]
  if (ncol(dt.NoConvergidos) == length(noconvergidos.feats)) {
    setcolorder(dt.NoConvergidos, noconvergidos.feats)
  } else {
    cat("[WARN] Wrong number of columns in dt.NoConvergidos =", ncol(dt.NoConvergidos), "!=", length(noconvergidos.feats), "= noconvergidos.feats\n")
    cat("Only in dt.NoConvergidos:   ", colnames(dt.NoConvergidos)[(! colnames(dt.NoConvergidos) %in% noconvergidos.feats)], "\n")
    cat("Only in noconvergidos.feats:", noconvergidos.feats[(! noconvergidos.feats %in% colnames(dt.NoConvergidos))], "\n")
  }
  
  dt.NoConvergidos <- cleanData(dt.NoConvergidos, month)
  
  ofile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-train-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.NoConvergidos), "\n")
  save(dt.NoConvergidos, file = ofile)
  
  invisible(list(dt.Convergidos=dt.Convergidos, dt.NoConvergidos=dt.NoConvergidos))
}
#--------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
  } else {
    main_05_clean_convergidos_and_not_convergidos(opt$month, NULL, NULL)
  }
}
