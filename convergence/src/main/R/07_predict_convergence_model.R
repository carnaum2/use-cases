source("configuration.R")
suppressMessages(library(caret))
suppressMessages(library(ROCR))
suppressMessages(library(xgboost))

cleanDataCurrentSnapshot <- function(dt.data, month) {
  cat("[INFO] CleanData() \n")
  # Get the last day of the month
  month <- paste0(month, "01")
  month.posix <- as.POSIXct(strptime(month, "%Y%m%d"))
  last.day <- seq.POSIXt(month.posix, length=2, by="months")[2]
  
  dt.data[, part_status := as.factor(part_status)]
  dt.data[, x_tipo_ident := as.factor(x_tipo_ident)]
  
  dt.data[, x_fecha_nacimiento := as.POSIXct(strptime(x_fecha_nacimiento, "%Y-%m-%d %H:%M:%S"))]
  dt.data[, IS_NA_FECHA_NACIMIENTO := 0]
  dt.data[is.na(x_fecha_nacimiento), IS_NA_FECHA_NACIMIENTO := 1]
  dt.data[is.na(x_fecha_nacimiento), x_fecha_nacimiento := as.POSIXct(strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))]
  dt.data[, EDAD := as.numeric(last.day - x_fecha_nacimiento)/365]
  dt.data[, x_fecha_nacimiento := NULL]
  
  dt.data[, x_nacionalidad := as.factor(x_nacionalidad)]
  
  dt.data[, IS_NA_SEG_CLIENTE := 0]
  dt.data[is.na(SEG_CLIENTE), IS_NA_SEG_CLIENTE := 1]
  dt.data[is.na(SEG_CLIENTE), SEG_CLIENTE := 0]
  
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
  dt.data[FECHA_FIN_CP_VF == "", IS_NA_FECHA_FIN_CP_VF := 1]
  dt.data[, FECHA_FIN_CP_VF := as.character(FECHA_FIN_CP_VF)]
  dt.data[IS_NA_FECHA_FIN_CP_VF == 1, FECHA_FIN_CP_VF := "1970-01-01"]
  
  dt.data[, FECHA_FIN_CP_VF := as.POSIXct(strptime(FECHA_FIN_CP_VF, "%Y-%m-%d"))]
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
  dt.data[, FECHA_FIN_CP_TARIFA := as.POSIXct(strptime(FECHA_FIN_CP_TARIFA, "%Y-%m-%d"))]
  
  dt.data[, DIAS_DESDE_FECHA_FIN_CP_TARIFA := as.numeric(FECHA_FIN_CP_TARIFA - last.day)/86400]
  dt.data[, FECHA_FIN_CP_TARIFA := NULL]
  
  dt.data[, IS_NA_MESES_FIN_CP_TARIFA := 0]
  dt.data[is.na(MESES_FIN_CP_TARIFA), IS_NA_MESES_FIN_CP_TARIFA := 1]
  dt.data[is.na(MESES_FIN_CP_TARIFA), MESES_FIN_CP_TARIFA := 0]
  
  dt.data[, modelo := as.factor(modelo)]
  
  dt.data[, sistema_operativo := as.factor(sistema_operativo)]
  
  dt.data[, LAST_UPD := as.POSIXct(strptime(substr(LAST_UPD, 1, 10), "%Y-%m-%d"))]  
  dt.data[is.na(LAST_UPD), LAST_UPD := as.POSIXct(strptime("1970-01-01", "%Y-%m-%d"))]
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
  
  dt.data[, IS_NA_CUOTAS_PENDIENTES := 0]
  dt.data[is.na(CUOTAS_PENDIENTES), IS_NA_CUOTAS_PENDIENTES := 1]
  dt.data[is.na(CUOTAS_PENDIENTES), CUOTAS_PENDIENTES := 0]
  
  dt.data[, IS_NA_CANTIDAD_PENDIENTE := 0]    
  dt.data[is.na(CANTIDAD_PENDIENTE), IS_NA_CANTIDAD_PENDIENTE := 1]    
  dt.data[is.na(CANTIDAD_PENDIENTE), CANTIDAD_PENDIENTE := 0]    
}

prepareData <- function(dt.data, month) {
  cat("[INFO] prepareData()\n")

  ifile <- file.path(data.folder, "Campaign", month, paste0("dt.Campaigns.", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  setkey(dt.Campaigns, x_num_ident)
  
  dt.data <- dt.Campaigns[dt.data]
  nombres <- names(dt.Campaigns)
  nombres <- nombres[!nombres %in% "x_num_ident" ]
  dt.data[is.na(number_of_contacts), nombres := -1, with = F]
  
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
  
}

main <- function(month, test.month, model) {
  
  
  # load train.
  #load("/")
  
  if (model == "rf") {
    ifile <- file.path(models.folder, month, paste0("rf.model.caret-", month, ".RData"))
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    dt.test <- prepareData(dt.test)
    
  } else if (model == "xg") {
    ifile <- file.path(models.folder, month, paste0("xgb.model-", month, ".RData"))
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    dt.test <- prepareDataXG(dt.test)
    
  }
  
  
  cat("Predicting...\n")
  
 if(model == "rf") {
    pred.1 <- predict(rf.combined, dt.test, type = "prob")
  
 } else if (model == "xg") {
    pred.2 <- predict(xgb.model, data.matrix(dt.test))
  }
  

  ofile <- file.path(predictions.folder, paste0(model, ".", month, ".pred.", test.month, ".", auc, 
                  #as.character(format(Sys.time(), "%d-%m-%Y-%H:%M:%S")), 
                  ".csv"))
  cat("[SAVE] ", ofile, "\n")
  write.table(dt.test[, c("x_num_ident", "PRED", "CLASS"), with = F], file = ofile, sep = "|", quote = F, row.names = F)
}

#--------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-tm", "--testmonth"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-model", "--model"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$month)) {
  print_help(opt_parser)
  stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
} else {
  main(opt$month, opt$testmonth, opt$model)
}
