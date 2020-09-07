source("configuration.R")

getDiscounts <- function(dt.train, month) {
  
  month.tmp <- paste0(month, "01")
  month.posix <- as.POSIXct(strptime(month.tmp, "%Y%m%d"))
  last.day <- seq.POSIXt(month.posix, length=4, by="months")[4]
  
  ifile <- paste0(cvm.input.folder, month, "/EXTR_DT_RIESG_FINPR_M_", month, ".TXT")
  cat("[LOAD] ", ifile, "\n")
  dt.desc_m <- fread(ifile)
  dt.desc_m <- unique(dt.desc_m, by = "MSISDN")
  
  dt.desc_m <- dt.desc_m[, c("MSISDN", "CODIGO_DESCUENTO", "ESTADO_DESCUENTO", "FX_ACTIVACION_DTO", "FX_DESACTIVACION_DTO")] 
  dt.desc_m[, FX_DESACTIVACION_DTO := as.POSIXct(strptime(FX_DESACTIVACION_DTO, "%Y%m%d"))]
  dt.desc_m[, FX_ACTIVACION_DTO := as.POSIXct(strptime(FX_ACTIVACION_DTO, "%Y%m%d"))]
  
  dt.desc_m[, DIAS_HASTA_FIN_DTO := round(as.numeric(FX_DESACTIVACION_DTO - last.day))/86400]
  dt.desc_m[, DIAS_DESDE_INI_DTO := round(as.numeric(last.day - FX_ACTIVACION_DTO))]
  
  dt.desc_m[, FX_ACTIVACION_DTO := NULL]
  dt.desc_m[, FX_DESACTIVACION_DTO := NULL]
  
  dt.desc_m[, MSISDN := as.character(MSISDN)]
  setkey(dt.desc_m, MSISDN)
  setkey(dt.train, MSISDN)
  dt.train <- dt.desc_m[dt.train]
  
  ifile <- paste0(cvm.input.folder, month, "/EXTR_DT_RIESG_FINPR_F_", month, ".TXT")
  cat("[LOAD] ", ifile, "\n")
  dt.desc_f <- fread(ifile)
  dt.desc_f <- unique(dt.desc_f, by = "NIF")
  dt.desc_f <- dt.desc_f[, c("NIF", "CODIGO_DESCUENTO", "ESTADO_DESCUENTO", "FX_ACTIVACION_DTO", "FX_DESACTIVACION_DTO")] 
  
  dt.desc_f[, FX_DESACTIVACION_DTO := as.POSIXct(strptime(FX_DESACTIVACION_DTO, "%Y%m%d"))]
  dt.desc_f[, FX_ACTIVACION_DTO := as.POSIXct(strptime(FX_ACTIVACION_DTO, "%Y%m%d"))]
  
  dt.desc_f[, DIAS_HASTA_FIN_DTO_F := round(as.numeric(FX_DESACTIVACION_DTO - last.day))/86400]
  dt.desc_f[, DIAS_DESDE_INI_DTO_F := round(as.numeric(last.day - FX_ACTIVACION_DTO))]
  
  dt.desc_f[, FX_ACTIVACION_DTO := NULL]
  dt.desc_f[, FX_DESACTIVACION_DTO := NULL]
  
  setnames(dt.desc_f, c("NIF", "CODIGO_DESCUENTO_F", "ESTADO_DESCUENTO_F", "DIAS_HASTA_FIN_DTO_F", "DIAS_DESDE_INI_DTO_F"))
  
  setkey(dt.desc_f, NIF)
  setkey(dt.train, NIF)
  
  dt.train <- dt.desc_f[dt.train]
  dt.train[is.na(dt.train)] <- -1
  
  dt.train[, CODIGO_DESCUENTO := as.factor(CODIGO_DESCUENTO)]
  dt.train[, ESTADO_DESCUENTO := as.factor(ESTADO_DESCUENTO)]
  
  dt.train[, CODIGO_DESCUENTO_F := as.factor(CODIGO_DESCUENTO_F)]
  dt.train[, ESTADO_DESCUENTO_F := as.factor(ESTADO_DESCUENTO_F)]
}

main <- function(car.month, rem.month, sol.month) {
  
  ifile <- paste0(acc.data.folder, car.month, "/dt.car.mo.",car.month, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
 
  dt.test <- copy(dt.car.mo)
  rm(dt.car.mo); dev.null <- gc()

  dt.test <- getDiscounts(dt.test, car.month)
  
  ifile <- paste0(cvm.data.folder, rem.month, "/dt.solicitudes.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)

  dt.test <- dt.test[!MSISDN %in% dt.solicitudes$MSISDN,]
  
  ifile <- paste0(cvm.data.folder, sol.month, "/dt.solicitudes.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.test[, CLASS := 0]
  dt.test[MSISDN %in% dt.solicitudes$MSISDN, CLASS := 1]
  
  cat("Number of port-outs: ", nrow(dt.test[CLASS == 1]), " from ", nrow(dt.test), " customers \n")
  
  print(summary(dt.test$CLASS))
  
  dt.test[, CLASS := as.factor(CLASS)]
  dt.test[is.na(dt.test)] <- -1
  
  ofile <- paste0(churn.datasets.folder, "/dt.test.mo.", car.month, ".", sol.month,".RData")
  cat("[SAVE] ", ofile, "\n")
  save(dt.test, file = ofile)
  
}
#----------------------------------------------------------------------------------
#main("201612", "201701", "201702")
#main("201701", "201702", "201703")
#main("201702", "201703", "201704")
main("201703", "201704", "201705")
